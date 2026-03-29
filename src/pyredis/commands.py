"""Command registry and concrete command implementations."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .datastore import DataStore
from .errors import CommandError, PyRedisError
from .persistence import AppendOnlyManager, SnapshotManager
from .protocol import encode_array, encode_bulk, encode_error, encode_integer, encode_simple


@dataclass(slots=True)
class CommandContext:
    datastore: DataStore
    stats: dict[str, object]
    server_started_at: int
    snapshot_manager: SnapshotManager | None = None
    aof_manager: AppendOnlyManager | None = None
    require_password: str | None = None
    authenticated: bool = False
    connection_id: int = 0
    connection_stats: dict[str, int] = field(default_factory=dict)
    in_transaction: bool = False
    transaction_queue: list[list[str]] = field(default_factory=list)
    replaying_aof: bool = False


class Command:
    name = ""
    is_write = False
    immediate = False

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        raise NotImplementedError


COMMANDS: dict[str, type[Command]] = {}
AUTH_WHITELIST = {"AUTH", "PING"}


def register_command(name: str):
    def decorator(cls: type[Command]) -> type[Command]:
        cls.name = name
        COMMANDS[name] = cls
        return cls

    return decorator


async def dispatch_command(context: CommandContext, command_parts: list[str]) -> bytes:
    if not command_parts:
        return encode_error("ERR empty command")
    command_name = command_parts[0].upper()
    if context.require_password and not context.authenticated and command_name not in AUTH_WHITELIST:
        _increment_counter(context.stats, "command_errors")
        return encode_error("NOAUTH Authentication required")
    if context.in_transaction and command_name not in {"EXEC", "DISCARD", "MULTI", "AUTH"}:
        context.transaction_queue.append(command_parts.copy())
        return encode_simple("QUEUED")
    return await _run_command(context, command_parts)


async def _run_command(
    context: CommandContext,
    command_parts: list[str],
    *,
    bypass_auth: bool = False,
) -> bytes:
    if not command_parts:
        return encode_error("ERR empty command")
    name = command_parts[0].upper()
    if not bypass_auth and context.require_password and not context.authenticated and name not in AUTH_WHITELIST:
        _increment_counter(context.stats, "command_errors")
        return encode_error("NOAUTH Authentication required")
    command_cls = COMMANDS.get(name)
    if command_cls is None:
        _increment_counter(context.stats, "command_errors")
        return encode_error(f"ERR unknown command '{command_parts[0]}'")

    _increment_counter(context.stats, "commands_processed")
    _increment_nested_counter(context.stats, "per_command_counts", name)
    started = time.perf_counter_ns()
    command = command_cls()
    try:
        response = await command.execute(context, command_parts[1:])
        latency_us = (time.perf_counter_ns() - started) / 1000
        _record_latency(context.stats, name, latency_us)
        if command.is_write and context.aof_manager is not None and not context.replaying_aof:
            await context.aof_manager.append(command_parts)
        return response
    except (CommandError, PyRedisError, ValueError) as exc:
        latency_us = (time.perf_counter_ns() - started) / 1000
        _record_latency(context.stats, name, latency_us)
        _increment_counter(context.stats, "command_errors")
        return encode_error(str(exc))
    except Exception:
        latency_us = (time.perf_counter_ns() - started) / 1000
        _record_latency(context.stats, name, latency_us)
        _increment_counter(context.stats, "command_errors")
        return encode_error("ERR internal server error")


def _increment_counter(stats: dict[str, object], key: str, amount: int = 1) -> None:
    stats[key] = int(stats.get(key, 0)) + amount


def _increment_nested_counter(stats: dict[str, object], key: str, nested_key: str) -> None:
    bucket = stats.setdefault(key, {})
    assert isinstance(bucket, dict)
    bucket[nested_key] = int(bucket.get(nested_key, 0)) + 1


def _record_latency(stats: dict[str, object], command_name: str, latency_us: float) -> None:
    stats["last_command_latency_us"] = round(latency_us, 2)
    totals = stats.setdefault("per_command_latency_us_total", {})
    assert isinstance(totals, dict)
    totals[command_name] = round(float(totals.get(command_name, 0.0)) + latency_us, 2)


def _parse_int_arg(value: str, command_name: str, message: str | None = None) -> int:
    try:
        return int(value)
    except ValueError as exc:
        if message is not None:
            raise CommandError(message) from exc
        raise CommandError(f"ERR value for '{command_name}' must be an integer") from exc


def _parse_float_arg(value: str, command_name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise CommandError(f"ERR value for '{command_name}' must be a float") from exc


def _encode_nullable_array(values: list[str | None]) -> bytes:
    parts = [f"*{len(values)}\r\n".encode("utf-8")]
    for value in values:
        parts.append(encode_bulk(value))
    return b"".join(parts)


def _encode_raw_array(values: list[bytes]) -> bytes:
    return f"*{len(values)}\r\n".encode("utf-8") + b"".join(values)


@register_command("PING")
class PingCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if not args:
            return encode_simple("PONG")
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'PING'")
        return encode_bulk(args[0])


@register_command("ECHO")
class EchoCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'ECHO'")
        return encode_bulk(args[0])


@register_command("SET")
class SetCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) not in (2, 4):
            raise CommandError("ERR wrong number of arguments for 'SET'")
        key, value = args[0], args[1]
        ex = None
        if len(args) == 4:
            if args[2].upper() != "EX":
                raise CommandError("ERR syntax error")
            ex = _parse_int_arg(args[3], "SET", "ERR invalid expire time in 'SET'")
            if ex <= 0:
                raise CommandError("ERR invalid expire time in 'SET'")
        await context.datastore.set(key, value, ex=ex)
        return encode_simple("OK")


@register_command("GET")
class GetCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'GET'")
        return encode_bulk(await context.datastore.get(args[0]))


@register_command("GETSET")
class GetSetCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'GETSET'")
        return encode_bulk(await context.datastore.getset(args[0], args[1]))


@register_command("DEL")
class DelCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if not args:
            raise CommandError("ERR wrong number of arguments for 'DEL'")
        return encode_integer(await context.datastore.delete(*args))


@register_command("EXISTS")
class ExistsCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if not args:
            raise CommandError("ERR wrong number of arguments for 'EXISTS'")
        return encode_integer(await context.datastore.exists(*args))


@register_command("EXPIRE")
class ExpireCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'EXPIRE'")
        seconds = _parse_int_arg(args[1], "EXPIRE", "ERR invalid expire time in 'EXPIRE'")
        if seconds <= 0:
            raise CommandError("ERR invalid expire time in 'EXPIRE'")
        return encode_integer(1 if await context.datastore.expire(args[0], seconds) else 0)


@register_command("PEXPIRE")
class PExpireCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'PEXPIRE'")
        milliseconds = _parse_int_arg(args[1], "PEXPIRE", "ERR invalid expire time in 'PEXPIRE'")
        if milliseconds <= 0:
            raise CommandError("ERR invalid expire time in 'PEXPIRE'")
        return encode_integer(1 if await context.datastore.pexpire(args[0], milliseconds) else 0)


@register_command("TTL")
class TTLCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'TTL'")
        return encode_integer(await context.datastore.ttl(args[0]))


@register_command("PTTL")
class PTTLCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'PTTL'")
        return encode_integer(await context.datastore.pttl(args[0]))


@register_command("INCR")
class IncrCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'INCR'")
        return encode_integer(await context.datastore.incr(args[0]))


@register_command("INCRBY")
class IncrByCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'INCRBY'")
        return encode_integer(await context.datastore.incrby(args[0], _parse_int_arg(args[1], "INCRBY")))


@register_command("DECR")
class DecrCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'DECR'")
        return encode_integer(await context.datastore.incrby(args[0], -1))


@register_command("DECRBY")
class DecrByCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'DECRBY'")
        return encode_integer(await context.datastore.incrby(args[0], -_parse_int_arg(args[1], "DECRBY")))


@register_command("MSET")
class MSetCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 2 or len(args) % 2 != 0:
            raise CommandError("ERR wrong number of arguments for 'MSET'")
        pairs = [(args[index], args[index + 1]) for index in range(0, len(args), 2)]
        await context.datastore.mset(pairs)
        return encode_simple("OK")


@register_command("MGET")
class MGetCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if not args:
            raise CommandError("ERR wrong number of arguments for 'MGET'")
        return _encode_nullable_array(await context.datastore.mget(args))


@register_command("SETNX")
class SetNxCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'SETNX'")
        return encode_integer(1 if await context.datastore.setnx(args[0], args[1]) else 0)


@register_command("APPEND")
class AppendCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'APPEND'")
        return encode_integer(await context.datastore.append(args[0], args[1]))


@register_command("STRLEN")
class StrLenCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'STRLEN'")
        return encode_integer(await context.datastore.strlen(args[0]))


@register_command("KEYS")
class KeysCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'KEYS'")
        return encode_array(await context.datastore.keys(args[0]))


@register_command("RENAME")
class RenameCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'RENAME'")
        await context.datastore.rename(args[0], args[1])
        return encode_simple("OK")


@register_command("FLUSHALL")
class FlushAllCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'FLUSHALL'")
        await context.datastore.flushall()
        return encode_simple("OK")


@register_command("PERSIST")
class PersistCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'PERSIST'")
        return encode_integer(1 if await context.datastore.persist(args[0]) else 0)


@register_command("TYPE")
class TypeCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'TYPE'")
        return encode_simple((await context.datastore.type_of(args[0])).upper())


@register_command("DBSIZE")
class DbSizeCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'DBSIZE'")
        return encode_integer(await context.datastore.dbsize())


@register_command("INFO")
class InfoCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'INFO'")
        info = await context.datastore.info()
        uptime = int(time.time()) - context.server_started_at
        command_counts = context.stats.get("per_command_counts", {})
        assert isinstance(command_counts, dict)
        latency_totals = context.stats.get("per_command_latency_us_total", {})
        assert isinstance(latency_totals, dict)
        latency_lines = [
            f"command_count_{name.lower()}:{count}" for name, count in sorted(command_counts.items())
        ] + [
            f"command_latency_us_total_{name.lower()}:{value}" for name, value in sorted(latency_totals.items())
        ]
        info_lines = [
            "# Server",
            f"commands_processed:{context.stats['commands_processed']}",
            f"command_errors:{context.stats['command_errors']}",
            f"active_connections:{context.stats['active_connections']}",
            f"total_connections:{context.stats['total_connections']}",
            f"uptime_seconds:{uptime}",
            f"last_command_latency_us:{context.stats.get('last_command_latency_us', 0)}",
            "# Keyspace",
            f"keys:{info['keys']}",
            f"max_keys:{info['max_keys']}",
            f"expiring_keys:{info['expiring_keys']}",
            f"read_hits:{info['read_hits']}",
            f"read_misses:{info['read_misses']}",
            f"expired_keys:{info['expired_keys']}",
            f"evicted_keys:{info['evicted_keys']}",
            f"total_reads:{info['total_reads']}",
            f"total_writes:{info['total_writes']}",
            f"snapshot_saves:{info['snapshot_saves']}",
            f"snapshot_loads:{info['snapshot_loads']}",
        ] + latency_lines
        return encode_bulk("\r\n".join(info_lines))


@register_command("AUTH")
class AuthCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'AUTH'")
        if context.require_password is None:
            context.authenticated = True
            return encode_simple("OK")
        if args[0] != context.require_password:
            raise CommandError("ERR invalid password")
        context.authenticated = True
        return encode_simple("OK")


@register_command("SAVE")
class SaveCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'SAVE'")
        if context.snapshot_manager is None:
            raise CommandError("ERR snapshot persistence is not configured")
        await context.snapshot_manager.save(context.datastore)
        if context.aof_manager is not None:
            await context.aof_manager.rewrite_from_snapshot(context.datastore)
        return encode_simple("OK")


@register_command("BGSAVE")
class BgSaveCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'BGSAVE'")
        if context.snapshot_manager is None:
            raise CommandError("ERR snapshot persistence is not configured")
        started = await context.snapshot_manager.bgsave(context.datastore)
        if not started:
            raise CommandError("ERR background save already in progress")
        return encode_simple("Background saving started")


@register_command("MULTI")
class MultiCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'MULTI'")
        if context.in_transaction:
            raise CommandError("ERR MULTI calls can not be nested")
        context.in_transaction = True
        context.transaction_queue.clear()
        return encode_simple("OK")


@register_command("DISCARD")
class DiscardCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'DISCARD'")
        if not context.in_transaction:
            raise CommandError("ERR DISCARD without MULTI")
        context.in_transaction = False
        context.transaction_queue.clear()
        return encode_simple("OK")


@register_command("EXEC")
class ExecCommand(Command):
    immediate = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'EXEC'")
        if not context.in_transaction:
            raise CommandError("ERR EXEC without MULTI")
        queued = list(context.transaction_queue)
        context.transaction_queue.clear()
        context.in_transaction = False
        responses = [await _run_command(context, command_parts, bypass_auth=True) for command_parts in queued]
        return _encode_raw_array(responses)


@register_command("ZADD")
class ZAddCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 3 or len(args) % 2 == 0:
            raise CommandError("ERR wrong number of arguments for 'ZADD'")
        key = args[0]
        pairs: list[tuple[float, str]] = []
        raw_pairs = args[1:]
        for index in range(0, len(raw_pairs), 2):
            pairs.append((_parse_float_arg(raw_pairs[index], "ZADD"), raw_pairs[index + 1]))
        return encode_integer(await context.datastore.zadd(key, pairs))


@register_command("ZRANGE")
class ZRangeCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 3:
            raise CommandError("ERR wrong number of arguments for 'ZRANGE'")
        members = await context.datastore.zrange(
            args[0],
            _parse_int_arg(args[1], "ZRANGE"),
            _parse_int_arg(args[2], "ZRANGE"),
        )
        return encode_array(members)


@register_command("ZSCORE")
class ZScoreCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'ZSCORE'")
        score = await context.datastore.zscore(args[0], args[1])
        return encode_bulk(None if score is None else f"{score:g}")


@register_command("ZCARD")
class ZCardCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'ZCARD'")
        return encode_integer(await context.datastore.zcard(args[0]))


@register_command("ZREM")
class ZRemCommand(Command):
    is_write = True

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 2:
            raise CommandError("ERR wrong number of arguments for 'ZREM'")
        return encode_integer(await context.datastore.zrem(args[0], *args[1:]))
