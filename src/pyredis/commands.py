"""Command registry and concrete command implementations."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .datastore import DataStore
from .errors import CommandError, PyRedisError
from .persistence import SnapshotManager
from .protocol import encode_array, encode_bulk, encode_error, encode_integer, encode_simple


@dataclass(slots=True)
class CommandContext:
    datastore: DataStore
    stats: dict[str, int]
    server_started_at: int
    snapshot_manager: SnapshotManager | None = None
    require_password: str | None = None
    authenticated: bool = False
    connection_id: int = 0
    connection_stats: dict[str, int] = field(default_factory=dict)


class Command:
    name = ""

    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        raise NotImplementedError


COMMANDS: dict[str, type[Command]] = {}


def register_command(name: str):
    def decorator(cls: type[Command]) -> type[Command]:
        cls.name = name
        COMMANDS[name] = cls
        return cls

    return decorator


async def dispatch_command(context: CommandContext, command_parts: list[str]) -> bytes:
    if not command_parts:
        return encode_error("ERR empty command")
    name = command_parts[0].upper()
    if context.require_password and not context.authenticated and name not in {"AUTH", "PING"}:
        context.stats["command_errors"] += 1
        return encode_error("NOAUTH Authentication required")
    command_cls = COMMANDS.get(name)
    if command_cls is None:
        context.stats["command_errors"] += 1
        return encode_error(f"ERR unknown command '{command_parts[0]}'")
    context.stats["commands_processed"] += 1
    command = command_cls()
    try:
        return await command.execute(context, command_parts[1:])
    except (CommandError, PyRedisError, ValueError) as exc:
        context.stats["command_errors"] += 1
        return encode_error(str(exc))
    except Exception:
        context.stats["command_errors"] += 1
        return encode_error("ERR internal server error")


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


@register_command("PING")
class PingCommand(Command):
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


@register_command("DEL")
class DelCommand(Command):
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
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'EXPIRE'")
        seconds = _parse_int_arg(args[1], "EXPIRE", "ERR invalid expire time in 'EXPIRE'")
        if seconds <= 0:
            raise CommandError("ERR invalid expire time in 'EXPIRE'")
        return encode_integer(1 if await context.datastore.expire(args[0], seconds) else 0)


@register_command("TTL")
class TTLCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'TTL'")
        return encode_integer(await context.datastore.ttl(args[0]))


@register_command("INCR")
class IncrCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'INCR'")
        return encode_integer(await context.datastore.incr(args[0]))


@register_command("INCRBY")
class IncrByCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'INCRBY'")
        return encode_integer(await context.datastore.incrby(args[0], _parse_int_arg(args[1], "INCRBY")))


@register_command("DECR")
class DecrCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'DECR'")
        return encode_integer(await context.datastore.incrby(args[0], -1))


@register_command("DECRBY")
class DecrByCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'DECRBY'")
        return encode_integer(await context.datastore.incrby(args[0], -_parse_int_arg(args[1], "DECRBY")))


@register_command("FLUSHALL")
class FlushAllCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'FLUSHALL'")
        await context.datastore.flushall()
        return encode_simple("OK")


@register_command("INFO")
class InfoCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'INFO'")
        info = await context.datastore.info()
        uptime = int(time.time()) - context.server_started_at
        info_lines = [
            "# Server",
            f"commands_processed:{context.stats['commands_processed']}",
            f"command_errors:{context.stats['command_errors']}",
            f"active_connections:{context.stats['active_connections']}",
            f"total_connections:{context.stats['total_connections']}",
            f"uptime_seconds:{uptime}",
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
        ]
        return encode_bulk("\r\n".join(info_lines))


@register_command("ZADD")
class ZAddCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 3 or len(args) % 2 == 0:
            raise CommandError("ERR wrong number of arguments for 'ZADD'")
        key = args[0]
        pairs: list[tuple[float, str]] = []
        raw_pairs = args[1:]
        for index in range(0, len(raw_pairs), 2):
            score = _parse_float_arg(raw_pairs[index], "ZADD")
            member = raw_pairs[index + 1]
            pairs.append((score, member))
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


@register_command("PERSIST")
class PersistCommand(Command):
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


@register_command("MGET")
class MGetCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if not args:
            raise CommandError("ERR wrong number of arguments for 'MGET'")
        values = await context.datastore.mget(args)
        return _encode_nullable_array(values)


@register_command("MSET")
class MSetCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 2 or len(args) % 2 != 0:
            raise CommandError("ERR wrong number of arguments for 'MSET'")
        pairs = [(args[index], args[index + 1]) for index in range(0, len(args), 2)]
        await context.datastore.mset(pairs)
        return encode_simple("OK")


@register_command("SETNX")
class SetNxCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) != 2:
            raise CommandError("ERR wrong number of arguments for 'SETNX'")
        return encode_integer(1 if await context.datastore.setnx(args[0], args[1]) else 0)


@register_command("APPEND")
class AppendCommand(Command):
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


@register_command("SAVE")
class SaveCommand(Command):
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if args:
            raise CommandError("ERR wrong number of arguments for 'SAVE'")
        if context.snapshot_manager is None:
            raise CommandError("ERR snapshot persistence is not configured")
        await context.snapshot_manager.save(context.datastore)
        return encode_simple("OK")


@register_command("AUTH")
class AuthCommand(Command):
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


def _encode_nullable_array(values: list[str | None]) -> bytes:
    parts = [f"*{len(values)}\r\n".encode("utf-8")]
    for value in values:
        parts.append(encode_bulk(value))
    return b"".join(parts)


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
    async def execute(self, context: CommandContext, args: list[str]) -> bytes:
        if len(args) < 2:
            raise CommandError("ERR wrong number of arguments for 'ZREM'")
        return encode_integer(await context.datastore.zrem(args[0], *args[1:]))
