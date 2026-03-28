"""Async TCP server implementation for PyRedis."""

from __future__ import annotations

import asyncio
import contextlib
import time

from .commands import CommandContext, dispatch_command
from .config import ServerConfig
from .datastore import DataStore
from .errors import ProtocolError
from .persistence import SnapshotManager
from .protocol import encode_error, read_command


class PyRedisServer:
    def __init__(self, config: ServerConfig | None = None) -> None:
        self.config = config or ServerConfig()
        self.datastore = DataStore(max_keys=self.config.max_keys)
        self.snapshot_manager = SnapshotManager(self.config.snapshot_path)
        self.stats = {
            "commands_processed": 0,
            "command_errors": 0,
            "total_connections": 0,
            "active_connections": 0,
            "started_at": int(time.time()),
        }
        self._server: asyncio.AbstractServer | None = None
        self._expiry_task: asyncio.Task | None = None
        self._connection_sequence = 0

    async def start(self) -> None:
        if self._server is not None:
            return
        if self.config.load_snapshot_on_startup:
            await self.snapshot_manager.load(self.datastore)
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.config.host,
            port=self.config.port,
        )
        self._expiry_task = asyncio.create_task(self._expiry_loop())

    async def serve_forever(self) -> None:
        await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self.config.snapshot_on_shutdown:
            await self.snapshot_manager.save(self.datastore)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._expiry_task is not None:
            self._expiry_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._expiry_task
            self._expiry_task = None

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None or not self._server.sockets:
            return self.config.host, self.config.port
        host, port = self._server.sockets[0].getsockname()[:2]
        return host, port

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.stats["total_connections"] += 1
        self.stats["active_connections"] += 1
        self._connection_sequence += 1
        context = CommandContext(
            datastore=self.datastore,
            stats=self.stats,
            server_started_at=self.stats["started_at"],
            snapshot_manager=self.snapshot_manager,
            require_password=self.config.require_password,
            authenticated=self.config.require_password is None,
            connection_id=self._connection_sequence,
        )
        try:
            while True:
                try:
                    parts = await asyncio.wait_for(
                        read_command(
                            reader,
                            max_command_parts=self.config.max_command_parts,
                            max_bulk_length=self.config.max_bulk_length,
                        ),
                        timeout=self.config.client_idle_timeout,
                    )
                except EOFError:
                    break
                except asyncio.TimeoutError:
                    writer.write(encode_error("ERR client idle timeout"))
                    await writer.drain()
                    break
                except ProtocolError as exc:
                    self.stats["command_errors"] += 1
                    writer.write(encode_error(f"ERR protocol error: {exc}"))
                    await writer.drain()
                    break
                response = await dispatch_command(context, parts)
                writer.write(response)
                await writer.drain()
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            self.stats["active_connections"] = max(self.stats["active_connections"] - 1, 0)
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def _expiry_loop(self) -> None:
        while True:
            await self.datastore.purge_expired()
            await asyncio.sleep(self.config.ttl_check_interval)
