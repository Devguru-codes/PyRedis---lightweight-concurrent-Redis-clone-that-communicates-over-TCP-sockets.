"""Configuration values for PyRedis."""

from dataclasses import dataclass


@dataclass(slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 6380
    max_keys: int = 1024
    ttl_check_interval: float = 0.25
    max_command_parts: int = 128
    max_bulk_length: int = 1024 * 1024
    client_idle_timeout: float = 30.0
