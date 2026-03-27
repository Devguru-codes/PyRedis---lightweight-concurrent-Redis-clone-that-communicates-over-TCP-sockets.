"""Application-specific exceptions for PyRedis."""

from __future__ import annotations


class PyRedisError(Exception):
    """Base exception for application-level errors."""


class CommandError(PyRedisError):
    """Raised when a command cannot be executed successfully."""


class WrongTypeError(CommandError):
    """Raised when a key holds an unexpected type."""


class ProtocolError(PyRedisError):
    """Raised when a client sends invalid RESP data."""
