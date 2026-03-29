from __future__ import annotations

from pathlib import Path

import pytest

from pyredis.config import ServerConfig, load_config, validate_config


def test_load_config_from_toml(tmp_path: Path):
    config_path = tmp_path / "pyredis.toml"
    config_path.write_text(
        """
[server]
host = "0.0.0.0"
port = 6390
max_keys = 55
ttl_check_interval = 0.1
require_password = "secret"
snapshot_path = "data/custom-dump.json"
snapshot_on_shutdown = true
load_snapshot_on_startup = false
snapshot_interval_seconds = 1.5
appendonly_enabled = true
appendonly_path = "data/appendonly.aof"
appendfsync_always = true
metrics_enabled = true
metrics_host = "0.0.0.0"
metrics_port = 9200
log_level = "DEBUG"
""".strip(),
        encoding="utf-8",
    )
    config = load_config(str(config_path))
    assert config.host == "0.0.0.0"
    assert config.port == 6390
    assert config.max_keys == 55
    assert config.ttl_check_interval == 0.1
    assert config.require_password == "secret"
    assert config.snapshot_path == "data/custom-dump.json"
    assert config.snapshot_on_shutdown is True
    assert config.load_snapshot_on_startup is False
    assert config.snapshot_interval_seconds == 1.5
    assert config.appendonly_enabled is True
    assert config.appendonly_path == "data/appendonly.aof"
    assert config.appendfsync_always is True
    assert config.metrics_enabled is True
    assert config.metrics_host == "0.0.0.0"
    assert config.metrics_port == 9200
    assert config.log_level == "DEBUG"


def test_load_config_raises_for_missing_file(tmp_path: Path):
    missing = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError):
        load_config(str(missing))


def test_validate_config_rejects_invalid_values():
    with pytest.raises(ValueError):
        validate_config(ServerConfig(port=70000))
    with pytest.raises(ValueError):
        validate_config(ServerConfig(metrics_port=-1))
    with pytest.raises(ValueError):
        validate_config(ServerConfig(log_format="yaml"))
