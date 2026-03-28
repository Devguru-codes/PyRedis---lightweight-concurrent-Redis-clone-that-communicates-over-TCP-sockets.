from __future__ import annotations

from pathlib import Path

import pytest

from pyredis.config import load_config


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


def test_load_config_raises_for_missing_file(tmp_path: Path):
    missing = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError):
        load_config(str(missing))
