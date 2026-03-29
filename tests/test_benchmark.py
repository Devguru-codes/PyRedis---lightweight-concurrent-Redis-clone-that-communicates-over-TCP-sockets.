from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_stress_client_writes_json_report(redis_server, tmp_path: Path):
    host, port, _server = redis_server
    output_path = tmp_path / "benchmark.json"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "scripts/stress_client.py",
        "--host",
        host,
        "--port",
        str(port),
        "--requests",
        "8",
        "--concurrency",
        "2",
        "--p95-threshold-ms",
        "1000",
        "--json-out",
        str(output_path),
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode("utf-8")
    assert "p95_ms=" in stdout.decode("utf-8")
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["requests"] == 8
    assert report["concurrency"] == 2
    assert report["p50_ms"] >= 0
    assert report["p95_ms"] >= report["p50_ms"]
    assert report["p99_ms"] >= report["p95_ms"]


@pytest.mark.asyncio
async def test_stress_client_supports_pipelining(redis_server, tmp_path: Path):
    host, port, _server = redis_server
    output_path = tmp_path / "pipeline-benchmark.json"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "scripts/stress_client.py",
        "--host",
        host,
        "--port",
        str(port),
        "--requests",
        "4",
        "--concurrency",
        "2",
        "--pipeline-depth",
        "3",
        "--p95-threshold-ms",
        "1000",
        "--json-out",
        str(output_path),
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode("utf-8")
    assert "pipeline_depth=3" in stdout.decode("utf-8")
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["pipeline_depth"] == 3
