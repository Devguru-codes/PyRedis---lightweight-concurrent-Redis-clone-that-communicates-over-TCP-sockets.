# PyRedis

PyRedis is a lightweight Redis-inspired in-memory key-value datastore built from scratch with Python and `asyncio`. It accepts TCP socket connections, parses RESP-style commands, stores data in memory, expires keys through a min-heap, and evicts old keys with an LRU policy backed by a hash map and doubly linked list.

## Features
- Concurrent TCP server built with `asyncio`
- RESP-style protocol support for socket clients
- Command-pattern dispatch with decorator-based command registration
- LRU eviction using a hash map + doubly linked list
- TTL expiration using a min-heap
- Sorted sets implemented with a skip list
- Stronger protocol and command error handling
- Snapshot persistence support
- Append-only logging with replay on startup
- Transaction support with `MULTI` / `EXEC` / `DISCARD`
- Authentication and TOML config support
- Prometheus-style metrics endpoint and structured request logging
- Richer server metrics in `INFO`
- Socket-level integration tests using `pytest-asyncio`
- Docker, `docker-compose`, and GitHub Actions CI support

## Project Structure
```text
PyRedis/
|-- implementation_plan.md
|-- requirements.txt
|-- pyredis.toml
|-- pyproject.toml
|-- Dockerfile
|-- docker-compose.yml
|-- .github/workflows/ci.yml
|-- scripts/
|   |-- async_client_demo.py
|   |-- build_image.ps1
|   `-- stress_client.py
|-- src/pyredis/
|   |-- __main__.py
|   |-- commands.py
|   |-- config.py
|   |-- datastore.py
|   |-- errors.py
|   |-- lru.py
|   |-- persistence.py
|   |-- protocol.py
|   |-- server.py
|   |-- skiplist.py
|   |-- ttl.py
|   `-- zset.py
`-- tests/
    |-- test_benchmark.py
    |-- test_config.py
    |-- test_datastore.py
    |-- test_integration.py
    |-- test_persistence.py
    `-- test_protocol.py
```

## Implemented Commands
- `PING [message]`
- `ECHO message`
- `SET key value [EX seconds]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `KEYS pattern`
- `SCAN cursor [MATCH pattern] [COUNT n]`
- `RENAME source target`
- `RENAMENX source target`
- `UNLINK key [key ...]`
- `EXPIRE key seconds`
- `PEXPIRE key milliseconds`
- `TTL key`
- `PTTL key`
- `INCR key`
- `INCRBY key amount`
- `DECR key`
- `DECRBY key amount`
- `GETSET key value`
- `MSET key value [key value ...]`
- `MGET key [key ...]`
- `SETNX key value`
- `APPEND key value`
- `STRLEN key`
- `FLUSHALL`
- `INFO`
- `TYPE key`
- `DBSIZE`
- `PERSIST key`
- `AUTH password`
- `SAVE`
- `BGSAVE`
- `MULTI`
- `EXEC`
- `DISCARD`
- `ZADD key score member [score member ...]`
- `ZRANGE key start stop`
- `ZRANGE key start stop WITHSCORES`
- `ZCARD key`
- `ZRANK key member`
- `ZSCORE key member`
- `ZREM key member [member ...]`

## Local Setup
```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Run The Server
```powershell
python -m pyredis --config pyredis.toml --metrics-enabled --metrics-port 9101
```

Or override values directly:

```powershell
python -m pyredis --host 127.0.0.1 --port 6380 --snapshot-path data\dump.json --appendonly-enabled --appendonly-path data\appendonly.aof
```

Useful hardening options:

```powershell
python -m pyredis --config pyredis.toml --metrics-enabled --metrics-port 9101 --log-format json --log-level DEBUG
```

## Run A Demo Client
Open another terminal and run:

```powershell
python scripts\async_client_demo.py --host 127.0.0.1 --port 6380
```

## Run Tests
```powershell
ruff check .
pytest
pytest tests\test_protocol.py
pytest tests\test_integration.py -k snapshot
pytest tests\test_integration.py -k "multi or aof or metrics"
pytest tests\test_integration.py -k "scan or pipeline"
python scripts\stress_client.py --host 127.0.0.1 --port 6380 --requests 400 --concurrency 50
python scripts\stress_client.py --host 127.0.0.1 --port 6380 --requests 400 --concurrency 50 --pipeline-depth 4 --json-out benchmark.json
```

## Test Coverage Highlights
- config loading and missing-file handling
- snapshot export/import and on-disk snapshot validation
- append-only replay and background snapshot behavior
- auth enforcement and post-auth command flow
- transaction queueing and `EXEC` response handling
- `SCAN`, pipelined requests, and advanced keyspace/ZSET command behavior
- protocol parsing edge cases and malformed RESP payloads
- benchmark report generation
- metrics endpoint output validation
- key metrics such as hit/miss, eviction, and expiration counters

## Docker Image Build
Build the Docker image manually:

```powershell
docker build -t pyredis:latest .
```

Or use the helper script:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build_image.ps1
```

## Run With Docker
Run the server container directly:

```powershell
docker run --rm -p 6380:6380 -p 9101:9101 -v ${PWD}\data:/app/data pyredis:latest
```

Run the full compose stack:

```powershell
docker compose up --build
```

This starts:
- `pyredis` server container
- two load-test client containers that send concurrent requests
- a bind-mounted `data/` directory for snapshots
- append-only logs and replay state under `data/`
- a bind-mounted `benchmarks/` directory for load-test JSON reports
- an optional bind-mounted `logs/` directory for runtime logs

## Docker Files
The project includes:
- [Dockerfile](C:\Users\devgu\Downloads\PyRedis\Dockerfile) for building the PyRedis image
- [docker-compose.yml](C:\Users\devgu\Downloads\PyRedis\docker-compose.yml) for starting the server and load-test clients together
- [scripts/build_image.ps1](C:\Users\devgu\Downloads\PyRedis\scripts\build_image.ps1) for a one-command image build on Windows PowerShell

## Docker Notes
- the container starts with `pyredis.toml` by default via `PYREDIS_CONFIG`
- snapshots are written under `/app/data`
- append-only logs can be written under `/app/data/appendonly.aof`
- metrics can be exposed on port `9101`
- compose load tests use pipelined requests by default
- compose load tests write benchmark artifacts under `/app/benchmarks`
- local runtime artifacts such as `data/`, `benchmarks/`, and `benchmark.json` are ignored by git

## Design Notes
- `lru.py`: tracks most/least recently used keys using a doubly linked list and hash map
- `ttl.py`: keeps expirations ordered in a min-heap for efficient cleanup
- `commands.py`: registers commands with decorators and dispatches them using a command pattern
- `server.py`: handles concurrent socket clients with `asyncio.start_server`
- `errors.py`: centralizes protocol and command exceptions
- `persistence.py`: saves and restores snapshots from disk
- `skiplist.py` and `zset.py`: provide sorted set support

## Config And Auth
- Configure the server with [pyredis.toml](C:\Users\devgu\Downloads\PyRedis\pyredis.toml)
- Set `require_password` to a non-empty value to enable authentication
- Clients must run `AUTH <password>` before most commands when auth is enabled

## Persistence And Metrics
- `SAVE` writes a snapshot to the configured `snapshot_path`
- `BGSAVE` schedules a background snapshot
- Snapshots can be auto-loaded on startup
- When AOF is enabled, mutating commands are appended and replayed on startup
- AOF rewrite now preserves writes that happen during compaction
- `INFO` now exposes request, hit/miss, expiration, eviction, per-command, and snapshot metrics
- When metrics are enabled, `GET /metrics` exposes Prometheus-style counters, gauges, and latency histogram buckets

## CI
- GitHub Actions runs linting, the full pytest suite, and a benchmark smoke test from [.github/workflows/ci.yml](C:\Users\devgu\Downloads\PyRedis\.github\workflows\ci.yml)

## Transactions
- `MULTI` starts a queued transaction for the current connection
- queued commands return `QUEUED`
- `EXEC` runs the queued commands and returns an array of raw RESP replies
- `DISCARD` clears the queued transaction without applying it

## Hardening
- config validation rejects invalid ports, intervals, and logging formats
- graceful shutdown is handled through signal-aware server startup
- logging supports `plain` and `json` output formats
