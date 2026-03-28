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
- Authentication and TOML config support
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
    `-- test_protocol.py
```

## Implemented Commands
- `PING [message]`
- `ECHO message`
- `SET key value [EX seconds]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `EXPIRE key seconds`
- `TTL key`
- `INCR key`
- `INCRBY key amount`
- `DECR key`
- `DECRBY key amount`
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
- `ZADD key score member [score member ...]`
- `ZRANGE key start stop`
- `ZCARD key`
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
python -m pyredis --config pyredis.toml
```

Or override values directly:

```powershell
python -m pyredis --host 127.0.0.1 --port 6380 --snapshot-path data\dump.json
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
python scripts\stress_client.py --host 127.0.0.1 --port 6380 --requests 400 --concurrency 50
python scripts\stress_client.py --host 127.0.0.1 --port 6380 --requests 400 --concurrency 50 --json-out benchmark.json
```

## Test Coverage Highlights
- config loading and missing-file handling
- snapshot export/import and on-disk snapshot validation
- auth enforcement and post-auth command flow
- protocol parsing edge cases and malformed RESP payloads
- benchmark report generation
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
docker run --rm -p 6380:6380 -v ${PWD}\data:/app/data pyredis:latest
```

Run the full compose stack:

```powershell
docker compose up --build
```

This starts:
- `pyredis` server container
- two load-test client containers that send concurrent requests
- a bind-mounted `data/` directory for snapshots
- a bind-mounted `benchmarks/` directory for load-test JSON reports

## Docker Files
The project includes:
- [Dockerfile](C:\Users\devgu\Downloads\PyRedis\Dockerfile) for building the PyRedis image
- [docker-compose.yml](C:\Users\devgu\Downloads\PyRedis\docker-compose.yml) for starting the server and load-test clients together
- [scripts/build_image.ps1](C:\Users\devgu\Downloads\PyRedis\scripts\build_image.ps1) for a one-command image build on Windows PowerShell

## Docker Notes
- the container starts with `pyredis.toml` by default via `PYREDIS_CONFIG`
- snapshots are written under `/app/data`
- compose load tests write benchmark artifacts under `/app/benchmarks`

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
- Snapshots can be auto-loaded on startup
- `INFO` now exposes request, hit/miss, expiration, eviction, and snapshot metrics
