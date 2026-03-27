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
- Basic server metrics in `INFO`
- Socket-level integration tests using `pytest-asyncio`
- Docker, `docker-compose`, and GitHub Actions CI support

## Project Structure
```text
PyRedis/
|-- implementation_plan.md
|-- requirements.txt
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
|   |-- lru.py
|   |-- protocol.py
|   |-- server.py
|   |-- skiplist.py
|   |-- ttl.py
|   `-- zset.py
`-- tests/
    |-- test_datastore.py
    `-- test_integration.py
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
- `MSET key value [key value ...]`
- `MGET key [key ...]`
- `FLUSHALL`
- `INFO`
- `TYPE key`
- `DBSIZE`
- `PERSIST key`
- `ZADD key score member [score member ...]`
- `ZRANGE key start stop`

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
python -m pyredis --host 127.0.0.1 --port 6380
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
python scripts\stress_client.py --host 127.0.0.1 --port 6380 --requests 400 --concurrency 50
```

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
docker run --rm -p 6380:6380 pyredis:latest
```

Run the full compose stack:

```powershell
docker compose up --build
```

This starts:
- `pyredis` server container
- two load-test client containers that send concurrent requests

## Docker Files
The project includes:
- [Dockerfile](C:\Users\devgu\Downloads\PyRedis\Dockerfile) for building the PyRedis image
- [docker-compose.yml](C:\Users\devgu\Downloads\PyRedis\docker-compose.yml) for starting the server and load-test clients together
- [scripts/build_image.ps1](C:\Users\devgu\Downloads\PyRedis\scripts\build_image.ps1) for a one-command image build on Windows PowerShell

## Design Notes
- `lru.py`: tracks most/least recently used keys using a doubly linked list and hash map
- `ttl.py`: keeps expirations ordered in a min-heap for efficient cleanup
- `commands.py`: registers commands with decorators and dispatches them using a command pattern
- `server.py`: handles concurrent socket clients with `asyncio.start_server`
- `errors.py`: centralizes protocol and command exceptions
- `skiplist.py` and `zset.py`: provide sorted set support

## Next Improvements
- Add persistence with snapshots or append-only logging
- Add more Redis-compatible commands
- Add authentication and configuration file support
- Add benchmarking and observability metrics
