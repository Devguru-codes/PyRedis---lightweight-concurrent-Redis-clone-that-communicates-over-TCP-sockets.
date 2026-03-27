# PyRedis Implementation Plan

## Goal
Build a lightweight in-memory Redis-inspired datastore with:
- asyncio-based TCP server for concurrent clients
- command-pattern command dispatch
- TTL expiration managed by a min-heap
- LRU eviction powered by a hash map plus doubly linked list
- socket-level integration tests
- Docker and CI support for repeatable validation and stress checks

## Phase 1: Project Bootstrap
- Create repository scaffolding and Python virtual environment
- Add `.gitignore`, `requirements.txt`, and packaging metadata
- Define source, test, and tooling layout
- Document startup and development workflow

## Phase 2: Core Storage Engine
- Implement in-memory storage records for strings and metadata
- Build TTL expiration management with a min-heap
- Build LRU eviction with a doubly linked list and hash map
- Add thread-safe / task-safe coordination with `asyncio.Lock`
- Define datastore APIs for `GET`, `SET`, `DEL`, `EXPIRE`, `TTL`, `EXISTS`, `FLUSHALL`

## Phase 3: Command System and Protocol
- Implement a lightweight RESP parser and serializer over TCP streams
- Add command registration via custom decorators
- Model commands with a command-pattern architecture
- Support a practical command set:
  - `PING`, `ECHO`
  - `SET`, `GET`, `DEL`, `EXISTS`
  - `EXPIRE`, `TTL`
  - `INCR`
  - `FLUSHALL`
  - `INFO`
- Return Redis-style protocol responses and errors

## Phase 4: Server Concurrency
- Create an `asyncio` TCP server using `asyncio.start_server`
- Handle multiple clients concurrently
- Add graceful startup/shutdown hooks
- Run background expiration cleanup without blocking request handling
- Add basic observability counters for uptime and command counts

## Phase 5: Extended Structures
- Add a minimal sorted-set implementation backed by a skip list
- Support `ZADD` and `ZRANGE`
- Reuse datastore TTL and eviction handling where appropriate

## Phase 6: Testing and Verification
- Add unit tests for the LRU cache, TTL heap behavior, and skip list
- Add socket-level integration tests with `pytest-asyncio`
- Add a simple async stress test measuring latency percentiles
- Verify core flows locally

## Phase 7: Containerization and CI
- Add `Dockerfile` and `docker-compose.yml`
- Add a client load script container for concurrent request generation
- Add GitHub Actions workflow for tests and stress checks
- Document how to run locally and in containers

## Progress Checklist
- [x] Phase 1 plan drafted
- [x] Phase 1 bootstrap completed
- [x] Phase 2 core storage engine completed
- [x] Phase 3 command system and protocol completed
- [x] Phase 4 server concurrency completed
- [x] Phase 5 extended structures completed
- [x] Phase 6 testing and verification completed
- [x] Phase 7 containerization and CI completed
