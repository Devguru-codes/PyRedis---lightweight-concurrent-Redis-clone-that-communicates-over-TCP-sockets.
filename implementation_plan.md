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

## Phase 8: Durable Persistence
- Add append-only logging (AOF) for mutating commands
- Replay AOF on startup for crash recovery
- Support background snapshots with atomic file replacement
- Add snapshot compaction by rewriting the current state to a clean snapshot
- Expose `BGSAVE` and config toggles for snapshot/AOF behavior

## Phase 9: Expanded Redis Compatibility
- Add more keyspace and string commands:
  - `GETSET`, `KEYS`, `RENAME`
  - `PTTL`, `PEXPIRE`
- Improve sorted-set and generic command support where helpful
- Keep responses Redis-style and compatible with existing RESP tests

## Phase 10: Transactions And Request Flow
- Add per-connection transaction state
- Support `MULTI`, `EXEC`, and `DISCARD`
- Ensure pipelined requests continue to work cleanly with transactions
- Add regression coverage for queued command semantics

## Phase 11: Observability And Runtime UX
- Add structured request logging
- Track per-command latency metrics and command histograms/counters
- Expose a Prometheus-style `/metrics` HTTP endpoint
- Document operational visibility and benchmark output

## Phase 12: CI Restoration And Extended Validation
- Restore GitHub Actions workflow for lint, tests, and benchmark checks
- Add tests for AOF replay, background snapshots, metrics endpoint, and transactions
- Keep new code paths covered by unit and socket-level integration tests

## Milestone 2 Checklist
- [x] Phase 8 durable persistence completed
- [x] Phase 9 expanded Redis compatibility completed
- [x] Phase 10 transactions and request flow completed
- [x] Phase 11 observability and runtime UX completed
- [x] Phase 12 CI restoration and extended validation completed
