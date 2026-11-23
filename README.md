# kafka-sync-pipeline

Production-style **Apache Kafka (KRaft)** event-streaming and Change Data Capture (CDC) reconciliation pipeline built to mirror real-world enterprise systems integration (modeled on cross-system record synchronization, schema enforcement, idempotent consumption, and Dead Letter Queue replay).

## Why This Project Exists
Many backend, platform, data, and systems-integration engineering roles require hands-on Apache Kafka architecture beyond simple tutorials. This repository demonstrates production Kafka patterns in a self-contained local stack:
- **Zero-ZooKeeper KRaft broker** + **Schema Registry** + **PostgreSQL** via Docker Compose
- **Partitioned topics** (`source.records.raw`, `records.reconciled`, `records.retry.1m`, `records.dlq`) guaranteeing per-entity ordering
- **Idempotent consumer group** with manual offset commits (`enable.auto.commit=false`) and PostgreSQL deduplication ledger
- **Schema validation** (JSON Schema / Avro) rejecting malformed records cleanly to the DLQ without poisoning the consumer loop
- **Automated retry backoff + DLQ replay CLI** with full audit trail
- **Observability & Chaos tests**: consumer lag metrics, duplicate-delivery replay test, broker-restart recovery test, and consumer group rebalance verification

## Planned Stack
- **Broker & Streaming:** Apache Kafka (KRaft mode), Schema Registry, Docker Compose
- **Services:** Python (`confluent-kafka`, FastAPI metrics/admin API, Pydantic), PostgreSQL
- **Testing & Ops:** `pytest` integration/chaos suite, Prometheus `/metrics` endpoint, GitHub Actions CI

See [PLAN.md](./PLAN.md) for the milestone roadmap.

## Status
| Milestone | Scope | State |
|---|---|---|
| 1 | Local KRaft Kafka + Schema Registry + PostgreSQL stack, topics, outbox table | Done |
| 2 | Idempotent producer, outbox publisher, schema contract | Next |
| 3 | Idempotent reconciliation consumer, retry and DLQ routing | Planned |
| 4 | DLQ replay CLI, metrics, chaos tests | Planned |

## Quick Start
Requirements: Docker or Podman with the Compose plugin, GNU Make, and [uv](https://docs.astral.sh/uv/) (tests only).

```bash
make up      # start Kafka, Schema Registry, Postgres; apply migrations; create topics
make seed    # insert 100 sample change events into the outbox table
make test    # verify the stack against the contract in tests/test_stack.py
make down    # stop (data is kept; `make clean` also deletes the volumes)
```

`make` on its own lists every target. The runtime is auto-detected (`docker compose`, then `podman compose`); force one with `COMPOSE="podman compose" make up`.

| Service | From the host | From another container |
|---|---|---|
| Kafka | `localhost:9092` | `kafka:19092` |
| Schema Registry | `http://localhost:8081` | `http://schema-registry:8081` |
| PostgreSQL | `localhost:5432` (user, password and database: `sync`) | `postgres:5432` |

All ports bind to loopback. Copy `.env.example` to `.env` to move a port that is already taken.

## Topics
Created by [`scripts/init_topics.sh`](./scripts/init_topics.sh), which is safe to re-run and re-applies topic configs each time.

| Topic | Partitions | Cleanup | Retention | Purpose |
|---|---|---|---|---|
| `source.records.raw` | 6 | delete | 7 days | Change events from the source system, keyed by `entity_id` |
| `records.reconciled` | 6 | compact + delete | 30 days | Latest reconciled state per entity |
| `records.retry.1m` | 3 | delete | 1 day | Transient failures waiting for a delayed retry |
| `records.dlq` | 3 | delete | 30 days | Permanent failures, kept for inspection and replay |

## Design Notes
- **Keying by `entity_id`.** Kafka orders messages within a partition only. Hashing the entity id to a partition keeps every event for one entity in order while different entities are processed in parallel.
- **Partition counts are fixed at creation.** Adding partitions to a keyed topic changes which partition a key hashes to, so new events for an entity could be consumed before older ones. The init script refuses to continue on a mismatch instead of repartitioning.
- **Auto topic creation is off.** A misspelled topic name fails at produce time rather than silently creating a one-partition topic with default settings. A test covers this.
- **Two Kafka listeners.** A client reconnects to the address the broker advertises, so containers (`kafka:19092`) and host tools (`localhost:9092`) each need their own listener.
- **`records.reconciled` is compacted.** It is a changelog of current state: a new consumer can rebuild every entity's latest value without replaying the full history.
- **Outbox table.** `source_events` is written in the same database transaction as the business change, which removes the dual-write problem of updating a database and publishing to Kafka separately. See [`db/migrations/001_source_events.sql`](./db/migrations/001_source_events.sql).
- **Single broker, replication factor 1.** This is a laptop lab, so there is no replication and `acks=all` waits on one replica. A production cluster would run replication factor 3 with `min.insync.replicas=2`, and the client settings used here would not change.

## Repository Layout
```
compose.yaml          Kafka (KRaft), Schema Registry, PostgreSQL 16 with healthchecks
Makefile              up, down, clean, topics, migrate, seed, test, lag, psql, logs
scripts/              init_topics.sh, migrate.sh, seed.sh, wait_for_stack.sh
db/migrations/        ordered SQL migrations, tracked in schema_migrations
db/seed/              sample outbox data
tests/                pytest suite that runs against the live stack
```
