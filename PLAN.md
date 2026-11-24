# Milestone Plan: kafka-sync-pipeline

Goal: Build a clean, tested Apache Kafka event-streaming and reconciliation lab that moves batch reconciliation to real-time event streaming.

## Milestone 1: Local KRaft Kafka + Postgres + Schema Registry Stack (done)
- `compose.yaml` running single-node Kafka (KRaft, no ZooKeeper), Schema Registry, and PostgreSQL 16 with healthchecks.
- Topic bootstrap script (`scripts/init_topics.sh`) creating:
  - `source.records.raw` (6 partitions, keyed by `entity_id`)
  - `records.reconciled` (6 partitions, compacted + retention)
  - `records.retry.1m` (3 partitions)
  - `records.dlq` (3 partitions)
- `Makefile` targets: `up`, `down`, `topics`, `seed`, `test`, `lag`.
- `source_events` outbox table via `scripts/migrate.sh`, sample data via `make seed`, and a pytest suite pinning the topic and schema contract.

## Milestone 2: Producer + Schema Validation + Outbox/CDC Publisher (done)
- Python producer (`src/producer/`) using `confluent-kafka` with `enable.idempotence=true`, `acks=all`, and deterministic partitioning by `entity_id`.
- Transactional outbox poller reading changes from a Postgres `source_events` table and publishing to `source.records.raw` (at-least-once delivery guarantee).
- JSON Schema / Avro contract enforced at publish and consume boundaries.

## Milestone 3: Idempotent Reconciliation Consumer + Retry/DLQ Routing (done)
- Consumer group (`src/consumer/`) with `enable.auto.commit=false`:
  - Matches incoming records against destination state (simulating two systems without a shared primary key, matching by composite natural key + timestamp).
  - Writes reconciled state + processed `event_id` in a single Postgres transaction before committing Kafka offsets.
  - Routes transient failures to `records.retry.1m` (bounded retry header counter) and permanent/malformed payloads to `records.dlq` with error reason, original partition, and offset headers.

## Milestone 4: DLQ Replay CLI, Observability & Chaos Test Suite (done)
- CLI (`src/cli/dlq_replay.py`) to inspect DLQ payloads, filter by error type, and replay fixed messages back to `source.records.raw`.
- FastAPI admin/health service exposing `/health`, `/metrics` (Prometheus consumer lag, processed/retried/DLQ counters), and `/reconciliations`.
- Automated `pytest` integration tests verifying:
  1. Duplicate event delivery produces exactly-once side effects.
  2. Poison-pill payload routes to DLQ without stalling subsequent messages on the same partition.
  3. Consumer restart resumes from last committed offset without data loss.
  4. Multi-instance consumer group rebalance preserves per-key ordering.

## Notes On What Was Built
All four milestones are complete. Where the build differs from the plan above:
- Code lives in one package, `src/kafka_sync/` (`producer/`, `consumer/`, `cli/`, `api/`), instead of top-level `src/producer/` and `src/consumer/`.
- The contract is JSON Schema in the Confluent wire format. Avro was not added.
- The outbox table is polled. There is no log-based CDC connector.
- A fifth chaos test was added beyond the four listed: a broker outage (`make chaos`).
- Metrics are derived from committed offsets and the ledger table at scrape time, not from in-process counters, so they cover every consumer instance.
- Developed and verified with Podman Compose. The Compose file and scripts also target Docker Compose, which has not been run here.

## Scope
- Personal engineering lab demonstrating hands-on Apache Kafka architecture: KRaft broker ops, partition key design, idempotent producers (`acks=all`), manual offset commits, outbox publishing, retry/DLQ topology, consumer lag monitoring, and rebalance testing.
