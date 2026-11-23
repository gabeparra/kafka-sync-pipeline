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
