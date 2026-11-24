"""Runtime settings, read once from the environment.

Everything that names an external resource (topics, consumer group, database)
lives here so tests can run the real code against throwaway resources by
building a different `Settings`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Topics:
    raw: str = "source.records.raw"
    reconciled: str = "records.reconciled"
    retry: str = "records.retry.1m"
    dlq: str = "records.dlq"


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "127.0.0.1:9092"
    schema_registry_url: str = "http://localhost:8081"
    postgres_dsn: str = "host=localhost port=5432 user=sync password=sync dbname=sync"
    topics: Topics = field(default_factory=Topics)

    consumer_group: str = "reconciler"
    # Schema Registry subject holding the event contract (TopicNameStrategy for the raw topic).
    schema_subject: str = "source.records.raw-value"

    # A message is tried once and then retried at most this many times before the DLQ.
    max_retries: int = 3
    # How long a message waits in the retry topic before it is attempted again.
    retry_delay_seconds: float = 60.0

    # How long the producer keeps trying to deliver a message before reporting failure.
    delivery_timeout_ms: int = 30_000

    outbox_batch_size: int = 500
    outbox_poll_seconds: float = 1.0
    consumer_batch_size: int = 100
    session_timeout_ms: int = 45_000

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        default = cls()
        return cls(
            bootstrap_servers=env.get(
                "KAFKA_BOOTSTRAP_SERVERS", f"127.0.0.1:{env.get('KAFKA_HOST_PORT', '9092')}"
            ),
            schema_registry_url=env.get(
                "SCHEMA_REGISTRY_URL",
                f"http://localhost:{env.get('SCHEMA_REGISTRY_HOST_PORT', '8081')}",
            ),
            postgres_dsn=env.get(
                "POSTGRES_DSN",
                f"host=localhost port={env.get('POSTGRES_HOST_PORT', '5432')} "
                f"user={env.get('POSTGRES_USER', 'sync')} "
                f"password={env.get('POSTGRES_PASSWORD', 'sync')} "
                f"dbname={env.get('POSTGRES_DB', 'sync')}",
            ),
            consumer_group=env.get("CONSUMER_GROUP", default.consumer_group),
            max_retries=int(env.get("MAX_RETRIES", default.max_retries)),
            retry_delay_seconds=float(env.get("RETRY_DELAY_SECONDS", default.retry_delay_seconds)),
        )
