"""Fixtures for running the real pipeline against throwaway resources.

Every integration test gets its own topics and consumer group, and the session
gets its own database, so tests never touch (or depend on) the data in the
developer's local stack.
"""

from __future__ import annotations

import contextlib
import dataclasses
import time
import uuid
from collections.abc import Iterator

import psycopg
import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql
from psycopg.conninfo import make_conninfo

from kafka_sync.config import Settings, Topics
from kafka_sync.contract import EventCodec
from kafka_sync.lag import topic_end_offsets
from kafka_sync.migrations import apply_migrations

TABLES = ("source_events", "processed_events", "reconciled_records", "dlq_replays")


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    """Settings for the developer's local stack, as `make` sees them."""
    return Settings.from_env()


@pytest.fixture(scope="session")
def admin(base_settings: Settings) -> AdminClient:
    client = AdminClient({"bootstrap.servers": base_settings.bootstrap_servers})
    try:
        client.list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001 - any failure here means "stack is down"
        pytest.fail(
            f"Kafka is not reachable at {base_settings.bootstrap_servers}; "
            f"run `make up` first ({exc})"
        )
    return client


@pytest.fixture(scope="session")
def test_dsn(base_settings: Settings) -> Iterator[str]:
    """A freshly migrated database that exists only for this test session."""
    name = f"sync_test_{uuid.uuid4().hex[:8]}"
    try:
        control = psycopg.connect(base_settings.postgres_dsn, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.fail(f"Postgres is not reachable; run `make up` first ({exc})")
    with control:
        control.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        dsn = make_conninfo(base_settings.postgres_dsn, dbname=name)
        try:
            with psycopg.connect(dsn) as conn:
                apply_migrations(conn)
            yield dsn
        finally:
            control.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


def _wait_for_partition_leaders(admin: AdminClient, topics: list[str]) -> None:
    """Topic creation returns before every partition has a leader; wait for that."""
    deadline = time.monotonic() + 30
    while True:
        try:
            topic_end_offsets(admin, topics)
            return
        except KafkaException:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


@pytest.fixture
def settings(base_settings: Settings, admin: AdminClient, test_dsn: str) -> Iterator[Settings]:
    """Isolated settings: unique topics and group, empty tables, short timings."""
    prefix = f"test.{uuid.uuid4().hex[:8]}"
    topics = Topics(
        raw=f"{prefix}.raw",
        reconciled=f"{prefix}.reconciled",
        retry=f"{prefix}.retry",
        dlq=f"{prefix}.dlq",
    )
    # Same partition counts as the real topics (see scripts/init_topics.sh).
    partitions = {topics.raw: 6, topics.reconciled: 6, topics.retry: 3, topics.dlq: 3}
    created = admin.create_topics(
        [
            NewTopic(name, num_partitions=count, replication_factor=1)
            for name, count in partitions.items()
        ]
    )
    for future in created.values():
        future.result(timeout=30)
    _wait_for_partition_leaders(admin, list(partitions))

    with psycopg.connect(test_dsn, autocommit=True) as conn:
        conn.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY")

    yield dataclasses.replace(
        base_settings,
        postgres_dsn=test_dsn,
        topics=topics,
        consumer_group=f"{prefix}.group",
        max_retries=2,
        retry_delay_seconds=1.5,
        delivery_timeout_ms=10_000,
        session_timeout_ms=6_000,
    )

    for future in admin.delete_topics(list(partitions)).values():
        future.result(timeout=30)
    # Not every test starts a consumer, so the group may not exist.
    for future in admin.delete_consumer_groups([f"{prefix}.group"]).values():
        with contextlib.suppress(KafkaException):
            future.result(timeout=30)


@pytest.fixture
def db(settings: Settings) -> Iterator[psycopg.Connection]:
    """Autocommit connection to the test database, for arranging and asserting."""
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture
def codec(settings: Settings) -> EventCodec:
    return EventCodec(settings)
