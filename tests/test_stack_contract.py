"""The local stack is up and shaped the way the pipeline assumes.

These are the invariants later milestones build on (partition counts drive key
ordering, cleanup policies drive retention, the outbox table drives publishing),
so they are pinned here rather than left as script side effects. Unlike the
rest of the suite, these tests look at the real topics and database that
`make up` created.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Iterator

import psycopg
import pytest
from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import AdminClient, ConfigResource

from kafka_sync.config import Settings

pytestmark = pytest.mark.integration


@pytest.fixture
def dev_db(base_settings: Settings) -> Iterator[psycopg.Connection]:
    """The developer database. Work is rolled back so tests leave no rows behind."""
    conn = psycopg.connect(base_settings.postgres_dsn, connect_timeout=5)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


DAY_MS = 24 * 60 * 60 * 1000

# topic -> (partitions, expected topic-level configs)
EXPECTED_TOPICS: dict[str, tuple[int, dict[str, str]]] = {
    "source.records.raw": (6, {"cleanup.policy": "delete", "retention.ms": str(7 * DAY_MS)}),
    "records.reconciled": (
        6,
        {
            "cleanup.policy": "compact,delete",
            "retention.ms": str(30 * DAY_MS),
            "min.cleanable.dirty.ratio": "0.1",
        },
    ),
    "records.retry.1m": (3, {"cleanup.policy": "delete", "retention.ms": str(DAY_MS)}),
    "records.dlq": (3, {"cleanup.policy": "delete", "retention.ms": str(30 * DAY_MS)}),
}


def test_broker_runs_in_kraft_mode_as_single_node(admin: AdminClient) -> None:
    metadata = admin.list_topics(timeout=10)

    assert len(metadata.brokers) == 1
    # A KRaft cluster reports a controller without any ZooKeeper involvement.
    assert metadata.controller_id in metadata.brokers


@pytest.mark.parametrize("topic", EXPECTED_TOPICS)
def test_topic_has_expected_partitions(admin: AdminClient, topic: str) -> None:
    expected_partitions, _ = EXPECTED_TOPICS[topic]
    metadata = admin.list_topics(timeout=10)

    assert topic in metadata.topics, f"{topic} missing; run `make topics`"
    assert len(metadata.topics[topic].partitions) == expected_partitions


@pytest.mark.parametrize("topic", EXPECTED_TOPICS)
def test_topic_has_expected_configs(admin: AdminClient, topic: str) -> None:
    _, expected_configs = EXPECTED_TOPICS[topic]
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)

    configs = admin.describe_configs([resource])[resource].result(timeout=10)

    actual = {name: configs[name].value for name in expected_configs}
    assert actual == expected_configs


def test_topic_auto_creation_is_disabled(admin: AdminClient, base_settings: Settings) -> None:
    """Producing to an unknown topic must fail instead of creating it.

    A producer's metadata request is what triggers auto-creation on a default
    broker, so this is the path a typo in a topic name would take.
    """
    name = "this.topic.must.not.exist"
    errors: list[KafkaError | None] = []
    producer = Producer(
        {"bootstrap.servers": base_settings.bootstrap_servers, "message.timeout.ms": 3000}
    )

    producer.produce(name, value=b"x", on_delivery=lambda err, _msg: errors.append(err))
    producer.flush(10)

    assert len(errors) == 1 and errors[0] is not None
    assert name not in admin.list_topics(timeout=10).topics


def test_schema_registry_is_reachable_and_backed_by_kafka(
    admin: AdminClient, base_settings: Settings
) -> None:
    url = f"{base_settings.schema_registry_url}/subjects"
    with urllib.request.urlopen(url, timeout=5) as response:
        subjects = json.load(response)

    assert isinstance(subjects, list)
    # The registry persists schemas in a Kafka topic, proving the two are wired together.
    assert "_schemas" in admin.list_topics(timeout=10).topics


def test_outbox_table_accepts_events_and_defaults_to_unpublished(
    dev_db: psycopg.Connection,
) -> None:
    row = dev_db.execute(
        """
        INSERT INTO source_events (entity_id, event_type, payload)
        VALUES ('person-test', 'record.created', '{"status": "active"}')
        RETURNING event_id, published_at
        """
    ).fetchone()

    assert row is not None
    event_id, published_at = row
    assert event_id is not None
    assert published_at is None


def test_outbox_rejects_duplicate_event_id(dev_db: psycopg.Connection) -> None:
    insert = """
        INSERT INTO source_events (event_id, entity_id, event_type, payload)
        VALUES ('00000000-0000-0000-0000-000000000001', 'person-test', 'record.created', '{}')
    """
    dev_db.execute(insert)

    with pytest.raises(psycopg.errors.UniqueViolation):
        dev_db.execute(insert)
