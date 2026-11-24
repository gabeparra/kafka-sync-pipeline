"""Outbox publisher against a real broker and database."""

from __future__ import annotations

import psycopg
import pytest
from helpers import insert_outbox, make_event

from kafka_sync.config import Settings
from kafka_sync.contract import EventCodec
from kafka_sync.producer import OutboxPublisher
from kafka_sync.producer.outbox import OUTBOX_LOCK_KEY
from kafka_sync.snapshot import read_topic

pytestmark = pytest.mark.integration


@pytest.fixture
def publisher(settings: Settings) -> OutboxPublisher:
    conn = psycopg.connect(settings.postgres_dsn)
    return OutboxPublisher(settings, conn)


def test_publishes_rows_keyed_by_entity_and_marks_them_published(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, publisher: OutboxPublisher
) -> None:
    events = [make_event(e, version=v) for v in (1, 2, 3) for e in ("person-a", "person-b")]
    insert_outbox(db, *events)

    result = publisher.publish_batch()

    assert (result.published, result.rejected, result.unconfirmed) == (6, 0, 0)
    records = read_topic(settings, settings.topics.raw)
    assert {r.key for r in records} == {b"person-a", b"person-b"}
    assert sorted(codec.decode(r.value)["event_id"] for r in records) == sorted(
        e["event_id"] for e in events
    )
    assert all(r.headers["event_id"] == codec.decode(r.value)["event_id"] for r in records)
    assert db.execute(
        "SELECT count(*) FROM source_events WHERE published_at IS NULL"
    ).fetchone() == (0,)
    assert publisher.publish_batch().idle


def test_events_of_one_entity_share_a_partition_and_keep_their_order(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, publisher: OutboxPublisher
) -> None:
    entities = [f"person-{n}" for n in range(8)]
    insert_outbox(db, *(make_event(e, version=v) for v in range(1, 6) for e in entities))

    publisher.publish_batch()

    records = read_topic(settings, settings.topics.raw)
    assert len({r.partition for r in records}) > 1, "keys should spread over partitions"
    for entity in entities:
        mine = [r for r in records if r.key == entity.encode()]
        assert len({r.partition for r in mine}) == 1
        # read_topic returns offset order within a partition.
        assert [codec.decode(r.value)["payload"]["version"] for r in mine] == [1, 2, 3, 4, 5]


def test_row_violating_the_contract_is_parked_and_does_not_block_the_queue(
    settings: Settings, db: psycopg.Connection, publisher: OutboxPublisher
) -> None:
    bad = make_event("person-bad", status="not-a-status")
    good = make_event("person-good")
    insert_outbox(db, bad, good)

    result = publisher.publish_batch()

    assert (result.published, result.rejected) == (1, 1)
    assert [r.key for r in read_topic(settings, settings.topics.raw)] == [b"person-good"]
    published_at, error = db.execute(
        "SELECT published_at, publish_error FROM source_events WHERE event_id = %s",
        (bad["event_id"],),
    ).fetchone()
    assert published_at is None
    assert "not-a-status" in error
    assert publisher.publish_batch().idle, "the parked row must not be retried forever"


def test_crash_after_broker_ack_republishes_the_batch(
    settings: Settings, db: psycopg.Connection, codec: EventCodec
) -> None:
    """The at-least-once window: Kafka has the messages, the database mark is lost.

    Rolling back the publisher's transaction after it returned is exactly what a
    crash between the broker acknowledgement and the commit leaves behind.
    """
    events = [make_event("person-a", version=v) for v in (1, 2)]
    insert_outbox(db, *events)

    with psycopg.connect(settings.postgres_dsn) as conn:
        publisher = OutboxPublisher(settings, conn)
        with conn.transaction(force_rollback=True):
            assert publisher.publish_batch().published == 2
        assert publisher.publish_batch().published == 2

    delivered = [
        codec.decode(r.value)["event_id"] for r in read_topic(settings, settings.topics.raw)
    ]
    assert sorted(delivered) == sorted([e["event_id"] for e in events] * 2)


def test_second_publisher_stands_down_while_another_holds_the_lock(
    settings: Settings, db: psycopg.Connection, publisher: OutboxPublisher
) -> None:
    insert_outbox(db, make_event())

    with psycopg.connect(settings.postgres_dsn) as leader, leader.transaction():
        leader.execute("SELECT pg_advisory_xact_lock(%s)", (OUTBOX_LOCK_KEY,))
        result = publisher.publish_batch()

    assert not result.is_leader
    assert read_topic(settings, settings.topics.raw) == []
    assert publisher.publish_batch().published == 1, "takes over once the lock is free"
