"""Failure-mode tests: the guarantees the pipeline is built to keep.

1. Duplicate delivery produces exactly-once side effects.
2. A poison pill goes to the DLQ without stalling its partition.
3. A restarted consumer resumes from its last committed offset, losing nothing.
4. A consumer group rebalance preserves per-key ordering.
"""

from __future__ import annotations

import json
import threading
import time

import psycopg
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from helpers import ConsumerRunner, Recorder, make_event, send, send_events, total_lag, wait_until

from kafka_sync.clients import producer_config
from kafka_sync.config import Settings
from kafka_sync.consumer import Handler, Origin, ReconcileResult, routing
from kafka_sync.contract import EventCodec
from kafka_sync.models import RecordEvent
from kafka_sync.snapshot import read_topic

pytestmark = pytest.mark.integration


class SimulatedCrash(BaseException):
    """Kills the consumer loop the way SIGKILL would: no cleanup, no offset commit."""


def ledger_ids(db: psycopg.Connection) -> set[str]:
    return {row[0] for row in db.execute("SELECT event_id::text FROM processed_events")}


def applied_counts(db: psycopg.Connection) -> dict[str, int]:
    return dict(
        db.execute("SELECT source_entity_id, applied_count FROM reconciled_records").fetchall()
    )


# 1 ---------------------------------------------------------------------------


def test_duplicate_delivery_has_exactly_once_side_effects(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    v1 = make_event("person-a", version=1)
    v2 = make_event("person-a", version=2, status="inactive")
    # The same event three times, as after a publisher crash or a producer retry,
    # including a copy that arrives after a newer event.
    send_events(settings, codec, [v1, v1, v2, v1])

    with ConsumerRunner(settings) as runner:
        wait_until(lambda: total_lag(admin, settings) == 0, what="all 4 messages consumed")
        stats = dict(runner.worker.stats)

    assert stats == {"applied": 2, "duplicate": 2}
    assert ledger_ids(db) == {v1["event_id"], v2["event_id"]}
    assert db.execute("SELECT status, applied_count FROM reconciled_records").fetchone() == (
        "inactive",
        2,
    )


# 2 ---------------------------------------------------------------------------


def test_poison_pills_go_to_dlq_without_stalling_the_partition(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    key = "person-a"
    before = make_event(key, version=1)
    after = make_event(key, version=2, status="inactive")
    wire_header = codec.encode(before)[:5]
    poison = {
        "not json at all": b"\xff\xfe garbage",
        "json without the wire format": json.dumps(make_event(key)).encode(),
        "violates the schema": wire_header
        + json.dumps(make_event(key, status="not-a-status")).encode(),
        "passes the schema, impossible date": wire_header
        + json.dumps(make_event(key, birth_date="2026-02-30")).encode(),
    }
    # One key, therefore one partition: every pill sits between two good events.
    send(
        settings,
        [
            (key, codec.encode(before)),
            *((key, v) for v in poison.values()),
            (key, codec.encode(after)),
        ],
    )

    with ConsumerRunner(settings):
        wait_until(lambda: total_lag(admin, settings) == 0, what="partition fully consumed")

    # The good events on both sides of the pills were applied.
    assert ledger_ids(db) == {before["event_id"], after["event_id"]}
    assert db.execute("SELECT status FROM reconciled_records").fetchone() == ("inactive",)

    # Each pill is in the DLQ, unmodified, with the reason and where it came from.
    dlq = read_topic(settings, settings.topics.dlq)
    assert [r.value for r in dlq] == list(poison.values())
    assert [r.headers[routing.H_ERROR_TYPE] for r in dlq] == [
        "contract_violation",
        "contract_violation",
        "contract_violation",
        "invalid_event",
    ]
    assert [int(r.headers[routing.H_ORIGINAL_OFFSET]) for r in dlq] == [1, 2, 3, 4]
    assert {r.headers[routing.H_ORIGINAL_TOPIC] for r in dlq} == {settings.topics.raw}
    assert all(r.key == key.encode() and r.headers[routing.H_ERROR_REASON] for r in dlq)
    # Permanent failures are never retried.
    assert read_topic(settings, settings.topics.retry) == []


# 3 ---------------------------------------------------------------------------


def test_restarted_consumer_resumes_from_last_committed_offset(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    entities = [f"person-{n}" for n in range(5)]
    first = [make_event(e, version=v) for v in (1, 2) for e in entities]
    second = [make_event(e, version=3) for e in entities]
    recorder = Recorder()

    send_events(settings, codec, first)
    with ConsumerRunner(settings, name="before-restart", wrap=recorder.wrap):
        wait_until(lambda: total_lag(admin, settings) == 0, what="first batch committed")

    send_events(settings, codec, second)
    with ConsumerRunner(settings, name="after-restart", wrap=recorder.wrap):
        wait_until(lambda: total_lag(admin, settings) == 0, what="second batch committed")

    # Nothing lost, and nothing from before the restart was read again.
    assert ledger_ids(db) == {e["event_id"] for e in first + second}
    assert sorted(recorder.event_ids("after-restart")) == sorted(e["event_id"] for e in second)
    assert applied_counts(db) == dict.fromkeys(entities, 3)


def test_crash_before_offset_commit_redelivers_without_double_applying(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    events = [make_event("person-a", version=v) for v in range(1, 9)]
    crash_on = events[2]["event_id"]
    recorder = Recorder()

    def crash_after_database_commit(_name: str, handler: Handler) -> Handler:
        def handle(event: RecordEvent, origin: Origin) -> ReconcileResult:
            result = handler(event, origin)
            if str(event.event_id) == crash_on:
                # The database has the change; Kafka has not been told.
                raise SimulatedCrash
            return result

        return handle

    send_events(settings, codec, events)
    crashed = ConsumerRunner(settings, name="crashing", wrap=crash_after_database_commit).start()
    crashed.join()
    assert isinstance(crashed.take_error(), SimulatedCrash)
    assert crash_on in ledger_ids(db), "the crash must come after the database commit"

    with ConsumerRunner(settings, name="replacement", wrap=recorder.wrap) as runner:
        wait_until(lambda: total_lag(admin, settings) == 0, what="backlog consumed")
        stats = dict(runner.worker.stats)

    # The uncommitted event came back, was recognised, and was not applied twice.
    assert crash_on in recorder.event_ids("replacement")
    assert stats["duplicate"] >= 1
    assert ledger_ids(db) == {e["event_id"] for e in events}
    assert applied_counts(db) == {"person-a": len(events)}


# 4 ---------------------------------------------------------------------------


def test_rebalance_preserves_per_key_ordering(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    """Members join and leave mid-stream; every entity's events still apply in order."""
    entities = [f"person-{n:02d}" for n in range(12)]
    versions = range(1, 41)
    events = {(e, v): make_event(e, version=v) for v in versions for e in entities}
    encoded = {k: codec.encode(event) for k, event in events.items()}
    recorder = Recorder()

    def produce_steadily() -> None:
        producer = Producer(producer_config(settings))
        for version in versions:
            for entity in entities:
                producer.produce(settings.topics.raw, key=entity, value=encoded[entity, version])
            producer.flush(10)
            time.sleep(0.2)

    stream = threading.Thread(target=produce_steadily, daemon=True)
    with ConsumerRunner(settings, name="member-a", wrap=recorder.wrap) as a:
        assert a.worker.wait_until_assigned(30)
        alone = len(a.worker.assignment())
        stream.start()
        wait_until(lambda: len(recorder.calls) >= 24, what="member-a processing")

        # Rebalance 1: a second member joins and takes over part of the work.
        with ConsumerRunner(settings, name="member-b", wrap=recorder.wrap) as b:
            assert b.worker.wait_until_assigned(30)
            wait_until(
                lambda: len(a.worker.assignment()) < alone, what="member-a giving up partitions"
            )
            wait_until(lambda: recorder.event_ids("member-b"), what="member-b processing")

            # Rebalance 2: the first member leaves; the second inherits everything.
            a.stop()
            stream.join(60)
            wait_until(lambda: total_lag(admin, settings) == 0, what="stream fully consumed")
            assert len(b.worker.assignment()) == alone

    assert recorder.event_ids("member-a") and recorder.event_ids("member-b")

    # Per entity, the order in which versions were first handled is 1..40 with
    # no gaps: nothing skipped and nothing handled ahead of an earlier version.
    first_seen: dict[str, list[int]] = {e: [] for e in entities}
    for _member, event in recorder.calls:
        seen = first_seen[event.entity_id]
        if event.payload.version not in seen:
            seen.append(event.payload.version)
    assert first_seen == {e: list(versions) for e in entities}

    # And the destination agrees: every event applied once, newest one last.
    assert applied_counts(db) == dict.fromkeys(entities, len(versions))
    last = dict(
        db.execute(
            "SELECT source_entity_id, last_event_id::text FROM reconciled_records"
        ).fetchall()
    )
    assert last == {e: events[e, versions[-1]]["event_id"] for e in entities}
