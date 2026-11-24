"""Reconciliation consumer: applying, matching, staleness, retry and DLQ routing."""

from __future__ import annotations

import threading
import time
from collections import defaultdict

import psycopg
import pytest
from confluent_kafka.admin import AdminClient
from helpers import ConsumerRunner, make_event, send_events, total_lag, wait_until

from kafka_sync.config import Settings
from kafka_sync.consumer import (
    Handler,
    Origin,
    PostgresReconciler,
    ReconcileResult,
    TransientError,
    routing,
)
from kafka_sync.contract import EventCodec
from kafka_sync.models import RecordEvent
from kafka_sync.snapshot import read_topic

pytestmark = pytest.mark.integration


def ledger_count(db: psycopg.Connection) -> int:
    return db.execute("SELECT count(*) FROM processed_events").fetchone()[0]


class FailingFor:
    """Handler that raises a transient error the first `times` calls for one entity."""

    def __init__(self, dsn: str, entity: str, times: int) -> None:
        self._inner: Handler = PostgresReconciler(dsn)
        self._entity = entity
        self._remaining = times
        self._lock = threading.Lock()
        self.call_times: dict[str, list[float]] = defaultdict(list)

    def __call__(self, event: RecordEvent, origin: Origin) -> ReconcileResult:
        with self._lock:
            self.call_times[event.entity_id].append(time.time())
            if event.entity_id == self._entity and self._remaining > 0:
                self._remaining -= 1
                raise TransientError("destination temporarily unavailable")
        return self._inner(event, origin)


def test_applies_events_in_order_and_publishes_reconciled_state(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    events = [
        make_event("person-a", version=1, status="pending"),
        make_event("person-a", version=2, status="active"),
        make_event("person-a", version=3, status="inactive"),
    ]
    send_events(settings, codec, events)

    with ConsumerRunner(settings):
        wait_until(lambda: ledger_count(db) == 3, what="3 events in the ledger")
        wait_until(lambda: total_lag(admin, settings) == 0, what="offsets committed")

    status, applied, last_event = db.execute(
        "SELECT status, applied_count, last_event_id::text FROM reconciled_records"
    ).fetchone()
    assert (status, applied, last_event) == ("inactive", 3, events[-1]["event_id"])

    changelog = read_topic(settings, settings.topics.reconciled)
    assert [r.key for r in changelog] == [b"person-a"] * 3
    assert b'"status": "inactive"' in changelog[-1].value


def test_matches_destination_record_by_natural_key_not_by_source_id(
    settings: Settings, db: psycopg.Connection, codec: EventCodec
) -> None:
    """Two source ids describing the same person update one destination row."""
    same_person = {"document_number": "DOC-777", "birth_date": "1985-05-05"}
    send_events(
        settings,
        codec,
        [
            make_event("erp-id-1", version=1, full_name="Ana Old", **same_person),
            make_event("crm-id-9", version=2, full_name="Ana New", **same_person),
        ],
    )

    with ConsumerRunner(settings):
        wait_until(lambda: ledger_count(db) == 2, what="2 events in the ledger")

    rows = db.execute(
        "SELECT document_number, full_name, source_entity_id, applied_count FROM reconciled_records"
    ).fetchall()
    assert rows == [("DOC-777", "Ana New", "crm-id-9", 2)]


def test_older_event_arriving_late_does_not_overwrite_newer_state(
    settings: Settings, db: psycopg.Connection, codec: EventCodec
) -> None:
    newer = make_event("person-a", version=2, status="active")
    older = make_event("person-a", version=1, status="pending")
    send_events(settings, codec, [newer, older])

    with ConsumerRunner(settings):
        wait_until(lambda: ledger_count(db) == 2, what="2 events in the ledger")

    assert db.execute("SELECT status, applied_count FROM reconciled_records").fetchone() == (
        "active",
        1,
    )
    outcomes = dict(db.execute("SELECT event_id::text, outcome FROM processed_events").fetchall())
    assert outcomes == {newer["event_id"]: "applied", older["event_id"]: "stale"}


def test_transient_failure_is_retried_after_the_delay_without_blocking_other_events(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    flaky = make_event("person-flaky")
    others = [make_event(f"person-{n}") for n in range(5)]
    send_events(settings, codec, [flaky, *others])
    handler = FailingFor(settings.postgres_dsn, "person-flaky", times=2)

    with ConsumerRunner(settings, handler=handler):
        wait_until(lambda: ledger_count(db) == 6, what="all 6 events applied")
        wait_until(lambda: total_lag(admin, settings) == 0, what="offsets committed")

    # Two failures -> two trips through the retry topic, then success.
    retries = read_topic(settings, settings.topics.retry)
    assert sorted(r.headers[routing.H_ATTEMPT] for r in retries) == ["1", "2"]
    assert {r.key for r in retries} == {b"person-flaky"}
    assert read_topic(settings, settings.topics.dlq) == []

    # Each retry waited at least the configured delay.
    first, second, third = handler.call_times["person-flaky"]
    delay = settings.retry_delay_seconds - 0.1
    assert second - first >= delay and third - second >= delay

    # The healthy events were not held up behind the failing one.
    processed_at = dict(
        db.execute("SELECT event_id::text, processed_at FROM processed_events").fetchall()
    )
    assert all(processed_at[e["event_id"]] < processed_at[flaky["event_id"]] for e in others)


def test_message_goes_to_dlq_with_its_origin_once_retries_are_exhausted(
    settings: Settings, db: psycopg.Connection, codec: EventCodec, admin: AdminClient
) -> None:
    doomed = make_event("person-doomed")
    healthy = make_event("person-ok")
    send_events(settings, codec, [doomed, healthy])
    handler = FailingFor(settings.postgres_dsn, "person-doomed", times=99)

    with ConsumerRunner(settings, handler=handler):
        dlq = wait_until(
            lambda: read_topic(settings, settings.topics.dlq), what="message in the DLQ"
        )
        wait_until(lambda: total_lag(admin, settings) == 0, what="offsets committed")

    # Tried once plus max_retries more, then given up on.
    assert len(handler.call_times["person-doomed"]) == settings.max_retries + 1

    (dead,) = dlq
    (original,) = [
        r for r in read_topic(settings, settings.topics.raw) if r.key == b"person-doomed"
    ]
    headers = dead.headers
    assert dead.key == b"person-doomed"
    assert dead.value == original.value, "payload must reach the DLQ byte for byte"
    assert headers[routing.H_ERROR_TYPE] == routing.RETRIES_EXHAUSTED
    assert headers[routing.H_ERROR_REASON] == "destination temporarily unavailable"
    assert headers[routing.H_ATTEMPT] == str(settings.max_retries)
    assert headers[routing.H_ORIGINAL_TOPIC] == settings.topics.raw
    assert headers[routing.H_ORIGINAL_PARTITION] == str(original.partition)
    assert headers[routing.H_ORIGINAL_OFFSET] == str(original.offset)

    # Only the healthy event changed destination state.
    assert db.execute("SELECT entity_id FROM processed_events").fetchall() == [("person-ok",)]
