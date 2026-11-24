"""DLQ inspection and replay, end to end: fail, inspect, fix, replay, reconcile."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import psycopg
import pytest
from confluent_kafka.admin import AdminClient
from helpers import ConsumerRunner, make_event, send, total_lag, wait_until

from kafka_sync.cli import dlq_replay
from kafka_sync.cli.dlq_replay import error_type_of, read_dlq, replay, select
from kafka_sync.config import Settings
from kafka_sync.consumer import Origin, ReconcileResult, TransientError, routing
from kafka_sync.contract import EventCodec
from kafka_sync.models import RecordEvent
from kafka_sync.snapshot import read_topic

pytestmark = pytest.mark.integration


def always_unavailable(_event: RecordEvent, _origin: Origin) -> ReconcileResult:
    raise TransientError("destination temporarily unavailable")


@pytest.fixture
def dead(settings: Settings, codec: EventCodec, admin: AdminClient) -> dict[str, dict[str, Any]]:
    """Fill the DLQ through the real consumer with one message of each kind.

    Returns the events by kind: 'outage' was valid but the destination was down,
    'bad_status' violates the schema, and 'garbage' is not JSON.
    """
    outage = make_event("person-outage")
    bad_status = make_event("person-bad", status="not-a-status")
    send(
        settings,
        [
            ("person-outage", codec.encode(outage)),
            ("person-bad", codec.encode(outage)[:5] + json.dumps(bad_status).encode()),
            ("person-garbage", b"\xff\xfe garbage"),
        ],
    )
    no_retries = dataclasses.replace(settings, max_retries=0)
    with ConsumerRunner(no_retries, handler=always_unavailable):
        wait_until(lambda: total_lag(admin, settings) == 0, what="failures routed")
    assert len(read_dlq(settings)) == 3
    return {"outage": outage, "bad_status": bad_status}


def reconcile_everything(settings: Settings, admin: AdminClient) -> dict[str, int]:
    """Run a healthy consumer until the raw topic is drained; return its stats."""
    with ConsumerRunner(settings, name="healthy") as runner:
        wait_until(lambda: total_lag(admin, settings) == 0, what="replays consumed")
        return dict(runner.worker.stats)


def statuses(db: psycopg.Connection) -> dict[str, str]:
    return dict(db.execute("SELECT source_entity_id, status FROM reconciled_records").fetchall())


def test_lists_and_filters_dead_letters_by_error_type(
    settings: Settings, dead: dict, capsys: pytest.CaptureFixture[str]
) -> None:
    records = read_dlq(settings)

    assert sorted(error_type_of(r) for r in records) == [
        "contract_violation",
        "contract_violation",
        "retries_exhausted",
    ]
    assert [r.key for r in select(records, error_type="retries_exhausted")] == [b"person-outage"]

    assert dlq_replay.main(["list", "--error-type", "contract_violation"], settings) == 0
    output = capsys.readouterr().out
    assert "2 message(s)" in output
    assert "key=person-bad" in output and "person-outage" not in output
    assert f"{settings.topics.raw}[" in output, "origin of each message is shown"


def test_replays_a_message_that_failed_for_transient_reasons_exactly_once(
    settings: Settings, dead: dict, db: psycopg.Connection, admin: AdminClient
) -> None:
    chosen = select(read_dlq(settings), error_type="retries_exhausted")

    (result,) = replay(settings, chosen)

    assert result.status == "replayed"
    replayed = read_topic(settings, settings.topics.raw)[-1]
    assert replayed.headers[routing.H_REPLAYED_FROM].startswith(settings.topics.dlq)
    assert routing.H_ERROR_TYPE not in replayed.headers, "failure headers are not carried over"
    assert reconcile_everything(settings, admin) == {"applied": 1}
    assert statuses(db) == {"person-outage": "active"}

    # The replay is on record, and asking again does nothing.
    audit = db.execute(
        "SELECT event_id, error_type, target_topic, patched FROM dlq_replays"
    ).fetchall()
    assert audit == [(dead["outage"]["event_id"], "retries_exhausted", settings.topics.raw, False)]
    (again,) = replay(settings, chosen)
    assert again.status == "skipped"

    # Forcing a second replay is safe too: the consumer recognises the event.
    (forced,) = replay(settings, chosen, force=True)
    assert forced.status == "replayed"
    assert reconcile_everything(settings, admin) == {"duplicate": 1}
    assert db.execute("SELECT applied_count FROM reconciled_records").fetchone() == (1,)


def test_invalid_message_needs_a_patch_and_is_revalidated_before_replay(
    settings: Settings, dead: dict, db: psycopg.Connection, admin: AdminClient
) -> None:
    chosen = [r for r in read_dlq(settings) if r.key == b"person-bad"]
    raw_before = len(read_topic(settings, settings.topics.raw))

    # Unchanged, it would only fail again, so it is refused.
    (unpatched,) = replay(settings, chosen)
    assert unpatched.status == "failed" and "not-a-status" in unpatched.detail
    # A patch that does not fix the problem is refused as well.
    (wrong,) = replay(settings, chosen, patch={"payload": {"status": "still-wrong"}})
    assert wrong.status == "failed"
    assert len(read_topic(settings, settings.topics.raw)) == raw_before
    assert db.execute("SELECT count(*) FROM dlq_replays").fetchone() == (0,)

    (fixed,) = replay(settings, chosen, patch={"payload": {"status": "active"}})

    assert fixed.status == "replayed"
    assert reconcile_everything(settings, admin) == {"applied": 1}
    assert statuses(db) == {"person-bad": "active"}
    assert db.execute("SELECT event_id, patched FROM dlq_replays").fetchall() == [
        (dead["bad_status"]["event_id"], True)
    ]


def test_message_that_is_not_json_cannot_be_replayed(settings: Settings, dead: dict) -> None:
    chosen = [r for r in read_dlq(settings) if r.key == b"person-garbage"]

    (result,) = replay(settings, chosen, patch={"payload": {"status": "active"}})

    assert result.status == "failed" and "not JSON" in result.detail


def test_dry_run_validates_but_sends_and_records_nothing(
    settings: Settings, dead: dict, db: psycopg.Connection
) -> None:
    raw_before = len(read_topic(settings, settings.topics.raw))

    results = replay(settings, read_dlq(settings), dry_run=True)

    assert sorted(r.status for r in results) == ["failed", "failed", "would-replay"]
    assert len(read_topic(settings, settings.topics.raw)) == raw_before
    assert db.execute("SELECT count(*) FROM dlq_replays").fetchone() == (0,)
