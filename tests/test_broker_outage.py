"""Stops the broker mid-flight. Excluded from `make test`; run with `make chaos`.

Disruptive on purpose: it restarts the Kafka container of the local stack, so
anything else using that broker sees an outage for a few seconds.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path

import psycopg
import pytest
from confluent_kafka.admin import AdminClient
from helpers import ConsumerRunner, insert_outbox, make_event, wait_until

from kafka_sync.config import Settings
from kafka_sync.producer import OutboxPublisher

pytestmark = [pytest.mark.integration, pytest.mark.disruptive]

REPO_ROOT = Path(__file__).resolve().parents[1]


def compose(*args: str) -> None:
    """Run a compose command the same way the Makefile resolves it."""
    command = shlex.split(os.environ.get("COMPOSE", "")) or (
        ["docker", "compose"] if shutil.which("docker") else ["podman", "compose"]
    )
    subprocess.run(
        [*command, *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        env={**os.environ, "PODMAN_COMPOSE_WARNING_LOGS": "false"},
    )


def broker_is_up(admin: AdminClient) -> bool:
    try:
        admin.list_topics(timeout=2)
    except Exception:  # noqa: BLE001 - any failure means "not yet"
        return False
    return True


def count(db: psycopg.Connection, query: str) -> int:
    return db.execute(query).fetchone()[0]


def test_pipeline_recovers_from_a_broker_outage_without_losing_events(
    settings: Settings, db: psycopg.Connection, admin: AdminClient
) -> None:
    before = [make_event(f"person-{n}", version=1) for n in range(5)]
    during = [make_event(f"person-{n}", version=2, status="inactive") for n in range(5)]
    unpublished = "SELECT count(*) FROM source_events WHERE published_at IS NULL"
    reconciled = "SELECT count(*) FROM processed_events"

    with psycopg.connect(settings.postgres_dsn) as conn, ConsumerRunner(settings):
        publisher = OutboxPublisher(settings, conn)
        insert_outbox(db, *before)
        assert publisher.publish_batch().published == 5
        wait_until(lambda: count(db, reconciled) == 5, what="events before the outage applied")

        compose("stop", "kafka")
        try:
            # The application keeps writing to the outbox; that needs no broker.
            insert_outbox(db, *during)
            result = publisher.publish_batch()

            # Nothing was acknowledged, so nothing is marked published.
            assert (result.published, result.unconfirmed) == (0, 5)
            assert count(db, unpublished) == 5
        finally:
            compose("start", "kafka")
            wait_until(lambda: broker_is_up(admin), timeout=120, interval=1, what="broker back up")

        # The publisher catches up, and the consumer that sat through the outage
        # carries on without a restart.
        wait_until(
            lambda: publisher.publish_batch() and count(db, unpublished) == 0,
            timeout=60,
            interval=1,
            what="outbox drained after recovery",
        )
        wait_until(lambda: count(db, reconciled) == 10, timeout=90, what="all events applied")

    rows = db.execute("SELECT status, applied_count FROM reconciled_records").fetchall()
    assert rows == [("inactive", 2)] * 5
