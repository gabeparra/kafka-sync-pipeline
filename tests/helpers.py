"""Building blocks shared by the integration tests."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from psycopg.types.json import Jsonb

from kafka_sync.clients import producer_config
from kafka_sync.config import Settings
from kafka_sync.consumer import (
    Handler,
    Origin,
    PostgresReconciler,
    ReconcileResult,
    ReconciliationConsumer,
)
from kafka_sync.contract import EventCodec
from kafka_sync.lag import consumer_group_lag
from kafka_sync.models import RecordEvent

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def make_event(
    entity: str = "person-1",
    *,
    version: int = 1,
    status: str = "active",
    event_id: str | None = None,
    occurred_at: datetime | None = None,
    **payload: Any,
) -> dict[str, Any]:
    """A contract-valid event. Higher `version` means a later `occurred_at`."""
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "entity_id": entity,
        "event_type": "record.created" if version == 1 else "record.updated",
        "occurred_at": (occurred_at or BASE_TIME + timedelta(seconds=version)).isoformat(),
        "payload": {
            "source_system": "erp",
            "document_number": f"DOC-{entity}",
            "birth_date": "1990-01-01",
            "full_name": f"Name of {entity}",
            "email": f"{entity}@example.com",
            "status": status,
            "version": version,
            **payload,
        },
    }


def insert_outbox(db: psycopg.Connection, *events: dict[str, Any]) -> None:
    """Write events to the outbox the way an application transaction would."""
    for event in events:
        db.execute(
            """
            INSERT INTO source_events (event_id, entity_id, event_type, payload, occurred_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                event["event_id"],
                event["entity_id"],
                event["event_type"],
                Jsonb(event["payload"]),
                event["occurred_at"],
            ),
        )


def send(
    settings: Settings,
    values: Iterable[tuple[str, bytes]],
    *,
    topic: str | None = None,
    headers: list[tuple[str, bytes]] | None = None,
) -> None:
    """Produce (key, raw value) pairs and wait for the broker to acknowledge them."""
    producer = Producer(producer_config(settings))
    errors: list[Any] = []
    for key, value in values:
        producer.produce(
            topic or settings.topics.raw,
            key=key,
            value=value,
            headers=headers,
            on_delivery=lambda err, _msg: errors.append(err) if err else None,
        )
    assert producer.flush(30) == 0, "broker did not acknowledge test messages"
    assert not errors, errors


def send_events(settings: Settings, codec: EventCodec, events: Iterable[dict[str, Any]]) -> None:
    send(settings, [(event["entity_id"], codec.encode(event)) for event in events])


def wait_until(
    predicate: Callable[[], Any], *, timeout: float = 30.0, interval: float = 0.1, what: str = ""
) -> Any:
    """Poll until `predicate` is truthy and return its value; fail the test on timeout."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {what or predicate}")
        time.sleep(interval)


def total_lag(admin: AdminClient, settings: Settings) -> int:
    topics = [settings.topics.raw, settings.topics.retry]
    return sum(row.lag for row in consumer_group_lag(admin, settings.consumer_group, topics))


class Recorder:
    """Wraps a handler and remembers every call, across consumers and threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: list[tuple[str, RecordEvent]] = []

    def wrap(self, consumer_name: str, handler: Handler) -> Handler:
        def recording(event: RecordEvent, origin: Origin) -> ReconcileResult:
            with self._lock:
                self.calls.append((consumer_name, event))
            return handler(event, origin)

        return recording

    def event_ids(self, consumer_name: str | None = None) -> list[str]:
        with self._lock:
            return [
                str(event.event_id)
                for name, event in self.calls
                if consumer_name is None or name == consumer_name
            ]


class ConsumerRunner:
    """Runs a real ReconciliationConsumer on a background thread.

    Use as a context manager. An exception that escapes the consumer loop is
    kept in `error`; leaving the block re-raises it unless the test consumed it
    with `take_error()`, so a crashed consumer can never pass silently.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        name: str = "consumer-a",
        handler: Handler | None = None,
        wrap: Callable[[str, Handler], Handler] | None = None,
    ) -> None:
        self._reconciler = PostgresReconciler(settings.postgres_dsn)
        inner = handler or self._reconciler
        self.worker = ReconciliationConsumer(
            settings, wrap(name, inner) if wrap else inner, name=name
        )
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def _run(self) -> None:
        try:
            self.worker.run(self._stop)
        except BaseException as exc:  # noqa: BLE001 - includes simulated crashes
            self.error = exc
        finally:
            self._reconciler.close()

    def start(self) -> ConsumerRunner:
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop after the current batch and leave the group cleanly."""
        self._stop.set()
        self._thread.join(timeout=60)
        assert not self._thread.is_alive(), "consumer thread did not stop"

    def join(self, timeout: float = 30.0) -> None:
        """Wait for the loop to end on its own (after a simulated crash)."""
        self._thread.join(timeout)
        assert not self._thread.is_alive(), "consumer thread is still running"

    def take_error(self) -> BaseException | None:
        error, self.error = self.error, None
        return error

    def __enter__(self) -> ConsumerRunner:
        return self.start()

    def __exit__(self, exc_type: Any, *_: Any) -> None:
        self.stop()
        if self.error is not None and exc_type is None:
            raise AssertionError(f"consumer {self.worker.name} crashed") from self.error
