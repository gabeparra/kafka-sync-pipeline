"""Publishes rows from the `source_events` outbox table to Kafka.

Delivery is at-least-once: a row is marked published only after the broker has
acknowledged it, so a crash in between re-publishes the row on the next run.
Consumers remove those duplicates using `event_id`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg
from confluent_kafka import KafkaError, Producer

from kafka_sync.clients import producer_config
from kafka_sync.config import Settings
from kafka_sync.contract import ContractError, EventCodec

log = logging.getLogger(__name__)

# Advisory lock id shared by every publisher instance (arbitrary constant).
OUTBOX_LOCK_KEY = 7_265_001


@dataclass(frozen=True)
class BatchResult:
    published: int = 0
    # Rows that violate the event contract and were parked with a reason.
    rejected: int = 0
    # Rows the broker did not acknowledge; they stay queued for the next batch.
    unconfirmed: int = 0
    # False when another publisher holds the lock and this one stood down.
    is_leader: bool = True

    @property
    def idle(self) -> bool:
        return self.published == 0 and self.rejected == 0 and self.unconfirmed == 0


def confirmed_in_order(sent: list[tuple[int, str]], failed_ids: set[int]) -> list[int]:
    """Row ids that may be marked published without breaking per-entity order.

    `sent` is (row id, entity id) in publish order. Once one row of an entity
    fails, every later row of that entity is held back too, even if the broker
    acknowledged it: the failed row will be re-sent, and re-sending the later
    ones after it keeps the entity's newest event last in the partition.
    """
    blocked_entities: set[str] = set()
    confirmed: list[int] = []
    for row_id, entity_id in sent:
        if row_id in failed_ids:
            blocked_entities.add(entity_id)
        elif entity_id not in blocked_entities:
            confirmed.append(row_id)
    return confirmed


class OutboxPublisher:
    def __init__(
        self,
        settings: Settings,
        conn: psycopg.Connection,
        *,
        producer: Producer | None = None,
        codec: EventCodec | None = None,
    ) -> None:
        self._settings = settings
        self._conn = conn
        self._producer = producer or Producer(producer_config(settings))
        self._codec = codec or EventCodec(settings)

    def publish_batch(self) -> BatchResult:
        """Publish up to one batch of unpublished rows, oldest first."""
        with self._conn.transaction():
            # Only one publisher may run at a time. Two publishers reading
            # different slices of the table could interleave an entity's
            # events; the lock is released automatically at commit or crash.
            is_leader = self._conn.execute(
                "SELECT pg_try_advisory_xact_lock(%s)", (OUTBOX_LOCK_KEY,)
            ).fetchone()[0]
            if not is_leader:
                return BatchResult(is_leader=False)

            rows = self._conn.execute(
                """
                SELECT id, event_id, entity_id, event_type, payload, occurred_at
                FROM source_events
                WHERE published_at IS NULL AND publish_error IS NULL
                ORDER BY id
                LIMIT %s
                """,
                (self._settings.outbox_batch_size,),
            ).fetchall()
            if not rows:
                return BatchResult()

            rejected: list[tuple[str, int]] = []
            sent: list[tuple[int, str]] = []
            acked: set[int] = set()

            for row_id, event_id, entity_id, event_type, payload, occurred_at in rows:
                event = {
                    "event_id": str(event_id),
                    "entity_id": entity_id,
                    "event_type": event_type,
                    "occurred_at": occurred_at.isoformat(),
                    "payload": payload,
                }
                try:
                    value = self._codec.encode(event)
                except ContractError as exc:
                    log.warning("outbox row %s rejected: %s", row_id, exc)
                    rejected.append((str(exc), row_id))
                    continue
                self._produce(row_id, entity_id, value, str(event_id), event_type, acked)
                sent.append((row_id, entity_id))

            # Block until every message is acknowledged or has timed out.
            if self._producer.flush(self._settings.delivery_timeout_ms / 1000 + 5) > 0:
                # Drop what is still queued so it is not delivered after we have
                # already decided to re-send it.
                self._producer.purge()
                self._producer.flush(5)

            failed = {row_id for row_id, _ in sent} - acked
            confirmed = confirmed_in_order(sent, failed)

            self._conn.execute(
                "UPDATE source_events SET published_at = now() WHERE id = ANY(%s)", (confirmed,)
            )
            if rejected:
                self._conn.cursor().executemany(
                    "UPDATE source_events SET publish_error = %s WHERE id = %s", rejected
                )

        result = BatchResult(
            published=len(confirmed),
            rejected=len(rejected),
            unconfirmed=len(sent) - len(confirmed),
        )
        if result.unconfirmed:
            log.warning("%d rows not confirmed by the broker; will retry", result.unconfirmed)
        return result

    def _produce(
        self,
        row_id: int,
        entity_id: str,
        value: bytes,
        event_id: str,
        event_type: str,
        acked: set[int],
    ) -> None:
        def on_delivery(err: KafkaError | None, _msg: Any) -> None:
            if err is None:
                acked.add(row_id)
            else:
                log.warning("delivery failed for outbox row %s: %s", row_id, err)

        kwargs = {
            "key": entity_id,
            "value": value,
            "headers": [("event_id", event_id.encode()), ("event_type", event_type.encode())],
            "on_delivery": on_delivery,
        }
        try:
            self._producer.produce(self._settings.topics.raw, **kwargs)
        except BufferError:
            # Local queue is full: let deliveries drain, then try once more.
            self._producer.poll(1)
            self._producer.produce(self._settings.topics.raw, **kwargs)
        self._producer.poll(0)
