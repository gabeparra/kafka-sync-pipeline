"""Applies a record event to the destination tables, exactly once.

The ledger insert and the state change share one database transaction. Either
both happen or neither does, which is what turns Kafka's at-least-once delivery
into exactly-once *effects*.
"""

from __future__ import annotations

import enum
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from kafka_sync.consumer.errors import PermanentError, TransientError
from kafka_sync.models import RecordEvent

log = logging.getLogger(__name__)


class Outcome(enum.StrEnum):
    APPLIED = "applied"
    # An event older than the state already stored; recorded, not applied.
    STALE = "stale"
    # This event_id was handled before; nothing was written.
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class Origin:
    """Where the consumer read the message from."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ReconcileResult:
    outcome: Outcome
    # Current destination row for the record, if one exists.
    state: dict[str, Any] | None = None


Handler = Callable[[RecordEvent, Origin], ReconcileResult]

_STATE_COLUMNS = """
    id, document_number, birth_date, source_system, source_entity_id,
    full_name, email, status, last_event_id, last_event_at, applied_count
"""


class PostgresReconciler:
    """The production handler: matches on the natural key and upserts."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def __call__(self, event: RecordEvent, origin: Origin) -> ReconcileResult:
        try:
            return self._reconcile(event, origin)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            # Connection-level trouble: drop the connection and let the message retry.
            self.close()
            raise TransientError(f"database unavailable: {exc}") from exc
        except (psycopg.DataError, psycopg.IntegrityError) as exc:
            # The database rejected the values themselves; retrying changes nothing.
            raise PermanentError("rejected_by_destination", str(exc)) from exc

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self._dsn, row_factory=dict_row)
        return self._conn

    def _reconcile(self, event: RecordEvent, origin: Origin) -> ReconcileResult:
        conn = self._connection()
        record = event.payload
        natural_key = (record.document_number, record.birth_date)

        with conn.transaction():
            # 1. Claim the event. A concurrent duplicate blocks on this insert
            #    until we commit, then sees the conflict and backs off.
            claimed = conn.execute(
                """
                INSERT INTO processed_events
                    (event_id, entity_id, outcome, source_topic, source_partition, source_offset)
                VALUES (%s, %s, 'applied', %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (event.event_id, event.entity_id, origin.topic, origin.partition, origin.offset),
            ).fetchone()
            if claimed is None:
                return ReconcileResult(Outcome.DUPLICATE, self._current_state(conn, natural_key))

            # 2. Upsert by natural key, but never let an older event overwrite
            #    newer state. Events can arrive out of order after a retry.
            state = conn.execute(
                f"""
                INSERT INTO reconciled_records
                    (document_number, birth_date, source_system, source_entity_id,
                     full_name, email, status, last_event_id, last_event_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (document_number, birth_date) DO UPDATE SET
                    source_system    = EXCLUDED.source_system,
                    source_entity_id = EXCLUDED.source_entity_id,
                    full_name        = EXCLUDED.full_name,
                    email            = EXCLUDED.email,
                    status           = EXCLUDED.status,
                    last_event_id    = EXCLUDED.last_event_id,
                    last_event_at    = EXCLUDED.last_event_at,
                    applied_count    = reconciled_records.applied_count + 1,
                    updated_at       = now()
                WHERE reconciled_records.last_event_at < EXCLUDED.last_event_at
                RETURNING {_STATE_COLUMNS}
                """,
                (
                    record.document_number,
                    record.birth_date,
                    record.source_system,
                    event.entity_id,
                    record.full_name,
                    record.email,
                    record.status,
                    event.event_id,
                    event.occurred_at,
                ),
            ).fetchone()
            if state is not None:
                return ReconcileResult(Outcome.APPLIED, state)

            conn.execute(
                "UPDATE processed_events SET outcome = 'stale' WHERE event_id = %s",
                (event.event_id,),
            )
            return ReconcileResult(Outcome.STALE, self._current_state(conn, natural_key))

    @staticmethod
    def _current_state(conn: psycopg.Connection, natural_key: tuple) -> dict[str, Any] | None:
        return conn.execute(
            f"""
            SELECT {_STATE_COLUMNS} FROM reconciled_records
            WHERE document_number = %s AND birth_date = %s
            """,
            natural_key,
        ).fetchone()
