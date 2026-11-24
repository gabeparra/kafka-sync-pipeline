"""The reconciliation consumer loop.

Order of operations for every batch, which is the whole delivery guarantee:

1. handle each message (the handler commits its own database transaction),
2. flush everything this batch produced (reconciled state, retries, DLQ),
3. only then commit the consumer offsets.

A crash at any point leaves offsets behind the work, never ahead of it, so the
worst case is a redelivery that the idempotency ledger turns into a no-op.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime
from typing import Any
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from pydantic import ValidationError

from kafka_sync.clients import consumer_config, headers_to_dict, producer_config
from kafka_sync.config import Settings
from kafka_sync.consumer import routing
from kafka_sync.consumer.errors import PermanentError
from kafka_sync.consumer.reconciler import Handler, Origin, Outcome
from kafka_sync.contract import ContractError, EventCodec
from kafka_sync.models import RecordEvent

log = logging.getLogger(__name__)

# Commit failures that only mean "the group is rebalancing". The offsets stay
# pending and are committed on the next pass if we still own the partition.
_REBALANCE_ERRORS = frozenset(
    {
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError._ASSIGNMENT_LOST,
        KafkaError._STATE,
    }
)

# Consume errors that mean the deployment is wrong, not that a broker is busy.
_CONFIGURATION_ERRORS = frozenset(
    {
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
    }
)

Partition = tuple[str, int]


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"cannot serialise {type(value).__name__}")


class ReconciliationConsumer:
    def __init__(
        self,
        settings: Settings,
        handler: Handler,
        *,
        codec: EventCodec | None = None,
        name: str = "reconciler",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._codec = codec or EventCodec(settings)
        self._clock = clock
        self.name = name
        # Per-outcome counts for logs and tests (applied, stale, duplicate, retry, dlq).
        self.stats: Counter[str] = Counter()

        self._producer = Producer(producer_config(settings) | {"client.id": name})
        self._delivery_errors: list[KafkaError] = []

        # Next offset to commit per partition, for work that is already durable.
        self._pending: dict[Partition, int] = {}
        # Retry partitions paused until their head message is due: partition -> epoch seconds.
        self._paused: dict[Partition, float] = {}
        # Partitions this member owns, maintained by the rebalance callbacks so
        # other threads can read it without touching the (not thread-safe) client.
        self._owned: set[Partition] = set()
        self._owned_lock = threading.Lock()
        self._assigned = threading.Event()

        self._consumer = Consumer(consumer_config(settings) | {"client.id": name})
        self._consumer.subscribe(
            [settings.topics.raw, settings.topics.retry],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    # ------------------------------------------------------------------ loop

    def run(self, stop: threading.Event) -> None:
        """Process batches until `stop` is set, then leave the group cleanly."""
        try:
            while not stop.is_set():
                self.run_once()
        finally:
            self.close()

    def run_once(self, timeout: float = 1.0) -> int:
        """Fetch and fully process one batch. Returns the number of messages handled."""
        self._resume_due_partitions()
        if self._pending:
            self._commit_pending()

        messages = self._consumer.consume(self._settings.consumer_batch_size, timeout)
        handled = 0
        deferred: set[Partition] = set()

        for msg in messages:
            if msg.error():
                self._on_consume_error(msg.error())
                continue
            partition = (msg.topic(), msg.partition())
            if partition in deferred:
                # Already rewound this partition; the message will be fetched again.
                continue
            if msg.topic() == self._settings.topics.retry and not self._is_due(msg):
                self._defer(msg)
                deferred.add(partition)
                continue
            self._process(msg)
            self._pending[partition] = msg.offset() + 1
            handled += 1

        if handled:
            self._flush_produced()
            self._commit_pending()
        return handled

    def _on_consume_error(self, error: KafkaError) -> None:
        """Ride out connection trouble; stop on errors that waiting cannot fix."""
        if error.fatal() or error.code() in _CONFIGURATION_ERRORS:
            raise KafkaException(error)
        # Broker down, leader moving, coordinator loading: the client reconnects
        # and resumes from the committed offsets by itself.
        log.warning("%s consume error (will keep polling): %s", self.name, error)

    def wait_until_assigned(self, timeout: float) -> bool:
        """True once the group has given this consumer at least one partition."""
        return self._assigned.wait(timeout)

    def assignment(self) -> set[Partition]:
        """The (topic, partition) pairs this member currently owns. Thread-safe."""
        with self._owned_lock:
            return set(self._owned)

    def close(self) -> None:
        """Leave the group. Deliberately commits nothing: only `run_once` commits."""
        try:
            self._producer.flush(10)
        finally:
            self._consumer.close()

    # ------------------------------------------------------------ processing

    def _process(self, msg: Message) -> None:
        attempt = routing.retry_attempt(headers_to_dict(msg.headers()))
        origin = Origin(msg.topic(), msg.partition(), msg.offset())
        try:
            event = self._parse(msg)
            result = self._handler(event, origin)
        except Exception as exc:  # noqa: BLE001 - every failure must be routed, not crash the loop
            route = routing.plan_failure(exc, attempt, self._settings.max_retries)
            log.warning(
                "%s %s[%d]@%d -> %s (%s: %s)",
                self.name,
                origin.topic,
                origin.partition,
                origin.offset,
                route.target,
                route.error_type,
                route.reason,
            )
            self._route_failure(msg, route)
            return

        self.stats[result.outcome.value] += 1
        # A duplicate also re-emits: the previous attempt may have crashed after
        # its database commit but before this produce. The topic is compacted
        # and keyed, so repeating the latest state is harmless.
        if result.state is not None and result.outcome in (Outcome.APPLIED, Outcome.DUPLICATE):
            self._producer.produce(
                self._settings.topics.reconciled,
                key=event.entity_id,
                value=json.dumps(result.state, default=_json_default).encode(),
                on_delivery=self._on_delivery,
            )

    def _parse(self, msg: Message) -> RecordEvent:
        try:
            return RecordEvent.model_validate(self._codec.decode(msg.value()))
        except ContractError as exc:
            raise PermanentError("contract_violation", str(exc)) from exc
        except ValidationError as exc:
            first = exc.errors()[0]
            where = ".".join(str(part) for part in first["loc"])
            raise PermanentError("invalid_event", f"{where}: {first['msg']}") from exc

    def _route_failure(self, msg: Message, route: routing.FailureRoute) -> None:
        topics = self._settings.topics
        self.stats[route.target] += 1
        self._producer.produce(
            topics.retry if route.target == "retry" else topics.dlq,
            key=msg.key(),
            value=msg.value(),
            headers=routing.build_headers(
                msg.headers(),
                route,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                now=self._clock(),
                retry_delay_seconds=self._settings.retry_delay_seconds,
            ),
            on_delivery=self._on_delivery,
        )

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._delivery_errors.append(err)

    def _flush_produced(self) -> None:
        """Make this batch's output durable, or fail before any offset is committed."""
        remaining = self._producer.flush(self._settings.delivery_timeout_ms / 1000 + 5)
        if remaining or self._delivery_errors:
            errors, self._delivery_errors = self._delivery_errors, []
            # Fail-stop: the batch is redelivered after restart and deduplicated.
            raise RuntimeError(
                f"could not deliver batch output ({remaining} unsent, errors: {errors})"
            )

    # --------------------------------------------------------------- commits

    def _commit_pending(self) -> None:
        offsets = [TopicPartition(t, p, o) for (t, p), o in self._pending.items()]
        try:
            self._consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() not in _REBALANCE_ERRORS:
                raise
            log.warning("%s commit deferred during rebalance: %s", self.name, exc)
            return
        self._pending.clear()

    # ---------------------------------------------------------- retry delays

    def _is_due(self, msg: Message) -> bool:
        return self._clock() >= routing.not_before(headers_to_dict(msg.headers()))

    def _defer(self, msg: Message) -> None:
        """Park a retry partition until its head message is due.

        Pausing stops fetches for that partition only, so the consumer keeps
        polling (and stays in the group) while it waits. Seeking back makes the
        same message the first one returned after resume.
        """
        partition = TopicPartition(msg.topic(), msg.partition(), msg.offset())
        self._consumer.pause([partition])
        self._consumer.seek(partition)
        self._paused[(msg.topic(), msg.partition())] = routing.not_before(
            headers_to_dict(msg.headers())
        )

    def _resume_due_partitions(self) -> None:
        now = self._clock()
        for partition in [p for p, due in self._paused.items() if now >= due]:
            del self._paused[partition]
            self._consumer.resume([TopicPartition(*partition)])

    # ------------------------------------------------------------ rebalances

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Cooperative rebalancing: `partitions` are the newly added ones only.
        log.info("%s assigned %s", self.name, _describe(partitions))
        with self._owned_lock:
            self._owned.update((p.topic, p.partition) for p in partitions)
        if partitions:
            self._assigned.set()

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Last chance to commit finished work for partitions we are giving up."""
        log.info("%s revoked %s", self.name, _describe(partitions))
        offsets = [
            TopicPartition(p.topic, p.partition, self._pending[(p.topic, p.partition)])
            for p in partitions
            if (p.topic, p.partition) in self._pending
        ]
        if offsets:
            try:
                consumer.commit(offsets=offsets, asynchronous=False)
            except KafkaException as exc:
                log.warning("%s could not commit on revoke: %s", self.name, exc)
        self._forget(partitions)

    def _on_lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Ownership is already gone, so committing is not allowed; the new
        # owner re-reads from the last commit and the ledger absorbs the overlap.
        log.warning("%s lost %s", self.name, _describe(partitions))
        self._forget(partitions)

    def _forget(self, partitions: list[TopicPartition]) -> None:
        with self._owned_lock:
            self._owned.difference_update((p.topic, p.partition) for p in partitions)
        for p in partitions:
            self._pending.pop((p.topic, p.partition), None)
            self._paused.pop((p.topic, p.partition), None)


def _describe(partitions: list[TopicPartition]) -> str:
    return ", ".join(f"{p.topic}[{p.partition}]" for p in partitions) or "nothing"
