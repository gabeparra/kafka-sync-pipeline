"""Kafka client configuration shared by every service.

The settings that give the pipeline its delivery guarantees are set here, in
one place, rather than repeated (and drifting) across producer and consumer code.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError

from kafka_sync.config import Settings

# What a partition answers while its leader is being elected (new topic, broker
# restart). Always temporary, so read-only tooling retries instead of failing.
LEADER_ELECTION_ERRORS = frozenset(
    {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
    }
)


def producer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        # Idempotence: the broker de-duplicates the producer's own retries, so a
        # retried send can neither duplicate nor reorder messages in a partition.
        "enable.idempotence": True,
        # Wait for every in-sync replica before treating a send as successful.
        "acks": "all",
        # Same key -> same partition as the Java clients (librdkafka's default
        # hash differs), so producers in other languages agree on placement.
        "partitioner": "murmur2_random",
        "message.timeout.ms": settings.delivery_timeout_ms,
        "linger.ms": 5,
    }


def consumer_config(settings: Settings, *, group: str | None = None) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group or settings.consumer_group,
        # Offsets are committed by the application, only after the work is durable.
        "enable.auto.commit": False,
        # A brand-new group starts from the oldest retained message, not the newest.
        "auto.offset.reset": "earliest",
        # Incremental rebalancing: only the partitions that actually move are
        # revoked, instead of every consumer stopping on each membership change.
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": settings.session_timeout_ms,
    }


def headers_to_dict(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    """Message headers as text. Later duplicates of a name win, as on the broker."""
    return {name: (value or b"").decode("utf-8", "replace") for name, value in headers or []}
