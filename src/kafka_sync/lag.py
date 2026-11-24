"""Consumer group lag: how far the group's committed offsets trail the log end."""

from __future__ import annotations

import time
from dataclasses import dataclass

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from kafka_sync.clients import LEADER_ELECTION_ERRORS


@dataclass(frozen=True)
class PartitionLag:
    topic: str
    partition: int
    # None when the group has never committed for this partition.
    committed: int | None
    end: int
    lag: int


def consumer_group_lag(
    admin: AdminClient, group: str, topics: list[str], *, timeout: float = 10.0
) -> list[PartitionLag]:
    """Lag per partition of `topics` for `group`.

    A partition without a committed offset counts its whole retained log as
    lag, matching what a consumer with auto.offset.reset=earliest would read.
    """
    metadata = admin.list_topics(timeout=timeout)
    partitions = [
        TopicPartition(topic, partition)
        for topic in topics
        if topic in metadata.topics
        for partition in sorted(metadata.topics[topic].partitions)
    ]
    if not partitions:
        return []

    committed_future = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, partitions)], request_timeout=timeout
    )[group]
    committed = {
        (tp.topic, tp.partition): tp.offset
        for tp in committed_future.result(timeout).topic_partitions
    }
    end = _offsets(admin, partitions, OffsetSpec.latest(), timeout)
    start = _offsets(admin, partitions, OffsetSpec.earliest(), timeout)

    result = []
    for tp in partitions:
        key = (tp.topic, tp.partition)
        offset = committed.get(key, -1)
        position = offset if offset >= 0 else start[key]
        result.append(
            PartitionLag(
                topic=tp.topic,
                partition=tp.partition,
                committed=offset if offset >= 0 else None,
                end=end[key],
                lag=max(0, end[key] - position),
            )
        )
    return result


def topic_end_offsets(
    admin: AdminClient, topics: list[str], *, timeout: float = 10.0
) -> dict[str, int]:
    """Total messages ever written to each topic (sum of partition end offsets)."""
    metadata = admin.list_topics(timeout=timeout)
    partitions = [
        TopicPartition(topic, partition)
        for topic in topics
        if topic in metadata.topics
        for partition in metadata.topics[topic].partitions
    ]
    totals = dict.fromkeys(topics, 0)
    for (topic, _), offset in _offsets(admin, partitions, OffsetSpec.latest(), timeout).items():
        totals[topic] += offset
    return totals


def _offsets(
    admin: AdminClient, partitions: list[TopicPartition], spec: OffsetSpec, timeout: float
) -> dict[tuple[str, int], int]:
    if not partitions:
        return {}
    deadline = time.monotonic() + timeout
    while True:
        futures = admin.list_offsets(dict.fromkeys(partitions, spec), request_timeout=timeout)
        try:
            return {(tp.topic, tp.partition): f.result(timeout).offset for tp, f in futures.items()}
        except KafkaException as exc:
            # A partition between leaders answers within milliseconds; ask again.
            if exc.args[0].code() not in LEADER_ELECTION_ERRORS or time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
