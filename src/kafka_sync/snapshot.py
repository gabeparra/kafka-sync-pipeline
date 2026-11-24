"""Read everything currently in a topic, once, without joining a consumer group."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition

from kafka_sync.clients import LEADER_ELECTION_ERRORS, headers_to_dict
from kafka_sync.config import Settings


@dataclass(frozen=True)
class TopicRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    raw_headers: list[tuple[str, bytes | None]]

    @property
    def headers(self) -> dict[str, str]:
        return headers_to_dict(self.raw_headers)


def read_topic(settings: Settings, topic: str, *, timeout: float = 10.0) -> list[TopicRecord]:
    """All retained messages of `topic`, ordered by partition then offset.

    Reads a snapshot up to the end offsets seen at the start. Uses manual
    partition assignment and never commits, so it leaves no consumer group
    behind and does not disturb the pipeline's own offsets.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"snapshot-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=timeout).topics[topic]
        if metadata.error is not None:
            raise KafkaException(metadata.error)

        remaining: dict[int, int] = {}  # partition -> end offset still to reach
        start: list[TopicPartition] = []
        for partition in metadata.partitions:
            low, high = _watermarks(consumer, TopicPartition(topic, partition), timeout)
            if high > low:
                remaining[partition] = high
                start.append(TopicPartition(topic, partition, low))
        if not start:
            return []

        consumer.assign(start)
        records: list[TopicRecord] = []
        while remaining:
            msg = consumer.poll(timeout)
            if msg is None:
                raise TimeoutError(f"timed out reading {topic}; partitions left: {remaining}")
            if msg.error():
                raise KafkaException(msg.error())
            if msg.partition() not in remaining:
                continue
            records.append(
                TopicRecord(
                    topic,
                    msg.partition(),
                    msg.offset(),
                    msg.key(),
                    msg.value(),
                    msg.headers() or [],
                )
            )
            if msg.offset() + 1 >= remaining[msg.partition()]:
                del remaining[msg.partition()]
                consumer.pause([TopicPartition(topic, msg.partition())])
        return sorted(records, key=lambda r: (r.partition, r.offset))
    finally:
        consumer.close()


def _watermarks(consumer: Consumer, partition: TopicPartition, timeout: float) -> tuple[int, int]:
    """Low and high offsets, riding out the moment a new topic has no leader yet."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return consumer.get_watermark_offsets(partition, timeout=timeout)
        except KafkaException as exc:
            if exc.args[0].code() not in LEADER_ELECTION_ERRORS or time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
