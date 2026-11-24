"""Prometheus metrics, computed at scrape time from Kafka and Postgres.

The numbers come from durable state (committed offsets, log end offsets, the
ledger table) instead of in-process counters. They therefore survive restarts,
cover every consumer instance at once, and cannot drift from what happened.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import psycopg
from confluent_kafka.admin import AdminClient
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from kafka_sync.config import Settings
from kafka_sync.lag import consumer_group_lag, topic_end_offsets

log = logging.getLogger(__name__)


class PipelineCollector(Collector):
    def __init__(self, settings: Settings, admin: AdminClient) -> None:
        self._settings = settings
        self._admin = admin

    def collect(self) -> Iterator[Metric]:
        up = GaugeMetricFamily(
            "sync_dependency_up", "1 if the dependency answered this scrape.", labels=["dependency"]
        )
        for name, source in (("kafka", self._kafka_metrics), ("postgres", self._postgres_metrics)):
            try:
                # Collect fully before yielding so a failure exposes no partial data.
                metrics = list(source())
            except Exception:  # noqa: BLE001 - a dependency being down must not break the scrape
                log.exception("metrics: %s unavailable", name)
                up.add_metric([name], 0)
            else:
                up.add_metric([name], 1)
                yield from metrics
        yield up

    def _kafka_metrics(self) -> Iterator[Metric]:
        topics = self._settings.topics
        group = self._settings.consumer_group

        lag = GaugeMetricFamily(
            "kafka_consumer_group_lag",
            "Messages between the group's committed offset and the end of the partition.",
            labels=["group", "topic", "partition"],
        )
        for row in consumer_group_lag(self._admin, group, [topics.raw, topics.retry]):
            lag.add_metric([group, row.topic, str(row.partition)], row.lag)
        yield lag

        written = topic_end_offsets(self._admin, [topics.retry, topics.dlq])
        yield CounterMetricFamily(
            "sync_events_retried",
            "Messages routed to the retry topic.",
            value=written[topics.retry],
        )
        yield CounterMetricFamily(
            "sync_events_dead_lettered",
            "Messages routed to the dead letter queue.",
            value=written[topics.dlq],
        )

    def _postgres_metrics(self) -> Iterator[Metric]:
        with psycopg.connect(self._settings.postgres_dsn, connect_timeout=5) as conn:
            outcomes = dict(
                conn.execute("SELECT outcome, count(*) FROM processed_events GROUP BY 1").fetchall()
            )
            unpublished, rejected = conn.execute(
                """
                SELECT count(*) FILTER (WHERE published_at IS NULL AND publish_error IS NULL),
                       count(*) FILTER (WHERE publish_error IS NOT NULL)
                FROM source_events
                """
            ).fetchone()
            (records,) = conn.execute("SELECT count(*) FROM reconciled_records").fetchone()

        processed = CounterMetricFamily(
            "sync_events_processed",
            "Distinct events handled by the reconciliation consumer.",
            labels=["outcome"],
        )
        for outcome in ("applied", "stale"):
            processed.add_metric([outcome], outcomes.get(outcome, 0))
        yield processed
        yield GaugeMetricFamily(
            "sync_outbox_unpublished_events", "Outbox rows waiting to be published.", unpublished
        )
        yield GaugeMetricFamily(
            "sync_outbox_rejected_events",
            "Outbox rows parked for violating the contract.",
            rejected,
        )
        yield GaugeMetricFamily(
            "sync_reconciled_records", "Records present in the destination table.", records
        )
