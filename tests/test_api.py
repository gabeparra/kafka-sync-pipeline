"""Admin API: health, Prometheus metrics (including consumer lag), reconciled state."""

from __future__ import annotations

import dataclasses

import psycopg
import pytest
from confluent_kafka.admin import AdminClient
from fastapi.testclient import TestClient
from helpers import ConsumerRunner, make_event, send, send_events, total_lag, wait_until
from prometheus_client.parser import text_string_to_metric_families

from kafka_sync.api import create_app
from kafka_sync.config import Settings
from kafka_sync.contract import EventCodec

pytestmark = pytest.mark.integration


@pytest.fixture
def client(settings: Settings) -> TestClient:
    return TestClient(create_app(settings))


def scrape(client: TestClient) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse /metrics into {sample name: [(labels, value), ...]}."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    samples: dict[str, list[tuple[dict[str, str], float]]] = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            samples.setdefault(sample.name, []).append((sample.labels, sample.value))
    return samples


def raw_lag(samples: dict, settings: Settings) -> float:
    return sum(
        value
        for labels, value in samples["kafka_consumer_group_lag"]
        if labels["topic"] == settings.topics.raw and labels["group"] == settings.consumer_group
    )


def test_health_reports_every_dependency(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"kafka": "ok", "postgres": "ok", "schema_registry": "ok"},
    }


def test_health_is_503_and_names_the_dependency_that_is_down(settings: Settings) -> None:
    # Port 1 is closed, so the connection is refused immediately.
    broken = dataclasses.replace(settings, postgres_dsn="host=127.0.0.1 port=1 dbname=x")

    response = TestClient(create_app(broken)).get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"].startswith("error")
    assert body["checks"]["kafka"] == "ok"


def test_metrics_expose_consumer_lag_and_pipeline_counters(
    settings: Settings, client: TestClient, codec: EventCodec, admin: AdminClient
) -> None:
    send_events(settings, codec, [make_event(f"person-{n}") for n in range(5)])
    send(settings, [("person-x", b"poison")])

    # Nothing has consumed yet: the whole topic is lag.
    assert raw_lag(scrape(client), settings) == 6

    with ConsumerRunner(settings):
        wait_until(lambda: total_lag(admin, settings) == 0, what="backlog consumed")

    samples = scrape(client)
    assert raw_lag(samples, settings) == 0
    assert len(samples["kafka_consumer_group_lag"]) == 9, "6 raw + 3 retry partitions"
    assert ({"outcome": "applied"}, 5.0) in samples["sync_events_processed_total"]
    assert samples["sync_events_dead_lettered_total"] == [({}, 1.0)]
    assert samples["sync_events_retried_total"] == [({}, 0.0)]
    assert samples["sync_reconciled_records"] == [({}, 5.0)]
    assert samples["sync_outbox_unpublished_events"] == [({}, 0.0)]
    assert sorted((labels["dependency"], v) for labels, v in samples["sync_dependency_up"]) == [
        ("kafka", 1.0),
        ("postgres", 1.0),
    ]


def test_metrics_scrape_survives_a_dependency_outage(settings: Settings) -> None:
    broken = dataclasses.replace(settings, postgres_dsn="host=127.0.0.1 port=1 dbname=x")

    samples = scrape(TestClient(create_app(broken)))

    assert ({"dependency": "postgres"}, 0.0) in samples["sync_dependency_up"]
    assert ({"dependency": "kafka"}, 1.0) in samples["sync_dependency_up"]
    assert "kafka_consumer_group_lag" in samples, "Kafka metrics are still served"
    assert "sync_reconciled_records" not in samples


def test_reconciliations_lists_filters_and_limits(
    settings: Settings, client: TestClient, codec: EventCodec, db: psycopg.Connection
) -> None:
    send_events(settings, codec, [make_event(f"person-{n}", status="pending") for n in range(3)])
    with ConsumerRunner(settings):
        wait_until(
            lambda: db.execute("SELECT count(*) FROM reconciled_records").fetchone()[0] == 3,
            what="3 reconciled records",
        )

    everything = client.get("/reconciliations").json()
    assert {r["source_entity_id"] for r in everything} == {"person-0", "person-1", "person-2"}
    assert {r["status"] for r in everything} == {"pending"}

    (one,) = client.get("/reconciliations", params={"document_number": "DOC-person-1"}).json()
    assert (one["source_entity_id"], one["applied_count"]) == ("person-1", 1)

    assert len(client.get("/reconciliations", params={"limit": 2}).json()) == 2
    assert client.get("/reconciliations", params={"limit": 0}).status_code == 422
