"""FastAPI application: health, Prometheus metrics, and reconciled state."""

from __future__ import annotations

import urllib.request
from collections.abc import Callable
from datetime import date, datetime
from typing import Annotated
from uuid import UUID

import psycopg
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from psycopg.rows import dict_row
from pydantic import BaseModel

from kafka_sync.api.metrics import PipelineCollector
from kafka_sync.config import Settings


class Health(BaseModel):
    status: str
    checks: dict[str, str]


class Reconciliation(BaseModel):
    id: int
    document_number: str
    birth_date: date
    source_system: str
    source_entity_id: str
    full_name: str
    email: str | None
    status: str
    last_event_id: UUID
    last_event_at: datetime
    applied_count: int
    updated_at: datetime


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    registry = CollectorRegistry()
    registry.register(PipelineCollector(settings, admin))

    app = FastAPI(title="kafka-sync-pipeline", summary="Health, metrics and reconciled state.")

    def check_kafka() -> None:
        admin.list_topics(timeout=3)

    def check_postgres() -> None:
        with psycopg.connect(settings.postgres_dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")

    def check_schema_registry() -> None:
        urllib.request.urlopen(f"{settings.schema_registry_url}/subjects", timeout=3).close()

    checks: dict[str, Callable[[], None]] = {
        "kafka": check_kafka,
        "postgres": check_postgres,
        "schema_registry": check_schema_registry,
    }

    # Plain `def` endpoints: FastAPI runs them in a thread pool, which suits
    # the blocking Kafka and Postgres clients used here.

    @app.get("/health", response_model=Health)
    def health(response: Response) -> Health:
        results: dict[str, str] = {}
        for name, check in checks.items():
            try:
                check()
            except Exception as exc:  # noqa: BLE001 - report any failure as unhealthy
                results[name] = f"error: {type(exc).__name__}"
            else:
                results[name] = "ok"
        healthy = all(value == "ok" for value in results.values())
        if not healthy:
            response.status_code = 503
        return Health(status="ok" if healthy else "degraded", checks=results)

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/reconciliations", response_model=list[Reconciliation])
    def reconciliations(
        document_number: Annotated[str | None, Query(description="Exact match filter")] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> list[dict]:
        """Most recently updated destination records."""
        with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
            return conn.execute(
                """
                SELECT id, document_number, birth_date, source_system, source_entity_id,
                       full_name, email, status, last_event_id, last_event_at,
                       applied_count, updated_at
                FROM reconciled_records
                WHERE %(doc)s::text IS NULL OR document_number = %(doc)s
                ORDER BY updated_at DESC, id DESC
                LIMIT %(limit)s
                """,
                {"doc": document_number, "limit": limit},
            ).fetchall()

    return app
