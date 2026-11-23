"""Shared fixtures: connection settings and clients for the local stack."""

from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from confluent_kafka.admin import AdminClient

BOOTSTRAP_SERVERS = f"localhost:{os.getenv('KAFKA_HOST_PORT', '9092')}"
SCHEMA_REGISTRY_URL = f"http://localhost:{os.getenv('SCHEMA_REGISTRY_HOST_PORT', '8081')}"
POSTGRES_DSN = (
    f"host=localhost port={os.getenv('POSTGRES_HOST_PORT', '5432')} "
    f"user={os.getenv('POSTGRES_USER', 'sync')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'sync')} "
    f"dbname={os.getenv('POSTGRES_DB', 'sync')}"
)


@pytest.fixture(scope="session")
def admin() -> AdminClient:
    client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        client.list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001 - any failure here means "stack is down"
        pytest.fail(f"Kafka is not reachable at {BOOTSTRAP_SERVERS}; run `make up` first ({exc})")
    return client


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    return BOOTSTRAP_SERVERS


@pytest.fixture(scope="session")
def schema_registry_url() -> str:
    return SCHEMA_REGISTRY_URL


@pytest.fixture
def db() -> Iterator[psycopg.Connection]:
    """A connection whose work is rolled back, so tests leave no rows behind."""
    try:
        conn = psycopg.connect(POSTGRES_DSN, connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.fail(f"Postgres is not reachable; run `make up` first ({exc})")
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
