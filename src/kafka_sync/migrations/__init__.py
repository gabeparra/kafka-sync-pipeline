"""Ordered SQL migrations, each applied exactly once.

Applied versions are recorded in `schema_migrations`. A file runs in one
transaction together with its bookkeeping row, so a failed migration leaves
nothing behind and is retried on the next run.
"""

from __future__ import annotations

import logging
from importlib import resources

import psycopg

log = logging.getLogger(__name__)


def apply_migrations(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations and return the versions that ran."""
    with conn.transaction():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    files = sorted(
        (f for f in resources.files(__package__).iterdir() if f.name.endswith(".sql")),
        key=lambda f: f.name,
    )
    applied: list[str] = []
    for file in files:
        version = file.name.removesuffix(".sql")
        with conn.transaction():
            # Serialise concurrent runners (two services starting at once).
            conn.execute("LOCK TABLE schema_migrations IN EXCLUSIVE MODE")
            done = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = %s", (version,)
            ).fetchone()
            if done:
                continue
            log.info("applying migration %s", version)
            conn.execute(file.read_text("utf-8"))  # type: ignore[arg-type]
            conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            applied.append(version)
    return applied
