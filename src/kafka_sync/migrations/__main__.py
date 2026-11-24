"""`python -m kafka_sync.migrations`: apply pending migrations to the configured database."""

import logging

import psycopg

from kafka_sync.config import Settings
from kafka_sync.migrations import apply_migrations


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="==> %(message)s")
    with psycopg.connect(Settings.from_env().postgres_dsn) as conn:
        applied = apply_migrations(conn)
    logging.info("migrations up to date (%d applied)", len(applied))


if __name__ == "__main__":
    main()
