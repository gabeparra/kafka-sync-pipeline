"""`python -m kafka_sync.consumer`: run one member of the reconciliation group."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
import time

from kafka_sync.config import Settings
from kafka_sync.consumer import PostgresReconciler, ReconciliationConsumer
from kafka_sync.log import configure_logging

log = logging.getLogger("kafka_sync.consumer")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile record events into Postgres.")
    parser.add_argument("--name", default="reconciler", help="client id shown in logs and lag")
    parser.add_argument(
        "--idle-exit",
        type=float,
        metavar="SECONDS",
        help="exit after this long without messages (for demos and scripts)",
    )
    args = parser.parse_args()
    configure_logging()

    settings = Settings.from_env()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    reconciler = PostgresReconciler(settings.postgres_dsn)
    consumer = ReconciliationConsumer(settings, reconciler, name=args.name)
    last_activity = time.monotonic()
    try:
        while not stop.is_set():
            if consumer.run_once():
                last_activity = time.monotonic()
                log.info("totals %s", dict(consumer.stats))
            elif args.idle_exit and time.monotonic() - last_activity > args.idle_exit:
                break
    finally:
        consumer.close()
        reconciler.close()
    log.info("stopped; totals %s", dict(consumer.stats))


if __name__ == "__main__":
    main()
