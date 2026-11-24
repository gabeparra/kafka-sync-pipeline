"""`python -m kafka_sync.producer`: run the outbox publisher."""

from __future__ import annotations

import argparse
import logging
import signal
import threading

import psycopg

from kafka_sync.config import Settings
from kafka_sync.log import configure_logging
from kafka_sync.producer.outbox import OutboxPublisher

log = logging.getLogger("kafka_sync.producer")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish outbox rows to Kafka.")
    parser.add_argument("--once", action="store_true", help="drain the outbox, then exit")
    args = parser.parse_args()
    configure_logging()

    settings = Settings.from_env()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    total = 0
    with psycopg.connect(settings.postgres_dsn) as conn:
        publisher = OutboxPublisher(settings, conn)
        while not stop.is_set():
            result = publisher.publish_batch()
            total += result.published
            if not result.idle:
                log.info(
                    "published=%d rejected=%d unconfirmed=%d",
                    result.published,
                    result.rejected,
                    result.unconfirmed,
                )
            if result.published == 0:
                # Nothing moved: outbox empty, broker unreachable, or not the leader.
                if args.once:
                    break
                stop.wait(settings.outbox_poll_seconds)
    log.info("stopped after publishing %d events", total)


if __name__ == "__main__":
    main()
