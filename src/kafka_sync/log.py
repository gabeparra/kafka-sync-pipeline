"""Logging setup shared by the long-running services."""

from __future__ import annotations

import logging


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    # The Schema Registry client logs every HTTP request at INFO.
    logging.getLogger("httpx2").setLevel(logging.WARNING)
