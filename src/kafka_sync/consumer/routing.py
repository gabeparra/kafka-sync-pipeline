"""Decides where a failed message goes, and what travels with it.

Kept free of Kafka clients so the policy can be unit-tested as plain functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from kafka_sync.consumer.errors import PermanentError

# Headers the pipeline adds to messages it re-routes.
H_ATTEMPT = "x-retry-attempt"
H_NOT_BEFORE_MS = "x-retry-not-before-ms"
H_ERROR_TYPE = "x-error-type"
H_ERROR_REASON = "x-error-reason"
H_ORIGINAL_TOPIC = "x-original-topic"
H_ORIGINAL_PARTITION = "x-original-partition"
H_ORIGINAL_OFFSET = "x-original-offset"
H_FAILED_AT_MS = "x-failed-at-ms"
H_REPLAYED_FROM = "x-replayed-from"

PIPELINE_HEADERS = frozenset(
    {
        H_ATTEMPT,
        H_NOT_BEFORE_MS,
        H_ERROR_TYPE,
        H_ERROR_REASON,
        H_ORIGINAL_TOPIC,
        H_ORIGINAL_PARTITION,
        H_ORIGINAL_OFFSET,
        H_FAILED_AT_MS,
        H_REPLAYED_FROM,
    }
)

RETRIES_EXHAUSTED = "retries_exhausted"
MAX_REASON_LENGTH = 500


@dataclass(frozen=True)
class FailureRoute:
    target: Literal["retry", "dlq"]
    error_type: str
    reason: str
    # Retry attempt number the message will carry (0 = never retried).
    attempt: int


def plan_failure(error: Exception, attempt: int, max_retries: int) -> FailureRoute:
    """Pick retry or DLQ for a message that failed on its `attempt`-th retry."""
    reason = (str(error) or type(error).__name__)[:MAX_REASON_LENGTH]
    if isinstance(error, PermanentError):
        return FailureRoute("dlq", error.error_type, reason, attempt)
    if attempt >= max_retries:
        return FailureRoute("dlq", RETRIES_EXHAUSTED, reason, attempt)
    return FailureRoute("retry", type(error).__name__, reason, attempt + 1)


def retry_attempt(headers: dict[str, str]) -> int:
    """How many times this message has already been retried."""
    try:
        return max(0, int(headers.get(H_ATTEMPT, "0")))
    except ValueError:
        return 0


def not_before(headers: dict[str, str]) -> float:
    """Epoch seconds before which a retry message must not be processed."""
    try:
        return int(headers[H_NOT_BEFORE_MS]) / 1000
    except (KeyError, ValueError):
        return 0.0


def build_headers(
    original: list[tuple[str, bytes | None]] | None,
    route: FailureRoute,
    *,
    topic: str,
    partition: int,
    offset: int,
    now: float,
    retry_delay_seconds: float,
) -> list[tuple[str, bytes]]:
    """Headers for the re-routed copy of a failed message.

    Application headers pass through untouched. The original coordinates are
    written on the first failure only, so a message that bounces through the
    retry topic still points back to where it first appeared.
    """
    existing = {name: value for name, value in original or [] if value is not None}
    headers = {name: value for name, value in existing.items() if name not in PIPELINE_HEADERS}

    for name, value in (
        (H_ORIGINAL_TOPIC, topic),
        (H_ORIGINAL_PARTITION, partition),
        (H_ORIGINAL_OFFSET, offset),
    ):
        headers[name] = existing.get(name) or str(value).encode()

    headers[H_ATTEMPT] = str(route.attempt).encode()
    headers[H_ERROR_TYPE] = route.error_type.encode()
    headers[H_ERROR_REASON] = route.reason.encode()
    headers[H_FAILED_AT_MS] = str(int(now * 1000)).encode()
    if route.target == "retry":
        headers[H_NOT_BEFORE_MS] = str(int((now + retry_delay_seconds) * 1000)).encode()
    return list(headers.items())
