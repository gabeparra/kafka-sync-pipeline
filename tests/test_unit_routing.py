"""Failure routing policy. Pure functions, no broker needed."""

from __future__ import annotations

from kafka_sync.consumer import PermanentError, TransientError, routing


def test_permanent_error_goes_straight_to_dlq_with_its_own_type() -> None:
    route = routing.plan_failure(PermanentError("contract_violation", "bad"), 0, 3)

    assert (route.target, route.error_type, route.attempt) == ("dlq", "contract_violation", 0)


def test_transient_error_is_retried_with_incremented_attempt() -> None:
    route = routing.plan_failure(TransientError("db down"), 1, 3)

    assert (route.target, route.error_type, route.attempt) == ("retry", "TransientError", 2)
    assert route.reason == "db down"


def test_unexpected_exception_is_treated_as_transient() -> None:
    """An unknown bug gets bounded retries rather than an instant DLQ or a crash loop."""
    assert routing.plan_failure(KeyError("x"), 0, 3).target == "retry"


def test_transient_error_goes_to_dlq_once_retries_are_exhausted() -> None:
    route = routing.plan_failure(TransientError("still down"), 3, 3)

    assert (route.target, route.error_type, route.attempt) == ("dlq", "retries_exhausted", 3)
    assert route.reason == "still down"


def test_zero_max_retries_disables_the_retry_topic() -> None:
    assert routing.plan_failure(TransientError("x"), 0, 0).target == "dlq"


def test_reason_is_truncated_so_headers_stay_small() -> None:
    route = routing.plan_failure(TransientError("x" * 5000), 0, 3)

    assert len(route.reason) == routing.MAX_REASON_LENGTH


def test_retry_attempt_defaults_to_zero_for_missing_or_garbage_header() -> None:
    assert routing.retry_attempt({}) == 0
    assert routing.retry_attempt({routing.H_ATTEMPT: "abc"}) == 0
    assert routing.retry_attempt({routing.H_ATTEMPT: "-4"}) == 0
    assert routing.retry_attempt({routing.H_ATTEMPT: "2"}) == 2


def test_retry_headers_carry_delay_origin_and_application_headers() -> None:
    route = routing.plan_failure(TransientError("db down"), 0, 3)

    headers = dict(
        routing.build_headers(
            [("event_id", b"abc")],
            route,
            topic="source.records.raw",
            partition=4,
            offset=17,
            now=1000.0,
            retry_delay_seconds=60,
        )
    )

    assert headers["event_id"] == b"abc"
    assert headers[routing.H_ATTEMPT] == b"1"
    assert headers[routing.H_NOT_BEFORE_MS] == b"1060000"
    assert headers[routing.H_ORIGINAL_TOPIC] == b"source.records.raw"
    assert headers[routing.H_ORIGINAL_PARTITION] == b"4"
    assert headers[routing.H_ORIGINAL_OFFSET] == b"17"
    assert headers[routing.H_ERROR_REASON] == b"db down"
    assert routing.not_before({k: v.decode() for k, v in headers.items()}) == 1060.0


def test_original_coordinates_survive_a_second_hop_through_the_retry_topic() -> None:
    first_hop = routing.build_headers(
        None,
        routing.plan_failure(TransientError("x"), 0, 1),
        topic="source.records.raw",
        partition=4,
        offset=17,
        now=1000.0,
        retry_delay_seconds=60,
    )

    second_hop = dict(
        routing.build_headers(
            first_hop,
            routing.plan_failure(TransientError("x"), 1, 1),
            topic="records.retry.1m",
            partition=0,
            offset=3,
            now=2000.0,
            retry_delay_seconds=60,
        )
    )

    assert second_hop[routing.H_ERROR_TYPE] == b"retries_exhausted"
    assert second_hop[routing.H_ORIGINAL_TOPIC] == b"source.records.raw"
    assert second_hop[routing.H_ORIGINAL_PARTITION] == b"4"
    assert second_hop[routing.H_ORIGINAL_OFFSET] == b"17"
    # A DLQ message is not scheduled for another attempt.
    assert routing.H_NOT_BEFORE_MS not in second_hop
