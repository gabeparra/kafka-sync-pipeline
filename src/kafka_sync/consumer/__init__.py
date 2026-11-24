"""Idempotent reconciliation consumer with retry and DLQ routing."""

from kafka_sync.consumer.errors import PermanentError, TransientError
from kafka_sync.consumer.reconciler import (
    Handler,
    Origin,
    Outcome,
    PostgresReconciler,
    ReconcileResult,
)
from kafka_sync.consumer.worker import ReconciliationConsumer

__all__ = [
    "Handler",
    "Origin",
    "Outcome",
    "PermanentError",
    "PostgresReconciler",
    "ReconcileResult",
    "ReconciliationConsumer",
    "TransientError",
]
