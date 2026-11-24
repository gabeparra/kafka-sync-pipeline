"""How a handler tells the consumer what kind of failure it hit."""

from __future__ import annotations


class PermanentError(Exception):
    """Retrying cannot help: the message itself is the problem. Goes to the DLQ."""

    def __init__(self, error_type: str, reason: str) -> None:
        super().__init__(reason)
        self.error_type = error_type


class TransientError(Exception):
    """Worth another attempt later, e.g. a dependency was briefly unavailable."""
