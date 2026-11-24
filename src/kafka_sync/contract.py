"""The event contract: one JSON Schema enforced at both ends of the topic.

Messages use the Confluent wire format (magic byte, 4-byte schema id, JSON
body). The producer registers the schema with Schema Registry, which rejects an
incompatible change before any consumer can see it; both sides validate every
message against the schema.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from kafka_sync.config import Settings

SCHEMA_STR = (
    resources.files("kafka_sync.schemas").joinpath("record_event.schema.json").read_text("utf-8")
)

# Confluent wire format: 1 magic byte (0) + 4-byte big-endian schema id.
WIRE_HEADER_SIZE = 5


class ContractError(ValueError):
    """The bytes or object do not satisfy the event contract."""


class EventCodec:
    """Encodes and decodes record events, validating against the schema both ways."""

    def __init__(self, settings: Settings) -> None:
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._serializer = JSONSerializer(
            SCHEMA_STR,
            registry,
            conf={
                # One contract regardless of which topic carries it (raw, retry, replay).
                "subject.name.strategy": lambda _ctx, _record_name: settings.schema_subject,
            },
        )
        self._deserializer = JSONDeserializer(SCHEMA_STR)
        self._context = SerializationContext(settings.topics.raw, MessageField.VALUE)

    def encode(self, event: dict[str, Any]) -> bytes:
        try:
            return self._serializer(event, self._context)
        except SerializationError as exc:
            raise ContractError(str(exc)) from exc

    def decode(self, data: bytes | None) -> dict[str, Any]:
        if data is None:
            raise ContractError("message has no value")
        try:
            event = self._deserializer(data, self._context)
        except SerializationError as exc:
            raise ContractError(str(exc)) from exc
        if not isinstance(event, dict):
            raise ContractError("message value is not a JSON object")
        return event


def loose_json(data: bytes | None) -> Any:
    """Best-effort parse of a rejected message so a human can inspect or patch it.

    Accepts both wire-format messages and bare JSON. Raises ContractError when
    the bytes are not JSON at all.
    """
    if not data:
        raise ContractError("message has no value")
    body = data[WIRE_HEADER_SIZE:] if data[0] == 0 and len(data) > WIRE_HEADER_SIZE else data
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"not JSON: {exc}") from exc
