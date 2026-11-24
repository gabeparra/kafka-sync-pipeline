"""Typed in-process view of a record event.

JSON Schema is the wire contract shared with other teams and languages; this
model is what the Python code works with once a message has passed it. Parsing
here also catches what a regex cannot, such as 2026-02-30 being no real date.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict


class RecordPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_system: str
    document_number: str
    birth_date: date
    full_name: str
    email: str | None = None
    status: Literal["active", "inactive", "pending"]
    version: int | None = None


class RecordEvent(BaseModel):
    event_id: UUID
    entity_id: str
    event_type: Literal["record.created", "record.updated"]
    occurred_at: AwareDatetime
    payload: RecordPayload

    @classmethod
    def build(
        cls,
        *,
        event_id: UUID | str,
        entity_id: str,
        event_type: str,
        occurred_at: datetime,
        payload: dict,
    ) -> RecordEvent:
        return cls.model_validate(
            {
                "event_id": event_id,
                "entity_id": entity_id,
                "event_type": event_type,
                "occurred_at": occurred_at,
                "payload": payload,
            }
        )
