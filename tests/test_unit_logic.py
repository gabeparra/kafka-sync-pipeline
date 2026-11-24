"""Small pure pieces of the pipeline: ordering rule, merge patch, typed model."""

from __future__ import annotations

import pytest
from helpers import make_event
from pydantic import ValidationError

from kafka_sync.cli.dlq_replay import merge_patch
from kafka_sync.contract import ContractError, loose_json
from kafka_sync.models import RecordEvent
from kafka_sync.producer import confirmed_in_order


class TestConfirmedInOrder:
    def test_everything_is_confirmed_when_nothing_failed(self) -> None:
        sent = [(1, "a"), (2, "b"), (3, "a")]

        assert confirmed_in_order(sent, set()) == [1, 2, 3]

    def test_rows_after_a_failure_for_the_same_entity_are_held_back(self) -> None:
        sent = [(1, "a"), (2, "a"), (3, "a")]

        assert confirmed_in_order(sent, {2}) == [1]

    def test_a_failure_does_not_hold_back_other_entities(self) -> None:
        sent = [(1, "a"), (2, "b"), (3, "a"), (4, "b")]

        assert confirmed_in_order(sent, {1}) == [2, 4]


class TestMergePatch:
    def test_nested_objects_merge_and_keep_untouched_fields(self) -> None:
        target = {"entity_id": "p1", "payload": {"status": "bogus", "full_name": "A"}}

        result = merge_patch(target, {"payload": {"status": "active"}})

        assert result == {"entity_id": "p1", "payload": {"status": "active", "full_name": "A"}}

    def test_null_removes_a_field(self) -> None:
        assert merge_patch({"a": 1, "b": 2}, {"b": None}) == {"a": 1}

    def test_input_is_not_mutated(self) -> None:
        target = {"payload": {"status": "bogus"}}

        merge_patch(target, {"payload": {"status": "active"}})

        assert target == {"payload": {"status": "bogus"}}


class TestRecordEvent:
    def test_accepts_a_contract_valid_event(self) -> None:
        event = RecordEvent.model_validate(make_event("person-9", version=3))

        assert event.entity_id == "person-9"
        assert event.occurred_at.tzinfo is not None

    def test_rejects_a_date_that_matches_the_pattern_but_does_not_exist(self) -> None:
        with pytest.raises(ValidationError):
            RecordEvent.model_validate(make_event(birth_date="2026-02-30"))


class TestLooseJson:
    def test_reads_bare_json_and_wire_format_alike(self) -> None:
        assert loose_json(b'{"a": 1}') == {"a": 1}
        assert loose_json(b"\x00\x00\x00\x00\x07" + b'{"a": 1}') == {"a": 1}

    def test_rejects_bytes_that_are_not_json(self) -> None:
        with pytest.raises(ContractError):
            loose_json(b"\xff\xfe not json")
