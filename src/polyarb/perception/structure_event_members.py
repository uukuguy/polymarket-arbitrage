"""Canonical immutable envelopes for durable raw event-member staging."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DecodedMemberBatch:
    members: tuple[tuple[int, dict[str, object], str], ...]
    next_member_ordinal: int
    next_byte_offset: int
    complete: bool


_DECODER = json.JSONDecoder()
_JSON_NUMBER_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")


def _skip_whitespace(payload: str, offset: int) -> int:
    while offset < len(payload) and payload[offset] in " \t\r\n":
        offset += 1
    return offset


def _scan_string(payload: str, offset: int) -> tuple[str, int]:
    if offset >= len(payload) or payload[offset] != '"':
        raise ValueError("invalid-structure-event-member-json")
    try:
        value, end = json.decoder.scanstring(payload, offset + 1, True)
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError("invalid-structure-event-member-json") from error
    return value, end


def _skip_json_value(payload: str, offset: int) -> int:
    """Lexically skip one JSON value without materializing it."""
    offset = _skip_whitespace(payload, offset)
    if offset >= len(payload):
        raise ValueError("invalid-structure-event-member-json")
    if payload[offset] == '"':
        return _scan_string(payload, offset)[1]
    if payload[offset] == "[":
        offset = _skip_whitespace(payload, offset + 1)
        if offset < len(payload) and payload[offset] == "]":
            return offset + 1
        while True:
            offset = _skip_whitespace(payload, _skip_json_value(payload, offset))
            if offset >= len(payload) or payload[offset] not in ",]":
                raise ValueError("invalid-structure-event-member-json")
            if payload[offset] == "]":
                return offset + 1
            offset = _skip_whitespace(payload, offset + 1)
            if offset >= len(payload) or payload[offset] == "]":
                raise ValueError("invalid-structure-event-member-json")
    if payload[offset] == "{":
        offset = _skip_whitespace(payload, offset + 1)
        if offset < len(payload) and payload[offset] == "}":
            return offset + 1
        while True:
            _key, offset = _scan_string(payload, offset)
            offset = _skip_whitespace(payload, offset)
            if offset >= len(payload) or payload[offset] != ":":
                raise ValueError("invalid-structure-event-member-json")
            offset = _skip_whitespace(
                payload, _skip_json_value(payload, _skip_whitespace(payload, offset + 1))
            )
            if offset >= len(payload) or payload[offset] not in ",}":
                raise ValueError("invalid-structure-event-member-json")
            if payload[offset] == "}":
                return offset + 1
            offset = _skip_whitespace(payload, offset + 1)
            if offset >= len(payload) or payload[offset] == "}":
                raise ValueError("invalid-structure-event-member-json")
    end = offset
    while end < len(payload) and payload[end] not in ",]} \t\r\n":
        end += 1
    token = payload[offset:end]
    if token not in {"true", "false", "null"} and _JSON_NUMBER_RE.fullmatch(token) is None:
        raise ValueError("invalid-structure-event-member-json")
    return end


def _locate_markets_array(payload: str) -> int:
    offset = _skip_whitespace(payload, 0)
    if offset >= len(payload) or payload[offset] != "{":
        raise ValueError("invalid-structure-event-member-json")
    offset = _skip_whitespace(payload, offset + 1)
    found = False
    while offset < len(payload) and payload[offset] != "}":
        key, offset = _scan_string(payload, offset)
        offset = _skip_whitespace(payload, offset)
        if offset >= len(payload) or payload[offset] != ":":
            raise ValueError("invalid-structure-event-member-json")
        offset = _skip_whitespace(payload, offset + 1)
        if key == "markets":
            if found:
                raise ValueError("duplicate-structure-event-markets")
            found = True
            if offset >= len(payload) or payload[offset] != "[":
                raise ValueError("invalid-structure-event-markets")
            return offset
        offset = _skip_whitespace(payload, _skip_json_value(payload, offset))
        if offset >= len(payload) or payload[offset] not in ",}":
            raise ValueError("invalid-structure-event-member-json")
        if payload[offset] == ",":
            offset = _skip_whitespace(payload, offset + 1)
            if offset >= len(payload) or payload[offset] == "}":
                raise ValueError("invalid-structure-event-member-json")
    raise ValueError("missing-structure-event-markets")


def _validate_after_markets(payload: str, offset: int) -> None:
    offset = _skip_whitespace(payload, offset)
    while offset < len(payload) and payload[offset] == ",":
        offset = _skip_whitespace(payload, offset + 1)
        key, offset = _scan_string(payload, offset)
        if key == "markets":
            raise ValueError("duplicate-structure-event-markets")
        offset = _skip_whitespace(payload, offset)
        if offset >= len(payload) or payload[offset] != ":":
            raise ValueError("invalid-structure-event-member-json")
        offset = _skip_whitespace(
            payload, _skip_json_value(payload, _skip_whitespace(payload, offset + 1))
        )
    if offset >= len(payload) or payload[offset] != "}":
        raise ValueError("invalid-structure-event-member-json")
    if _skip_whitespace(payload, offset + 1) != len(payload):
        raise ValueError("invalid-structure-event-member-trailing-data")


def _byte_offset(payload: str, character_offset: int) -> int:
    if payload.isascii():
        return character_offset
    return len(payload[:character_offset].encode("utf-8"))


def _character_offset(payload: str, byte_offset: int) -> int:
    if payload.isascii():
        return byte_offset
    try:
        prefix = payload.encode("utf-8")[:byte_offset].decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("structure-event-member-cursor-mismatch") from error
    if len(prefix.encode("utf-8")) != byte_offset:
        raise ValueError("structure-event-member-cursor-mismatch")
    return len(prefix)


def decode_event_member_batch(
    payload_json: str,
    *,
    member_ordinal: int,
    member_byte_offset: int,
    limit: int,
) -> DecodedMemberBatch:
    """Decode at most ``limit`` objects from the top-level markets array."""
    if (
        not isinstance(payload_json, str)
        or type(member_ordinal) is not int
        or member_ordinal < 0
        or type(member_byte_offset) is not int
        or member_byte_offset < 0
        or not 1 <= limit <= 500
    ):
        raise ValueError("invalid-structure-event-member-batch")
    if member_ordinal == 0:
        if member_byte_offset != 0:
            raise ValueError("structure-event-member-cursor-mismatch")
        array_offset = _locate_markets_array(payload_json)
        offset = _skip_whitespace(payload_json, array_offset + 1)
    else:
        if member_byte_offset == 0:
            raise ValueError("structure-event-member-cursor-mismatch")
        offset = _character_offset(payload_json, member_byte_offset)
        previous = offset - 1
        while previous >= 0 and payload_json[previous] in " \t\r\n":
            previous -= 1
        if previous < 0 or payload_json[previous] != ",":
            raise ValueError("structure-event-member-cursor-mismatch")
    if offset >= len(payload_json):
        raise ValueError("structure-event-member-cursor-mismatch")
    members: list[tuple[int, dict[str, object], str]] = []
    ordinal = member_ordinal
    if offset < len(payload_json) and payload_json[offset] == "]":
        _validate_after_markets(payload_json, offset + 1)
        return DecodedMemberBatch((), ordinal, _byte_offset(payload_json, offset), True)
    while len(members) < limit:
        start = offset
        try:
            member, offset = _DECODER.raw_decode(payload_json, start)
        except json.JSONDecodeError as error:
            raise ValueError("invalid-structure-event-member-json") from error
        if not isinstance(member, dict):
            raise ValueError("invalid-structure-event-member-object")
        members.append((ordinal, member, payload_json[start:offset]))
        ordinal += 1
        offset = _skip_whitespace(payload_json, offset)
        if offset >= len(payload_json):
            raise ValueError("invalid-structure-event-member-json")
        if payload_json[offset] == "]":
            _validate_after_markets(payload_json, offset + 1)
            return DecodedMemberBatch(
                tuple(members), ordinal, _byte_offset(payload_json, offset), True
            )
        if payload_json[offset] != ",":
            raise ValueError("invalid-structure-event-member-json")
        offset = _skip_whitespace(payload_json, offset + 1)
        if offset >= len(payload_json) or payload_json[offset] == "]":
            raise ValueError("invalid-structure-event-member-json")
    return DecodedMemberBatch(tuple(members), ordinal, _byte_offset(payload_json, offset), False)


def _strict_string(value: object) -> str | None:
    return value if isinstance(value, str) and value and value.strip() == value else None


def extract_structure_event_member_row(
    *,
    window_id: str,
    event_id: str,
    event_ordinal: int,
    member_ordinal: int,
    member: dict[str, object],
    event_group_id: object = None,
) -> StructureEventMemberRow:
    """Preserve one raw member while extracting only exact typed metadata."""
    payload_json = json.dumps(
        member,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    market_id = _strict_string(member.get("id"))
    nested_group_id = _strict_string(member.get("negRiskMarketID", member.get("groupId")))
    expected_group_id = _strict_string(event_group_id)
    group_id = (
        nested_group_id
        if expected_group_id is None or nested_group_id == expected_group_id
        else None
    )
    active = member.get("active") if type(member.get("active")) is bool else None
    closed = member.get("closed") if type(member.get("closed")) is bool else None
    other = member.get("negRiskOther")
    derived_kind = (
        "other"
        if other is True and active is not None and closed is not None
        else "inactive-reserved"
        if other is False and active is False and closed is not None
        else "named"
        if other is False and active is True and closed is not None
        else None
    )
    explicit_kind = _strict_string(member.get("memberKind"))
    member_kind = derived_kind if explicit_kind is None else explicit_kind
    return StructureEventMemberRow(
        window_id=window_id,
        event_id=event_id,
        event_ordinal=event_ordinal,
        member_ordinal=member_ordinal,
        market_id=market_id,
        market_sort_key=market_id or "",
        group_id=group_id,
        member_kind=member_kind,
        active=active,
        closed=closed,
        payload_json=payload_json,
        payload_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
    )


@dataclass(frozen=True)
class StructureEventMemberRow:
    window_id: str
    event_id: str
    event_ordinal: int
    member_ordinal: int
    market_id: str | None
    market_sort_key: str
    group_id: str | None
    member_kind: str | None
    active: bool | None
    closed: bool | None
    payload_json: str
    payload_hash: str


@dataclass(frozen=True)
class StructureEventMemberProgress:
    window_id: str
    event_cursor: str
    member_ordinal: int
    rows_written: int
    member_byte_offset: int
    member_state: str
    diagnostic_state: str
    checkpoint_at_ms: int
    completed_at_ms: int | None
    failure_reason: str | None


@dataclass(frozen=True)
class StructureEventMemberReceipt:
    window_id: str
    source_event_count: int
    source_event_root: str
    source_identity_hash: str
    metadata_contract: str
    member_row_count: int
    member_row_root: str
    invalid_member_count: int
    invalid_member_root: str
    terminal_event_cursor: str
    terminal_member_ordinal: int
    terminal_member_byte_offset: int
    sealed_at_ms: int
    receipt_digest: str
