"""Canonical immutable envelopes for durable raw event-member staging."""

from __future__ import annotations

from dataclasses import dataclass


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
