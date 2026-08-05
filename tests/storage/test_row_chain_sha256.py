from __future__ import annotations

import hashlib
import json

import pytest

from polyarb.storage.row_chain_sha256 import (
    ROW_CHAIN_DOMAINS,
    ROW_CHAIN_SHA256_V2,
    RowChainSHA256,
)

PREFIX = b"polyarb.structure-drift.row-chain-sha256-v2\x00"
CLASSIFIER_V3_DOMAINS = frozenset(
    {
        "projection-exclusion/non-neg-risk-market",
        "projection-exclusion/market-side-quarantine",
        "projection-exclusion/non-neg-risk-event-member",
        "projection-exclusion/current-nontradable-event-member",
        "projection-exclusion/augmented-group",
        "projection-exclusion/fresh-group-ineligible",
        "projection-exclusion/event-only-quarantine",
    }
)
EXPECTED_DOMAINS = frozenset(
    {
        "source-event",
        "source-market",
        "source-group-truth",
        "projection-member",
        "generation-member",
        "generation-group-truth",
        "source-identity",
        "legacy-reconstruction",
        "generation-reconstruction",
        "class/shared",
        "class/fresh-addition",
        "class/current-nontradable",
        "class/event-only-quarantine",
        "class/market-side-quarantine",
        "class/fresh-source-absent",
        "class/fresh-group-ineligible",
        "class/overlap-conflict",
        "class/unclassified",
        "diagnostic/unclassified",
        *CLASSIFIER_V3_DOMAINS,
    }
)


def _frame(operation: str, domain: str) -> bytes:
    operation_bytes = operation.encode("ascii")
    domain_bytes = domain.encode("ascii")
    return (
        PREFIX
        + len(operation_bytes).to_bytes(2, "big")
        + operation_bytes
        + len(domain_bytes).to_bytes(2, "big")
        + domain_bytes
    )


def _partitioned_root(
    rows: list[object], cuts: tuple[int, ...], *, domain: str = "source-market"
) -> str:
    chain = RowChainSHA256.new(domain)
    offset = 0
    for size in cuts:
        for row in rows[offset : offset + size]:
            chain.update(row)
        chain = RowChainSHA256.from_json(
            chain.to_json(), expected_domain=domain
        )
        offset += size
    assert offset == len(rows)
    return chain.hexdigest()


def test_row_chain_registry_and_algorithm_are_frozen() -> None:
    assert ROW_CHAIN_SHA256_V2 == "row-chain-sha256-v2"
    assert ROW_CHAIN_DOMAINS == EXPECTED_DOMAINS


def test_domain_registry_adds_only_classifier_v2_and_v3_domains() -> None:
    old_domains = EXPECTED_DOMAINS - CLASSIFIER_V3_DOMAINS - {
        "class/fresh-group-ineligible",
        "diagnostic/unclassified",
    }
    assert ROW_CHAIN_DOMAINS - old_domains == CLASSIFIER_V3_DOMAINS | {
        "class/fresh-group-ineligible",
        "diagnostic/unclassified",
    }


@pytest.mark.parametrize("domain", sorted(EXPECTED_DOMAINS))
def test_row_chain_empty_root_matches_spec_formula(domain: str) -> None:
    initial = hashlib.sha256(_frame("init", domain)).digest()
    expected = hashlib.sha256(
        _frame("root", domain) + (0).to_bytes(8, "big") + initial
    ).hexdigest()

    assert RowChainSHA256.new(domain).hexdigest() == expected


@pytest.mark.parametrize("cuts", ((500,), (1, 499), (17, 100, 82, 301)))
def test_row_chain_root_is_chunk_boundary_independent(
    cuts: tuple[int, ...],
) -> None:
    rows: list[object] = [
        (index, {"z": index, "a": "值"}) for index in range(500)
    ]

    assert _partitioned_root(rows, cuts) == _partitioned_root(rows, (500,))


def test_row_chain_canonicalizes_mapping_key_order() -> None:
    first = RowChainSHA256.new("source-market")
    first.update((1, {"z": 2, "a": 1}))
    second = RowChainSHA256.new("source-market")
    second.update((1, {"a": 1, "z": 2}))

    assert first.hexdigest() == second.hexdigest()


@pytest.mark.parametrize(
    "mutation",
    (
        [("event-2", "market-1")],
        [("event-1", "market-2")],
        [("event-1", "market-1"), ("event-1", "market-1")],
        [],
    ),
)
def test_row_chain_binds_row_fields_count_and_duplicates(
    mutation: list[object],
) -> None:
    original = _partitioned_root([("event-1", "market-1")], (1,))

    assert _partitioned_root(mutation, (len(mutation),)) != original


def test_row_chain_binds_row_order_and_domain() -> None:
    rows: list[object] = [(1, "first"), (2, "second")]

    assert _partitioned_root(rows, (2,)) != _partitioned_root(
        list(reversed(rows)), (2,)
    )
    assert _partitioned_root(rows, (2,)) != _partitioned_root(
        rows, (2,), domain="source-event"
    )


def test_row_chain_state_round_trip_is_strict_and_canonical() -> None:
    chain = RowChainSHA256.new("source-market")
    chain.update(("market-1", {"active": True}))
    encoded = chain.to_json()

    assert encoded == json.dumps(
        json.loads(encoded), sort_keys=True, separators=(",", ":")
    )
    restored = RowChainSHA256.from_json(
        encoded, expected_domain="source-market"
    )
    assert restored.to_json() == encoded
    assert restored.hexdigest() == chain.hexdigest()


@pytest.mark.parametrize(
    "mutation",
    (
        lambda state: {**state, "extra": 1},
        lambda state: {key: value for key, value in state.items() if key != "count"},
        lambda state: {**state, "algorithm": "serializable-sha256-v1"},
        lambda state: {**state, "domain": "source-event"},
        lambda state: {**state, "count": -1},
        lambda state: {**state, "count": True},
        lambda state: {**state, "state_hex": "AA" * 32},
        lambda state: {**state, "state_hex": "gg" * 32},
        lambda state: {**state, "state_hex": "aa" * 31},
    ),
)
def test_row_chain_rejects_malformed_or_cross_domain_state(mutation) -> None:
    state = json.loads(RowChainSHA256.new("source-market").to_json())

    with pytest.raises(ValueError, match="invalid-row-chain-sha256-state"):
        RowChainSHA256.from_json(
            json.dumps(mutation(state)), expected_domain="source-market"
        )


def test_row_chain_rejects_unknown_domain_and_noncanonical_number() -> None:
    with pytest.raises(ValueError, match="invalid-row-chain-sha256-domain"):
        RowChainSHA256.new("unknown")
    chain = RowChainSHA256.new("source-market")
    with pytest.raises(ValueError, match="Out of range float values"):
        chain.update(("market-1", float("nan")))
