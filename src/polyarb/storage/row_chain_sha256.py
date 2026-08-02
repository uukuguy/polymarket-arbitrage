"""Versioned, resumable C-backed SHA-256 row commitments for Structure drift."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

ROW_CHAIN_SHA256_V2 = "row-chain-sha256-v2"
ROW_CHAIN_DOMAINS = frozenset(
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
        "class/overlap-conflict",
        "class/unclassified",
    }
)

_PREFIX = b"polyarb.structure-drift.row-chain-sha256-v2\x00"
_STATE_KEYS = {"algorithm", "count", "domain", "state_hex"}


def _validate_domain(domain: str) -> None:
    if domain not in ROW_CHAIN_DOMAINS:
        raise ValueError("invalid-row-chain-sha256-domain")


def _frame(operation: str, domain: str) -> bytes:
    operation_bytes = operation.encode("ascii")
    domain_bytes = domain.encode("ascii")
    return (
        _PREFIX
        + len(operation_bytes).to_bytes(2, "big")
        + operation_bytes
        + len(domain_bytes).to_bytes(2, "big")
        + domain_bytes
    )


def _canonical(row: object) -> bytes:
    return json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass
class RowChainSHA256:
    """One ordered row stream whose 32-byte state is safe to persist."""

    domain: str
    count: int
    _state: bytes

    @classmethod
    def new(cls, domain: str) -> RowChainSHA256:
        _validate_domain(domain)
        return cls(domain, 0, hashlib.sha256(_frame("init", domain)).digest())

    @classmethod
    def from_json(
        cls,
        encoded: str,
        *,
        expected_domain: str,
    ) -> RowChainSHA256:
        _validate_domain(expected_domain)
        try:
            raw = json.loads(encoded)
            if not isinstance(raw, dict) or set(raw) != _STATE_KEYS:
                raise ValueError
            algorithm = raw["algorithm"]
            count = raw["count"]
            domain = raw["domain"]
            state_hex = raw["state_hex"]
            if (
                algorithm != ROW_CHAIN_SHA256_V2
                or domain != expected_domain
                or type(count) is not int
                or count < 0
                or not isinstance(state_hex, str)
                or len(state_hex) != 64
                or state_hex != state_hex.lower()
                or any(character not in "0123456789abcdef" for character in state_hex)
            ):
                raise ValueError
            state = bytes.fromhex(state_hex)
            if len(state) != 32:
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-row-chain-sha256-state") from error
        return cls(domain, count, state)

    def update(self, row: object) -> None:
        canonical = _canonical(row)
        leaf = hashlib.sha256(
            _frame("leaf", self.domain)
            + len(canonical).to_bytes(8, "big")
            + canonical
        ).digest()
        self._state = hashlib.sha256(
            _frame("chain", self.domain) + self._state + leaf
        ).digest()
        self.count += 1

    def to_json(self) -> str:
        return json.dumps(
            {
                "algorithm": ROW_CHAIN_SHA256_V2,
                "count": self.count,
                "domain": self.domain,
                "state_hex": self._state.hex(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def hexdigest(self) -> str:
        return hashlib.sha256(
            _frame("root", self.domain)
            + self.count.to_bytes(8, "big")
            + self._state
        ).hexdigest()
