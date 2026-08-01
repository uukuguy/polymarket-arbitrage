"""Small, portable SHA-256 state for bounded durable hashing.

This implements only the FIPS 180-4 state needed to pause and resume the
canonical Structure comparison byte stream.  The serialized form is data, not
an implementation-specific hashlib/OpenSSL object.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

_MASK = 0xFFFFFFFF
_MAX_BYTE_COUNT = ((1 << 64) - 1) // 8
_INITIAL = (
    0x6A09E667,
    0xBB67AE85,
    0x3C6EF372,
    0xA54FF53A,
    0x510E527F,
    0x9B05688C,
    0x1F83D9AB,
    0x5BE0CD19,
)
_ROUND_CONSTANTS = (
    0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5,
    0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
    0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
    0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
    0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC,
    0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
    0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7,
    0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
    0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
    0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
    0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3,
    0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
    0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5,
    0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
    0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
    0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
)


def _rotate_right(value: int, width: int) -> int:
    return ((value >> width) | (value << (32 - width))) & _MASK


def _compress(words: tuple[int, ...], block: bytes) -> tuple[int, ...]:
    schedule = [int.from_bytes(block[index : index + 4], "big") for index in range(0, 64, 4)]
    for index in range(16, 64):
        before = schedule[index - 15]
        near = schedule[index - 2]
        sigma0 = _rotate_right(before, 7) ^ _rotate_right(before, 18) ^ (before >> 3)
        sigma1 = _rotate_right(near, 17) ^ _rotate_right(near, 19) ^ (near >> 10)
        schedule.append(
            (schedule[index - 16] + sigma0 + schedule[index - 7] + sigma1) & _MASK
        )
    a, b, c, d, e, f, g, h = words
    for constant, item in zip(_ROUND_CONSTANTS, schedule, strict=True):
        choice = (e & f) ^ ((~e) & g)
        majority = (a & b) ^ (a & c) ^ (b & c)
        upper0 = _rotate_right(a, 2) ^ _rotate_right(a, 13) ^ _rotate_right(a, 22)
        upper1 = _rotate_right(e, 6) ^ _rotate_right(e, 11) ^ _rotate_right(e, 25)
        first = (h + upper1 + choice + constant + item) & _MASK
        second = (upper0 + majority) & _MASK
        h, g, f, e, d, c, b, a = g, f, e, (d + first) & _MASK, c, b, a, (first + second) & _MASK
    return tuple(
        (old + new) & _MASK
        for old, new in zip(words, (a, b, c, d, e, f, g, h), strict=True)
    )


@dataclass
class SerializableSHA256:
    _words: tuple[int, ...]
    _byte_count: int
    _tail: bytes

    @classmethod
    def new(cls) -> SerializableSHA256:
        return cls(_INITIAL, 0, b"")

    @classmethod
    def from_json(cls, encoded: str) -> SerializableSHA256:
        try:
            raw = json.loads(encoded)
            words = tuple(raw["words"])
            byte_count = raw["byte_count"]
            tail_hex = raw["tail_hex"]
            tail = bytes.fromhex(tail_hex)
            valid = (
                isinstance(raw, dict)
                and set(raw) == {"words", "byte_count", "tail_hex"}
                and len(words) == 8
                and all(type(word) is int and 0 <= word <= _MASK for word in words)
                and type(byte_count) is int
                and 0 <= byte_count <= _MAX_BYTE_COUNT
                and isinstance(tail_hex, str)
                and len(tail) <= 63
                and len(tail) == byte_count % 64
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            valid = False
        if not valid:
            raise ValueError("invalid-serializable-sha256-state")
        return cls(words, byte_count, tail)

    def to_json(self) -> str:
        return json.dumps(
            {
                "words": list(self._words),
                "byte_count": self._byte_count,
                "tail_hex": self._tail.hex(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def update(self, payload: bytes) -> None:
        if not isinstance(payload, bytes):
            raise TypeError("serializable-sha256-update-requires-bytes")
        if len(payload) > _MAX_BYTE_COUNT - self._byte_count:
            raise ValueError("serializable-sha256-input-too-long")
        self._byte_count += len(payload)
        buffered = self._tail + payload
        offset = 0
        while len(buffered) - offset >= 64:
            self._words = _compress(self._words, buffered[offset : offset + 64])
            offset += 64
        self._tail = buffered[offset:]

    def hexdigest(self) -> str:
        words = self._words
        final = self._tail + b"\x80"
        final += b"\x00" * ((56 - len(final) % 64) % 64)
        final += (self._byte_count * 8).to_bytes(8, "big")
        for offset in range(0, len(final), 64):
            words = _compress(words, final[offset : offset + 64])
        return "".join(word.to_bytes(4, "big").hex() for word in words)
