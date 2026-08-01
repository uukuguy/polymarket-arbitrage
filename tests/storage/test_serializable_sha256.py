from __future__ import annotations

import hashlib
import json
import random

import pytest

from polyarb.storage.serializable_sha256 import SerializableSHA256


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"abc",
        b"a" * 1_000_000,
        bytes(range(256)),
    ],
)
def test_serializable_sha256_matches_hashlib_vectors(payload: bytes) -> None:
    digest = SerializableSHA256.new()
    digest.update(payload)
    assert digest.hexdigest() == hashlib.sha256(payload).hexdigest()


def test_serializable_sha256_resumes_at_every_tail_boundary() -> None:
    payload = bytes(range(130))
    expected = hashlib.sha256(payload).hexdigest()
    for split in range(len(payload) + 1):
        digest = SerializableSHA256.new()
        digest.update(payload[:split])
        reopened = SerializableSHA256.from_json(digest.to_json())
        reopened.update(payload[split:])
        assert reopened.hexdigest() == expected
        assert len(bytes.fromhex(json.loads(reopened.to_json())["tail_hex"])) <= 63


def test_serializable_sha256_random_chunk_partitions_match_hashlib() -> None:
    randomizer = random.Random(20260801)
    for size in (0, 1, 55, 56, 63, 64, 65, 127, 128, 129, 1024):
        payload = randomizer.randbytes(size)
        digest = SerializableSHA256.new()
        position = 0
        while position < size:
            width = randomizer.randint(1, 31)
            digest.update(payload[position : position + width])
            position += width
            digest = SerializableSHA256.from_json(digest.to_json())
        assert digest.hexdigest() == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    "state",
    [
        "not-json",
        "{}",
        '{"words":[],"byte_count":0,"tail_hex":""}',
        '{"words":[0,0,0,0,0,0,0,0],"byte_count":-1,"tail_hex":""}',
        '{"words":[0,0,0,0,0,0,0,4294967296],"byte_count":0,"tail_hex":""}',
        '{"words":[0,0,0,0,0,0,0,0],"byte_count":1,"tail_hex":""}',
        '{"words":[0,0,0,0,0,0,0,0],"byte_count":64,"tail_hex":"00"}',
        '{"words":[0,0,0,0,0,0,0,0],"byte_count":64,"tail_hex":"zz"}',
        '{"words":[0,0,0,0,0,0,0,0],"byte_count":2305843009213693952,'
        '"tail_hex":""}',
    ],
)
def test_serializable_sha256_rejects_malformed_state(state: str) -> None:
    with pytest.raises(ValueError, match="invalid-serializable-sha256-state"):
        SerializableSHA256.from_json(state)


def test_hexdigest_does_not_consume_resumable_state() -> None:
    digest = SerializableSHA256.new()
    digest.update(b"prefix")
    assert digest.hexdigest() == hashlib.sha256(b"prefix").hexdigest()
    digest.update(b"-suffix")
    assert digest.hexdigest() == hashlib.sha256(b"prefix-suffix").hexdigest()
