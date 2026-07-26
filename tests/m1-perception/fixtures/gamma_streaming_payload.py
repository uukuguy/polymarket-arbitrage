"""Realistic synthetic Gamma payload generator for D-23 memory regression tests.

Plan 02-09 (Wave 3.5) — feedback_profile-with-real-data-2026-05 mandates that
synthetic dicts approximate real Polymarket payload shapes. Naive fixtures like
``{"id": "x", "question": "y"}`` underestimate per-dict size by ~5x and would
make the memory regression test pass falsely.

Real-shape fields used (subset of Polymarket /markets response):
    id              — 6-digit number as string
    conditionId     — 64-char hex
    slug            — 30-80 char URL-safe
    question        — 60-200 char English
    clobTokenIds    — JSON-encoded list of TWO 77-digit uint256 strings
                      (the field that exploded memory pre-Plan-02-04)
    outcomePrices   — JSON-encoded list of TWO float strings
    liquidity*      — numeric strings + Num doubles
    volume*         — numeric strings + Num doubles
    endDate         — ISO 8601 timestamps
    active/closed/negRisk — booleans
    negRiskMarketID — 64-char hex string or null

Liquidity distribution (B-2 fix): log-normal with mu=log(500), sigma=2.
This produces median $500, ~35% > $1k, ~12% > $10k, ~3% > $50k — matches
the rough shape of real Polymarket where most active markets have low-but-
nonzero liquidity and a long right tail.
"""

from __future__ import annotations

import json
import math
from random import Random

# Log-normal parameters chosen so P(X > $1k) ≈ 36% (close to T5 expectation).
_LIQ_MU = math.log(500.0)
_LIQ_SIGMA = 2.0


def _sample_liquidity(rnd: Random) -> float:
    """Draw a liquidity value from a log-normal distribution."""
    return math.exp(rnd.gauss(_LIQ_MU, _LIQ_SIGMA))


def make_realistic_market(idx: int, rnd: Random | None = None) -> dict:
    """Build one Polymarket-shaped /markets dict.

    Deterministic given idx (the Random is seeded with idx) so tests are
    reproducible. Size ~5-10KB pre-strip — when the paginator applies
    ``_MARKET_KEEP`` the stripped dict is ~2KB.
    """
    rnd = rnd or Random(idx)
    token_yes = "".join(str(rnd.randint(0, 9)) for _ in range(77))
    token_no = "".join(str(rnd.randint(0, 9)) for _ in range(77))

    subjects = [
        "Trump",
        "Biden",
        "Bitcoin",
        "Ethereum",
        "Apple",
        "OpenAI",
        "Nvidia",
        "Elon",
        "the Fed",
        "Russia",
        "Ukraine",
        "Israel",
        "Iran",
        "China",
        "Tesla",
        "SpaceX",
    ]
    verbs = ["reach", "hit", "exceed", "fall below", "announce"]
    objects = ["$100k", "$150k", "100,000 users", "5% inflation"]
    suffixes = ["by 2026?", "by end of year?", "this quarter?", "before May 31?"]

    parts = [
        rnd.choice(["Will", "Did", "Can", "Has"]),
        *[rnd.choice(subjects) for _ in range(rnd.randint(2, 4))],
        rnd.choice(verbs),
        rnd.choice(objects),
        rnd.choice(suffixes),
    ]
    question = " ".join(parts)
    slug = "-".join(parts[:5]).lower().replace("$", "").replace("?", "")[:60]
    liq = _sample_liquidity(rnd)
    vol = liq * rnd.uniform(0.5, 50.0)
    return {
        "id": str(500000 + idx),
        "conditionId": "0x" + "".join(rnd.choice("0123456789abcdef") for _ in range(64)),
        "slug": slug,
        "question": question,
        "clobTokenIds": json.dumps([token_yes, token_no]),
        "outcomePrices": json.dumps(
            [f"{rnd.uniform(0.05, 0.95):.4f}", f"{1 - rnd.uniform(0.05, 0.95):.4f}"]
        ),
        "active": True,
        "closed": False,
        "negRisk": rnd.random() < 0.3,
        "negRiskMarketID": (
            "0x" + "".join(rnd.choice("0123456789abcdef") for _ in range(64))
            if rnd.random() < 0.3
            else None
        ),
        "liquidity": f"{liq:.2f}",
        "liquidityNum": liq,
        "volume": f"{vol:.2f}",
        "volumeNum": vol,
        "endDate": "2026-06-30T23:59:59Z",
        "end_date_iso": "2026-06-30T23:59:59Z",
    }


def make_realistic_event(idx: int, rnd: Random | None = None) -> dict:
    """Build one Polymarket-shaped /events dict (small — events stays
    materialized per Decision A; size mostly drives event_tags volume)."""
    rnd = rnd or Random(idx)
    return {
        "id": str(10000 + idx),
        "slug": f"event-{idx}",
        "title": f"Event {idx} headline",
        "ticker": f"EV{idx:05d}",
        "active": True,
        "closed": False,
        "liquidity": f"{rnd.uniform(1000, 100000):.2f}",
        "liquidityNum": rnd.uniform(1000, 100000),
        "volume": f"{rnd.uniform(1000, 1_000_000):.2f}",
        "volumeNum": rnd.uniform(1000, 1_000_000),
        "endDate": "2026-06-30T23:59:59Z",
        "tags": [{"id": str(rnd.randint(1, 100)), "label": "Politics", "slug": "politics"}],
        "markets": [
            {"id": str(500000 + idx * 2)},
            {"id": str(500000 + idx * 2 + 1)},
        ],
    }
