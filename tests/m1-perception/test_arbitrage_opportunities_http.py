from __future__ import annotations

from dataclasses import dataclass

from polyarb.routing.opportunity_scanner import (
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
)


@dataclass
class _Opportunity:
    group_id: str = "group-1"

    def to_dict(self) -> dict:
        return {"group_id": self.group_id, "gross_edge_bps": 250.0}


def test_opportunity_endpoint_returns_explicit_gross_basis(http_test_client, monkeypatch) -> None:
    monkeypatch.setattr(
        "polyarb.http.arbitrage.scan_neg_risk_quote_run",
        lambda *_args, **_kwargs: [_Opportunity()],
    )

    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=100")

    assert response.status_code == 200
    assert response.json() == {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "known-universe",
        "quote_sla_seconds": 300,
        "universe_sla_seconds": 50400,
        "count": 1,
        "opportunities": [{"group_id": "group-1", "gross_edge_bps": 250.0}],
    }


def test_opportunity_endpoint_rejects_non_finite_threshold(http_test_client) -> None:
    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=nan")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid numeric query"}


def test_opportunity_endpoint_returns_bounded_503_for_quote_run_preconditions(
    http_test_client, monkeypatch
) -> None:
    cases = [
        (QuoteRunUnavailableError("quote run unavailable"), "quote run unavailable"),
        (StaleQuoteRunError("quote age 300.1s exceeds 300.0s"), "quote age 300.1s exceeds 300.0s"),
        (
            StaleUniverseError("universe age 50400.1s exceeds 50400.0s"),
            "universe age 50400.1s exceeds 50400.0s",
        ),
    ]
    for error, expected in cases:
        monkeypatch.setattr(
            "polyarb.http.arbitrage.scan_neg_risk_quote_run",
            lambda *_args, error=error, **_kwargs: (_ for _ in ()).throw(error),
        )

        response = http_test_client.get("/arbitrage/opportunities")

        assert response.status_code == 503
        assert response.json() == {"error": expected}
