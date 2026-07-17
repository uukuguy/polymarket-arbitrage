from __future__ import annotations

from dataclasses import dataclass


@dataclass
class _Opportunity:
    group_id: str = "group-1"

    def to_dict(self) -> dict:
        return {"group_id": self.group_id, "gross_edge_bps": 250.0}


def test_opportunity_endpoint_returns_explicit_gross_basis(
    http_test_client, monkeypatch
) -> None:
    monkeypatch.setattr(
        "polyarb.http.arbitrage.scan_neg_risk_buy_all",
        lambda *_args, **_kwargs: [_Opportunity()],
    )

    response = http_test_client.get(
        "/arbitrage/opportunities?min_edge_bps=100"
    )

    assert response.status_code == 200
    assert response.json() == {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "count": 1,
        "opportunities": [{"group_id": "group-1", "gross_edge_bps": 250.0}],
    }


def test_opportunity_endpoint_rejects_non_finite_threshold(http_test_client) -> None:
    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=nan")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid numeric query"}
