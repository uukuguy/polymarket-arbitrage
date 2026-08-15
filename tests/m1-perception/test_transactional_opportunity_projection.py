import hashlib
from datetime import UTC, datetime

from polyarb.control_plane.models import QuoteBatchLeg, QuoteBatchReceipt
from polyarb.control_plane.opportunity_projection import build_opportunity_rows
from polyarb.control_plane.opportunity_worker import TransactionalOpportunityCertifier


def test_complete_authenticated_quote_group_projects_only_positive_buy_all_edge() -> None:
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    rows = build_opportunity_rows(
        legs=legs,
        quotes=(
            {
                "yes_token_id": "token-a", "terminal_state": "executable",
                "best_ask_price": 0.4, "best_ask_size": 4,
            },
            {
                "yes_token_id": "token-b", "terminal_state": "executable",
                "best_ask_price": 0.5, "best_ask_size": 3,
            },
        ),
        structure_observed_at_ms=1,
        quote_started_at_ms=2,
        quote_quoted_at_ms=3,
    )

    assert rows == ({
        "group_id": "group-a", "event_id": "event-a", "membership_hash": "m-a",
        "bundle_cost": 0.9, "gross_edge_bps": 1000.0, "max_bundle_size": 3.0,
        "legs": [
            {
                "market_id": "market-a", "condition_id": "condition-a", "slug": "a",
                "yes_token_id": "token-a", "ask_price": 0.4, "ask_size": 4.0,
            },
            {
                "market_id": "market-b", "condition_id": "condition-b", "slug": "b",
                "yes_token_id": "token-b", "ask_price": 0.5, "ask_size": 3.0,
            },
        ],
        "structure_observed_at_ms": 1, "quote_started_at_ms": 2, "quote_quoted_at_ms": 3,
    },)


def test_certifier_authenticates_r2_quote_payload_before_atomic_publish() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    payload = (
        b'{"structure_receipt_digest":"' + b"a" * 64 + b'","token_range_digest":"'
        + b"b" * 64 + b'","universe_hash":"' + b"c" * 64 + b'"}\n'
        + (
            b'{"best_ask_price":0.4,"best_ask_size":4,'
            b'"terminal_state":"executable","yes_token_id":"token-a"}\n'
        )
        + (
            b'{"best_ask_price":0.5,"best_ask_size":3,'
            b'"terminal_state":"executable","yes_token_id":"token-b"}\n'
        )
    )

    class ControlPlane:
        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                "job", "d" * 64, "quotes/key", hashlib.sha256(payload).hexdigest(), 2
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, ((legs, receipt, quoted_at),)

        def publish_opportunity_projection(self, **kwargs):
            self.published = kwargs
            return "e" * 64

    class Client:
        def get_object(self, **kwargs):
            assert kwargs == {"Bucket": "bucket", "Key": "quotes/key"}
            return {"Body": type("Body", (), {"read": lambda self: payload})()}

    control_plane = ControlPlane()
    assert TransactionalOpportunityCertifier(
        control_plane=control_plane, object_client=Client(), bucket="bucket", now=lambda: quoted_at
    ).run_once() == "e" * 64
    assert control_plane.published["rows"][0]["gross_edge_bps"] == 1000.0
