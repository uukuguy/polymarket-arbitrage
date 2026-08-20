import hashlib
from datetime import UTC, datetime

from polyarb.control_plane.models import QuoteBatchLeg, QuoteBatchReceipt, QuoteBatchSpec
from polyarb.control_plane.opportunity_projection import build_opportunity_rows
from polyarb.control_plane.opportunity_worker import TransactionalOpportunityCertifier
from polyarb.control_plane.postgres import OpportunityProjectionCurrentError
from polyarb.control_plane.quote_artifact import QuoteBatchInputArtifact


def test_complete_authenticated_quote_group_projects_only_positive_buy_all_edge() -> None:
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    rows = build_opportunity_rows(
        legs=legs,
        quotes=(
            {
                "yes_token_id": "token-a",
                "terminal_state": "executable",
                "best_ask_price": 0.4,
                "best_ask_size": 4,
            },
            {
                "yes_token_id": "token-b",
                "terminal_state": "executable",
                "best_ask_price": 0.5,
                "best_ask_size": 3,
            },
        ),
        structure_observed_at_ms=1,
        quote_started_at_ms=2,
        quote_quoted_at_ms=3,
    )

    assert rows == (
        {
            "group_id": "group-a",
            "event_id": "event-a",
            "membership_hash": "m-a",
            "bundle_cost": 0.9,
            "gross_edge_bps": 1000.0,
            "max_bundle_size": 3.0,
            "legs": [
                {
                    "market_id": "market-a",
                    "condition_id": "condition-a",
                    "slug": "a",
                    "yes_token_id": "token-a",
                    "ask_price": 0.4,
                    "ask_size": 4.0,
                },
                {
                    "market_id": "market-b",
                    "condition_id": "condition-b",
                    "slug": "b",
                    "yes_token_id": "token-b",
                    "ask_price": 0.5,
                    "ask_size": 3.0,
                },
            ],
            "structure_observed_at_ms": 1,
            "quote_started_at_ms": 2,
            "quote_quoted_at_ms": 3,
        },
    )


def test_certifier_authenticates_r2_quote_payload_before_atomic_publish() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + b"b" * 64
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
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
        def claim_job(self, **kwargs):
            self.claim = kwargs
            return type("Lease", (), {"job_key": "quote:job:opportunity-certify"})()

        def finish(self, *args, **kwargs):
            self.finished = kwargs

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
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane, object_client=Client(), bucket="bucket", now=lambda: quoted_at
    ).run_once()
    assert result.job_key == "quote:job:opportunity-certify"
    assert result.outcome == "certified:" + "e" * 64
    assert control_plane.published["rows"][0]["gross_edge_bps"] == 1000.0


def test_certifier_skips_r2_when_current_quote_is_already_projected() -> None:
    class ControlPlane:
        def claim_job(self, **kwargs):
            return type("Lease", (), {"job_key": "quote:job:opportunity-certify"})()

        def finish(self, *args, **kwargs):
            self.finished = kwargs

        def current_quote_projection_inputs(self):
            raise OpportunityProjectionCurrentError("already projected")

    class Client:
        def get_object(self, **kwargs):
            raise AssertionError(f"R2 must not be read: {kwargs}")

    result = TransactionalOpportunityCertifier(
        control_plane=ControlPlane(),
        object_client=Client(),
        bucket="bucket",
        now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
    ).run_once()

    assert result.outcome == "current"


def test_certifier_uses_fenced_r2_input_when_postgres_legs_are_compacted() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    batch = QuoteBatchSpec.from_legs(
        structure_receipt_digest="a" * 64,
        universe_hash="c" * 64,
        ordinal=0,
        legs=legs,
    )
    input_artifact = QuoteBatchInputArtifact.from_spec(batch)
    quote_payload = (
        b'{"structure_receipt_digest":"' + b"a" * 64
        + b'","token_range_digest":"' + batch.token_range_digest.encode()
        + b'","universe_hash":"' + b"c" * 64 + b'"}\n'
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
        def claim_job(self, **kwargs):
            return type("Lease", (), {"job_key": "quote:job:opportunity-certify"})()

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                batch.job_key,
                "d" * 64,
                "quotes/key",
                hashlib.sha256(quote_payload).hexdigest(),
                2,
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, (((), receipt, quoted_at),)

        def quote_batch_input_reference(self, job_key):
            assert job_key == batch.job_key
            return input_artifact.key, input_artifact.sha256, len(batch.legs)

        def publish_opportunity_projection(self, **kwargs):
            self.published = kwargs
            return "e" * 64

        def finish(self, *args, **kwargs):
            pass

    class Client:
        def get_object(self, **kwargs):
            payloads = {input_artifact.key: input_artifact.payload, "quotes/key": quote_payload}
            return {"Body": type("Body", (), {"read": lambda self: payloads[kwargs["Key"]]})()}

    control_plane = ControlPlane()
    TransactionalOpportunityCertifier(
        control_plane=control_plane, object_client=Client(), bucket="bucket", now=lambda: quoted_at
    ).run_once()

    assert control_plane.published["rows"][0]["gross_edge_bps"] == 1000.0
