from __future__ import annotations

import pytest

from polyarb.routing.opportunity_diagnosis import diagnose_opportunity_feed

VALID_ZERO_BODY = (
    '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees",'
    '"coverage":"known-universe","quote_sla_seconds":300,'
    '"universe_sla_seconds":50400,'
    '"count":0,"opportunities":[]}'
)


def test_200_zero_is_the_only_zero_opportunity_result() -> None:
    result = diagnose_opportunity_feed(200, VALID_ZERO_BODY)

    assert (result.kind, result.count, result.exit_code) == (
        "available-zero",
        0,
        0,
    )


def test_200_nonzero_is_available_with_safe_metadata() -> None:
    result = diagnose_opportunity_feed(
        200,
        '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees",'
        '"coverage":"known-universe","quote_sla_seconds":300,'
        '"universe_sla_seconds":50400,'
        '"count":1,"opportunities":[{"group_id":"g1"}]}',
    )

    assert (result.kind, result.count, result.strategy, result.profit_basis, result.exit_code) == (
        "available-opportunities",
        1,
        "neg-risk-buy-all",
        "gross-before-fees",
        0,
    )


def test_503_snapshot_age_is_stale_not_zero() -> None:
    result = diagnose_opportunity_feed(503, '{"error":"snapshot age 1216.9s exceeds 900.0s"}')

    assert result.kind == "stale-snapshot"
    assert result.snapshot_age_seconds == 1216.9
    assert result.max_snapshot_age_seconds == 900.0
    assert result.age_seconds == 1216.9
    assert result.max_age_seconds == 900.0
    assert result.exit_code == 2


def test_503_quote_and_universe_ages_are_bounded_stale_kinds() -> None:
    quote = diagnose_opportunity_feed(503, '{"error":"quote age 300.1s exceeds 300.0s"}')
    universe = diagnose_opportunity_feed(503, '{"error":"universe age 50400.1s exceeds 50400.0s"}')

    assert (quote.kind, quote.reason, quote.quote_age_seconds, quote.max_quote_age_seconds) == (
        "stale-quote-run",
        "quote-age-exceeded",
        300.1,
        300.0,
    )
    assert (
        universe.kind,
        universe.reason,
        universe.universe_age_seconds,
        universe.max_universe_age_seconds,
    ) == (
        "stale-universe",
        "universe-age-exceeded",
        50400.1,
        50400.0,
    )
    assert quote.age_seconds == 300.1
    assert universe.max_age_seconds == 50400.0


def test_unrelated_503_is_unavailable() -> None:
    result = diagnose_opportunity_feed(503, '{"error":"upstream unavailable"}')

    assert (result.kind, result.count, result.exit_code) == (
        "feed-unavailable",
        None,
        2,
    )


@pytest.mark.parametrize(
    "body, expected_reason",
    [
        ("not-json", "invalid-json"),
        (
            '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees","opportunities":[]}',
            "invalid-schema",
        ),
        (VALID_ZERO_BODY.replace('"count":0,', '"count":"0",'), "invalid-schema"),
        (VALID_ZERO_BODY.replace('"count":0,', '"count":-1,'), "invalid-schema"),
        (VALID_ZERO_BODY.replace('"count":0,', '"count":true,'), "invalid-schema"),
        (VALID_ZERO_BODY.replace('"opportunities":[]', '"opportunities":{}'), "invalid-schema"),
        (VALID_ZERO_BODY.replace('"count":0', '"count":1'), "invalid-schema"),
        (
            VALID_ZERO_BODY.replace('"strategy":"neg-risk-buy-all"', '"strategy":""'),
            "invalid-schema",
        ),
        (
            VALID_ZERO_BODY.replace('"profit_basis":"gross-before-fees"', '"profit_basis":""'),
            "invalid-schema",
        ),
    ],
)
def test_200_invalid_payload_is_not_a_zero_result(body: str, expected_reason: str) -> None:
    result = diagnose_opportunity_feed(200, body)

    assert (result.kind, result.count, result.reason, result.exit_code) == (
        "invalid-response",
        None,
        expected_reason,
        2,
    )


@pytest.mark.parametrize(
    "body",
    [
        VALID_ZERO_BODY.replace('"coverage":"known-universe",', ""),
        VALID_ZERO_BODY.replace('"coverage":"known-universe"', '"coverage":"snapshot"'),
        VALID_ZERO_BODY.replace('"quote_sla_seconds":300', '"quote_sla_seconds":"300"'),
        VALID_ZERO_BODY.replace('"quote_sla_seconds":300', '"quote_sla_seconds":301'),
        VALID_ZERO_BODY.replace('"universe_sla_seconds":50400', '"universe_sla_seconds":"50400"'),
        VALID_ZERO_BODY.replace('"universe_sla_seconds":50400', '"universe_sla_seconds":50401'),
    ],
)
def test_200_requires_fixed_known_universe_coverage_and_slas(body: str) -> None:
    result = diagnose_opportunity_feed(200, body)

    assert (result.kind, result.reason, result.exit_code) == (
        "invalid-response",
        "invalid-schema",
        2,
    )


def test_non_503_non_2xx_is_unavailable() -> None:
    result = diagnose_opportunity_feed(502, '{"error":"database /secret/path"}')

    assert (result.kind, result.reason, result.exit_code) == (
        "feed-unavailable",
        "non-success-status",
        2,
    )


def test_diagnostic_output_omits_none_values_and_server_error_text() -> None:
    result = diagnose_opportunity_feed(503, '{"error":"private server trace"}')

    assert result.to_dict() == {
        "kind": "feed-unavailable",
        "http_status": 503,
        "reason": "non-success-status",
    }


def test_reasons_are_limited_to_operator_safe_vocabulary() -> None:
    diagnostics = [
        diagnose_opportunity_feed(200, VALID_ZERO_BODY),
        diagnose_opportunity_feed(
            200,
            '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees",'
            '"coverage":"known-universe","quote_sla_seconds":300,'
            '"universe_sla_seconds":50400,'
            '"count":1,"opportunities":[{}]}',
        ),
        diagnose_opportunity_feed(503, '{"error":"snapshot age 1200s exceeds 900s"}'),
        diagnose_opportunity_feed(503, '{"error":"unbounded server details"}'),
        diagnose_opportunity_feed(200, "not-json"),
        diagnose_opportunity_feed(200, '{"count":0}'),
    ]

    assert {diagnostic.reason for diagnostic in diagnostics} <= {
        "valid-empty-feed",
        "valid-feed",
        "snapshot-age-exceeded",
        "quote-age-exceeded",
        "universe-age-exceeded",
        "non-success-status",
        "invalid-json",
        "invalid-schema",
    }
