"""Secret-free retry failure identity contracts."""

from polyarb.control_plane.failure_identity import retry_failure_fingerprint


def _failure_at_site_a(message: str) -> ValueError:
    try:
        raise ValueError(message)
    except ValueError as error:
        return error


def _failure_at_site_b(message: str) -> ValueError:
    try:
        raise ValueError(message)
    except ValueError as error:
        return error


def test_failure_fingerprint_is_stable_by_type_and_code_site_not_message() -> None:
    first = _failure_at_site_a("first secret")
    replay = _failure_at_site_a("second secret")
    other_site = _failure_at_site_b("first secret")

    first_fingerprint = retry_failure_fingerprint(first, component="structure-fetch")
    assert first_fingerprint == retry_failure_fingerprint(replay, component="structure-fetch")
    assert first_fingerprint != retry_failure_fingerprint(other_site, component="structure-fetch")
    assert first_fingerprint.startswith("sha256:")
    assert "secret" not in first_fingerprint
