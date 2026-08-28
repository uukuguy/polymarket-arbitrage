"""Runtime-image contract for isolated transactional migrations."""

from pathlib import Path


def test_runtime_image_contains_alembic_configuration_and_revisions() -> None:
    dockerfile = Path("Dockerfile").read_text()

    assert "COPY --chown=polyarb:polyarb alembic.ini /app/alembic.ini" in dockerfile
    assert "COPY --chown=polyarb:polyarb alembic/ /app/alembic/" in dockerfile


def test_runtime_image_keeps_dependency_layer_independent_of_application_source() -> None:
    dockerfile = Path("Dockerfile").read_text()

    assert dockerfile.count("uv sync --locked") == 1
    assert "uv sync --locked --no-install-project --no-editable" in dockerfile


def test_runtime_image_bounds_and_authenticates_supercronic_download() -> None:
    dockerfile = Path("Dockerfile").read_text()

    assert "ARG SUPERCRONIC_VERSION=v0.2.30" in dockerfile
    assert (
        "ARG SUPERCRONIC_SHA256="
        "55f3a65b6ef29856d948230a96448f6ec7376d39fca367fae49d2512167e29e5" in dockerfile
    )
    assert "timeout --signal=TERM --kill-after=5s 500s" in dockerfile
    assert "--connect-timeout 15" in dockerfile
    assert "--max-time 240" in dockerfile
    assert "--retry 1" in dockerfile
    assert "--retry-all-errors" in dockerfile
    assert 'echo "${SUPERCRONIC_SHA256}  /usr/local/bin/supercronic" | sha256sum -c -' in dockerfile
