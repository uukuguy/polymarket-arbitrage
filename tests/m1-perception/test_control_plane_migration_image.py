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
