# Phase 02 production daemon — multi-stage uv build (RESEARCH §6)
# Source: docs.astral.sh/uv/guides/integration/docker/ (verified 2026-05-12)

# ───── Builder stage ─────────────────────────────────────────────────
# The formal Fly control-plane Machines are linux/amd64.  Pinning both stages
# prevents an Apple-Silicon local build or a heterogeneous remote builder from
# publishing an arm64 image that Fly cannot roll onto those Machines.
FROM --platform=linux/amd64 python:3.12-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5.0 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_DEV=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable
COPY pyproject.toml uv.lock README.md ./

# ───── Runtime stage ─────────────────────────────────────────────────
FROM --platform=linux/amd64 python:3.12-slim-bookworm AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Supercronic — cron with full cron syntax (W8 RESOLVED — see RESEARCH Open Q #4)
ARG SUPERCRONIC_VERSION=v0.2.30
ARG SUPERCRONIC_SHA256=55f3a65b6ef29856d948230a96448f6ec7376d39fca367fae49d2512167e29e5
# 12,432,517 bytes at a 64 KiB/s floor is ~190s. The 240s transfer budget
# adds TLS/redirect/filesystem margin; 500s owns two attempts plus teardown.
RUN timeout --signal=TERM --kill-after=5s 500s curl -fsSL \
        --connect-timeout 15 \
        --max-time 240 \
        --retry 1 \
        --retry-all-errors \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
        -o /usr/local/bin/supercronic \
    && echo "${SUPERCRONIC_SHA256}  /usr/local/bin/supercronic" | sha256sum -c - \
    && chmod +x /usr/local/bin/supercronic

# Non-root user (UID 10001 follows ASVS V14.1.1 — distroless-style numeric UID)
RUN groupadd --system --gid 10001 polyarb \
    && useradd --system --uid 10001 --gid polyarb --no-create-home polyarb
COPY --from=builder --chown=polyarb:polyarb /app/.venv /app/.venv
COPY --chown=polyarb:polyarb src/ /app/src/
# Transactional control-plane migrations run inside an isolated Fly worker;
# retain the Alembic config and immutable revision chain in that runtime image.
COPY --chown=polyarb:polyarb alembic.ini /app/alembic.ini
COPY --chown=polyarb:polyarb alembic/ /app/alembic/
RUN mkdir -p /data /app/logs && chown -R polyarb:polyarb /data /app/logs

# Copy crontab for Supercronic process group
COPY --chown=polyarb:polyarb crontab /app/crontab
COPY --chown=polyarb:polyarb scripts/polywatch/healthz_watcher.py /app/scripts/polywatch/healthz_watcher.py

# NOTE: POLYARB_ALLOW_EXTERNAL_PATHS=1 is REQUIRED for /data abs path acceptance by
# config.py Settings._within_project validator (PATTERNS §5.1 gotcha #2).
# This is a documented prod-only escape hatch; tests gate it via fixture override.
#
# POLYARB_HTTP_PORT=8080 overrides the default 19080 so the daemon listens on
# the standard container port. Fly internal_port and HEALTHCHECK match this.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1 \
    POLYARB_DATA_DIR=/data \
    POLYARB_LOG_DIR=/app/logs \
    POLYARB_ALLOW_EXTERNAL_PATHS=1 \
    POLYARB_HTTP_PORT=8080 \
    TZ=UTC
WORKDIR /app
USER polyarb
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:8080/health || exit 1
EXPOSE 8080
CMD ["python", "-m", "polyarb.daemon.main"]
