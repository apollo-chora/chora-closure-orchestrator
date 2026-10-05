# syntax=docker/dockerfile:1.6
#
# Multi-stage build for chora-closure-orchestrator (Python LangGraph).
# Build context = this repository. Cloud-neutral: PostgreSQL for
# persistence, NATS JetStream for events, MinIO for the cold archive,
# env-backed secrets (CHORA_LOCAL_KEK / DSNs).
#
# Per Tier 2 D5 hybrid kernel mandate: this is the Python LangGraph half
# of the federated closure saga (Tier 3 D11).

ARG PY_VERSION=3.13
ARG SERVICE_NAME=chora-closure-orchestrator
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown

############################
# Stage 1 — build
############################
FROM python:${PY_VERSION}-slim AS builder

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /src

# Project metadata first for layer caching. The COPY paths below are
# relative to the REPO-ROOT build context.
COPY pyproject.toml ./pyproject.toml
COPY src ./src

# Build wheel + install into a virtualenv. The vendored chora_contracts_gen
# Protobuf stubs (src/chora_contracts_gen) are part of the wheel — no
# external contracts install is required.
RUN python -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install .

############################
# Stage 2 — runtime
############################
#
# Runtime base is python:3.13-slim (NOT distroless/python3-debian12:nonroot)
# because pyproject.toml pins requires-python = ">=3.13" and the binary
# wheels in /venv/lib/python3.13/site-packages (e.g. pydantic_core) carry a
# cpython-313 ABI tag that is incompatible with the Python 3.11 interpreter
# shipped in the current distroless python3-debian12 tag. Until a
# distroless python3.13 tag ships we trade ~80 MB of image size for a
# working runtime. The user (nonroot, uid 65532) + read-only-rootfs (from
# the container runtime SecurityContext) keep the security posture
# parity-close to distroless.
FROM python:${PY_VERSION}-slim AS runtime

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.source="https://github.com/apollo-chora/chora-closure-orchestrator" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.vendor="Chora Platform" \
      org.opencontainers.image.licenses="UNLICENSED" \
      io.chora.service="${SERVICE_NAME}" \
      io.chora.runtime="python" \
      io.chora.git-sha="${GIT_SHA}" \
      io.chora.build-time="${BUILD_TIME}"

# Create nonroot user with uid 65532 matching distroless-nonroot semantics
# + the container SecurityContext (runAsUser: 65532). Slim images ship
# without a nonroot user by default.
RUN groupadd --system --gid 65532 nonroot \
    && useradd  --system --uid 65532 --gid 65532 --no-create-home nonroot \
    && mkdir -p /app \
    && chown -R nonroot:nonroot /app

WORKDIR /app

# Bring the venv with installed dependencies + sources.
COPY --from=builder --chown=nonroot:nonroot /venv /venv
COPY --from=builder --chown=nonroot:nonroot /src/src /app/src

ENV PATH="/venv/bin:${PATH}" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    SERVICE_NAME=${SERVICE_NAME} \
    GIT_SHA=${GIT_SHA} \
    BUILD_TIME=${BUILD_TIME}

EXPOSE 8080

USER nonroot:nonroot
ENTRYPOINT ["/venv/bin/python", "-m", "uvicorn", "chora_closure_orchestrator.main:app", "--host", "0.0.0.0", "--port", "8080"]
