"""Env-backed DSN resolver for chora-closure-orchestrator.

Cloud-neutral secret resolution: DSNs come from the environment (compose
injected them, typically from Docker secrets or a ``.env`` file). The
Secret Manager indirection is gone — the ``*_SECRET_ID`` env vars are
retained as aliases that carry the DSN plaintext directly, so existing
deployment env shapes keep working without a credentials service.

The orchestrator needs two distinct DSNs, each resolved by the same
direct-env-then-secret-alias rule (``_resolve_dsn`` below):

- **chora_identity** — LangGraph PostgresSaver checkpointer / saga state
  (``resolve_dsn``). Saga state lives in chora_identity per
  .claude/rules/ddd-enforcement.md + ``account-closure-saga`` skill.
- **chora_ai_kernel** — the live FastAPI ``wiring.py`` adapters:
  PostgresCoordinatorRepository + transactional outbox + the durable
  wrapped-DEK store (ADR-186) + the NATS outbox dispatcher
  (``resolve_ai_kernel_dsn``).

Per-domain pseudonymisation runs through NATS events — those domains own
their own DSNs; this resolver only handles the two orchestrator-owned
DSNs above.

Environment contract:

- ``CHORA_CLOSURE_PG_DSN`` / ``CHORA_CLOSURE_PG_DSN_SECRET_ID`` — the
  saga-state (chora_identity) DSN: direct, else the secret-alias env.
- ``CHORA_AI_KERNEL_PG_DSN`` (alias ``CHORA_AI_KERNEL_CONNINFO``) /
  ``CHORA_AI_KERNEL_PG_DSN_SECRET_ID`` — the chora_ai_kernel DSN.

Behaviour (per DSN):

- A direct DSN env set → return it immediately.
- Only the secret-alias env set → its value IS the DSN (env-backed secret).
- All unset (or whitespace-only) → return empty string. Caller is
  expected to fall through to the in-memory adapters in dev / unit tests.

The resolver is a stateless pure function — multi-replica safe; each
replica reads its own DSN at startup; no shared state. Re-running
``resolve_dsn()`` is deterministic (same env → same output), so retries /
restarts are idempotent.
"""

from __future__ import annotations

import os


def _resolve_dsn(direct_envs: tuple[str, ...], secret_id_env: str) -> str:
    """Resolve a DSN: first non-empty direct env wins; else the
    env-backed secret alias.

    Returns the empty string when no direct env is set and the secret-alias
    env is unset/whitespace — callers MUST treat empty as ``unset`` and
    fall back to in-memory adapters.
    """
    for name in direct_envs:
        direct = (os.getenv(name) or "").strip()
        if direct:
            return direct

    secret = (os.getenv(secret_id_env) or "").strip()
    if not secret:
        return ""
    return _fetch_secret(secret_name=secret)


def resolve_dsn() -> str:
    """Resolve the saga-state (chora_identity) DSN from env.

    Returns the empty string when neither path is configured — caller MUST
    treat empty as ``unset``.
    """
    return _resolve_dsn(("CHORA_CLOSURE_PG_DSN",), "CHORA_CLOSURE_PG_DSN_SECRET_ID")


def resolve_ai_kernel_dsn() -> str:
    """Resolve the chora_ai_kernel DSN from env.

    Feeds ``wiring.plan_from_env`` (PostgresCoordinatorRepository +
    transactional outbox + the durable wrapped-DEK store + the NATS
    dispatcher). Direct ``CHORA_AI_KERNEL_PG_DSN`` /
    ``CHORA_AI_KERNEL_CONNINFO`` win; otherwise the
    ``CHORA_AI_KERNEL_PG_DSN_SECRET_ID`` env-backed secret is used.
    Empty when all unset.
    """
    return _resolve_dsn(
        ("CHORA_AI_KERNEL_PG_DSN", "CHORA_AI_KERNEL_CONNINFO"),
        "CHORA_AI_KERNEL_PG_DSN_SECRET_ID",
    )


def _fetch_secret(*, secret_name: str) -> str:
    """Resolve an env-backed secret: the secret-alias env var carries the
    payload plaintext (compose injects it from a Docker secret / .env).

    Kept as a private seam so unit tests can monkeypatch the resolution
    without touching the public surface.
    """
    return secret_name


__all__ = ["resolve_ai_kernel_dsn", "resolve_dsn"]
