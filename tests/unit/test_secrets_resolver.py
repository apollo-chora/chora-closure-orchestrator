"""Tests for the chora-closure-orchestrator env-backed DSN resolver.

Per `secrets-and-env`: DSNs come from the environment (compose injected;
typically Docker secrets / .env). The resolver must:

- Prefer the direct DSN env var when set (dev override).
- Use the secret-alias env var (whose value IS the DSN plaintext — the
  env-backed secret) when only the secret env var is set.
- Return empty string when both are unset (dev / unit-test fallback).
- Be import-cheap (no third-party imports at module load time).

Closure orchestrator's saga state lives in the ``chora_identity``
database; the DSN points there. Per-domain pseudonymisation flows
through NATS events — those domains hold their own DSNs.
"""

from __future__ import annotations

import pytest


def test_resolver_prefers_direct_dsn_over_secret_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHORA_CLOSURE_PG_DSN", "postgresql://direct/db")
    monkeypatch.setenv("CHORA_CLOSURE_PG_DSN_SECRET_ID", "ignored")

    from chora_closure_orchestrator.adapter.secrets import resolve_dsn

    dsn = resolve_dsn()
    assert dsn == "postgresql://direct/db"


def test_resolver_returns_empty_when_both_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CHORA_CLOSURE_PG_DSN", raising=False)
    monkeypatch.delenv("CHORA_CLOSURE_PG_DSN_SECRET_ID", raising=False)

    from chora_closure_orchestrator.adapter.secrets import resolve_dsn

    dsn = resolve_dsn()
    assert dsn == ""


def test_resolver_uses_secret_alias_when_only_secret_id_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """When only CHORA_CLOSURE_PG_DSN_SECRET_ID is set, the env-backed
    secret alias carries the DSN plaintext."""
    monkeypatch.delenv("CHORA_CLOSURE_PG_DSN", raising=False)
    monkeypatch.setenv(
        "CHORA_CLOSURE_PG_DSN_SECRET_ID",
        "postgresql://from-env-secret/db",
    )

    from chora_closure_orchestrator.adapter.secrets import resolve_dsn

    dsn = resolve_dsn()
    assert dsn == "postgresql://from-env-secret/db"


def test_resolver_calls_fetcher_when_only_secret_id_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """The resolution is isolated in the monkeypatchable ``_fetch_secret``
    seam so unit tests stay hermetic."""
    monkeypatch.delenv("CHORA_CLOSURE_PG_DSN", raising=False)
    monkeypatch.setenv(
        "CHORA_CLOSURE_PG_DSN_SECRET_ID",
        "postgresql://from-secret-alias/db",
    )

    captured: dict[str, str] = {}

    def fake_fetch(*, secret_name: str) -> str:
        captured["secret_name"] = secret_name
        return "postgresql://from-fetch/db"

    from chora_closure_orchestrator.adapter.secrets import resolver as r

    monkeypatch.setattr(r, "_fetch_secret", fake_fetch)
    dsn = r.resolve_dsn()
    assert dsn == "postgresql://from-fetch/db"
    assert captured["secret_name"] == "postgresql://from-secret-alias/db"


def test_resolver_blank_secret_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whitespace-only env vars are treated as unset (defensive)."""
    monkeypatch.setenv("CHORA_CLOSURE_PG_DSN", "   ")
    monkeypatch.setenv("CHORA_CLOSURE_PG_DSN_SECRET_ID", "")

    from chora_closure_orchestrator.adapter.secrets import resolve_dsn

    dsn = resolve_dsn()
    assert dsn == ""


# ---------------------------------------------------------------------------
# resolve_ai_kernel_dsn — the chora_ai_kernel DSN (coordinator repo + outbox +
# wrapped-DEK store). The deployment injects the secret alias
# (``CHORA_AI_KERNEL_PG_DSN_SECRET_ID``); wiring.py previously read only the
# direct ``CHORA_AI_KERNEL_PG_DSN``/``_CONNINFO`` env vars, so the secret id
# was never resolved and the store silently fell back to in-memory
# (§3 of HANDOFF_D11_CRYPTO_SHRED_CONTINUE_2026-06-20 — degraded crypto-shred).
# ---------------------------------------------------------------------------

_AI_KERNEL_ENV = [
    "CHORA_AI_KERNEL_PG_DSN",
    "CHORA_AI_KERNEL_CONNINFO",
    "CHORA_AI_KERNEL_PG_DSN_SECRET_ID",
]


def _clear_ai_kernel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in _AI_KERNEL_ENV:
        monkeypatch.delenv(k, raising=False)


def test_ai_kernel_prefers_direct_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ai_kernel_env(monkeypatch)
    monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN", "postgresql://direct/chora_ai_kernel")
    monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN_SECRET_ID", "ignored")

    from chora_closure_orchestrator.adapter.secrets import resolve_ai_kernel_dsn

    assert resolve_ai_kernel_dsn() == "postgresql://direct/chora_ai_kernel"


def test_ai_kernel_conninfo_alias_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ai_kernel_env(monkeypatch)
    monkeypatch.setenv("CHORA_AI_KERNEL_CONNINFO", "postgresql://alias/chora_ai_kernel")

    from chora_closure_orchestrator.adapter.secrets import resolve_ai_kernel_dsn

    assert resolve_ai_kernel_dsn() == "postgresql://alias/chora_ai_kernel"


def test_ai_kernel_returns_empty_when_all_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_ai_kernel_env(monkeypatch)

    from chora_closure_orchestrator.adapter.secrets import resolve_ai_kernel_dsn

    assert resolve_ai_kernel_dsn() == ""


def test_ai_kernel_resolves_secret_alias_when_direct_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deployed-reality path: only the secret alias is set → its value
    is the DSN (env-backed secret)."""
    _clear_ai_kernel_env(monkeypatch)
    monkeypatch.setenv(
        "CHORA_AI_KERNEL_PG_DSN_SECRET_ID",
        "postgresql://127.0.0.1:5432/chora_ai_kernel",
    )

    captured: dict[str, str] = {}

    def fake_fetch(*, secret_name: str) -> str:
        captured["secret_name"] = secret_name
        return "postgresql://127.0.0.1:5432/chora_ai_kernel"

    from chora_closure_orchestrator.adapter.secrets import resolver as r

    monkeypatch.setattr(r, "_fetch_secret", fake_fetch)
    assert r.resolve_ai_kernel_dsn() == "postgresql://127.0.0.1:5432/chora_ai_kernel"
    assert captured["secret_name"] == "postgresql://127.0.0.1:5432/chora_ai_kernel"


def test_ai_kernel_direct_dsn_skips_secret_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct DSN must never trigger a secret round-trip."""
    _clear_ai_kernel_env(monkeypatch)
    monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN", "postgresql://direct/db")
    monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN_SECRET_ID", "would-explode")

    def boom(*, secret_name: str) -> str:  # pragma: no cover
        raise AssertionError("secret fetch must not be called when direct DSN set")

    from chora_closure_orchestrator.adapter.secrets import resolver as r

    monkeypatch.setattr(r, "_fetch_secret", boom)
    assert r.resolve_ai_kernel_dsn() == "postgresql://direct/db"
