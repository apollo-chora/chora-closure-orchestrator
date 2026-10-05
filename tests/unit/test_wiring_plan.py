"""WiringPlan — env-driven adapter selection (CHO-1719 gap 1).

``plan_from_env()`` is the pure selection function ``build_app_from_env``
uses to decide fake vs real adapters. Env contract (cloud-neutral):

* ``CHORA_AI_KERNEL_PG_DSN``        → Postgres coordinator repo + outbox
  (alias: ``CHORA_AI_KERNEL_CONNINFO`` — the checkpointer's historic name)
* ``CHORA_SOURCE_PROJECT``          → NATS outbox dispatcher
* ``CHORA_LOCAL_KEK``               → LocalKMSClient
* ``CHORA_COLD_ARCHIVE_BUCKET``     → MinioColdArchiveClient
* ``CLOSURE_ACK_SUBSCRIPTIONS``     → comma-joined ack subscriptions
* ``CHORA_CLOSURE_FAKE_ADAPTERS``   → force fakes (dev/test escape hatch)
"""

from __future__ import annotations

import pytest

from chora_closure_orchestrator.domain.closure import REQUIRED_DOMAINS
from chora_closure_orchestrator.wiring import plan_from_env

_ALL_ENV = [
    "CHORA_AI_KERNEL_PG_DSN",
    "CHORA_AI_KERNEL_CONNINFO",
    "CHORA_AI_KERNEL_PG_DSN_SECRET_ID",
    "CHORA_SOURCE_PROJECT",
    "CHORA_LOCAL_KEK",
    "CHORA_COLD_ARCHIVE_BUCKET",
    "CLOSURE_ACK_SUBSCRIPTIONS",
    "CHORA_CLOSURE_FAKE_ADAPTERS",
    "CLOSURE_DRIVER_POLL_SECONDS",
    "CLOSURE_OUTBOX_POLL_SECONDS",
    "CLOSURE_GRACE_PERIOD_DAYS_DEFAULT",
    "CLOSURE_REPO_POOL_MAX_SIZE",
    "CLOSURE_OUTBOX_RETRY_BACKOFF_SECONDS",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in _ALL_ENV:
        monkeypatch.delenv(k, raising=False)


class TestDefaults:
    def test_empty_env_is_all_fake(self) -> None:
        p = plan_from_env()
        assert p.repo_kind == "inmem"
        assert p.publisher_kind == "inmem"
        assert p.kms_kind == "fake"
        assert p.archive_kind == "inmem"
        assert p.dispatcher_enabled is False
        assert p.ack_subscriptions == []

    def test_poll_defaults(self) -> None:
        p = plan_from_env()
        assert p.driver_poll_seconds == 30.0
        assert p.outbox_poll_seconds == 0.5
        assert p.grace_period_days_default == 30

    def test_repo_pool_default_covers_the_ack_fan_in(self) -> None:
        """G19: sized to CONCURRENCY, not QPS.

        The fan-out publishes one pseudonymise request per required domain and
        the acks come back together, so the default must leave room for one
        handler coroutine per domain plus the SagaDriver plus HTTP headroom.
        """
        p = plan_from_env()
        assert p.repo_pool_max_size == 12
        assert p.repo_pool_max_size > len(REQUIRED_DOMAINS), (
            "a pool smaller than the ack fan-in queues every closure behind "
            "itself at exactly the moment the saga is busiest"
        )

    def test_outbox_retry_backoff_defaults_above_the_poll_interval(self) -> None:
        """G12 invariant 3. A backoff shorter than the poll is not a backoff."""
        p = plan_from_env()
        assert p.outbox_retry_backoff_seconds == 60
        assert p.outbox_retry_backoff_seconds > p.outbox_poll_seconds, (
            "a failing row would be re-fetched on the very next poll and burn "
            "every attempt in seconds, at the HEAD of each batch"
        )

    def test_outbox_retry_backoff_is_env_overridable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLOSURE_OUTBOX_RETRY_BACKOFF_SECONDS", "5")
        assert plan_from_env().outbox_retry_backoff_seconds == 5

    def test_repo_pool_size_is_env_overridable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLOSURE_REPO_POOL_MAX_SIZE", "25")
        assert plan_from_env().repo_pool_max_size == 25


class TestRealSelection:
    def test_dsn_selects_postgres_repo_and_outbox(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN", "postgresql://x/chora_ai_kernel")
        p = plan_from_env()
        assert p.repo_kind == "postgres"
        assert p.publisher_kind == "outbox"
        assert p.dsn == "postgresql://x/chora_ai_kernel"

    def test_conninfo_alias_honoured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_AI_KERNEL_CONNINFO", "postgresql://y/chora_ai_kernel")
        p = plan_from_env()
        assert p.repo_kind == "postgres"
        assert p.dsn == "postgresql://y/chora_ai_kernel"

    def test_source_project_enables_dispatcher(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN", "postgresql://x/d")
        monkeypatch.setenv("CHORA_SOURCE_PROJECT", "chora-local")
        p = plan_from_env()
        assert p.dispatcher_enabled is True
        assert p.pubsub_project == "chora-local"

    def test_dispatcher_off_without_dsn(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_SOURCE_PROJECT", "chora-local")
        p = plan_from_env()
        assert p.dispatcher_enabled is False

    def test_secret_id_resolves_dsn_via_env_backed_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Deployed-reality regression (§3 D11): the deployment injects
        ONLY ``CHORA_AI_KERNEL_PG_DSN_SECRET_ID`` (no direct DSN). The plan
        MUST resolve it (env-backed secret) and select the Postgres-backed
        adapters — otherwise the wrapped-DEK store + repo + outbox silently
        fall back to in-memory and crypto-shred is non-durable.
        """
        from chora_closure_orchestrator.adapter.secrets import resolver as r

        monkeypatch.setattr(
            r,
            "_fetch_secret",
            lambda *, secret_name: "postgresql://127.0.0.1:5432/chora_ai_kernel",
        )
        monkeypatch.setenv(
            "CHORA_AI_KERNEL_PG_DSN_SECRET_ID",
            "chora-dev-chora_ai_kernel-app_rw-dsn",
        )
        monkeypatch.setenv("CHORA_SOURCE_PROJECT", "chora-local")
        p = plan_from_env()
        assert p.dsn == "postgresql://127.0.0.1:5432/chora_ai_kernel"
        assert p.repo_kind == "postgres"
        assert p.publisher_kind == "outbox"
        assert p.dispatcher_enabled is True

    def test_kek_selects_local_kms(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_LOCAL_KEK", "AAAA")
        p = plan_from_env()
        assert p.kms_kind == "local"

    def test_bucket_selects_minio_archive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_COLD_ARCHIVE_BUCKET", "chora-cold-archive-sg")
        p = plan_from_env()
        assert p.archive_kind == "minio"
        assert p.cold_archive_bucket == "chora-cold-archive-sg"

    def test_ack_subscriptions_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "CLOSURE_ACK_SUBSCRIPTIONS",
            "closure-ack-identity, closure-ack-creation ,closure-ack-tenancy",
        )
        p = plan_from_env()
        assert p.ack_subscriptions == [
            "closure-ack-identity",
            "closure-ack-creation",
            "closure-ack-tenancy",
        ]


class TestFakeOverride:
    def test_fake_flag_forces_fakes_even_with_real_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHORA_CLOSURE_FAKE_ADAPTERS", "true")
        monkeypatch.setenv("CHORA_AI_KERNEL_PG_DSN", "postgresql://x/d")
        monkeypatch.setenv("CHORA_SOURCE_PROJECT", "chora-local")
        monkeypatch.setenv("CHORA_LOCAL_KEK", "AAAA")
        monkeypatch.setenv("CHORA_COLD_ARCHIVE_BUCKET", "b")
        p = plan_from_env()
        assert p.repo_kind == "inmem"
        assert p.publisher_kind == "inmem"
        assert p.kms_kind == "fake"
        assert p.archive_kind == "inmem"
        assert p.dispatcher_enabled is False
