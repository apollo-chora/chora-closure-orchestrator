"""Env-driven adapter wiring for chora-closure-orchestrator (CHO-1719).

``plan_from_env()`` is the PURE selection step (unit-tested); the
``start_runtime``/``Runtime`` pair performs the impure assembly inside the
FastAPI lifespan (container startup).

Env contract (cloud-neutral; compose injects every value):

============================  =============================================
``CHORA_AI_KERNEL_PG_DSN``    chora_ai_kernel DSN → PostgresCoordinator
                              Repository + TransactionalOutboxPublisher +
                              dispatcher + the durable wrapped-DEK store.
                              Alias: ``CHORA_AI_KERNEL_CONNINFO``. When
                              neither direct env is set the secret-alias env
                              ``CHORA_AI_KERNEL_PG_DSN_SECRET_ID`` is used
                              (env-backed secret — its value IS the DSN) —
                              see ``adapter.secrets.resolve_ai_kernel_dsn``.
``CHORA_SOURCE_PROJECT``      logical source name stamped into the event
                              envelope (``source_project``); enables the
                              outbox dispatcher (requires the DSN too).
``CHORA_LOCAL_KEK``           base64-encoded 32-byte master KEK →
                              LocalKMSClient (env-backed secret).
``CHORA_COLD_ARCHIVE_BUCKET``  MinIO bucket → MinioColdArchiveClient.
``CHORA_NATS_URL``            NATS broker URL (default
                              ``nats://localhost:4222``) — outbox
                              dispatcher + per-domain ack subscribers.
``CHORA_MINIO_ENDPOINT``      MinIO endpoint for the cold-archive writer.
``CHORA_MINIO_ACCESS_KEY``    MinIO access key.
``CHORA_MINIO_SECRET_KEY``    MinIO secret key.
``CHORA_MINIO_SECURE``        ``true`` for TLS (default false).
``CLOSURE_ACK_SUBSCRIPTIONS``  comma-joined ack topic names for the 10
                              ``chora.{domain}.account.pseudonymised.v1``
                              ack subjects. When unset, the default is
                              the 10 canonical subjects derived from
                              ``REQUIRED_DOMAINS`` (the subjects the 10
                              domain services actually publish).
``CLOSURE_DRIVER_POLL_SECONDS``    saga-driver poll (default 30).
``CLOSURE_OUTBOX_POLL_SECONDS``    outbox dispatcher poll (default 0.5).
``CLOSURE_GRACE_PERIOD_DAYS_DEFAULT``  default grace days (default 30).
``CHORA_CLOSURE_FAKE_ADAPTERS``    "true" → force in-memory/fake adapters
                              (dev escape hatch; NEVER set in prod).
============================  =============================================
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
from dataclasses import dataclass, field
from typing import Any

from chora_closure_orchestrator.adapter.pubsub.ack_consumer import (
    ack_topic_for_domain,
)
from chora_closure_orchestrator.domain.closure import REQUIRED_DOMAINS

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}

_NATS_URL_DEFAULT = "nats://localhost:4222"


@dataclass(frozen=True)
class WiringPlan:
    """Pure description of which adapter implementations to wire."""

    repo_kind: str = "inmem"  # inmem | postgres
    publisher_kind: str = "inmem"  # inmem | outbox
    kms_kind: str = "fake"  # fake | local
    archive_kind: str = "inmem"  # inmem | minio
    dispatcher_enabled: bool = False
    dsn: str = ""
    pubsub_project: str = ""
    kek: str = ""
    cold_archive_bucket: str = ""
    ack_subscriptions: list[str] = field(default_factory=list)
    driver_poll_seconds: float = 30.0
    outbox_poll_seconds: float = 0.5
    grace_period_days_default: int = 30
    # D3: SUSPENDED sagas escalate to PSEUDONYMISE_PARTIAL after this many
    # seconds without all per-domain acks. Default 24h.
    ack_timeout_seconds: float = 86400.0
    # G19: the repository's pool ceiling. Sized to the CONCURRENCY, not to the
    # QPS: one ack-handler coroutine per ack subscription (10 domains) plus the
    # SagaDriver task plus HTTP request headroom. Too small does not corrupt
    # anything, it just queues.
    repo_pool_max_size: int = 12
    # G12 invariant 3: how long a FAILED outbox row waits before it is eligible
    # again. The poll runs every outbox_poll_seconds (0.5s default), so without
    # this a failing row would burn all max_attempts in ~2.5 seconds and, since
    # a retrying row keeps its original occurred_at, it would sit at the HEAD of
    # every ORDER BY occurred_at batch while doing so. Enforced in the SQL
    # WHERE clause, not in the dispatcher, so a backing-off row is genuinely
    # SKIPPED rather than fetched into a LIMIT slot and discarded.
    outbox_retry_backoff_seconds: int = 60

    @property
    def any_real(self) -> bool:
        return self.repo_kind != "inmem" or self.kms_kind != "fake" or self.archive_kind != "inmem"

    def describe(self) -> dict[str, Any]:
        return {
            "repo": self.repo_kind,
            "publisher": self.publisher_kind,
            "kms": self.kms_kind,
            "archive": self.archive_kind,
            "dispatcher": self.dispatcher_enabled,
            "ack_subscriptions": len(self.ack_subscriptions),
            "repo_pool_max_size": self.repo_pool_max_size,
            "outbox_retry_backoff_seconds": self.outbox_retry_backoff_seconds,
        }


def plan_from_env() -> WiringPlan:
    """Derive the WiringPlan from the environment.

    Deterministic given ``os.environ``: when no direct
    ``CHORA_AI_KERNEL_PG_DSN``/``_CONNINFO`` is set the chora_ai_kernel DSN
    is resolved from ``CHORA_AI_KERNEL_PG_DSN_SECRET_ID`` via
    ``resolve_ai_kernel_dsn`` (the resolution is isolated in the
    monkeypatchable ``adapter.secrets.resolver._fetch_secret`` seam, so the
    pure-selection unit tests stay hermetic when no secret id is set).
    """
    from chora_closure_orchestrator.adapter.secrets import (
        resolve_ai_kernel_dsn,
    )

    forced_fake = os.getenv("CHORA_CLOSURE_FAKE_ADAPTERS", "").strip().lower() in _TRUTHY
    driver_poll = _float_env("CLOSURE_DRIVER_POLL_SECONDS", 30.0)
    outbox_poll = _float_env("CLOSURE_OUTBOX_POLL_SECONDS", 0.5)
    grace_default = _int_env("CLOSURE_GRACE_PERIOD_DAYS_DEFAULT", 30)
    ack_timeout = _float_env("CLOSURE_ACK_TIMEOUT_SECONDS", 86400.0)
    repo_pool_max = _int_env("CLOSURE_REPO_POOL_MAX_SIZE", 12)
    outbox_backoff = _int_env("CLOSURE_OUTBOX_RETRY_BACKOFF_SECONDS", 60)

    if forced_fake:
        return WiringPlan(
            driver_poll_seconds=driver_poll,
            outbox_poll_seconds=outbox_poll,
            grace_period_days_default=grace_default,
            ack_timeout_seconds=ack_timeout,
            repo_pool_max_size=repo_pool_max,
            outbox_retry_backoff_seconds=outbox_backoff,
        )

    dsn = resolve_ai_kernel_dsn()
    project = os.getenv("CHORA_SOURCE_PROJECT", "").strip()
    kek = os.getenv("CHORA_LOCAL_KEK", "").strip()
    bucket = os.getenv("CHORA_COLD_ARCHIVE_BUCKET", "").strip()
    raw_subs = os.getenv("CLOSURE_ACK_SUBSCRIPTIONS", "").strip()
    if raw_subs:
        subs = [s.strip() for s in raw_subs.split(",") if s.strip()]
    else:
        # Default: the 10 canonical per-domain ack subjects, derived from
        # the orchestrator's REQUIRED_DOMAINS registry — the exact subjects
        # the 10 domain services publish their pseudonymisation acks on.
        # The deployed stack injects no override; without this default no
        # ack subscriber is ever bound and every saga stalls at SUSPENDED.
        subs = [ack_topic_for_domain(d) for d in REQUIRED_DOMAINS]

    return WiringPlan(
        repo_kind="postgres" if dsn else "inmem",
        publisher_kind="outbox" if dsn else "inmem",
        kms_kind="local" if kek else "fake",
        archive_kind="minio" if bucket else "inmem",
        dispatcher_enabled=bool(dsn and project),
        dsn=dsn,
        pubsub_project=project,
        kek=kek,
        cold_archive_bucket=bucket,
        ack_subscriptions=subs,
        driver_poll_seconds=driver_poll,
        outbox_poll_seconds=outbox_poll,
        grace_period_days_default=grace_default,
        ack_timeout_seconds=ack_timeout,
        repo_pool_max_size=repo_pool_max,
        outbox_retry_backoff_seconds=outbox_backoff,
    )


# ---------------------------------------------------------------------------
# Impure assembly (lifespan)
# ---------------------------------------------------------------------------


@dataclass
class Runtime:
    """Live adapter set + background tasks started by ``start_runtime``."""

    repo: Any
    publisher: Any
    kms: Any
    archive: Any
    tasks: list[asyncio.Task] = field(default_factory=list)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    conns: list[Any] = field(default_factory=list)
    streaming_futures: list[Any] = field(default_factory=list)

    async def stop(self) -> None:
        self.stop_event.set()
        for fut in self.streaming_futures:
            with contextlib.suppress(Exception):
                fut.cancel()
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        for conn in self.conns:
            with contextlib.suppress(Exception):
                await conn.close()


async def _connect_autocommit(dsn: str) -> Any:  # pragma: no cover
    """Open a fresh autocommit psycopg connection. Used as the reconnect
    factory for the idle-prone outbox-publisher + wrapped-DEK-store
    connections (see ``adapter.db.ReconnectingConnection``)."""
    import psycopg

    return await psycopg.AsyncConnection.connect(dsn, autocommit=True)


async def _connect_nats() -> Any:  # pragma: no cover
    """Open a NATS connection for the outbox dispatcher / ack subscribers."""
    import nats

    url = os.getenv("CHORA_NATS_URL", _NATS_URL_DEFAULT).strip()
    return await nats.connect(url)


class _NatsMsgView:
    """Adapt a nats-py JetStream ``Msg`` to the message shape
    ``AckAfterProcessingSubscriber`` expects (``attributes`` / ``data`` /
    ``ack()`` / ``nack()``).

    nats-py's ``ack``/``nak`` are coroutines; the subscriber wrapper awaits
    the result when it is awaitable, so the view just returns it.
    """

    def __init__(self, msg: Any) -> None:
        self._msg = msg
        self.attributes = {str(k): str(v) for k, v in (msg.header or {}).items()}

    @property
    def data(self) -> bytes:
        return bytes(self._msg.data)

    def ack(self) -> Any:
        return self._msg.ack()

    def nack(self) -> Any:
        return self._msg.nack()


async def start_runtime(plan: WiringPlan) -> Runtime:  # pragma: no cover
    """Assemble real adapters + background loops per the plan.

    Impure (network). Covered by the deploy runbook probes; unit
    coverage targets ``plan_from_env`` + the individual adapters.
    """
    from chora_closure_orchestrator.adapter.coldarchive import (
        InMemoryColdArchiveClient,
    )
    from chora_closure_orchestrator.adapter.events import (
        InMemoryClosurePublisher,
    )
    from chora_closure_orchestrator.adapter.kms import FakeKMSClient
    from chora_closure_orchestrator.adapter.repository import (
        InMemoryCoordinatorRepository,
    )

    repo: Any = InMemoryCoordinatorRepository()
    publisher: Any = InMemoryClosurePublisher()
    kms: Any = FakeKMSClient()
    archive: Any = InMemoryColdArchiveClient(bucket=plan.cold_archive_bucket or "chora-cold-archive-dev")
    runtime = Runtime(repo=repo, publisher=publisher, kms=kms, archive=archive)

    if plan.repo_kind == "postgres":
        from psycopg_pool import AsyncConnectionPool

        # ⚠ G19: A POOL, NOT ONE CONNECTION, AND THAT IS A CORRECTNESS FIX.
        #
        # This ONE repository instance is handed to THREE independent
        # concurrent users below: the SagaDriver task, one DomainAckHandler
        # coroutine per ack subscription (10 domains, and the fan-out makes
        # them arrive together), and the FastAPI handlers via
        # app.state.adapters.repo. On a single shared connection the read
        # methods' `finally: rollback()` is a CONNECTION-level rollback, so a
        # reader running between two statements of a concurrent save() rolled
        # back the WRITER's work and the writer's commit() then returned
        # SUCCESS on an empty transaction. Silent, and measured to leave a torn
        # aggregate (history row present, saga row missing).
        #
        # outbox_conn and dek_store_conn below already own dedicated
        # connections. This was the one that missed the pattern.
        repo_pool = AsyncConnectionPool(
            plan.dsn,
            min_size=1,
            max_size=plan.repo_pool_max_size,
            open=False,
        )
        # ⚠ wait=True IS LOAD-BEARING. AsyncConnectionPool.open() defaults to
        # wait=False and fills the pool in the background, so a bad DSN or an
        # unreachable database would NOT surface here: the service would come up
        # READY and fail on first use instead. The connection this replaced was
        # `await psycopg.AsyncConnection.connect(dsn)`, which raised at boot.
        # Keeping that fail-loud boot behaviour is not optional.
        await repo_pool.open(wait=True)
        # Runtime.stop() awaits .close() on everything here; the pool has one.
        runtime.conns.append(repo_pool)
        from chora_closure_orchestrator.adapter.repository.postgres import (
            PostgresCoordinatorRepository,
        )

        runtime.repo = PostgresCoordinatorRepository(pool=repo_pool)

    if plan.publisher_kind == "outbox":
        from chora_closure_orchestrator.adapter.db import (
            ReconnectingConnection,
        )

        # Reconnect-on-idle-close: the outbox publisher is touched only on a
        # closure event, so its connection idles out between closures and the
        # next write 500'd (real incident 2026-06-20). Autocommit ⇒ a
        # reopen can never drop an in-flight transaction.
        outbox_conn = ReconnectingConnection(
            dsn=plan.dsn,
            conn=await _connect_autocommit(plan.dsn),
            connect=_connect_autocommit,
        )
        runtime.conns.append(outbox_conn)
        from chora_closure_orchestrator.adapter.events.publisher_outbox import (
            TransactionalOutboxPublisher,
        )

        if not plan.pubsub_project:
            raise RuntimeError(
                "CHORA_SOURCE_PROJECT required when the outbox publisher is enabled (envelope source_project)"
            )
        runtime.publisher = TransactionalOutboxPublisher(
            conn=outbox_conn,
            source_project=plan.pubsub_project,
            source_service="chora-closure-orchestrator",
        )

    if plan.kms_kind == "local":
        from chora_closure_orchestrator.adapter.kms.local import (
            LocalKMSClient,
        )
        from chora_closure_orchestrator.adapter.kms.store import (
            PostgresWrappedDEKStore,
        )

        # ADR-186: wrapped DEKs MUST be durable — they bridge the
        # archive→retention→shred gap (years). Persist them in
        # chora_ai_kernel.closure_user_dek_wrap (migration 0054) over a
        # DEDICATED autocommit connection (sidesteps the long-lived-conn
        # InFailedSqlTransaction sticky state). Falls back to the in-memory
        # store only when no DSN is configured (dev escape hatch).
        if plan.dsn:
            from chora_closure_orchestrator.adapter.db import (
                ReconnectingConnection,
            )

            # Same idle-close exposure as the outbox publisher: the wrapped-DEK
            # store is touched only at archive + shred. Reconnect-on-closed
            # over its dedicated autocommit connection.
            dek_store_conn = ReconnectingConnection(
                dsn=plan.dsn,
                conn=await _connect_autocommit(plan.dsn),
                connect=_connect_autocommit,
            )
            runtime.conns.append(dek_store_conn)
            runtime.kms = LocalKMSClient.from_env(store=PostgresWrappedDEKStore(conn=dek_store_conn))
        else:
            logger.warning(
                "local KMS selected WITHOUT a DSN — wrapped DEKs are "
                "in-memory and will NOT survive a restart (dev only)"
            )
            runtime.kms = LocalKMSClient.from_env()

    if plan.archive_kind == "minio":
        from chora_closure_orchestrator.adapter.coldarchive.minio import (
            MinioColdArchiveClient,
        )

        runtime.archive = MinioColdArchiveClient(
            bucket=plan.cold_archive_bucket,
            endpoint=os.getenv("CHORA_MINIO_ENDPOINT", "").strip(),
            access_key=os.getenv("CHORA_MINIO_ACCESS_KEY", "").strip(),
            secret_key=os.getenv("CHORA_MINIO_SECRET_KEY", "").strip(),
            secure=os.getenv("CHORA_MINIO_SECURE", "").strip().lower() in _TRUTHY,
        )

    # Outbox dispatcher — drains closure_outbox_events → NATS JetStream.
    if plan.dispatcher_enabled:
        import psycopg

        from chora_closure_orchestrator.adapter.pubsub.dispatcher import (
            OutboxDispatcher,
        )
        from chora_closure_orchestrator.adapter.pubsub.publisher import (
            NatsPublisher,
        )
        from chora_closure_orchestrator.adapter.pubsub.store import (
            PostgresOutboxStore,
        )

        dispatch_conn = await psycopg.AsyncConnection.connect(plan.dsn)
        runtime.conns.append(dispatch_conn)
        nats_conn = await _connect_nats()
        runtime.conns.append(nats_conn)
        worker_id = f"{socket.gethostname()}:{os.getpid()}"
        store = PostgresOutboxStore(
            conn=dispatch_conn,
            worker_id=worker_id,
            retry_backoff_seconds=plan.outbox_retry_backoff_seconds,
        )
        nats_publisher = NatsPublisher(client=nats_conn.jetstream())
        dispatcher = OutboxDispatcher(store=store, publisher=nats_publisher, worker_id=worker_id)

        async def _dispatch_loop() -> None:
            while not runtime.stop_event.is_set():
                try:
                    await dispatcher.drain_once()
                except Exception as exc:  # noqa: BLE001 — loop survives
                    logger.warning(
                        "outbox_dispatch_loop_error",
                        extra={"error": str(exc)[:500]},
                    )
                try:
                    await asyncio.wait_for(
                        runtime.stop_event.wait(),
                        timeout=plan.outbox_poll_seconds,
                    )
                except TimeoutError:
                    continue

        runtime.tasks.append(asyncio.create_task(_dispatch_loop()))

    # Saga driver — grace-expiry fan-out + all-acked completion.
    from chora_closure_orchestrator.orchestrators.saga_driver import SagaDriver

    driver = SagaDriver(
        repo=runtime.repo,
        publisher=runtime.publisher,
        kms=runtime.kms,
        archive=runtime.archive,
        ack_timeout_seconds=plan.ack_timeout_seconds,
    )
    runtime.tasks.append(
        asyncio.create_task(
            driver.run_forever(
                interval_seconds=plan.driver_poll_seconds,
                stop=runtime.stop_event,
            )
        )
    )

    # Per-domain ack subscribers (NATS JetStream).
    if plan.ack_subscriptions:
        from chora_closure_orchestrator.adapter.pubsub.ack_consumer import (
            DomainAckHandler,
        )
        from chora_closure_orchestrator.adapter.pubsub.subscriber import (
            AckAfterProcessingSubscriber,
        )

        ack_nats = await _connect_nats()
        runtime.conns.append(ack_nats)
        js = ack_nats.jetstream()
        loop = asyncio.get_running_loop()
        wrapper = AckAfterProcessingSubscriber(handler=DomainAckHandler(repo=runtime.repo).handle)

        for sub in plan.ack_subscriptions:
            psub = await js.subscribe(
                sub,
                cb=lambda msg: asyncio.run_coroutine_threadsafe(wrapper.process_one(_NatsMsgView(msg)), loop),
            )
            runtime.streaming_futures.append(psub)
            logger.info("closure_ack_subscriber_bound", extra={"subscription": sub})

    logger.info("closure_runtime_started", extra=plan.describe())
    return runtime


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


__all__ = ["Runtime", "WiringPlan", "plan_from_env", "start_runtime"]
