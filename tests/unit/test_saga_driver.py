"""SagaDriver — the background loop that advances closure sagas.

Replaces the Agent-Engine-era "run the whole graph in one query" driver:
the driver is stateless over the CoordinatorRepository (pod-death safe by
construction — saga state lives in ``chora_ai_kernel.closure_saga``) and
re-uses the existing closure_graph node functions:

* CLOSING + grace expired (or fast-close)  → advance → pseudonymise fan-out
* PSEUDONYMIZED + all 10 domain acks       → aggregated-complete →
                                              cold-archive → crypto-shred
"""

from __future__ import annotations

import datetime as _dt

import pytest

from chora_closure_orchestrator.adapter.coldarchive import (
    InMemoryColdArchiveClient,
)
from chora_closure_orchestrator.adapter.events import InMemoryClosurePublisher
from chora_closure_orchestrator.adapter.kms import FakeKMSClient
from chora_closure_orchestrator.adapter.repository import (
    InMemoryCoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import (
    REQUIRED_DOMAINS,
    NewParams,
    State,
    new,
)
from chora_closure_orchestrator.orchestrators.saga_driver import SagaDriver

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _driver(
    *, ack_timeout_seconds: float = 86400.0
) -> tuple[
    SagaDriver,
    InMemoryCoordinatorRepository,
    InMemoryClosurePublisher,
]:
    repo = InMemoryCoordinatorRepository()
    pub = InMemoryClosurePublisher()
    driver = SagaDriver(
        repo=repo,
        publisher=pub,
        kms=FakeKMSClient(),
        archive=InMemoryColdArchiveClient(bucket="chora-cold-archive-dev"),
        ack_timeout_seconds=ack_timeout_seconds,
    )
    return driver, repo, pub


def _saga(*, fast_close: bool = False):  # noqa: ANN202
    return new(
        NewParams(
            gcid=GCID,
            tenant_id=TENANT,
            grace_period_days=30,
            requested_by_gcid=GCID,
            reason="driver test",
            fast_close=fast_close,
        )
    )


class _FailingFanoutPublisher(InMemoryClosurePublisher):
    """Publisher whose per-domain fan-out always raises (adapter-failure path)."""

    async def publish_pseudonymise_requested(self, ev):  # noqa: ANN001, ANN201
        raise RuntimeError("pubsub down")


class TestFanoutResilience:
    @pytest.mark.asyncio
    async def test_fanout_publish_failure_is_not_counted(self) -> None:
        repo = InMemoryCoordinatorRepository()
        driver = SagaDriver(
            repo=repo,
            publisher=_FailingFanoutPublisher(),
            kms=FakeKMSClient(),
            archive=InMemoryColdArchiveClient(bucket="chora-cold-archive-dev"),
        )
        c = _saga(fast_close=True)
        await repo.save(c)

        stats = await driver.tick()
        # The advance to SUSPENDED happened, but fan-out failed → not counted;
        # the saga is retried (re-fanned-out) on a later tick / stays visible.
        assert stats["fanned_out"] == 0


class TestGraceExpiryFanout:
    @pytest.mark.asyncio
    async def test_fast_close_saga_fans_out_on_tick(self) -> None:
        driver, repo, pub = _driver()
        c = _saga(fast_close=True)
        await repo.save(c)

        stats = await driver.tick()
        assert stats["fanned_out"] == 1

        got = await repo.get(c.saga_id)
        # D3: fan-out advances CLOSING -> SUSPENDED only. The saga must NOT
        # reach PSEUDONYMIZED until every required domain has acked (or the
        # ack timeout escalates it to PSEUDONYMISE_PARTIAL).
        assert got.state is State.SUSPENDED
        topics = [e.topic for e in pub.snapshot()]
        for d in REQUIRED_DOMAINS:
            assert f"chora.{d}.pii.pseudonymise.requested.v1" in topics

    @pytest.mark.asyncio
    async def test_unexpired_saga_untouched(self) -> None:
        driver, repo, pub = _driver()
        c = _saga(fast_close=False)
        await repo.save(c)

        stats = await driver.tick()
        assert stats["fanned_out"] == 0
        got = await repo.get(c.saga_id)
        assert got.state is State.CLOSING
        assert pub.count() == 0


class TestAckCompletion:
    @pytest.mark.asyncio
    async def test_suspended_without_acks_awaits(self) -> None:
        # D3: a fanned-out saga with no acks stays SUSPENDED — it must NOT
        # silently advance to PSEUDONYMIZED (the old bug). A long ack timeout
        # means no escalation either.
        driver, repo, _pub = _driver(ack_timeout_seconds=86400)
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()  # → SUSPENDED + fan-out, no acks yet

        stats = await driver.tick()
        assert stats["completed"] == 0
        got = await repo.get(c.saga_id)
        assert got.state is State.SUSPENDED

    @pytest.mark.asyncio
    async def test_ack_timeout_escalates_to_partial(self) -> None:
        # D3: when acks do not arrive within CLOSURE_ACK_TIMEOUT_SECONDS the
        # saga escalates to PSEUDONYMISE_PARTIAL (admin queue) — never a silent
        # advance to PSEUDONYMIZED, never a silent stall.
        driver, repo, _pub = _driver(ack_timeout_seconds=1)
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()  # → SUSPENDED + fan-out
        got = await repo.get(c.saga_id)
        assert got.state is State.SUSPENDED

        # No acks; jump well past the 1s timeout.
        future = _dt.datetime.now(_dt.UTC) + _dt.timedelta(seconds=600)
        stats = await driver.tick(now=future)
        assert stats["partial"] == 1
        final = await repo.get(c.saga_id)
        assert final.state is State.PSEUDONYMISE_PARTIAL

    @pytest.mark.asyncio
    async def test_partial_recovers_when_late_acks_complete(self) -> None:
        # D3: a PSEUDONYMISE_PARTIAL saga that later receives all acks recovers
        # to PSEUDONYMIZED and runs to completion (no manual intervention).
        driver, repo, _pub = _driver(ack_timeout_seconds=1)
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()
        future = _dt.datetime.now(_dt.UTC) + _dt.timedelta(seconds=600)
        await driver.tick(now=future)  # → PSEUDONYMISE_PARTIAL

        got = await repo.get(c.saga_id)
        now = _dt.datetime.now(_dt.UTC)
        for d in REQUIRED_DOMAINS:
            got.record_domain_ack(d, now)
        await repo.save(got)

        stats = await driver.tick(now=future)
        assert stats["completed"] == 1
        final = await repo.get(c.saga_id)
        assert final.state is State.CRYPTO_SHREDDED

    @pytest.mark.asyncio
    async def test_timeout_disabled_never_escalates(self) -> None:
        # ack_timeout_seconds<=0 disables escalation: a fanned-out saga waits in
        # SUSPENDED indefinitely (still never silently advancing).
        driver, repo, _pub = _driver(ack_timeout_seconds=0)
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()

        future = _dt.datetime.now(_dt.UTC) + _dt.timedelta(days=3650)
        stats = await driver.tick(now=future)
        assert stats["partial"] == 0
        got = await repo.get(c.saga_id)
        assert got.state is State.SUSPENDED

    @pytest.mark.asyncio
    async def test_pseudonymized_resume_safety(self) -> None:
        # Pod-death resume: a saga promoted to PSEUDONYMIZED in a prior tick
        # (acks complete) must be finalised by a later tick's PSEUDONYMIZED loop.
        driver, repo, _pub = _driver()
        c = _saga(fast_close=True)
        c.advance(State.SUSPENDED, "grace_expired", c.requested_by_gcid)
        now = _dt.datetime.now(_dt.UTC)
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, now)
        c.advance(State.PSEUDONYMIZED, "all_domains_acked", c.requested_by_gcid)
        await repo.save(c)  # simulate the pre-pod-death checkpoint

        stats = await driver.tick()
        assert stats["completed"] == 1
        final = await repo.get(c.saga_id)
        assert final.state is State.CRYPTO_SHREDDED

    @pytest.mark.asyncio
    async def test_all_acked_saga_runs_to_crypto_shredded(self) -> None:
        driver, repo, pub = _driver()
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()

        got = await repo.get(c.saga_id)
        now = _dt.datetime.now(_dt.UTC)
        for d in REQUIRED_DOMAINS:
            got.record_domain_ack(d, now)
        await repo.save(got)

        stats = await driver.tick()
        assert stats["completed"] == 1

        final = await repo.get(c.saga_id)
        assert final.state is State.CRYPTO_SHREDDED

        topics = [e.topic for e in pub.snapshot()]
        assert "chora.closure.pseudonymise_per_domain_complete.v1" in topics
        assert "chora.closure.crypto_shred_complete.v1" in topics
        assert "chora.closure.closed.v1" in topics

    @pytest.mark.asyncio
    async def test_tick_is_idempotent_after_terminal(self) -> None:
        driver, repo, _pub = _driver()
        c = _saga(fast_close=True)
        await repo.save(c)
        await driver.tick()
        got = await repo.get(c.saga_id)
        now = _dt.datetime.now(_dt.UTC)
        for d in REQUIRED_DOMAINS:
            got.record_domain_ack(d, now)
        await repo.save(got)
        await driver.tick()

        stats = await driver.tick()
        assert stats == {"fanned_out": 0, "completed": 0, "partial": 0}


# ── AgentTerminated emission at the terminal boundary ──────────────────────
#
# Restores the emission contract deleted with the Agent Engine deploy chain
# in 89233c3d3. The old boundary was ClosureSagaAgent.query, which ran the
# whole graph per request; the live boundary is SagaDriver._finalize, which
# runs the terminal stages. The four behaviours map across unchanged:
#
#   query returns cleanly        -> _finalize completes  -> emit SUCCESS
#   query raises                 -> a stage raises       -> emit RUNTIME_ERROR, re-raise
#   result has __interrupt__     -> a stage returns      -> emit NOTHING (saga alive,
#     (paused, not terminated)      errors (retryable)     retried next tick)
#   publisher explodes on the failure path -> the ORIGINAL error still propagates
#
# The third is the one worth stating: a stage returning `errors` is NOT a
# termination. The saga stays in its state and the next tick retries it, so
# emitting there would report a live saga as terminated and corrupt the
# per-tenant KPI and the tracing streams the event exists to feed.

_TERMINATED_TOPIC = "chora.ai_kernel.agent.terminated.v1"


def _terminated(pub: InMemoryClosurePublisher) -> list:
    return [e for e in pub._events if e.topic == _TERMINATED_TOPIC]  # noqa: SLF001


async def _run_to_finalize(driver, repo, saga) -> None:
    """Drive a saga to the terminal stage, the way the driver really gets
    there: fanned out to SUSPENDED, then all ten domains acked, then the
    tick that runs the terminal stages. Mirrors
    ``test_pseudonymized_resume_safety`` rather than hand-advancing, so the
    transitions the state machine actually permits are the ones exercised.
    """
    saga.advance(State.SUSPENDED, "grace_expired", saga.requested_by_gcid)
    now = _dt.datetime.now(_dt.UTC)
    for d in REQUIRED_DOMAINS:
        saga.record_domain_ack(d, now)
    saga.advance(State.PSEUDONYMIZED, "all_domains_acked", saga.requested_by_gcid)
    await repo.save(saga)
    await driver.tick()


class TestFinalizeEmitsAgentTerminatedSuccess:
    """A saga reaching CRYPTO_SHREDDED emits exactly one AgentTerminated."""

    @pytest.mark.asyncio
    async def test_emit_on_success_path(self) -> None:
        driver, repo, pub = _driver()
        saga = _saga()
        await _run_to_finalize(driver, repo, saga)

        events = _terminated(pub)
        assert len(events) == 1
        p = events[0].payload
        assert p["agent_id"] == "closure_saga"
        assert p["execution_id"] == saga.saga_id
        assert p["termination_code"] == "AGENT_TERMINATION_CODE_SUCCESS"
        assert p["runtime"] == "AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON"
        assert p["crew_pattern"] == "P8_LANGGRAPH_STATEFUL_SAGA"
        assert p["context"]["last_state_node"] == "crypto_shredded"
        assert p["context"]["last_error_message"] == ""


class TestFinalizeDoesNotEmitOnRetryableStall:
    """A stage returning `errors` leaves the saga ALIVE for the next tick.
    Emitting there would report a live saga as terminated.

    ⚠ Parametrised over ALL THREE terminal stages deliberately. An earlier
    version pinned only ``archive_to_coldline``, which proved the rule for
    one stage and left the other two branches both untested and uncovered.
    The defect class here is per-stage: a future edit that adds an emit to
    one branch would have passed a single-stage test.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stage_attr",
        [
            "await_per_domain_acks_node",
            "archive_to_coldline_node",
            "crypto_shred_node",
        ],
    )
    async def test_no_emit_when_a_stage_reports_errors(self, stage_attr: str) -> None:
        driver, repo, pub = _driver()
        saga = _saga()

        async def _stalls(*_a, **_k):
            return {"errors": [f"{stage_attr} unavailable"]}

        import chora_closure_orchestrator.orchestrators.saga_driver as sd

        original = getattr(sd, stage_attr)
        setattr(sd, stage_attr, _stalls)
        try:
            await _run_to_finalize(driver, repo, saga)
        finally:
            setattr(sd, stage_attr, original)

        assert _terminated(pub) == []


class TestFinalizeEmitsRuntimeErrorAndReraises:
    """A raising stage emits RUNTIME_ERROR carrying the message, then the
    original exception propagates.
    """

    @pytest.mark.asyncio
    async def test_emit_on_failure_path_and_reraise(self) -> None:
        driver, repo, pub = _driver()
        saga = _saga()

        async def _explodes(*_a, **_k):
            raise RuntimeError("coldline blew up")

        import chora_closure_orchestrator.orchestrators.saga_driver as sd

        original = sd.archive_to_coldline_node
        sd.archive_to_coldline_node = _explodes
        try:
            with pytest.raises(RuntimeError, match="coldline blew up"):
                await _run_to_finalize(driver, repo, saga)
        finally:
            sd.archive_to_coldline_node = original

        events = _terminated(pub)
        assert len(events) == 1
        p = events[0].payload
        assert p["termination_code"] == "AGENT_TERMINATION_CODE_RUNTIME_ERROR"
        assert p["execution_id"] == saga.saga_id
        assert "coldline blew up" in p["context"]["last_error_message"]


class TestEmissionFailureDoesNotMaskSagaError:
    """If the publisher itself raises while emitting on the failure path,
    the ORIGINAL saga exception MUST still propagate. We never swallow a
    saga failure to keep emission clean.
    """

    @pytest.mark.asyncio
    async def test_publisher_explosion_does_not_mask_runtime_error(self) -> None:
        class _Exploding(InMemoryClosurePublisher):
            async def publish_agent_terminated(self, e):  # noqa: ANN001, ANN201
                raise RuntimeError("outbox connection refused")

        repo = InMemoryCoordinatorRepository()
        pub = _Exploding()
        driver = SagaDriver(
            repo=repo,
            publisher=pub,
            kms=FakeKMSClient(),
            archive=InMemoryColdArchiveClient(bucket="chora-cold-archive-dev"),
            ack_timeout_seconds=86400.0,
        )
        saga = _saga()

        async def _explodes(*_a, **_k):
            raise RuntimeError("graph blew up")

        import chora_closure_orchestrator.orchestrators.saga_driver as sd

        original = sd.archive_to_coldline_node
        sd.archive_to_coldline_node = _explodes
        try:
            with pytest.raises(RuntimeError, match="graph blew up"):
                await _run_to_finalize(driver, repo, saga)
        finally:
            sd.archive_to_coldline_node = original
