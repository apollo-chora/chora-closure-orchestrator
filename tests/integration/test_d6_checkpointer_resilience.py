"""D6 gate — LangGraph checkpointer parity is first-class resilience.

Per ``feedback_d6_resilience_first_class`` memory + ADR-145 D6 gate +
``feedback_resilience_priority`` (dead-pod / DLQ / checkpointer-resume /
multi-user-concurrent non-negotiable):

When D6 work happens, the test battery MUST include:

1. **Pod-death survival** — state survives across invocation boundaries
   with same thread_id, even if the in-process graph is rebuilt from
   scratch (simulates pod restart).
2. **Interrupt pause** — ``interrupt_before`` halts cleanly before the
   named node; state at the boundary is queryable.
3. **Command resume** — second invocation with same thread_id picks up
   from the interrupt; downstream nodes execute.
4. **Concurrent thread isolation** — two thread_ids in flight at once
   each maintain their own state without cross-contamination.

This file covers (1)-(4) against ``InMemorySaver`` for fast local feedback.
The same scenarios are repeated against ``AsyncPostgresSaver`` in B.5
(gated by ``CHORA_AI_KERNEL_CONNINFO``) and against the deployed
Reasoning Engine in B.6.1 (real pod-death via force-delete).

We pause at ``advance_to_pseudonymized`` — the EARLIEST node downstream of
``grace_start``. That sidesteps the Coordinator's domain-ack gate
(``c.all_domains_acked()`` would otherwise block advance to COLD_ARCHIVED
because the test harness doesn't simulate per-domain NATS subscribers
that call ``c.add_ack(...)``). The D6 contract is about *checkpointer
durability* — not about driving the saga to terminal. The full happy-path
end-to-end already lives in ``test_full_saga.py``; this file isolates the
resilience semantics.

Composes with: ``adapter/repository/postgres_saver.py`` (the
``AsyncPostgresSaver`` factory), ``orchestrators/closure_graph.py``
(``build_graph(interrupt_before=...)`` extension).
"""

from __future__ import annotations

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from chora_closure_orchestrator.adapter.coldarchive import (
    InMemoryColdArchiveClient,
)
from chora_closure_orchestrator.adapter.events import InMemoryClosurePublisher
from chora_closure_orchestrator.adapter.kms import FakeKMSClient
from chora_closure_orchestrator.adapter.repository import (
    InMemoryCoordinatorRepository,
)
from chora_closure_orchestrator.adapter.repository.postgres_saver import (
    CONNINFO_ENV,
    conninfo_from_env,
)
from chora_closure_orchestrator.domain.closure.state import State
from chora_closure_orchestrator.domain.state import new_request_id
from chora_closure_orchestrator.orchestrators.closure_graph import build_graph

# Test fixtures
TENANT = "01970000-0000-7000-8000-000000000001"
GCID_A = "01970000-0000-7000-9000-00000000000a"
GCID_B = "01970000-0000-7000-9000-00000000000b"

# Interrupt at advance_to_pseudonymized — the first node downstream of
# grace_start. By the time we pause here, request_node has created the
# Coordinator + saga_id; grace_start has fired the grace-started event.
# advance_to_pseudonymized has NOT run yet — current_state is still CLOSING
# (set by request_node).
INTERRUPT_NODE = "advance_to_pseudonymized"


def _new_state(*, gcid: str, request_id: str | None = None) -> dict:
    """Build a fresh saga state dict suitable for graph.ainvoke()."""
    return {
        "request_id": request_id or new_request_id(),
        "saga_id": "",
        "gcid": gcid,
        "tenant_id": TENANT,
        "requested_by_gcid": gcid,
        "current_state": State.ACTIVE.value,
        "grace_period_seconds": 30,
        "jurisdiction": "SG",
        "reason": "user-initiated",
        "domains_acked": [],
        "domain_record_counts": {},
        "dek_resource_name": "",
        "kms_operation_id": "",
        "archive_uri": "",
        "trace": [],
        "errors": [],
        "cancel_requested": False,
        "compensation_started": False,
    }


def _fresh_adapters() -> tuple[
    InMemoryCoordinatorRepository,
    InMemoryClosurePublisher,
    FakeKMSClient,
    InMemoryColdArchiveClient,
]:
    return (
        InMemoryCoordinatorRepository(),
        InMemoryClosurePublisher(),
        FakeKMSClient(),
        InMemoryColdArchiveClient(bucket="chora-cold-archive-test"),
    )


# -----------------------------------------------------------------------------
# D6.A — interrupt_before pauses cleanly at a named node
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_interrupt_before_pauses_at_named_node() -> None:
    """``interrupt_before=[INTERRUPT_NODE]`` halts cleanly before that node fires.

    By the pause point, ``request`` and ``grace_start`` have executed:
    saga_id is populated + Coordinator persisted in repo + ClosureRequested
    event published. ``advance_to_pseudonymized`` has NOT executed —
    current_state is still CLOSING (set by request_node), NOT
    PSEUDONYMIZED.

    The pause boundary is the clean checkpoint anchor for the chaos drill
    in B.6.1 — kill the engine here, redeploy, query state, resume.
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    graph = build_graph(
        repo=repo,
        publisher=pub,
        kms=kms,
        archive=archive,
        checkpointer=saver,
        interrupt_before=[INTERRUPT_NODE],
    )

    config = {"configurable": {"thread_id": "saga-interrupt-1"}}
    await graph.ainvoke(_new_state(gcid=GCID_A), config=config)

    snapshot = await graph.aget_state(config)
    assert snapshot is not None, "snapshot must be queryable at pause"
    assert INTERRUPT_NODE in snapshot.next, f"next pending node must be {INTERRUPT_NODE}; got {snapshot.next}"
    # request_node ran first — saga_id is populated
    assert snapshot.values["saga_id"], "request_node must have run before pause; saga_id should be populated"
    # advance_to_pseudonymized has NOT run — state is still CLOSING (the
    # state request_node leaves the Coordinator in)
    assert snapshot.values["current_state"] == State.CLOSING.value, (
        f"state at pause must be CLOSING (advance_to_pseudonymized not yet run); "
        f"got {snapshot.values.get('current_state')}"
    )
    # Coordinator entity is in the repo (request_node persisted it)
    coord = await repo.get(snapshot.values["saga_id"])
    assert coord.state == State.CLOSING, f"Coordinator entity in repo must be CLOSING; got {coord.state}"


# -----------------------------------------------------------------------------
# D6.B — Command resume picks up after interrupt
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_resume_picks_up_after_interrupt() -> None:
    """After ``interrupt_before`` pause, second invocation advances past the boundary.

    Phase 1: invoke with full state — graph runs request + grace_start,
    pauses before advance_to_pseudonymized. State at pause: CLOSING.

    Phase 2: invoke with ``None`` input on same thread_id — graph runs
    advance_to_pseudonymized (and downstream as far as Coordinator gates
    permit). After advance, the Coordinator's state machine transitions
    SUSPENDED → PSEUDONYMIZED. Saga doesn't necessarily reach
    CRYPTO_SHREDDED (depends on Coordinator domain-ack gate) — the D6
    contract is "resume advanced PAST the interrupt", not "saga
    terminated".

    On the deployed engine the resume mechanism is identical: same
    thread_id + invoke with ``None`` (or ``Command`` for stateful resume
    payloads). The PostgresSaver checkpoint underwrites continuity.
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    graph = build_graph(
        repo=repo,
        publisher=pub,
        kms=kms,
        archive=archive,
        checkpointer=saver,
        interrupt_before=[INTERRUPT_NODE],
    )

    config = {"configurable": {"thread_id": "saga-resume-1"}}

    # Phase 1 — initial run to interrupt
    await graph.ainvoke(_new_state(gcid=GCID_A), config=config)
    paused = await graph.aget_state(config)
    assert INTERRUPT_NODE in paused.next, f"phase-1: must pause before {INTERRUPT_NODE}; got {paused.next}"
    saga_id_at_pause = paused.values["saga_id"]
    assert paused.values["current_state"] == State.CLOSING.value

    # Phase 2 — resume with None input
    await graph.ainvoke(None, config=config)
    after_resume = await graph.aget_state(config)

    # advance_to_pseudonymized has now executed — current_state is
    # PSEUDONYMIZED (the Coordinator transitions CLOSING → SUSPENDED →
    # PSEUDONYMIZED in a single advance_to_pseudonymized_node call).
    assert after_resume.values["current_state"] == State.PSEUDONYMIZED.value, (
        f"after resume, current_state must be PSEUDONYMIZED; got {after_resume.values.get('current_state')}"
    )
    # saga_id continuity — same saga across the resume boundary
    assert after_resume.values["saga_id"] == saga_id_at_pause, "saga_id must be stable across the resume boundary"
    # Coordinator entity in repo is updated
    coord = await repo.get(saga_id_at_pause)
    assert coord.state == State.PSEUDONYMIZED


# -----------------------------------------------------------------------------
# D6.C — Pod-death survival: state survives graph rebuild
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_checkpoint_state_survives_graph_rebuild() -> None:
    """Pod-death proof (local) — discard the graph object after pausing.

    Phase 1: build graph_a with checkpointer + interrupt, run to pause.
    Phase 2: discard graph_a entirely (simulates pod restart). Build a
    NEW graph_b with the SAME checkpointer + repo + adapters. Query state
    via the new graph using the SAME thread_id. State must match exactly.

    This is the local equivalent of: deploy to Agent Engine → invoke → pod
    dies → redeploy → query state via aget_state. Real pod-death (via
    force-delete of the deployed Reasoning Engine) lands in B.6.1 against
    the live deploy.
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    thread_id = "saga-pod-death-1"
    config = {"configurable": {"thread_id": thread_id}}

    # First "pod" — build graph_a, run to pause
    graph_a = build_graph(
        repo=repo,
        publisher=pub,
        kms=kms,
        archive=archive,
        checkpointer=saver,
        interrupt_before=[INTERRUPT_NODE],
    )
    await graph_a.ainvoke(_new_state(gcid=GCID_A), config=config)
    snap_a = await graph_a.aget_state(config)
    assert INTERRUPT_NODE in snap_a.next
    saga_id = snap_a.values["saga_id"]
    assert saga_id

    # "Pod dies" — discard graph_a entirely
    del graph_a

    # Second "pod" — build graph_b with SAME checkpointer + repo
    graph_b = build_graph(
        repo=repo,
        publisher=pub,
        kms=kms,
        archive=archive,
        checkpointer=saver,
        interrupt_before=[INTERRUPT_NODE],
    )

    # Query checkpoint state via the new graph — must match exactly
    snap_b = await graph_b.aget_state(config)
    assert snap_b is not None, "checkpoint must survive graph rebuild"
    assert snap_b.values["saga_id"] == saga_id, "saga_id survives rebuild — checkpointer is the source of truth"
    assert snap_b.values["current_state"] == State.CLOSING.value
    assert INTERRUPT_NODE in snap_b.next, "after rebuild, the pause boundary must still be queryable"

    # And the rebuilt graph can resume successfully
    await graph_b.ainvoke(None, config=config)
    final = await graph_b.aget_state(config)
    assert final.values["current_state"] == State.PSEUDONYMIZED.value, (
        "after rebuild + resume, advance_to_pseudonymized must have executed"
    )


# -----------------------------------------------------------------------------
# D6.D — Concurrent thread isolation
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_concurrent_threads_are_isolated() -> None:
    """Two saga threads with distinct thread_ids must NOT cross-contaminate state.

    Run two sagas through the same graph + checkpointer + adapters. Each
    has its own thread_id and its own GCID. Each pauses at the interrupt;
    each is independently queryable.

    Serial isolation here is the necessary baseline for the parallel-load
    proof in B.6.3 (10 concurrent sagas).
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    graph = build_graph(
        repo=repo,
        publisher=pub,
        kms=kms,
        archive=archive,
        checkpointer=saver,
        interrupt_before=[INTERRUPT_NODE],
    )

    config_a = {"configurable": {"thread_id": "saga-iso-a"}}
    config_b = {"configurable": {"thread_id": "saga-iso-b"}}

    await graph.ainvoke(_new_state(gcid=GCID_A), config=config_a)
    await graph.ainvoke(_new_state(gcid=GCID_B), config=config_b)

    snap_a = await graph.aget_state(config_a)
    snap_b = await graph.aget_state(config_b)

    assert snap_a.values["gcid"] == GCID_A
    assert snap_b.values["gcid"] == GCID_B
    assert snap_a.values["saga_id"] != snap_b.values["saga_id"], "two distinct sagas must have distinct saga_ids"
    # Both paused at the same boundary independently
    assert INTERRUPT_NODE in snap_a.next
    assert INTERRUPT_NODE in snap_b.next


# -----------------------------------------------------------------------------
# D6.E — thread_id required when checkpointer is wired
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_invocation_without_thread_id_fails_with_checkpointer() -> None:
    """LangGraph requires ``configurable.thread_id`` when a checkpointer is set.

    Proves the resilience contract: every saga is identified by a stable
    thread_id, no anonymous runs against a checkpointed graph.
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    graph = build_graph(repo=repo, publisher=pub, kms=kms, archive=archive, checkpointer=saver)

    with pytest.raises((ValueError, KeyError, TypeError)):
        await graph.ainvoke(_new_state(gcid=GCID_A), config={"configurable": {}})


# -----------------------------------------------------------------------------
# D6.F — Baseline: graph runs end-to-end WITHOUT interrupt_before
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_d6_no_interrupt_runs_through_without_pause() -> None:
    """Without ``interrupt_before``, the graph runs as far as the Coordinator
    state machine permits in a single ainvoke (advances through request →
    grace_start → advance_to_pseudonymized → ...).

    Acts as the negative-control for the interrupt tests: confirms that
    when no interrupt is configured, the graph does NOT pause.
    """
    repo, pub, kms, archive = _fresh_adapters()
    saver = InMemorySaver()
    graph = build_graph(repo=repo, publisher=pub, kms=kms, archive=archive, checkpointer=saver)

    config = {"configurable": {"thread_id": "saga-no-interrupt-1"}}
    await graph.ainvoke(_new_state(gcid=GCID_A), config=config)

    snap = await graph.aget_state(config)
    # Without interrupt, advance_to_pseudonymized SHOULD have run
    # (current_state advanced past CLOSING).
    assert snap.values["current_state"] != State.CLOSING.value, (
        f"without interrupt_before, the saga must advance past CLOSING; got {snap.values.get('current_state')}"
    )
    # And there's no pending interrupt waiting
    assert INTERRUPT_NODE not in snap.next, (
        f"without interrupt_before, {INTERRUPT_NODE} should NOT be in next; got {snap.next}"
    )


# -----------------------------------------------------------------------------
# AsyncPostgresSaver factory — error-path unit (the live PG test lives in B.5)
# -----------------------------------------------------------------------------


def test_postgres_saver_conninfo_from_env_raises_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``feedback_no_inline_config`` discipline — refuses to fall back."""
    monkeypatch.delenv(CONNINFO_ENV, raising=False)
    with pytest.raises(RuntimeError) as exc_info:
        conninfo_from_env()
    assert CONNINFO_ENV in str(exc_info.value)
    assert "feedback_no_inline_config" in str(exc_info.value)


def test_postgres_saver_conninfo_from_env_passes_through_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When env is set, the value is returned verbatim."""
    monkeypatch.setenv(
        CONNINFO_ENV,
        "postgresql://test:test@localhost:5432/chora_ai_kernel",
    )
    cs = conninfo_from_env()
    assert cs == "postgresql://test:test@localhost:5432/chora_ai_kernel"
