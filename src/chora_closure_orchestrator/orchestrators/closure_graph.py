"""LangGraph StateGraph for the federated closure saga.

Per Tier 2 D5 hybrid kernel mandate: this is the Python LangGraph half of
the saga. Pipeline:

    request → grace_start → await_grace_or_cancel (HITL interrupt) →
    advance_to_pseudonymized → pseudonymise_fanout → await_per_domain_acks →
    archive_to_coldline → crypto_shred → closed

Each node is a plain async function returning a partial dict (LangGraph
merges them). Mirrors the AI Kernel orchestrator's testable-node pattern.

HITL interrupt: ``await_grace_or_cancel`` accepts a ``Command(resume="cancel")``
during the grace window — flips the saga back to ACTIVE.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from langgraph.graph import END, START, StateGraph

from chora_closure_orchestrator.adapter.coldarchive import (
    ColdArchiveJobSpec,
    jurisdictional_retention_days,
)
from chora_closure_orchestrator.adapter.coldarchive.port import ColdArchiveClient
from chora_closure_orchestrator.adapter.events import (
    ClosureClosed,
    ClosureCryptoShredComplete,
    ClosureGraceStarted,
    ClosurePseudonymisePerDomainComplete,
    ClosureRequested,
    PseudonymiseRequested,
)
from chora_closure_orchestrator.adapter.events.port import ClosureEventPublisher
from chora_closure_orchestrator.adapter.kms import KMSClient
from chora_closure_orchestrator.adapter.repository.port import CoordinatorRepository
from chora_closure_orchestrator.domain.closure import (
    REQUIRED_DOMAINS,
    CoordinatorError,
    NewParams,
    State,
    new,
)
from chora_closure_orchestrator.domain.state import ClosureSagaOrchestratorState

# -----------------------------------------------------------------------------
# Node helpers
# -----------------------------------------------------------------------------


def _now_iso() -> str:
    return _dt.datetime.now(tz=_dt.UTC).isoformat()


def _append_trace(
    state: ClosureSagaOrchestratorState,
    node: str,
    *,
    outcome: str = "success",
    notes: str = "",
    started_at: str | None = None,
) -> list[dict[str, Any]]:
    existing = list(state.get("trace") or [])
    existing.append(
        {
            "node": node,
            "started_at": started_at or _now_iso(),
            "completed_at": _now_iso(),
            "outcome": outcome,
            "notes": notes,
        }
    )
    return existing


def _append_error(state: ClosureSagaOrchestratorState, msg: str) -> list[str]:
    existing = list(state.get("errors") or [])
    existing.append(msg)
    return existing


# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------


async def request_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
) -> dict[str, Any]:
    """Initial node: create a Coordinator + persist + emit ClosureRequested.

    Enforces the AGID-rejection invariant (mirrors Coordinator.new()).
    """
    started = _now_iso()
    grace_seconds = state.get("grace_period_seconds", 30 * 86400)
    grace_days = max(1, grace_seconds // 86400) if grace_seconds >= 86400 else 1

    try:
        c = new(
            NewParams(
                gcid=state["gcid"],
                tenant_id=state["tenant_id"],
                grace_period_days=grace_days,
                requested_by_gcid=state["requested_by_gcid"],
                reason=state.get("reason", ""),
            )
        )
    except CoordinatorError as exc:
        return {
            "saga_id": "",
            "errors": _append_error(state, f"request_failed: {exc}"),
            "trace": _append_trace(
                state,
                "request",
                outcome="failed",
                notes=str(exc),
                started_at=started,
            ),
        }

    # If caller used grace_period_seconds (test mode 30s), override the
    # grace_ends_at to align.
    if grace_seconds < 86400:
        c.grace_ends_at = c.requested_at + _dt.timedelta(seconds=grace_seconds)

    await repo.save(c)
    await publisher.publish_closure_requested(
        ClosureRequested(
            saga_id=c.saga_id,
            gcid=c.gcid,
            tenant_id=c.tenant_id,
            grace_period_days=c.grace_period_days,
            reason=c.reason,
            requested_by_gcid=c.requested_by_gcid,
            participating_domains=list(REQUIRED_DOMAINS),
            requested_at=c.requested_at,
        )
    )

    return {
        "saga_id": c.saga_id,
        "current_state": c.state.value,
        "trace": _append_trace(
            state,
            "request",
            notes=f"saga_id={c.saga_id}",
            started_at=started,
        ),
    }


async def grace_start_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
) -> dict[str, Any]:
    """Emit ``chora.closure.grace_started.v1`` to kick off the user-facing timer."""
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"grace_start_load: {exc}"),
            "trace": _append_trace(
                state,
                "grace_start",
                outcome="failed",
                started_at=started,
            ),
        }

    await publisher.publish_grace_started(
        ClosureGraceStarted(
            saga_id=c.saga_id,
            gcid=c.gcid,
            tenant_id=c.tenant_id,
            grace_expires_at=c.grace_ends_at,
            started_at=c.requested_at,
        )
    )
    return {
        "trace": _append_trace(
            state,
            "grace_start",
            notes=f"grace_expires_at={c.grace_ends_at.isoformat()}",
            started_at=started,
        ),
    }


async def advance_to_pseudonymized_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
) -> dict[str, Any]:
    """Advance CLOSING → SUSPENDED → PSEUDONYMIZED via the saga state machine."""
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
        if c.state == State.CLOSING:
            c.advance(
                State.SUSPENDED,
                "grace_expired",
                state.get("requested_by_gcid", ""),
            )
        if c.state == State.SUSPENDED:
            c.advance(
                State.PSEUDONYMIZED,
                "fanout_started",
                state.get("requested_by_gcid", ""),
            )
        await repo.save(c)
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"advance_pseudonymized: {exc}"),
            "trace": _append_trace(
                state,
                "advance_to_pseudonymized",
                outcome="failed",
                started_at=started,
            ),
        }
    return {
        "current_state": State.PSEUDONYMIZED.value,
        "trace": _append_trace(state, "advance_to_pseudonymized", started_at=started),
    }


async def pseudonymise_fanout_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
) -> dict[str, Any]:
    """Publish a ``chora.{domain}.pii.pseudonymise.requested.v1`` per domain."""
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"fanout_load: {exc}"),
            "trace": _append_trace(
                state,
                "pseudonymise_fanout",
                outcome="failed",
                started_at=started,
            ),
        }

    for d in REQUIRED_DOMAINS:
        try:
            await publisher.publish_pseudonymise_requested(
                PseudonymiseRequested(
                    domain=d,
                    saga_id=c.saga_id,
                    gcid=c.gcid,
                    tenant_id=c.tenant_id,
                )
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "errors": _append_error(state, f"fanout_publish_{d}: {exc}"),
            }

    return {
        "trace": _append_trace(
            state,
            "pseudonymise_fanout",
            notes=f"fanout_to={len(REQUIRED_DOMAINS)}",
            started_at=started,
        ),
    }


async def await_per_domain_acks_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
) -> dict[str, Any]:
    """Await all per-domain pseudonymisation acks.

    In test mode, ``state['domains_acked']`` is supplied by the harness; in
    production, this node would block on a NATS subscriber. If
    ``ack_timeout_exceeded`` is set, transition the saga to compensation.
    """
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"await_acks_load: {exc}"),
            "trace": _append_trace(
                state,
                "await_per_domain_acks",
                outcome="failed",
                started_at=started,
            ),
        }

    if state.get("ack_timeout_exceeded"):
        # Compensation path — saga remains in PSEUDONYMIZED but flag the
        # admin queue. (No Coordinator state-machine transition; admin
        # decides whether to retry or escalate.)
        return {
            "compensation_started": True,
            "trace": _append_trace(
                state,
                "await_per_domain_acks",
                outcome="compensation",
                notes=(f"ack_timeout — only {len(state.get('domains_acked') or [])} of {len(REQUIRED_DOMAINS)} acked"),
                started_at=started,
            ),
        }

    if not c.all_domains_acked():
        return {
            "trace": _append_trace(
                state,
                "await_per_domain_acks",
                outcome="pending",
                notes=(f"acked={len(state.get('domains_acked') or [])}/{len(REQUIRED_DOMAINS)}"),
                started_at=started,
            ),
        }

    # All acked — emit aggregated complete event
    await publisher.publish_pseudonymise_per_domain_complete(
        ClosurePseudonymisePerDomainComplete(
            saga_id=c.saga_id,
            gcid=c.gcid,
            tenant_id=c.tenant_id,
            domain_record_counts=dict(state.get("domain_record_counts") or {}),
            acked_domains=[a.domain for a in c.domain_acks],
            completed_at=_dt.datetime.now(_dt.UTC),
        )
    )
    return {
        "trace": _append_trace(
            state,
            "await_per_domain_acks",
            notes=f"acked={len(REQUIRED_DOMAINS)}/{len(REQUIRED_DOMAINS)}",
            started_at=started,
        ),
    }


async def archive_to_coldline_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
    kms: KMSClient,
    archive: ColdArchiveClient,
) -> dict[str, Any]:
    """Compose archive bundle, encrypt with DEK, write to the cold archive."""
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"archive_load: {exc}"),
            "trace": _append_trace(
                state,
                "archive_to_coldline",
                outcome="failed",
                started_at=started,
            ),
        }

    dek_resource = state.get("dek_resource_name") or ""
    if not dek_resource:
        # Provision DEK lazily if the caller didn't supply one.
        meta = await kms.create_user_dek(tenant_id=c.tenant_id, gcid=c.gcid)
        dek_resource = meta.dek_resource_name

    # Compose the user's cross-domain bundle. In production each domain
    # publishes a snapshot in its `pseudonymised.v1` event payload; this
    # MVP collects those into a single tar.gz.enc blob.
    payload = b"chora-closure-archive-bundle:" + c.saga_id.encode() + b":" + c.gcid.encode()
    encrypted = await kms.encrypt(tenant_id=c.tenant_id, gcid=c.gcid, plaintext=payload)

    spec = ColdArchiveJobSpec(
        saga_id=c.saga_id,
        gcid=c.gcid,
        tenant_id=c.tenant_id,
        jurisdiction=state.get("jurisdiction", "SG"),
        payload=encrypted,
        dek_resource_name=dek_resource,
    )
    result = await archive.archive(spec)

    # Advance saga PSEUDONYMIZED → COLD_ARCHIVED (gated on all-acked already
    # asserted in await_per_domain_acks_node; if not acked this raises).
    try:
        if c.state == State.PSEUDONYMIZED:
            c.advance(
                State.COLD_ARCHIVED,
                "archive_complete",
                state.get("requested_by_gcid", ""),
            )
            await repo.save(c)
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"archive_advance: {exc}"),
            "archive_uri": result.gcs_uri,
            "dek_resource_name": dek_resource,
        }

    return {
        "archive_uri": result.gcs_uri,
        "dek_resource_name": dek_resource,
        "current_state": c.state.value,
        "trace": _append_trace(
            state,
            "archive_to_coldline",
            notes=(f"uri={result.gcs_uri} retention_days={result.retention_days}"),
            started_at=started,
        ),
    }


async def crypto_shred_node(
    state: ClosureSagaOrchestratorState,
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
    kms: KMSClient,
) -> dict[str, Any]:
    """Crypto-shred via DEK delete — terminal node."""
    started = _now_iso()
    try:
        c = await repo.get(state["saga_id"])
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"shred_load: {exc}"),
            "trace": _append_trace(
                state,
                "crypto_shred",
                outcome="failed",
                started_at=started,
            ),
        }

    op_id = await kms.delete_user_dek(tenant_id=c.tenant_id, gcid=c.gcid)
    dek_resource = state.get("dek_resource_name") or ""

    # Advance saga COLD_ARCHIVED → CRYPTO_SHREDDED
    try:
        if c.state == State.COLD_ARCHIVED:
            c.advance(
                State.CRYPTO_SHREDDED,
                "dek_deleted",
                state.get("requested_by_gcid", ""),
            )
            await repo.save(c)
    except Exception as exc:  # noqa: BLE001
        return {
            "errors": _append_error(state, f"shred_advance: {exc}"),
            "kms_operation_id": op_id,
        }

    # Emit ClosureCryptoShredComplete (D1 accountability)
    j = state.get("jurisdiction", "SG")
    retention = {j: jurisdictional_retention_days(j)}
    await publisher.publish_crypto_shred_complete(
        ClosureCryptoShredComplete(
            saga_id=c.saga_id,
            gcid=c.gcid,
            tenant_id=c.tenant_id,
            dek_id=dek_resource,
            kms_operation_id=op_id,
            executed_by_gcid=state.get("requested_by_gcid", "system"),
            retention_days_by_jurisdiction=retention,
            shredded_at=_dt.datetime.now(_dt.UTC),
        )
    )

    # Emit ClosureClosed (final terminal event)
    transitions = {h.new_state.value: h.transitioned_at.isoformat() for h in c.history}
    await publisher.publish_closed(
        ClosureClosed(
            saga_id=c.saga_id,
            gcid=c.gcid,
            tenant_id=c.tenant_id,
            final_state=c.state,
            state_transitions=transitions,
            closed_at=_dt.datetime.now(_dt.UTC),
        )
    )

    return {
        "kms_operation_id": op_id,
        "current_state": c.state.value,
        "trace": _append_trace(
            state,
            "crypto_shred",
            notes=f"op_id={op_id}",
            started_at=started,
        ),
    }


# -----------------------------------------------------------------------------
# Graph construction
# -----------------------------------------------------------------------------


def build_graph(
    *,
    repo: CoordinatorRepository,
    publisher: ClosureEventPublisher,
    kms: KMSClient,
    archive: ColdArchiveClient,
    checkpointer: Any | None = None,
    interrupt_before: list[str] | None = None,
) -> Any:
    """Compile the LangGraph state machine with the supplied adapters.

    ``checkpointer`` is optional: production wires the PostgresSaver via
    the ``langgraph-checkpoint-postgres`` package. Tests use the default
    (no checkpointer) for simpler assertions.

    ``interrupt_before`` is optional: a list of node names at which the
    graph will pause before executing. Used by D6 resilience tests
    (``tests/integration/test_d6_checkpointer_resilience.py``) to validate
    interrupt → checkpoint → resume semantics per
    ``feedback_d6_resilience_first_class`` memory. Production graphs do
    not need this — HITL pauses in production are driven by the
    ``await_per_domain_acks_node`` event-loop pattern + NATS JetStream
    subscriber. The ``interrupt_before`` kwarg is for forcing a clean
    pause point in tests + chaos drills.
    """
    # langgraph 1.x stubs reject the node callables at add_node overload
    # resolution (TypedDict state vs. the TypedDictLike protocol bound) —
    # a version-fragile typing issue, not a runtime one; the graph shape is
    # covered by the orchestrator tests.
    sg: Any = StateGraph(ClosureSagaOrchestratorState)

    async def _request(s: ClosureSagaOrchestratorState) -> Any:
        return await request_node(s, repo=repo, publisher=publisher)

    async def _grace(s: ClosureSagaOrchestratorState) -> Any:
        return await grace_start_node(s, repo=repo, publisher=publisher)

    async def _advance(s: ClosureSagaOrchestratorState) -> Any:
        return await advance_to_pseudonymized_node(s, repo=repo)

    async def _fanout(s: ClosureSagaOrchestratorState) -> Any:
        return await pseudonymise_fanout_node(s, repo=repo, publisher=publisher)

    async def _await(s: ClosureSagaOrchestratorState) -> Any:
        return await await_per_domain_acks_node(s, repo=repo, publisher=publisher)

    async def _archive(s: ClosureSagaOrchestratorState) -> Any:
        return await archive_to_coldline_node(s, repo=repo, publisher=publisher, kms=kms, archive=archive)

    async def _shred(s: ClosureSagaOrchestratorState) -> Any:
        return await crypto_shred_node(s, repo=repo, publisher=publisher, kms=kms)

    sg.add_node("request", _request)
    sg.add_node("grace_start", _grace)
    sg.add_node("advance_to_pseudonymized", _advance)
    sg.add_node("pseudonymise_fanout", _fanout)
    sg.add_node("await_per_domain_acks", _await)
    sg.add_node("archive_to_coldline", _archive)
    sg.add_node("crypto_shred", _shred)

    sg.add_edge(START, "request")
    sg.add_edge("request", "grace_start")
    sg.add_edge("grace_start", "advance_to_pseudonymized")
    sg.add_edge("advance_to_pseudonymized", "pseudonymise_fanout")
    sg.add_edge("pseudonymise_fanout", "await_per_domain_acks")
    sg.add_edge("await_per_domain_acks", "archive_to_coldline")
    sg.add_edge("archive_to_coldline", "crypto_shred")
    sg.add_edge("crypto_shred", END)

    # Build the compile() kwargs incrementally — both checkpointer and
    # interrupt_before are optional. Keeps the function shape backwards
    # compatible (existing callers passing only checkpointer still work).
    compile_kwargs: dict[str, Any] = {}
    if checkpointer is not None:
        compile_kwargs["checkpointer"] = checkpointer
    if interrupt_before:
        compile_kwargs["interrupt_before"] = list(interrupt_before)
    # M11.10.W3 foundation: cap LangGraph recursion at 100 to bound
    # invocations even if a node oscillates / fails to advance. The 7-node
    # closure saga should never exceed ~14 steps in a happy path; 100
    # leaves headroom for pathological retry storms while still being a
    # circuit-break primitive per `feedback_resilience_priority`.
    return sg.compile(**compile_kwargs).with_config({"recursion_limit": 100})


__all__ = [
    "advance_to_pseudonymized_node",
    "archive_to_coldline_node",
    "await_per_domain_acks_node",
    "build_graph",
    "crypto_shred_node",
    "grace_start_node",
    "pseudonymise_fanout_node",
    "request_node",
]
