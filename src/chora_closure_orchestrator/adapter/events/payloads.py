"""Closure event payloads — typed record carriers for the event topics.

Mirrors ``chora-contracts/proto/events/closure/saga.proto`` (the canonical
wire shape; Protobuf binary encoding handled by the publisher's
``protomarshal``).
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field

from chora_closure_orchestrator.domain.closure import State


@dataclass
class ClosureRequested:
    """``chora.closure.requested.v1`` payload."""

    saga_id: str
    gcid: str
    tenant_id: str
    grace_period_days: int
    reason: str
    requested_by_gcid: str
    participating_domains: list[str]
    requested_at: _dt.datetime
    traceparent: str = ""
    tracestate: str = ""


@dataclass
class ClosureGraceStarted:
    """``chora.closure.grace_started.v1`` payload."""

    saga_id: str
    gcid: str
    tenant_id: str
    grace_expires_at: _dt.datetime
    started_at: _dt.datetime


@dataclass
class ClosurePseudonymisePerDomainComplete:
    """``chora.closure.pseudonymise_per_domain_complete.v1`` payload."""

    saga_id: str
    gcid: str
    tenant_id: str
    domain_record_counts: dict[str, int]
    acked_domains: list[str]
    completed_at: _dt.datetime


@dataclass
class ClosureCryptoShredComplete:
    """``chora.closure.crypto_shred_complete.v1`` payload.

    HIGHLY audited — feeds IMDA D1 Accountability evidence chain.
    """

    saga_id: str
    gcid: str
    tenant_id: str
    dek_id: str
    kms_operation_id: str
    executed_by_gcid: str
    retention_days_by_jurisdiction: dict[str, int]
    shredded_at: _dt.datetime


@dataclass
class ClosureClosed:
    """``chora.closure.closed.v1`` payload — terminal saga event."""

    saga_id: str
    gcid: str
    tenant_id: str
    final_state: State
    state_transitions: dict[str, str] = field(default_factory=dict)
    closed_at: _dt.datetime = field(default_factory=lambda: _dt.datetime.now(_dt.UTC))


@dataclass
class ClosureCancelled:
    """``chora.closure.cancelled.v1`` payload."""

    saga_id: str
    gcid: str
    tenant_id: str
    cancelled_by_gcid: str
    reason: str
    cancelled_at: _dt.datetime


@dataclass
class PseudonymiseRequested:
    """``chora.{domain}.pii.pseudonymise.requested.v1`` payload.

    The ``domain`` field is NOT serialised in the body; it routes the topic.
    """

    domain: str
    saga_id: str
    gcid: str
    tenant_id: str


@dataclass
class AgentTerminated:
    """``chora.ai_kernel.agent.terminated.v1`` payload.

    Mirrors ``chora.ai_kernel.v1.AgentTerminated`` in
    ``chora-contracts/proto/events/ai_kernel/agent.proto``. One event per
    execution lifecycle, emitted by the Python orchestrator entry boundary on
    BOTH success and failure paths. Failure mode is captured via
    ``termination_code``.

    The boundary is ``SagaDriver._finalize``. It was ``ClosureSagaAgent.query``
    until 89233c3d3 retired the Agent Engine deploy chain with that class, and
    this docstring pointed at the deleted name until 2026-09-03.

    ⚠ A stage returning ``errors`` is NOT a termination and emits nothing: the
    saga keeps its state and the next tick retries it. Only a clean run
    (SUCCESS) or a raising stage (RUNTIME_ERROR) terminates.

    Per the W3 foundation audit (2026-05-12): structured terminate-event
    primitive is foundational for multi-crew chaos forensics + tracing
    span correlation + per-tenant attribution.
    """

    agent_id: str
    execution_id: str
    termination_code: str  # AGENT_TERMINATION_CODE_* enum name
    runtime: str  # AGENT_EXECUTION_RUNTIME_* enum name
    tenant_id: str
    gcid: str
    terminated_at: _dt.datetime
    agent_agid: str = ""
    crew_id: str = ""
    crew_pattern: str = ""
    last_state_node: str = ""
    last_tool_name: str = ""
    last_error_message: str = ""
    iteration_count: int = 0
    partial_state: dict[str, str] = field(default_factory=dict)
    current_span_id: str = ""
    traceparent: str = ""
    tracestate: str = ""
