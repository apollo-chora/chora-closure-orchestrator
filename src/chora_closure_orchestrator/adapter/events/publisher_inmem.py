"""In-memory ClosureEventPublisher — MVP / unit-test double.

Records each published event with a fully-populated EventEnvelope (event_id
UUIDv7, idempotency_key, tenant_id, gcid, occurred_at, published_at,
traceparent, tracestate, source_project, source_service, schema_version).

Production replacement: a NATS JetStream adapter wrapping the payloads in
Protobuf binary form per the closure-saga contracts.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import os
import uuid
from dataclasses import dataclass, field
from typing import Any

from chora_closure_orchestrator.adapter.events.payloads import (
    AgentTerminated,
    ClosureCancelled,
    ClosureClosed,
    ClosureCryptoShredComplete,
    ClosureGraceStarted,
    ClosurePseudonymisePerDomainComplete,
    ClosureRequested,
    PseudonymiseRequested,
)
from chora_closure_orchestrator.domain.closure import is_known_domain


@dataclass(frozen=True)
class Envelope:
    """Mirror of ``chora.common.v1.EventEnvelope``."""

    event_id: str
    idempotency_key: str
    tenant_id: str
    gcid: str
    occurred_at: _dt.datetime
    published_at: _dt.datetime
    traceparent: str
    tracestate: str
    source_project: str
    source_service: str
    schema_version: int
    correlation_id: str = ""
    causation_id: str = ""


@dataclass(frozen=True)
class PublishedEvent:
    """A single envelope-conforming event recorded in memory."""

    topic: str
    envelope: Envelope
    payload: dict[str, Any] = field(default_factory=dict)


class InMemoryClosurePublisher:
    """Thread-safe in-memory publisher used by tests + MVP."""

    def __init__(self) -> None:
        self._events: list[PublishedEvent] = []
        self._lock = asyncio.Lock()

    def snapshot(self) -> list[PublishedEvent]:
        """Return a copy of the published events."""
        return list(self._events)

    def count(self) -> int:
        return len(self._events)

    async def publish_closure_requested(self, e: ClosureRequested) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, e.traceparent, e.tracestate)
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.requested.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "grace_period_days": e.grace_period_days,
                        "reason": e.reason,
                        "requested_by_gcid": e.requested_by_gcid,
                        "participating_domains": list(e.participating_domains),
                        "requested_at": _iso(e.requested_at),
                        "chora_imda_dimension": "accountability",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_grace_started(self, e: ClosureGraceStarted) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.grace_started.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "grace_expires_at": _iso(e.grace_expires_at),
                        "started_at": _iso(e.started_at),
                        "chora_imda_dimension": "accountability",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_pseudonymise_per_domain_complete(self, e: ClosurePseudonymisePerDomainComplete) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.pseudonymise_per_domain_complete.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "domain_record_counts": dict(e.domain_record_counts),
                        "acked_domains": list(e.acked_domains),
                        "completed_at": _iso(e.completed_at),
                        "chora_imda_dimension": "accountability",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_crypto_shred_complete(self, e: ClosureCryptoShredComplete) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.crypto_shred_complete.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "dek_id": e.dek_id,
                        "kms_operation_id": e.kms_operation_id,
                        "executed_by_gcid": e.executed_by_gcid,
                        "retention_days_by_jurisdiction": dict(e.retention_days_by_jurisdiction),
                        "shredded_at": _iso(e.shredded_at),
                        # Per ADR-141: this is the most senior compliance
                        # signal in the closure pipeline; D1 accountability.
                        "chora_imda_dimension": "accountability",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_closed(self, e: ClosureClosed) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.closed.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "final_state": str(e.final_state.value),
                        "state_transitions": dict(e.state_transitions),
                        "closed_at": _iso(e.closed_at),
                        "chora_imda_dimension": "accountability",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_cancelled(self, e: ClosureCancelled) -> None:
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.closure.cancelled.v1",
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                        "cancelled_by_gcid": e.cancelled_by_gcid,
                        "reason": e.reason,
                        "cancelled_at": _iso(e.cancelled_at),
                        "chora_imda_dimension": "safety_and_robustness",
                        "imda_lifecycle_stage": "runtime",
                    },
                )
            )

    async def publish_pseudonymise_requested(self, e: PseudonymiseRequested) -> None:
        if not is_known_domain(e.domain):
            raise ValueError(f"unknown federated domain: {e.domain!r} (not in REQUIRED_DOMAINS)")
        env = _make_envelope(e.tenant_id, e.gcid, "", "")
        topic = f"chora.{e.domain}.pii.pseudonymise.requested.v1"
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic=topic,
                    envelope=env,
                    payload={
                        "saga_id": e.saga_id,
                        "gcid": e.gcid,
                        "tenant_id": e.tenant_id,
                    },
                )
            )

    async def publish_agent_terminated(self, e: AgentTerminated) -> None:
        """Record AgentTerminated on the canonical
        ``chora.ai_kernel.agent.terminated.v1`` topic. Mirrors the
        TransactionalOutboxPublisher body shape so subscribers stay
        symmetric between unit-test and live wire paths.
        """
        env = _make_envelope(e.tenant_id, e.gcid, e.traceparent, e.tracestate)
        async with self._lock:
            self._events.append(
                PublishedEvent(
                    topic="chora.ai_kernel.agent.terminated.v1",
                    envelope=env,
                    payload={
                        "agent_id": e.agent_id,
                        "agent_agid": e.agent_agid,
                        "execution_id": e.execution_id,
                        "runtime": e.runtime,
                        "termination_code": e.termination_code,
                        "crew_id": e.crew_id,
                        "crew_pattern": e.crew_pattern,
                        "context": {
                            "last_state_node": e.last_state_node,
                            "last_tool_name": e.last_tool_name,
                            "last_error_message": e.last_error_message,
                            "iteration_count": e.iteration_count,
                            "partial_state": dict(e.partial_state),
                            "current_span_id": e.current_span_id,
                        },
                        "terminated_at": _iso(e.terminated_at),
                    },
                )
            )


def _make_envelope(tenant_id: str, gcid: str, traceparent: str, tracestate: str) -> Envelope:
    """Auto-fill the canonical EventEnvelope mandatory fields."""
    eid = _uuidv7_str()
    now = _dt.datetime.now(_dt.UTC)
    src_project = os.getenv("CHORA_SOURCE_PROJECT", "chora-local")
    return Envelope(
        event_id=eid,
        idempotency_key=eid,
        tenant_id=tenant_id,
        gcid=gcid,
        occurred_at=now,
        published_at=now,
        traceparent=traceparent,
        tracestate=tracestate,
        source_project=src_project,
        source_service="chora-closure-orchestrator",
        schema_version=1,
    )


def _uuidv7_str() -> str:
    try:
        import uuid7 as _u7

        return str(_u7.uuid7())
    except ImportError:
        return str(uuid.uuid4())


def _iso(dt: _dt.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.UTC)
    return dt.isoformat()
