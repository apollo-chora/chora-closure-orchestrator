"""TransactionalOutboxPublisher — B.6.2.a producer-side durable emission.

Implements ``ClosureEventPublisher`` by writing each emitted event to
``chora_ai_kernel.closure_outbox_events`` (the table created by migration
``0051_closure_outbox.sql``). A background dispatcher (B.6.2 sub-deliverable;
deferred to subsequent commit) polls ``status='pending`` rows + publishes
to the NATS JetStream event bus + marks rows ``status='published'``.

Per ``feedback_d6_resilience_first_class``: durable emission survives
orchestrator pod-death. Composes with the LangGraph PostgresSaver
checkpoint write (same DB, same connection pool); a saga node that
emits an event AND advances the saga state will land BOTH writes in
``chora_ai_kernel`` before returning.

Per CLAUDE.md cross-cutting rule, each row carries the 11 mandatory
envelope fields. Per the D6.3 multi-tenant chaos directive (user
2026-05-12), ``tenant_id`` is a top-level column for queryable
isolation, not just an envelope field.

POC scope:
* Payload serialization is JSON (Protobuf swap at M14 per
  ``chora-contracts/proto/events/closure/saga.proto``).
* Same-connection-as-PostgresSaver atomicity is NOT enforced at the
  adapter layer — the saga node calls ``publisher.publish_*`` then
  returns + LangGraph writes its checkpoint. Both writes hit the same
  Postgres instance; an outbox-row-without-checkpoint orphan is
  acceptable (dispatcher will still publish; the saga state is
  reconstructed from PostgresSaver on resume).
* idempotency_key is the event_id (UUIDv7). Saga-resume re-emission is
  collapsed by the unique index on ``idempotency_key`` (POC: re-emission
  raises IntegrityError; M14: ON CONFLICT DO NOTHING wrap).
"""

from __future__ import annotations

import datetime as _dt
import json
from typing import Any

import uuid_utils as _uuid_utils

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

SCHEMA_VERSION = "1"


_INSERT_SQL = """
INSERT INTO closure_outbox_events (
    id, saga_id, tenant_id, gcid, event_type, topic,
    payload, envelope, idempotency_key, occurred_at, status
) VALUES (
    %(id)s, %(saga_id)s, %(tenant_id)s, %(gcid)s, %(event_type)s, %(topic)s,
    %(payload)s, %(envelope)s, %(idempotency_key)s, %(occurred_at)s, %(status)s
)
""".strip()


class TransactionalOutboxPublisher:
    """Durable-emission ``ClosureEventPublisher`` backed by
    ``closure_outbox_events`` in ``chora_ai_kernel``.

    The dispatcher (separate concern) reads pending rows + publishes.
    """

    def __init__(
        self,
        *,
        conn: Any,
        source_project: str,
        source_service: str,
    ) -> None:
        if not source_project:
            raise ValueError("source_project required")
        if not source_service:
            raise ValueError("source_service required")
        self._conn = conn
        self._source_project = source_project
        self._source_service = source_service

    # --------------------------------------------------------------
    # ClosureEventPublisher protocol
    # --------------------------------------------------------------

    async def publish_closure_requested(self, e: ClosureRequested) -> None:
        await self._write(
            event_type="closure.requested",
            topic="chora.closure.requested.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.requested_at,
            traceparent=e.traceparent,
            tracestate=e.tracestate,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "grace_period_days": e.grace_period_days,
                "reason": e.reason,
                "requested_by_gcid": e.requested_by_gcid,
                "participating_domains": list(e.participating_domains),
                "requested_at": e.requested_at.isoformat(),
            },
        )

    async def publish_grace_started(self, e: ClosureGraceStarted) -> None:
        await self._write(
            event_type="closure.grace_started",
            topic="chora.closure.grace_started.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.started_at,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "grace_expires_at": e.grace_expires_at.isoformat(),
                "started_at": e.started_at.isoformat(),
            },
        )

    async def publish_pseudonymise_per_domain_complete(self, e: ClosurePseudonymisePerDomainComplete) -> None:
        await self._write(
            event_type="closure.pseudonymise_per_domain_complete",
            topic="chora.closure.pseudonymise_per_domain_complete.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.completed_at,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "domain_record_counts": dict(e.domain_record_counts),
                "acked_domains": list(e.acked_domains),
                "completed_at": e.completed_at.isoformat(),
            },
        )

    async def publish_crypto_shred_complete(self, e: ClosureCryptoShredComplete) -> None:
        await self._write(
            event_type="closure.crypto_shred_complete",
            topic="chora.closure.crypto_shred_complete.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.shredded_at,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "dek_id": e.dek_id,
                "kms_operation_id": e.kms_operation_id,
                "executed_by_gcid": e.executed_by_gcid,
                "retention_days_by_jurisdiction": dict(e.retention_days_by_jurisdiction),
                "shredded_at": e.shredded_at.isoformat(),
            },
        )

    async def publish_closed(self, e: ClosureClosed) -> None:
        await self._write(
            event_type="closure.closed",
            topic="chora.closure.closed.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.closed_at,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "final_state": e.final_state.value,
                "state_transitions": dict(e.state_transitions),
                "closed_at": e.closed_at.isoformat(),
            },
        )

    async def publish_cancelled(self, e: ClosureCancelled) -> None:
        await self._write(
            event_type="closure.cancelled",
            topic="chora.closure.cancelled.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.cancelled_at,
            body={
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
                "cancelled_at": e.cancelled_at.isoformat(),
                "cancelled_by_gcid": e.cancelled_by_gcid,
            },
        )

    async def publish_pseudonymise_requested(self, e: PseudonymiseRequested) -> None:
        await self._write(
            event_type="pii.pseudonymise.requested",
            topic=f"chora.{e.domain}.pii.pseudonymise.requested.v1",
            saga_id=e.saga_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=_dt.datetime.now(_dt.UTC),
            body={
                "domain": e.domain,
                "saga_id": e.saga_id,
                "gcid": e.gcid,
                "tenant_id": e.tenant_id,
            },
        )

    async def publish_agent_terminated(self, e: AgentTerminated) -> None:
        """Emit ``chora.ai_kernel.agent.terminated.v1`` via the outbox.

        Reuses ``saga_id`` column as the execution-correlation key
        (LangGraph thread_id / ADK session_id). The dispatcher routes by
        ``topic``; subscribers correlate by ``saga_id`` and the AgentTerminated
        proto's ``execution_id``.
        """
        await self._write(
            event_type="ai_kernel.agent.terminated",
            topic="chora.ai_kernel.agent.terminated.v1",
            saga_id=e.execution_id,
            tenant_id=e.tenant_id,
            gcid=e.gcid,
            occurred_at=e.terminated_at,
            traceparent=e.traceparent,
            tracestate=e.tracestate,
            body={
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
                "terminated_at": e.terminated_at.isoformat(),
            },
        )

    # --------------------------------------------------------------
    # Internal write
    # --------------------------------------------------------------

    async def _write(
        self,
        *,
        event_type: str,
        topic: str,
        saga_id: str,
        tenant_id: str,
        gcid: str,
        occurred_at: _dt.datetime,
        body: dict[str, Any],
        traceparent: str = "",
        tracestate: str = "",
    ) -> None:
        event_id = str(_uuid_utils.uuid7())
        envelope = {
            "event_id": event_id,
            "idempotency_key": event_id,  # POC: same as event_id; future: caller-supplied
            "tenant_id": tenant_id,
            "gcid": gcid,
            "occurred_at": occurred_at.isoformat(),
            "traceparent": traceparent,
            "tracestate": tracestate,
            "source_project": self._source_project,
            "source_service": self._source_service,
            "schema_version": SCHEMA_VERSION,
        }
        payload = json.dumps(body, default=str).encode("utf-8")
        async with self._conn.cursor() as cur:
            await cur.execute(
                _INSERT_SQL,
                {
                    "id": event_id,
                    "saga_id": saga_id,
                    "tenant_id": tenant_id,
                    "gcid": gcid,
                    "event_type": event_type,
                    "topic": topic,
                    "payload": payload,
                    "envelope": json.dumps(envelope),
                    "idempotency_key": event_id,
                    "occurred_at": occurred_at,
                    "status": "pending",
                },
            )


__all__ = ["TransactionalOutboxPublisher", "SCHEMA_VERSION"]
