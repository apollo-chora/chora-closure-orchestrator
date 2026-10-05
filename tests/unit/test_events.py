"""Closure event publisher tests.

Verifies the canonical ``chora.closure.*`` topic namespace per the M13
contracts reconciliation (S0). Each event embeds the EventEnvelope (event_id
UUIDv7, idempotency_key, tenant_id, gcid, occurred_at, published_at,
traceparent, tracestate, source_project, source_service, schema_version).
"""

from __future__ import annotations

import datetime as _dt

import pytest

from chora_closure_orchestrator.adapter.events import (
    AgentTerminated,
    ClosureCancelled,
    ClosureClosed,
    ClosureCryptoShredComplete,
    ClosureGraceStarted,
    ClosurePseudonymisePerDomainComplete,
    ClosureRequested,
    InMemoryClosurePublisher,
    PseudonymiseRequested,
)
from chora_closure_orchestrator.domain.closure.state import State

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


class TestInMemoryClosurePublisher:
    @pytest.mark.asyncio
    async def test_publish_closure_requested(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                reason="test",
                requested_by_gcid=GCID,
                participating_domains=["creation", "consumption"],
                requested_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert len(events) == 1
        # Canonical topic namespace per S0 contracts reconciliation.
        assert events[0].topic == "chora.closure.requested.v1"

    @pytest.mark.asyncio
    async def test_publish_grace_started(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_grace_started(
            ClosureGraceStarted(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                grace_expires_at=_dt.datetime.now(_dt.UTC),
                started_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert events[0].topic == "chora.closure.grace_started.v1"

    @pytest.mark.asyncio
    async def test_publish_pseudonymise_per_domain_complete(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_pseudonymise_per_domain_complete(
            ClosurePseudonymisePerDomainComplete(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                domain_record_counts={"creation": 5, "consumption": 3},
                acked_domains=["creation", "consumption"],
                completed_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert events[0].topic == "chora.closure.pseudonymise_per_domain_complete.v1"

    @pytest.mark.asyncio
    async def test_publish_crypto_shred_complete(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_crypto_shred_complete(
            ClosureCryptoShredComplete(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                dek_id="kms-dek-12345",
                kms_operation_id="op-67890",
                executed_by_gcid=GCID,
                retention_days_by_jurisdiction={"SG": 2557, "EU": 2557},
                shredded_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert events[0].topic == "chora.closure.crypto_shred_complete.v1"

    @pytest.mark.asyncio
    async def test_publish_closed(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_closed(
            ClosureClosed(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                final_state=State.CRYPTO_SHREDDED,
                state_transitions={"closing": "2026-01-01T00:00:00Z"},
                closed_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert events[0].topic == "chora.closure.closed.v1"

    @pytest.mark.asyncio
    async def test_publish_cancelled(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_cancelled(
            ClosureCancelled(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                cancelled_by_gcid=GCID,
                reason="changed_mind",
                cancelled_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        assert events[0].topic == "chora.closure.cancelled.v1"

    @pytest.mark.asyncio
    async def test_pseudonymise_requested_per_domain_topic(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_pseudonymise_requested(
            PseudonymiseRequested(
                domain="creation",
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
            )
        )
        events = pub.snapshot()
        # Per skill: chora.{domain}.account.pseudonymised.v1 for ack;
        # request fan-out goes on chora.{domain}.pii.pseudonymise.requested.v1
        assert events[0].topic == ("chora.creation.pii.pseudonymise.requested.v1")

    @pytest.mark.asyncio
    async def test_pseudonymise_requested_unknown_domain_raises(self) -> None:
        pub = InMemoryClosurePublisher()
        with pytest.raises(ValueError):
            await pub.publish_pseudonymise_requested(
                PseudonymiseRequested(
                    domain="ai_kernel",  # not in REQUIRED_DOMAINS
                    saga_id="saga-1",
                    gcid=GCID,
                    tenant_id=TENANT,
                )
            )

    @pytest.mark.asyncio
    async def test_envelope_has_mandatory_fields(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id="saga-1",
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                reason="test",
                requested_by_gcid=GCID,
                participating_domains=["creation"],
                requested_at=_dt.datetime.now(_dt.UTC),
            )
        )
        events = pub.snapshot()
        env = events[0].envelope
        assert env.event_id != ""
        assert env.idempotency_key != ""
        assert env.tenant_id == TENANT
        assert env.gcid == GCID
        assert env.source_service == "chora-closure-orchestrator"
        assert env.schema_version >= 1


class TestInMemoryAgentTerminated:
    """``publish_agent_terminated`` records the AgentTerminated event on the
    canonical ``chora.ai_kernel.agent.terminated.v1`` topic with the
    structured-terminate payload shape.
    """

    @pytest.mark.asyncio
    async def test_publish_success_termination(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id="thread-1",
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT,
                gcid=GCID,
                terminated_at=_dt.datetime.now(_dt.UTC),
                crew_pattern="P8_LANGGRAPH_STATEFUL_SAGA",
            )
        )
        events = pub.snapshot()
        assert len(events) == 1
        assert events[0].topic == "chora.ai_kernel.agent.terminated.v1"
        payload = events[0].payload
        assert payload["agent_id"] == "closure_saga"
        assert payload["execution_id"] == "thread-1"
        assert payload["termination_code"] == "AGENT_TERMINATION_CODE_SUCCESS"
        assert payload["runtime"] == "AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON"
        assert payload["crew_pattern"] == "P8_LANGGRAPH_STATEFUL_SAGA"

    @pytest.mark.asyncio
    async def test_publish_failure_termination_with_context(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id="thread-2",
                termination_code="AGENT_TERMINATION_CODE_RUNTIME_ERROR",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT,
                gcid=GCID,
                terminated_at=_dt.datetime.now(_dt.UTC),
                last_state_node="advance_to_pseudonymized",
                last_error_message="psycopg.OperationalError: bang",
                iteration_count=2,
                partial_state={"current_state": "CLOSING"},
                current_span_id="0123456789abcdef",
            )
        )
        events = pub.snapshot()
        ctx = events[0].payload["context"]
        assert ctx["last_state_node"] == "advance_to_pseudonymized"
        assert ctx["last_error_message"].startswith("psycopg.OperationalError")
        assert ctx["iteration_count"] == 2
        assert ctx["partial_state"] == {"current_state": "CLOSING"}
        assert ctx["current_span_id"] == "0123456789abcdef"

    @pytest.mark.asyncio
    async def test_envelope_present_with_tenant_attribution(self) -> None:
        pub = InMemoryClosurePublisher()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id="thread-3",
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT,
                gcid=GCID,
                terminated_at=_dt.datetime.now(_dt.UTC),
                traceparent="00-trace-span-03",
                tracestate="vendor=chora",
            )
        )
        env = pub.snapshot()[0].envelope
        assert env.tenant_id == TENANT
        assert env.gcid == GCID
        assert env.source_service == "chora-closure-orchestrator"
        assert env.traceparent == "00-trace-span-03"
        assert env.tracestate == "vendor=chora"
