"""Tests for ``TransactionalOutboxPublisher`` — the B.6.2.a producer-side
durable-emission adapter.

The adapter implements ``ClosureEventPublisher`` (drop-in replacement for
``InMemoryClosurePublisher``) and writes each emitted event to
``chora_ai_kernel.closure_outbox_events`` so that:

1. Event emission is durable — survives orchestrator pod-death + engine
   redeploy (D6 first-class resilience per
   ``feedback_d6_resilience_first_class``).
2. Multi-tenant + multi-workflow isolation is enforced at the row level —
   the D6.3 expanded scope requires tenant_id to be a queryable column,
   not just a payload field.
3. Idempotency is enforced via a unique index on ``idempotency_key``.
4. Dispatcher (background) polls ``status='pending'`` rows + publishes to
   the NATS event bus, then marks ``status='published'``.

Unit tests use a mocked psycopg AsyncConnection. Live DB integration
exists in ``tests/integration/test_outbox_writer_live.py`` (marked
``live`` — skipped without a live broker).
"""

from __future__ import annotations

import datetime as _dt
import json
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.events.payloads import (
    AgentTerminated,
    ClosureClosed,
    ClosureCryptoShredComplete,
    ClosureGraceStarted,
    ClosurePseudonymisePerDomainComplete,
    ClosureRequested,
    PseudonymiseRequested,
)
from chora_closure_orchestrator.adapter.events.publisher_outbox import (
    TransactionalOutboxPublisher,
)
from chora_closure_orchestrator.domain.closure import State

# ---------------------------------------------------------------------------
# Mock connection helpers
# ---------------------------------------------------------------------------


class _MockCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    async def __aenter__(self) -> _MockCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))


class _MockConn:
    def __init__(self) -> None:
        self.cursor_obj = _MockCursor()
        self.committed = False

    def cursor(self) -> _MockCursor:
        return self.cursor_obj

    async def commit(self) -> None:
        self.committed = True


def _now() -> _dt.datetime:
    return _dt.datetime(2026, 5, 12, 8, 0, 0, tzinfo=_dt.UTC)


SAGA_ID = "d42940c4-3edf-4a3e-83ed-938c5fef441d"
TENANT_ID = "01970000-0000-7000-8000-000000000001"
GCID = "01970000-0000-7000-9000-000000000001"


# ---------------------------------------------------------------------------
# Constructor + protocol shape
# ---------------------------------------------------------------------------


class TestTransactionalOutboxPublisherInit:
    def test_constructor_captures_connection(self) -> None:
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        assert pub._conn is conn  # noqa: SLF001
        assert pub._source_project == "chora-local"  # noqa: SLF001
        assert pub._source_service == "chora-closure-orchestrator"  # noqa: SLF001

    def test_rejects_empty_source_project(self) -> None:
        with pytest.raises(ValueError, match="source_project required"):
            TransactionalOutboxPublisher(
                conn=_MockConn(),
                source_project="",
                source_service="x",
            )

    def test_rejects_empty_source_service(self) -> None:
        with pytest.raises(ValueError, match="source_service required"):
            TransactionalOutboxPublisher(
                conn=_MockConn(),
                source_project="x",
                source_service="",
            )


# ---------------------------------------------------------------------------
# ClosureRequested — the canonical write-path test
# ---------------------------------------------------------------------------


class TestPublishClosureRequested:
    @pytest.mark.asyncio
    async def test_writes_one_row_to_outbox(self) -> None:
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        event = ClosureRequested(
            saga_id=SAGA_ID,
            gcid=GCID,
            tenant_id=TENANT_ID,
            grace_period_days=30,
            reason="user-requested",
            requested_by_gcid=GCID,
            participating_domains=["creation", "consumption", "delivery"],
            requested_at=_now(),
            traceparent="00-trace-span-01",
            tracestate="vendor=chora",
        )

        await pub.publish_closure_requested(event)

        assert len(conn.cursor_obj.executed) == 1
        sql, params = conn.cursor_obj.executed[0]
        assert "INSERT INTO closure_outbox_events" in sql

    @pytest.mark.asyncio
    async def test_row_carries_correct_topic_event_type(self) -> None:
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
            )
        )

        _, params = conn.cursor_obj.executed[0]
        params_dict = _params_to_dict(params)
        assert params_dict["topic"] == "chora.closure.requested.v1"
        assert params_dict["event_type"] == "closure.requested"

    @pytest.mark.asyncio
    async def test_row_carries_tenant_isolation_columns(self) -> None:
        """D6.3 multi-tenant chaos — tenant_id MUST be a queryable column."""
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
            )
        )

        _, params = conn.cursor_obj.executed[0]
        params_dict = _params_to_dict(params)
        assert params_dict["tenant_id"] == TENANT_ID
        assert params_dict["saga_id"] == SAGA_ID
        assert params_dict["gcid"] == GCID

    @pytest.mark.asyncio
    async def test_envelope_carries_mandatory_fields(self) -> None:
        """Per CLAUDE.md cross-cutting rule, envelope MUST carry the 11
        mandatory fields. Test the 9 stable ones at write time
        (published_at is NULL until dispatched)."""
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
                traceparent="00-trace-span-01",
                tracestate="vendor=chora",
            )
        )

        _, params = conn.cursor_obj.executed[0]
        params_dict = _params_to_dict(params)
        envelope = json.loads(params_dict["envelope"])
        for required in (
            "event_id",
            "idempotency_key",
            "tenant_id",
            "gcid",
            "occurred_at",
            "traceparent",
            "tracestate",
            "source_project",
            "source_service",
            "schema_version",
        ):
            assert required in envelope, f"envelope missing field: {required}"
        assert envelope["source_project"] == "chora-local"
        assert envelope["source_service"] == "chora-closure-orchestrator"
        assert envelope["traceparent"] == "00-trace-span-01"

    @pytest.mark.asyncio
    async def test_status_pending_on_initial_write(self) -> None:
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
            )
        )

        _, params = conn.cursor_obj.executed[0]
        params_dict = _params_to_dict(params)
        # status is set by the DEFAULT clause; the INSERT either omits
        # status entirely (relying on default) OR explicitly passes
        # 'pending'. Either way, no published_at should be set.
        assert params_dict.get("status", "pending") == "pending"

    @pytest.mark.asyncio
    async def test_idempotency_key_unique_per_event(self) -> None:
        """Two distinct events get distinct idempotency keys (UUIDv7 random)."""
        conn = _MockConn()
        pub = TransactionalOutboxPublisher(
            conn=conn,
            source_project="chora-local",
            source_service="chora-closure-orchestrator",
        )

        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
            )
        )
        await pub.publish_closure_requested(
            ClosureRequested(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="x",
                requested_by_gcid=GCID,
                participating_domains=[],
                requested_at=_now(),
            )
        )

        keys = [_params_to_dict(p)["idempotency_key"] for _, p in conn.cursor_obj.executed]
        assert keys[0] != keys[1]


# ---------------------------------------------------------------------------
# Coverage of every protocol method
# ---------------------------------------------------------------------------


class TestEveryProtocolMethodWritesToOutbox:
    @pytest.mark.asyncio
    async def test_publish_grace_started(self) -> None:
        conn, pub = _fresh()
        await pub.publish_grace_started(
            ClosureGraceStarted(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                grace_expires_at=_now(),
                started_at=_now(),
            )
        )
        assert _last_topic(conn) == "chora.closure.grace_started.v1"

    @pytest.mark.asyncio
    async def test_publish_pseudonymise_per_domain_complete(self) -> None:
        conn, pub = _fresh()
        await pub.publish_pseudonymise_per_domain_complete(
            ClosurePseudonymisePerDomainComplete(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                domain_record_counts={"creation": 7},
                acked_domains=["creation"],
                completed_at=_now(),
            )
        )
        assert _last_topic(conn) == "chora.closure.pseudonymise_per_domain_complete.v1"

    @pytest.mark.asyncio
    async def test_publish_crypto_shred_complete(self) -> None:
        conn, pub = _fresh()
        await pub.publish_crypto_shred_complete(
            ClosureCryptoShredComplete(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                dek_id="dek-123",
                kms_operation_id="op-456",
                executed_by_gcid=GCID,
                retention_days_by_jurisdiction={"SG": 1825},
                shredded_at=_now(),
            )
        )
        assert _last_topic(conn) == "chora.closure.crypto_shred_complete.v1"

    @pytest.mark.asyncio
    async def test_publish_closed(self) -> None:
        conn, pub = _fresh()
        await pub.publish_closed(
            ClosureClosed(
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
                final_state=State.CRYPTO_SHREDDED,
            )
        )
        assert _last_topic(conn) == "chora.closure.closed.v1"

    @pytest.mark.asyncio
    async def test_publish_pseudonymise_requested(self) -> None:
        conn, pub = _fresh()
        await pub.publish_pseudonymise_requested(
            PseudonymiseRequested(
                domain="consumption",
                saga_id=SAGA_ID,
                gcid=GCID,
                tenant_id=TENANT_ID,
            )
        )
        # Topic is per-domain — chora.{domain}.pii.pseudonymise.requested.v1
        assert _last_topic(conn) == "chora.consumption.pii.pseudonymise.requested.v1"


# ---------------------------------------------------------------------------
# AgentTerminated — W3 foundation Phase 3 (2026-05-12)
# ---------------------------------------------------------------------------


EXECUTION_ID = "thread-01970000-0000-7000-a000-000000000001"


class TestPublishAgentTerminated:
    """``publish_agent_terminated`` writes one outbox row to the canonical
    ``chora.ai_kernel.agent.terminated.v1`` topic. Reuses the saga_id
    column as the execution-correlation key (LangGraph thread_id).
    """

    @pytest.mark.asyncio
    async def test_writes_one_row_to_outbox(self) -> None:
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
            )
        )
        assert len(conn.cursor_obj.executed) == 1
        sql, _ = conn.cursor_obj.executed[0]
        assert "INSERT INTO closure_outbox_events" in sql

    @pytest.mark.asyncio
    async def test_row_carries_canonical_topic_and_event_type(self) -> None:
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
            )
        )
        params = _params_to_dict(conn.cursor_obj.executed[0][1])
        assert params["topic"] == "chora.ai_kernel.agent.terminated.v1"
        assert params["event_type"] == "ai_kernel.agent.terminated"

    @pytest.mark.asyncio
    async def test_execution_id_lands_in_saga_id_column(self) -> None:
        """saga_id column is the execution-correlation key for AgentTerminated.

        The dispatcher routes by `topic`; saga_id is the joinable
        correlation column on which subscribers correlate the
        AgentTerminated event with other saga rows. For the AgentTerminated
        contract, that correlation key is the LangGraph thread_id.
        """
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
            )
        )
        params = _params_to_dict(conn.cursor_obj.executed[0][1])
        assert params["saga_id"] == EXECUTION_ID
        assert params["tenant_id"] == TENANT_ID
        assert params["gcid"] == GCID

    @pytest.mark.asyncio
    async def test_body_carries_agent_terminated_shape(self) -> None:
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_RUNTIME_ERROR",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
                agent_agid="agid-closure-001",
                crew_id="crew-1",
                crew_pattern="P8_LANGGRAPH_STATEFUL_SAGA",
                last_state_node="advance_to_pseudonymized",
                last_error_message="psycopg.OperationalError: connection refused",
                iteration_count=4,
                partial_state={"current_state": "CLOSING"},
                current_span_id="abc1234567890def",
                traceparent="00-trace-span-01",
                tracestate="vendor=chora",
            )
        )
        params = _params_to_dict(conn.cursor_obj.executed[0][1])
        body = json.loads(params["payload"].decode("utf-8"))
        assert body["agent_id"] == "closure_saga"
        assert body["execution_id"] == EXECUTION_ID
        assert body["termination_code"] == "AGENT_TERMINATION_CODE_RUNTIME_ERROR"
        assert body["runtime"] == "AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON"
        assert body["agent_agid"] == "agid-closure-001"
        assert body["crew_id"] == "crew-1"
        assert body["crew_pattern"] == "P8_LANGGRAPH_STATEFUL_SAGA"
        ctx = body["context"]
        assert ctx["last_state_node"] == "advance_to_pseudonymized"
        assert ctx["last_error_message"].startswith("psycopg.OperationalError")
        assert ctx["iteration_count"] == 4
        assert ctx["partial_state"] == {"current_state": "CLOSING"}
        assert ctx["current_span_id"] == "abc1234567890def"

    @pytest.mark.asyncio
    async def test_envelope_carries_traceparent_tracestate(self) -> None:
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="closure_saga",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_SUCCESS",
                runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
                traceparent="00-trace-span-02",
                tracestate="vendor=chora",
            )
        )
        params = _params_to_dict(conn.cursor_obj.executed[0][1])
        envelope = json.loads(params["envelope"])
        assert envelope["traceparent"] == "00-trace-span-02"
        assert envelope["tracestate"] == "vendor=chora"
        assert envelope["tenant_id"] == TENANT_ID

    @pytest.mark.asyncio
    async def test_failure_code_round_trips(self) -> None:
        conn, pub = _fresh()
        await pub.publish_agent_terminated(
            AgentTerminated(
                agent_id="familiar_companion",
                execution_id=EXECUTION_ID,
                termination_code="AGENT_TERMINATION_CODE_MAX_ITERATIONS",
                runtime="AGENT_EXECUTION_RUNTIME_ADK_GO",
                tenant_id=TENANT_ID,
                gcid=GCID,
                terminated_at=_now(),
                iteration_count=100,
            )
        )
        params = _params_to_dict(conn.cursor_obj.executed[0][1])
        body = json.loads(params["payload"].decode("utf-8"))
        assert body["termination_code"] == "AGENT_TERMINATION_CODE_MAX_ITERATIONS"
        assert body["runtime"] == "AGENT_EXECUTION_RUNTIME_ADK_GO"
        assert body["context"]["iteration_count"] == 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh() -> tuple[_MockConn, TransactionalOutboxPublisher]:
    conn = _MockConn()
    pub = TransactionalOutboxPublisher(
        conn=conn,
        source_project="chora-local",
        source_service="chora-closure-orchestrator",
    )
    return conn, pub


def _last_topic(conn: _MockConn) -> str:
    assert conn.cursor_obj.executed
    _, params = conn.cursor_obj.executed[-1]
    return _params_to_dict(params)["topic"]


def _params_to_dict(params: Any) -> dict[str, Any]:
    """The adapter binds params as a dict (psycopg supports %(name)s); the
    tests assert on that dict shape. If the adapter later switches to a
    positional tuple, this helper must be updated to keep tests stable."""
    if isinstance(params, dict):
        return params
    if isinstance(params, (list, tuple)):
        # Positional binding — the test layer doesn't know order; the
        # adapter is expected to bind by name. Fail fast.
        raise AssertionError("Adapter must bind INSERT params by name (dict), not positional")
    raise AssertionError(f"unexpected params type: {type(params)}")
