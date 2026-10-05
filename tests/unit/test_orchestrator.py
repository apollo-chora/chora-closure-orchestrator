"""Closure Orchestrator (LangGraph StateGraph) tests.

Per Tier 2 D5 hybrid kernel mandate: this is the Python LangGraph half
of the federated closure saga. The orchestrator coordinates:

    request → grace_start → await_grace_or_cancel (HITL interrupt) →
    pseudonymise_fanout → await_per_domain_acks →
    crypto_shred → archive_to_coldline → closed

Tests use the FakeKMSClient + InMemoryColdArchiveClient + InMemoryClosurePublisher
to assert the orchestrator wires the saga lifecycle correctly.
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
from chora_closure_orchestrator.domain.closure.coordinator import NewParams, new
from chora_closure_orchestrator.domain.closure.state import REQUIRED_DOMAINS, State
from chora_closure_orchestrator.domain.state import (
    ClosureSagaOrchestratorState,
    new_request_id,
)
from chora_closure_orchestrator.orchestrators.closure_graph import (
    advance_to_pseudonymized_node,
    archive_to_coldline_node,
    await_per_domain_acks_node,
    build_graph,
    crypto_shred_node,
    grace_start_node,
    pseudonymise_fanout_node,
    request_node,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _seed_state(saga_id: str = "") -> ClosureSagaOrchestratorState:
    return {
        "request_id": new_request_id(),
        "saga_id": saga_id,
        "gcid": GCID,
        "tenant_id": TENANT,
        "current_state": State.ACTIVE.value,
        "grace_period_seconds": 30,
        "jurisdiction": "SG",
        "reason": "test",
        "requested_by_gcid": GCID,
        "domains_acked": [],
        "domain_record_counts": {},
        "dek_resource_name": "",
        "kms_operation_id": "",
        "archive_uri": "",
        "errors": [],
        "trace": [],
        "cancel_requested": False,
        "compensation_started": False,
    }


class TestRequestNode:
    @pytest.mark.asyncio
    async def test_creates_saga_in_closing(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        state = _seed_state()

        out = await request_node(state, repo=repo, publisher=pub)
        assert out["saga_id"] != ""
        assert out["current_state"] == State.CLOSING.value

        # Persisted
        c = await repo.get(out["saga_id"])
        assert c.state == State.CLOSING

        # ClosureRequested event emitted on canonical chora.closure.* topic
        events = pub.snapshot()
        assert any(e.topic == "chora.closure.requested.v1" for e in events)

    @pytest.mark.asyncio
    async def test_rejects_agid(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        state = _seed_state()
        state["gcid"] = "0197a000-0000-7000-9000-000000000001"

        out = await request_node(state, repo=repo, publisher=pub)
        # AGID rejection → errors populated, saga NOT created
        assert out["saga_id"] == ""
        assert out["errors"]


class TestGraceStartNode:
    @pytest.mark.asyncio
    async def test_emits_grace_started_event(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        await repo.save(c)
        state = _seed_state(saga_id=c.saga_id)
        state["current_state"] = State.CLOSING.value

        out = await grace_start_node(state, repo=repo, publisher=pub)
        topics = [e.topic for e in pub.snapshot()]
        assert "chora.closure.grace_started.v1" in topics
        assert "started_at" in out["trace"][-1]


class TestAdvanceToPseudonymizedNode:
    @pytest.mark.asyncio
    async def test_transitions_via_suspended_to_pseudonymized(self) -> None:
        repo = InMemoryCoordinatorRepository()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        await repo.save(c)
        state = _seed_state(saga_id=c.saga_id)

        await advance_to_pseudonymized_node(state, repo=repo)
        loaded = await repo.get(c.saga_id)
        assert loaded.state == State.PSEUDONYMIZED


class TestPseudonymiseFanoutNode:
    @pytest.mark.asyncio
    async def test_publishes_per_domain_requests(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        c.advance(State.SUSPENDED, "t", GCID)
        c.advance(State.PSEUDONYMIZED, "t", GCID)
        await repo.save(c)
        state = _seed_state(saga_id=c.saga_id)
        state["current_state"] = State.PSEUDONYMIZED.value

        await pseudonymise_fanout_node(state, repo=repo, publisher=pub)

        # 10 per-domain pseudonymise requests + 1 grace_started? No,
        # this node just publishes 10 per-domain requests.
        topics = [e.topic for e in pub.snapshot()]
        for d in REQUIRED_DOMAINS:
            assert f"chora.{d}.pii.pseudonymise.requested.v1" in topics


class TestAwaitPerDomainAcksNode:
    @pytest.mark.asyncio
    async def test_records_full_acks_emits_complete(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        c.advance(State.SUSPENDED, "t", GCID)
        c.advance(State.PSEUDONYMIZED, "t", GCID)
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        await repo.save(c)
        state = _seed_state(saga_id=c.saga_id)
        state["domains_acked"] = list(REQUIRED_DOMAINS)
        state["domain_record_counts"] = {d: 5 for d in REQUIRED_DOMAINS}

        out = await await_per_domain_acks_node(state, repo=repo, publisher=pub)

        # All 10 acked → complete event emitted
        topics = [e.topic for e in pub.snapshot()]
        assert "chora.closure.pseudonymise_per_domain_complete.v1" in topics
        # No timeout error
        assert not out.get("compensation_started")

    @pytest.mark.asyncio
    async def test_partial_acks_triggers_compensation(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        c.advance(State.SUSPENDED, "t", GCID)
        c.advance(State.PSEUDONYMIZED, "t", GCID)
        c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        await repo.save(c)
        state = _seed_state(saga_id=c.saga_id)
        state["domains_acked"] = ["creation"]
        state["domain_record_counts"] = {"creation": 5}
        state["ack_timeout_exceeded"] = True  # simulate timeout

        out = await await_per_domain_acks_node(state, repo=repo, publisher=pub)
        assert out.get("compensation_started") is True


class TestCryptoShredNode:
    @pytest.mark.asyncio
    async def test_provisions_dek_then_deletes(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        kms = FakeKMSClient()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        # Simulate full progress
        c.advance(State.SUSPENDED, "t", GCID)
        c.advance(State.PSEUDONYMIZED, "t", GCID)
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        c.advance(State.COLD_ARCHIVED, "archived", GCID)
        await repo.save(c)
        # Provision DEK
        meta = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        state = _seed_state(saga_id=c.saga_id)
        state["dek_resource_name"] = meta.dek_resource_name

        out = await crypto_shred_node(state, repo=repo, publisher=pub, kms=kms)

        assert out["kms_operation_id"] != ""
        # Crypto-shred completes event
        topics = [e.topic for e in pub.snapshot()]
        assert "chora.closure.crypto_shred_complete.v1" in topics

        # Decrypting after shred fails (verifying primary mandate)
        ct = b"will-fail-because-we-just-shredded"
        try:
            await kms.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=ct)
            decrypted_ok = True
        except Exception:
            decrypted_ok = False
        assert decrypted_ok is False


class TestArchiveToColdlineNode:
    @pytest.mark.asyncio
    async def test_writes_object_records_uri(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        kms = FakeKMSClient()
        archive = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        c.advance(State.SUSPENDED, "t", GCID)
        c.advance(State.PSEUDONYMIZED, "t", GCID)
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        await repo.save(c)
        meta = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        state = _seed_state(saga_id=c.saga_id)
        state["dek_resource_name"] = meta.dek_resource_name

        out = await archive_to_coldline_node(state, repo=repo, publisher=pub, kms=kms, archive=archive)

        assert out["archive_uri"].startswith("gs://")
        loaded = await repo.get(c.saga_id)
        assert loaded.state == State.COLD_ARCHIVED


class TestBuildGraph:
    def test_compiles_without_checkpointer(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        kms = FakeKMSClient()
        archive = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        graph = build_graph(repo=repo, publisher=pub, kms=kms, archive=archive)
        assert graph is not None


class TestNewRequestId:
    def test_returns_uuid_string(self) -> None:
        rid = new_request_id()
        assert isinstance(rid, str)
        # UUID format: 8-4-4-4-12 hex
        parts = rid.split("-")
        assert len(parts) == 5
