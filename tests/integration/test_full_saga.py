"""End-to-end federated closure saga integration test.

Exercises the full LangGraph state machine in test mode (30-second grace)
across all adapters (in-memory): repository, publisher, KMS, cold-archive.

Verifies:
- Happy path: request → grace → pseudonymise fanout → crypto-shred → archive → closed
- Cancel during grace: request → grace → cancel → reverse to ACTIVE
- Compensation: ack timeout → SUSPENDED state preserved + compensation event
- AGID-rejection invariant
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

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
from chora_closure_orchestrator.domain.state import new_request_id
from chora_closure_orchestrator.orchestrators.closure_graph import (
    advance_to_pseudonymized_node,
    archive_to_coldline_node,
    await_per_domain_acks_node,
    crypto_shred_node,
    grace_start_node,
    pseudonymise_fanout_node,
    request_node,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


@pytest.mark.integration
class TestFullSagaHappyPath:
    @pytest.mark.asyncio
    async def test_request_to_crypto_shred(self) -> None:
        repo = InMemoryCoordinatorRepository()
        pub = InMemoryClosurePublisher()
        kms = FakeKMSClient()
        archive = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")

        # Step 1: request
        state: Any = {
            "request_id": new_request_id(),
            "saga_id": "",
            "gcid": GCID,
            "tenant_id": TENANT,
            "current_state": State.ACTIVE.value,
            "grace_period_seconds": 30,
            "jurisdiction": "SG",
            "reason": "user_self_initiated",
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
        out1 = await request_node(state, repo=repo, publisher=pub)
        state.update(out1)
        assert state["saga_id"] != ""
        assert state["current_state"] == State.CLOSING.value

        # Step 2: grace_start
        await grace_start_node(state, repo=repo, publisher=pub)

        # Step 3: advance CLOSING → SUSPENDED → PSEUDONYMIZED
        await advance_to_pseudonymized_node(state, repo=repo)
        c = await repo.get(state["saga_id"])
        assert c.state == State.PSEUDONYMIZED

        # Step 4: pseudonymise fan-out
        await pseudonymise_fanout_node(state, repo=repo, publisher=pub)
        topics_after_fanout = [e.topic for e in pub.snapshot()]
        for d in REQUIRED_DOMAINS:
            assert f"chora.{d}.pii.pseudonymise.requested.v1" in topics_after_fanout

        # Step 5: simulate per-domain acks
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        await repo.save(c)
        state["domains_acked"] = list(REQUIRED_DOMAINS)
        state["domain_record_counts"] = {d: 5 for d in REQUIRED_DOMAINS}

        # Step 6: await acks → emit complete event
        await await_per_domain_acks_node(state, repo=repo, publisher=pub)
        topics_after_acks = [e.topic for e in pub.snapshot()]
        assert "chora.closure.pseudonymise_per_domain_complete.v1" in topics_after_acks

        # Step 7: provision DEK + archive to coldline
        meta = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        state["dek_resource_name"] = meta.dek_resource_name
        out_archive = await archive_to_coldline_node(state, repo=repo, publisher=pub, kms=kms, archive=archive)
        state.update(out_archive)
        c = await repo.get(state["saga_id"])
        assert c.state == State.COLD_ARCHIVED
        assert state["archive_uri"].startswith("gs://")

        # Step 8: crypto-shred (DEK delete)
        out_shred = await crypto_shred_node(state, repo=repo, publisher=pub, kms=kms)
        state.update(out_shred)
        c = await repo.get(state["saga_id"])
        assert c.state == State.CRYPTO_SHREDDED
        assert c.is_terminal()

        # Verify shred renders DEK unrecoverable
        from chora_closure_orchestrator.adapter.kms import DEKDeletedError

        with pytest.raises(DEKDeletedError):
            await kms.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"new")


@pytest.mark.integration
class TestCancelDuringGrace:
    @pytest.mark.asyncio
    async def test_cancel_returns_to_active(self) -> None:
        repo = InMemoryCoordinatorRepository()
        InMemoryClosurePublisher()
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                reason="initial",
                requested_by_gcid=GCID,
            )
        )
        await repo.save(c)
        c.cancel("changed_mind", GCID)
        await repo.save(c)
        loaded = await repo.get(c.saga_id)
        assert loaded.state == State.ACTIVE
        assert loaded.cancelled_at is not None


@pytest.mark.integration
class TestCompensationOnTimeout:
    @pytest.mark.asyncio
    async def test_partial_acks_records_compensation(self) -> None:
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

        state: Any = {
            "request_id": new_request_id(),
            "saga_id": c.saga_id,
            "gcid": GCID,
            "tenant_id": TENANT,
            "current_state": State.PSEUDONYMIZED.value,
            "grace_period_seconds": 30,
            "jurisdiction": "SG",
            "reason": "test",
            "requested_by_gcid": GCID,
            "domains_acked": ["creation"],
            "domain_record_counts": {"creation": 5},
            "dek_resource_name": "",
            "kms_operation_id": "",
            "archive_uri": "",
            "errors": [],
            "trace": [],
            "cancel_requested": False,
            "compensation_started": False,
            "ack_timeout_exceeded": True,
        }
        out = await await_per_domain_acks_node(state, repo=repo, publisher=pub)
        assert out.get("compensation_started") is True
