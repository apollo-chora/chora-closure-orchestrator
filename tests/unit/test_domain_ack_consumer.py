"""DomainAckHandler — consumes the per-domain pseudonymisation acks on
``chora.{domain}.account.pseudonymised.v1`` (reconciled canonical name per
CHO-1719 gap 4: taxonomy-correct ``{aggregate}.{event_type}`` — matches the
asyncapi contract + skill; the Go domain
services were renamed to emit it).

Wraps into ``AckAfterProcessingSubscriber`` for the pull loop: raising
``TransientError`` NACKs (redeliver — e.g. the ack raced the saga write).
"""

from __future__ import annotations

import datetime as _dt
import json

import pytest

from chora_closure_orchestrator.adapter.pubsub.ack_consumer import (
    ACK_TOPIC_TEMPLATE,
    DomainAckHandler,
    ack_topic_for_domain,
)
from chora_closure_orchestrator.adapter.pubsub.subscriber import TransientError
from chora_closure_orchestrator.adapter.repository import (
    InMemoryCoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import NewParams, new

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _payload(saga_id: str, domain: str = "creation") -> bytes:
    return json.dumps(
        {
            "saga_id": saga_id,
            "gcid": GCID,
            "tenant_id": TENANT,
            "domain": domain,
            "completed_at": _dt.datetime.now(_dt.UTC).isoformat(),
        }
    ).encode("utf-8")


async def _saved_saga(repo: InMemoryCoordinatorRepository) -> str:
    c = new(
        NewParams(
            gcid=GCID,
            tenant_id=TENANT,
            grace_period_days=30,
            requested_by_gcid=GCID,
        )
    )
    await repo.save(c)
    return c.saga_id


class TestTopicNaming:
    def test_template_is_taxonomy_correct(self) -> None:
        assert ACK_TOPIC_TEMPLATE == "chora.{domain}.account.pseudonymised.v1"

    def test_ack_topic_for_domain(self) -> None:
        assert ack_topic_for_domain("identity") == "chora.identity.account.pseudonymised.v1"


class TestDomainAckHandler:
    @pytest.mark.asyncio
    async def test_records_ack(self) -> None:
        repo = InMemoryCoordinatorRepository()
        saga_id = await _saved_saga(repo)
        handler = DomainAckHandler(repo=repo)

        await handler.handle({}, _payload(saga_id, "creation"))

        got = await repo.get(saga_id)
        assert {a.domain for a in got.domain_acks} == {"creation"}

    @pytest.mark.asyncio
    async def test_duplicate_ack_is_idempotent(self) -> None:
        repo = InMemoryCoordinatorRepository()
        saga_id = await _saved_saga(repo)
        handler = DomainAckHandler(repo=repo)

        await handler.handle({}, _payload(saga_id, "identity"))
        await handler.handle({}, _payload(saga_id, "identity"))

        got = await repo.get(saga_id)
        assert len(got.domain_acks) == 1

    @pytest.mark.asyncio
    async def test_unknown_saga_raises_transient(self) -> None:
        handler = DomainAckHandler(repo=InMemoryCoordinatorRepository())
        with pytest.raises(TransientError):
            await handler.handle({}, _payload("01970000-0000-7000-a000-00000000dead"))

    @pytest.mark.asyncio
    async def test_malformed_payload_raises_value_error(self) -> None:
        repo = InMemoryCoordinatorRepository()
        await _saved_saga(repo)
        handler = DomainAckHandler(repo=repo)
        with pytest.raises(ValueError):
            await handler.handle({}, b"{not-json")

    @pytest.mark.asyncio
    async def test_missing_fields_raise_value_error(self) -> None:
        repo = InMemoryCoordinatorRepository()
        saga_id = await _saved_saga(repo)
        handler = DomainAckHandler(repo=repo)
        bad = json.dumps({"saga_id": saga_id}).encode()
        with pytest.raises(ValueError):
            await handler.handle({}, bad)
