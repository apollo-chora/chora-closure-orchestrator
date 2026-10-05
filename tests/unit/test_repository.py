"""In-memory Coordinator repository tests."""

from __future__ import annotations

import pytest

from chora_closure_orchestrator.adapter.repository import InMemoryCoordinatorRepository
from chora_closure_orchestrator.domain.closure.coordinator import NewParams, new
from chora_closure_orchestrator.domain.closure.errors import ErrSagaNotFound
from chora_closure_orchestrator.domain.closure.state import State

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _saga():
    return new(
        NewParams(
            gcid=GCID,
            tenant_id=TENANT,
            grace_period_days=30,
            requested_by_gcid=GCID,
        )
    )


class TestInMemoryRepository:
    @pytest.mark.asyncio
    async def test_save_and_get(self) -> None:
        repo = InMemoryCoordinatorRepository()
        c = _saga()
        await repo.save(c)
        loaded = await repo.get(c.saga_id)
        assert loaded.saga_id == c.saga_id

    @pytest.mark.asyncio
    async def test_get_unknown_raises(self) -> None:
        repo = InMemoryCoordinatorRepository()
        with pytest.raises(ErrSagaNotFound):
            await repo.get("nonexistent")

    @pytest.mark.asyncio
    async def test_get_by_gcid(self) -> None:
        repo = InMemoryCoordinatorRepository()
        c = _saga()
        await repo.save(c)
        loaded = await repo.get_by_gcid(GCID)
        assert loaded is not None
        assert loaded.gcid == GCID

    @pytest.mark.asyncio
    async def test_get_by_gcid_missing_returns_none(self) -> None:
        repo = InMemoryCoordinatorRepository()
        result = await repo.get_by_gcid("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_list_by_state(self) -> None:
        repo = InMemoryCoordinatorRepository()
        c1 = _saga()
        c2 = _saga()
        await repo.save(c1)
        await repo.save(c2)
        items = await repo.list_by_state(State.CLOSING, limit=10)
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_list_by_state_with_limit(self) -> None:
        repo = InMemoryCoordinatorRepository()
        for _ in range(5):
            await repo.save(_saga())
        items = await repo.list_by_state(State.CLOSING, limit=2)
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_save_upserts(self) -> None:
        repo = InMemoryCoordinatorRepository()
        c = _saga()
        await repo.save(c)
        # Same saga_id, mutated state
        c.cancel("test", GCID)
        await repo.save(c)
        loaded = await repo.get(c.saga_id)
        assert loaded.state == State.ACTIVE
