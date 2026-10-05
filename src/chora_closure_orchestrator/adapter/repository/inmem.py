"""In-memory CoordinatorRepository — MVP / unit-test double."""

from __future__ import annotations

import asyncio

from chora_closure_orchestrator.domain.closure import (
    Coordinator,
    ErrSagaNotFound,
    State,
)


class InMemoryCoordinatorRepository:
    """Thread-safe in-memory store for Coordinator aggregates.

    Production replacement: Postgres adapter against chora_ai_kernel DB.
    """

    def __init__(self) -> None:
        self._store: dict[str, Coordinator] = {}
        self._lock = asyncio.Lock()

    async def save(self, c: Coordinator) -> None:
        """Upsert by saga_id."""
        async with self._lock:
            self._store[c.saga_id] = c

    async def get(self, saga_id: str) -> Coordinator:
        async with self._lock:
            c = self._store.get(saga_id)
            if c is None:
                raise ErrSagaNotFound(f"saga {saga_id!r} not found")
            return c

    async def get_by_gcid(self, gcid: str) -> Coordinator | None:
        async with self._lock:
            # Return the most recent matching saga (sort by updated_at).
            matches = [c for c in self._store.values() if c.gcid == gcid]
            if not matches:
                return None
            matches.sort(key=lambda c: c.updated_at, reverse=True)
            return matches[0]

    async def list_by_state(self, state: State, limit: int = 50) -> list[Coordinator]:
        async with self._lock:
            out: list[Coordinator] = []
            for c in self._store.values():
                if c.state == state:
                    out.append(c)
                    if limit > 0 and len(out) >= limit:
                        break
            return out
