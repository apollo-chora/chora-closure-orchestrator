"""CoordinatorRepository port (Protocol)."""

from __future__ import annotations

from typing import Protocol

from chora_closure_orchestrator.domain.closure import Coordinator, State


class CoordinatorRepository(Protocol):
    """Persistence port for the Coordinator aggregate."""

    async def save(self, c: Coordinator) -> None:
        """Upsert the saga by saga_id."""
        ...

    async def get(self, saga_id: str) -> Coordinator:
        """Return the saga by id.

        :raises ErrSagaNotFound: saga missing.
        """
        ...

    async def get_by_gcid(self, gcid: str) -> Coordinator | None:
        """Return the most recent saga for the GCID, or None."""
        ...

    async def list_by_state(self, state: State, limit: int = 50) -> list[Coordinator]:
        """Return sagas in the given state, capped to limit (0 = unlimited)."""
        ...
