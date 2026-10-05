"""Coordinator persistence port + in-memory adapter.

Production swaps to a Postgres adapter targeting ``chora_ai_kernel`` DB
per Tier 3 D11 + ``project_chora_data_plane`` memory ("AI Kernel owns
the Closure Orchestrator").
"""

from chora_closure_orchestrator.adapter.repository.inmem import (
    InMemoryCoordinatorRepository,
)
from chora_closure_orchestrator.adapter.repository.port import (
    CoordinatorRepository,
)

__all__ = ["CoordinatorRepository", "InMemoryCoordinatorRepository"]
