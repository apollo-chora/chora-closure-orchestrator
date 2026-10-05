"""ColdArchiveClient port (Protocol)."""

from __future__ import annotations

from typing import Protocol

from chora_closure_orchestrator.adapter.coldarchive.inmem import (
    ColdArchiveJobSpec,
    ColdArchiveResult,
)


class ColdArchiveClient(Protocol):
    """Cold-archive object-store writer port."""

    async def archive(self, spec: ColdArchiveJobSpec) -> ColdArchiveResult: ...
