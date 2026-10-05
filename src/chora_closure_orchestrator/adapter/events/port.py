"""ClosureEventPublisher port (Protocol)."""

from __future__ import annotations

from typing import Protocol

from chora_closure_orchestrator.adapter.events.payloads import (
    AgentTerminated,
    ClosureCancelled,
    ClosureClosed,
    ClosureCryptoShredComplete,
    ClosureGraceStarted,
    ClosurePseudonymisePerDomainComplete,
    ClosureRequested,
    PseudonymiseRequested,
)


class ClosureEventPublisher(Protocol):
    """Federated-saga event publisher port."""

    async def publish_closure_requested(self, e: ClosureRequested) -> None: ...

    async def publish_grace_started(self, e: ClosureGraceStarted) -> None: ...

    async def publish_pseudonymise_per_domain_complete(self, e: ClosurePseudonymisePerDomainComplete) -> None: ...

    async def publish_crypto_shred_complete(self, e: ClosureCryptoShredComplete) -> None: ...

    async def publish_closed(self, e: ClosureClosed) -> None: ...

    async def publish_cancelled(self, e: ClosureCancelled) -> None: ...

    async def publish_pseudonymise_requested(self, e: PseudonymiseRequested) -> None: ...

    async def publish_agent_terminated(self, e: AgentTerminated) -> None: ...
