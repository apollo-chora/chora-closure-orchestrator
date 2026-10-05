"""Closure event publisher port + payload types + in-memory adapter.

Topics emitted (canonical chora.closure.* per S0 contracts reconciliation):

- ``chora.closure.requested.v1`` — saga initiated
- ``chora.closure.grace_started.v1`` — grace window kicked off
- ``chora.closure.pseudonymise_per_domain_complete.v1`` — all domains acked
- ``chora.closure.crypto_shred_complete.v1`` — DEK deleted (data unrecoverable)
- ``chora.closure.closed.v1`` — terminal state reached
- ``chora.closure.cancelled.v1`` — cancelled-during-grace (CLOSING -> ACTIVE)

Plus per-domain pseudonymise request fanout topic:

- ``chora.{domain}.pii.pseudonymise.requested.v1`` (consumers: 10 domains)
"""

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
from chora_closure_orchestrator.adapter.events.port import (
    ClosureEventPublisher,
)
from chora_closure_orchestrator.adapter.events.publisher_inmem import (
    Envelope,
    InMemoryClosurePublisher,
    PublishedEvent,
)

__all__ = [
    "AgentTerminated",
    "ClosureCancelled",
    "ClosureClosed",
    "ClosureCryptoShredComplete",
    "ClosureEventPublisher",
    "ClosureGraceStarted",
    "ClosurePseudonymisePerDomainComplete",
    "ClosureRequested",
    "Envelope",
    "InMemoryClosurePublisher",
    "PseudonymiseRequested",
    "PublishedEvent",
]
