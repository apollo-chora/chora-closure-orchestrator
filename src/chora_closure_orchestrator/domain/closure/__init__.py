"""Closure saga domain — Coordinator aggregate + state machine + errors.

Mirrors Go ``services/chora-closure-orchestrator/internal/domain/saga/``
ported to Python under hexagonal layout (no infrastructure deps).
"""

from chora_closure_orchestrator.domain.closure.coordinator import (
    Coordinator,
    CoordinatorError,
    DomainAck,
    HistoryEntry,
    NewParams,
    new,
)
from chora_closure_orchestrator.domain.closure.errors import (
    ErrCancelTooLate,
    ErrInvalidTransition,
    ErrPendingDomainAcks,
    ErrSagaNotFound,
    ErrUnknownDomain,
)
from chora_closure_orchestrator.domain.closure.state import (
    ALL_STATES,
    REQUIRED_DOMAINS,
    State,
    can_transition,
    is_cancellable,
    is_known_domain,
    is_terminal_state,
)

__all__ = [
    "ALL_STATES",
    "Coordinator",
    "CoordinatorError",
    "DomainAck",
    "ErrCancelTooLate",
    "ErrInvalidTransition",
    "ErrPendingDomainAcks",
    "ErrSagaNotFound",
    "ErrUnknownDomain",
    "HistoryEntry",
    "NewParams",
    "REQUIRED_DOMAINS",
    "State",
    "can_transition",
    "is_cancellable",
    "is_known_domain",
    "is_terminal_state",
    "new",
]
