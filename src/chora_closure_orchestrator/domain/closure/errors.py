"""Domain-layer errors — pure Python, no infrastructure deps."""

from __future__ import annotations


class CoordinatorError(Exception):
    """Base class for all closure coordinator domain errors."""


class ErrInvalidTransition(CoordinatorError):
    """Returned by Coordinator.advance for a forbidden transition."""


class ErrCancelTooLate(CoordinatorError):
    """Returned by Coordinator.cancel once the saga has progressed past CLOSING.

    SUSPENDED + later states cannot be reverted.
    """


class ErrPendingDomainAcks(CoordinatorError):
    """Returned by Coordinator.advance when COLD_ARCHIVED is requested before
    every required domain has acked its pseudonymisation duty.
    """


class ErrSagaNotFound(CoordinatorError):
    """Returned by Repository.get when a saga is missing."""


class ErrUnknownDomain(CoordinatorError):
    """Returned by Coordinator.record_domain_ack for a domain not in the
    required-domains list.
    """
