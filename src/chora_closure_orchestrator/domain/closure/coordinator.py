"""Coordinator — the federated closure saga aggregate root.

Per Tier 3 D11 (federated saga, NOT hard-delete): the orchestrator owns
the saga lifecycle + dispatches per-domain pseudonymisation requests via
the event bus; each domain owns its ``PII_Closure_Map.yaml`` and acks back
when its pseudonymisation duty is discharged. COLD_ARCHIVED is only
reachable once every required domain has acked.

History is append-only (an event log of every transition for audit).
AGIDs cannot own a closure saga (per ddd-enforcement aggregate-invariant
#10 — agents have no lifecycle).

Hexagonal layer: domain. No infrastructure imports.
"""

from __future__ import annotations

import datetime as _dt
import uuid
from dataclasses import dataclass, field

from chora_closure_orchestrator.domain.closure.errors import (
    CoordinatorError,
    ErrCancelTooLate,
    ErrInvalidTransition,
    ErrPendingDomainAcks,
    ErrUnknownDomain,
)
from chora_closure_orchestrator.domain.closure.state import (
    REQUIRED_DOMAINS,
    State,
    can_transition,
    is_cancellable,
    is_terminal_state,
)

# -----------------------------------------------------------------------------
# AGID detection — duplicated from chora-identity to keep this layer
# dependency-free across services. AGID UUIDv7s start with the literal
# prefix "0197a" (lowercased) per chora-identity convention.
# -----------------------------------------------------------------------------


def _is_agid(identifier: str) -> bool:
    """Lightweight AGID predicate. Mirrors Go isAGID()."""
    if not identifier:
        return False
    return identifier.lower().startswith("0197a")


# -----------------------------------------------------------------------------
# Value types
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class HistoryEntry:
    """Single saga state transition. Append-only; never mutated."""

    prior_state: State
    new_state: State
    reason: str
    actor_gcid: str
    transitioned_at: _dt.datetime


@dataclass(frozen=True)
class DomainAck:
    """Single per-domain pseudonymisation acknowledgement."""

    domain: str
    acked_at: _dt.datetime


@dataclass
class NewParams:
    """Coordinator constructor input."""

    gcid: str
    tenant_id: str
    grace_period_days: int
    requested_by_gcid: str
    reason: str = ""
    # Operator fast-close (ADR-181 ruling 12 / CHO-1719 gap 6): collapses
    # the grace window to zero for test accounts. The HTTP layer gates this
    # behind PLATFORM_OPERATOR; the domain only honours the collapsed
    # grace. grace_period_days keeps its 1..365 invariant (DB CHECK) — only
    # grace_ends_at collapses, so there is no silent global grace change.
    fast_close: bool = False


# -----------------------------------------------------------------------------
# Coordinator aggregate root
# -----------------------------------------------------------------------------


@dataclass
class Coordinator:
    """Federated closure saga aggregate root.

    Tracks lifecycle state + federated per-domain pseudonymisation acks.
    """

    saga_id: str
    gcid: str
    tenant_id: str
    state: State
    reason: str
    requested_by_gcid: str
    grace_period_days: int
    requested_at: _dt.datetime
    grace_ends_at: _dt.datetime
    updated_at: _dt.datetime
    history: list[HistoryEntry] = field(default_factory=list)
    domain_acks: list[DomainAck] = field(default_factory=list)
    cancelled_at: _dt.datetime | None = None

    def advance(self, to: State, reason: str, actor_gcid: str) -> None:
        """Transition the saga to ``to``, validating through the state machine.

        :raises ErrInvalidTransition: forbidden transition.
        :raises ErrPendingDomainAcks: PSEUDONYMIZED -> COLD_ARCHIVED requested
            before all required domain acks recorded.
        """
        if not isinstance(to, State):
            raise ErrInvalidTransition(f"unknown state {to!r}")
        if not can_transition(self.state, to):
            raise ErrInvalidTransition(f"{self.state} -> {to}")
        # Federation gate: COLD_ARCHIVED requires every required domain acked.
        if self.state == State.PSEUDONYMIZED and to == State.COLD_ARCHIVED and not self.all_domains_acked():
            raise ErrPendingDomainAcks("federated pseudonymisation acks pending")
        now = _dt.datetime.now(_dt.UTC)
        self.history.append(
            HistoryEntry(
                prior_state=self.state,
                new_state=to,
                reason=reason.strip(),
                actor_gcid=actor_gcid.strip(),
                transitioned_at=now,
            )
        )
        self.state = to
        self.updated_at = now

    def cancel(self, reason: str, actor_gcid: str) -> None:
        """Revert a saga in CLOSING back to ACTIVE.

        :raises ErrCancelTooLate: saga has progressed past CLOSING.
        """
        if not is_cancellable(self.state):
            raise ErrCancelTooLate(f"state={self.state}")
        now = _dt.datetime.now(_dt.UTC)
        self.history.append(
            HistoryEntry(
                prior_state=self.state,
                new_state=State.ACTIVE,
                reason=reason.strip(),
                actor_gcid=actor_gcid.strip(),
                transitioned_at=now,
            )
        )
        self.state = State.ACTIVE
        self.cancelled_at = now
        self.updated_at = now

    def is_terminal(self) -> bool:
        """Report whether the saga has reached a terminal state."""
        return is_terminal_state(self.state)

    def grace_expired_at(self, now: _dt.datetime) -> bool:
        """Pure-function grace-expiry check at the given instant."""
        return now > self.grace_ends_at

    def record_domain_ack(self, domain: str, at: _dt.datetime) -> bool:
        """Record that ``domain`` has acked its pseudonymisation duty.

        Idempotent: a re-ack returns ``False``. First ack returns ``True``.

        :raises ErrUnknownDomain: domain not in REQUIRED_DOMAINS.
        """
        d = domain.strip().lower()
        if d not in REQUIRED_DOMAINS:
            raise ErrUnknownDomain(f"unknown federated domain: {domain!r}")
        for ack in self.domain_acks:
            if ack.domain == d:
                return False
        now_utc = at.astimezone(_dt.UTC) if at.tzinfo else at.replace(tzinfo=_dt.UTC)
        self.domain_acks.append(DomainAck(domain=d, acked_at=now_utc))
        self.updated_at = _dt.datetime.now(_dt.UTC)
        return True

    def all_domains_acked(self) -> bool:
        """Report whether every required domain has acked."""
        if len(self.domain_acks) < len(REQUIRED_DOMAINS):
            return False
        have = {a.domain for a in self.domain_acks}
        return all(d in have for d in REQUIRED_DOMAINS)

    def suspended_at(self) -> _dt.datetime | None:
        """Return when the saga entered SUSPENDED (fan-out start), or None.

        Read from the append-only history (the most recent transition INTO
        SUSPENDED) so no extra persisted column is needed. Used by the saga
        driver to measure the per-domain ack timeout (D3).
        """
        for entry in reversed(self.history):
            if entry.new_state == State.SUSPENDED:
                return entry.transitioned_at
        return None


# -----------------------------------------------------------------------------
# Constructor
# -----------------------------------------------------------------------------


def new(p: NewParams) -> Coordinator:
    """Construct a Coordinator in CLOSING state with the requested grace.

    :raises CoordinatorError: any invariant violation (empty fields, AGID,
        out-of-range grace).
    """
    gcid = p.gcid.strip()
    if not gcid:
        raise CoordinatorError("gcid is required")
    if _is_agid(gcid):
        raise CoordinatorError("AGID cannot own a closure saga (agents have no lifecycle)")
    tenant = p.tenant_id.strip()
    if not tenant:
        raise CoordinatorError("tenant_id is required")
    if p.grace_period_days < 1:
        raise CoordinatorError(f"grace_period_days must be >= 1; got {p.grace_period_days}")
    if p.grace_period_days > 365:
        raise CoordinatorError(f"grace_period_days must be <= 365; got {p.grace_period_days}")
    requested_by = p.requested_by_gcid.strip()
    if not requested_by:
        raise CoordinatorError("requested_by_gcid is required")

    saga_id = _uuidv7_str()
    now = _dt.datetime.now(_dt.UTC)
    grace_ends = now if p.fast_close else now + _dt.timedelta(days=p.grace_period_days)

    return Coordinator(
        saga_id=saga_id,
        gcid=gcid,
        tenant_id=tenant,
        state=State.CLOSING,
        reason=p.reason.strip(),
        requested_by_gcid=requested_by,
        grace_period_days=p.grace_period_days,
        requested_at=now,
        grace_ends_at=grace_ends,
        updated_at=now,
        history=[
            HistoryEntry(
                prior_state=State.ACTIVE,
                new_state=State.CLOSING,
                reason=p.reason.strip(),
                actor_gcid=requested_by,
                transitioned_at=now,
            )
        ],
        domain_acks=[],
    )


def _uuidv7_str() -> str:
    """Generate a UUIDv7 string (time-ordered, RFC 9562)."""
    try:
        import uuid7 as _u7

        return str(_u7.uuid7())
    except ImportError:
        # Fallback to UUIDv4 if uuid7 unavailable.
        return str(uuid.uuid4())
