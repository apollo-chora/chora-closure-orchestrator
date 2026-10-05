"""Closure saga state machine — pure functions on the 5-state lifecycle.

Per .claude/rules/ddd-enforcement.md (Account Closure section) + Tier 3 D11:
5-state machine ``ACTIVE -> CLOSING (grace) -> SUSPENDED -> PSEUDONYMIZED ->
COLD_ARCHIVED -> CRYPTO_SHREDDED``. Pseudonymise + crypto-shred — NEVER
hard-delete.

CLOSING -> ACTIVE is the ONLY backward transition (cancellation during
grace). Once SUSPENDED is reached the saga is no longer cancellable.

Hexagonal: this module is dependency-free w.r.t. infrastructure.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final


class State(StrEnum):
    """Saga lifecycle state. Values match the Protobuf
    ``chora.closure.v1.ClosureSagaState`` wire enum (less the prefix).
    """

    ACTIVE = "active"
    CLOSING = "closing"
    SUSPENDED = "suspended"
    PSEUDONYMIZED = "pseudonymized"
    # D3: escalation state when the per-domain pseudonymisation acks do not all
    # arrive within CLOSURE_ACK_TIMEOUT_SECONDS. The saga does NOT advance to
    # PSEUDONYMIZED (which would falsely assert every domain pseudonymised) and
    # does NOT silently stall in SUSPENDED — it surfaces in the O+ admin queue
    # for operator attention. Recovers to PSEUDONYMIZED if the late acks land.
    PSEUDONYMISE_PARTIAL = "pseudonymise_partial"
    COLD_ARCHIVED = "cold_archived"
    CRYPTO_SHREDDED = "crypto_shredded"


# Canonical ordered list (used for table-driven tests, admin queue dropdowns,
# observability dashboards).
ALL_STATES: Final[tuple[State, ...]] = (
    State.ACTIVE,
    State.CLOSING,
    State.SUSPENDED,
    State.PSEUDONYMIZED,
    State.PSEUDONYMISE_PARTIAL,
    State.COLD_ARCHIVED,
    State.CRYPTO_SHREDDED,
)


# -----------------------------------------------------------------------------
# Transition table
# -----------------------------------------------------------------------------

# Forward progressions are one-step-at-a-time. CLOSING -> ACTIVE is the ONE
# permitted backward transition (cancel-during-grace).
_ALLOWED: Final[dict[State, frozenset[State]]] = {
    State.ACTIVE: frozenset({State.CLOSING}),
    State.CLOSING: frozenset({State.SUSPENDED, State.ACTIVE}),
    # D3: SUSPENDED resolves to PSEUDONYMIZED (all domains acked) or
    # PSEUDONYMISE_PARTIAL (ack timeout) — never advances on fan-out alone.
    State.SUSPENDED: frozenset({State.PSEUDONYMIZED, State.PSEUDONYMISE_PARTIAL}),
    # A partial saga recovers forward to PSEUDONYMIZED once the late acks land.
    State.PSEUDONYMISE_PARTIAL: frozenset({State.PSEUDONYMIZED}),
    State.PSEUDONYMIZED: frozenset({State.COLD_ARCHIVED}),
    State.COLD_ARCHIVED: frozenset({State.CRYPTO_SHREDDED}),
    State.CRYPTO_SHREDDED: frozenset(),  # terminal
}


def can_transition(from_state: State, to_state: State) -> bool:
    """Report whether ``from_state -> to_state`` is a permitted transition.

    Same-state transitions (from == to) always return False.
    """
    if from_state == to_state:
        return False
    successors = _ALLOWED.get(from_state)
    if successors is None:
        return False
    return to_state in successors


def is_cancellable(s: State) -> bool:
    """Report whether a saga in state ``s`` may still be cancelled.

    Only ``StateClosing`` qualifies (cancel-during-grace).
    """
    return s == State.CLOSING


def is_terminal_state(s: State) -> bool:
    """Report whether ``s`` is a terminal state (no successor)."""
    successors = _ALLOWED.get(s)
    if successors is None:
        return True
    return len(successors) == 0


# -----------------------------------------------------------------------------
# Federated domain registry
# -----------------------------------------------------------------------------

# Canonical list of domains that MUST acknowledge pseudonymisation.
# Excludes ai_kernel — the Closure Orchestrator IS part of AI Kernel.
#
# Source: .claude/rules/ddd-enforcement.md "11-database topology"; closure
# is federated across 5 core domains + 5 supporting (excluding ai_kernel).
REQUIRED_DOMAINS: Final[tuple[str, ...]] = (
    # 5 core Content {verb} domains
    "creation",
    "consumption",
    "delivery",
    "sharing",
    "a2a",
    # 5 supporting (ai_kernel excluded — orchestrator is in ai_kernel)
    "identity",
    "tenancy",
    "governance",
    "observability",
    "notifications",
)


def is_known_domain(d: str) -> bool:
    """Report whether ``d`` is in the required-domain registry.

    Performs case normalisation (returns True for "Creation"/"CREATION").
    """
    if not d:
        return False
    return d.strip().lower() in REQUIRED_DOMAINS
