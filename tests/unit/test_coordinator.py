"""Coordinator (saga aggregate root) unit tests.

Python port of Go coordinator_test.go. Preserves all invariants:

- AGID rejection (agents have no lifecycle — invariant #10 from ddd-enforcement)
- Grace period bounds (1 <= GracePeriodDays <= 365)
- Tenant ID required
- 5-state lifecycle (forward + cancel-during-grace)
- Federated ack gate before COLD_ARCHIVED
- Append-only history
"""

from __future__ import annotations

import datetime as _dt

import pytest

from chora_closure_orchestrator.domain.closure.coordinator import (
    Coordinator,
    CoordinatorError,
    NewParams,
    new,
)
from chora_closure_orchestrator.domain.closure.errors import (
    ErrCancelTooLate,
    ErrInvalidTransition,
    ErrPendingDomainAcks,
    ErrUnknownDomain,
)
from chora_closure_orchestrator.domain.closure.state import REQUIRED_DOMAINS, State

# UUIDv7 GCIDs for testing (uppercase first nibble: 0197A = AGID, others = GCID).
GCID_LEARNER = "01970000-0000-7000-9000-000000000001"
GCID_ADMIN = "01970000-0000-7000-9000-0000000000a1"
TENANT_ID = "01970000-0000-7000-8000-000000000001"
AGID_X = "0197a000-0000-7000-9000-000000000001"


# -----------------------------------------------------------------------------
# new() constructor
# -----------------------------------------------------------------------------


class TestNew:
    def test_starts_in_closing_state(self) -> None:
        c = new(
            NewParams(
                gcid=GCID_LEARNER,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                reason="learner_self_request",
                requested_by_gcid=GCID_LEARNER,
            )
        )
        assert c.saga_id != ""
        assert c.state == State.CLOSING
        assert len(c.history) == 1
        assert c.grace_period_days == 30
        # First history entry: ACTIVE -> CLOSING
        assert c.history[0].prior_state == State.ACTIVE
        assert c.history[0].new_state == State.CLOSING

    def test_rejects_agid(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid=AGID_X,
                    tenant_id=TENANT_ID,
                    grace_period_days=30,
                    requested_by_gcid=AGID_X,
                )
            )

    def test_rejects_zero_grace(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid=GCID_LEARNER,
                    tenant_id=TENANT_ID,
                    grace_period_days=0,
                    requested_by_gcid=GCID_LEARNER,
                )
            )

    def test_rejects_excessive_grace(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid=GCID_LEARNER,
                    tenant_id=TENANT_ID,
                    grace_period_days=366,
                    requested_by_gcid=GCID_LEARNER,
                )
            )

    def test_rejects_empty_tenant(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid=GCID_LEARNER,
                    tenant_id="",
                    grace_period_days=30,
                    requested_by_gcid=GCID_LEARNER,
                )
            )

    def test_rejects_empty_gcid(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid="",
                    tenant_id=TENANT_ID,
                    grace_period_days=30,
                    requested_by_gcid=GCID_LEARNER,
                )
            )

    def test_rejects_empty_requested_by_gcid(self) -> None:
        with pytest.raises(CoordinatorError):
            new(
                NewParams(
                    gcid=GCID_LEARNER,
                    tenant_id=TENANT_ID,
                    grace_period_days=30,
                    requested_by_gcid="",
                )
            )

    def test_grace_ends_at_in_future(self) -> None:
        c = new(
            NewParams(
                gcid=GCID_LEARNER,
                tenant_id=TENANT_ID,
                grace_period_days=30,
                requested_by_gcid=GCID_LEARNER,
            )
        )
        assert c.grace_ends_at > c.requested_at
        assert (c.grace_ends_at - c.requested_at).days == 30


# -----------------------------------------------------------------------------
# advance() — every transition + invalid transitions raise
# -----------------------------------------------------------------------------


def _new_saga() -> Coordinator:
    return new(
        NewParams(
            gcid=GCID_LEARNER,
            tenant_id=TENANT_ID,
            grace_period_days=30,
            reason="learner_self_request",
            requested_by_gcid=GCID_LEARNER,
        )
    )


class TestAdvance:
    def test_closing_to_suspended(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "grace_expired", GCID_ADMIN)
        assert c.state == State.SUSPENDED
        assert len(c.history) == 2

    def test_full_forward_chain(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "grace_expired", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "fanout_done", GCID_ADMIN)
        # All required acks must be recorded before COLD_ARCHIVED
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        c.advance(State.COLD_ARCHIVED, "all_acked", GCID_ADMIN)
        c.advance(State.CRYPTO_SHREDDED, "shred", GCID_ADMIN)
        assert c.is_terminal()

    def test_rejects_skipping(self) -> None:
        c = _new_saga()
        # CLOSING -> PSEUDONYMIZED is forbidden (must hit SUSPENDED first)
        with pytest.raises(ErrInvalidTransition):
            c.advance(State.PSEUDONYMIZED, "skip", GCID_ADMIN)

    def test_rejects_backwards_after_suspended(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "expired", GCID_ADMIN)
        with pytest.raises(ErrInvalidTransition):
            c.advance(State.ACTIVE, "rollback", GCID_ADMIN)

    def test_rejects_unknown_state(self) -> None:
        c = _new_saga()
        with pytest.raises(ErrInvalidTransition):
            c.advance("bogus", "x", GCID_ADMIN)  # type: ignore[arg-type]

    def test_to_cold_archived_requires_all_acks(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)

        with pytest.raises(ErrPendingDomainAcks):
            c.advance(State.COLD_ARCHIVED, "premature", GCID_ADMIN)

        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        c.advance(State.COLD_ARCHIVED, "all_acked", GCID_ADMIN)
        assert c.state == State.COLD_ARCHIVED


# -----------------------------------------------------------------------------
# cancel() — only allowed before SUSPENDED
# -----------------------------------------------------------------------------


class TestCancel:
    def test_during_closing_returns_to_active(self) -> None:
        c = _new_saga()
        c.cancel("changed_mind", GCID_LEARNER)
        assert c.state == State.ACTIVE
        assert c.cancelled_at is not None

    def test_after_suspended_rejected(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "expired", GCID_ADMIN)
        with pytest.raises(ErrCancelTooLate):
            c.cancel("too_late", GCID_LEARNER)

    def test_after_pseudonymized_rejected(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        with pytest.raises(ErrCancelTooLate):
            c.cancel("too_late", GCID_LEARNER)

    def test_appends_history_entry(self) -> None:
        c = _new_saga()
        n_before = len(c.history)
        c.cancel("changed_mind", GCID_LEARNER)
        assert len(c.history) == n_before + 1
        assert c.history[-1].new_state == State.ACTIVE


# -----------------------------------------------------------------------------
# Federated coordination — domains MUST ack before COLD_ARCHIVED
# -----------------------------------------------------------------------------


class TestRecordDomainAck:
    def test_first_ack_returns_true(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        result = c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        assert result is True

    def test_re_ack_is_idempotent(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        result = c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        assert result is False  # Re-ack is no-op

    def test_rejects_unknown_domain(self) -> None:
        c = _new_saga()
        with pytest.raises(ErrUnknownDomain):
            c.record_domain_ack("bogus_domain", _dt.datetime.now(_dt.UTC))

    def test_normalises_case(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        # Uppercase input should still be accepted as long as it normalises
        # to a known domain.
        result = c.record_domain_ack("CREATION", _dt.datetime.now(_dt.UTC))
        assert result is True


class TestAllDomainsAcked:
    def test_false_at_start(self) -> None:
        c = _new_saga()
        assert c.all_domains_acked() is False

    def test_true_after_full_set(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        for d in REQUIRED_DOMAINS:
            c.record_domain_ack(d, _dt.datetime.now(_dt.UTC))
        assert c.all_domains_acked() is True

    def test_false_with_partial_acks(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "t", GCID_ADMIN)
        c.advance(State.PSEUDONYMIZED, "t", GCID_ADMIN)
        c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        c.record_domain_ack("identity", _dt.datetime.now(_dt.UTC))
        assert c.all_domains_acked() is False


# -----------------------------------------------------------------------------
# Grace expiry helper
# -----------------------------------------------------------------------------


class TestGraceExpiredAt:
    def test_not_expired_at_request_time(self) -> None:
        c = _new_saga()
        assert c.grace_expired_at(c.requested_at) is False

    def test_expired_after_grace_ends(self) -> None:
        c = _new_saga()
        check = c.grace_ends_at + _dt.timedelta(seconds=1)
        assert c.grace_expired_at(check) is True


# -----------------------------------------------------------------------------
# D3 — suspended_at() (ack-timeout reference) + PSEUDONYMISE_PARTIAL transitions
# -----------------------------------------------------------------------------


class TestSuspendedAtAndPartial:
    def test_none_before_suspended(self) -> None:
        c = _new_saga()  # CLOSING
        assert c.suspended_at() is None

    def test_returns_transition_time_after_suspend(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "grace_expired", GCID_ADMIN)
        ts = c.suspended_at()
        assert ts is not None
        suspend_entries = [h for h in c.history if h.new_state == State.SUSPENDED]
        assert ts == suspend_entries[-1].transitioned_at

    def test_partial_escalation_then_recovery(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "grace_expired", GCID_ADMIN)
        c.advance(State.PSEUDONYMISE_PARTIAL, "ack_timeout", GCID_ADMIN)
        assert c.state is State.PSEUDONYMISE_PARTIAL
        # Late acks complete → recover forward to PSEUDONYMIZED.
        c.advance(State.PSEUDONYMIZED, "late_acks_complete", GCID_ADMIN)
        assert c.state is State.PSEUDONYMIZED
        assert c.suspended_at() is not None

    def test_cannot_cold_archive_directly_from_partial(self) -> None:
        c = _new_saga()
        c.advance(State.SUSPENDED, "grace_expired", GCID_ADMIN)
        c.advance(State.PSEUDONYMISE_PARTIAL, "ack_timeout", GCID_ADMIN)
        with pytest.raises(ErrInvalidTransition):
            c.advance(State.COLD_ARCHIVED, "x", GCID_ADMIN)
