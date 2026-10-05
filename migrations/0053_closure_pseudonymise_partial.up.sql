-- =============================================================================
-- chora-closure-orchestrator : 0053_closure_pseudonymise_partial.up.sql
--
-- D3 (Auth Phase A debt pass): add the PSEUDONYMISE_PARTIAL escalation state.
--
-- The saga driver used to leap CLOSING -> SUSPENDED -> PSEUDONYMIZED at fan-out
-- time, asserting every domain had pseudonymised with ZERO acks recorded. The
-- fix holds the saga at SUSPENDED until all 10 per-domain acks land
-- (-> PSEUDONYMIZED) or the ack timeout (CLOSURE_ACK_TIMEOUT_SECONDS) elapses
-- (-> PSEUDONYMISE_PARTIAL, surfaced in the O+ admin queue; recovers forward to
-- PSEUDONYMIZED if the late acks arrive). This widens the state CHECK to admit
-- the new value and includes it in the active-saga partial index.
-- =============================================================================
-- Lock discipline: the closure orchestrator runs always-on (min_instances=1,
-- D2) and continuously polls closure_saga, so a naive ADD CONSTRAINT can block
-- on its ACCESS EXCLUSIVE lock indefinitely (the first apply hung to the 900s
-- job timeout). Fail fast instead of hanging, and minimise the exclusive
-- window: ADD ... NOT VALID takes only a brief ACCESS EXCLUSIVE (no table
-- scan), then VALIDATE re-checks rows under the weaker SHARE UPDATE EXCLUSIVE
-- (does not block concurrent reads/writes). closure_saga is tiny, so VALIDATE
-- is near-instant.
SET lock_timeout = '45s';

ALTER TABLE closure_saga DROP CONSTRAINT IF EXISTS closure_saga_state_check;

ALTER TABLE closure_saga ADD CONSTRAINT closure_saga_state_check CHECK (state IN (
    'active', 'closing', 'suspended',
    'pseudonymized', 'pseudonymise_partial', 'cold_archived', 'crypto_shredded'
)) NOT VALID;

ALTER TABLE closure_saga VALIDATE CONSTRAINT closure_saga_state_check;

-- Active-saga index for the admin O+ closure queue — now surfaces PARTIAL sagas
-- so a stuck/timed-out federation is operator-visible (terminal crypto_shredded
-- stays excluded, as before).
DROP INDEX IF EXISTS idx_closure_saga_state;

CREATE INDEX IF NOT EXISTS idx_closure_saga_state
    ON closure_saga (state)
    WHERE state IN (
        'closing', 'suspended', 'pseudonymise_partial',
        'pseudonymized', 'cold_archived'
    );
