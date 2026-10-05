-- Reverse of 0053 — restores the pre-D3 state CHECK + active-saga index.
-- NOTE: this fails if any closure_saga row is currently in the
-- 'pseudonymise_partial' state (the narrowed CHECK would be violated); resolve
-- or advance those sagas before reverting.

DROP INDEX IF EXISTS idx_closure_saga_state;

CREATE INDEX IF NOT EXISTS idx_closure_saga_state
    ON closure_saga (state)
    WHERE state IN ('closing', 'suspended', 'pseudonymized', 'cold_archived');

ALTER TABLE closure_saga DROP CONSTRAINT IF EXISTS closure_saga_state_check;

ALTER TABLE closure_saga ADD CONSTRAINT closure_saga_state_check CHECK (state IN (
    'active', 'closing', 'suspended',
    'pseudonymized', 'cold_archived', 'crypto_shredded'
));
