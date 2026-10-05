-- =============================================================================
-- chora-closure-orchestrator — initial schema
-- =============================================================================
--
-- Source-of-truth: .claude/skills/account-closure-saga/SKILL.md
--                  .claude/rules/ddd-enforcement.md "Account Closure"
--                  Tier 3 D11 (federated saga; pseudonymise + crypto-shred)
--
-- Saga state persists in chora_ai_kernel database (per project_chora_data_plane
-- memory: "AI Kernel owns the Closure Orchestrator").
-- =============================================================================

-- Append-only history per state transition. Mirrors Coordinator.history.
CREATE TABLE IF NOT EXISTS closure_saga_history (
    saga_id        UUID NOT NULL,
    sequence_no    INT NOT NULL,
    prior_state    TEXT NOT NULL,
    new_state      TEXT NOT NULL,
    reason         TEXT NOT NULL,
    actor_gcid     UUID NOT NULL,
    transitioned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, sequence_no)
);

-- Per-domain pseudonymisation acks (10 domains; ai_kernel excluded).
CREATE TABLE IF NOT EXISTS closure_saga_domain_ack (
    saga_id   UUID NOT NULL,
    domain    TEXT NOT NULL,
    acked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, domain),
    CONSTRAINT closure_saga_domain_ack_known_domain
        CHECK (domain IN (
            'creation', 'consumption', 'delivery', 'sharing', 'a2a',
            'identity', 'tenancy', 'governance', 'observability', 'notifications'
        ))
);

-- Saga aggregate root.
CREATE TABLE IF NOT EXISTS closure_saga (
    saga_id              UUID PRIMARY KEY,
    gcid                 UUID NOT NULL,
    tenant_id            UUID NOT NULL,
    state                TEXT NOT NULL CHECK (state IN (
        'active', 'closing', 'suspended',
        'pseudonymized', 'cold_archived', 'crypto_shredded'
    )),
    requested_by_gcid    UUID NOT NULL,
    grace_period_days    INT NOT NULL CHECK (grace_period_days BETWEEN 1 AND 365),
    reason               TEXT NOT NULL DEFAULT '',
    requested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    grace_ends_at        TIMESTAMPTZ NOT NULL,
    cancelled_at         TIMESTAMPTZ,
    -- KMS / cold-archive lifecycle
    dek_resource_name    TEXT NOT NULL DEFAULT '',
    archive_uri          TEXT NOT NULL DEFAULT '',
    kms_operation_id     TEXT NOT NULL DEFAULT '',
    -- Multi-jurisdiction retention
    jurisdiction         TEXT NOT NULL DEFAULT 'SG' CHECK (jurisdiction IN (
        'EU', 'SG', 'US', 'ZA', 'BR'
    )),
    retention_ends_at    TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indices for admin O+ closure queue + admin-by-gcid lookups.
CREATE INDEX IF NOT EXISTS idx_closure_saga_state
    ON closure_saga (state)
    WHERE state IN ('closing', 'suspended', 'pseudonymized', 'cold_archived');

CREATE INDEX IF NOT EXISTS idx_closure_saga_gcid
    ON closure_saga (gcid);

CREATE INDEX IF NOT EXISTS idx_closure_saga_tenant
    ON closure_saga (tenant_id);

CREATE INDEX IF NOT EXISTS idx_closure_saga_grace_ends
    ON closure_saga (grace_ends_at)
    WHERE state = 'closing';

-- Multi-tenant RLS — composes with database-level domain isolation.
ALTER TABLE closure_saga ENABLE ROW LEVEL SECURITY;
ALTER TABLE closure_saga_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE closure_saga_domain_ack ENABLE ROW LEVEL SECURITY;

-- Tenant-scoped RLS policy. Production wires the tenant context via the
-- ``app.current_tenant`` GUC variable set on each connection.
CREATE POLICY closure_saga_tenant_isolation ON closure_saga
    USING (tenant_id::TEXT = current_setting('app.current_tenant', TRUE));

CREATE POLICY closure_saga_history_tenant_isolation ON closure_saga_history
    USING (
        saga_id IN (
            SELECT saga_id FROM closure_saga
            WHERE tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
        )
    );

CREATE POLICY closure_saga_domain_ack_tenant_isolation ON closure_saga_domain_ack
    USING (
        saga_id IN (
            SELECT saga_id FROM closure_saga
            WHERE tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
        )
    );

-- Grant the orchestrator service account read/write.
-- (Production wires this via the deployment's DSN/secret injection; the
-- role name comes from the environment.)
-- GRANT SELECT, INSERT, UPDATE ON closure_saga TO chora_closure_orch_sa;
-- GRANT SELECT, INSERT ON closure_saga_history TO chora_closure_orch_sa;
-- GRANT SELECT, INSERT ON closure_saga_domain_ack TO chora_closure_orch_sa;
