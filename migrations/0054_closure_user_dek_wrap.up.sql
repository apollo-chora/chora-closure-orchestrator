-- =============================================================================
-- chora-closure-orchestrator : 0054_closure_user_dek_wrap.up.sql
--
-- ADR-186 — crypto-shred = envelope encryption. The orchestrator's per-user
-- cold-archive DEK is a random 32-byte key WRAPPED by the shared master
-- KEK (CHORA_LOCAL_KEK, held only in the service environment); only the
-- wrapped form is persisted (the plaintext DEK is never stored).
-- Crypto-shred = delete the wrapped DEK → the cold-archive blob becomes
-- permanently undecryptable.
--
-- The wrapped DEK MUST be durable: it bridges archive_to_coldline_node
-- (create+wrap) and crypto_shred_node (delete) — years apart in production
-- (grace + multi-jurisdiction retention). An in-memory dict (the prior POC
-- store) loses it on any restart, so the terminal shred could not target
-- the right DEK. This table is that durable store.
--
-- Domain  : AI Kernel / Closure Saga (orchestrator owns this table)
-- Database: chora_ai_kernel
-- Date    : 2026-06-20
--
-- Tombstone semantics (ADR-186 §D3): ``mark_deleted`` sets ``deleted_at`` and
-- scrubs ``wrapped_dek`` to ''::bytea. ``deleted_at != NULL`` makes the data
-- functionally unrecoverable immediately (decrypt refuses); a daily sweeper
-- hard-DELETEs rows past the 24h reversible window — the app-layer
-- re-creation of KMS scheduled-destruction (envelope encryption has no
-- per-user KMS object to schedule-destroy).
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS closure_user_dek_wrap (
    tenant_id        UUID        NOT NULL,
    gcid             UUID        NOT NULL,
    wrapped_dek      BYTEA       NOT NULL,                  -- KEK-wrapped DEK; '' once shredded
    kek_version      TEXT        NOT NULL DEFAULT '',       -- KMS encrypt resp name (re-wrap on rotation)
    kms_operation_id TEXT        NOT NULL DEFAULT '',       -- logical shred op id (audit traceback)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at       TIMESTAMPTZ,                           -- crypto-shred tombstone (NULL = alive)
    PRIMARY KEY (tenant_id, gcid)
);

-- Sweeper index — the daily hard-delete scan for rows past the reversible
-- window (partial: only tombstoned rows).
CREATE INDEX IF NOT EXISTS idx_closure_user_dek_wrap_shred_sweep
    ON closure_user_dek_wrap (deleted_at)
    WHERE deleted_at IS NOT NULL;

-- NO Row-Level Security on this table — DELIBERATE (ADR-186 §D6):
--   * The Closure Orchestrator is the SOLE writer and is cross-tenant BY
--     DESIGN (ADR-184): it scans/keys by saga across every tenant and has
--     no tenant context to SET, so a tenant-GUC RLS policy is inapplicable
--     (the exact wall the 0050 policies hit → 0052 platform bypass).
--   * The rows hold OPAQUE master-KEK-wrapped key ciphertext keyed by
--     (tenant_id, gcid) — NOT tenant business data, NOT readable PII
--     (useless without the master KEK + the matching AAD).
--   * Therefore this adds a KEY TABLE, NOT an RLS-bypass policy: it does
--     NOT extend the ADR-184 / ADR-165 declared-bypass chain (no new
--     PERMISSIVE allow-all policy, no third bypass surface).
GRANT SELECT, INSERT, UPDATE, DELETE ON closure_user_dek_wrap
    TO chora_ai_kernel_app_rw;

COMMIT;
