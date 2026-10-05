-- =============================================================================
-- chora-closure-orchestrator : 0054_closure_user_dek_wrap.down.sql
--
-- Reverts 0054. Dropping the table discards every wrapped DEK — after this,
-- the orchestrator's local KMS adapter can no longer durably persist
-- DEKs and falls back to the in-memory store (dev only). Do NOT run against
-- a database holding live (non-shredded) cold-archive DEKs — it would orphan
-- the corresponding archive blobs (unrecoverable, which for shredded
-- users is intended, but for in-flight closures is data loss).
-- =============================================================================

BEGIN;

DROP INDEX IF EXISTS idx_closure_user_dek_wrap_shred_sweep;
DROP TABLE IF EXISTS closure_user_dek_wrap;

COMMIT;
