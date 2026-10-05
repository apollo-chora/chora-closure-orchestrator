-- =============================================================================
-- chora-closure-orchestrator : 0052_closure_saga_platform_policy.up.sql
--
-- The closure coordinator is CROSS-TENANT BY DESIGN: domain acks arrive
-- keyed by saga_id alone, and the dispatcher scans pending sagas across
-- every tenant — there is no tenant context to SET before the read. The
-- 0050 tenant-GUC policies therefore blocked the very first live request
-- (InsufficientPrivilege on INSERT, 2026-06-12) — the POC-era policies
-- assumed a per-tenant caller this service never was.
--
-- Fix: role-scoped PERMISSIVE allow-all policies for the orchestrator's
-- own app role. The 0050 tenant policies REMAIN for any other role that
-- ever gets SELECT on these tables (auditors etc.). Per the ADR-165
-- layered-defence rule this is a declared RLS-bypass surface: scope is
-- chora_ai_kernel closure_saga* tables + role chora_ai_kernel_app_rw
-- ONLY (single-service DB credentials), recorded in the AUTH Phase A
-- closure deploy (CHO-1719). Cross-DB isolation is untouched.
-- =============================================================================

CREATE POLICY closure_saga_platform_rw ON closure_saga
    AS PERMISSIVE FOR ALL TO chora_ai_kernel_app_rw
    USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY closure_saga_history_platform_rw ON closure_saga_history
    AS PERMISSIVE FOR ALL TO chora_ai_kernel_app_rw
    USING (TRUE) WITH CHECK (TRUE);

CREATE POLICY closure_saga_domain_ack_platform_rw ON closure_saga_domain_ack
    AS PERMISSIVE FOR ALL TO chora_ai_kernel_app_rw
    USING (TRUE) WITH CHECK (TRUE);
