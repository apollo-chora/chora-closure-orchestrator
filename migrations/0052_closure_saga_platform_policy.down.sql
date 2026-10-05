-- Reverse of 0052 — drops the platform role policies; the 0050 tenant
-- policies remain (the coordinator then breaks again by design).
DROP POLICY IF EXISTS closure_saga_platform_rw ON closure_saga;
DROP POLICY IF EXISTS closure_saga_history_platform_rw ON closure_saga_history;
DROP POLICY IF EXISTS closure_saga_domain_ack_platform_rw ON closure_saga_domain_ack;
