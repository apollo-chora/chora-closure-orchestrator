-- =============================================================================
-- chora-closure-orchestrator : 0002_outbox.sql
--
-- Adds the transactional outbox + DLQ tables for the closure saga's emitted
-- events, per `feedback_d6_resilience_first_class` B.6.2 sub-deliverable (a)
-- and the expanded D6.3 multi-tenant + multi-workflow chaos scope (user
-- directive 2026-05-12).
--
-- Domain  : AI Kernel / Closure Saga (orchestrator owns these tables)
-- Database: chora_ai_kernel
-- Date    : 2026-05-12
--
-- HARD INVARIANTS
--   * Outbox rows live in the SAME database as LangGraph PostgresSaver
--     checkpoints (chora_ai_kernel). When a saga node emits an event, the
--     outbox row write + the LangGraph checkpoint write are both
--     persisted to this DB; the dispatcher publishes to the NATS JetStream
--     event bus.
--   * tenant_id is captured as a top-level column for D6.3 multi-tenant
--     isolation indexing + RLS — production sagas may carry events from
--     many tenants through the same event pipe.
--   * idempotency_key + envelope mandatory per CLAUDE.md cross-cutting rule.
--   * Cross-DB queries remain forbidden — domain subscribers read events
--     from the event bus, never from this table.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS closure_outbox_events (
    id              TEXT        PRIMARY KEY,                 -- UUIDv7
    saga_id         UUID        NOT NULL,                    -- aggregate root
    tenant_id       UUID        NOT NULL,                    -- D6.3 isolation
    gcid            UUID        NOT NULL,                    -- subject
    event_type      TEXT        NOT NULL,                    -- e.g., 'closure.requested'
    topic           TEXT        NOT NULL,                    -- 'chora.closure.requested.v1'
    payload         BYTEA       NOT NULL,                    -- Protobuf bytes (POC: JSON)
    envelope        JSONB       NOT NULL,                    -- full envelope: event_id,
                                                             -- idempotency_key, traceparent,
                                                             -- tracestate, source_project,
                                                             -- source_service, schema_version
    idempotency_key TEXT        NOT NULL,                    -- dedupe key (extracted from envelope)
    occurred_at     TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dispatcher poll query — find next pending event by occurred_at.
CREATE INDEX IF NOT EXISTS closure_outbox_events_pending_idx
    ON closure_outbox_events (occurred_at ASC) WHERE status = 'pending';

-- D6.3 multi-tenant isolation lookup: dispatcher MAY filter per-tenant.
CREATE INDEX IF NOT EXISTS closure_outbox_events_tenant_idx
    ON closure_outbox_events (tenant_id, status, occurred_at);

-- Per-saga event lookup (debugging / replay).
CREATE INDEX IF NOT EXISTS closure_outbox_events_saga_idx
    ON closure_outbox_events (saga_id, occurred_at);

-- Per-topic dispatcher worker mode.
CREATE INDEX IF NOT EXISTS closure_outbox_events_topic_idx
    ON closure_outbox_events (topic, status);

-- Idempotency dedupe — events emitted twice (e.g., saga resume re-emits)
-- collapse on this key. Unique index because dedupe MUST be exact.
CREATE UNIQUE INDEX IF NOT EXISTS closure_outbox_events_idempotency_idx
    ON closure_outbox_events (idempotency_key);

-- Dispatcher checkpoint — at most one row per (worker_id, topic).
-- Tracks the last successfully published event so the worker can resume
-- after a pod-death without re-publishing already-acked events.
CREATE TABLE IF NOT EXISTS closure_outbox_dispatch_checkpoints (
    worker_id                  TEXT        NOT NULL,
    topic                      TEXT        NOT NULL,
    last_processed_outbox_id   TEXT        NOT NULL,
    last_processed_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker_id, topic)
);

CREATE INDEX IF NOT EXISTS closure_outbox_dispatch_checkpoints_topic_idx
    ON closure_outbox_dispatch_checkpoints (topic, updated_at DESC);

-- DLQ pointer — events that exceed max_retries land here. resolved_at
-- is set when an operator manually replays via the orchestrator runbook
-- (per B.6.2 sub-deliverable d "Orchestrator-side DLQ awareness").
CREATE TABLE IF NOT EXISTS closure_outbox_dead_letters (
    outbox_event_id   TEXT        PRIMARY KEY REFERENCES closure_outbox_events(id),
    failure_reason    TEXT        NOT NULL,
    attempt_count     INT         NOT NULL,
    worker_id         TEXT        NOT NULL,
    deadlettered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    resolution_note   TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS closure_outbox_dead_letters_unresolved_idx
    ON closure_outbox_dead_letters (deadlettered_at DESC) WHERE resolved_at IS NULL;

-- Multi-tenant RLS — same pattern as 0001_initial.sql (closure_saga table).
-- POC connections do NOT set app.current_tenant (the closure orchestrator
-- writes events across many tenants per session); production wires this
-- via per-connection GUC. RLS policy remains permissive when the GUC is
-- unset to keep the POC dispatcher functional; per ADR-141 tenant
-- isolation is also enforced at the application + envelope layer.
ALTER TABLE closure_outbox_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY closure_outbox_events_tenant_isolation ON closure_outbox_events
    USING (
        current_setting('app.current_tenant', TRUE) IS NULL
        OR current_setting('app.current_tenant', TRUE) = ''
        OR tenant_id::TEXT = current_setting('app.current_tenant', TRUE)
    );

COMMIT;
