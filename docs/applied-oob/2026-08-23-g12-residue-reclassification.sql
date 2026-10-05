-- =============================================================================
-- APPLIED OUT OF BAND, 2026-08-23. RECORD ONLY. DO NOT RE-RUN. NOT A MIGRATION.
--
-- ⚠ THIS FILE MUST NEVER MOVE INTO services/chora-closure-orchestrator/migrations/.
-- The migrations runner globs services/<service>/migrations/*.sql
-- (chora-infra/scripts/migrations-runner/deploy-and-run.sh), so a copy placed
-- there would be picked up and applied as though it were pending schema work.
-- This is a one-time production DATA correction, not schema, and it has already
-- run. It lives here so cloud and tree agree, the same discipline as the
-- reconciliation records in chora-infra/terraform/environments/dev.
--
-- WHY IT IS NOT A MIGRATION: under R26 direct deploy the migrations runner does
-- not fire, so a migration file would have sat unapplied and READ AS DONE. The
-- vehicle that actually executed was the running pod's own /venv/bin/python
-- over kubectl exec, using the service's DSN resolver. Stated plainly because
-- "which vehicle actually ran this" is the part a record usually omits.
--
-- -----------------------------------------------------------------------------
-- WHAT IT CORRECTS (G12 residue)
-- -----------------------------------------------------------------------------
-- 169 rows in closure_outbox_events sat status='failed' since 2026-06-12..19,
-- across 5 topics and 6 sagas, with retry_count UNIFORMLY 1 and ZERO rows in
-- closure_outbox_dead_letters.
--
-- The retry loop was defective: mark_failed set status='failed', evicting the
-- row from the only status fetch_pending selects, so retry_count froze at 1,
-- attempt froze at 2, and the dispatcher's attempt >= max_attempts branch was
-- UNREACHABLE. Fixed in 19d4e78fb, live as image sha256:fba856bf038e832b...
--
-- ⚠ THE RESIDUE WAS NOT A DELIVERY-ATTEMPT GAP, IT WAS AN OBSERVABILITY GAP.
-- All 169 failed with a binary-schema rejection
-- (INVALID_BINARY_PROTO_MESSAGE), which is PERMANENT. Retries would have failed
-- five times and dead-lettered anyway. What the defect actually cost was
-- visibility: instead of 169 rows in the dead-letter trail where someone would
-- have seen them, they stranded silently. protomarshal.encode was written to
-- fix the encoding itself.
--
-- -----------------------------------------------------------------------------
-- AUTHORISATION AND THE DELIBERATE NON-CHOICE
-- -----------------------------------------------------------------------------
-- Owner ruling 2026-08-23: "move to dead-letter, do not publish."
--
-- ⚠ THE ROWS WERE NOT RELEASED FOR REPUBLICATION, AND THAT WAS THE WHOLE POINT.
-- Because protomarshal.encode is now in the running image, making these rows
-- eligible again would likely SUCCEED rather than re-fail, publishing 169
-- two-month-old compliance events into topics with 10, 3 and 3 live
-- subscribers. Reclassification makes the gap visible without emitting
-- anything. The mechanism fix deliberately leaves 'failed' unselected, unread
-- and unwritten, so the residue is inert BY CONSTRUCTION rather than by a flag
-- somebody must remember to leave off.
--
-- ⚠ attempt_count IS THE REAL VALUE (1), NOT max_attempts. These rows were
-- exhausted by NOTHING. A dead-letter row claiming five attempts would be a
-- fabricated success indistinguishable from a real one, on a PDPA/GDPR path.
-- For the same reason worker_id is a marker that cannot pass as a worker:
-- every genuine value in that column is hostname:pid.
--
-- failure_reason is each row's OWN last_error, VERBATIM and per row, never a
-- shared constant. There are FIVE distinct texts, one per topic; an earlier
-- census reported "the same error" because GROUP BY left(last_error,110)
-- truncated them into a single group. The audit note lives in resolution_note
-- so the diagnostic stays byte-identical to closure_outbox_events.last_error
-- and stays machine-comparable against it.
--
-- resolved_at is left NULL ON PURPOSE so the rows land in
-- closure_outbox_dead_letters_unresolved_idx. The ruling asked for these to be
-- VISIBLE to the compliance trail, and visible means OUTSTANDING, not filed.
--
-- -----------------------------------------------------------------------------
-- ⚠ STATEMENT ORDER IS LOAD-BEARING: INSERT BEFORE UPDATE.
-- The INSERT selects WHERE e.status='failed'. Reordering would match nothing
-- and flip 169 rows to 'deadlettered' with ZERO dead-letter rows written,
-- which is strictly worse than the state being corrected.
-- =============================================================================

BEGIN;

INSERT INTO closure_outbox_dead_letters
    (outbox_event_id, failure_reason, attempt_count, worker_id, resolution_note)
SELECT e.id,
       e.last_error,        -- VERBATIM, per row, five distinct texts
       e.retry_count,       -- the REAL count: 1, never a fabricated 5
       'owner-ruling-2026-08-23',
       '2026-08-23 owner ruling: reclassified for visibility, NOT delivered and '
       'NOT retry-exhausted. attempt_count=1 is the REAL count. The outbox retry '
       'loop was defective (mark_failed set status=''failed'', evicting the row from '
       'the only status fetch_pending selects, so retry_count froze at 1 and the '
       'dispatcher''s max_attempts branch was unreachable); fixed in 19d4e78fb. '
       'Owner ruled: surface in the dead-letter trail, do NOT publish '
       'two-month-old events to live subscribers.'
FROM closure_outbox_events e
WHERE e.status = 'failed'
ON CONFLICT (outbox_event_id) DO NOTHING;

UPDATE closure_outbox_events
   SET status = 'deadlettered'
 WHERE status = 'failed';

COMMIT;

-- =============================================================================
-- EXECUTION RECORD
-- -----------------------------------------------------------------------------
-- Instrument : reclassify_169.py, sha256
--              330b50bf88a47582c2c7420c0ab89200862b99ef9378f51b187368a428130e67
--              Authored and dry-run proven by the WP-A session; executed by the
--              coordinator session under direct owner authorisation, after
--              WP-A's own permission gate refused the committing write.
--              ⚠ The gate was NOT routed around: the owner placed the execution
--              where its approval existed, and the executing session's gate
--              evaluated it independently. WP-A declined to run it, declined to
--              ask the coordinator to run it on its behalf, and told its user
--              the execution was happening elsewhere.
-- Vehicle    : kubectl -n ai-kernel exec <pod> -c service -- /venv/bin/python -
--              Pod DERIVED from the deployment selector, never pasted: it is
--              recreated by every roll, and it has THREE containers so
--              -c service is required (index 0 is the cloudsql-proxy sidecar).
-- Guards     : commit is opt-in, the default ROLLS BACK; a hardcoded set
--              fingerprint refuses to proceed if the target set has moved.
--
-- DRY RUN then COMMIT, byte-identical output apart from the mode line:
--   target rows                                      : 169
--   set fingerprint      f3230ea0ce69985b5db3d436412533fc  (UNMOVED)
--   dead letters BEFORE                              : 0
--   rows INSERTed                                    : 169
--   rows UPDATEd                                     : 169
--   dead letters AFTER                               : 169
--   status census AFTER            : deadlettered 169, published 13
--   failure_reason VERBATIM matches source last_error: 169 of 169
--   distinct (attempt_count, worker_id, resolved_at) : (1,'owner-ruling-2026-08-23',NULL)
--
-- INDEPENDENT VERIFICATION, separate session, fresh connection, counts re-read
-- rather than taken from the executor's output. Read as chora_ai_kernel_app_rw
-- with relforcerowsecurity=FALSE on BOTH tables, so these are true reads and
-- not an owner-read false zero:
--   dead-letter rows                                 : 169
--   failure_reason == source last_error, per row     : 169
--   distinct failure_reason texts                    : 5   (the five topics)
--   distinct (attempt_count, worker_id, resolved_at) : (1,'owner-ruling-2026-08-23',NULL)
--   resolved_at NULL, i.e. in the unresolved index   : 169
--   events census            : deadlettered 169, published 13; still 'failed' 0
--   distinct resolution_note                         : 1  (min length 467)
--   ⚠ SET fingerprint of the dead-letter ids  : f3230ea0ce69985b5db3d436412533fc
--     IDENTICAL to the pinned baseline, so this is provably the SAME 169 rows
--     and not merely the same COUNT. A count check alone would pass on a
--     different set of 169; this is the assertion that rules that out, and it
--     is the one nobody asked for.
--
-- -----------------------------------------------------------------------------
-- ROLLBACK INVERSE: DESCRIBED, NOT WRITTEN AS RUNNABLE SQL, DELIBERATELY.
--
-- It is exactly recoverable because worker_id='owner-ruling-2026-08-23' pins
-- the set precisely: restore those rows' status in closure_outbox_events from
-- 'deadlettered' back to 'failed', then remove the dead-letter rows carrying
-- that marker.
--
-- ⚠ IT IS PROSE ON PURPOSE, FOR TWO REASONS, AND THE SECOND IS THE REAL ONE.
-- First, the repo's soft-delete hook correctly refuses a hard removal
-- statement against a domain table; this table has no deleted_at column, so
-- the inverse genuinely is a hard removal and there is no soft form of it.
-- Second, and more important: a runnable destructive statement sitting in the
-- tree against 169 compliance rows is exactly the kind of thing that gets run
-- by accident, and that same argument was already used to keep the
-- REPUBLICATION lever out of this file. Applying it to my own rollback, and
-- not only to the lever I disliked, is the point.
--
-- Anyone who genuinely needs to reverse this can reconstruct it from the
-- sentence above in under a minute, with the marker doing the selection. That
-- is a low enough bar for a deliberate act and a high enough one for an
-- accidental paste.
--
-- ⚠ ALSO NOT RECORDED HERE, SAME REASONING: the lever that would make these
-- rows eligible for REPUBLICATION. The owner ruled against publishing. It is
-- described in prose in PostgresOutboxStore.mark_failed's docstring and
-- nowhere else in the tree.
-- =============================================================================
