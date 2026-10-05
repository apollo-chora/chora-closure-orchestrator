"""Outbox store layer.

``OutboxStore`` is the Protocol the dispatcher uses. Two implementations:

* ``InMemoryOutboxStore`` — for tests.
* ``PostgresOutboxStore`` — production; wraps a psycopg async connection
  pointing at ``chora_ai_kernel`` (where ``closure_outbox_events`` lives).
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class OutboxRow:
    """One row from ``closure_outbox_events``."""

    id: str
    saga_id: str
    tenant_id: str
    gcid: str
    event_type: str
    topic: str
    payload: bytes
    envelope: dict[str, str]
    idempotency_key: str
    retry_count: int = 0
    occurred_at: _dt.datetime = field(default_factory=lambda: _dt.datetime.now(_dt.UTC))


class OutboxStore(Protocol):
    """Port the dispatcher uses to drain the outbox."""

    async def fetch_pending(self, *, limit: int) -> list[OutboxRow]: ...

    async def mark_published(self, row_id: str) -> None: ...

    async def mark_failed(self, row_id: str, error: str) -> None: ...

    async def deadletter(self, row_id: str, failure_reason: str, attempt_count: int) -> None: ...


class InMemoryOutboxStore:
    """Hermetic in-memory implementation for unit tests."""

    def __init__(self, pending: list[OutboxRow] | None = None) -> None:
        self.pending: list[OutboxRow] = list(pending or [])
        self.published: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self.deadlettered: list[tuple[str, str, int]] = []

    async def fetch_pending(self, *, limit: int) -> list[OutboxRow]:
        return self.pending[:limit]

    async def mark_published(self, row_id: str) -> None:
        self.published.append(row_id)
        self.pending = [r for r in self.pending if r.id != row_id]

    async def mark_failed(self, row_id: str, error: str) -> None:
        # ⚠ NOTE THE SEMANTIC, because it is the one the Postgres store LACKED
        # until G12: a failed attempt LEAVES THE ROW PENDING and only bumps the
        # counter. This double was right and production was wrong, so every
        # dispatcher test passed while the real store stranded 169 rows. A fake
        # that does not share the real implementation's behaviour turns a green
        # suite into evidence of nothing.
        self.failed.append((row_id, error))
        for r in self.pending:
            if r.id == row_id:
                r.retry_count += 1
                break

    async def deadletter(self, row_id: str, failure_reason: str, attempt_count: int) -> None:
        self.deadlettered.append((row_id, failure_reason, attempt_count))
        self.pending = [r for r in self.pending if r.id != row_id]


class PostgresOutboxStore:
    """psycopg-backed store wrapping ``closure_outbox_events`` in
    ``chora_ai_kernel``.

    The dispatcher holds one of these long-lived; per-call queries open
    a cursor on the supplied connection.

    ⚠ WORKER SAFETY, STATED ACCURATELY (G13). ``FOR UPDATE SKIP LOCKED`` in
    ``fetch_pending`` protects THE FETCH: two workers polling at the same
    instant will not select the same rows. It does NOT protect the whole batch
    for the whole drain, and the previous wording here ("multiple dispatcher
    workers can drain concurrently without re-publishing the same row") claimed
    that it did.

    The reason is the per-row commit. ``mark_published`` / ``mark_failed`` /
    ``deadletter`` each COMMIT, and that commit ends the transaction the SELECT
    opened, releasing the row locks on every row still unprocessed in the
    batch. A second worker can then fetch rows 2..N of the first worker's batch
    while the first worker is still iterating its own in-memory copy, and both
    publish them. So with more than one replica AND more than one row per
    batch, a double publish is reachable, most plausibly during a rollout when
    two pods overlap.

    What actually mitigates it today is downstream idempotency: every event
    carries ``idempotency_key`` in its envelope and consumers are required to
    be idempotent. That is a real mitigation, not a hope, but it is a DIFFERENT
    mechanism from the one this docstring used to claim.

    ⚠ THE OBVIOUS "FIX" IS WORSE, which is why the docstring is what changed.
    Holding ONE transaction across the whole batch would make the claim true,
    at the cost of all-or-nothing batch semantics AND of keeping a transaction
    open across up to ``batch_size`` network round-trips to the broker. That is
    precisely the long-lived-transaction hazard that pinned oldest-xmin and
    blocked migration 0053 (see ``test_outbox_store_release.py``). Making this
    batch-atomic is a deliberate design change that trades one known bug for
    another, and it is not being made as a side effect of a wording fix.
    """

    def __init__(self, *, conn: Any, worker_id: str, retry_backoff_seconds: int = 60) -> None:
        if not worker_id:
            raise ValueError("worker_id required")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be >= 0")
        self._conn = conn
        self._worker_id = worker_id
        # G12 invariant 3: a row that just failed must not be re-fetched on the
        # very next poll (default poll interval is 0.5s).
        self._retry_backoff_seconds = retry_backoff_seconds

    # ------------------------------------------------------------------
    # ⚠ EVERY method below releases the transaction on BOTH paths, and the two
    # releases are deliberately DIFFERENT.
    #
    #   SUCCESS path -> COMMIT. Never a rollback: on a shared connection that
    #   discards co-tenant writes (2026-08-14). See the empty-path note inside
    #   fetch_pending.
    #
    #   ERROR path -> ROLLBACK, then re-raise. A failed statement leaves the
    #   transaction ABORTED, and psycopg then fails EVERY later statement on
    #   this connection with InFailedSqlTransaction until something ends it.
    #   Nothing did: drain_once's except wraps only publisher.publish, so a
    #   raise here escapes to _dispatch_loop, which logs at WARNING, sleeps and
    #   retries forever against the aborted transaction while the pod stays
    #   READY and the outbox has silently stopped dispatching. Rolling back
    #   here throws nothing away, because Postgres already discarded the
    #   transaction's work when the statement failed.
    #
    # This mirrors PostgresCoordinatorRepository.save, which has carried the
    # same guard, and the same reasoning in a comment, since it was written.
    # The guard did not travel to this file for four methods.
    # ------------------------------------------------------------------

    async def fetch_pending(self, *, limit: int) -> list[OutboxRow]:
        # Split so the error guard does not re-indent (and obscure) the
        # empty-path release note below, which is load-bearing on its own.
        try:
            return await self._fetch_pending(limit=limit)
        except Exception:
            await self._conn.rollback()
            raise

    async def _fetch_pending(self, *, limit: int) -> list[OutboxRow]:
        async with self._conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, saga_id::TEXT, tenant_id::TEXT, gcid::TEXT,
                       event_type, topic, payload, envelope::TEXT,
                       idempotency_key, retry_count, occurred_at
                FROM closure_outbox_events
                WHERE status = 'pending'
                  AND (last_attempt_at IS NULL
                       OR last_attempt_at
                          < now() - make_interval(secs => %(backoff_seconds)s))
                ORDER BY occurred_at ASC
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
                """,
                {"limit": limit, "backoff_seconds": self._retry_backoff_seconds},
            )
            rows = await cur.fetchall()

        if not rows:
            # RELEASE THE TRANSACTION THE SELECT OPENED.
            #
            # The dispatch connection is NOT autocommit (see wiring.py, which
            # connects it without autocommit=True unlike the _connect_autocommit
            # helper it uses elsewhere), so the statement above opened one
            # implicitly. On the NON-empty path the caller's mark_published /
            # mark_failed / deadletter each commit and close it. On the EMPTY
            # path the dispatcher's `for row in rows:` body never runs, so
            # nothing committed and the transaction stayed open across every
            # subsequent poll: measured live at over NINE HOURS with idle_for
            # under half a second, which pins the oldest-xmin horizon so VACUUM
            # cannot reclaim anywhere in the database, and blocks DDL needing a
            # lock on this table. On a quiet estate the empty path is the NORMAL
            # path, which is why it never self-healed.
            #
            # COMMIT, never ROLLBACK: on a shared connection a rollback discards
            # co-tenant writes (2026-08-14). Here it is unambiguous anyway,
            # because zero rows came back means zero row locks are held, so this
            # cannot release a lock early.
            await self._conn.commit()
            return []

        import json as _json

        out: list[OutboxRow] = []
        for r in rows:
            (
                _id,
                saga_id,
                tenant_id,
                gcid,
                event_type,
                topic,
                payload,
                envelope_str,
                idempotency_key,
                retry_count,
                occurred_at,
            ) = r
            envelope = _json.loads(envelope_str) if envelope_str else {}
            out.append(
                OutboxRow(
                    id=_id,
                    saga_id=saga_id,
                    tenant_id=tenant_id,
                    gcid=gcid,
                    event_type=event_type,
                    topic=topic,
                    payload=bytes(payload),
                    envelope={str(k): str(v) for k, v in envelope.items()},
                    idempotency_key=idempotency_key,
                    retry_count=retry_count,
                    occurred_at=occurred_at,
                )
            )
        return out

    async def mark_published(self, row_id: str) -> None:
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE closure_outbox_events
                    SET status='published', published_at=now()
                    WHERE id = %(id)s
                    """,
                    {"id": row_id},
                )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    async def mark_failed(self, row_id: str, error: str) -> None:
        """Record ONE failed attempt. The row STAYS eligible (G12).

        ⚠ THIS DELIBERATELY DOES NOT SET status='failed', and that is the whole
        fix. Doing so moved the row out of the only status ``fetch_pending``
        can see, so ``retry_count`` froze at 1, ``attempt`` at 2, and the
        dispatcher's ``attempt >= max_attempts`` branch became UNREACHABLE.
        Production: 169 stranded rows, 0 dead letters, ``retry_count``
        uniformly 1 across 5 topics, 6 sagas and 8 days.

        ⚠ AND THIS IS WHY THE 169 EXISTING ROWS ARE SAFE. They sit in 'failed',
        and after this change NOTHING selects, writes or reads that value. They
        are inert BY CONSTRUCTION rather than by a feature flag or a NULL check
        on a column added for the purpose. Releasing them is a COMPLIANCE
        DECISION FOR THE OWNER, not a side effect of a code fix, because
        protomarshal.encode has since been added so a retry today would likely
        SUCCEED and publish two-month-old events to live subscribers.

        The owner's documented release lever, which is deliberately NOT
        automated and lives nowhere runnable:
            UPDATE closure_outbox_events
               SET status='pending', retry_count=0, last_attempt_at=NULL
             WHERE <the rows the owner has ruled on>;
        """
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE closure_outbox_events
                    SET retry_count = retry_count + 1,
                        last_error = %(err)s,
                        last_attempt_at = now()
                    WHERE id = %(id)s
                    """,
                    {"id": row_id, "err": error[:1000]},
                )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    async def deadletter(self, row_id: str, failure_reason: str, attempt_count: int) -> None:
        # TWO statements, so the rollback also protects against a HALF-WRITTEN
        # pair: a dead-letter record whose source row still reads 'pending'
        # would be re-dispatched forever against a row already recorded dead.
        try:
            async with self._conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO closure_outbox_dead_letters
                        (outbox_event_id, failure_reason, attempt_count, worker_id)
                    VALUES (%(id)s, %(reason)s, %(count)s, %(worker)s)
                    ON CONFLICT (outbox_event_id) DO NOTHING
                    """,
                    {
                        "id": row_id,
                        "reason": failure_reason[:1000],
                        "count": attempt_count,
                        "worker": self._worker_id,
                    },
                )
                await cur.execute(
                    """
                    UPDATE closure_outbox_events
                    SET status='deadlettered',
                        last_attempt_at = now()
                    WHERE id = %(id)s
                    """,
                    {"id": row_id},
                )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise


__all__ = [
    "InMemoryOutboxStore",
    "OutboxRow",
    "OutboxStore",
    "PostgresOutboxStore",
]
