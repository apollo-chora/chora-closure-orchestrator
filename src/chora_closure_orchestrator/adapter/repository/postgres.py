"""PostgresCoordinatorRepository — real ``CoordinatorRepository`` against
``chora_ai_kernel`` (tables from ``migrations/0050_closure_initial.sql``).

Connection: a psycopg_pool ``AsyncConnectionPool``. Every public method
acquires its OWN connection for the duration of one logical operation
(autocommit OFF; ``save`` is a single transaction across saga + history +
acks, committed at the end).

⚠ THE POOL IS A CORRECTNESS REQUIREMENT, NOT A THROUGHPUT ONE (G19).

This class previously held ONE long-lived connection, and that one instance
was handed to THREE independent concurrent users: the SagaDriver task, the
per-domain ack handlers (one coroutine per ack subscription, and there are
10 domains), and the FastAPI request handlers. The read methods end with
``finally: conn.rollback()``, which is a CONNECTION-level rollback and is
not scoped to a cursor. So a reader running between two statements of a
concurrent ``save`` rolled back the WRITER's uncommitted work, and the
writer's ``commit()`` then returned SUCCESS on an empty transaction. No
exception anywhere. Measured against the shipped code, an interleaved read
left the saga row missing while a history row for it survived, so the
failure mode was a TORN AGGREGATE rather than a clean loss.

The old note here claimed "psycopg3 serialises concurrent task access to one
connection". That is true of STATEMENTS and false of TRANSACTIONS: psycopg3's
lock is held for one operation, so the wire never corrupts, but nothing
scopes a transaction to a task. The rest of it, "the orchestrator's modest
closure QPS makes a single connection adequate", was a probability argument
rather than a correctness one, and the burst is designed in: fan-out
publishes 10 pseudonymise requests at once, so the 10 acks come back
together and are handled concurrently while the driver ticks the same sagas.

⚠ DO NOT "SIMPLIFY" BY REMOVING THE ROLLBACKS IN THE READ METHODS. They exist
for a different real bug (see ``_get``): without them a read left the
connection idle-in-transaction holding ``closure_saga`` locks, which blocked
migration 0053. The fix for that bug created the mechanism for this one. With
a connection per operation both are correct at once, which is the point.

``outbox_conn`` and ``dek_store_conn`` already owned dedicated connections;
this was the one that missed the pattern.

The orchestrator connects as the table-owning role, so the tenant-RLS
policies from 0001 do not constrain it (platform service operating across
tenants by design — saga rows are keyed by tenant_id for O+ queries).
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from chora_closure_orchestrator.domain.closure import (
    Coordinator,
    ErrSagaNotFound,
    State,
)
from chora_closure_orchestrator.domain.closure.coordinator import (
    DomainAck,
    HistoryEntry,
)

_SAGA_UPSERT = """
INSERT INTO closure_saga (
    saga_id, gcid, tenant_id, state, requested_by_gcid,
    grace_period_days, reason, requested_at, grace_ends_at,
    cancelled_at, updated_at
) VALUES (
    %(saga_id)s, %(gcid)s, %(tenant_id)s, %(state)s, %(requested_by_gcid)s,
    %(grace_period_days)s, %(reason)s, %(requested_at)s, %(grace_ends_at)s,
    %(cancelled_at)s, %(updated_at)s
)
ON CONFLICT (saga_id) DO UPDATE SET
    state = EXCLUDED.state,
    reason = EXCLUDED.reason,
    grace_ends_at = EXCLUDED.grace_ends_at,
    cancelled_at = EXCLUDED.cancelled_at,
    updated_at = EXCLUDED.updated_at
""".strip()

_HISTORY_INSERT = """
INSERT INTO closure_saga_history (
    saga_id, sequence_no, prior_state, new_state, reason,
    actor_gcid, transitioned_at
) VALUES (
    %(saga_id)s, %(sequence_no)s, %(prior_state)s, %(new_state)s,
    %(reason)s, %(actor_gcid)s, %(transitioned_at)s
)
ON CONFLICT (saga_id, sequence_no) DO NOTHING
""".strip()

_ACK_INSERT = """
INSERT INTO closure_saga_domain_ack (saga_id, domain, acked_at)
VALUES (%(saga_id)s, %(domain)s, %(acked_at)s)
ON CONFLICT (saga_id, domain) DO NOTHING
""".strip()

_SAGA_COLUMNS = """
    saga_id::TEXT, gcid::TEXT, tenant_id::TEXT, state, reason,
    requested_by_gcid::TEXT, grace_period_days, requested_at,
    grace_ends_at, cancelled_at, updated_at
""".strip()

_SAGA_SELECT = f"""
SELECT {_SAGA_COLUMNS}
FROM closure_saga
WHERE saga_id = %(saga_id)s
""".strip()

_SAGA_SELECT_BY_GCID = f"""
SELECT {_SAGA_COLUMNS}
FROM closure_saga
WHERE gcid = %(gcid)s
ORDER BY requested_at DESC
LIMIT 1
""".strip()

_SAGA_SELECT_BY_STATE = f"""
SELECT {_SAGA_COLUMNS}
FROM closure_saga
WHERE state = %(state)s
ORDER BY updated_at ASC
""".strip()

_HISTORY_SELECT = """
SELECT prior_state, new_state, reason, actor_gcid::TEXT, transitioned_at
FROM closure_saga_history
WHERE saga_id = %(saga_id)s
ORDER BY sequence_no ASC
""".strip()

_ACK_SELECT = """
SELECT domain, acked_at
FROM closure_saga_domain_ack
WHERE saga_id = %(saga_id)s
ORDER BY acked_at ASC
""".strip()


class PostgresCoordinatorRepository:
    """psycopg-backed ``CoordinatorRepository`` over a CONNECTION POOL.

    ⚠ ONE CONNECTION PER LOGICAL OPERATION, and that is a correctness
    requirement rather than a throughput one. See the module docstring.
    """

    def __init__(self, *, pool: Any) -> None:
        # Anything exposing psycopg_pool's ``connection()`` async context
        # manager. Each acquisition is EXCLUSIVE for its duration, which is
        # what stops one caller's rollback from ending another's transaction.
        self._pool = pool

    # ------------------------------------------------------------------
    # CoordinatorRepository protocol
    # ------------------------------------------------------------------

    async def save(self, c: Coordinator) -> None:
        async with self._pool.connection() as conn:
            await self._save(conn, c)

    async def _save(self, conn: Any, c: Coordinator) -> None:
        # On any failure the transaction MUST be rolled back, else psycopg
        # leaves the connection in InFailedSqlTransaction and every subsequent
        # statement on it fails ("current transaction is aborted"). A pooled
        # connection would carry that poison back to the pool for the NEXT
        # caller, so this guard matters more now, not less.
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    _SAGA_UPSERT,
                    {
                        "saga_id": c.saga_id,
                        "gcid": c.gcid,
                        "tenant_id": c.tenant_id,
                        "state": c.state.value,
                        "requested_by_gcid": c.requested_by_gcid,
                        "grace_period_days": c.grace_period_days,
                        "reason": c.reason,
                        "requested_at": c.requested_at,
                        "grace_ends_at": c.grace_ends_at,
                        "cancelled_at": c.cancelled_at,
                        "updated_at": c.updated_at,
                    },
                )
                for i, h in enumerate(c.history, start=1):
                    await cur.execute(
                        _HISTORY_INSERT,
                        {
                            "saga_id": c.saga_id,
                            "sequence_no": i,
                            "prior_state": h.prior_state.value,
                            "new_state": h.new_state.value,
                            "reason": h.reason,
                            "actor_gcid": h.actor_gcid,
                            "transitioned_at": h.transitioned_at,
                        },
                    )
                for a in c.domain_acks:
                    await cur.execute(
                        _ACK_INSERT,
                        {
                            "saga_id": c.saga_id,
                            "domain": a.domain,
                            "acked_at": a.acked_at,
                        },
                    )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def get(self, saga_id: str) -> Coordinator:
        async with self._pool.connection() as conn:
            return await self._get(conn, saga_id)

    async def _get(self, conn: Any, saga_id: str) -> Coordinator:
        # rollback in finally ends the read transaction so the connection never
        # goes back to the pool idle-in-transaction holding closure_saga locks
        # (which blocked migration 0053). A read commits nothing. This is safe
        # ONLY because the connection is ours alone for this call; on the old
        # shared connection this same line ate concurrent writers (G19).
        try:
            async with conn.cursor() as cur:
                await cur.execute(_SAGA_SELECT, {"saga_id": saga_id})
                row = await cur.fetchone()
            if row is None:
                raise ErrSagaNotFound(f"saga not found: {saga_id}")
            return await self._hydrate(conn, row)
        finally:
            await conn.rollback()

    async def get_by_gcid(self, gcid: str) -> Coordinator | None:
        async with self._pool.connection() as conn:
            return await self._get_by_gcid(conn, gcid)

    async def _get_by_gcid(self, conn: Any, gcid: str) -> Coordinator | None:
        try:
            async with conn.cursor() as cur:
                await cur.execute(_SAGA_SELECT_BY_GCID, {"gcid": gcid})
                row = await cur.fetchone()
            if row is None:
                return None
            return await self._hydrate(conn, row)
        finally:
            await conn.rollback()

    async def list_by_state(self, state: State, limit: int = 50) -> list[Coordinator]:
        async with self._pool.connection() as conn:
            return await self._list_by_state(conn, state, limit)

    async def _list_by_state(self, conn: Any, state: State, limit: int = 50) -> list[Coordinator]:
        sql = _SAGA_SELECT_BY_STATE
        params: dict[str, Any] = {"state": state.value}
        if limit > 0:
            sql += "\nLIMIT %(limit)s"
            params["limit"] = limit
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
            return [await self._hydrate(conn, r) for r in rows]
        finally:
            await conn.rollback()

    # ------------------------------------------------------------------
    # Hydration
    # ------------------------------------------------------------------

    async def _hydrate(self, conn: Any, row: Any) -> Coordinator:
        (
            saga_id,
            gcid,
            tenant_id,
            state,
            reason,
            requested_by_gcid,
            grace_period_days,
            requested_at,
            grace_ends_at,
            cancelled_at,
            updated_at,
        ) = row
        async with conn.cursor() as cur:
            await cur.execute(_HISTORY_SELECT, {"saga_id": saga_id})
            history_rows = await cur.fetchall()
        async with conn.cursor() as cur:
            await cur.execute(_ACK_SELECT, {"saga_id": saga_id})
            ack_rows = await cur.fetchall()
        return Coordinator(
            saga_id=str(saga_id),
            gcid=str(gcid),
            tenant_id=str(tenant_id),
            state=State(state),
            reason=reason or "",
            requested_by_gcid=str(requested_by_gcid),
            grace_period_days=int(grace_period_days),
            requested_at=_aware(requested_at),
            grace_ends_at=_aware(grace_ends_at),
            updated_at=_aware(updated_at),
            cancelled_at=_aware(cancelled_at) if cancelled_at else None,
            history=[
                HistoryEntry(
                    prior_state=State(h[0]),
                    new_state=State(h[1]),
                    reason=h[2] or "",
                    actor_gcid=str(h[3]),
                    transitioned_at=_aware(h[4]),
                )
                for h in history_rows
            ],
            domain_acks=[DomainAck(domain=a[0], acked_at=_aware(a[1])) for a in ack_rows],
        )


def _aware(ts: _dt.datetime) -> _dt.datetime:
    """Normalise DB timestamps to UTC-aware."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=_dt.UTC)
    return ts.astimezone(_dt.UTC)


__all__ = ["PostgresCoordinatorRepository"]
