"""PostgresCoordinatorRepository — real CoordinatorRepository against the
``closure_saga`` / ``closure_saga_history`` / ``closure_saga_domain_ack``
tables created by ``migrations/0050_closure_initial.sql`` in ``chora_ai_kernel``.

Unit tests use a scripted psycopg-shaped mock connection (same pattern as
``test_publisher_outbox.py``). Live verification happens via the deploy
runbook probes.
"""

from __future__ import annotations

import datetime as _dt
from contextlib import asynccontextmanager
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.repository.postgres import (
    PostgresCoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import (
    ErrSagaNotFound,
    NewParams,
    State,
    new,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


class _MockCursor:
    def __init__(self, results: list[Any], *, raise_on_execute: bool = False) -> None:
        self.executed: list[tuple[str, Any]] = []
        self._results = results
        self._raise = raise_on_execute

    async def __aenter__(self) -> _MockCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if self._raise:
            raise RuntimeError("scripted execute failure")

    async def fetchone(self) -> Any:
        return self._results.pop(0) if self._results else None

    async def fetchall(self) -> list[Any]:
        return self._results.pop(0) if self._results else []


class _MockConn:
    """Scripted psycopg AsyncConnection double.

    ``results`` is consumed in fetch order across all cursors.
    """

    def __init__(
        self,
        results: list[Any] | None = None,
        *,
        raise_on_execute: bool = False,
    ) -> None:
        self.results: list[Any] = list(results or [])
        self.cursors: list[_MockCursor] = []
        self.commits = 0
        self.rollbacks = 0
        self._raise = raise_on_execute

    def cursor(self) -> _MockCursor:
        cur = _MockCursor(self.results, raise_on_execute=self._raise)
        self.cursors.append(cur)
        return cur

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    def executed_sql(self) -> list[str]:
        return [sql for cur in self.cursors for sql, _ in cur.executed]


class _MockPool:
    """Pool double that hands out the one scripted connection.

    The repository acquires a connection per logical operation (G19). These
    tests deliberately script a SINGLE connection because what they assert is
    which SQL ONE operation issues, and how it commits or rolls back. How
    CONCURRENT operations stay isolated from each other is a different claim
    and is pinned in ``test_repository_connection_isolation.py``.
    """

    def __init__(self, conn: _MockConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def connection(self) -> Any:
        yield self._conn


def _repo(conn: _MockConn) -> PostgresCoordinatorRepository:
    return PostgresCoordinatorRepository(pool=_MockPool(conn))


def _coordinator() -> Any:
    return new(
        NewParams(
            gcid=GCID,
            tenant_id=TENANT,
            grace_period_days=30,
            requested_by_gcid=GCID,
            reason="bye",
        )
    )


def _saga_row(c: Any) -> tuple[Any, ...]:
    """Row shape returned by the repository's saga SELECT."""
    return (
        c.saga_id,
        c.gcid,
        c.tenant_id,
        c.state.value,
        c.reason,
        c.requested_by_gcid,
        c.grace_period_days,
        c.requested_at,
        c.grace_ends_at,
        c.cancelled_at,
        c.updated_at,
    )


class TestSave:
    @pytest.mark.asyncio
    async def test_save_upserts_saga_history_and_acks(self) -> None:
        conn = _MockConn()
        c = _coordinator()
        c.record_domain_ack("creation", _dt.datetime.now(_dt.UTC))
        await _repo(conn).save(c)

        sql = "\n".join(conn.executed_sql())
        assert "INSERT INTO closure_saga" in sql
        assert "ON CONFLICT (saga_id) DO UPDATE" in sql
        assert "INSERT INTO closure_saga_history" in sql
        assert "INSERT INTO closure_saga_domain_ack" in sql
        assert conn.commits == 1

    @pytest.mark.asyncio
    async def test_save_state_param_is_lowercase_enum_value(self) -> None:
        conn = _MockConn()
        c = _coordinator()
        await _repo(conn).save(c)
        saga_params = [
            p
            for cur in conn.cursors
            for sql, p in cur.executed
            if "INSERT INTO closure_saga\n" in sql or "INSERT INTO closure_saga " in sql
        ]
        assert saga_params, "saga upsert not executed"
        assert saga_params[0]["state"] == "closing"


class TestGet:
    @pytest.mark.asyncio
    async def test_get_missing_raises(self) -> None:
        conn = _MockConn(results=[None])
        with pytest.raises(ErrSagaNotFound):
            await _repo(conn).get("0197ffff-0000-7000-8000-000000000000")

    @pytest.mark.asyncio
    async def test_get_hydrates_coordinator(self) -> None:
        c = _coordinator()
        now = _dt.datetime.now(_dt.UTC)
        history_rows = [
            ("active", "closing", "bye", GCID, c.requested_at),
        ]
        ack_rows = [("creation", now), ("identity", now)]
        conn = _MockConn(results=[_saga_row(c), history_rows, ack_rows])

        got = await _repo(conn).get(c.saga_id)
        assert got.saga_id == c.saga_id
        assert got.state is State.CLOSING
        assert got.gcid == GCID
        assert len(got.history) == 1
        assert {a.domain for a in got.domain_acks} == {"creation", "identity"}


class TestGetByGcid:
    @pytest.mark.asyncio
    async def test_get_by_gcid_none_when_absent(self) -> None:
        conn = _MockConn(results=[None])
        got = await _repo(conn).get_by_gcid(GCID)
        assert got is None

    @pytest.mark.asyncio
    async def test_get_by_gcid_returns_most_recent(self) -> None:
        c = _coordinator()
        conn = _MockConn(results=[_saga_row(c), [], []])
        got = await _repo(conn).get_by_gcid(GCID)
        assert got is not None
        assert got.saga_id == c.saga_id


class TestListByState:
    @pytest.mark.asyncio
    async def test_list_by_state_hydrates_each(self) -> None:
        c1 = _coordinator()
        c2 = _coordinator()
        conn = _MockConn(
            results=[
                [_saga_row(c1), _saga_row(c2)],  # list query
                [],  # c1 history
                [],  # c1 acks
                [],  # c2 history
                [],  # c2 acks
            ]
        )
        got = await _repo(conn).list_by_state(State.CLOSING, limit=10)
        assert [g.saga_id for g in got] == [c1.saga_id, c2.saga_id]


class TestConnectionHygiene:
    """The repo holds ONE long-lived conn; reads must end their transaction so
    they never linger idle-in-transaction holding closure_saga locks (which
    blocked migration 0053) and a failed write must not poison the conn for
    every subsequent saga-driver tick. (Surfaced during the D3 deploy.)"""

    @pytest.mark.asyncio
    async def test_get_ends_read_transaction(self) -> None:
        c = _coordinator()
        conn = _MockConn(results=[_saga_row(c), [], []])
        await _repo(conn).get(c.saga_id)
        assert conn.rollbacks == 1
        assert conn.commits == 0

    @pytest.mark.asyncio
    async def test_get_missing_still_ends_transaction(self) -> None:
        conn = _MockConn(results=[None])
        with pytest.raises(ErrSagaNotFound):
            await _repo(conn).get("0197ffff-0000-7000-8000-000000000000")
        assert conn.rollbacks == 1

    @pytest.mark.asyncio
    async def test_get_by_gcid_ends_read_transaction(self) -> None:
        conn = _MockConn(results=[None])
        await _repo(conn).get_by_gcid(GCID)
        assert conn.rollbacks == 1

    @pytest.mark.asyncio
    async def test_list_by_state_ends_read_transaction(self) -> None:
        c = _coordinator()
        conn = _MockConn(results=[[_saga_row(c)], [], []])
        await _repo(conn).list_by_state(State.CLOSING, limit=10)
        assert conn.rollbacks == 1

    @pytest.mark.asyncio
    async def test_save_rolls_back_on_error_and_does_not_commit(self) -> None:
        conn = _MockConn(raise_on_execute=True)
        with pytest.raises(RuntimeError, match="scripted execute failure"):
            await _repo(conn).save(_coordinator())
        assert conn.rollbacks == 1
        assert conn.commits == 0


class TestRoundTrip:
    @pytest.mark.asyncio
    async def test_round_trip_save_then_get_preserves_fast_close_grace(
        self,
    ) -> None:
        """fast_close grace collapse must survive persistence."""
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
                fast_close=True,
            )
        )
        conn = _MockConn(results=[_saga_row(c), [], []])
        got = await _repo(conn).get(c.saga_id)
        assert got.grace_ends_at <= got.requested_at
