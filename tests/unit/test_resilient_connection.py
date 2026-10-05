"""Unit tests for ``ReconnectingConnection`` — reconnect-on-closed wrapper.

The closure orchestrator's outbox publisher + wrapped-DEK store each hold a
long-lived **autocommit** psycopg ``AsyncConnection`` that is only touched when
a closure event is emitted (fan-out / archive / shred). Between closures —
which can be hours or days apart — the server drops the idle connection, and
the next write failed with ``OperationalError: the connection is closed`` (a
real incident, 2026-06-20: the first ``POST /v1/closure/request`` after a
~3h47m-idle pod 500'd because the publisher connection had died with no
reconnect). These tests pin the wrapper that transparently reconnects.

The wrapper is exercised with fakes (an injected ``connect`` factory) so the
logic is fully unit-covered without a live Postgres. Autocommit-only by design:
each statement stands alone, so a reconnect can never drop an in-flight
transaction (the warm, transactional coordinator-repo + dispatcher connections
are out of scope — they are kept alive by the 30s driver tick / 0.5s poll).
"""

from __future__ import annotations

from typing import Any

import psycopg

from chora_closure_orchestrator.adapter.db.resilient import (
    ReconnectingConnection,
)


class _FakeCursorCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeCursorCtx:
        return self

    async def __aexit__(self, *a: Any) -> bool:
        return False

    async def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((sql, params))

    async def fetchone(self) -> Any:
        return self._conn.next_row


class _FakeConn:
    """Duck-types the slice of psycopg.AsyncConnection the wrapper touches."""

    def __init__(
        self,
        *,
        closed: bool = False,
        broken: bool = False,
        raise_on_cursor: bool = False,
        next_row: Any = None,
    ) -> None:
        self.closed = closed
        # Silent-death: a lost socket leaves .closed 0 but .broken True. Such a
        # conn does NOT raise at .cursor() (it would raise later at execute) —
        # the wrapper must reconnect on the .broken pre-check, not reactively.
        self.broken = broken
        self.raise_on_cursor = raise_on_cursor
        self.next_row = next_row
        self.executed: list[tuple[str, Any]] = []
        self.close_calls = 0

    def cursor(self) -> _FakeCursorCtx:
        # psycopg's .cursor() calls _check_connection_ok() and raises
        # OperationalError("the connection is closed") on a CLEANLY-closed conn.
        if self.closed or self.raise_on_cursor:
            raise psycopg.OperationalError("the connection is closed")
        return _FakeCursorCtx(self)

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _Factory:
    """Injected async connect() that hands out pre-made connections in order."""

    def __init__(self, *conns: _FakeConn) -> None:
        self._conns = list(conns)
        self.calls = 0

    async def __call__(self, dsn: str) -> _FakeConn:
        self.calls += 1
        return self._conns.pop(0)


async def test_uses_existing_open_connection_no_reconnect() -> None:
    initial = _FakeConn()
    factory = _Factory()  # empty — a reconnect here would raise loudly
    rc = ReconnectingConnection(dsn="dsn", conn=initial, connect=factory)

    async with rc.cursor() as cur:
        await cur.execute("INSERT 1", {"a": 1})

    assert factory.calls == 0
    assert initial.executed == [("INSERT 1", {"a": 1})]


async def test_reconnects_when_underlying_closed() -> None:
    dead = _FakeConn(closed=True)
    fresh = _FakeConn()
    factory = _Factory(fresh)
    rc = ReconnectingConnection(dsn="dsn", conn=dead, connect=factory)

    async with rc.cursor() as cur:
        await cur.execute("INSERT 2", None)

    assert factory.calls == 1
    assert fresh.executed == [("INSERT 2", None)]
    assert dead.executed == []  # the dead conn was never used


async def test_retries_once_when_cursor_creation_raises() -> None:
    # closed flag still False, but .cursor() raises OperationalError (psycopg
    # only learned the conn was dead at use-time) — the wrapper must still
    # reconnect and retry exactly once.
    racy = _FakeConn(closed=False, raise_on_cursor=True)
    fresh = _FakeConn()
    factory = _Factory(fresh)
    rc = ReconnectingConnection(dsn="dsn", conn=racy, connect=factory)

    async with rc.cursor() as cur:
        await cur.execute("UPDATE x", None)

    assert factory.calls == 1
    assert fresh.executed == [("UPDATE x", None)]


async def test_reconnects_when_underlying_silently_broken() -> None:
    # Silent death: .closed is 0 but .broken is True (lost socket / failover).
    # .cursor() does NOT raise — the wrapper must reconnect on the .broken
    # pre-check, else it hands out a cursor on a dead conn that 500s at execute.
    broken = _FakeConn(closed=False, broken=True)
    fresh = _FakeConn()
    factory = _Factory(fresh)
    rc = ReconnectingConnection(dsn="dsn", conn=broken, connect=factory)

    async with rc.cursor() as cur:
        await cur.execute("INSERT 3", None)

    assert factory.calls == 1
    assert fresh.executed == [("INSERT 3", None)]
    assert broken.executed == []  # the broken conn was never used


async def test_fetchone_reads_through_a_reconnect() -> None:
    dead = _FakeConn(closed=True)
    fresh = _FakeConn(next_row=(b"w", "kv", None, "op"))
    factory = _Factory(fresh)
    rc = ReconnectingConnection(dsn="dsn", conn=dead, connect=factory)

    async with rc.cursor() as cur:
        await cur.execute("SELECT", {"g": 1})
        row = await cur.fetchone()

    assert row == (b"w", "kv", None, "op")


async def test_close_closes_underlying_and_is_idempotent() -> None:
    initial = _FakeConn()
    rc = ReconnectingConnection(dsn="dsn", conn=initial, connect=_Factory())

    await rc.close()
    assert initial.close_calls == 1
    await rc.close()  # already closed → no-op
    assert initial.close_calls == 1


async def test_closed_property_reflects_underlying() -> None:
    initial = _FakeConn()
    rc = ReconnectingConnection(dsn="dsn", conn=initial, connect=_Factory())
    assert rc.closed is False
    initial.closed = True
    assert rc.closed is True


async def test_closed_property_reflects_broken() -> None:
    initial = _FakeConn()
    rc = ReconnectingConnection(dsn="dsn", conn=initial, connect=_Factory())
    assert rc.closed is False
    initial.broken = True  # silent death — .closed still 0
    assert rc.closed is True
