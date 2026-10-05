"""Reconnect-on-closed wrapper for idle-prone **autocommit** psycopg
connections.

The outbox publisher (``adapter.events.publisher_outbox``) and the wrapped-DEK
store (``adapter.kms.store``) each hold a dedicated autocommit
``psycopg.AsyncConnection`` that is touched ONLY when a closure event is emitted
(fan-out / archive / shred). Between closures — hours or days — the database
drops the idle connection, and the next write previously failed hard with
``OperationalError: the connection is closed`` (real incident 2026-06-20:
the first ``POST /v1/closure/request`` after a ~3h47m-idle pod 500'd).

``ReconnectingConnection`` wraps such a connection and transparently reopens it
when the server has closed it. It is a drop-in for the ``conn`` those two
adapters hold: it exposes ``.cursor()`` (async context manager), ``.close()``
and ``.closed`` — the only surface those adapters use.

**Autocommit-only by design.** Each statement is standalone, so reopening the
connection can never lose an in-flight transaction. The warm, *transactional*
connections (coordinator repository + outbox dispatcher) are intentionally NOT
wrapped — the 30s saga-driver tick and 0.5s dispatcher poll keep them alive, and
a mid-transaction reconnect would silently drop a partial transaction.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

# psycopg surfaces a server-dropped / broken connection as one of these.
_CONN_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

ConnectFn = Callable[[str], Awaitable[Any]]


def _is_dead(conn: Any) -> bool:
    """True when there is no usable underlying connection.

    psycopg3 ``AsyncConnection.closed`` is an int (0 = open) and ``.broken`` is a
    bool set when an error left the connection unusable. Checking BOTH covers the
    silent-death case — a lost socket (failover / maintenance / network
    blip) can leave ``.closed`` 0 but ``.broken`` true, so a ``.closed``-only
    pre-check would hand out a cursor on a dead conn and 500 the (sole) saga
    driver's next write. None = never connected. Mirrors the proven
    chora-ai-kernel-orchestrator ReconnectingAsyncConnection pattern.
    """
    if conn is None:
        return True
    return bool(getattr(conn, "closed", False)) or bool(getattr(conn, "broken", False))


class _ReconnectingCursorCtx:
    """Async context manager that opens its cursor on a guaranteed-live
    connection (reconnecting first if the held connection is dead)."""

    def __init__(self, owner: ReconnectingConnection) -> None:
        self._owner = owner
        self._cm: Any = None

    async def __aenter__(self) -> Any:
        self._cm = await self._owner._open_cursor()
        return await self._cm.__aenter__()

    async def __aexit__(self, *exc: Any) -> Any:
        if self._cm is None:
            return False
        return await self._cm.__aexit__(*exc)


class ReconnectingConnection:
    """Autocommit psycopg ``AsyncConnection`` that reconnects on idle-close.

    :param dsn: connection string passed to ``connect`` on each reopen.
    :param conn: the initial, already-open autocommit connection (created at
        startup so the service still fails loudly if the DB is unreachable at
        boot).
    :param connect: async factory ``connect(dsn) -> AsyncConnection`` returning
        a fresh autocommit connection; injectable for tests.
    """

    def __init__(self, *, dsn: str, conn: Any, connect: ConnectFn) -> None:
        self._dsn = dsn
        self._conn = conn
        self._connect = connect
        self._lock = asyncio.Lock()

    @property
    def closed(self) -> bool:
        return _is_dead(self._conn)

    def cursor(self) -> _ReconnectingCursorCtx:
        return _ReconnectingCursorCtx(self)

    async def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            await self._conn.close()

    async def _open_cursor(self) -> Any:
        """Return a cursor context manager on a live connection, reopening the
        connection at most once if it is (or has just been detected) closed."""
        async with self._lock:
            if _is_dead(self._conn):
                self._conn = await self._reconnect()
            try:
                return self._conn.cursor()
            except _CONN_ERRORS:
                # ``.closed`` read False but psycopg only learned of the drop
                # when it ran _check_connection_ok() inside .cursor().
                self._conn = await self._reconnect()
                return self._conn.cursor()

    async def _reconnect(self) -> Any:
        logger.warning(
            "closure_db_reconnect",
            extra={"reason": "idle_connection_closed"},
        )
        return await self._connect(self._dsn)


__all__ = ["ReconnectingConnection"]
