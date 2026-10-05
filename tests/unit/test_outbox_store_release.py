"""``PostgresOutboxStore.fetch_pending`` must RELEASE its transaction.

The defect these pin, measured live on 2026-08-23: pid 365 held one transaction
open for over NINE HOURS with ``idle_for`` under half a second. That shape is
not a stuck backend, it is a live polling loop re-running the drain inside a
transaction it never closes.

Mechanism: the dispatch connection is NOT autocommit (``wiring.py`` connects it
without ``autocommit=True``, unlike the ``_connect_autocommit`` helper it uses
elsewhere), so the first ``execute`` opens a transaction implicitly.
``mark_published`` / ``mark_failed`` / ``deadletter`` all commit; ``fetch_pending``
did not. When the fetch comes back EMPTY the dispatcher's ``for row in rows:``
body never runs, so none of those three is called and nothing commits. In a
quiet estate the empty path is the NORMAL path, which is why it never self-heals.

Cost of leaving it open: the snapshot pins the oldest-xmin horizon so VACUUM
cannot reclaim dead tuples anywhere in the database, and it blocks DDL needing a
lock on ``closure_outbox_events`` (migrations ran during W4 with it open).

⚠ RELEASE WITH COMMIT, NEVER ROLLBACK. On a shared connection a rollback
destroys co-tenant writes (the 2026-08-14 incident). On the empty path this is
free and unambiguous: zero rows came back, so zero row locks are held, so a
commit cannot release anything early.

⚠ SCOPE. These tests deliberately pin that the NON-empty path is UNCHANGED.
Committing there would drop the ``FOR UPDATE`` locks and narrow the batch
semantics, which is a design change and not part of a transaction-leak fix.
"""

from __future__ import annotations

from typing import Any

import pytest

from chora_closure_orchestrator.adapter.pubsub.store import PostgresOutboxStore


class _TxCursor:
    """A cursor that models the ONE thing under test: executing a statement on a
    non-autocommit connection opens a transaction."""

    def __init__(self, conn: _TxConn, rows: list[tuple[Any, ...]]) -> None:
        self._conn = conn
        self._rows = rows

    async def __aenter__(self) -> _TxCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((sql, params))
        self._conn.in_transaction = True

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _TxConn:
    """Tracks transaction state rather than just counting commits.

    Counting commits is the weaker assertion: it would pass on a store that
    committed somewhere irrelevant. What the defect is ABOUT is whether a
    transaction is still open when fetch_pending returns, so that is what this
    models.
    """

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, Any]] = []
        self.in_transaction = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _TxCursor:
        return _TxCursor(self, self.rows)

    async def commit(self) -> None:
        self.commits += 1
        self.in_transaction = False

    async def rollback(self) -> None:
        self.rollbacks += 1
        self.in_transaction = False


def _row(row_id: str = "r1") -> tuple[Any, ...]:
    """One row shaped as fetch_pending unpacks it (11 columns, envelope is TEXT)."""
    import datetime as _dt

    return (
        row_id,
        "acf424ad-12b7-4dab-8fc5-f0d4c59c8979",
        "11111111-1111-7111-8111-111111111111",
        "00000000-0000-7000-8000-000000001999",
        "closure.requested",
        "chora.identity.account.lifecycle_changed.v1",
        b"\x08\x01",
        '{"event_id": "e1"}',
        "idem-1",
        0,
        _dt.datetime(2026, 5, 12, 8, 0, 0, tzinfo=_dt.UTC),
    )


@pytest.mark.asyncio
async def test_an_empty_fetch_leaves_no_transaction_open() -> None:
    """THE REGRESSION. Not "the empty path runs" but "the transaction is closed
    when it returns", which is the thing that was actually broken."""
    conn = _TxConn(rows=[])
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    rows = await store.fetch_pending(limit=10)

    assert rows == []
    assert conn.in_transaction is False, (
        "fetch_pending returned with a transaction still open. On a quiet estate "
        "this is the normal path, so the transaction stays open forever, pinning "
        "the oldest-xmin horizon and blocking DDL on closure_outbox_events."
    )


@pytest.mark.asyncio
async def test_the_empty_path_releases_with_commit_and_never_rollback() -> None:
    """A rollback on a SHARED connection destroys co-tenant writes (2026-08-14).
    Pinned separately so a future 'simplification' to rollback fails loudly."""
    conn = _TxConn(rows=[])
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    await store.fetch_pending(limit=10)

    assert conn.commits == 1, "the empty path must release with exactly one COMMIT"
    assert conn.rollbacks == 0, "NEVER rollback here: on a shared connection it discards co-tenant writes"


@pytest.mark.asyncio
async def test_a_non_empty_fetch_is_deliberately_unchanged() -> None:
    """SCOPE CONTROL. Committing on the non-empty path would drop the FOR UPDATE
    locks the batch is holding and narrow SKIP LOCKED's worker safety. That is a
    design change, tracked separately, and must NOT ride a leak fix. If this test
    starts failing, someone widened the fix past its remit."""
    conn = _TxConn(rows=[_row()])
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    rows = await store.fetch_pending(limit=10)

    assert len(rows) == 1
    assert conn.commits == 0, (
        "fetch_pending must not commit when it returned rows: the caller's "
        "mark_published / mark_failed own that transaction's end"
    )
    assert conn.rollbacks == 0
