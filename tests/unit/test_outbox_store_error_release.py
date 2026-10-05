"""``PostgresOutboxStore`` must ROLL BACK when a statement raises.

Sibling of ``test_outbox_store_release.py``. That file pinned the half of the
discipline that DID get fixed: releasing the transaction the SELECT opened when
``fetch_pending`` comes back empty. This file pins the half that never
propagated: releasing it when a statement RAISES.

The census that found it: across the whole service there are exactly two
long-lived non-autocommit connections, ``repo_conn`` and ``dispatch_conn``
(``wiring.py`` 226 and 331; everything else goes through
``_connect_autocommit``). Every commit and rollback call site in the service
sits in two files. ``postgres.py`` has 1 commit and 4 rollbacks and guards every
path. ``store.py`` has 4 commits and ZERO rollbacks.

MECHANISM, and it is a silent permanent outage rather than a lost message. Each
store method is ``async with cursor: execute(...)`` then ``commit()``, with no
``try``. If an execute raises (deadlock, an admin killing the connection, a
network blip, a constraint) the exception propagates with the transaction left
ABORTED, and psycopg then fails EVERY subsequent statement on that connection
with ``InFailedSqlTransaction`` until someone ends it. Nothing does:

  * ``drain_once``'s ``except`` wraps ONLY ``self._publisher.publish``
    (``dispatcher.py`` 64-65), not the ``mark_published`` / ``mark_failed`` /
    ``deadletter`` calls at 79, 94 and 105, so a raise there escapes it.
  * ``_dispatch_loop`` (``wiring.py`` 347) catches it, logs
    ``outbox_dispatch_loop_error`` at WARNING, sleeps and retries.
  * The next tick's ``fetch_pending`` raises the same aborted-transaction error.
    Forever. The pod stays READY and the closure outbox stops dispatching.

The knowledge already existed IN THE NEIGHBOURING FILE. ``postgres.py`` 130-134
spells out this exact hazard for the sibling connection, naming
``InFailedSqlTransaction`` and "until the pod is bounced". It was written down
and it did not travel.

⚠ WHY A ROLLBACK HERE DOES NOT CONTRADICT THE "NEVER ROLLBACK" RULE NEXT DOOR.
``test_outbox_store_release.py`` says release the EMPTY SUCCESS path with COMMIT
and never ROLLBACK, because on a shared connection a rollback discards
co-tenant writes. That rule is about the SUCCESS path. On the ERROR path
PostgreSQL has already discarded everything in the transaction, so a rollback
throws away nothing that survived, and it is the only thing that clears the
aborted state. Success path commits, error path rolls back. Both are required.

⚠ WHAT THESE TESTS ASSERT, and it is deliberately not "rollback was called".
Counting rollbacks would pass on a store that rolled back somewhere useless.
What the defect is ABOUT is whether the connection is still USABLE afterwards,
so every test drives a REAL SUBSEQUENT OPERATION and requires it to succeed.
"""

from __future__ import annotations

from typing import Any

import pytest

from chora_closure_orchestrator.adapter.pubsub.store import PostgresOutboxStore


class _BoomError(Exception):
    """The original failure: a deadlock, a killed connection, a constraint."""


class _InFailedSqlTransactionError(Exception):
    """Stands in for ``psycopg.errors.InFailedSqlTransaction``.

    (The Error suffix is ruff N818; psycopg's own class carries no suffix.)

    Modelling this is the whole point. A fake connection that happily accepts
    the next statement after a failed one cannot express the defect, and a test
    written against such a fake would pass on the broken store.
    """


class _AbortingCursor:
    def __init__(self, conn: _AbortingConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _AbortingCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        if self._conn.aborted:
            raise _InFailedSqlTransactionError(
                "current transaction is aborted, commands ignored until end of transaction block"
            )
        self._conn.in_transaction = True
        if self._conn.fail_on and self._conn.fail_on in sql:
            self._conn.fail_on = None  # one-shot, like a real transient fault
            self._conn.aborted = True
            raise _BoomError("statement failed")
        self._conn.executed.append((sql, params))

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._conn.rows


class _AbortingConn:
    """A connection that goes STICKY-ABORTED after a failed statement."""

    def __init__(
        self,
        *,
        fail_on: str | None = None,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self.fail_on = fail_on
        self.rows = rows or []
        self.executed: list[tuple[str, Any]] = []
        self.in_transaction = False
        self.aborted = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _AbortingCursor:
        return _AbortingCursor(self)

    async def commit(self) -> None:
        # Faithful to PostgreSQL: COMMIT on an aborted transaction performs a
        # rollback and ends it. The store never reaches this on the failure
        # path (the raise happens first), which is exactly the defect.
        self.commits += 1
        self.aborted = False
        self.in_transaction = False

    async def rollback(self) -> None:
        self.rollbacks += 1
        self.aborted = False
        self.in_transaction = False


# (label, fail_on fragment, coroutine factory) for each write path.
_FAILING_CALLS = [
    (
        "fetch_pending",
        "FOR UPDATE SKIP LOCKED",
        lambda s: s.fetch_pending(limit=10),
    ),
    ("mark_published", "status='published'", lambda s: s.mark_published("r1")),
    # G12 changed this SQL: mark_failed no longer sets status='failed' (that is
    # what stranded 169 rows). Anchor on the counter bump, which is the one
    # statement this method will always issue.
    (
        "mark_failed",
        "retry_count = retry_count + 1",
        lambda s: s.mark_failed("r1", "boom"),
    ),
    (
        "deadletter_insert",
        "closure_outbox_dead_letters",
        lambda s: s.deadletter("r1", failure_reason="x", attempt_count=5),
    ),
    (
        "deadletter_update",
        "status='deadlettered'",
        lambda s: s.deadletter("r1", failure_reason="x", attempt_count=5),
    ),
]


@pytest.mark.parametrize(
    ("label", "fail_on", "call"),
    _FAILING_CALLS,
    ids=[c[0] for c in _FAILING_CALLS],
)
async def test_a_raising_statement_leaves_the_connection_usable(label: str, fail_on: str, call: Any) -> None:
    """THE REGRESSION, stated as the outage rather than as the missing call.

    Not "rollback was invoked" but "the NEXT tick still works", because the
    damage is that every later statement on this connection fails forever while
    the pod stays READY.
    """
    conn = _AbortingConn(fail_on=fail_on)
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    with pytest.raises(_BoomError):
        await call(store)

    # The next dispatcher tick. This is the assertion; everything else is
    # corroboration.
    try:
        rows = await store.fetch_pending(limit=10)
    except _InFailedSqlTransactionError as exc:  # pragma: no cover - the RED path
        pytest.fail(
            f"{label} left the transaction ABORTED: the next fetch_pending "
            f"raised {exc!r}. Every subsequent statement on dispatch_conn now "
            "fails the same way, _dispatch_loop logs it at WARNING and retries "
            "forever, and the pod stays READY while the closure outbox has "
            "silently stopped dispatching."
        )
    assert rows == []
    assert conn.aborted is False
    assert conn.in_transaction is False


@pytest.mark.parametrize(
    ("label", "fail_on", "call"),
    _FAILING_CALLS,
    ids=[c[0] for c in _FAILING_CALLS],
)
async def test_the_original_error_is_re_raised_not_swallowed(label: str, fail_on: str, call: Any) -> None:
    """Fail-loud. The recovery must not turn a real fault into a quiet success,
    and the caller must see the ORIGINAL exception rather than a rollback
    artefact, or the dispatcher's log line names the wrong cause."""
    conn = _AbortingConn(fail_on=fail_on)
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    with pytest.raises(_BoomError):
        await call(store)

    assert conn.rollbacks == 1, "exactly one rollback, on the error path only"


async def test_deadletter_rolls_back_its_partial_insert() -> None:
    """The two-statement path is the one that can leave a HALF-WRITTEN row.

    ``deadletter`` inserts into closure_outbox_dead_letters and then updates
    closure_outbox_events. If the UPDATE fails, the INSERT must not survive:
    a dead-letter record whose source row still reads 'pending' would be
    re-dispatched forever against a row already recorded as dead.
    """
    conn = _AbortingConn(fail_on="status='deadlettered'")
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    with pytest.raises(_BoomError):
        await store.deadletter("r1", failure_reason="x", attempt_count=5)

    assert conn.rollbacks == 1
    assert conn.commits == 0, "a failed deadletter must NEVER commit"
    assert conn.in_transaction is False


async def test_a_successful_call_does_not_roll_back() -> None:
    """SCOPE CONTROL. The rollback belongs to the error path ONLY.

    If this fails, someone widened the fix into the success path, where a
    rollback on a shared connection discards co-tenant writes (2026-08-14) and
    where the empty-path release must stay a COMMIT.
    """
    conn = _AbortingConn(rows=[])
    store = PostgresOutboxStore(conn=conn, worker_id="w1")

    assert await store.fetch_pending(limit=10) == []
    await store.mark_published("r1")
    await store.mark_failed("r1", "transient")
    await store.deadletter("r1", failure_reason="x", attempt_count=5)

    assert conn.rollbacks == 0, "no rollback on ANY success path"
    assert conn.commits == 4, (
        "every success path releases with exactly one COMMIT: the empty fetch, "
        "mark_published, mark_failed and deadletter"
    )
    assert conn.in_transaction is False
