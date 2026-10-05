"""A concurrent READ must not destroy an in-flight WRITE (G19).

THE DEFECT. ``PostgresCoordinatorRepository`` was built over ONE long-lived
non-autocommit connection (``wiring.py`` 226/232) and that one instance was
then handed to THREE independent concurrent users:

  1. ``SagaDriver``, its own asyncio task (``wiring.py`` 366, 373).
  2. ``DomainAckHandler`` (``wiring.py``), driven from the NATS subscriber
     THREAD via ``asyncio.run_coroutine_threadsafe`` onto the SAME event loop
     (``wiring.py`` 399), bound once PER ack subscription (``wiring.py`` 403).
     There are 10 domains, so up to 10 concurrent handler coroutines.
  3. The FastAPI request handlers, because ``app.state.adapters.repo`` IS
     ``runtime.repo`` (``handlers.py`` 354).

The three read methods end with ``finally: await self._conn.rollback()``. That
is a CONNECTION-level rollback. It is not scoped to a cursor, and psycopg has
no nested transaction here. ``save()`` meanwhile is a MULTI-statement
transaction (saga upsert, then N history rows, then M acks, then commit) with
an await between every statement.

So a reader that runs between two of the writer's statements rolls back the
WRITER's uncommitted work, and then:

  ⚠ THE WRITER'S ``commit()`` RETURNS SUCCESS ON AN EMPTY TRANSACTION.

That is what makes it silent. No exception is raised anywhere. The driver
believes the saga advanced; the database says it did not.

WHY THE DOCSTRING'S DEFENCE DOES NOT HOLD. ``postgres.py`` says "psycopg3
serialises concurrent task access to one connection". True of STATEMENTS, false
of TRANSACTIONS: psycopg3's lock is held for one operation, so the wire never
corrupts, but nothing scopes a transaction to a task. The rest of that sentence,
"the orchestrator's modest closure QPS makes a single connection adequate", is a
probability argument rather than a correctness one.

AND THE BURST IS DESIGNED IN. Fan-out publishes 10 pseudonymise requests at
once, so the 10 acks come back together and are handled concurrently, precisely
while the driver is ticking the same sagas. The system's busiest moment is the
moment interleaving is most likely.

THE SCENARIO MODELLED HERE IS A REAL PRODUCTION PATH, not a contrived one: an
ack arrives for a saga id the lookup misses (``DomainAckHandler`` catches
``ErrSagaNotFound`` and raises ``TransientError`` so the broker redelivers, exactly
because the ack CAN race the saga write). That miss still runs its
``finally: rollback()``, and that rollback is what eats the driver's write.

⚠ THE FIX IS A CONNECTION PER LOGICAL OPERATION (a pool), NOT A LOCK. A lock
makes the bug unreachable; a connection per operation makes it non-existent. The
pattern was already in this service: ``outbox_conn`` and ``dek_store_conn`` each
own a dedicated connection. ``repo_conn`` was the one that missed it.

⚠ DO NOT "SIMPLIFY" BY DELETING THE ROLLBACKS IN THE READ METHODS. They exist
for a different real bug: without them the read left the long-lived connection
idle-in-transaction holding ``closure_saga`` locks, which BLOCKED migration
0053 (``postgres.py`` 170-173). The fix for one bug created the mechanism for
the next. Once each operation owns its connection, those rollbacks are both
correct and harmless, which is the entire point.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.repository.postgres import (
    PostgresCoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import (
    ErrSagaNotFound,
    NewParams,
    new,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _label(sql: str) -> str:
    if "INSERT INTO closure_saga (" in sql:
        return "saga_upsert"
    if "closure_saga_history" in sql and sql.lstrip().startswith("INSERT"):
        return "history_insert"
    if "closure_saga_domain_ack" in sql and sql.lstrip().startswith("INSERT"):
        return "ack_insert"
    return "select"


class _Db:
    """The durable side. Only what a COMMIT has flushed lives here."""

    def __init__(self) -> None:
        self.committed: list[str] = []


class _Conn:
    """A connection with REAL transaction semantics, which is the whole point.

    A double that merely counts commits and rollbacks cannot express this
    defect: the damage is that a rollback DISCARDS another task's pending
    work and the subsequent commit still succeeds. So pending work is
    modelled explicitly and rollback throws it away.
    """

    def __init__(self, db: _Db, on_execute: Any = None) -> None:
        self._db = db
        self._on_execute = on_execute
        self.pending: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    async def commit(self) -> None:
        self.commits += 1
        self._db.committed.extend(self.pending)
        self.pending.clear()

    async def rollback(self) -> None:
        self.rollbacks += 1
        self.pending.clear()  # the destructive bit


class _Cursor:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        label = _label(sql)
        self._conn.pending.append(label)
        if self._conn._on_execute is not None:
            await self._conn._on_execute(label)

    async def fetchone(self) -> Any:
        return None  # saga not found: the racing-ack path

    async def fetchall(self) -> list[Any]:
        return []


class _SharedPool:
    """DEGENERATE pool: hands the SAME connection to every caller.

    This is the shared-connection topology expressed as a pool so both
    topologies can be driven through one repository API. It exists to prove the
    harness can still SEE the defect; if this ever stops losing the write, the
    isolation test below has gone vacuous.
    """

    def __init__(self, db: _Db, on_execute: Any = None) -> None:
        self.conn = _Conn(db, on_execute)

    @asynccontextmanager
    async def connection(self) -> Any:
        yield self.conn


class _IsolatingPool:
    """A pool that hands each caller its OWN connection over one database.

    ``on_execute`` is attached to the FIRST connection handed out, which the
    writer takes.
    """

    def __init__(self, db: _Db, on_execute: Any = None) -> None:
        self._db = db
        self._on_execute = on_execute
        self.conns: list[_Conn] = []

    @asynccontextmanager
    async def connection(self) -> Any:
        hook = self._on_execute if not self.conns else None
        conn = _Conn(self._db, hook)
        self.conns.append(conn)
        yield conn


def _coordinator() -> Any:
    return new(
        NewParams(
            gcid=GCID,
            tenant_id=TENANT,
            requested_by_gcid=GCID,
            reason="g19",
            grace_period_days=30,
        )
    )


async def _drive_the_interleaving(pool: Any) -> _Db:
    """Rendezvous the two tasks deterministically rather than by timing.

    The writer is paused immediately after its FIRST statement, so it is
    unambiguously mid-transaction. The reader then runs to completion,
    including its ``finally: rollback()``. Only then does the writer proceed
    to its ``commit()``.
    """
    repo = PostgresCoordinatorRepository(pool=pool)
    writer_is_mid_transaction = asyncio.Event()
    reader_is_done = asyncio.Event()

    async def on_execute(label: str) -> None:
        if label == "saga_upsert" and not writer_is_mid_transaction.is_set():
            writer_is_mid_transaction.set()
            await reader_is_done.wait()

    pool._on_execute = on_execute  # noqa: SLF001 - test double
    if isinstance(pool, _SharedPool):
        pool.conn._on_execute = on_execute  # noqa: SLF001 - test double

    async def writer() -> None:
        await repo.save(_coordinator())

    async def reader() -> None:
        await writer_is_mid_transaction.wait()
        with pytest.raises(ErrSagaNotFound):
            await repo.get("00000000-0000-7000-8000-00000000dead")
        reader_is_done.set()

    await asyncio.gather(writer(), reader())
    return pool


async def test_a_concurrent_read_does_not_destroy_an_in_flight_write() -> None:
    """THE REGRESSION. A reader's rollback must not eat a writer's transaction.

    Stated as the durable outcome rather than as "each caller got its own
    connection", because the connection topology is the fix and the surviving
    write is the requirement.
    """
    db = _Db()
    pool = _IsolatingPool(db)
    await _drive_the_interleaving(pool)

    assert "saga_upsert" in db.committed, (
        "the driver's saga write was LOST. A concurrent reader's "
        "finally: rollback() discarded the writer's uncommitted rows on the "
        "shared connection, and the writer's commit() then returned SUCCESS on "
        "an empty transaction. No exception is raised anywhere: the driver "
        "believes the saga advanced and the database says it did not."
    )


async def test_the_harness_can_still_see_the_defect() -> None:
    """POSITIVE CONTROL on the test itself, not on the code.

    Drives the identical interleaving over a DEGENERATE pool that hands both
    tasks the same connection, and requires the write to be LOST. If this ever
    passes silently, the isolation test above has stopped discriminating and is
    proving nothing.
    """
    db = _Db()
    pool = _SharedPool(db)
    await _drive_the_interleaving(pool)

    assert "saga_upsert" not in db.committed, (
        "the shared-connection topology no longer loses the write, so this "
        "harness can no longer detect the defect it exists to pin"
    )
    assert pool.conn.commits == 1, "the writer still committed, and still succeeded"
