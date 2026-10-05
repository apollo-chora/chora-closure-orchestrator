"""A failed publish must stay eligible until max_attempts, then dead-letter.

G12 mechanism + G15 coverage. ``mark_published``, ``mark_failed`` and
``deadletter`` were the three untested methods, and they are exactly the paths
that produced 169 permanently stranded rows in production.

THE DEFECT, and it is provable rather than merely observed:

  * ``fetch_pending`` selects ``WHERE status = 'pending'`` and nothing else.
  * ``drain_once`` computes ``attempt = row.retry_count + 1`` and dead-letters
    only when ``attempt >= max_attempts`` (default 5).
  * ``mark_failed`` set ``status='failed'``, moving the row OUT of the only
    status the poll can see.
  * NOTHING anywhere writes a row back to ``'pending'``.

So ``retry_count`` is confined to {0, 1}, ``attempt`` to {1, 2}, and
``attempt >= 5`` can never be true. The dead-letter branch was UNREACHABLE and
every publish failure stranded permanently, with no dead-letter row and no
alert. Measured live before the fix: 169 failed, 0 dead letters, and
``retry_count`` uniformly 1 across all 169 rows, 5 topics, 6 sagas and 8 days.
A working retry loop would show a spread of 1..4; a perfectly uniform 1 is the
frozen counter itself.

THE FIX KEEPS THE ROW IN ``'pending'`` rather than widening the SELECT, and
that choice is load-bearing for a reason that is not about elegance:

  ⚠ THE 169 EXISTING ROWS ARE A COMPLIANCE DECISION THAT BELONGS TO THE OWNER.

Widening ``fetch_pending`` to ``status IN ('pending','failed')`` would make all
169 eligible on the very next poll and republish two-month-old compliance
events into topics with 10, 3 and 3 live subscribers. Those rows all failed
with ``INVALID_BINARY_PROTO_MESSAGE``, and ``protomarshal.encode`` has since
been added, so on a retry today they would likely SUCCEED rather than re-fail.
Releasing them is a live publish, not a no-op.

By leaving a failed ATTEMPT in ``'pending'`` and never reading, writing or
selecting ``'failed'`` again, the historical rows are inert BY CONSTRUCTION:
not by a feature flag somebody must remember to leave off, and not by a NULL
check on a column added for the purpose. ``test_the_historical_residue_is_inert``
pins that, and it is the test to run if anyone proposes widening the SELECT.

WHY THE FAKE READS THE SQL. The defect lives in the SQL predicate, not in
Python control flow, so a double that hard-codes "the select returns pending
rows" would be testing the double rather than the store. ``_FakeDb`` instead
PARSES the statement it is given (the status literal in the WHERE, the SET
clauses in the UPDATEs) and applies it to in-memory rows. Change the predicate
in ``store.py`` and these tests change behaviour, which is the property that
makes them worth having.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.pubsub.dispatcher import OutboxDispatcher
from chora_closure_orchestrator.adapter.pubsub.store import PostgresOutboxStore

_SET_STATUS = re.compile(r"SET\s+status\s*=\s*'([a-z_]+)'", re.I)
_WHERE_STATUS = re.compile(r"WHERE\s+status\s*=\s*'([a-z_]+)'", re.I)
_BACKOFF_SECS = re.compile(r"backoff_seconds", re.I)


class _FakeDb:
    """In-memory closure_outbox_events + closure_outbox_dead_letters.

    Owns a controllable ``now`` so the backoff can be tested without sleeping.
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.dead_letters: list[dict[str, Any]] = []
        self.now = _dt.datetime(2026, 8, 23, 12, 0, 0, tzinfo=_dt.UTC)

    def add(
        self, row_id: str, *, status: str = "pending", retry_count: int = 0, last_attempt_at: _dt.datetime | None = None
    ) -> None:
        self.rows.append(
            {
                "id": row_id,
                "saga_id": "acf424ad-12b7-4dab-8fc5-f0d4c59c8979",
                "tenant_id": "11111111-1111-7111-8111-111111111111",
                "gcid": "00000000-0000-7000-8000-000000001999",
                "event_type": "closure.requested",
                "topic": "chora.closure.requested.v1",
                "payload": b"\x08\x01",
                "envelope": '{"event_id": "e1"}',
                "idempotency_key": f"idem-{row_id}",
                "status": status,
                "retry_count": retry_count,
                "last_attempt_at": last_attempt_at,
                "occurred_at": _dt.datetime(2026, 8, 23, 8, 0, 0, tzinfo=_dt.UTC),
            }
        )

    def by_id(self, row_id: str) -> dict[str, Any]:
        return next(r for r in self.rows if r["id"] == row_id)

    def advance(self, seconds: int) -> None:
        self.now += _dt.timedelta(seconds=seconds)


class _FakeCursor:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn
        self._result: list[tuple[Any, ...]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        params = params or {}
        db = self._conn.db
        if "closure_outbox_dead_letters" in sql and "INSERT" in sql.upper():
            db.dead_letters.append(dict(params))
            return
        if sql.lstrip().upper().startswith("UPDATE"):
            row = db.by_id(params["id"])
            m = _SET_STATUS.search(sql)
            if m:
                row["status"] = m.group(1)
            if "retry_count = retry_count + 1" in sql:
                row["retry_count"] += 1
            if "last_attempt_at = now()" in sql:
                row["last_attempt_at"] = db.now
            if "published_at=now()" in sql or "published_at = now()" in sql:
                row["published_at"] = db.now
            return
        # The poll. Honour the predicate the store actually wrote.
        wanted = _WHERE_STATUS.search(sql)
        status = wanted.group(1) if wanted else "pending"
        eligible = [r for r in db.rows if r["status"] == status]
        if _BACKOFF_SECS.search(sql):
            secs = int(params.get("backoff_seconds", 0))
            cutoff = db.now - _dt.timedelta(seconds=secs)
            eligible = [r for r in eligible if r["last_attempt_at"] is None or r["last_attempt_at"] <= cutoff]
        eligible.sort(key=lambda r: r["occurred_at"])
        limit = int(params.get("limit", 100))
        self._result = [
            (
                r["id"],
                r["saga_id"],
                r["tenant_id"],
                r["gcid"],
                r["event_type"],
                r["topic"],
                r["payload"],
                r["envelope"],
                r["idempotency_key"],
                r["retry_count"],
                r["occurred_at"],
            )
            for r in eligible[:limit]
        ]

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._result


class _FakeConn:
    def __init__(self, db: _FakeDb) -> None:
        self.db = db
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _AlwaysFailsPublisher:
    def __init__(self) -> None:
        self.attempts = 0

    async def publish(self, row: Any) -> str:
        self.attempts += 1
        raise RuntimeError(
            '400 Invalid data in message: Message failed schema validation. [reason: "INVALID_BINARY_PROTO_MESSAGE"]'
        )


class _AlwaysPublishes:
    async def publish(self, row: Any) -> str:
        return "msg-1"


def _store(db: _FakeDb) -> tuple[PostgresOutboxStore, _FakeConn]:
    conn = _FakeConn(db)
    return PostgresOutboxStore(conn=conn, worker_id="w1"), conn


# ---------------------------------------------------------------------------
# The mechanism
# ---------------------------------------------------------------------------


async def test_exhausting_max_attempts_actually_dead_letters() -> None:
    """THE REGRESSION, stated as the outcome the estate needs.

    Not "mark_failed keeps the status" but "a row that never publishes ends up
    in closure_outbox_dead_letters", because the production symptom was 169
    stranded rows and ZERO dead letters.
    """
    db = _FakeDb()
    db.add("r1")
    store, _ = _store(db)
    publisher = _AlwaysFailsPublisher()
    dispatcher = OutboxDispatcher(store=store, publisher=publisher, worker_id="w1")

    for _ in range(10):  # more polls than attempts; the cap must stop it
        await dispatcher.drain_once()
        db.advance(3600)  # past any backoff

    assert publisher.attempts == 5, (
        f"expected exactly max_attempts publish attempts, got "
        f"{publisher.attempts}. 1 means the row left 'pending' after its first "
        "failure and was never re-fetched, which is the frozen-counter defect."
    )
    assert len(db.dead_letters) == 1, (
        "the row exhausted its attempts and did NOT land in "
        "closure_outbox_dead_letters. This is the unreachable-branch defect: "
        "production showed 169 stranded rows and 0 dead letters."
    )
    assert db.by_id("r1")["status"] == "deadlettered"


async def test_a_failed_attempt_keeps_the_row_eligible() -> None:
    """The mechanism itself: one failure must not evict the row from the poll."""
    db = _FakeDb()
    db.add("r1")
    store, _ = _store(db)
    await store.mark_failed("r1", "boom")

    db.advance(3600)
    assert [r.id for r in await store.fetch_pending(limit=10)] == ["r1"], (
        "after ONE failure the row is no longer returned by fetch_pending, so "
        "retry_count freezes at 1 and attempt can never reach max_attempts"
    )
    assert db.by_id("r1")["retry_count"] == 1
    assert db.by_id("r1")["last_attempt_at"] is not None


async def test_a_just_failed_row_is_not_refetched_on_the_next_poll() -> None:
    """Invariant 3: no busy-loop. The poll runs every 0.5s by default."""
    db = _FakeDb()
    db.add("r1")
    store, _ = _store(db)
    await store.mark_failed("r1", "boom")

    assert await store.fetch_pending(limit=10) == [], (
        "the row was re-fetched immediately after failing, which spins the "
        "dispatcher against a failing publish at the poll interval"
    )
    db.advance(3600)
    assert len(await store.fetch_pending(limit=10)) == 1, (
        "positive control: after the backoff elapses the row MUST come back, "
        "otherwise this test would pass on a store that never retries at all"
    )


async def test_the_historical_residue_is_inert() -> None:
    """⚠ THE COMPLIANCE GUARD. Run this before widening the SELECT.

    The 169 production rows sit in 'failed'. They are a compliance decision for
    the owner, and making them eligible republishes two-month-old events into
    topics with live subscribers. Inertness must not depend on a flag.
    """
    db = _FakeDb()
    for i in range(169):
        db.add(f"old{i}", status="failed", retry_count=1, last_attempt_at=_dt.datetime(2026, 6, 15, tzinfo=_dt.UTC))
    store, _ = _store(db)
    db.advance(86400 * 60)

    assert await store.fetch_pending(limit=500) == [], (
        "fetch_pending returned historical 'failed' rows. Those are two-month-"
        "old compliance events and the owner has not ruled on releasing them."
    )
    db.add("fresh")
    assert [r.id for r in await store.fetch_pending(limit=500)] == ["fresh"], (
        "positive control: the query CAN return a row, so the empty result "
        "above is inertness rather than a broken predicate"
    )


# ---------------------------------------------------------------------------
# G15: the three untested methods
# ---------------------------------------------------------------------------


async def test_mark_published_is_terminal() -> None:
    db = _FakeDb()
    db.add("r1")
    store, conn = _store(db)
    await store.mark_published("r1")

    assert db.by_id("r1")["status"] == "published"
    assert db.by_id("r1")["published_at"] is not None
    assert conn.commits == 1
    assert await store.fetch_pending(limit=10) == [], "published must not re-poll"


async def test_deadletter_records_the_reason_and_is_terminal() -> None:
    db = _FakeDb()
    db.add("r1", retry_count=4)
    store, conn = _store(db)
    await store.deadletter("r1", failure_reason="schema rejected", attempt_count=5)

    assert db.by_id("r1")["status"] == "deadlettered"
    assert len(db.dead_letters) == 1
    dl = db.dead_letters[0]
    assert dl["reason"] == "schema rejected"
    assert dl["count"] == 5
    assert dl["worker"] == "w1"
    assert conn.commits == 1, "the dead-letter row and the status flip are ONE tx"
    assert await store.fetch_pending(limit=10) == [], "deadlettered must not re-poll"


async def test_a_successful_publish_never_retries() -> None:
    """Scope control: the retry path must not touch the happy path."""
    db = _FakeDb()
    db.add("r1")
    store, _ = _store(db)
    dispatcher = OutboxDispatcher(store=store, publisher=_AlwaysPublishes(), worker_id="w1")

    assert await dispatcher.drain_once() == 1
    assert db.by_id("r1")["status"] == "published"
    assert db.by_id("r1")["retry_count"] == 0
    assert db.dead_letters == []


@pytest.mark.parametrize("max_attempts", [1, 3, 5])
async def test_the_attempt_cap_is_honoured_exactly(max_attempts: int) -> None:
    """The cap is the contract with the subscription's own policy."""
    db = _FakeDb()
    db.add("r1")
    store, _ = _store(db)
    publisher = _AlwaysFailsPublisher()
    dispatcher = OutboxDispatcher(store=store, publisher=publisher, worker_id="w1", max_attempts=max_attempts)

    for _ in range(max_attempts + 5):
        await dispatcher.drain_once()
        db.advance(3600)

    assert publisher.attempts == max_attempts
    assert len(db.dead_letters) == 1


@pytest.mark.parametrize(
    ("kwargs", "why"),
    [
        ({"worker_id": ""}, "a blank worker_id makes every dead-letter row anonymous"),
        (
            {"worker_id": "w1", "retry_backoff_seconds": -1},
            "a negative backoff makes the SQL interval nonsense and the row eligible forever",
        ),
    ],
    ids=["blank_worker_id", "negative_backoff"],
)
def test_the_constructor_refuses_nonsense(kwargs: Any, why: str) -> None:
    """Fail loud at construction rather than at 3am in the drain loop."""
    with pytest.raises(ValueError):
        PostgresOutboxStore(conn=_FakeConn(_FakeDb()), **kwargs)
