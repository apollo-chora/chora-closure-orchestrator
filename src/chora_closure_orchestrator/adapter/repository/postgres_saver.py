"""AsyncPostgresSaver factory for the closure-saga LangGraph checkpointer.

Per ADR-145 D6 gate + ``feedback_d6_resilience_first_class`` memory: the
LangGraph checkpointer is first-class resilience infrastructure, not a
"checkpoint exists" demo. This factory wires
``langgraph-checkpoint-postgres`` against ``chora_ai_kernel`` (the
orchestrator's own database; see the 11-DB topology).

Per ``feedback_no_inline_config`` — connection string MUST come from env
(compose injects it).

Two callsites:

* **Production / Agent Engine deploy** — `open_postgres_saver_from_env()`
  reads ``CHORA_AI_KERNEL_CONNINFO`` and yields a context-managed
  ``AsyncPostgresSaver`` with the checkpoint schema migrated.

* **Tests** — pass an explicit conninfo via ``open_postgres_saver(...)`` to
  point at a local Postgres container or pin a CI service.

Resilience properties (verified by ``tests/integration/test_d6_checkpointer_resilience.py``):

1. State survives across runs with the same ``thread_id`` (D6.1 pod-death).
2. ``interrupt_before`` pauses cleanly, ``Command(resume=...)`` advances.
3. Concurrent thread_ids are isolated (D6.3 load).
4. Trace context propagates across resume (D6.4 — TODO; needs ``observability/tracing.py``
   to wrap put/get calls).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

CONNINFO_ENV = "CHORA_AI_KERNEL_CONNINFO"


def conninfo_from_env() -> str:
    """Read the chora_ai_kernel connection string from env.

    Expected: postgresql://USER:PASS@HOST:PORT/chora_ai_kernel?sslmode=require

    Raises RuntimeError if unset — no inline default per
    ``feedback_no_inline_config``.
    """
    cs = os.environ.get(CONNINFO_ENV)
    if not cs:
        raise RuntimeError(
            f"{CONNINFO_ENV} env var required; deployment must "
            f"inject the chora_ai_kernel connection string. "
            f"No inline default per feedback_no_inline_config."
        )
    return cs


@asynccontextmanager
async def open_postgres_saver(conninfo: str, *, pipeline: bool = False) -> AsyncIterator[AsyncPostgresSaver]:
    """Open AsyncPostgresSaver against the supplied conninfo.

    Calls ``setup()`` to migrate the checkpoint schema (idempotent).

    The langgraph-checkpoint-postgres ``from_conn_string`` context manager
    owns connection lifecycle. On exit, connections are released.
    """
    async with AsyncPostgresSaver.from_conn_string(conninfo, pipeline=pipeline) as saver:
        await saver.setup()
        yield saver


@asynccontextmanager
async def open_postgres_saver_from_env() -> AsyncIterator[AsyncPostgresSaver]:
    """Convenience wrapper — reads ``CHORA_AI_KERNEL_CONNINFO`` from env then
    delegates to ``open_postgres_saver``.

    Use this from production / Agent Engine entry points. Tests should call
    ``open_postgres_saver(conninfo=...)`` directly to keep the wiring
    explicit.
    """
    async with open_postgres_saver(conninfo_from_env()) as saver:
        yield saver


__all__ = [
    "CONNINFO_ENV",
    "conninfo_from_env",
    "open_postgres_saver",
    "open_postgres_saver_from_env",
]
