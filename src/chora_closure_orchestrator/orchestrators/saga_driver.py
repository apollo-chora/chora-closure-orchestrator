"""SagaDriver — background loop advancing closure sagas.

The Agent-Engine-era driver ran the whole LangGraph in one ``query()``
call. The saga is event-paced instead:

* tick 1 — ``CLOSING`` + grace expired (incl. operator fast-close) →
  ``advance_to_pseudonymized_node`` → ``pseudonymise_fanout_node``
  (publishes ``chora.{domain}.pii.pseudonymise.requested.v1`` × 10).
* tick 2 — ``PSEUDONYMIZED`` + all 10 acks recorded (by
  ``DomainAckHandler``) → ``await_per_domain_acks_node`` (aggregated
  complete event) → ``archive_to_coldline_node`` → ``crypto_shred_node``.

Pod-death resilience by construction: the driver holds NO in-process
state — every tick re-reads ``chora_ai_kernel.closure_saga`` via the
CoordinatorRepository, so any replica can resume any saga (D6.1).
Node functions are idempotent against the saga state machine (each
re-checks ``c.state`` before advancing).
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
from typing import Any

from chora_closure_orchestrator.adapter.coldarchive.port import (
    ColdArchiveClient,
)
from chora_closure_orchestrator.adapter.events.payloads import (
    AgentTerminated,
)
from chora_closure_orchestrator.adapter.events.port import (
    ClosureEventPublisher,
)
from chora_closure_orchestrator.adapter.kms.port import KMSClient
from chora_closure_orchestrator.adapter.repository.port import (
    CoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import (
    REQUIRED_DOMAINS,
    Coordinator,
    State,
)
from chora_closure_orchestrator.orchestrators.closure_graph import (
    archive_to_coldline_node,
    await_per_domain_acks_node,
    crypto_shred_node,
    pseudonymise_fanout_node,
)

logger = logging.getLogger(__name__)


class SagaDriver:
    """Stateless saga advancement loop over the CoordinatorRepository."""

    def __init__(
        self,
        *,
        repo: CoordinatorRepository,
        publisher: ClosureEventPublisher,
        kms: KMSClient,
        archive: ColdArchiveClient,
        batch_size: int = 50,
        ack_timeout_seconds: float = 0.0,
    ) -> None:
        self._repo = repo
        self._publisher = publisher
        self._kms = kms
        self._archive = archive
        self._batch = batch_size
        # D3: how long a SUSPENDED saga may wait for all per-domain acks before
        # escalating to PSEUDONYMISE_PARTIAL. <=0 disables the timeout (the saga
        # waits indefinitely — still never silently advances to PSEUDONYMIZED).
        self._ack_timeout_seconds = ack_timeout_seconds

    async def tick(self, now: _dt.datetime | None = None) -> dict[str, int]:
        """Advance every eligible saga one stage. Returns counters.

        D3: fan-out advances CLOSING -> SUSPENDED only. A saga reaches
        PSEUDONYMIZED solely once every required domain has acked; if the acks
        do not all arrive within ``ack_timeout_seconds`` it escalates to
        PSEUDONYMISE_PARTIAL (admin queue) rather than silently advancing or
        silently stalling. A PARTIAL saga recovers forward if the late acks
        eventually land.
        """
        at = now or _dt.datetime.now(_dt.UTC)
        fanned_out = 0
        completed = 0
        partial = 0

        # CLOSING + grace expired (incl. operator fast-close) -> SUSPENDED + fan-out.
        for c in await self._repo.list_by_state(State.CLOSING, self._batch):
            if not c.grace_expired_at(at):
                continue
            if await self._fanout(c):
                fanned_out += 1

        # SUSPENDED -> PSEUDONYMIZED (all acked) -> run to completion, or
        # -> PSEUDONYMISE_PARTIAL (ack timeout). Never advance on fan-out alone.
        for c in await self._repo.list_by_state(State.SUSPENDED, self._batch):
            if c.all_domains_acked() and await self._promote_and_finalize(c, "all_domains_acked"):
                completed += 1
            elif self._ack_timeout_exceeded(c, at) and await self._escalate_partial(c):
                partial += 1

        # PSEUDONYMISE_PARTIAL recovers forward once the late acks complete.
        for c in await self._repo.list_by_state(State.PSEUDONYMISE_PARTIAL, self._batch):
            if not c.all_domains_acked():
                continue
            if await self._promote_and_finalize(c, "late_acks_complete"):
                completed += 1

        # PSEUDONYMIZED + all acked -> finalize (pod-death resume safety: the
        # saga was promoted in a prior tick before the pod died mid-finalize).
        for c in await self._repo.list_by_state(State.PSEUDONYMIZED, self._batch):
            if not c.all_domains_acked():
                continue
            if await self._finalize(c):
                completed += 1

        return {
            "fanned_out": fanned_out,
            "completed": completed,
            "partial": partial,
        }

    def _ack_timeout_exceeded(self, c: Coordinator, at: _dt.datetime) -> bool:
        """Report whether a SUSPENDED saga has waited past the ack timeout."""
        if self._ack_timeout_seconds <= 0:
            return False
        started = c.suspended_at()
        if started is None:
            return False
        return (at - started).total_seconds() > self._ack_timeout_seconds

    async def run_forever(self, *, interval_seconds: float, stop: asyncio.Event) -> None:
        """Poll loop for the lifespan task."""
        while not stop.is_set():
            try:
                stats = await self.tick()
                if stats["fanned_out"] or stats["completed"]:
                    logger.info("saga_driver_tick", extra=stats)
            except Exception as exc:  # noqa: BLE001 — loop must survive
                # Surface the cause in the message (plain text logs drop the
                # `extra` dict) + the full traceback. A swallowed error here hid
                # a saga-driver outage for an entire deploy window.
                logger.warning(
                    "saga_driver_tick_failed: %s: %s",
                    type(exc).__name__,
                    str(exc)[:500],
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue

    # ------------------------------------------------------------------
    # Stages
    # ------------------------------------------------------------------

    async def _fanout(self, c: Coordinator) -> bool:
        # D3: advance CLOSING -> SUSPENDED ONLY, then fan out. The saga stays
        # SUSPENDED until the per-domain acks resolve it (no premature
        # PSEUDONYMIZED leap that falsely asserts every domain pseudonymised).
        try:
            c.advance(
                State.SUSPENDED,
                "grace_expired",
                c.requested_by_gcid,
            )
            await self._repo.save(c)
        except Exception as exc:  # noqa: BLE001
            self._log_errors(c, "advance_to_suspended", {"errors": [str(exc)[:500]]})
            return False
        state = self._node_state(c)
        r = await pseudonymise_fanout_node(state, repo=self._repo, publisher=self._publisher)
        if r.get("errors"):
            self._log_errors(c, "pseudonymise_fanout", r)
            return False
        return True

    async def _promote_and_finalize(self, c: Coordinator, reason: str) -> bool:
        # SUSPENDED / PSEUDONYMISE_PARTIAL -> PSEUDONYMIZED (all domains acked),
        # then run the post-pseudonymisation stages to completion.
        try:
            if c.state in (State.SUSPENDED, State.PSEUDONYMISE_PARTIAL):
                c.advance(State.PSEUDONYMIZED, reason, c.requested_by_gcid)
                await self._repo.save(c)
        except Exception as exc:  # noqa: BLE001
            self._log_errors(c, "advance_to_pseudonymized", {"errors": [str(exc)[:500]]})
            return False
        return await self._finalize(c)

    async def _escalate_partial(self, c: Coordinator) -> bool:
        # SUSPENDED -> PSEUDONYMISE_PARTIAL: ack timeout. Surfaces in the O+
        # admin queue; NOT a silent advance and NOT a silent stall.
        try:
            c.advance(
                State.PSEUDONYMISE_PARTIAL,
                "ack_timeout",
                c.requested_by_gcid,
            )
            await self._repo.save(c)
        except Exception as exc:  # noqa: BLE001
            self._log_errors(c, "escalate_partial", {"errors": [str(exc)[:500]]})
            return False
        logger.warning(
            "closure_ack_timeout",
            extra={
                "saga_id": c.saga_id,
                "tenant_id": c.tenant_id,
                "acked": len(c.domain_acks),
                "required": len(REQUIRED_DOMAINS),
            },
        )
        return True

    async def _finalize(self, c: Coordinator) -> bool:
        """Run the terminal stages, and emit AgentTerminated on the way out.

        THE EXECUTION BOUNDARY. ``chora.ai_kernel.agent.terminated.v1`` is
        one event per execution lifecycle, and this method IS the lifecycle
        end for a closure saga. The Agent-Engine-era boundary was
        ``ClosureSagaAgent.query``, which ran the whole graph per request and
        was retired with that deploy chain; the emission contract it carried
        is restored here against the driver, unchanged in meaning.

        ⚠ THREE OUTCOMES, and only two of them terminate:

        * every stage clean -> SUCCESS, ``last_state_node`` = the terminal
          state the saga actually reached.
        * a stage RAISES -> RUNTIME_ERROR carrying the message, then the
          original exception propagates.
        * a stage RETURNS ``errors`` -> **no event at all**. That is a
          retryable stall, not a termination: the saga keeps its state and
          the next tick retries it. Emitting here would tell the two live
          subscribers of that topic a saga had finished while it is still
          running, which corrupts exactly the per-tenant KPI and the tracing
          streams the event exists to feed. A wrong emit is worse than none.
        """
        state = self._node_state(c)
        stage = ""
        try:
            stage = "await_per_domain_acks"
            r = await await_per_domain_acks_node(state, repo=self._repo, publisher=self._publisher)
            if r.get("errors"):
                self._log_errors(c, stage, r)
                return False
            stage = "archive_to_coldline"
            r = await archive_to_coldline_node(
                state,
                repo=self._repo,
                publisher=self._publisher,
                kms=self._kms,
                archive=self._archive,
            )
            if r.get("errors"):
                self._log_errors(c, stage, r)
                return False
            state["dek_resource_name"] = r.get("dek_resource_name", "")
            stage = "crypto_shred"
            r = await crypto_shred_node(state, repo=self._repo, publisher=self._publisher, kms=self._kms)
            if r.get("errors"):
                self._log_errors(c, stage, r)
                return False
        except Exception as exc:
            # Re-read so last_state_node reports where the saga actually
            # stopped rather than where it started this tick.
            await self._emit_terminated(
                c,
                code="RUNTIME_ERROR",
                last_state_node=stage,
                error=str(exc)[:500],
            )
            raise
        final = await self._repo.get(c.saga_id)
        await self._emit_terminated(
            c,
            code="SUCCESS",
            last_state_node=(final.state.value if final else State.CRYPTO_SHREDDED.value),
        )
        return True

    async def _emit_terminated(
        self,
        c: Coordinator,
        *,
        code: str,
        last_state_node: str,
        error: str = "",
    ) -> None:
        """Publish AgentTerminated, and NEVER let that failure become the
        caller's failure.

        The publisher writes through the transactional outbox, so it can
        raise for reasons that have nothing to do with the saga. On the
        exception path this method is called while an exception is already
        in flight, and the original one is the one the operator needs: a
        publisher error surfacing INSTEAD would replace a real saga failure
        with an infrastructure one and send the diagnosis somewhere else
        entirely. So emission failure is logged loudly and swallowed HERE,
        and only here, which is what lets the ``raise`` above re-raise the
        original untouched.

        ⚠ Swallowing is confined to the emission itself. Nothing about the
        saga's own outcome is suppressed: a stage failure still returns
        False or propagates, exactly as before this method existed.
        """
        try:
            await self._publisher.publish_agent_terminated(
                AgentTerminated(
                    agent_id="closure_saga",
                    execution_id=c.saga_id,
                    termination_code=f"AGENT_TERMINATION_CODE_{code}",
                    runtime="AGENT_EXECUTION_RUNTIME_LANGGRAPH_PYTHON",
                    tenant_id=c.tenant_id,
                    gcid=c.gcid,
                    terminated_at=_dt.datetime.now(_dt.UTC),
                    crew_id="closure_saga",
                    crew_pattern="P8_LANGGRAPH_STATEFUL_SAGA",
                    last_state_node=last_state_node,
                    last_error_message=error,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "agent_terminated_emit_failed",
                extra={
                    "saga_id": c.saga_id,
                    "tenant_id": c.tenant_id,
                    "termination_code": code,
                    "error": str(exc)[:500],
                },
            )

    @staticmethod
    def _node_state(c: Coordinator) -> Any:
        return {
            "saga_id": c.saga_id,
            "gcid": c.gcid,
            "tenant_id": c.tenant_id,
            "requested_by_gcid": c.requested_by_gcid,
            "reason": c.reason,
            "trace": [],
            "errors": [],
        }

    @staticmethod
    def _log_errors(c: Coordinator, stage: str, result: dict[str, Any]) -> None:
        logger.warning(
            "saga_driver_stage_failed",
            extra={
                "saga_id": c.saga_id,
                "tenant_id": c.tenant_id,
                "stage": stage,
                "errors": result.get("errors"),
            },
        )


__all__ = ["SagaDriver"]
