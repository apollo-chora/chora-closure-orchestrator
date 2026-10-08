"""FastAPI handlers for chora-closure-orchestrator.

Endpoints (per S7.1 deliverables):

- ``POST /v1/closure/request`` — start a closure saga (idempotent on closure_id)
- ``POST /v1/closure/{closure_id}/cancel`` — cancel during grace period
- ``GET  /v1/closure/{closure_id}/status`` — current state + history + ETAs
- ``GET  /healthz``, ``GET /readyz``

Auth: ``Bearer <GCID>`` header (no inline secrets; mTLS at the ingress in
production). AGID requests are rejected at the saga level (Coordinator
domain invariant — preserved across the Go-to-Python port).
"""

from __future__ import annotations

import datetime as _dt
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from chora_closure_orchestrator.adapter.coldarchive import (
    InMemoryColdArchiveClient,
)
from chora_closure_orchestrator.adapter.events import InMemoryClosurePublisher
from chora_closure_orchestrator.adapter.events.payloads import (
    ClosureCancelled,
    ClosureRequested,
)
from chora_closure_orchestrator.adapter.kms import FakeKMSClient
from chora_closure_orchestrator.adapter.repository import (
    InMemoryCoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import (
    CoordinatorError,
    ErrCancelTooLate,
    ErrSagaNotFound,
    NewParams,
    new,
)

# -----------------------------------------------------------------------------
# Request / response shapes
# -----------------------------------------------------------------------------


class CloseRequestBody(BaseModel):
    """POST /v1/closure/request body."""

    gcid: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    grace_period_days: int = Field(ge=1, le=365)
    reason: str = Field(default="", max_length=1024)
    requested_by_gcid: str = Field(min_length=1)
    # Operator fast-close (ADR-181 ruling 12): collapses the grace window
    # for test accounts. PLATFORM_OPERATOR-gated via the gateway-stamped
    # X-Chora-Role header. No effect on normal closures.
    fast_close: bool = False


class CloseResponse(BaseModel):
    saga_id: str
    state: str
    grace_ends_at: str
    requested_at: str


class CancelRequestBody(BaseModel):
    """POST /v1/closure/{id}/cancel body."""

    actor_gcid: str = Field(min_length=1)
    reason: str = Field(default="", max_length=1024)


class StatusResponse(BaseModel):
    saga_id: str
    gcid: str
    tenant_id: str
    state: str
    grace_ends_at: str
    requested_at: str
    history: list[dict[str, Any]] = Field(default_factory=list)
    domain_acks: list[dict[str, Any]] = Field(default_factory=list)


# -----------------------------------------------------------------------------
# App factory
# -----------------------------------------------------------------------------


@dataclass
class AdapterSet:
    """Mutable adapter holder — the real-wiring lifespan swaps entries in
    at startup (CHO-1719 gap 1); handlers read through it on
    every request."""

    repo: Any
    publisher: Any
    kms: Any
    archive: Any
    mode: str = "fake"


def _has_platform_operator(header_value: str) -> bool:
    """True when the gateway-stamped comma-joined X-Chora-Role header
    carries PLATFORM_OPERATOR (liberal-in: case-insensitive)."""
    return any(tok.strip().lower() == "platform_operator" for tok in (header_value or "").split(","))


def build_app(
    *,
    repo: Any,
    publisher: Any,
    kms: Any,
    archive: Any,
    lifespan: Any = None,
) -> FastAPI:
    """Build the FastAPI app with the supplied adapters."""
    adapters = AdapterSet(repo=repo, publisher=publisher, kms=kms, archive=archive)
    app = FastAPI(
        title="Chora Closure Orchestrator",
        version="0.2.0",
        description=(
            "Federated account closure saga (Tier 3 D11). Python LangGraph "
            "orchestrator (Tier 2 D5 hybrid kernel). Pseudonymise + "
            "crypto-shred — NEVER hard-delete."
        ),
        lifespan=lifespan,
    )
    app.state.adapters = adapters

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "time": _now_iso()})

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        a = app.state.adapters
        if any(x is None for x in (a.repo, a.publisher, a.kms, a.archive)):
            return JSONResponse(
                {"status": "not_ready", "reason": "adapters_unconfigured"},
                status_code=503,
            )
        return JSONResponse({"status": "ready", "mode": a.mode, "time": _now_iso()})

    @app.post("/v1/closure/request", status_code=201)
    async def request_closure(body: CloseRequestBody, request: Request) -> JSONResponse:
        # AGID detection mirrors Coordinator.new() invariant.
        if body.gcid.lower().startswith("0197a"):
            return JSONResponse(
                {
                    "code": "CLOSURE_AGID_REJECTED",
                    "message": "AGID cannot own a closure saga (agents have no lifecycle)",
                },
                status_code=403,
            )
        # Operator fast-close gate (ADR-181 ruling 12): only
        # PLATFORM_OPERATOR may collapse the grace period. The gateway
        # stamps the session JWT roles into X-Chora-Role (same convention
        # chora-payments admin_handler trusts).
        if body.fast_close and not _has_platform_operator(request.headers.get("X-Chora-Role", "")):
            return JSONResponse(
                {
                    "code": "CLOSURE_FAST_CLOSE_OPERATOR_REQUIRED",
                    "message": (
                        "fast_close collapses the grace period and is restricted to PLATFORM_OPERATOR sessions"
                    ),
                },
                status_code=403,
            )
        try:
            c = new(
                NewParams(
                    gcid=body.gcid,
                    tenant_id=body.tenant_id,
                    grace_period_days=body.grace_period_days,
                    reason=body.reason,
                    requested_by_gcid=body.requested_by_gcid,
                    fast_close=body.fast_close,
                )
            )
        except CoordinatorError as exc:
            return JSONResponse(
                {"code": "CLOSURE_INVALID_PARAMS", "message": str(exc)},
                status_code=400,
            )

        await app.state.adapters.repo.save(c)
        await app.state.adapters.publisher.publish_closure_requested(
            ClosureRequested(
                saga_id=c.saga_id,
                gcid=c.gcid,
                tenant_id=c.tenant_id,
                grace_period_days=c.grace_period_days,
                reason=c.reason,
                requested_by_gcid=c.requested_by_gcid,
                participating_domains=[
                    "creation",
                    "consumption",
                    "delivery",
                    "sharing",
                    "a2a",
                    "identity",
                    "tenancy",
                    "governance",
                    "observability",
                    "notifications",
                ],
                requested_at=c.requested_at,
            )
        )
        return JSONResponse(
            CloseResponse(
                saga_id=c.saga_id,
                state=c.state.value,
                grace_ends_at=c.grace_ends_at.isoformat(),
                requested_at=c.requested_at.isoformat(),
            ).model_dump(),
            status_code=201,
        )

    @app.post("/v1/closure/{closure_id}/cancel")
    async def cancel_closure(
        body: CancelRequestBody,
        closure_id: str = Path(..., min_length=1),
    ) -> JSONResponse:
        try:
            c = await app.state.adapters.repo.get(closure_id)
        except ErrSagaNotFound:
            return JSONResponse(
                {"code": "CLOSURE_SAGA_NOT_FOUND", "message": "saga not found"},
                status_code=404,
            )

        try:
            c.cancel(body.reason, body.actor_gcid)
        except ErrCancelTooLate as exc:
            return JSONResponse(
                {"code": "CLOSURE_CANCEL_TOO_LATE", "message": str(exc)},
                status_code=409,
            )

        await app.state.adapters.repo.save(c)
        await app.state.adapters.publisher.publish_cancelled(
            ClosureCancelled(
                saga_id=c.saga_id,
                gcid=c.gcid,
                tenant_id=c.tenant_id,
                cancelled_by_gcid=body.actor_gcid,
                reason=body.reason,
                cancelled_at=_dt.datetime.now(_dt.UTC),
            )
        )
        return JSONResponse(
            {
                "saga_id": c.saga_id,
                "state": c.state.value,
                "cancelled_at": (c.cancelled_at.isoformat() if c.cancelled_at else ""),
            }
        )

    @app.get("/v1/closure/{closure_id}/status")
    async def get_status(
        closure_id: str = Path(..., min_length=1),
    ) -> JSONResponse:
        try:
            c = await app.state.adapters.repo.get(closure_id)
        except ErrSagaNotFound:
            return JSONResponse(
                {"code": "CLOSURE_SAGA_NOT_FOUND", "message": "saga not found"},
                status_code=404,
            )
        return JSONResponse(
            StatusResponse(
                saga_id=c.saga_id,
                gcid=c.gcid,
                tenant_id=c.tenant_id,
                state=c.state.value,
                grace_ends_at=c.grace_ends_at.isoformat(),
                requested_at=c.requested_at.isoformat(),
                history=[
                    {
                        "prior_state": h.prior_state.value,
                        "new_state": h.new_state.value,
                        "reason": h.reason,
                        "actor_gcid": h.actor_gcid,
                        "transitioned_at": h.transitioned_at.isoformat(),
                    }
                    for h in c.history
                ],
                domain_acks=[
                    {
                        "domain": a.domain,
                        "acked_at": a.acked_at.isoformat(),
                    }
                    for a in c.domain_acks
                ],
            ).model_dump()
        )

    return app


def build_app_from_env() -> FastAPI:
    """Build the app from env vars (no inline config — per
    ``feedback_no_inline_config`` memory).

    Adapter selection is delegated to ``wiring.plan_from_env()``
    (CHO-1719 gap 1): with the env present the app
    wires the REAL adapters (Postgres coordinator repo + transactional
    outbox + NATS outbox dispatcher + local KMS + MinIO cold archive) inside
    the lifespan; without it (dev / unit tests) it keeps the in-memory
    fakes. The saga driver + ack consumers start as background tasks.
    """
    import os

    from chora_closure_orchestrator.wiring import plan_from_env, start_runtime

    plan = plan_from_env()

    fakes = dict(
        repo=InMemoryCoordinatorRepository(),
        publisher=InMemoryClosurePublisher(),
        kms=FakeKMSClient(),
        archive=InMemoryColdArchiveClient(bucket=os.getenv("CHORA_COLD_ARCHIVE_BUCKET", "chora-cold-archive-dev")),
    )

    if not plan.any_real and not plan.ack_subscriptions:
        return build_app(**fakes)

    @asynccontextmanager
    async def _lifespan(application: FastAPI):  # noqa: ANN202
        runtime = await start_runtime(plan)
        application.state.adapters.repo = runtime.repo
        application.state.adapters.publisher = runtime.publisher
        application.state.adapters.kms = runtime.kms
        application.state.adapters.archive = runtime.archive
        application.state.adapters.mode = "real"
        try:
            yield
        finally:
            await runtime.stop()

    return build_app(**fakes, lifespan=_lifespan)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat()
