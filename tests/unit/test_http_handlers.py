"""HTTP handler tests for chora-closure-orchestrator.

Endpoints:
- POST /v1/closure/request — start a new closure saga (idempotent on closure_id)
- POST /v1/closure/{closure_id}/cancel — cancel during grace
- GET  /v1/closure/{closure_id}/status — current state + history + ETAs
- GET  /healthz, /readyz
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from chora_closure_orchestrator.adapter.coldarchive import (
    InMemoryColdArchiveClient,
)
from chora_closure_orchestrator.adapter.events import InMemoryClosurePublisher
from chora_closure_orchestrator.adapter.http.handlers import build_app
from chora_closure_orchestrator.adapter.kms import FakeKMSClient
from chora_closure_orchestrator.adapter.repository import (
    InMemoryCoordinatorRepository,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"
AGID = "0197a000-0000-7000-9000-000000000001"


def _client() -> TestClient:
    repo = InMemoryCoordinatorRepository()
    pub = InMemoryClosurePublisher()
    kms = FakeKMSClient()
    archive = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
    app = build_app(repo=repo, publisher=pub, kms=kms, archive=archive)
    return TestClient(app)


class TestHealth:
    def test_healthz(self) -> None:
        c = _client()
        r = c.get("/healthz")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"

    def test_readyz_ready_when_deps_set(self) -> None:
        c = _client()
        r = c.get("/readyz")
        assert r.status_code == 200


class TestRequestEndpoint:
    def test_request_creates_saga(self) -> None:
        c = _client()
        body = {
            "gcid": GCID,
            "tenant_id": TENANT,
            "grace_period_days": 30,
            "reason": "self",
            "requested_by_gcid": GCID,
        }
        r = c.post("/v1/closure/request", json=body)
        assert r.status_code == 201
        out = r.json()
        assert out["saga_id"] != ""
        assert out["state"] == "closing"

    def test_request_rejects_agid(self) -> None:
        c = _client()
        body = {
            "gcid": AGID,
            "tenant_id": TENANT,
            "grace_period_days": 30,
            "reason": "test",
            "requested_by_gcid": AGID,
        }
        r = c.post("/v1/closure/request", json=body)
        # AGID rejection — invariant from Go port
        assert r.status_code == 403
        assert r.json()["code"] == "CLOSURE_AGID_REJECTED"

    def test_request_rejects_invalid_grace(self) -> None:
        c = _client()
        body = {
            "gcid": GCID,
            "tenant_id": TENANT,
            "grace_period_days": 0,
            "reason": "test",
            "requested_by_gcid": GCID,
        }
        r = c.post("/v1/closure/request", json=body)
        # Pydantic returns 422 for body-level validation failure; 400 also
        # acceptable when validation moves to the domain layer.
        assert r.status_code in (400, 422)


class TestCancelEndpoint:
    def test_cancel_during_grace(self) -> None:
        c = _client()
        body = {
            "gcid": GCID,
            "tenant_id": TENANT,
            "grace_period_days": 30,
            "reason": "self",
            "requested_by_gcid": GCID,
        }
        created = c.post("/v1/closure/request", json=body).json()
        sid = created["saga_id"]

        r = c.post(
            f"/v1/closure/{sid}/cancel",
            json={"actor_gcid": GCID, "reason": "changed_mind"},
        )
        assert r.status_code == 200
        assert r.json()["state"] == "active"

    def test_cancel_unknown_returns_404(self) -> None:
        c = _client()
        r = c.post(
            "/v1/closure/unknown/cancel",
            json={"actor_gcid": GCID, "reason": "x"},
        )
        assert r.status_code == 404


class TestStatusEndpoint:
    def test_status_returns_state_history(self) -> None:
        c = _client()
        created = c.post(
            "/v1/closure/request",
            json={
                "gcid": GCID,
                "tenant_id": TENANT,
                "grace_period_days": 30,
                "reason": "self",
                "requested_by_gcid": GCID,
            },
        ).json()
        sid = created["saga_id"]
        r = c.get(f"/v1/closure/{sid}/status")
        assert r.status_code == 200
        body = r.json()
        assert body["saga_id"] == sid
        assert body["state"] == "closing"
        assert isinstance(body["history"], list)
        assert "grace_ends_at" in body

    def test_status_unknown_returns_404(self) -> None:
        c = _client()
        r = c.get("/v1/closure/unknown/status")
        assert r.status_code == 404
