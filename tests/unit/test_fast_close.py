"""Operator fast-close (CHO-1719 gap 6 / ADR-181 ruling 12).

``fast_close`` collapses the grace period for test accounts. It is
PLATFORM_OPERATOR-gated at the orchestrator (the gateway stamps the
comma-joined ``X-Chora-Role`` header from the session JWT — same
convention chora-payments admin_handler trusts). No silent global grace
change: a non-fast-close request keeps the full grace window.
"""

from __future__ import annotations

import datetime as _dt

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
from chora_closure_orchestrator.domain.closure import NewParams, new

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


def _client() -> TestClient:
    app = build_app(
        repo=InMemoryCoordinatorRepository(),
        publisher=InMemoryClosurePublisher(),
        kms=FakeKMSClient(),
        archive=InMemoryColdArchiveClient(bucket="chora-cold-archive-dev"),
    )
    return TestClient(app)


def _body(**overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "gcid": GCID,
        "tenant_id": TENANT,
        "grace_period_days": 30,
        "reason": "test fast close",
        "requested_by_gcid": GCID,
    }
    body.update(overrides)
    return body


class TestDomainFastClose:
    def test_fast_close_collapses_grace(self) -> None:
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
                fast_close=True,
            )
        )
        assert c.grace_ends_at <= c.requested_at
        assert c.grace_expired_at(_dt.datetime.now(_dt.UTC))

    def test_default_keeps_full_grace(self) -> None:
        c = new(
            NewParams(
                gcid=GCID,
                tenant_id=TENANT,
                grace_period_days=30,
                requested_by_gcid=GCID,
            )
        )
        delta = c.grace_ends_at - c.requested_at
        assert delta == _dt.timedelta(days=30)
        assert not c.grace_expired_at(_dt.datetime.now(_dt.UTC))


class TestFastCloseEndpoint:
    def test_fast_close_without_operator_role_403(self) -> None:
        c = _client()
        r = c.post("/v1/closure/request", json=_body(fast_close=True))
        assert r.status_code == 403
        assert r.json()["code"] == "CLOSURE_FAST_CLOSE_OPERATOR_REQUIRED"

    def test_fast_close_with_wrong_role_403(self) -> None:
        c = _client()
        r = c.post(
            "/v1/closure/request",
            json=_body(fast_close=True),
            headers={"X-Chora-Role": "tenant_admin,learner"},
        )
        assert r.status_code == 403

    def test_fast_close_with_operator_role_201_grace_collapsed(self) -> None:
        c = _client()
        r = c.post(
            "/v1/closure/request",
            json=_body(fast_close=True),
            headers={"X-Chora-Role": "PLATFORM_OPERATOR"},
        )
        assert r.status_code == 201
        body = r.json()
        grace_ends = _dt.datetime.fromisoformat(body["grace_ends_at"])
        requested = _dt.datetime.fromisoformat(body["requested_at"])
        assert grace_ends <= requested

    def test_fast_close_accepts_lowercase_alias(self) -> None:
        # gateway stampRoleHeader canonicalises, but the orchestrator is
        # liberal-in: lowercase alias accepted.
        c = _client()
        r = c.post(
            "/v1/closure/request",
            json=_body(fast_close=True),
            headers={"X-Chora-Role": "platform_operator"},
        )
        assert r.status_code == 201

    def test_normal_close_needs_no_role_and_keeps_grace(self) -> None:
        c = _client()
        r = c.post("/v1/closure/request", json=_body())
        assert r.status_code == 201
        body = r.json()
        grace_ends = _dt.datetime.fromisoformat(body["grace_ends_at"])
        requested = _dt.datetime.fromisoformat(body["requested_at"])
        assert (grace_ends - requested) >= _dt.timedelta(days=29)
