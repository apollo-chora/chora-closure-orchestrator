"""Tests for ``NatsPublisher`` — the thin async wrapper used
by the outbox dispatcher to publish messages to the NATS JetStream
event bus.

The envelope is encoded as message HEADERS (event_id, idempotency_key,
tenant_id, gcid, occurred_at, traceparent, tracestate, source_project,
source_service, schema_version) so subscribers can filter without
parsing the payload.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from chora_closure_orchestrator.adapter.pubsub import (
    NatsPublisher,
    OutboxRow,
)


class _MockPubAck:
    def __init__(self, stream: str, seq: int) -> None:
        self.stream = stream
        self.seq = seq


class _MockClient:
    """JetStream-shaped mock: publish(subject, payload, headers=...)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str]]] = []
        self._next_seq = 1

    async def publish(self, subject: str, payload: bytes, headers: dict[str, str]) -> _MockPubAck:
        self.published.append((subject, payload, dict(headers)))
        ack = _MockPubAck(stream="CHORA_EVENTS", seq=self._next_seq)
        self._next_seq += 1
        return ack


def _envelope_dict() -> dict[str, str]:
    return {
        "event_id": "01970000-7777-7000-a000-000000000001",
        "idempotency_key": "01970000-7777-7000-a000-000000000001",
        "tenant_id": "01970000-0000-7000-8000-000000000001",
        "gcid": "01970000-0000-7000-9000-000000000001",
        "occurred_at": "2026-05-12T08:00:00+00:00",
        "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        "tracestate": "vendor=chora",
        "source_project": "chora-local",
        "source_service": "chora-closure-orchestrator",
        "schema_version": "1",
    }


def _row(**overrides: Any) -> OutboxRow:
    base: dict[str, Any] = {
        "id": "01970000-7777-7000-a000-000000000001",
        "saga_id": "d42940c4-3edf-4a3e-83ed-938c5fef4401",
        "tenant_id": "01970000-0000-7000-8000-000000000001",
        "gcid": "01970000-0000-7000-9000-000000000001",
        "event_type": "closure.requested",
        "topic": "chora.closure.requested.v1",
        "payload": json.dumps({"hello": "world"}).encode("utf-8"),
        "idempotency_key": "01970000-7777-7000-a000-000000000001",
    }
    base.update(overrides)
    # Sync envelope to mirror the row's canonical columns — production's
    # TransactionalOutboxPublisher writes both consistently.
    base["envelope"] = {
        **_envelope_dict(),
        "tenant_id": base["tenant_id"],
        "gcid": base["gcid"],
        "event_id": base["id"],
        "idempotency_key": base["idempotency_key"],
    }
    return OutboxRow(**base)


class TestNatsPublisherInit:
    def test_constructor_captures_client(self) -> None:
        client = _MockClient()
        pub = NatsPublisher(client=client)
        assert pub._client is client  # noqa: SLF001


class TestPublishOutboxRow:
    @pytest.mark.asyncio
    async def test_publishes_binary_proto_to_bound_topic(self) -> None:
        """The 6 chora.closure.* lifecycle topics MUST receive canonical
        Protobuf binary, not JSON — the binary-bound wire schema rejects
        JSON."""
        from chora_contracts_gen.events.closure import (
            saga_pb2,
        )

        client = _MockClient()
        pub = NatsPublisher(client=client)

        body = json.dumps(
            {
                "saga_id": "d42940c4-3edf-4a3e-83ed-938c5fef4401",
                "gcid": "g1",
                "tenant_id": "t1",
                "grace_period_days": 30,
                "participating_domains": ["creation", "consumption"],
                "requested_at": "2026-06-20T08:00:00+00:00",
            }
        ).encode("utf-8")
        message_id = await pub.publish(_row(payload=body))

        assert message_id == "CHORA_EVENTS-1"
        assert len(client.published) == 1
        subject, data, _headers = client.published[0]
        assert subject == "chora.closure.requested.v1"
        # JSON parse MUST fail; the bytes are proto binary.
        with pytest.raises((ValueError, UnicodeDecodeError)):
            json.loads(data)
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(data)
        assert m.saga_id == "d42940c4-3edf-4a3e-83ed-938c5fef4401"
        assert list(m.participating_domains) == ["creation", "consumption"]
        # The envelope (incl. the just-stamped published_at) rode along.
        assert m.envelope.event_id == _headers["Chora-Event-Id"]
        assert m.envelope.published_at.ToJsonString().endswith("Z")

    @pytest.mark.asyncio
    async def test_schemaless_topic_ships_json_verbatim(self) -> None:
        """The 10 per-domain fan-out topics are schemaless JSON BY DESIGN —
        the publisher must NOT re-encode them."""
        client = _MockClient()
        pub = NatsPublisher(client=client)

        body = {"domain": "creation", "saga_id": "s1"}
        await pub.publish(
            _row(
                topic="chora.creation.pii.pseudonymise.requested.v1",
                payload=json.dumps(body).encode("utf-8"),
            )
        )

        subject, data, _headers = client.published[0]
        assert json.loads(data) == body

    @pytest.mark.asyncio
    async def test_envelope_lands_as_headers(self) -> None:
        """Each envelope field MUST be a message header for subscriber-side
        filtering without payload parse."""
        client = _MockClient()
        pub = NatsPublisher(client=client)

        await pub.publish(_row())

        _subject, _data, headers = client.published[0]
        # All 10 mandatory envelope fields present, under the canonical names
        # the Go eventbus reads.
        for required in (
            "Chora-Event-Id",
            "Chora-Idempotency-Key",
            "Chora-Tenant-Id",
            "Chora-Gcid",
            "Chora-Occurred-At",
            "Traceparent",
            "Tracestate",
            "Chora-Source-Project",
            "Chora-Source-Service",
            "Chora-Schema-Version",
        ):
            assert required in headers, f"header missing: {required}"

    @pytest.mark.asyncio
    async def test_per_tenant_isolation_via_header(self) -> None:
        """D6.3 multi-tenant chaos: subscribers MAY filter by tenant_id
        header. The publisher MUST set it correctly per-message even when
        many tenants share the pipe."""
        client = _MockClient()
        pub = NatsPublisher(client=client)

        await pub.publish(_row(tenant_id="01970000-0000-7000-8000-000000000001"))
        await pub.publish(_row(tenant_id="01970000-0000-7000-8000-000000000002"))

        tenants = [h["Chora-Tenant-Id"] for _, _, h in client.published]
        assert tenants == [
            "01970000-0000-7000-8000-000000000001",
            "01970000-0000-7000-8000-000000000002",
        ]

    @pytest.mark.asyncio
    async def test_topic_becomes_subject(self) -> None:
        client = _MockClient()
        pub = NatsPublisher(client=client)

        await pub.publish(_row(topic="chora.closure.crypto_shred_complete.v1"))

        subject, _data, _headers = client.published[0]
        assert subject == "chora.closure.crypto_shred_complete.v1"

    @pytest.mark.asyncio
    async def test_publish_error_propagates(self) -> None:
        """Dispatcher needs to see publish failures to drive retry."""

        class _FailingClient(_MockClient):
            async def publish(self, subject: str, payload: bytes, headers: dict[str, str]) -> _MockPubAck:
                raise RuntimeError("NATS unavailable")

        pub = NatsPublisher(
            client=_FailingClient(),
        )

        with pytest.raises(RuntimeError, match="NATS unavailable"):
            await pub.publish(_row())


class TestGoSubscriberAttributeContract:
    """chora-go-common's envelopeFromAttributes() HARD-REQUIRES parseable
    ``occurred_at`` + ``published_at`` headers and routes on the
    ``topic`` header — without them every per-domain Go closure
    subscriber NACKs every fan-out message (CHO-1719 gap 4 finding)."""

    @pytest.mark.asyncio
    async def test_publish_stamps_published_at_and_topic(self) -> None:
        client = _MockClient()
        pub = NatsPublisher(client=client)
        row = OutboxRow(
            id="row-1",
            saga_id="saga-1",
            tenant_id="t-1",
            gcid="g-1",
            event_type="pii.pseudonymise.requested",
            topic="chora.identity.pii.pseudonymise.requested.v1",
            payload=b"{}",
            envelope=_envelope_dict(),
            idempotency_key="k-1",
        )
        await pub.publish(row)
        _subject, _data, headers = client.published[0]
        assert headers["topic"] == "chora.identity.pii.pseudonymise.requested.v1"
        assert headers.get("Chora-Published-At"), "published_at header required"
        # RFC3339-parseable (Go time.RFC3339Nano)
        import datetime as _dt

        _dt.datetime.fromisoformat(headers["Chora-Published-At"])

    @pytest.mark.asyncio
    async def test_existing_published_at_not_overwritten(self) -> None:
        client = _MockClient()
        pub = NatsPublisher(client=client)
        env = _envelope_dict()
        env["published_at"] = "2026-05-12T08:00:01+00:00"
        row = OutboxRow(
            id="row-2",
            saga_id="saga-1",
            tenant_id="t-1",
            gcid="g-1",
            event_type="x",
            topic="chora.closure.requested.v1",
            payload=b"{}",
            envelope=env,
            idempotency_key="k-2",
        )
        await pub.publish(row)
        _subject, _data, headers = client.published[0]
        assert headers["Chora-Published-At"] == "2026-05-12T08:00:01+00:00"
