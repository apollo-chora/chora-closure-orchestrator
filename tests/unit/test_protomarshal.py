"""Round-trip tests for the closure-lifecycle protomarshal encoder (D4).

The orchestrator's transactional outbox stores each event body as JSON.
The 6 ``chora.closure.*`` lifecycle topics are published as Protobuf
topics to a BINARY Protobuf schema (the flat
``events-flat/closure/saga/*.proto`` copies) — so publishing the JSON
``data`` verbatim is rejected with ``INVALID_BINARY_PROTO_MESSAGE`` and
the audit-trail row sits ``failed`` in ``closure_outbox_events``.

``protomarshal.encode`` re-encodes the JSON body (+ the envelope dict) to
canonical Protobuf binary at publish time. These tests are the
load-bearing assertion (the notifications ``protomarshal`` precedent):
if the JSON-body → wire-bytes → ``ParseFromString`` round-trip passes,
the Schema Registry will accept the bytes.

The 10 per-domain ``chora.{domain}.pii.pseudonymise.requested.v1``
fan-out/ack topics are schemaless JSON BY DESIGN — ``encode`` returns
``None`` for them and the publisher ships the JSON verbatim.

Wire-identity note: the generated ``saga_pb2`` messages embed
``chora.common.v1.EventEnvelope`` (field 1) and
``google.protobuf.Timestamp``; the registered flat schema inlines those
as nested ``Envelope`` / ``Timestamp`` messages with IDENTICAL field
numbers + wire types. Protobuf wire format depends only on field number
+ type, so the bytes produced here are byte-for-byte parseable by the
registered flat schema.
"""

from __future__ import annotations

import json

import pytest

from chora_closure_orchestrator.adapter.pubsub import protomarshal

# The generated bindings MUST be importable in the test venv (chora-contracts
# is a path-install dep). If this fails, the venv is missing the contracts.
from chora_contracts_gen.events.closure import saga_pb2

_T = "2026-06-20T08:00:00+00:00"


def _envelope() -> dict[str, str]:
    return {
        "event_id": "01970000-7777-7000-a000-000000000001",
        "idempotency_key": "01970000-7777-7000-a000-000000000001",
        "tenant_id": "01970000-0000-7000-8000-000000000001",
        "gcid": "01970000-0000-7000-9000-000000000001",
        "occurred_at": _T,
        "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        "tracestate": "vendor=chora",
        "source_project": "chora-local",
        "source_service": "chora-closure-orchestrator",
        "schema_version": "1",
    }


def _encode(topic: str, body: dict) -> bytes:
    payload = json.dumps(body, default=str).encode("utf-8")
    out = protomarshal.encode(topic, payload, _envelope())
    assert out is not None, f"expected binary for bound topic {topic}"
    assert isinstance(out, bytes)
    return out


class TestTopicMembership:
    def test_six_lifecycle_topics(self) -> None:
        assert (
            frozenset(
                {
                    "chora.closure.requested.v1",
                    "chora.closure.grace_started.v1",
                    "chora.closure.pseudonymise_per_domain_complete.v1",
                    "chora.closure.crypto_shred_complete.v1",
                    "chora.closure.closed.v1",
                    "chora.closure.cancelled.v1",
                }
            )
            == protomarshal.CLOSURE_LIFECYCLE_TOPICS
        )

    @pytest.mark.parametrize(
        "topic",
        [
            "chora.creation.pii.pseudonymise.requested.v1",
            "chora.consumption.pii.pseudonymise.requested.v1",
            "chora.identity.pii.pseudonymise.requested.v1",
            "chora.ai_kernel.agent.terminated.v1",
        ],
    )
    def test_schemaless_topics_return_none(self, topic: str) -> None:
        """The 10 per-domain fan-out topics + agent.terminated stay JSON."""
        payload = json.dumps({"domain": "creation"}).encode("utf-8")
        assert protomarshal.encode(topic, payload, _envelope()) is None


class TestEnvelopeRoundTrip:
    def test_envelope_fields_land_on_proto(self) -> None:
        body = {
            "saga_id": "d42940c4-3edf-4a3e-83ed-938c5fef4401",
            "gcid": "g1",
            "tenant_id": "t1",
            "grace_period_days": 30,
            "reason": "user-initiated",
            "requested_by_gcid": "self",
            "participating_domains": ["creation", "consumption"],
            "requested_at": _T,
        }
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(_encode("chora.closure.requested.v1", body))

        env = _envelope()
        assert m.envelope.event_id == env["event_id"]
        assert m.envelope.idempotency_key == env["idempotency_key"]
        assert m.envelope.tenant_id == env["tenant_id"]
        assert m.envelope.gcid == env["gcid"]
        assert m.envelope.traceparent == env["traceparent"]
        assert m.envelope.tracestate == env["tracestate"]
        assert m.envelope.source_project == env["source_project"]
        assert m.envelope.source_service == env["source_service"]
        # schema_version is int32 on the wire even though the envelope dict
        # carries it as a string attribute.
        assert m.envelope.schema_version == 1
        assert m.envelope.occurred_at.ToJsonString() == "2026-06-20T08:00:00Z"

    def test_blank_schema_version_does_not_crash(self) -> None:
        env = _envelope()
        env["schema_version"] = ""
        payload = json.dumps({"saga_id": "s"}).encode("utf-8")
        out = protomarshal.encode("chora.closure.requested.v1", payload, env)
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.envelope.schema_version == 0

    def test_non_integer_schema_version_is_tolerated(self) -> None:
        """A malformed (non-int) schema_version must not sink the publish."""
        env = _envelope()
        env["schema_version"] = "v1-not-an-int"
        out = protomarshal.encode("chora.closure.requested.v1", b"{}", env)
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.envelope.schema_version == 0

    def test_optional_envelope_fields_ride_along(self) -> None:
        """Forward-compatible envelope fields (correlation_id, IMDA tags) are
        encoded when present even though the current outbox writer omits them."""
        env = _envelope()
        env["correlation_id"] = "corr-1"
        env["chora_imda_dimension"] = "accountability"
        out = protomarshal.encode("chora.closure.requested.v1", b"{}", env)
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.envelope.correlation_id == "corr-1"
        assert m.envelope.chora_imda_dimension == "accountability"


class TestClosureRequested:
    def test_round_trip(self) -> None:
        body = {
            "saga_id": "saga-1",
            "gcid": "gcid-1",
            "tenant_id": "tenant-1",
            "grace_period_days": 30,
            "reason": "left every tenant",
            "requested_by_gcid": "admin-1",
            "participating_domains": ["creation", "consumption", "delivery"],
            "requested_at": _T,
        }
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(_encode("chora.closure.requested.v1", body))
        assert m.saga_id == "saga-1"
        assert m.gcid == "gcid-1"
        assert m.tenant_id == "tenant-1"
        assert m.grace_period_days == 30
        assert m.reason == "left every tenant"
        assert m.requested_by_gcid == "admin-1"
        assert list(m.participating_domains) == [
            "creation",
            "consumption",
            "delivery",
        ]
        assert m.requested_at.ToJsonString() == "2026-06-20T08:00:00Z"


class TestClosureGraceStarted:
    def test_round_trip(self) -> None:
        body = {
            "saga_id": "saga-2",
            "gcid": "gcid-2",
            "tenant_id": "tenant-2",
            "grace_expires_at": "2026-07-20T08:00:00+00:00",
            "started_at": _T,
        }
        m = saga_pb2.ClosureGraceStarted()
        m.ParseFromString(_encode("chora.closure.grace_started.v1", body))
        assert m.saga_id == "saga-2"
        assert m.grace_expires_at.ToJsonString() == "2026-07-20T08:00:00Z"
        assert m.started_at.ToJsonString() == "2026-06-20T08:00:00Z"


class TestClosurePseudonymisePerDomainComplete:
    def test_round_trip_maps_and_repeated(self) -> None:
        body = {
            "saga_id": "saga-3",
            "gcid": "gcid-3",
            "tenant_id": "tenant-3",
            "domain_record_counts": {"creation": 12, "consumption": 7},
            "acked_domains": ["creation", "consumption"],
            "completed_at": _T,
        }
        m = saga_pb2.ClosurePseudonymisePerDomainComplete()
        m.ParseFromString(_encode("chora.closure.pseudonymise_per_domain_complete.v1", body))
        assert m.saga_id == "saga-3"
        assert dict(m.domain_record_counts) == {"creation": 12, "consumption": 7}
        assert list(m.acked_domains) == ["creation", "consumption"]
        assert m.completed_at.ToJsonString() == "2026-06-20T08:00:00Z"


class TestClosureCryptoShredComplete:
    def test_round_trip(self) -> None:
        body = {
            "saga_id": "saga-4",
            "gcid": "gcid-4",
            "tenant_id": "tenant-4",
            "dek_id": "projects/p/locations/l/keyRings/k/cryptoKeys/c/cryptoKeyVersions/1",
            "kms_operation_id": "op-123",
            "executed_by_gcid": "system",
            "retention_days_by_jurisdiction": {"EU": 30, "SG": 90},
            "shredded_at": _T,
        }
        m = saga_pb2.ClosureCryptoShredComplete()
        m.ParseFromString(_encode("chora.closure.crypto_shred_complete.v1", body))
        assert m.saga_id == "saga-4"
        assert m.dek_id.endswith("cryptoKeyVersions/1")
        assert m.kms_operation_id == "op-123"
        assert m.executed_by_gcid == "system"
        assert dict(m.retention_days_by_jurisdiction) == {"EU": 30, "SG": 90}
        assert m.shredded_at.ToJsonString() == "2026-06-20T08:00:00Z"


class TestClosureClosed:
    def test_round_trip_enum_and_map(self) -> None:
        body = {
            "saga_id": "saga-5",
            "gcid": "gcid-5",
            "tenant_id": "tenant-5",
            "final_state": "crypto_shredded",
            "state_transitions": {
                "active": "2026-06-01T00:00:00+00:00",
                "crypto_shredded": _T,
            },
            "closed_at": _T,
        }
        m = saga_pb2.ClosureClosed()
        m.ParseFromString(_encode("chora.closure.closed.v1", body))
        assert m.saga_id == "saga-5"
        assert m.final_state == saga_pb2.ClosureSagaState.Value("CLOSURE_SAGA_STATE_CRYPTO_SHREDDED")
        assert dict(m.state_transitions)["crypto_shredded"] == _T
        assert m.closed_at.ToJsonString() == "2026-06-20T08:00:00Z"

    def test_unknown_state_maps_to_unspecified(self) -> None:
        body = {
            "saga_id": "saga-5b",
            "final_state": "pseudonymise_partial",
            "state_transitions": {},
            "closed_at": _T,
        }
        m = saga_pb2.ClosureClosed()
        m.ParseFromString(_encode("chora.closure.closed.v1", body))
        assert m.final_state == 0  # CLOSURE_SAGA_STATE_UNSPECIFIED

    def test_empty_state_maps_to_unspecified(self) -> None:
        body = {"saga_id": "saga-5c", "final_state": "", "closed_at": _T}
        m = saga_pb2.ClosureClosed()
        m.ParseFromString(_encode("chora.closure.closed.v1", body))
        assert m.final_state == 0


class TestClosureCancelled:
    def test_round_trip(self) -> None:
        body = {
            "saga_id": "saga-6",
            "gcid": "gcid-6",
            "tenant_id": "tenant-6",
            "cancelled_at": _T,
            "cancelled_by_gcid": "self",
        }
        m = saga_pb2.ClosureCancelled()
        m.ParseFromString(_encode("chora.closure.cancelled.v1", body))
        assert m.saga_id == "saga-6"
        assert m.cancelled_by_gcid == "self"
        assert m.cancelled_at.ToJsonString() == "2026-06-20T08:00:00Z"
        # reason is absent from the outbox body → proto default.
        assert m.reason == ""


class TestEdgeCases:
    def test_empty_payload_encodes_defaults(self) -> None:
        out = protomarshal.encode("chora.closure.requested.v1", b"", _envelope())
        assert out is not None
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.saga_id == ""
        # envelope still encoded from the dict.
        assert m.envelope.event_id == _envelope()["event_id"]

    def test_naive_timestamp_is_tolerated(self) -> None:
        """A timestamp without offset must not crash the encoder (it is
        skipped, leaving the proto field unset)."""
        body = {"saga_id": "s", "requested_at": "2026-06-20T08:00:00"}
        out = protomarshal.encode(
            "chora.closure.requested.v1",
            json.dumps(body).encode("utf-8"),
            _envelope(),
        )
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.saga_id == "s"
        # naive → either unset (skipped) or parsed; must not raise.
        assert isinstance(m.requested_at.seconds, int)

    def test_published_at_in_envelope_is_encoded(self) -> None:
        env = _envelope()
        env["published_at"] = "2026-06-20T08:00:01+00:00"
        out = protomarshal.encode("chora.closure.requested.v1", b"{}", env)
        m = saga_pb2.ClosureRequested()
        m.ParseFromString(out)
        assert m.envelope.published_at.ToJsonString() == "2026-06-20T08:00:01Z"
