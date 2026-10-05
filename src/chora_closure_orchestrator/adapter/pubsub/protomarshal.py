"""protomarshal — re-encode closure-lifecycle outbox bodies to Protobuf binary.

D4 (AUTH Phase-A debt pass). The 6 ``chora.closure.*`` lifecycle topics
are published on the NATS event bus as canonical Protobuf binary (the
flat ``chora-contracts/proto/events-flat/closure/saga/*.proto`` copies).
The orchestrator's transactional outbox stores each event body as JSON,
and the publisher historically shipped that JSON verbatim as the message
``data`` — which the binary-bound topics reject, leaving the audit-trail
row ``failed`` in ``closure_outbox_events``.

This module maps ``topic → proto encoder`` (Tier 2 D8 = Protobuf),
following the chora-notifications ``protomarshal`` precedent. At publish
time the ``NatsPublisher`` calls :func:`encode`; for a bound topic it
returns canonical Protobuf binary, otherwise ``None`` (the caller ships the
JSON body verbatim).

The 10 per-domain ``chora.{domain}.pii.pseudonymise.requested.v1`` fan-out/ack
topics + ``chora.ai_kernel.agent.terminated.v1`` are schemaless JSON BY DESIGN
— :func:`encode` returns ``None`` for them.

Wire-identity: the generated ``saga_pb2`` messages embed
``chora.common.v1.EventEnvelope`` (field 1) + ``google.protobuf.Timestamp``;
the registered flat schema inlines those as nested ``Envelope`` / ``Timestamp``
messages with IDENTICAL field numbers + wire types. Protobuf wire format
depends only on field number + type, so bytes produced here are byte-for-byte
parseable by the registered flat schema.

Graceful degradation: if ``chora_contracts_gen`` is not importable (e.g. a
local checkout without the contracts wheel), :func:`encode` returns ``None``
and logs once — the publisher falls back to JSON (the pre-D4 behaviour) rather
than crashing. Production images install ``chora-contracts`` so the binary
path is always taken.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

try:
    from chora_contracts_gen.events.closure import saga_pb2

    _AVAILABLE = True
except Exception as exc:  # pragma: no cover - exercised only when contracts absent
    saga_pb2 = None
    _AVAILABLE = False
    logger.warning(
        "protomarshal_contracts_unavailable",
        extra={"error": str(exc)[:200]},
    )


CLOSURE_LIFECYCLE_TOPICS = frozenset(
    {
        "chora.closure.requested.v1",
        "chora.closure.grace_started.v1",
        "chora.closure.pseudonymise_per_domain_complete.v1",
        "chora.closure.crypto_shred_complete.v1",
        "chora.closure.closed.v1",
        "chora.closure.cancelled.v1",
    }
)


# --------------------------------------------------------------------------
# Field helpers
# --------------------------------------------------------------------------


def _set_ts(field: Any, iso: str | None) -> None:
    """Parse an RFC 3339 / ``datetime.isoformat()`` string into the proto
    Timestamp ``field``. A blank or offset-less value is tolerated (skipped)
    so a malformed body never sinks the whole publish."""
    if not iso:
        return
    try:
        field.FromJsonString(iso)
    except Exception:  # noqa: BLE001 - tolerate any unparseable instant
        logger.debug("protomarshal_unparseable_timestamp", extra={"value": iso[:64]})


def _build_envelope(env: dict[str, str]) -> Any:
    """Map the outbox envelope dict (all string values) onto the canonical
    ``chora.common.v1.EventEnvelope``. ``schema_version`` is coerced str→int;
    timestamps are parsed; absent keys leave proto defaults."""
    e = saga_pb2.ClosureRequested().envelope.__class__()  # EventEnvelope
    e.event_id = env.get("event_id", "")
    e.idempotency_key = env.get("idempotency_key", "")
    e.tenant_id = env.get("tenant_id", "")
    e.gcid = env.get("gcid", "")
    _set_ts(e.occurred_at, env.get("occurred_at"))
    _set_ts(e.published_at, env.get("published_at"))
    e.traceparent = env.get("traceparent", "")
    e.tracestate = env.get("tracestate", "")
    e.source_project = env.get("source_project", "")
    e.source_service = env.get("source_service", "")
    sv = env.get("schema_version", "")
    if sv:
        try:
            e.schema_version = int(sv)
        except (TypeError, ValueError):
            logger.debug("protomarshal_bad_schema_version", extra={"value": sv})
    # Forward-compatible optional envelope fields (absent from the current
    # outbox writer but defined on the proto).
    for opt in ("correlation_id", "causation_id", "chora_imda_dimension", "imda_lifecycle_stage"):
        val = env.get(opt)
        if val and hasattr(e, opt):
            setattr(e, opt, val)
    return e


def _state_to_enum(value: str | None) -> int:
    """Map a domain ``State`` value (snake_case, e.g. ``crypto_shredded``) to
    the ``ClosureSagaState`` wire enum. Unknown/blank → UNSPECIFIED (0). The
    domain-only ``pseudonymise_partial`` has no wire enum and falls here."""
    if not value:
        return 0
    try:
        return int(saga_pb2.ClosureSagaState.Value(f"CLOSURE_SAGA_STATE_{value.upper()}"))
    except ValueError:
        return 0


# --------------------------------------------------------------------------
# Per-topic encoders (body dict + envelope dict → proto message)
# --------------------------------------------------------------------------


def _encode_requested(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosureRequested()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    m.grace_period_days = int(body.get("grace_period_days") or 0)
    m.requested_by_gcid = body.get("requested_by_gcid", "")
    m.reason = body.get("reason", "")
    m.participating_domains.extend(body.get("participating_domains") or [])
    _set_ts(m.requested_at, body.get("requested_at"))
    return m


def _encode_grace_started(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosureGraceStarted()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    _set_ts(m.grace_expires_at, body.get("grace_expires_at"))
    _set_ts(m.started_at, body.get("started_at"))
    return m


def _encode_pseudonymise_complete(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosurePseudonymisePerDomainComplete()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    for k, v in (body.get("domain_record_counts") or {}).items():
        m.domain_record_counts[k] = int(v)
    m.acked_domains.extend(body.get("acked_domains") or [])
    _set_ts(m.completed_at, body.get("completed_at"))
    return m


def _encode_crypto_shred_complete(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosureCryptoShredComplete()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    m.dek_id = body.get("dek_id", "")
    m.kms_operation_id = body.get("kms_operation_id", "")
    m.executed_by_gcid = body.get("executed_by_gcid", "")
    for k, v in (body.get("retention_days_by_jurisdiction") or {}).items():
        m.retention_days_by_jurisdiction[k] = int(v)
    _set_ts(m.shredded_at, body.get("shredded_at"))
    return m


def _encode_closed(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosureClosed()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    m.final_state = _state_to_enum(body.get("final_state"))
    for k, v in (body.get("state_transitions") or {}).items():
        m.state_transitions[k] = str(v)
    _set_ts(m.closed_at, body.get("closed_at"))
    return m


def _encode_cancelled(body: dict[str, Any], env: dict[str, str]) -> Any:
    m = saga_pb2.ClosureCancelled()
    m.envelope.CopyFrom(_build_envelope(env))
    m.saga_id = body.get("saga_id", "")
    m.gcid = body.get("gcid", "")
    m.tenant_id = body.get("tenant_id", "")
    m.cancelled_by_gcid = body.get("cancelled_by_gcid", "")
    m.reason = body.get("reason", "")
    _set_ts(m.cancelled_at, body.get("cancelled_at"))
    return m


_ENCODERS: dict[str, Callable[[dict[str, Any], dict[str, str]], Any]] = {
    "chora.closure.requested.v1": _encode_requested,
    "chora.closure.grace_started.v1": _encode_grace_started,
    "chora.closure.pseudonymise_per_domain_complete.v1": (_encode_pseudonymise_complete),
    "chora.closure.crypto_shred_complete.v1": _encode_crypto_shred_complete,
    "chora.closure.closed.v1": _encode_closed,
    "chora.closure.cancelled.v1": _encode_cancelled,
}


def encode(topic: str, payload: bytes, envelope: dict[str, str]) -> bytes | None:
    """Return canonical Protobuf binary for a schema-bound closure lifecycle
    topic, or ``None`` when the topic is schemaless JSON (caller publishes the
    payload verbatim).

    ``payload`` is the outbox JSON body bytes; ``envelope`` is the outbox
    envelope dict (all string values). Returns ``None`` (with a one-time
    warning at import) if ``chora_contracts_gen`` is unavailable.
    """
    encoder = _ENCODERS.get(topic)
    if encoder is None:
        return None
    if not _AVAILABLE:  # pragma: no cover - contracts always present in venv/image
        return None
    body: dict[str, Any] = json.loads(payload.decode("utf-8")) if payload else {}
    msg = encoder(body, envelope)
    data: bytes = msg.SerializeToString()
    return data


__all__ = ["encode", "CLOSURE_LIFECYCLE_TOPICS"]
