"""NatsPublisher — thin async wrapper around a NATS JetStream client.

The local, cloud-neutral message bus that replaces Google Cloud Pub/Sub.
The outbox dispatcher publishes each ``OutboxRow`` to its topic (the NATS
subject) with the event envelope as message HEADERS, so subscribers can
filter on ``tenant_id``, ``traceparent``, etc. without parsing the payload
(per the cross-cutting envelope rule + the D6.3 multi-tenant chaos
directive).

The envelope dict is encoded as headers so the chora-go-common
``envelopeFromAttributes()`` contract is preserved on the wire: subscribers
HARD-REQUIRE parseable ``occurred_at`` + ``published_at`` and route on the
``topic`` header. ``published_at`` is stamped at publish time (this is the
publish instant).

The 6 ``chora.closure.*`` lifecycle topics are published as canonical
Protobuf binary (``protomarshal.encode``); the schemaless topics (the 10
per-domain fan-out/ack + ``chora.ai_kernel.agent.terminated.v1``) ship the
JSON body verbatim.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from chora_closure_orchestrator.adapter.pubsub import protomarshal
from chora_closure_orchestrator.adapter.pubsub.store import OutboxRow

# The envelope dict is snake_case; the Go eventbus
# (chora-common/eventbus `envelopeFromHeaders`) reads canonical `Chora-*`
# header names and dead-letters a message whose envelope it cannot rebuild,
# before the handler ever runs. Map to the canonical names on the wire; keys
# absent from this map are forwarded verbatim. The internal dict keeps its
# snake_case keys because protomarshal.encode reads them.
_ENVELOPE_HEADER_NAMES = {
    "event_id": "Chora-Event-Id",
    "idempotency_key": "Chora-Idempotency-Key",
    "tenant_id": "Chora-Tenant-Id",
    "gcid": "Chora-Gcid",
    "source_service": "Chora-Source-Service",
    "source_project": "Chora-Source-Project",
    "correlation_id": "Chora-Correlation-Id",
    "causation_id": "Chora-Causation-Id",
    "chora_imda_dimension": "Chora-Imda-Dimension",
    "imda_lifecycle_stage": "Chora-Imda-Lifecycle-Stage",
    "schema_version": "Chora-Schema-Version",
    "occurred_at": "Chora-Occurred-At",
    "published_at": "Chora-Published-At",
    "traceparent": "Traceparent",
    "tracestate": "Tracestate",
}


class NatsPublisher:
    """Publishes a single ``OutboxRow`` to its topic via NATS JetStream.

    ``client`` is a connected ``nats.aio.client.Client`` or its JetStream
    context (``nc.jetstream()``); both expose ``await publish(subject,
    payload, headers=...)``.
    """

    def __init__(self, *, client: Any) -> None:
        self._client = client

    async def publish(self, row: OutboxRow) -> str:
        """Publish the row's payload to its topic. Returns a message id
        (``{stream}-{seq}``) on success; propagates exceptions on failure
        so the dispatcher can drive retry/deadletter logic."""
        attributes = {str(k): str(v) for k, v in row.envelope.items()}
        # Envelope contract: subscribers HARD-REQUIRE parseable occurred_at
        # + published_at and route on the "topic" attribute. published_at is
        # stamped at publish time (this is the publish instant).
        if not attributes.get("published_at"):
            attributes["published_at"] = _dt.datetime.now(_dt.UTC).isoformat()
        attributes.setdefault("topic", row.topic)

        # The 6 chora.closure.* lifecycle topics are published as canonical
        # Protobuf binary; schemaless topics (the 10 per-domain fan-out/ack
        # + agent.terminated) return None and ship the JSON verbatim.
        data = protomarshal.encode(row.topic, row.payload, attributes)
        if data is None:
            data = row.payload

        ack = await self._client.publish(
            row.topic,
            data,
            headers={_ENVELOPE_HEADER_NAMES.get(k, k): v for k, v in attributes.items()},
        )
        return f"{ack.stream}-{ack.seq}"


__all__ = ["NatsPublisher"]
