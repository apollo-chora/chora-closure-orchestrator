"""``AckAfterProcessingSubscriber`` (B.6.2.b) and ``ClosureDLQHandler``
(B.6.2.d).

Per ``feedback_d6_resilience_first_class``: subscribers MUST ack only
after successfully processing + writing downstream state. Transient
failures NACK so the broker redelivers with exponential backoff; exhausted
retries route to the DLQ per the subscription config.

``ClosureDLQHandler`` is the orchestrator-side DLQ-awareness handler —
when a message lands on any ``chora.dlq.closure.*`` topic, this handler
emits ``chora.ai_kernel.crew.run_failed.v1`` carrying the replay
metadata (saga_id, original_topic, attempt count) operators need to
drive ``engine.resume(thread_id)`` after fixing the underlying issue.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class TransientError(Exception):
    """Raise from a subscriber handler to signal that the broker should
    redeliver. Equivalent in effect to any other exception (both NACK),
    but the distinction is preserved in logs so on-call can grep."""


HandlerFn = Callable[[dict[str, str], bytes], Awaitable[None]]


class AckAfterProcessingSubscriber:
    """Wraps a user handler with ack-after-processing semantics.

    Construction: pass an async handler ``f(envelope, payload)``. The
    wrapper's ``process_one(message)`` calls the handler, then:
    - ACK on success
    - NACK on any exception (the broker redelivers; the subscription's
      ``max_delivery_attempts`` decides when to DLQ)

    For test isolation, ``process_one`` is the canonical entry point
    and processes a single message. Production wires this into the NATS
    JetStream subscription callback via ``wiring.py``.
    """

    def __init__(self, *, handler: HandlerFn) -> None:
        self._handler = handler

    async def process_one(self, message: Any) -> None:
        envelope = {str(k): str(v) for k, v in message.attributes.items()}
        try:
            await self._handler(envelope, bytes(message.data))
        except TransientError as exc:
            logger.info(
                "subscriber_transient_nack",
                extra={
                    "tenant_id": envelope.get("tenant_id", ""),
                    "event_id": envelope.get("event_id", ""),
                    "saga_id": envelope.get("saga_id", ""),
                    "error": str(exc)[:300],
                },
            )
            await self._ack_nack(message, "nack")
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "subscriber_handler_failed_nack",
                extra={
                    "tenant_id": envelope.get("tenant_id", ""),
                    "event_id": envelope.get("event_id", ""),
                    "saga_id": envelope.get("saga_id", ""),
                    "error": str(exc)[:300],
                },
            )
            await self._ack_nack(message, "nack")
            return
        await self._ack_nack(message, "ack")

    @staticmethod
    async def _ack_nack(message: Any, which: str) -> None:
        """Ack/nack the message. NATS messages return a coroutine from
        ack()/nak(); Pub/Sub-shaped test doubles return None — await the
        result only when it is awaitable."""
        result = message.ack() if which == "ack" else message.nack()
        if inspect.isawaitable(result):
            await result


# ---------------------------------------------------------------------------
# B.6.2.d — DLQ-aware orchestrator handler
# ---------------------------------------------------------------------------


@dataclass
class CrewRunFailed:
    """Payload for ``chora.ai_kernel.crew.run_failed.v1``.

    Operators use this to drive replay via ``engine.resume(thread_id)``
    after fixing the underlying issue. The replay metadata MUST carry
    enough to identify which saga + original topic + attempt-count.
    """

    saga_id: str
    tenant_id: str
    gcid: str
    original_topic: str
    original_event_id: str
    delivery_attempt: int
    source_service: str
    failure_origin: str = "dlq"
    extras: dict[str, str] = field(default_factory=dict)


class CrewRunFailedPublisher(Protocol):
    """Port — the orchestrator wires this to publish to
    ``chora.ai_kernel.crew.run_failed.v1`` (via the same NatsPublisher
    used by the dispatcher)."""

    async def publish(self, e: CrewRunFailed) -> None: ...


class ClosureDLQHandler:
    """Consumes messages from closure DLQ topics + emits run-failed events.

    Per ``feedback_d6_resilience_first_class`` B.6.2.d. The handler
    extracts the saga_id + original_topic + attempt count from the
    DLQ envelope (the broker stamps these as ``CloudPubSubDeadLetter*``
    attributes when redelivering to a DLQ) and emits the run-failed
    event.

    The handler does NOT re-emit the original event back to the source
    topic — that would loop. Operators run a separate runbook to drive
    ``engine.resume(thread_id)`` once they've understood + fixed the
    failure root cause.
    """

    def __init__(
        self,
        *,
        run_failed_publisher: CrewRunFailedPublisher,
        source_service: str,
    ) -> None:
        if not source_service:
            raise ValueError("source_service required")
        self._publisher = run_failed_publisher
        self._source_service = source_service

    async def handle(self, envelope: dict[str, str], payload: bytes) -> None:
        event = CrewRunFailed(
            saga_id=envelope.get("saga_id", ""),
            tenant_id=envelope.get("tenant_id", ""),
            gcid=envelope.get("gcid", ""),
            original_topic=envelope.get("CloudPubSubDeadLetterSourceTopic", "unknown"),
            original_event_id=envelope.get("event_id", ""),
            delivery_attempt=int(envelope.get("CloudPubSubDeadLetterSourceDeliveryAttempt", "0") or "0"),
            source_service=self._source_service,
            extras={k: v for k, v in envelope.items() if k.startswith("CloudPubSubDeadLetter")},
        )
        await self._publisher.publish(event)
        logger.warning(
            "closure_dlq_run_failed_emitted",
            extra={
                "saga_id": event.saga_id,
                "tenant_id": event.tenant_id,
                "original_topic": event.original_topic,
                "delivery_attempt": event.delivery_attempt,
            },
        )


__all__ = [
    "AckAfterProcessingSubscriber",
    "ClosureDLQHandler",
    "CrewRunFailed",
    "CrewRunFailedPublisher",
    "HandlerFn",
    "TransientError",
]
