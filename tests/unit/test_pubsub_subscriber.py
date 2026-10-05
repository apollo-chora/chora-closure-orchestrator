"""Tests for ``AckAfterProcessingSubscriber`` (B.6.2.b) and
``ClosureDLQHandler`` (B.6.2.d).

The wrapper codifies the ack-after-processing pattern that
``feedback_d6_resilience_first_class`` mandates:

1. Receive message
2. Run handler; if it succeeds, ACK.
3. If it raises ``TransientError``, NACK → the broker redelivers.
4. If it raises any other exception, log + NACK (the broker retries until
   ``max_delivery_attempts``, then routes to DLQ).

``ClosureDLQHandler`` uses the wrapper to consume the closure
orchestrator's OWN DLQ topics → emits
``chora.ai_kernel.crew.run_failed.v1`` with the replay metadata operators
need to drive ``engine.resume(thread_id)``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from chora_closure_orchestrator.adapter.pubsub import (
    AckAfterProcessingSubscriber,
    ClosureDLQHandler,
    TransientError,
)


class _MockMessage:
    def __init__(self, data: bytes, attributes: dict[str, str]) -> None:
        self.data = data
        self.attributes = attributes
        self.acked = False
        self.nacked = False

    def ack(self) -> None:
        self.acked = True

    def nack(self) -> None:
        self.nacked = True


class TestAckAfterProcessingHandler:
    @pytest.mark.asyncio
    async def test_successful_handler_acks(self) -> None:
        handler = AsyncMock(return_value=None)
        sub = AckAfterProcessingSubscriber(handler=handler)
        msg = _MockMessage(b'{"x":1}', {"tenant_id": "t-1"})

        await sub.process_one(msg)

        assert msg.acked is True
        assert msg.nacked is False
        handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_transient_error_nacks_for_redelivery(self) -> None:
        async def _handler(envelope: dict[str, str], payload: bytes) -> None:
            raise TransientError("db locked, retry me")

        sub = AckAfterProcessingSubscriber(handler=_handler)
        msg = _MockMessage(b'{"x":1}', {"tenant_id": "t-1"})

        await sub.process_one(msg)

        assert msg.acked is False
        assert msg.nacked is True

    @pytest.mark.asyncio
    async def test_terminal_error_also_nacks_eventually_dlq(self) -> None:
        """Non-transient errors NACK too — the broker retries until DLQ.
        The wrapper does NOT decide to send-to-DLQ; that's the broker's job
        based on max_delivery_attempts on the subscription."""

        async def _handler(envelope: dict[str, str], payload: bytes) -> None:
            raise RuntimeError("bug in handler")

        sub = AckAfterProcessingSubscriber(handler=_handler)
        msg = _MockMessage(b'{"x":1}', {"tenant_id": "t-1"})

        await sub.process_one(msg)

        assert msg.acked is False
        assert msg.nacked is True

    @pytest.mark.asyncio
    async def test_handler_receives_envelope_attributes_and_payload(self) -> None:
        captured: dict = {}

        async def _handler(envelope: dict[str, str], payload: bytes) -> None:
            captured["env"] = envelope
            captured["payload"] = payload

        sub = AckAfterProcessingSubscriber(handler=_handler)
        msg = _MockMessage(
            b'{"saga_id":"s-1"}',
            {"tenant_id": "t-1", "saga_id": "s-1", "event_id": "e-1"},
        )

        await sub.process_one(msg)

        assert captured["env"] == {
            "tenant_id": "t-1",
            "saga_id": "s-1",
            "event_id": "e-1",
        }
        assert captured["payload"] == b'{"saga_id":"s-1"}'

    @pytest.mark.asyncio
    async def test_concurrent_multi_tenant_messages_isolated(self) -> None:
        """D6.3 prerequisite: subscriber wrapper does NOT mix state
        across tenants. Each message gets its own handler call with its
        own envelope; ack/nack is per-message."""
        seen_tenants: list[str] = []

        async def _handler(envelope: dict[str, str], payload: bytes) -> None:
            seen_tenants.append(envelope["tenant_id"])

        sub = AckAfterProcessingSubscriber(handler=_handler)
        m1 = _MockMessage(b"", {"tenant_id": "tenant-A"})
        m2 = _MockMessage(b"", {"tenant_id": "tenant-B"})
        m3 = _MockMessage(b"", {"tenant_id": "tenant-A"})

        await sub.process_one(m1)
        await sub.process_one(m2)
        await sub.process_one(m3)

        assert seen_tenants == ["tenant-A", "tenant-B", "tenant-A"]
        assert m1.acked and m2.acked and m3.acked


# ---------------------------------------------------------------------------
# B.6.2.d — Orchestrator-side DLQ awareness
# ---------------------------------------------------------------------------


class TestClosureDLQHandler:
    @pytest.mark.asyncio
    async def test_dlq_message_emits_run_failed(self) -> None:
        """A message on the closure DLQ topic triggers an emission of
        chora.ai_kernel.crew.run_failed.v1 with replay metadata."""
        run_failed_publisher = AsyncMock()
        handler = ClosureDLQHandler(
            run_failed_publisher=run_failed_publisher,
            source_service="chora-closure-orchestrator",
        )

        envelope = {
            "tenant_id": "01970000-0000-7000-9b01-000000000001",
            "saga_id": "saga-d62d-1",
            "event_id": "01970000-7777-7000-a000-000000000001",
            "source_service": "chora-closure-orchestrator",
            "occurred_at": "2026-05-12T08:00:00+00:00",
        }
        payload = b'{"saga_id":"saga-d62d-1"}'
        # Mimic the broker's DLQ attributes — the original delivery attempt
        # count is exposed via the CloudPubSubDeadLetterSourceDeliveryAttempt
        # attribute.
        dlq_attributes = {
            **envelope,
            "CloudPubSubDeadLetterSourceDeliveryAttempt": "5",
            "CloudPubSubDeadLetterSourceTopicPublishTime": "2026-05-12T08:00:01+00:00",
            "CloudPubSubDeadLetterSourceTopic": "chora.closure.requested.v1",
        }

        await handler.handle(dlq_attributes, payload)

        run_failed_publisher.publish.assert_called_once()
        call_arg = run_failed_publisher.publish.call_args.args[0]
        # The published event's payload carries the replay metadata
        assert "saga-d62d-1" in str(call_arg)

    @pytest.mark.asyncio
    async def test_dlq_handler_acks_after_emission(self) -> None:
        """End-to-end: subscriber wrapper invokes the DLQ handler, which
        invokes the run-failed publisher, then acks the DLQ message."""
        run_failed_publisher = AsyncMock()
        dlq_handler = ClosureDLQHandler(
            run_failed_publisher=run_failed_publisher,
            source_service="chora-closure-orchestrator",
        )
        sub = AckAfterProcessingSubscriber(handler=dlq_handler.handle)

        msg = _MockMessage(
            b'{"saga_id":"saga-d62d-2"}',
            {
                "tenant_id": "01970000-0000-7000-9b02-000000000001",
                "saga_id": "saga-d62d-2",
                "CloudPubSubDeadLetterSourceDeliveryAttempt": "5",
                "CloudPubSubDeadLetterSourceTopic": "chora.closure.grace_started.v1",
            },
        )
        await sub.process_one(msg)

        assert msg.acked is True
        run_failed_publisher.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_failed_carries_tenant_id_for_isolation(self) -> None:
        """D6.3 multi-tenant: run-failed events MUST carry tenant_id so
        ops dashboards can scope alerts per-tenant."""
        run_failed_publisher = AsyncMock()
        handler = ClosureDLQHandler(
            run_failed_publisher=run_failed_publisher,
            source_service="chora-closure-orchestrator",
        )
        await handler.handle(
            {
                "tenant_id": "tenant-A-special",
                "saga_id": "saga-A",
                "CloudPubSubDeadLetterSourceTopic": "chora.closure.requested.v1",
            },
            b"{}",
        )

        emitted = run_failed_publisher.publish.call_args.args[0]
        assert emitted.tenant_id == "tenant-A-special"
        assert emitted.saga_id == "saga-A"
        assert emitted.original_topic == "chora.closure.requested.v1"
