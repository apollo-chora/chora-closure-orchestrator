"""NATS JetStream adapter for the closure orchestrator.

Components (per `feedback_d6_resilience_first_class` B.6.2):

* ``OutboxRow`` — DTO carrying one ``closure_outbox_events`` row from
  the store layer up to the dispatcher.
* ``OutboxStore`` — Protocol the dispatcher uses to fetch / mark
  pending rows (implementations: ``PostgresOutboxStore`` for live DB,
  ``InMemoryOutboxStore`` for tests).
* ``NatsPublisher`` — thin async wrapper around a NATS JetStream client
  (publishes payload + sets the envelope as message headers).
* ``OutboxDispatcher`` — coordinates fetch → publish → mark with
  retry + deadletter semantics.
"""

from chora_closure_orchestrator.adapter.pubsub.dispatcher import (
    OutboxDispatcher,
)
from chora_closure_orchestrator.adapter.pubsub.publisher import NatsPublisher
from chora_closure_orchestrator.adapter.pubsub.store import (
    InMemoryOutboxStore,
    OutboxRow,
    OutboxStore,
    PostgresOutboxStore,
)
from chora_closure_orchestrator.adapter.pubsub.subscriber import (
    AckAfterProcessingSubscriber,
    ClosureDLQHandler,
    CrewRunFailed,
    CrewRunFailedPublisher,
    HandlerFn,
    TransientError,
)

__all__ = [
    "AckAfterProcessingSubscriber",
    "ClosureDLQHandler",
    "CrewRunFailed",
    "CrewRunFailedPublisher",
    "HandlerFn",
    "InMemoryOutboxStore",
    "NatsPublisher",
    "OutboxDispatcher",
    "OutboxRow",
    "OutboxStore",
    "PostgresOutboxStore",
    "TransientError",
]
