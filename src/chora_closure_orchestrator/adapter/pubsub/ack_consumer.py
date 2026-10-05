"""Per-domain pseudonymisation ack consumer.

Domain services emit ``chora.{domain}.account.pseudonymised.v1`` (the
reconciled canonical ack topic — CHO-1719 gap 4; matches the
asyncapi contract and the ``account-closure-saga`` skill taxonomy). This handler
records the ack on the Coordinator aggregate so the SagaDriver can take
the saga COLD_ARCHIVED → CRYPTO_SHREDDED once all 10 domains acked.

Wire shape (JSON, per the Go services' ``PseudonymiseCompletedPayload``)::

    {"saga_id": ..., "gcid": ..., "tenant_id": ...,
     "domain": "identity", "completed_at": "..."}

NACK semantics: unknown saga raises ``TransientError`` (the ack may have
raced the saga write — the broker redelivers, DLQ after max attempts).
Malformed payloads raise ``ValueError`` (poison — DLQ after retries).
"""

from __future__ import annotations

import datetime as _dt
import json

from chora_closure_orchestrator.adapter.pubsub.subscriber import TransientError
from chora_closure_orchestrator.adapter.repository.port import (
    CoordinatorRepository,
)
from chora_closure_orchestrator.domain.closure import ErrSagaNotFound

ACK_TOPIC_TEMPLATE = "chora.{domain}.account.pseudonymised.v1"


def ack_topic_for_domain(domain: str) -> str:
    """Return the canonical ack topic for ``domain``."""
    return ACK_TOPIC_TEMPLATE.format(domain=domain.strip().lower())


class DomainAckHandler:
    """Records one per-domain pseudonymisation ack on the saga."""

    def __init__(self, *, repo: CoordinatorRepository) -> None:
        self._repo = repo

    async def handle(self, envelope: dict[str, str], payload: bytes) -> None:
        """``HandlerFn``-shaped entry (AckAfterProcessingSubscriber)."""
        try:
            body = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"ack payload not JSON: {exc}") from exc

        saga_id = str(body.get("saga_id") or "").strip()
        domain = str(body.get("domain") or "").strip().lower()
        if not saga_id or not domain:
            raise ValueError("ack payload requires saga_id + domain")

        acked_at = _parse_ts(str(body.get("completed_at") or ""))

        try:
            c = await self._repo.get(saga_id)
        except ErrSagaNotFound as exc:
            raise TransientError(
                f"ack for unknown saga {saga_id} (domain={domain}) — redeliver in case the ack raced the saga write"
            ) from exc

        # record_domain_ack is idempotent (re-ack returns False) and
        # raises ErrUnknownDomain on a bad domain (poison → DLQ).
        c.record_domain_ack(domain, acked_at)
        await self._repo.save(c)


def _parse_ts(raw: str) -> _dt.datetime:
    if raw:
        try:
            return _dt.datetime.fromisoformat(raw)
        except ValueError:
            pass
    return _dt.datetime.now(_dt.UTC)


__all__ = ["ACK_TOPIC_TEMPLATE", "DomainAckHandler", "ack_topic_for_domain"]
