"""ClosureSagaOrchestratorState TypedDict + helpers.

Per Tier 2 D5 hybrid kernel mandate: this is the LangGraph orchestrator's
shared state across nodes. Fields mirror the Go ``Coordinator`` struct
plus per-node intermediate metadata (DEK resource name, archive URI,
trace + errors).

LangGraph merges partial dicts into the running state — node helpers must
read+append list-typed fields (``trace``, ``errors``, ``domains_acked``)
before returning.
"""

from __future__ import annotations

import os
import secrets
import time
from typing import Any, NotRequired, TypedDict


class ClosureSagaOrchestratorState(TypedDict):
    """Shared LangGraph state for a single closure saga run.

    The ``request_id`` is the run's correlation ID (UUIDv7); ``saga_id``
    is the persisted saga's identifier (filled in by ``request_node``).
    """

    request_id: str
    saga_id: str

    # Subject identity
    gcid: str
    tenant_id: str
    requested_by_gcid: str

    # Saga config
    current_state: str  # mirrors ``Coordinator.state.value``
    grace_period_seconds: int  # 30 in test mode; 30*86400 in prod
    jurisdiction: str  # SG / EU / US / ZA / BR
    reason: str

    # Federated coordination
    domains_acked: list[str]
    domain_record_counts: dict[str, int]

    # KMS + archive lifecycle
    dek_resource_name: str
    kms_operation_id: str
    archive_uri: str

    # Per-node intermediate state
    trace: list[dict[str, Any]]
    errors: list[str]

    # HITL interrupt + compensation flags
    cancel_requested: bool
    compensation_started: bool

    # Optional — set by tests / production timeout watchdog
    ack_timeout_exceeded: NotRequired[bool]


def new_request_id() -> str:
    """Generate a UUIDv7 string. Mirrors fog-orchestrator convention."""
    try:
        import uuid7 as _u7

        return str(_u7.uuid7())
    except Exception:  # pragma: no cover — exercised only without uuid7 lib
        ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
        rand_a = int.from_bytes(os.urandom(2), "big") & 0x0FFF
        rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)
        version = 7 << 12
        variant = 0b10 << 62
        hi = (ts_ms << 16) | version | rand_a
        lo = variant | rand_b
        full = (hi << 64) | lo
        full ^= secrets.randbits(64)
        full &= ~(0xF000 << 64)
        full |= 0x7000 << 64
        full &= ~(0b11 << 62)
        full |= 0b10 << 62

        hexs = f"{full:032x}"
        return f"{hexs[0:8]}-{hexs[8:12]}-{hexs[12:16]}-{hexs[16:20]}-{hexs[20:32]}"
