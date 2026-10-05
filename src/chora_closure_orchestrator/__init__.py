"""Chora Closure Orchestrator — Python LangGraph federated saga.

Per Tier 2 D5 hybrid kernel mandate (LangGraph is Python-only per
``feedback_ai_go_first`` memory). Ports the previous Go implementation
(see git history for ``services/chora-closure-orchestrator/`` Go contents
removed in S7.1 / CHO-1472) to Python while preserving:

- 5-state lifecycle ACTIVE → CLOSING → SUSPENDED → PSEUDONYMIZED →
  COLD_ARCHIVED → CRYPTO_SHREDDED (per ``ddd-enforcement.md``)
- AGID-rejection invariant (agents have no lifecycle)
- Federated ack gate before COLD_ARCHIVED
- Cancel-during-grace as ONE permitted backward transition
- Append-only history audit
- Pseudonymise + crypto-shred (NEVER hard-delete)

New in the Python port:
- LangGraph StateGraph orchestrating end-to-end pipeline
- 10 per-domain ``PII_Closure_Map.yaml`` files (1 per domain service)
- Master KEK + per-user DEK lifecycle via the local KMS adapter
  (``CHORA_LOCAL_KEK`` env; AES-256-GCM envelope encryption)
- MinIO cold-archive writer
- Canonical ``chora.closure.*`` topic namespace (per S0 reconciliation)
"""

__version__ = "0.2.0"
