"""Env-backed secret adapter for chora-closure-orchestrator.

Per `feedback_no_inline_config`: DSNs come from the environment (compose
injected; typically Docker secrets / .env). This adapter resolves the
env vars to DSNs — no credentials service involved.

The closure orchestrator's federated saga state lives in the
``chora_identity`` database (per .claude/rules/ddd-enforcement.md +
`account-closure-saga` skill). Per-domain pseudonymisation is triggered
via NATS events; this resolver only feeds the DSN to the LangGraph
PostgresSaver checkpointer init.

The resolver module stays import-cheap: no third-party imports at module
load time.
"""

from chora_closure_orchestrator.adapter.secrets.resolver import (
    resolve_ai_kernel_dsn,
    resolve_dsn,
)

__all__ = ["resolve_ai_kernel_dsn", "resolve_dsn"]
