"""FastAPI entrypoint for chora-closure-orchestrator.

Run::

    uvicorn chora_closure_orchestrator.main:app --host 0.0.0.0 --port 8080

Startup sequence:
    1. Initialise OTLP tracing (exporter when OTEL_EXPORTER_OTLP_ENDPOINT set).
    2. Build the FastAPI app + adapters from env vars (no inline config).

Per Tier 2 D5 hybrid kernel mandate: this is the Python LangGraph half
of the federated closure saga. Cloud-neutral: PostgreSQL for persistence,
NATS JetStream for events, MinIO for the cold archive, env-backed secrets.
"""

from __future__ import annotations

import logging
import os

from chora_closure_orchestrator.adapter.http.handlers import build_app_from_env
from chora_closure_orchestrator.observability.tracing import init_tracing

logger = logging.getLogger(__name__)

init_tracing()

app = build_app_from_env()


def run() -> None:
    """Console-script entrypoint."""
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")  # noqa: S104


if __name__ == "__main__":  # pragma: no cover
    run()
