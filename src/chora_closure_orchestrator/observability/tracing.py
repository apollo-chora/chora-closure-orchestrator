"""OTLP tracing bootstrap for the closure orchestrator.

Cloud-neutral observability: traces are exported via OTLP-gRPC to the
collector endpoint in ``OTEL_EXPORTER_OTLP_ENDPOINT`` (the local OTel
Collector container in the compose stack). When the endpoint is unset the
tracer is a no-op — tracing is never a boot dependency.

Saga-level spans carry ``saga_id``, ``tenant_id``, ``gcid``, and
``thread_id`` attributes for cross-tenant chaos triage.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_initialized = False


def init_tracing(
    service_name: str = "chora-closure-orchestrator",
    project_id: str | None = None,
) -> bool:
    """Initialise OTLP tracing. Returns True if a real exporter was
    wired; False if no-op (env not configured / packages missing).

    Selection logic:
    * If ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set → OTLP-gRPC exporter
      (points at the local collector).
    * Else → no-op.

    Idempotent — calling more than once is safe; only the first call
    wires the provider.
    """
    global _initialized
    if _initialized:
        return True

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("opentelemetry-sdk not installed; tracing disabled")
        return False

    exporter = None
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter()
            logger.info("tracing exporter: OTLP-gRPC", extra={"endpoint": otlp_endpoint})
        except ImportError:
            logger.warning("otlp exporter unavailable; falling through")

    if exporter is None:
        logger.info("tracing disabled — no exporter configured")
        return False

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": "chora.ai_kernel",
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _initialized = True
    return True


def get_tracer(name: str = "chora-closure-orchestrator"):
    """Return an OTel tracer. Safe to call even when init_tracing was a
    no-op — returns the default no-op tracer in that case."""
    try:
        from opentelemetry import trace as _trace

        return _trace.get_tracer(name)
    except ImportError:  # pragma: no cover
        # Stub tracer that swallows all calls
        class _NoOpSpan:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def set_attribute(self, *args, **kwargs):
                pass

            def set_attributes(self, *args, **kwargs):
                pass

        class _NoOpTracer:
            def start_as_current_span(self, *args, **kwargs):
                return _NoOpSpan()

        return _NoOpTracer()
