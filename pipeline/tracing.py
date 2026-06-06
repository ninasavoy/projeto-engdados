"""
pipeline/tracing.py
Tracing distribuído com OpenTelemetry → Jaeger (rubrica A#1).

init_tracing() é idempotente e configura:
  - TracerProvider com OTLP exporter (endpoint via OTEL_EXPORTER_OTLP_ENDPOINT)
  - auto-instrumentação de requests (download) e SQLAlchemy (DB)

A propagação do contexto entre serviços desacoplados é feita manualmente pelo
RabbitMQ: inject_context() injeta o trace nos headers da mensagem na ingestão e
extract_context() o recupera na transformação — assim a trace conecta
`ingestion → fila → transform`, mostrando a latência cruzando a fila.

Sem OTEL_EXPORTER_OTLP_ENDPOINT, tudo vira no-op (tracing desligado) — o pipeline
roda normalmente sem Jaeger.
"""

import os

from opentelemetry import trace
from opentelemetry.propagate import inject, extract
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from pipeline.logger import get_logger

log = get_logger("tracing")

_provider: TracerProvider | None = None
_initialized = False


def init_tracing(service_name: str):
    """Configura o tracing uma única vez por processo. Retorna um tracer."""
    global _provider, _initialized
    if _initialized:
        return trace.get_tracer(service_name)
    _initialized = True

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        log.info("tracing_disabled", reason="no OTEL_EXPORTER_OTLP_ENDPOINT")
        return trace.get_tracer(service_name)

    resource = Resource.create({"service.name": service_name})
    _provider = TracerProvider(resource=resource)
    _provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(_provider)

    # Auto-instrumentação (cada uma é opcional / tolerante a falha).
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001
        log.warning("requests_instrumentation_failed", error=str(e))
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001
        log.warning("sqlalchemy_instrumentation_failed", error=str(e))

    log.info("tracing_initialized", service=service_name, endpoint=endpoint)
    return trace.get_tracer(service_name)


def get_tracer(service_name: str):
    return trace.get_tracer(service_name)


def shutdown_tracing() -> None:
    """Garante o flush dos spans antes de um processo de vida curta terminar."""
    if _provider is not None:
        try:
            _provider.force_flush()
        except Exception:  # noqa: BLE001
            pass


# ── Propagação de contexto via RabbitMQ (headers da mensagem) ─────────────────

def inject_context(carrier: dict | None = None) -> dict:
    """Injeta o contexto de trace atual num dict (vira headers AMQP)."""
    carrier = carrier or {}
    inject(carrier)
    return carrier


def extract_context(carrier: dict | None):
    """Recupera o contexto de trace a partir dos headers da mensagem."""
    return extract(carrier or {})
