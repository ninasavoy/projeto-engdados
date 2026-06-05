"""
pipeline/metrics.py
Métricas Prometheus expostas em :8000/metrics.
Importar e chamar start_metrics_server() uma vez por processo.
"""

import os
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from pipeline.logger import get_logger

log = get_logger("metrics")

# ── Contadores ────────────────────────────────────────────────────────────────
records_ingested = Counter(
    "prf_records_ingested_total",
    "Total de registros ingeridos no Bronze",
    ["source_year"],
)

records_processed = Counter(
    "prf_records_processed_total",
    "Total de registros processados no Silver",
)

records_loaded_gold = Counter(
    "prf_records_loaded_gold_total",
    "Total de registros carregados no Gold (fato_acidente)",
)

pipeline_errors = Counter(
    "prf_pipeline_errors_total",
    "Total de erros no pipeline",
    ["stage", "error_type"],
)

# ── Gauges ────────────────────────────────────────────────────────────────────
queue_depth = Gauge(
    "prf_rabbitmq_queue_depth",
    "Quantidade de mensagens pendentes na fila RabbitMQ",
)

last_run_timestamp = Gauge(
    "prf_pipeline_last_run_timestamp",
    "Unix timestamp da última execução bem-sucedida",
    ["stage"],
)

# ── Histogramas ───────────────────────────────────────────────────────────────
stage_duration_seconds = Histogram(
    "prf_stage_duration_seconds",
    "Duração de cada etapa do pipeline em segundos",
    ["stage"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)


def start_metrics_server() -> None:
    port = int(os.getenv("PROMETHEUS_PORT", 8000))
    start_http_server(port)
    log.info("metrics_server_started", port=port)