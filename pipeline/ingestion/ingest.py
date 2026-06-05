"""
pipeline/ingestion/ingest.py
Etapa BRONZE:
  1. Baixa CSVs da PRF (dados.prf.gov.br)
  2. Salva em data/bronze/
  3. Publica cada linha como mensagem JSON no RabbitMQ
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
import pika
import requests
from dotenv import load_dotenv

from pipeline.logger import get_logger
from pipeline.metrics import (
    pipeline_errors,
    records_ingested,
    stage_duration_seconds,
    last_run_timestamp,
    start_metrics_server,
)

load_dotenv()
log = get_logger("ingestion")

BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "./data/bronze"))
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# URLs dos datasets anuais da PRF (acidentes agrupados por ocorrência)
PRF_URLS = {
    "2021": "https://dados.prf.gov.br/api/download/v1/operacoes/ocorrencias/2021/csv",
    "2022": "https://dados.prf.gov.br/api/download/v1/operacoes/ocorrencias/2022/csv",
    "2023": "https://dados.prf.gov.br/api/download/v1/operacoes/ocorrencias/2023/csv",
}


# ── Download ──────────────────────────────────────────────────────────────────

def download_csv(year: str, url: str) -> Path:
    """Baixa o CSV de um ano e salva no Bronze. Retorna o path do arquivo."""
    dest = BRONZE_DIR / f"acidentes_{year}.csv"
    if dest.exists():
        log.info("csv_already_exists", year=year, path=str(dest))
        return dest

    log.info("downloading_csv", year=year, url=url)
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest.stat().st_size / 1_048_576
    log.info("csv_downloaded", year=year, path=str(dest), size_mb=round(size_mb, 2))
    return dest


# ── RabbitMQ ──────────────────────────────────────────────────────────────────

def get_rabbitmq_channel():
    """Cria conexão e canal RabbitMQ, declara a fila."""
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "prf"),
        os.getenv("RABBITMQ_PASSWORD", "prf123"),
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(
        queue=os.getenv("RABBITMQ_QUEUE", "prf_raw_data"),
        durable=True,           # sobrevive a restart do broker
    )
    return connection, channel


def publish_to_queue(channel, queue: str, records: list[dict], batch_size: int = 500) -> int:
    """
    Publica registros na fila em batches para não sobrecarregar o broker.
    Retorna total publicado.
    """
    total = 0
    batch = []

    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            _publish_batch(channel, queue, batch)
            total += len(batch)
            batch = []

    if batch:
        _publish_batch(channel, queue, batch)
        total += len(batch)

    return total


def _publish_batch(channel, queue: str, batch: list[dict]) -> None:
    message = json.dumps(batch, ensure_ascii=False, default=str)
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=message.encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent  # persiste em disco
        ),
    )


# ── Orquestração da ingestão ──────────────────────────────────────────────────

def run_ingestion(years: list[str] | None = None) -> None:
    """
    Ponto de entrada principal.
    Baixa CSVs dos anos solicitados e publica no RabbitMQ.
    """
    if years is None:
        years_env = os.getenv("PRF_YEARS", "2021,2022,2023")
        years = [y.strip() for y in years_env.split(",")]

    queue = os.getenv("RABBITMQ_QUEUE", "prf_raw_data")
    t0 = time.time()

    log.info("ingestion_started", years=years)

    try:
        connection, channel = get_rabbitmq_channel()
    except Exception as e:
        pipeline_errors.labels(stage="ingestion", error_type="rabbitmq_connect").inc()
        log.error("rabbitmq_connection_failed", error=str(e))
        raise

    total_published = 0

    for year in years:
        url = PRF_URLS.get(year)
        if not url:
            log.warning("year_not_configured", year=year)
            continue

        try:
            csv_path = download_csv(year, url)

            df = pd.read_csv(
                csv_path,
                encoding="latin-1",
                sep=";",
                on_bad_lines="skip",
                low_memory=False,
            )
            df["_source_year"] = year
            df["_source_file"] = csv_path.name

            records = df.to_dict(orient="records")
            published = publish_to_queue(channel, queue, records)

            records_ingested.labels(source_year=year).inc(published)
            total_published += published

            log.info("year_ingested", year=year, records=published)

        except Exception as e:
            pipeline_errors.labels(stage="ingestion", error_type=type(e).__name__).inc()
            log.error("ingestion_error", year=year, error=str(e))
            raise

    connection.close()

    elapsed = time.time() - t0
    stage_duration_seconds.labels(stage="ingestion").observe(elapsed)
    last_run_timestamp.labels(stage="ingestion").set_to_current_time()

    log.info(
        "ingestion_finished",
        total_published=total_published,
        elapsed_seconds=round(elapsed, 2),
    )


if __name__ == "__main__":
    start_metrics_server()
    run_ingestion()