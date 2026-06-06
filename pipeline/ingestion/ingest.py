"""
pipeline/ingestion/ingest.py
Etapa BRONZE:
  1. Lê os CSVs do datatran (PRF) em data/bronze/acidentes_<ano>.csv
     (ou baixa, se PRF_URL_<ano> estiver configurada).
  2. Persiste o dado bruto em bronze.acidentes_raw (JSONB) — landing/auditoria.
  3. Publica os registros em batches JSON no RabbitMQ para a transformação.
"""

import json
import os
import shutil
import time
from pathlib import Path

import pandas as pd
import pika
import requests
from dotenv import load_dotenv

from pipeline.db import get_psycopg2_conn
from pipeline.logger import get_logger
from pipeline.metrics import (
    pipeline_errors,
    records_ingested,
    stage_duration_seconds,
    last_run_timestamp,
    push_metrics,
    start_metrics_server,
)
from pipeline.tracing import init_tracing, inject_context, shutdown_tracing

load_dotenv()
log = get_logger("ingestion")

BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "./data/bronze"))
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# URLs dos datasets anuais da PRF (acidentes agrupados por ocorrência).
# A PRF distribui os arquivos ZIPADOS via Google Drive (links na página oficial:
# https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes),
# portanto NÃO há uma URL de API estável. Configure as URLs reais por env se quiser
# download automático (ex.: PRF_URL_2023=https://...). Sem URL e sem arquivo local,
# a ingestão usa a amostra de fallback (data/sample/).
#
# Alternativa recomendada (mais robusta): baixe os CSVs manualmente e coloque em
#   data/bronze/acidentes_2021.csv, acidentes_2022.csv, acidentes_2023.csv
# A ingestão detecta e usa esses arquivos automaticamente (sem precisar de URL).
PRF_URLS = {
    "2021": os.getenv("PRF_URL_2021", ""),
    "2022": os.getenv("PRF_URL_2022", ""),
    "2023": os.getenv("PRF_URL_2023", ""),
}

# Header de browser — o portal da PRF rejeita user-agents "não-browser" (403)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/octet-stream,*/*",
}

DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", 3))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))


# ── Download ──────────────────────────────────────────────────────────────────

def download_csv(year: str, url: str) -> Path:
    """
    Baixa o CSV de um ano e salva no Bronze. Retorna o path do arquivo.
    Tenta DOWNLOAD_RETRIES vezes com backoff. Lança exceção se todas falharem.
    """
    dest = BRONZE_DIR / f"acidentes_{year}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        log.info("csv_already_exists", year=year, path=str(dest))
        return dest

    if not url:
        # Sem arquivo local e sem URL configurada → deixa o chamador cair no fallback.
        raise RuntimeError(
            f"Sem CSV local ({dest}) e sem URL configurada para {year} "
            f"(defina PRF_URL_{year} ou coloque o arquivo em data/bronze/)."
        )

    last_error = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            log.info("downloading_csv", year=year, url=url, attempt=attempt)
            response = requests.get(
                url, timeout=DOWNLOAD_TIMEOUT, stream=True, headers=BROWSER_HEADERS
            )
            response.raise_for_status()

            with open(dest, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_mb = dest.stat().st_size / 1_048_576
            log.info("csv_downloaded", year=year, path=str(dest), size_mb=round(size_mb, 2))
            return dest

        except Exception as e:  # noqa: BLE001 — queremos capturar qualquer falha de rede
            last_error = e
            wait = 2 ** attempt
            log.warning("download_attempt_failed", year=year, attempt=attempt,
                        error=str(e), retry_in_s=wait)
            time.sleep(wait)

    raise RuntimeError(f"Falha ao baixar CSV {year} após {DOWNLOAD_RETRIES} tentativas: {last_error}")


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
    # Injeta o contexto de trace nos headers → a transformação continua a mesma
    # trace ao consumir (tracing distribuído cruzando a fila).
    headers = inject_context({})
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=message.encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # persiste em disco
            headers=headers,
        ),
    )


# ── Bronze (camada raw) ───────────────────────────────────────────────────────

def persist_bronze(records: list[dict], source_file: str) -> int:
    """Persiste os registros BRUTOS (sem transformação) em bronze.acidentes_raw.

    É a camada de landing/auditoria da arquitetura Medallion: cada linha vira um
    JSONB exatamente como chegou do CSV. Append-only por natureza.
    """
    if not records:
        return 0
    rows = [
        (json.dumps(r, ensure_ascii=False, default=str), source_file)
        for r in records
    ]
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO bronze.acidentes_raw (raw_data, source_file) "
                "VALUES (%s::jsonb, %s)",
                rows,
            )
    return len(rows)


# ── Leitura e publicação de um CSV ────────────────────────────────────────────

def _process_csv(channel, queue: str, csv_path: Path, source: str,
                 max_records: int | None) -> int:
    """Lê um CSV, persiste o bruto no Bronze e publica na fila. Retorna total publicado."""
    df = pd.read_csv(
        csv_path,
        encoding="latin-1",
        sep=";",
        on_bad_lines="skip",
        low_memory=False,
    )
    if max_records:
        df = df.head(max_records)

    # NaN -> None para gerar JSONB válido (Postgres rejeita NaN em jsonb).
    df = df.astype(object).where(df.notna(), None)
    df["_source_year"] = source
    df["_source_file"] = csv_path.name

    # Garante o arquivo bruto na landing zone em disco (data/bronze/).
    if csv_path.parent.resolve() != BRONZE_DIR.resolve():
        dest = BRONZE_DIR / csv_path.name
        shutil.copyfile(csv_path, dest)
        log.info("bronze_file_landed", file=str(dest))

    records = df.to_dict(orient="records")

    # Bronze (tabela): dado bruto em JSONB.
    bronze_n = persist_bronze(records, csv_path.name)
    log.info("bronze_persisted", source=source, file=csv_path.name, rows=bronze_n)

    # Publica na fila para a transformação (Silver).
    published = publish_to_queue(channel, queue, records)
    log.info("csv_published", source=source, file=csv_path.name, records=published)
    return published


# ── Orquestração da ingestão ──────────────────────────────────────────────────

def run_ingestion(years: list[str] | None = None, max_records: int | None = None) -> None:
    """
    Ponto de entrada principal.
    Lê os CSVs do datatran dos anos solicitados (data/bronze/) e publica no RabbitMQ.
    Lança erro se nenhum CSV for encontrado (sem fallback).

    max_records: limita registros por arquivo (útil para demo rápida).
                 Default vem de PRF_MAX_RECORDS (0 = sem limite).
    """
    if years is None:
        years_env = os.getenv("PRF_YEARS", "2021,2022,2023")
        years = [y.strip() for y in years_env.split(",")]

    if max_records is None:
        max_records = int(os.getenv("PRF_MAX_RECORDS", 0)) or None

    queue = os.getenv("RABBITMQ_QUEUE", "prf_raw_data")
    t0 = time.time()
    tracer = init_tracing("prf-ingestion")

    log.info("ingestion_started", years=years, max_records=max_records)

    with tracer.start_as_current_span("ingestion") as span:
        span.set_attribute("prf.years", ",".join(years))

        try:
            connection, channel = get_rabbitmq_channel()
        except Exception as e:
            pipeline_errors.labels(stage="ingestion", error_type="rabbitmq_connect").inc()
            log.error("rabbitmq_connection_failed", error=str(e))
            raise

        total_published = 0
        downloaded_any = False

        for year in years:
            url = PRF_URLS.get(year)
            try:
                # Usa data/bronze/acidentes_{year}.csv se existir; senão baixa (se houver URL).
                csv_path = download_csv(year, url)
            except Exception as e:  # noqa: BLE001
                pipeline_errors.labels(stage="ingestion", error_type="download_failed").inc()
                log.warning("download_failed", year=year, error=str(e))
                continue

            downloaded_any = True
            published = _process_csv(channel, queue, csv_path, source=year, max_records=max_records)
            records_ingested.labels(source_year=year).inc(published)
            total_published += published

        if not downloaded_any:
            connection.close()
            raise FileNotFoundError(
                "Nenhum CSV da PRF encontrado para os anos "
                f"{years}. Baixe o datatran em "
                "https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes "
                "e coloque em data/bronze/acidentes_<ano>.csv (ou configure PRF_URL_<ano>)."
            )

        connection.close()
        span.set_attribute("prf.records_published", total_published)

    elapsed = time.time() - t0
    stage_duration_seconds.labels(stage="ingestion").observe(elapsed)
    last_run_timestamp.labels(stage="ingestion").set_to_current_time()
    push_metrics("ingestion")
    shutdown_tracing()

    log.info(
        "ingestion_finished",
        total_published=total_published,
        elapsed_seconds=round(elapsed, 2),
    )


if __name__ == "__main__":
    start_metrics_server()
    run_ingestion()
