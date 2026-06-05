"""
pipeline/transform/transform.py
Etapa SILVER:
  1. Consome mensagens do RabbitMQ (batches de registros brutos)
  2. Limpa, tipifica e valida os dados
  3. Persiste na tabela silver.acidentes (PostgreSQL)
  4. Salva Parquet em data/silver/ como backup
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
import pika
from dotenv import load_dotenv

from pipeline.db import get_engine, get_psycopg2_conn
from pipeline.logger import get_logger
from pipeline.metrics import (
    pipeline_errors,
    records_processed,
    stage_duration_seconds,
    last_run_timestamp,
)

load_dotenv()
log = get_logger("transform")

SILVER_DIR = Path(os.getenv("SILVER_DIR", "./data/silver"))
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# Colunas esperadas do CSV da PRF (subset relevante)
SILVER_COLUMNS = [
    "data_inversa", "dia_semana", "horario", "uf", "br", "km",
    "municipio", "causa_acidente", "tipo_acidente", "classificacao_acidente",
    "fase_dia", "sentido_via", "condicao_metereologica", "tipo_pista",
    "tracado_via", "uso_solo", "pessoas", "mortos", "feridos_graves",
    "feridos_leves", "ilesos", "ignorados", "feridos", "veiculos",
    "latitude", "longitude", "regional", "delegacia", "uop",
]


# ── Limpeza e tipagem ─────────────────────────────────────────────────────────

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica limpeza e coerção de tipos num batch de registros.
    Retorna DataFrame pronto para silver.acidentes.
    """
    # Normaliza nomes de colunas
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    # Garante que todas as colunas existam (preenche com None se ausente)
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[SILVER_COLUMNS].copy()

    # Datas e horas
    df["data_inversa"] = pd.to_datetime(df["data_inversa"], errors="coerce").dt.date
    df["horario"] = pd.to_datetime(df["horario"], format="%H:%M:%S", errors="coerce").dt.time

    # Colunas numéricas
    int_cols = ["pessoas", "mortos", "feridos_graves", "feridos_leves",
                "ilesos", "ignorados", "feridos", "veiculos"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Coordenadas: trocar vírgula por ponto (padrão BR)
    for col in ["latitude", "longitude"]:
        df[col] = (
            df[col].astype(str)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    # KM: mesmo tratamento
    df["km"] = (
        df["km"].astype(str)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # UF: uppercase, 2 chars
    df["uf"] = df["uf"].astype(str).str.upper().str.strip().str[:2]

    # Strings: strip espaços, None em branco
    text_cols = [c for c in SILVER_COLUMNS if c not in int_cols + ["data_inversa", "horario",
                                                                     "latitude", "longitude", "km"]]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().replace({"nan": None, "": None, "None": None})

    # Remove linhas sem data (corrompidas)
    df = df[df["data_inversa"].notna()]

    return df


def insert_silver(df: pd.DataFrame) -> int:
    """Insere batch no PostgreSQL silver.acidentes. Retorna registros inseridos."""
    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    cols = SILVER_COLUMNS
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)

    sql = f"""
        INSERT INTO silver.acidentes ({col_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                sql,
                [[r.get(c) for c in cols] for r in records],
            )
    return len(records)


# ── Consumer RabbitMQ ─────────────────────────────────────────────────────────

def run_transform(max_messages: int = 0) -> None:
    """
    Consome mensagens do RabbitMQ até a fila esvaziar (ou max_messages).
    Cada mensagem é um batch (lista de dicts).
    """
    queue = os.getenv("RABBITMQ_QUEUE", "prf_raw_data")
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "prf"),
        os.getenv("RABBITMQ_PASSWORD", "prf123"),
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        credentials=credentials,
        heartbeat=600,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)

    total_processed = 0
    messages_consumed = 0
    all_dfs = []
    t0 = time.time()

    log.info("transform_started", queue=queue)

    while True:
        method_frame, _, body = channel.basic_get(queue=queue, auto_ack=False)
        if method_frame is None:
            log.info("queue_empty", consumed=messages_consumed)
            break

        try:
            raw = json.loads(body.decode("utf-8"))
            df = pd.DataFrame(raw)
            df_clean = clean_dataframe(df)
            inserted = insert_silver(df_clean)
            all_dfs.append(df_clean)

            records_processed.inc(inserted)
            total_processed += inserted
            messages_consumed += 1
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            pipeline_errors.labels(stage="transform", error_type=type(e).__name__).inc()
            log.error("transform_batch_error", error=str(e))
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

        if max_messages and messages_consumed >= max_messages:
            break

    connection.close()

    # Salva Parquet no Silver como backup/auditoria
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        parquet_path = SILVER_DIR / f"acidentes_{int(time.time())}.parquet"
        combined.to_parquet(parquet_path, index=False)
        log.info("silver_parquet_saved", path=str(parquet_path), rows=len(combined))

    elapsed = time.time() - t0
    stage_duration_seconds.labels(stage="transform").observe(elapsed)
    last_run_timestamp.labels(stage="transform").set_to_current_time()

    log.info(
        "transform_finished",
        total_processed=total_processed,
        elapsed_seconds=round(elapsed, 2),
    )


if __name__ == "__main__":
    run_transform()