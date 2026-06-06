"""
pipeline/load/load.py
Etapa GOLD:
  Lê silver.acidentes → popula dimensões → carrega fato_acidente
  Ao final, atualiza as views materializadas.
"""

import time

import pandas as pd
from sqlalchemy import text

from pipeline.db import get_engine, refresh_materialized_views
from pipeline.logger import get_logger
from pipeline.metrics import (
    pipeline_errors,
    records_loaded_gold,
    stage_duration_seconds,
    last_run_timestamp,
    push_metrics,
)
from pipeline.tracing import init_tracing, shutdown_tracing

log = get_logger("load")


# ── Helpers de upsert ─────────────────────────────────────────────────────────

def upsert_dim_tempo(engine, dates: pd.Series) -> dict:
    """Insere datas únicas em dim_tempo. Retorna {date: sk_tempo}."""
    unique_dates = dates.dropna().unique()
    rows = []
    for d in unique_dates:
        dt = pd.Timestamp(d)
        rows.append({
            "data_completa": d,
            "ano": dt.year,
            "trimestre": dt.quarter,
            "mes": dt.month,
            "nome_mes": dt.strftime("%B"),
            "semana_ano": dt.isocalendar().week,
            "dia_mes": dt.day,
            "dia_semana_num": dt.dayofweek,
            "dia_semana_nome": dt.strftime("%A"),
            "fim_de_semana": dt.dayofweek >= 5,
        })

    if not rows:
        return {}

    df = pd.DataFrame(rows)
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO gold.dim_tempo
                (data_completa, ano, trimestre, mes, nome_mes,
                 semana_ano, dia_mes, dia_semana_num, dia_semana_nome, fim_de_semana)
            SELECT
                d.data_completa::date, d.ano, d.trimestre, d.mes, d.nome_mes,
                d.semana_ano, d.dia_mes, d.dia_semana_num, d.dia_semana_nome, d.fim_de_semana
            FROM (VALUES {}) AS d(data_completa, ano, trimestre, mes, nome_mes,
                                    semana_ano, dia_mes, dia_semana_num, dia_semana_nome, fim_de_semana)
            ON CONFLICT (data_completa) DO NOTHING
        """.format(
            ", ".join(
                f"('{r['data_completa']}', {r['ano']}, {r['trimestre']}, {r['mes']}, "
                f"'{r['nome_mes']}', {r['semana_ano']}, {r['dia_mes']}, "
                f"{r['dia_semana_num']}, '{r['dia_semana_nome']}', {r['fim_de_semana']})"
                for r in rows
            )
        )))
        conn.commit()

        result = conn.execute(text(
            "SELECT data_completa, sk_tempo FROM gold.dim_tempo "
            "WHERE data_completa::text = ANY(:dates)",
        ), {"dates": [str(d) for d in unique_dates]})
        return {str(row[0]): row[1] for row in result}


def upsert_dim_lookup(engine, table: str, conflict_col: str, values: list[str]) -> dict:
    """
    Upsert genérico para dimensões simples de texto (dim_causa, dim_tipo_acidente).
    Retorna {valor: sk}.
    """
    unique = [v for v in set(values) if v and v != "nan"]
    if not unique:
        return {}

    sk_col = f"sk_{table.split('_', 1)[1]}"   # dim_causa -> sk_causa
    if table == "dim_tipo_acidente":
        sk_col = "sk_tipo"

    for val in unique:
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO gold.{table} ({conflict_col})
                VALUES (:val)
                ON CONFLICT ({conflict_col}) DO NOTHING
            """), {"val": val})
            conn.commit()

    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT {conflict_col}, {sk_col} FROM gold.{table}"
        ))
        return {row[0]: row[1] for row in result}


def _normalize_loc_key(frame: pd.DataFrame) -> pd.DataFrame:
    """Normaliza a chave natural de localização para o merge casar dos dois lados.

    km volta do banco como Decimal (NUMERIC) e no DataFrame é float — sem
    normalizar, o merge do pandas não casa (493 de 600 ficavam sem sk).
    """
    out = frame.copy()
    out["km"] = pd.to_numeric(out["km"], errors="coerce").round(3)
    for c in ["uf", "municipio", "br"]:
        out[c] = out[c].astype("string")
    return out


def upsert_dim_localizacao(engine, df: pd.DataFrame) -> pd.Series:
    """Upsert em dim_localizacao. Retorna Series com sk_localizacao por index."""
    loc_cols = ["uf", "municipio", "br", "km", "regional", "delegacia", "uop", "latitude", "longitude"]
    work = _normalize_loc_key(df[loc_cols])
    loc_df = work.drop_duplicates(subset=["uf", "municipio", "br", "km"]).dropna(subset=["uf"])

    with engine.connect() as conn:
        for _, row in loc_df.iterrows():
            conn.execute(text("""
                INSERT INTO gold.dim_localizacao
                    (uf, municipio, br, km, regional, delegacia, uop, latitude, longitude)
                VALUES
                    (:uf, :municipio, :br, :km, :regional, :delegacia, :uop, :latitude, :longitude)
                ON CONFLICT (uf, municipio, br, km) DO NOTHING
            """), row.to_dict())
        conn.commit()

        result = conn.execute(text(
            "SELECT sk_localizacao, uf, municipio, br, km FROM gold.dim_localizacao"
        ))
        loc_map = _normalize_loc_key(
            pd.DataFrame(result.fetchall(),
                         columns=["sk_localizacao", "uf", "municipio", "br", "km"])
        )

    merged = work[["uf", "municipio", "br", "km"]].merge(
        loc_map, on=["uf", "municipio", "br", "km"], how="left"
    )
    return merged["sk_localizacao"]


def upsert_dim_condicao(engine, df: pd.DataFrame) -> pd.Series:
    """Upsert em dim_condicao. Retorna Series com sk_condicao por index."""
    cond_cols = ["fase_dia", "condicao_metereologica", "tipo_pista", "tracado_via", "uso_solo", "sentido_via"]
    cond_df = df[cond_cols].fillna("Ignorado").drop_duplicates()

    with engine.connect() as conn:
        for _, row in cond_df.iterrows():
            conn.execute(text("""
                INSERT INTO gold.dim_condicao
                    (fase_dia, condicao_metereologica, tipo_pista, tracado_via, uso_solo, sentido_via)
                VALUES
                    (:fase_dia, :condicao_metereologica, :tipo_pista, :tracado_via, :uso_solo, :sentido_via)
                ON CONFLICT (fase_dia, condicao_metereologica, tipo_pista, tracado_via, uso_solo, sentido_via) DO NOTHING
            """), row.to_dict())
        conn.commit()

        result = conn.execute(text(
            "SELECT sk_condicao, fase_dia, condicao_metereologica, tipo_pista, tracado_via, uso_solo, sentido_via "
            "FROM gold.dim_condicao"
        ))
        cond_map = pd.DataFrame(result.fetchall(), columns=["sk_condicao"] + cond_cols)

    merged = df[cond_cols].fillna("Ignorado").merge(cond_map, on=cond_cols, how="left")
    return merged["sk_condicao"]


# ── Carga principal ───────────────────────────────────────────────────────────

def run_load(batch_size: int = 5000) -> None:
    """Lê silver.acidentes e popula o Star Schema no Gold."""
    # init_tracing ANTES de get_engine para o SQLAlchemy ser instrumentado.
    tracer = init_tracing("prf-load")
    t0 = time.time()
    log.info("load_started")

    try:
        with tracer.start_as_current_span("load") as span:
            engine = get_engine()

            # Lê Silver
            df = pd.read_sql("SELECT * FROM silver.acidentes ORDER BY id", engine)
            log.info("silver_read", rows=len(df))

            if df.empty:
                log.warning("silver_empty_nothing_to_load")
                return

            # Upsert dimensões
            log.info("upserting_dimensions")

            tempo_map = upsert_dim_tempo(engine, df["data_inversa"])
            causa_map = upsert_dim_lookup(engine, "dim_causa", "causa_acidente",
                                          df["causa_acidente"].tolist())
            tipo_map = upsert_dim_lookup(engine, "dim_tipo_acidente", "tipo_acidente",
                                         df["tipo_acidente"].tolist())

            df["sk_tempo"] = df["data_inversa"].astype(str).map(tempo_map)
            df["sk_causa"] = df["causa_acidente"].map(causa_map)
            df["sk_tipo"] = df["tipo_acidente"].map(tipo_map)
            df["sk_localizacao"] = upsert_dim_localizacao(engine, df)
            df["sk_condicao"] = upsert_dim_condicao(engine, df)

            log.info("dimensions_upserted")

            # Insere fato em batches
            fato_cols = [
                "sk_tempo", "sk_localizacao", "sk_causa", "sk_tipo", "sk_condicao",
                "pessoas", "mortos", "feridos_graves", "feridos_leves", "ilesos", "veiculos",
                "horario", "id",
            ]
            fato_df = df[fato_cols].rename(columns={"id": "source_id"})
            fato_df = fato_df.dropna(subset=["sk_tempo", "sk_localizacao"])

            total_loaded = 0
            for i in range(0, len(fato_df), batch_size):
                batch = fato_df.iloc[i:i + batch_size]
                batch.to_sql(
                    "fato_acidente",
                    engine,
                    schema="gold",
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                total_loaded += len(batch)
                log.info("fato_batch_loaded", batch=i // batch_size + 1, rows=len(batch))

            records_loaded_gold.inc(total_loaded)
            span.set_attribute("prf.rows_loaded", total_loaded)

            # Atualiza views materializadas
            refresh_materialized_views(engine)

            elapsed = time.time() - t0
            stage_duration_seconds.labels(stage="load").observe(elapsed)
            last_run_timestamp.labels(stage="load").set_to_current_time()
            push_metrics("load")

            log.info("load_finished", total_loaded=total_loaded,
                     elapsed_seconds=round(elapsed, 2))
    finally:
        shutdown_tracing()


if __name__ == "__main__":
    run_load()