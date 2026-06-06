"""
pipeline/db.py
Helpers de conexão com PostgreSQL via psycopg2 e SQLAlchemy.
"""

import os
from contextlib import contextmanager

import psycopg
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from pipeline.logger import get_logger

load_dotenv()
log = get_logger("db")


def get_connection_string() -> str:
    # Dialeto psycopg2: compatível com SQLAlchemy 1.4 (exigida pelo Airflow 3).
    # O dialeto psycopg (v3) só existe no SQLAlchemy >= 2.0.
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'prf')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'prf123')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'prf_dw')}"
    )


def get_read_connection_string() -> str:
    """String de conexão para LEITURAS — aponta para a read replica.

    Faz fallback para o primário se POSTGRES_REPLICA_HOST não estiver definido.
    """
    replica_host = os.getenv("POSTGRES_REPLICA_HOST")
    if not replica_host:
        return get_connection_string()
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'prf')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'prf123')}@"
        f"{replica_host}:"
        f"{os.getenv('POSTGRES_REPLICA_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'prf_dw')}"
    )


def get_engine():
    """Engine de ESCRITA — sempre o primário."""
    conn_str = get_connection_string()
    # future=True habilita a API estilo SQLAlchemy 2.0 (conn.commit()/rollback)
    # mesmo no SQLAlchemy 1.4 exigido pelo Airflow 3.
    engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5, future=True)
    log.info("db_engine_created", role="primary", url=conn_str.split("@")[-1])
    return engine


def get_read_engine():
    """Engine de LEITURA — read replica (com fallback ao primário)."""
    conn_str = get_read_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5, future=True)
    log.info("db_engine_created", role="replica", url=conn_str.split("@")[-1])
    return engine


@contextmanager
def get_psycopg2_conn():
    """Context manager para conexão direta com psycopg3."""
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "prf_dw"),
        user=os.getenv("POSTGRES_USER", "prf"),
        password=os.getenv("POSTGRES_PASSWORD", "prf123"),
    )
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error("db_transaction_error", error=str(e))
        raise
    finally:
        conn.close()


def refresh_materialized_views(engine) -> None:
    """Atualiza as views materializadas do Gold após carga."""
    views = ["gold.mv_resumo_uf_ano", "gold.mv_top_causas"]
    with engine.connect() as conn:
        for view in views:
            log.info("refreshing_materialized_view", view=view)
            conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
            conn.commit()
    log.info("materialized_views_refreshed", count=len(views))