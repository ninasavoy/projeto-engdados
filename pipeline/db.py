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
    return (
        f"postgresql+psycopg://"
        f"{os.getenv('POSTGRES_USER', 'prf')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'prf123')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'prf_dw')}"
    )


def get_engine():
    conn_str = get_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5)
    log.info("db_engine_created", url=conn_str.split("@")[-1])
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