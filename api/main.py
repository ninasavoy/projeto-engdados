"""
api/main.py
API de consumo do Data Warehouse PRF (rubrica A#5).

Destaques:
  - LÊ DA READ REPLICA (pipeline.db.get_read_engine) → distribui a carga de leitura.
  - Paginação por CURSOR (keyset): estável e escalável, ao contrário de OFFSET.
  - Instrumentada com OpenTelemetry → cada request vira uma trace no Jaeger,
    na mesma malha do pipeline.

Endpoints:
  GET /health
  GET /acidentes          → fato + dimensões, paginado por cursor (keyset)
  GET /acidentes_offset   → MESMA consulta com OFFSET (didático: compare no /docs)
  GET /resumo/uf-ano      → view materializada gold.mv_resumo_uf_ano
  GET /causas/top         → view materializada gold.mv_top_causas
"""

import base64

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text

from pipeline.db import get_read_engine
from pipeline.tracing import init_tracing

# Tracing primeiro (instrumenta SQLAlchemy), depois cria o engine de leitura.
init_tracing("prf-api")
engine = get_read_engine()

app = FastAPI(
    title="PRF Acidentes API",
    description="Consulta o DW de acidentes em rodovias federais (lê da read replica).",
    version="1.0.0",
)

# Instrumentação FastAPI → spans de cada request no Jaeger.
try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    FastAPIInstrumentor.instrument_app(app)
except Exception:  # noqa: BLE001
    pass


# ── Cursor (keyset) helpers ───────────────────────────────────────────────────

def encode_cursor(sk: int) -> str:
    """Cursor opaco = base64 do último sk_acidente da página."""
    return base64.urlsafe_b64encode(str(sk).encode()).decode()


def decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="cursor inválido")


ACIDENTES_SELECT = """
    SELECT f.sk_acidente, t.data_completa, t.ano, l.uf, l.municipio, l.br, l.km,
           c.causa_acidente, ti.tipo_acidente, ti.classificacao,
           f.mortos, f.feridos_graves, f.feridos_leves, f.veiculos
    FROM gold.fato_acidente f
    JOIN gold.dim_tempo        t  ON f.sk_tempo       = t.sk_tempo
    JOIN gold.dim_localizacao  l  ON f.sk_localizacao = l.sk_localizacao
    LEFT JOIN gold.dim_causa   c  ON f.sk_causa       = c.sk_causa
    LEFT JOIN gold.dim_tipo_acidente ti ON f.sk_tipo  = ti.sk_tipo
"""


def _rows_to_dicts(result) -> list[dict]:
    return [dict(r._mapping) for r in result]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    with engine.connect() as conn:
        in_recovery = conn.execute(text("SELECT pg_is_in_recovery()")).scalar()
    return {"status": "ok", "reading_from_replica": bool(in_recovery)}


@app.get("/acidentes")
def listar_acidentes(
    limit: int = Query(50, ge=1, le=500, description="Itens por página"),
    cursor: str | None = Query(None, description="Cursor opaco da página anterior"),
    uf: str | None = Query(None, min_length=2, max_length=2, description="Filtro por UF"),
):
    """Paginação por CURSOR (keyset): WHERE sk_acidente > :after ORDER BY sk_acidente.

    Escala bem em qualquer profundidade — usa o índice da PK e não relê linhas
    descartadas (ao contrário de OFFSET).
    """
    after = decode_cursor(cursor) if cursor else 0

    sql = ACIDENTES_SELECT + "    WHERE f.sk_acidente > :after\n"
    params = {"after": after, "limit": limit}
    if uf:
        sql += "      AND l.uf = :uf\n"
        params["uf"] = uf.upper()
    sql += "    ORDER BY f.sk_acidente\n    LIMIT :limit"

    with engine.connect() as conn:
        rows = _rows_to_dicts(conn.execute(text(sql), params))

    next_cursor = encode_cursor(rows[-1]["sk_acidente"]) if len(rows) == limit else None
    return {"count": len(rows), "next_cursor": next_cursor, "items": rows}


@app.get("/acidentes_offset")
def listar_acidentes_offset(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0, description="Didático: OFFSET piora em páginas profundas"),
    uf: str | None = Query(None, min_length=2, max_length=2),
):
    """MESMA consulta com OFFSET — incluída só para comparação no /docs e no
    EXPLAIN (ver docs/query-optimization.md). NÃO use em produção: OFFSET N
    relê e descarta N linhas a cada página."""
    sql = ACIDENTES_SELECT
    params = {"limit": limit, "offset": offset}
    if uf:
        sql += "    WHERE l.uf = :uf\n"
        params["uf"] = uf.upper()
    sql += "    ORDER BY f.sk_acidente\n    LIMIT :limit OFFSET :offset"

    with engine.connect() as conn:
        rows = _rows_to_dicts(conn.execute(text(sql), params))
    return {"count": len(rows), "offset": offset, "items": rows}


@app.get("/resumo/uf-ano")
def resumo_uf_ano(ano: int | None = Query(None)):
    """Totais agregados por UF e ano (view materializada)."""
    sql = "SELECT * FROM gold.mv_resumo_uf_ano"
    params = {}
    if ano:
        sql += " WHERE ano = :ano"
        params["ano"] = ano
    sql += " ORDER BY ano, total_acidentes DESC"
    with engine.connect() as conn:
        return {"items": _rows_to_dicts(conn.execute(text(sql), params))}


@app.get("/causas/top")
def top_causas(
    ano: int | None = Query(None),
    limit: int = Query(10, ge=1, le=100),
):
    """Ranking de causas por número de acidentes (view materializada)."""
    sql = "SELECT * FROM gold.mv_top_causas"
    params = {"limit": limit}
    if ano:
        sql += " WHERE ano = :ano"
        params["ano"] = ano
    sql += " ORDER BY total_acidentes DESC LIMIT :limit"
    with engine.connect() as conn:
        return {"items": _rows_to_dicts(conn.execute(text(sql), params))}
