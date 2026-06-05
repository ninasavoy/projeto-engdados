-- =============================================================================
-- init_db.sql  –  Inicialização do Data Warehouse da PRF
-- Executa automaticamente na primeira vez que o container sobe
-- =============================================================================

-- Schemas (camadas Medallion)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- BRONZE – dados brutos exatamente como chegam do CSV
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze.acidentes_raw (
    id              SERIAL PRIMARY KEY,
    raw_data        JSONB          NOT NULL,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ    DEFAULT NOW()
);

-- =============================================================================
-- SILVER – dados limpos e tipados
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.acidentes (
    id                  SERIAL PRIMARY KEY,
    data_inversa        DATE,
    dia_semana          TEXT,
    horario             TIME,
    uf                  CHAR(2),
    br                  TEXT,
    km                  NUMERIC(10,3),
    municipio           TEXT,
    causa_acidente      TEXT,
    tipo_acidente       TEXT,
    classificacao_acidente TEXT,
    fase_dia            TEXT,
    sentido_via         TEXT,
    condicao_metereologica TEXT,
    tipo_pista          TEXT,
    tracado_via         TEXT,
    uso_solo            TEXT,
    pessoas             INTEGER,
    mortos              INTEGER,
    feridos_graves      INTEGER,
    feridos_leves       INTEGER,
    ilesos              INTEGER,
    ignorados           INTEGER,
    feridos             INTEGER,
    veiculos            INTEGER,
    latitude            NUMERIC(12,8),
    longitude           NUMERIC(12,8),
    regional            TEXT,
    delegacia           TEXT,
    uop                 TEXT,
    processed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_data      ON silver.acidentes (data_inversa);
CREATE INDEX IF NOT EXISTS idx_silver_uf        ON silver.acidentes (uf);
CREATE INDEX IF NOT EXISTS idx_silver_br        ON silver.acidentes (br);
CREATE INDEX IF NOT EXISTS idx_silver_causa     ON silver.acidentes (causa_acidente);
CREATE INDEX IF NOT EXISTS idx_silver_data_brin ON silver.acidentes USING BRIN (data_inversa);

-- =============================================================================
-- GOLD – Star Schema dimensional
-- =============================================================================

-- Dimensão: Tempo
CREATE TABLE IF NOT EXISTS gold.dim_tempo (
    sk_tempo        SERIAL PRIMARY KEY,
    data_completa   DATE        NOT NULL UNIQUE,
    ano             INTEGER,
    trimestre       INTEGER,
    mes             INTEGER,
    nome_mes        TEXT,
    semana_ano      INTEGER,
    dia_mes         INTEGER,
    dia_semana_num  INTEGER,
    dia_semana_nome TEXT,
    fim_de_semana   BOOLEAN
);

-- Dimensão: Localização
CREATE TABLE IF NOT EXISTS gold.dim_localizacao (
    sk_localizacao  SERIAL PRIMARY KEY,
    uf              CHAR(2),
    municipio       TEXT,
    br              TEXT,
    km              NUMERIC(10,3),
    regional        TEXT,
    delegacia       TEXT,
    uop             TEXT,
    latitude        NUMERIC(12,8),
    longitude       NUMERIC(12,8),
    UNIQUE (uf, municipio, br, km)
);

-- Dimensão: Causa
CREATE TABLE IF NOT EXISTS gold.dim_causa (
    sk_causa        SERIAL PRIMARY KEY,
    causa_acidente  TEXT  NOT NULL UNIQUE,
    categoria_causa TEXT
);

-- Dimensão: Tipo de Acidente
CREATE TABLE IF NOT EXISTS gold.dim_tipo_acidente (
    sk_tipo         SERIAL PRIMARY KEY,
    tipo_acidente   TEXT  NOT NULL UNIQUE,
    classificacao   TEXT
);

-- Dimensão: Condição
CREATE TABLE IF NOT EXISTS gold.dim_condicao (
    sk_condicao     SERIAL PRIMARY KEY,
    fase_dia        TEXT,
    condicao_metereologica TEXT,
    tipo_pista      TEXT,
    tracado_via     TEXT,
    uso_solo        TEXT,
    sentido_via     TEXT,
    UNIQUE (fase_dia, condicao_metereologica, tipo_pista, tracado_via, uso_solo, sentido_via)
);

-- Fato: Acidente
CREATE TABLE IF NOT EXISTS gold.fato_acidente (
    sk_acidente     SERIAL PRIMARY KEY,
    sk_tempo        INTEGER REFERENCES gold.dim_tempo(sk_tempo),
    sk_localizacao  INTEGER REFERENCES gold.dim_localizacao(sk_localizacao),
    sk_causa        INTEGER REFERENCES gold.dim_causa(sk_causa),
    sk_tipo         INTEGER REFERENCES gold.dim_tipo_acidente(sk_tipo),
    sk_condicao     INTEGER REFERENCES gold.dim_condicao(sk_condicao),
    -- métricas
    pessoas         INTEGER DEFAULT 0,
    mortos          INTEGER DEFAULT 0,
    feridos_graves  INTEGER DEFAULT 0,
    feridos_leves   INTEGER DEFAULT 0,
    ilesos          INTEGER DEFAULT 0,
    veiculos        INTEGER DEFAULT 0,
    -- metadados
    horario         TIME,
    source_id       INTEGER REFERENCES silver.acidentes(id),
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Índices na fato (B-Tree para FKs, BRIN para tempo)
CREATE INDEX IF NOT EXISTS idx_fato_tempo       ON gold.fato_acidente (sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_local       ON gold.fato_acidente (sk_localizacao);
CREATE INDEX IF NOT EXISTS idx_fato_causa       ON gold.fato_acidente (sk_causa);
CREATE INDEX IF NOT EXISTS idx_fato_tipo        ON gold.fato_acidente (sk_tipo);

-- View materializada: resumo anual por UF (para dashboards)
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_resumo_uf_ano AS
SELECT
    dt.ano,
    dl.uf,
    COUNT(*)                    AS total_acidentes,
    SUM(fa.mortos)              AS total_mortos,
    SUM(fa.feridos_graves)      AS total_feridos_graves,
    SUM(fa.feridos_leves)       AS total_feridos_leves,
    SUM(fa.veiculos)            AS total_veiculos,
    ROUND(AVG(fa.mortos), 4)    AS media_mortos_por_acidente
FROM gold.fato_acidente fa
JOIN gold.dim_tempo       dt ON fa.sk_tempo      = dt.sk_tempo
JOIN gold.dim_localizacao dl ON fa.sk_localizacao = dl.sk_localizacao
GROUP BY dt.ano, dl.uf
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_resumo_uf_ano ON gold.mv_resumo_uf_ano (ano, uf);

-- View materializada: top causas por ano
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_top_causas AS
SELECT
    dt.ano,
    dc.causa_acidente,
    dc.categoria_causa,
    COUNT(*)            AS total_acidentes,
    SUM(fa.mortos)      AS total_mortos
FROM gold.fato_acidente fa
JOIN gold.dim_tempo  dt ON fa.sk_tempo  = dt.sk_tempo
JOIN gold.dim_causa  dc ON fa.sk_causa  = dc.sk_causa
GROUP BY dt.ano, dc.causa_acidente, dc.categoria_causa
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_causas ON gold.mv_top_causas (ano, causa_acidente);