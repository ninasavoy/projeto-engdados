# Pipeline de Dados — Acidentes PRF

Projeto final de Engenharia de Dados.
Operacionaliza um pipeline completo de dados de acidentes em rodovias federais brasileiras,
desde a ingestão dos CSVs da PRF até um Star Schema pronto para análise.

---

## Problema de Negócio

A equipe de segurança viária da PRF precisa de dados consolidados e confiáveis
para identificar trechos críticos, horários de maior risco e causas predominantes de acidentes.
O pipeline disponibiliza esses dados em um Data Warehouse dimensional consumível
por ferramentas de BI (Metabase, Power BI, Grafana) e por cientistas de dados.

---

## Arquitetura

```
CSV PRF (dados.prf.gov.br)
        │
        ▼
[ BRONZE ]  data/bronze/acidentes_YYYY.csv   ← dados brutos, sem transformação
        │
        │  RabbitMQ (fila desacoplada)
        ▼
[ SILVER ]  PostgreSQL schema silver          ← dados limpos e tipados
        │
        ▼
[ GOLD   ]  PostgreSQL schema gold            ← Star Schema dimensional
        │
        ▼
  Dashboard / Cientistas de dados
```

**Orquestração:** Apache Airflow (DAG `prf_pipeline`)
**Filas:** RabbitMQ (desacopla ingestão da transformação)
**Métricas:** Prometheus + Grafana
**Logging:** structlog (JSON estruturado)

---

## Star Schema

```
              dim_tempo
                  │
dim_condicao ─ fato_acidente ─ dim_causa
                  │
             dim_localizacao
                  │
           dim_tipo_acidente
```

### Tabelas Gold

| Tabela | Tipo | Descrição |
|---|---|---|
| `fato_acidente` | Fato | Um registro por acidente |
| `dim_tempo` | Dimensão | Data completa com atributos calendário |
| `dim_localizacao` | Dimensão | UF, município, BR, KM, coordenadas |
| `dim_causa` | Dimensão | Causa e categoria do acidente |
| `dim_tipo_acidente` | Dimensão | Tipo e classificação |
| `dim_condicao` | Dimensão | Fase do dia, clima, tipo de pista |
| `mv_resumo_uf_ano` | View Mat. | Totais agregados por UF e ano |
| `mv_top_causas` | View Mat. | Ranking de causas por ano |

---

## Estrutura de Pastas

```
prf-pipeline/
├── data/
│   ├── bronze/          # CSVs brutos baixados da PRF
│   ├── silver/          # Parquets limpos (backup/auditoria)
│   └── gold/            # (reservado para exports futuros)
├── dags/
│   └── prf_pipeline_dag.py   # DAG Airflow
├── pipeline/
│   ├── logger.py             # Logging estruturado (structlog)
│   ├── db.py                 # Conexões PostgreSQL
│   ├── metrics.py            # Métricas Prometheus
│   ├── ingestion/
│   │   └── ingest.py         # Bronze: download + RabbitMQ
│   ├── transform/
│   │   └── transform.py      # Silver: limpeza + tipagem
│   └── load/
│       └── load.py           # Gold: Star Schema
├── monitoring/
│   └── prometheus.yml        # Config Prometheus
├── scripts/
│   └── init_db.sql           # DDL completo (schemas + índices + views)
├── docker-compose.yml
├── requirements.txt
├── .env
└── setup.sh
```

---

## Como Rodar

### Pré-requisitos

- Docker e Docker Compose
- Python 3.10+

### Subir tudo com um comando

```bash
bash setup.sh
```

### Rodar etapas manualmente

```bash
source .venv/bin/activate

# 1. Bronze: baixa CSVs e publica no RabbitMQ
python -m pipeline.ingestion.ingest

# 2. Silver: consome RabbitMQ, limpa e salva no Postgres
python -m pipeline.transform.transform

# 3. Gold: popula Star Schema
python -m pipeline.load.load
```

### Acessar o Airflow

Abra http://localhost:8080 → login `admin` / `admin`
Ative a DAG `prf_pipeline` → ela rodará diariamente às 03:00 ou sob demanda.

### Acessar o RabbitMQ Management

http://localhost:15672 → login `prf` / `prf123`
Fila: `prf_raw_data`

### Ver métricas no Prometheus

http://localhost:9090

Métricas disponíveis:
- `prf_records_ingested_total` — registros ingeridos por ano
- `prf_records_processed_total` — registros transformados
- `prf_records_loaded_gold_total` — registros no Gold
- `prf_pipeline_errors_total` — erros por etapa
- `prf_stage_duration_seconds` — duração de cada etapa

---


## Fonte dos Dados

[Dados Abertos PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos)
Arquivos de ocorrências 2021, 2022 e 2023 (acidentes agrupados por ocorrência).
Licença: dados públicos do Governo Federal Brasileiro.
