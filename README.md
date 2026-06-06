# Pipeline de Dados — Acidentes em Rodovias Federais (PRF)

Projeto final de Engenharia de Dados. Operacionaliza, em ambiente containerizado,
um pipeline completo de dados de acidentes em rodovias federais brasileiras — da
ingestão dos CSVs da PRF a um Star Schema dimensional consumido por uma API e por
dashboards, com orquestração, mensageria, observabilidade (métricas + tracing
distribuído) e read replica.

---

## Problema de Negócio

A equipe de segurança viária da PRF precisa de dados consolidados e confiáveis
para identificar **trechos críticos, horários de maior risco e causas
predominantes** de acidentes. O pipeline disponibiliza esses dados em um Data
Warehouse dimensional, consumível por:

- **Dashboards / BI** (via API REST e views materializadas);
- **Cientistas de dados** (acesso direto ao Star Schema / read replica).

---

## Arquitetura

```
        datatran PRF (CSV real em data/bronze/acidentes_<ano>.csv)
                        │
                        ▼
   ┌──────────┐   [ BRONZE ]  bronze.acidentes_raw (JSONB) + data/bronze/*.csv
   │ Airflow  │        │  RabbitMQ (fila desacoplada, ctx de trace nos headers)
   │ (orques- │        ▼
   │  tração) │   [ SILVER ]  postgres (schema silver)   (limpo e tipado)
   └──────────┘        │
                        ▼
                   [ GOLD ]   postgres (schema gold)      (Star Schema + MVs)
                        │
            ┌───────────┴───────────┐
            ▼ (streaming repl.)      ▼
     postgres-replica  ◀────  API FastAPI (cursor pagination)  ──▶  Dashboards
        (leituras)

  Observabilidade transversal:
   • Métricas  → Pushgateway → Prometheus → Grafana
   • Tracing   → OpenTelemetry → Jaeger (ingestion → fila → transform → load → API)
   • Logs      → structlog (JSON estruturado)
```

| Camada | Responsabilidade | Tecnologia |
|---|---|---|
| Orquestração | Agenda, retries, dependências | Apache Airflow 3.0 |
| Mensageria | Desacopla ingestão da transformação | RabbitMQ |
| Armazenamento | DW Medallion + Star Schema | PostgreSQL 15 (+ read replica) |
| Servição de dados | Consumo paginado | FastAPI |
| Métricas | Saúde/performance do pipeline | Prometheus + Pushgateway + Grafana |
| Tracing | Latência entre serviços | OpenTelemetry + Jaeger |
| Logging | Eventos do pipeline | structlog (JSON) |

---

## Modelo de Dados — Star Schema

```
              dim_tempo
                  │
dim_condicao ─ fato_acidente ─ dim_causa
                  │   │
       dim_localizacao   dim_tipo_acidente
```

| Tabela | Tipo | Descrição |
|---|---|---|
| `gold.fato_acidente` | Fato | 1 linha por acidente; métricas aditivas (mortos, feridos, veículos) |
| `gold.dim_tempo` | Dimensão | Calendário (ano, trimestre, mês, dia da semana, fim de semana) |
| `gold.dim_localizacao` | Dimensão | UF, município, BR, KM, coordenadas |
| `gold.dim_causa` | Dimensão | Causa do acidente |
| `gold.dim_tipo_acidente` | Dimensão | Tipo e classificação |
| `gold.dim_condicao` | Dimensão | Fase do dia, clima, pista, traçado |
| `gold.mv_resumo_uf_ano` | View Mat. | Totais por UF e ano |
| `gold.mv_top_causas` | View Mat. | Ranking de causas por ano |

DDL completo em [`scripts/init_db.sql`](scripts/init_db.sql).

---

## Como Rodar

### Pré-requisitos
- Docker + Docker Compose
- Python 3.10+

### Um comando

```bash
bash setup.sh
```

Isso cria o `.env` (a partir do `.env.example`), instala a venv, constrói as
imagens e sobe toda a stack.

> **Portas:** se 5432 (Postgres) ou 5434 (réplica) já estiverem em uso na sua
> máquina, ajuste `POSTGRES_HOST_PORT` / `POSTGRES_REPLICA_HOST_PORT` (e os
> `*_PORT` correspondentes) no `.env`.

### Disparar o pipeline
- **Airflow:** abra http://localhost:8080 → DAG `prf_pipeline` → *Trigger*.
- **Ou pelo host:**
  ```bash
  .venv/bin/python -m pipeline.ingestion.ingest
  .venv/bin/python -m pipeline.transform.transform
  .venv/bin/python -m pipeline.load.load
  ```

### Serviços

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | sem login (dev) |
| API (Swagger) | http://localhost:8001/docs | — |
| Jaeger (tracing) | http://localhost:16686 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| RabbitMQ | http://localhost:15672 | prf / prf123 |

---

## Fonte dos Dados

[Dados Abertos PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes)
— acidentes "agrupados por ocorrência" (datatran). Licença: dados públicos do
Governo Federal.

O repositório já inclui o **datatran 2022 real** em
`data/bronze/acidentes_2022.csv` (~64 mil ocorrências), então o pipeline roda
sobre dados reais sem nenhuma configuração extra. Para usar outros anos, baixe o
CSV correspondente na página oficial, salve como
`data/bronze/acidentes_<ano>.csv` e inclua o ano em `PRF_YEARS`. A ingestão lê os
arquivos diretamente de `data/bronze/` (não há fallback sintético).

---

## Estrutura de Pastas

```
projeto-engdados/
├── dags/prf_pipeline_dag.py        
├── data/bronze/acidentes_2022.csv  
├── pipeline/
│   ├── ingestion/ingest.py         
│   ├── transform/transform.py      
│   ├── load/load.py                
│   ├── db.py                       
│   ├── logger.py                   
│   ├── metrics.py                  
│   └── tracing.py                  
├── api/                            
├── scripts/
│   ├── init_db.sql                 
│   ├── 01_replication.sh           
│   └── seed_perf_data.sql          
├── monitoring/                     
├── docker-compose.yml • Dockerfile • requirements.txt • setup.sh • .env.example
```

---

## Vídeo de Apresentação

📹 [Assista no YouTube](https://youtu.be/FPtOIU9iUOk)
