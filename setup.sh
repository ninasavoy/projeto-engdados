#!/bin/bash
# =============================================================================
# setup.sh — Sobe toda a stack do projeto PRF (um comando).
# Uso: bash setup.sh
# =============================================================================
set -e

echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║   Pipeline PRF — Engenharia de Dados — Setup        ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

# 1. Dependências
echo "→ Verificando dependências..."
command -v docker >/dev/null 2>&1 || { echo "ERRO: Docker não encontrado."; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERRO: 'docker compose' não encontrado."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERRO: Python3 não encontrado."; exit 1; }

# 2. .env
if [ ! -f .env ]; then
  echo "→ Criando .env a partir de .env.example..."
  cp .env.example .env
  echo "   ⚠  Se a porta 5432/5434 já estiver em uso, ajuste POSTGRES_HOST_PORT/"
  echo "      POSTGRES_REPLICA_HOST_PORT (e POSTGRES_PORT/REPLICA_PORT) no .env."
fi

# 3. venv (para rodar o pipeline/scripts fora do Docker)
echo "→ Criando ambiente virtual Python..."
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt
echo "   ✓ Dependências Python instaladas"

# 4. Diretórios de dados + permissões (Airflow roda como uid 50000)
echo "→ Preparando diretórios de dados..."
mkdir -p data/bronze data/silver data/gold logs
chmod -R 777 data logs   # ambiente local: garante escrita por qualquer uid de container

# 5. Confere o datatran no Bronze (dados reais da PRF)
if ! ls data/bronze/acidentes_*.csv >/dev/null 2>&1; then
  echo "   ⚠  Nenhum CSV em data/bronze/. Baixe o datatran da PRF e salve como"
  echo "      data/bronze/acidentes_<ano>.csv (e ajuste PRF_YEARS no .env)."
fi

# 6. Build das imagens (Airflow + API)
echo "→ Construindo imagens Docker (pode demorar na 1ª vez)..."
docker compose build

# 7. Sobe tudo
echo "→ Subindo containers..."
docker compose up -d

# 8. Espera serviços essenciais
echo "→ Aguardando Postgres (primário e réplica)..."
until docker exec prf-postgres pg_isready -U prf -d prf_dw >/dev/null 2>&1; do sleep 2; done
echo "   ✓ Primário pronto"
until docker exec prf-postgres-replica pg_isready -U prf -d prf_dw >/dev/null 2>&1; do sleep 2; done
echo "   ✓ Réplica pronta"
echo "→ Aguardando Airflow (standalone) iniciar (~1 min)..."
until curl -sf http://localhost:8080/ >/dev/null 2>&1; do sleep 3; done
echo "   ✓ Airflow no ar"

# 9. Resumo
echo ""
echo "╔════════════════════════════════════════════════════╗"
echo "║   Serviços disponíveis                              ║"
echo "╠════════════════════════════════════════════════════╣"
echo "║  Airflow      → http://localhost:8080  (sem login) ║"
echo "║  API          → http://localhost:8001/docs         ║"
echo "║  Jaeger       → http://localhost:16686             ║"
echo "║  Grafana      → http://localhost:3000  (admin/admin)║"
echo "║  Prometheus   → http://localhost:9090              ║"
echo "║  RabbitMQ     → http://localhost:15672 (prf/prf123)║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "Dispare o pipeline no Airflow (DAG 'prf_pipeline') ou rode do host:"
echo "  .venv/bin/python -m pipeline.ingestion.ingest"
echo "  .venv/bin/python -m pipeline.transform.transform"
echo "  .venv/bin/python -m pipeline.load.load"
echo ""
