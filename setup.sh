#!/bin/bash
# =============================================================================
# setup.sh — Sobe toda a infraestrutura e inicializa o pipeline PRF
# Uso: bash setup.sh
# =============================================================================

set -e   # para no primeiro erro

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Pipeline PRF — Configuração Inicial        ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# 1. Verifica dependências
echo "→ Verificando dependências..."
command -v docker >/dev/null 2>&1 || { echo "ERRO: Docker não encontrado."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v docker compose >/dev/null 2>&1 || { echo "ERRO: docker-compose não encontrado."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERRO: Python3 não encontrado."; exit 1; }

# 2. Cria virtualenv local (para rodar scripts fora do Docker)
echo "→ Criando ambiente virtual Python..."
python3 -m venv .venv
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "   ✓ Dependências Python instaladas"

# 3. Cria pastas de dados
echo "→ Criando diretórios de dados..."
mkdir -p data/bronze data/silver data/gold logs

# 4. Sobe os containers
echo "→ Subindo containers Docker..."
docker compose up -d postgres postgres-airflow rabbitmq prometheus grafana
echo "   ✓ Containers de infraestrutura iniciados"

# 5. Aguarda PostgreSQL ficar pronto
echo "→ Aguardando PostgreSQL ficar saudável..."
until docker exec prf-postgres pg_isready -U prf > /dev/null 2>&1; do
  sleep 2
done
echo "   ✓ PostgreSQL pronto"

# 6. Aguarda RabbitMQ
echo "→ Aguardando RabbitMQ ficar saudável..."
until docker exec prf-rabbitmq rabbitmq-diagnostics ping > /dev/null 2>&1; do
  sleep 3
done
echo "   ✓ RabbitMQ pronto"

# 7. Inicializa Airflow
echo "→ Inicializando Airflow (pode demorar ~1 min)..."
docker compose up -d airflow-init
sleep 30
docker compose up -d airflow-webserver airflow-scheduler
echo "   ✓ Airflow iniciado"

# 8. Resumo de URLs
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Serviços disponíveis                       ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Airflow    → http://localhost:8080          ║"
echo "║              login: admin / admin            ║"
echo "║  RabbitMQ   → http://localhost:15672         ║"
echo "║              login: prf / prf123             ║"
echo "║  Grafana    → http://localhost:3000          ║"
echo "║              login: admin / admin            ║"
echo "║  Prometheus → http://localhost:9090          ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "Para rodar o pipeline manualmente:"
echo "  source .venv/bin/activate"
echo "  python -m pipeline.ingestion.ingest"
echo "  python -m pipeline.transform.transform"
echo "  python -m pipeline.load.load"
echo ""