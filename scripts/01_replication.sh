#!/bin/bash
# =============================================================================
# 01_replication.sh — configura o PRIMÁRIO para streaming replication.
# Roda automaticamente na primeira inicialização do container (initdb).
#   1. Cria a role de replicação.
#   2. Libera conexões de replicação no pg_hba (trust, rede Docker isolada).
# wal_level/max_wal_senders são passados via `command` no docker-compose.
# =============================================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE replicator WITH REPLICATION LOGIN;
EOSQL

# Permite que a réplica (qualquer host da rede Docker do projeto) faça
# replicação como 'replicator' sem senha. Aceitável: rede interna isolada.
{
  echo "host replication replicator all trust"
} >> "$PGDATA/pg_hba.conf"

echo "[01_replication] role 'replicator' criada e pg_hba liberado para replicação"
