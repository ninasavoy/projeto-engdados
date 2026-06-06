# =============================================================================
# Imagem do Airflow com as dependências de runtime do pipeline instaladas.
# A imagem base já traz o Apache Airflow 3.0.6; aqui adicionamos as libs que as
# tasks da DAG importam (pandas, pika, psycopg, etc.).
#
# A instalação usa o arquivo de CONSTRAINTS oficial do Airflow para a versão e o
# Python da imagem. Isso garante que libs compartilhadas (structlog, requests,
# pyarrow, ...) fiquem nas versões que o Airflow exige, em vez de serem rebaixadas
# (o que quebraria, por ex., o apache-airflow-task-sdk).
# =============================================================================
FROM apache/airflow:3.0.6

ARG AIRFLOW_VERSION=3.0.6
COPY requirements.txt /tmp/requirements.txt

RUN PYV="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir -r /tmp/requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"
