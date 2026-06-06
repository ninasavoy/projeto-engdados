# =============================================================================
# Imagem da API de consumo (FastAPI). Serviço independente do Airflow.
# O código (api/ e pipeline/) é montado via volume no docker-compose para
# permitir edição sem rebuild; aqui só instalamos as dependências.
# =============================================================================
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1

COPY api/requirements.txt /app/api/requirements.txt
RUN pip install --no-cache-dir -r /app/api/requirements.txt

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8001"]
