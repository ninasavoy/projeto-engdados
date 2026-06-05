"""
dags/prf_pipeline_dag.py
DAG principal do projeto PRF.
Orquestra: ingestão → transformação → carga Gold
com retries, dependências claras e alertas de falha.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Argumentos padrão ─────────────────────────────────────────────────────────
default_args = {
    "owner": "engenharia-dados",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,                           # rubrica B: retries
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,      # espera cresce a cada retry
    "max_retry_delay": timedelta(minutes=30),
}


# ── Funções chamadas por cada task ────────────────────────────────────────────

def task_ingest(**context):
    """Wrapper Airflow → ingestão Bronze."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from pipeline.ingestion.ingest import run_ingestion
    from pipeline.metrics import start_metrics_server

    # Expõe métricas apenas se não estiver rodando em subprocesso já ativo
    try:
        start_metrics_server()
    except Exception:
        pass

    years = context["params"].get("years", None)
    run_ingestion(years=years)


def task_transform(**context):
    """Wrapper Airflow → transformação Silver."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from pipeline.transform.transform import run_transform
    run_transform()


def task_load(**context):
    """Wrapper Airflow → carga Gold."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from pipeline.load.load import run_load
    run_load()


def task_quality_check(**context):
    """
    Verificações básicas de qualidade após carga.
    Falha a task se os dados estiverem inconsistentes.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from pipeline.db import get_engine
    from sqlalchemy import text

    engine = get_engine()
    errors = []

    with engine.connect() as conn:
        # 1. Fato não pode ter registros sem dimensão tempo
        r = conn.execute(text(
            "SELECT COUNT(*) FROM gold.fato_acidente WHERE sk_tempo IS NULL"
        )).scalar()
        if r > 0:
            errors.append(f"fato_acidente tem {r} registros sem sk_tempo")

        # 2. Silver não pode estar vazio
        r = conn.execute(text("SELECT COUNT(*) FROM silver.acidentes")).scalar()
        if r == 0:
            errors.append("silver.acidentes está vazio")

        # 3. Mortos não podem ser negativos
        r = conn.execute(text(
            "SELECT COUNT(*) FROM gold.fato_acidente WHERE mortos < 0"
        )).scalar()
        if r > 0:
            errors.append(f"fato_acidente tem {r} registros com mortos < 0")

    if errors:
        raise ValueError("Falhas na verificação de qualidade:\n" + "\n".join(errors))

    print(f"Verificação de qualidade OK — silver rows: {r}")


def task_notify_failure(context):
    """Callback chamado em falha de qualquer task."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from pipeline.logger import get_logger
    log = get_logger("airflow.failure")

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    log.error(
        "dag_task_failed",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=str(execution_date),
        exception=str(exception),
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="prf_pipeline",
    description="Pipeline completo PRF: Bronze → Silver → Gold",
    default_args=default_args,
    schedule="0 3 * * *",       # todo dia às 03:00
    catchup=False,
    max_active_runs=1,
    tags=["prf", "engenharia-dados", "transporte"],
    params={
        "years": ["2021", "2022", "2023"],
    },
    on_failure_callback=task_notify_failure,
) as dag:

    start = EmptyOperator(task_id="start")

    ingest = PythonOperator(
        task_id="ingestion_bronze",
        python_callable=task_ingest,
    )

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load_gold",
        python_callable=task_load,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependências lineares: start → ingest → transform → load → quality → end
    start >> ingest >> transform >> load >> quality >> end