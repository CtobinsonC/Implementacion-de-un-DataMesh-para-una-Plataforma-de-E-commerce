"""
DAG: gold_reabastecimiento
Orquesta el pipeline Silver → Gold de reabastecimiento (Data Mesh E-commerce).

Flujo:
  [check Silver inventario] ──┐
                               ├──► [Dataproc PySpark Job] ──► [check Gold output]
  [check Silver ventas]   ────┘

Schedule: Diario a las 06:00 UTC
Alertas:  Email en fallo de cualquier tarea
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PROJECT_ID  = "gcp-implementacion-datamesh"
REGION      = "us-east1"
CLUSTER     = "cluster-analisis-inventario"
TEMP_BUCKET = "dataproc-staging-us-east1-161952831522-bvymgmk4"
SCRIPT_URI  = f"gs://{TEMP_BUCKET}/reabastecimiento.py"
JAR_URI     = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"

ALERT_EMAIL = ["caleb.tobinson@gmail.com"]  # ← Reemplaza con tu email

# ---------------------------------------------------------------------------
# Definición del job de Dataproc
# ---------------------------------------------------------------------------
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": SCRIPT_URI,
        "jar_file_uris": [JAR_URI],
        "args": [
            f"--project={PROJECT_ID}",
            f"--temp_bucket={TEMP_BUCKET}",
        ],
    },
}

# ---------------------------------------------------------------------------
# Argumentos por defecto
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-mesh-inventario",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ALERT_EMAIL,
    "email_on_failure": True,   # ← Alerta por email en fallo
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="gold_reabastecimiento",
    default_args=default_args,
    description="Pipeline Silver→Gold: niveles de reabastecimiento por producto (Data Mesh E-commerce)",
    schedule_interval="0 6 * * *",  # Diario 06:00 UTC = 01:00 AM Colombia
    start_date=datetime(2026, 3, 17),  # Fecha pasada requerida por Airflow
    catchup=False,
    max_active_runs=1,
    tags=["data-mesh", "gold", "inventario", "dataproc"],
) as dag:

    # -----------------------------------------------------------------------
    # 1. Pre-checks: verificar que las fuentes Silver tienen datos
    # -----------------------------------------------------------------------
    check_silver_inventario = BigQueryCheckOperator(
        task_id="check_silver_inventario",
        sql="""
            SELECT COUNT(*) > 0
            FROM `gcp-implementacion-datamesh.lake_ecommerce_curated.productos_limpios`
        """,
        use_legacy_sql=False,
        location="us-central1",
    )

    check_silver_ventas = BigQueryCheckOperator(
        task_id="check_silver_ventas",
        sql="""
            SELECT COUNT(*) > 0
            FROM `gcp-implementacion-datamesh.lake_ecommerce_curated.ventas_limpias`
        """,
        use_legacy_sql=False,
        location="us-central1",
    )

    # -----------------------------------------------------------------------
    # 2. Job principal: Dataproc PySpark Silver → Gold
    # -----------------------------------------------------------------------
    submit_reabastecimiento = DataprocSubmitJobOperator(
        task_id="submit_reabastecimiento_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # -----------------------------------------------------------------------
    # 3. Post-check: verificar que la tabla Gold fue actualizada hoy
    # -----------------------------------------------------------------------
    check_gold_output = BigQueryCheckOperator(
        task_id="check_gold_output",
        sql="""
            SELECT COUNT(*) > 28000
            FROM `gcp-implementacion-datamesh.lake_ecommerce_curated.gold_reabastecimiento`
            WHERE DATE(processing_timestamp) = CURRENT_DATE()
        """,
        use_legacy_sql=False,
        location="us-central1",
    )

    # -----------------------------------------------------------------------
    # Dependencias: ambos checks Silver deben pasar antes del job
    # -----------------------------------------------------------------------
    [check_silver_inventario, check_silver_ventas] >> submit_reabastecimiento >> check_gold_output
