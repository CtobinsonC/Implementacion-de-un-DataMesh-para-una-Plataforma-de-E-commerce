"""
reabastecimiento.py
====================
Job PySpark para transformación Silver → Gold (Arquitectura Medallón).

Origen A : lake_ecommerce_raw.productos        (BigQuery via Datastream/MySQL)
Origen B : lake_ecommerce_public.v_analisis_ventas_360 (BigQuery via Spanner)
Destino  : lake_ecommerce_curated.gold_reabastecimiento

Lógica de negocio:
  - Inner Join por product_id
  - Sales Velocity = total_ventas / product_weight_g
  - reorder_status: URGENTE (>0.5) | MONITORIZAR (>0.2) | ESTABLE
  - Añade processing_timestamp

Ejecución en Dataproc:
  gcloud dataproc jobs submit pyspark gs://<BUCKET>/reabastecimiento.py \\
    --cluster=<CLUSTER> --region=<REGION> \\
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \\
    -- --project=<PROJECT_ID> --temp_bucket=<TEMP_BUCKET>
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuración de logging estructurado
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("reabastecimiento")


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
APP_NAME = "silver_to_gold_reabastecimiento"

# Tablas BigQuery — siempre leer de la capa Silver (Curated), no de Raw
TABLE_PRODUCTOS = "lake_ecommerce_curated.productos_limpios"  # Silver: tipos ya corregidos
TABLE_VENTAS    = "lake_ecommerce_curated.ventas_limpias"      # Silver: product_id + precio por pedido
TABLE_GOLD      = "lake_ecommerce_curated.gold_reabastecimiento"  # Gold: producto de datos final

# Umbrales de velocidad de ventas
THRESHOLD_URGENTE = 0.5
THRESHOLD_MONITORIZAR = 0.2


# ---------------------------------------------------------------------------
# Inicialización de SparkSession
# ---------------------------------------------------------------------------
def create_spark_session(project_id: str, temp_bucket: str) -> SparkSession:
    """
    Crea y configura la SparkSession con el conector de BigQuery.

    Args:
        project_id:  ID del proyecto de GCP.
        temp_bucket: Bucket GCS para archivos temporales del conector BQ.

    Returns:
        SparkSession configurada.
    """
    logger.info("Inicializando SparkSession para proyecto '%s'.", project_id)
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.repl.eagerEval.enabled", True)
        # Guardar en SparkConf para recuperarlos en funciones de lectura
        .config("bq.project_id",   project_id)
        .config("bq.temp_bucket",  temp_bucket)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession creada correctamente. Versión Spark: %s", spark.version)
    return spark


# ---------------------------------------------------------------------------
# Lectura de datos
# ---------------------------------------------------------------------------
def read_bigquery_table(
    spark: SparkSession,
    table: str,
    bq_options: dict,
) -> DataFrame:
    """
    Lee una tabla o vista de BigQuery usando el conector oficial.

    La única forma fiable con el conector 0.30.0 de pasar viewsEnabled
    es via .option() directamente en el DataFrameReader — ni spark.conf
    ni hadoopConfiguration() se propagan al JAR en todos los entornos.

    Args:
        spark:      SparkSession activa.
        table:      Nombre completo de la tabla (dataset.tabla).
        bq_options: Diccionario de opciones del conector BigQuery.
                    Debe incluir siempre: parentProject, temporaryGcsBucket,
                    viewsEnabled, materializationProject, materializationDataset.

    Returns:
        DataFrame con los datos de la tabla.
    """
    logger.info("Leyendo desde BigQuery: '%s'.", table)
    reader = spark.read.format("bigquery").option("table", table)
    for key, value in bq_options.items():
        reader = reader.option(key, value)
    df = reader.load()
    # Nota: NO se llama df.count() aqui para evitar una Action de Spark
    # que forzaria la materializacion completa (SELECT *) de la vista en BQ,
    # agotando el limite de CPU-segundos del modelo on-demand.
    logger.info(
        "Lectura completada '%s': esquema = %s.",
        table,
        [f.name for f in df.schema.fields],
    )
    return df


def build_bq_options(project_id: str, temp_bucket: str) -> dict:
    """
    Construye el diccionario de opciones base para el conector BigQuery.

    viewsEnabled=true es necesario tanto para vistas de BigQuery como para
    tablas replicadas via Datastream que BigQuery expone como vistas autorizadas.

    Args:
        project_id:  ID del proyecto GCP.
        temp_bucket: Bucket GCS para materializar vistas temporalmente.

    Returns:
        Diccionario listo para pasar a read_bigquery_table().
    """
    return {
        "parentProject":          project_id,
        "temporaryGcsBucket":     temp_bucket,
        "viewsEnabled":           "true",
        "materializationProject": project_id,
        "materializationDataset": "lake_ecommerce_curated",
    }


def read_inventario(spark: SparkSession, bq_options: dict) -> DataFrame:
    """
    Lee la tabla Silver 'productos_limpios' desde lake_ecommerce_curated.

    Leemos de la capa Silver y no de Raw porque los tipos ya estan
    corregidos por el pipeline de Datastream (product_weight_g es FLOAT64,
    no STRING). Aplicamos solo los filtros de negocio necesarios.
    """
    sql = """
        SELECT
            CAST(product_id AS STRING) AS product_id,
            peso_gramos
        FROM `{table}`
        WHERE product_id  IS NOT NULL
          AND peso_gramos IS NOT NULL
          AND peso_gramos > 0
    """.format(table=TABLE_PRODUCTOS)

    reader = spark.read.format("bigquery").option("query", sql)
    for key, value in bq_options.items():
        reader = reader.option(key, value)
    df = reader.load()
    logger.info("Inventario Silver cargado: esquema = %s.",
                [f.name for f in df.schema.fields])
    return df


def read_ventas(spark: SparkSession, bq_options: dict) -> DataFrame:
    """
    Lee ventas_limpias y agrega el total de pedidos por producto.

    La tabla ventas_limpias opera al nivel de linea de pedido
    (id_pedido + product_id + precio). Aqui agregamos COUNT(id_pedido)
    por product_id para obtener 'total_ventas': cuantas veces fue
    vendido cada producto, que es la metrica correcta para calcular
    la velocidad de ventas en el pipeline de reabastecimiento.

    Args:
        spark:      SparkSession activa.
        bq_options: Opciones del conector BigQuery.

    Returns:
        DataFrame con columnas [product_id (STRING), total_ventas (INT64)].
    """
    sql = """
        SELECT
            CAST(product_id AS STRING)  AS product_id,
            COUNT(id_pedido)            AS total_ventas
        FROM `{table}`
        WHERE product_id IS NOT NULL
        GROUP BY product_id
    """.format(table=TABLE_VENTAS)

    reader = spark.read.format("bigquery").option("query", sql)
    for key, value in bq_options.items():
        reader = reader.option(key, value)
    df = reader.load()
    logger.info("Ventas agregadas por product_id: esquema = %s.",
                [f.name for f in df.schema.fields])
    return df


# ---------------------------------------------------------------------------
# Transformaciones de negocio
# ---------------------------------------------------------------------------
def join_inventario_ventas(
    df_inventario: DataFrame, df_ventas: DataFrame
) -> DataFrame:
    """
    Realiza un Inner Join entre inventario y ventas por product_id.

    Args:
        df_inventario: DataFrame con datos de productos.
        df_ventas:     DataFrame con datos de ventas.

    Returns:
        DataFrame combinado.
    """
    logger.info("Ejecutando Inner Join por 'product_id'.")
    df_joined = df_inventario.join(df_ventas, on="product_id", how="inner")
    logger.info("Registros tras el join: %d.", df_joined.count())
    return df_joined


def calculate_sales_velocity(df: DataFrame) -> DataFrame:
    """
    Calcula la metrica 'sales_velocity' = total_ventas / peso_gramos.

    La division es segura porque peso_gramos fue filtrado > 0.

    Args:
        df: DataFrame con columnas total_ventas y peso_gramos.

    Returns:
        DataFrame con la columna 'sales_velocity' anadida.
    """
    logger.info("Calculando 'sales_velocity'.")
    return df.withColumn(
        "sales_velocity",
        F.round(
            F.col("total_ventas") / F.col("peso_gramos"),
            6,
        ),
    )


def classify_reorder_status(df: DataFrame) -> DataFrame:
    """
    Clasifica cada producto según su velocidad de ventas.

    Reglas:
      - sales_velocity >  THRESHOLD_URGENTE     → 'URGENTE'
      - sales_velocity >  THRESHOLD_MONITORIZAR → 'MONITORIZAR'
      - En caso contrario                       → 'ESTABLE'

    Args:
        df: DataFrame con la columna 'sales_velocity'.

    Returns:
        DataFrame con la columna 'reorder_status' añadida.
    """
    logger.info(
        "Clasificando 'reorder_status' (URGENTE > %.1f | MONITORIZAR > %.1f).",
        THRESHOLD_URGENTE,
        THRESHOLD_MONITORIZAR,
    )
    return df.withColumn(
        "reorder_status",
        F.when(F.col("sales_velocity") > THRESHOLD_URGENTE, "URGENTE")
        .when(F.col("sales_velocity") > THRESHOLD_MONITORIZAR, "MONITORIZAR")
        .otherwise("ESTABLE"),
    )


def add_processing_timestamp(df: DataFrame) -> DataFrame:
    """
    Añade la columna 'processing_timestamp' con el instante de ejecución UTC.

    Args:
        df: DataFrame de entrada.

    Returns:
        DataFrame con la columna 'processing_timestamp'.
    """
    logger.info("Añadiendo 'processing_timestamp'.")
    return df.withColumn("processing_timestamp", F.current_timestamp())


def build_gold_table(df: DataFrame) -> DataFrame:
    """
    Ensambla el esquema final de la tabla Gold seleccionando y ordenando
    las columnas en el orden canónico del Data Product.

    Args:
        df: DataFrame con todas las columnas calculadas.

    Returns:
        DataFrame listo para escritura en BigQuery.
    """
    gold_columns = [
        "product_id",
        "peso_gramos",
        "total_ventas",
        "sales_velocity",
        "reorder_status",
        "processing_timestamp",
    ]
    logger.info("Seleccionando columnas Gold: %s", gold_columns)
    return df.select(*gold_columns)


# ---------------------------------------------------------------------------
# Escritura en BigQuery
# ---------------------------------------------------------------------------
def write_to_bigquery(df: DataFrame, table: str, temp_bucket: str) -> None:
    """
    Escribe el DataFrame en BigQuery en modo 'overwrite'.

    Se aplica coalesce(1) previamente dado que el dataset es pequeno,
    minimizando el numero de objetos temporales en GCS.

    Args:
        df:          DataFrame Gold a escribir.
        table:       Tabla destino en formato 'dataset.tabla'.
        temp_bucket: Bucket GCS temporal requerido por el conector al escribir.
    """
    logger.info(
        "Escribiendo %d filas en BigQuery tabla '%s' (modo overwrite).",
        df.count(),
        table,
    )
    (
        df.coalesce(1)
        .write.format("bigquery")
        .option("table", table)
        .option("temporaryGcsBucket", temp_bucket)
        .mode("overwrite")
        .save()
    )
    logger.info("Escritura completada en '%s'.", table)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def run_pipeline(project_id: str, temp_bucket: str) -> None:
    """
    Orquesta el pipeline completo Silver → Gold.

    Pasos:
      1. Crear SparkSession
      2. Leer orígenes (inventario y ventas)
      3. Aplicar transformaciones de negocio
      4. Escribir tabla Gold

    Args:
        project_id:  ID del proyecto GCP.
        temp_bucket: Bucket temporal para el conector BigQuery.
    """
    logger.info("=== Inicio del pipeline Silver → Gold: reabastecimiento ===")

    spark = create_spark_session(project_id, temp_bucket)

    # Opciones del conector BigQuery pasadas directamente a cada .option()
    # Esta es la única forma fiable con spark-bigquery 0.30.0.
    bq_options = build_bq_options(project_id, temp_bucket)

    try:
        # -- Lectura ----------------------------------------------------------
        df_inventario = read_inventario(spark, bq_options)
        df_ventas = read_ventas(spark, bq_options)

        # -- Transformaciones -------------------------------------------------
        df_joined = join_inventario_ventas(df_inventario, df_ventas)
        df_velocity = calculate_sales_velocity(df_joined)
        df_status = classify_reorder_status(df_velocity)
        df_timestamped = add_processing_timestamp(df_status)
        df_gold = build_gold_table(df_timestamped)

        # -- Escritura --------------------------------------------------------
        write_to_bigquery(df_gold, TABLE_GOLD, temp_bucket)

        logger.info("=== Pipeline finalizado con éxito ===")

    except Exception as exc:  # noqa: BLE001
        logger.exception("Error crítico en el pipeline: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession cerrada.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parsea los argumentos de línea de comandos del job Dataproc."""
    parser = argparse.ArgumentParser(
        description="Silver → Gold: cálculo de Sales Velocity y reabastecimiento."
    )
    parser.add_argument(
        "--project",
        required=True,
        help="ID del proyecto de GCP (ej. mi-proyecto-123).",
    )
    parser.add_argument(
        "--temp_bucket",
        required=True,
        help="Bucket GCS para archivos temporales del conector BigQuery (sin gs://).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(project_id=args.project, temp_bucket=args.temp_bucket)
