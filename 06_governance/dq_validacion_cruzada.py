"""
=============================================================================
Script Data Quality: Validación Cruzada (Spanner vs PostgreSQL)
Herramienta: Pseudo Great Expectations / BigQuery Native DQ
Propósito: Validar la integridad referencial entre dominios descentralizados.
Asegura que los order_id de las reseñas (PostgreSQL) existan en los pedidos (Spanner).
=============================================================================
"""

from google.cloud import bigquery
import sys

# Configuración
PROJECT_ID = "gcp-implementacion-datamesh"
DATASET = "lake_ecommerce_curated"

def run_data_quality_check():
    client = bigquery.Client(project=PROJECT_ID)
    
    print("Iniciando Validación de Calidad de Datos (Data Mesh)...")
    print("-----------------------------------------------------------------")
    print("Regla: 'expect_foreign_key_to_exist_across_domains'")
    print(f"Origen (Hijo): {DATASET}.resenas_limpias (Dominio PostgreSQL)")
    print(f"Destino (Padre): {DATASET}.pedidos_limpios (Dominio Spanner)")
    print("-----------------------------------------------------------------")

    # Consulta SQL para encontrar registros huérfanos (anomalías)
    # Buscamos order_id en Postgres que NO existan en Spanner
    query = f"""
        SELECT 
            r.review_id,
            r.order_id as postgres_order_id
        FROM `{PROJECT_ID}.{DATASET}.resenas_limpias` r
        LEFT JOIN `{PROJECT_ID}.{DATASET}.pedidos_limpios` p 
            ON r.order_id = p.order_id
        WHERE p.order_id IS NULL 
          AND r.order_id IS NOT NULL
    """

    print("⏳ Ejecutando validación en BigQuery...")
    query_job = client.query(query)
    resultados = list(query_job.result())
    
    anomalias = len(resultados)
    
    if anomalias == 0:
        print("\n✅ RESULTADO: SUCCESS")
        print("La integridad referencial cruzada es perfecta. Todos los pedidos de "
              "las reseñas de PostgreSQL existen en el sistema transaccional de Spanner.")
        sys.exit(0)
    else:
        print("\n❌ RESULTADO: FAILED")
        print(f"Se encontraron {anomalias} anomalías (reseñas huérfanas sin pedido asociado).")
        print("Muestra de los primeros 5 order_id huérfanos:")
        for row in resultados[:5]:
            print(f" - Review ID: {row.review_id} | Order ID huérfano: {row.postgres_order_id}")
        sys.exit(1)

if __name__ == "__main__":
    run_data_quality_check()
