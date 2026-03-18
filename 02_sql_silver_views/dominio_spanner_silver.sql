-- =============================================================================
-- Dominio Transaccional: Spanner → BigQuery (Type Casting Silver Layer)
-- Fuente: lake_ecommerce_raw (tablas replicadas desde Spanner via Dataflow)
-- Destino: lake_ecommerce_curated (vistas Silver con tipos correctos)
-- Ejecutar en: BigQuery Console (proyecto gcp-implementacion-datamesh)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Vista Silver: Items de Pedidos con precios como FLOAT64
-- Origen: lake_ecommerce_raw.items_pedidos
-- Los campos price y freight_value vienen como STRING desde Spanner
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.items_pedidos_limpios` AS
SELECT
  CAST(order_id          AS STRING)  AS order_id,
  CAST(order_item_id     AS STRING)  AS item_numero,
  CAST(product_id        AS STRING)  AS product_id,
  CAST(seller_id         AS STRING)  AS seller_id,
  SAFE_CAST(price         AS FLOAT64) AS precio,
  SAFE_CAST(freight_value AS FLOAT64) AS flete,
  SAFE.PARSE_TIMESTAMP(
    '%Y-%m-%d %H:%M:%E*S',
    shipping_limit_date
  )                                   AS fecha_limite_envio
FROM `gcp-implementacion-datamesh.lake_ecommerce_raw.items_pedidos`
WHERE order_id   IS NOT NULL
  AND product_id IS NOT NULL
  AND SAFE_CAST(price AS FLOAT64) IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Verificación de la vista
-- -----------------------------------------------------------------------------
-- SELECT
--   COUNT(*)                    AS total_items,
--   ROUND(AVG(precio), 2)       AS precio_promedio,
--   ROUND(SUM(precio), 2)       AS ingresos_totales,
--   COUNTIF(precio IS NULL)     AS precios_nulos
-- FROM `lake_ecommerce_curated.items_pedidos_limpios`;
