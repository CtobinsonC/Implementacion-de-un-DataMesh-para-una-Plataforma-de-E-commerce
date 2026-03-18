-- =============================================================================
-- Dominio Transaccional (Spanner): Vistas Silver via EXTERNAL_QUERY
-- Conexión: gcp-implementacion-datamesh.us-central1.conn-spanner
-- Tablas disponibles: items_pedidos, pagos_pedidos, pedidos, ventas_validadas_v1
-- Ejecutar en: BigQuery Console
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Vista Silver: Pagos (desde Spanner → pagos_pedidos_v1)
-- Usamos la versión _v1 que ya tiene los tipos corregidos en Spanner
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.pagos_limpios` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn-spanner",
  "SELECT * FROM pagos_pedidos_v1"
);

-- -----------------------------------------------------------------------------
-- 2. Vista Silver: Pedidos (desde Spanner → pedidos_v1)
-- Incluye fechas ya convertidas a TIMESTAMP en la versión _v1
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.pedidos_limpios` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn-spanner",
  "SELECT * FROM pedidos_v1"
);

-- -----------------------------------------------------------------------------
-- 3. Vista Silver: Ventas validadas (desde Spanner → ventas_validadas_v1)
-- Vista ya consolidada en Spanner con validaciones de negocio
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.ventas_validadas` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn-spanner",
  "SELECT * FROM ventas_validadas_v1"
);

-- -----------------------------------------------------------------------------
-- Validación: contar registros en cada vista
-- -----------------------------------------------------------------------------
-- SELECT 'pagos_limpios'     AS vista, COUNT(*) AS total FROM `lake_ecommerce_curated.pagos_limpios`
-- UNION ALL
-- SELECT 'pedidos_limpios',           COUNT(*)          FROM `lake_ecommerce_curated.pedidos_limpios`
-- UNION ALL
-- SELECT 'ventas_validadas',          COUNT(*)          FROM `lake_ecommerce_curated.ventas_validadas`;
