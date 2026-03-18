-- =============================================================================
-- Dominio de Pedidos/Marketing: PostgreSQL → BigQuery (Federated Queries)
-- Conexión: gcp-implementacion-datamesh.us-central1.conn_marketing
-- Tablas reales en PostgreSQL: clientes, geolocalizacion, resenas
-- Ejecutar en: BigQuery Console
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Paso 1: Verificar columnas de cada tabla en PostgreSQL
-- (descomenta y ejecuta antes del CREATE VIEW para confirmar columnas)
-- -----------------------------------------------------------------------------
-- SELECT column_name, data_type
-- FROM EXTERNAL_QUERY(
--   "gcp-implementacion-datamesh.us-central1.conn_marketing",
--   "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'clientes' ORDER BY ordinal_position"
-- );

-- -----------------------------------------------------------------------------
-- 1. Vista Silver: Clientes (desde PostgreSQL → tabla 'clientes')
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.clientes_limpios_pg` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn_marketing",
  "SELECT * FROM clientes WHERE customer_id IS NOT NULL"
);

-- -----------------------------------------------------------------------------
-- 2. Vista Silver: Reseñas (desde PostgreSQL → tabla 'resenas')
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.resenas_limpias` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn_marketing",
  "SELECT * FROM resenas WHERE review_id IS NOT NULL"
);

-- -----------------------------------------------------------------------------
-- 3. Vista Silver: Geolocalización (desde PostgreSQL → tabla 'geolocalizacion')
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `gcp-implementacion-datamesh.lake_ecommerce_curated.geolocalizacion` AS
SELECT *
FROM EXTERNAL_QUERY(
  "gcp-implementacion-datamesh.us-central1.conn_marketing",
  "SELECT * FROM geolocalizacion"
);

-- -----------------------------------------------------------------------------
-- Validación: contar registros en cada vista
-- -----------------------------------------------------------------------------
-- SELECT 'clientes_limpios_pg' AS vista, COUNT(*) AS total FROM `lake_ecommerce_curated.clientes_limpios_pg`
-- UNION ALL
-- SELECT 'resenas_limpias',             COUNT(*)         FROM `lake_ecommerce_curated.resenas_limpias`
-- UNION ALL
-- SELECT 'geolocalizacion',             COUNT(*)         FROM `lake_ecommerce_curated.geolocalizacion`;
