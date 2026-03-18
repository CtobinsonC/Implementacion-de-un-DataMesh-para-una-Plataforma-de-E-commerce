-- =============================================================================
-- Dominio de Pedidos/Marketing: Preparación de Vistas en PostgreSQL
-- Propósito: Tipificar y limpiar datos en la base operacional PostgreSQL 
-- antes de ser consultados vía consulta federada desde BigQuery.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS "public";

-- VISTA DE RESEÑAS (Para cálculos de satisfacción)
CREATE OR REPLACE VIEW "public"."v_resenas" AS
SELECT 
  "review_id",
  "order_id",
  CAST("review_score" AS INTEGER) AS "score", -- Ahora es un número para promediar
  "review_comment_title",
  "review_comment_message",
  TO_TIMESTAMP("review_creation_date", 'YYYY-MM-DD HH24:MI:SS') AS "creation_date"
FROM "public"."resenas";

-- VISTA DE LOCALIZACIÓN (Para análisis geoespacial)
CREATE OR REPLACE VIEW "public"."v_localizacion" AS
SELECT 
  "zip_code",
  CAST("lat" AS NUMERIC) AS "latitud",
  CAST("lng" AS NUMERIC) AS "longitud",
  "city",
  "state"
FROM "public"."geolocalizacion";

-- VISTA DE CLIENTES
CREATE OR REPLACE VIEW "public"."v_clientes" AS
SELECT 
  "customer_id",
  "customer_unique_id",
  "customer_zip_code_prefix",
  "customer_city",
  "customer_state"
FROM "public"."clientes";