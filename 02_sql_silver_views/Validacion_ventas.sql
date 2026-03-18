-- =============================================================================
-- Dominio Transaccional (Spanner): Vista de Ventas Validadas
-- Propósito: Filtrar ventas canceladas y asegurar que los items estén asociados.
-- Esta vista se expone luego hacia BigQuery como 'ventas_validadas'.
-- =============================================================================

CREATE OR REPLACE VIEW ventas_validadas_v1 
SQL SECURITY INVOKER AS
SELECT 
  p.order_id,
  p.customer_id,
  p.order_status,
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', p.order_purchase_timestamp) as purchase_at,
  i.product_id,
  CAST(i.price AS FLOAT64) as price,
  CAST(i.freight_value AS FLOAT64) as freight,
  pay.payment_type,
  CAST(pay.payment_value AS FLOAT64) as total_payment
FROM pedidos AS p
INNER JOIN items_pedidos AS i ON p.order_id = i.order_id
INNER JOIN pagos_pedidos AS pay ON p.order_id = pay.order_id
WHERE i.price IS NOT NULL 
  AND i.price != ''
  AND p.order_status != 'canceled';