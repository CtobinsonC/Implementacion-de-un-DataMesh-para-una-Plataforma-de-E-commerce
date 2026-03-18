-- =============================================================================
-- Utilidades: Queries de testing y validación de tipos (Type Casting)
-- Propósito: Scripts de apoyo para validar que los valores numéricos y fechas
-- están siendo casteados correctamente en las vistas.
-- =============================================================================

-- Sumar flete y precio para ver el total por item
SELECT (price + freight) as total_por_item 
FROM items_pedidos_v1 
LIMIT 5;

-- Contar el total de registros en cada tabla
SELECT 
  (SELECT COUNT(*) FROM pedidos_v1) as total_pedidos,
  (SELECT COUNT(*) FROM items_pedidos_v1) as total_items,
  (SELECT COUNT(*) FROM pagos_pedidos_v1) as total_pagos;


-- Contar registros nulos en las vistas
SELECT 
COUNTIF(purchase_at IS NULL) as fechas_pedidos_nulas,
COUNTIF(price IS NULL) as precios_nulos,
COUNTIF(amount IS NULL) as montos_pagos_nulos
FROM pedidos_v1 AS p
LEFT JOIN items_pedidos_v1 AS i ON p.order_id = i.order_id
LEFT JOIN pagos_pedidos_v1 AS pay ON p.order_id = pay.order_id;


-- Obtener estadisticas de precios
SELECT 
  SUM(price) as suma_total_precios,
  AVG(price) as promedio_precio,
  MAX(price) as precio_maximo
FROM items_pedidos_v1;


-- Contar pedidos que no tienen items
SELECT COUNT(DISTINCT i.order_id) as pedidos_con_items
FROM items_pedidos_v1 i
WHERE i.order_id NOT IN (SELECT order_id FROM pedidos_v1);


-- Contar items que no tienen pedidos
SELECT 
  COUNT(*) as total_items_en_tabla_raw,
  COUNTIF(CAST(price AS FLOAT64) IS NULL) as errores_de_casting_puro
FROM items_pedidos;


-- Contar ventas limpias
SELECT 
  COUNT(*) as total_ventas_limpias,
  SUM(price) as ingresos_reales,
  COUNTIF(price IS NULL) as nulos_restantes
FROM ventas_validadas_v1;