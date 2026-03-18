#!/bin/bash
# ==============================================================================
# Script de Gobernanza: Dataplex / BigQuery Labels para Data Mesh
# Google Cloud ha deprecado los "Tag Templates" de Data Catalog antiguos.
# La nueva mejor práctica de Gobernanza Federada es usar Etiquetas Nativas (Labels)
# en BigQuery, las cuales son descubiertas automáticamente por Dataplex Search.
# ==============================================================================

PROJECT_ID="gcp-implementacion-datamesh"
DATASET="lake_ecommerce_curated"

echo "Aplicando política de Gobernanza Federada (Metadatos) vía Dataplex/BigQuery..."
echo "--------------------------------------------------------------------------------"

echo "1. Etiquetando Vista: productos_limpios"
echo "   -> Source: MySQL | Owner: Equipo Inventario"
bq update --set_label="source:mysql" --set_label="owner:equipo_inventario" $PROJECT_ID:$DATASET.productos_limpios

echo ""
echo "2. Etiquetando Vista: pedidos_limpios"
echo "   -> Source: Spanner | Owner: Operaciones"
bq update --set_label="source:spanner" --set_label="owner:operaciones" $PROJECT_ID:$DATASET.pedidos_limpios

echo ""
echo "3. Etiquetando Vista: clientes_limpios_pg"
echo "   -> Source: Postgres | Owner: Logistica"
bq update --set_label="source:postgres" --set_label="owner:logistica" $PROJECT_ID:$DATASET.clientes_limpios_pg

echo ""
echo "¡Gobernanza aplicada exitosamente! 🎉"
echo "Los metadatos nativos ahora están integrados. Puedes ir a GCP > Dataplex > Search,"
echo "buscar tus tablas y usar los filtros de 'Labels' (source, owner) en el panel izquierdo."
echo "================================================================================"
