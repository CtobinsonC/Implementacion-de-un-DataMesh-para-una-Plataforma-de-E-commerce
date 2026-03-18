"""
Script de Gobernanza: Data Catalog (Dataplex) para Data Mesh
Propósito:
1. Crea un Tag Template ('data_mesh_domain_info') para registrar el Source y Owner de los datos.
2. Aplica estas etiquetas a las vistas Silver en BigQuery.

Requisitos:
pip install google-cloud-datacatalog
"""

import os
from google.cloud import datacatalog_v1

PROJECT_ID = "gcp-implementacion-datamesh"
LOCATION = "us-central1"
DATASET_ID = "lake_ecommerce_curated"

def setup_datacatalog():
    client = datacatalog_v1.DataCatalogClient()
    location_path = f"projects/{PROJECT_ID}/locations/{LOCATION}"

    # -----------------------------------------------------------------------
    # 1. Crear el Tag Template
    # -----------------------------------------------------------------------
    template_id = "data_mesh_domain_info"
    template_path = f"{location_path}/tagTemplates/{template_id}"

    # Construir el template
    template = datacatalog_v1.TagTemplate()
    template.display_name = "Data Mesh Domain Info"

    # Campo: Source (Enum)
    template.fields["source"].display_name = "Sistema Origen"
    template.fields["source"].type_.enum_type.allowed_values = [
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="MySQL"),
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="Postgres"),
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="Spanner"),
    ]

    # Campo: Owner (Enum)
    template.fields["owner"].display_name = "Equipo Propietario"
    template.fields["owner"].type_.enum_type.allowed_values = [
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="Equipo_Inventario"),
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="Logistica"),
        datacatalog_v1.FieldType.EnumType.EnumValue(display_name="Operaciones"),
    ]

    try:
        # Intentar crear el template
        client.create_tag_template(
            parent=location_path,
            tag_template_id=template_id,
            tag_template=template
        )
        print(f"✅ Tag Template '{template_id}' creado exitosamente.")
    except Exception as e:
        if "Already exists" in str(e):
            print(f"ℹ️ El Tag Template '{template_id}' ya existe.")
        else:
            print(f"❌ Error creando template: {e}")
            return

    # -----------------------------------------------------------------------
    # 2. Diccionario de vistas a etiquetar
    # -----------------------------------------------------------------------
    vistas_a_etiquetar = [
        # (Table/View Name, Source Enum, Owner Enum)
        ("productos_limpios", "MySQL", "Equipo_Inventario"),
        ("pedidos_limpios", "Spanner", "Operaciones"),
        ("clientes_limpios_pg", "Postgres", "Logistica"),
    ]

    # -----------------------------------------------------------------------
    # 3. Aplicar las Etiquetas a BigQuery
    # -----------------------------------------------------------------------
    for view_id, source, owner in vistas_a_etiquetar:
        # En Data Catalog, los recursos de BigQuery tienen un "linked_resource"
        linked_resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{view_id}"
        
        try:
            # Buscar el entry en Data Catalog correspondiente a la tabla/vista de BigQuery
            request = datacatalog_v1.LookupEntryRequest(linked_resource=linked_resource)
            entry = client.lookup_entry(request=request)
            
            # Crear el Tag
            tag = datacatalog_v1.Tag()
            tag.template = template_path
            
            # Asignar valores a los campos
            tag.fields["source"].enum_value.display_name = source
            tag.fields["owner"].enum_value.display_name = owner
            
            # Aplicar el Tag al Entry
            client.create_tag(parent=entry.name, tag=tag)
            print(f"✅ Etiqueta aplicada a '{view_id}': [Source: {source}, Owner: {owner}]")
            
        except Exception as e:
            if "Already exists" in str(e):
                 print(f"ℹ️ La vista '{view_id}' ya tiene aplicada esta etiqueta.")
            else:
                 print(f"❌ Error etiquetando '{view_id}': {e}")


if __name__ == "__main__":
    print("Iniciando configuración de Data Catalog (Gobernanza)...")
    setup_datacatalog()
    print("Proceso finalizado. Ve a la consola de GCP (Dataplex > Search) para ver los metadatos.")
