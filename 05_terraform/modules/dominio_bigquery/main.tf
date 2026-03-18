# ===========================================================================
# Módulo: dominio_bigquery
# Crea dataset Raw en BigQuery para un dominio del Data Mesh.
# El dataset Curated (lake_ecommerce_curated) es compartido — no se recrea.
# ===========================================================================

resource "google_bigquery_dataset" "raw" {
  dataset_id    = "lake_${var.dominio}_raw"
  friendly_name = "Raw — Dominio ${title(var.dominio)}"
  description   = "Capa Raw del dominio ${var.dominio} en el Data Mesh E-commerce"
  location      = var.location
  project       = var.project_id

  labels = {
    dominio = var.dominio
    capa    = "raw"
    mesh    = "data-mesh-ecommerce"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role           = "EDITOR"
    user_by_email  = "sa-${var.dominio}@${var.project_id}.iam.gserviceaccount.com"
  }
}

resource "google_bigquery_dataset" "curated" {
  count         = var.crear_curated ? 1 : 0
  dataset_id    = "lake_${var.dominio}_curated"
  friendly_name = "Curated — Dominio ${title(var.dominio)}"
  description   = "Capa Silver/Gold del dominio ${var.dominio} en el Data Mesh E-commerce"
  location      = var.location
  project       = var.project_id

  labels = {
    dominio = var.dominio
    capa    = "curated"
    mesh    = "data-mesh-ecommerce"
  }
}
