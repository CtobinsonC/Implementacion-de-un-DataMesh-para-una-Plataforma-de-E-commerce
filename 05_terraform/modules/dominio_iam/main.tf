# ===========================================================================
# Módulo: dominio_iam
# Crea el Service Account y roles mínimos para un dominio del Data Mesh.
# Principio: mínimo privilegio — cada dominio tiene su propio SA.
# ===========================================================================

resource "google_service_account" "dominio_sa" {
  account_id   = "sa-${var.dominio}"
  display_name = "SA — Dominio ${title(var.dominio)} (Data Mesh)"
  description  = "Service Account del dominio ${var.dominio}. Accede a BigQuery y Cloud SQL."
  project      = var.project_id
}

# Permiso: leer/escribir en BigQuery (solo su dataset)
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dominio_sa.email}"
}

# Permiso: ejecutar queries en BigQuery
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dominio_sa.email}"
}

# Permiso: conectarse a Cloud SQL
resource "google_project_iam_member" "cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.dominio_sa.email}"
}

# Permiso: leer/escribir en GCS (para staging de Dataproc/Dataflow)
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dominio_sa.email}"
}
