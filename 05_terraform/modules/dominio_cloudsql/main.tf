# ===========================================================================
# Módulo: dominio_cloudsql
# Crea una instancia Cloud SQL PostgreSQL para un dominio del Data Mesh.
# ===========================================================================

resource "google_sql_database_instance" "dominio" {
  name             = "cloudsql-${var.dominio}"
  database_version = var.engine
  region           = var.region
  project          = var.project_id

  settings {
    tier              = var.tier
    availability_type = "ZONAL"

    ip_configuration {
      ipv4_enabled    = false
      private_network = "projects/${var.project_id}/global/networks/default"
    }

    backup_configuration {
      enabled    = true
      start_time = "03:00"
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  deletion_protection = false  # Cambiar a true en producción real
}

resource "google_sql_database" "dominio_db" {
  name     = "db_${var.dominio}"
  instance = google_sql_database_instance.dominio.name
  project  = var.project_id
}

resource "google_sql_user" "dominio_user" {
  name     = "user_${var.dominio}"
  instance = google_sql_database_instance.dominio.name
  password = var.db_password
  project  = var.project_id
}
