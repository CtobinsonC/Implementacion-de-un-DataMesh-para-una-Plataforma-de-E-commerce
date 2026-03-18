variable "dominio"    { type = string; description = "Nombre del dominio (ej: finanzas)" }
variable "project_id" { type = string }
variable "region"     { type = string; default = "us-central1" }
variable "engine"     { type = string; default = "POSTGRES_15" }
variable "tier"       { type = string; default = "db-f1-micro" }
variable "db_password" { type = string; sensitive = true }

output "instance_connection_name" {
  value = google_sql_database_instance.dominio.connection_name
}
output "database_name" { value = google_sql_database.dominio_db.name }
output "user_name"     { value = google_sql_user.dominio_user.name }
