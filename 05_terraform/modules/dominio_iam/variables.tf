variable "dominio"    { type = string; description = "Nombre del dominio (ej: finanzas)" }
variable "project_id" { type = string }

output "service_account_email" {
  value = google_service_account.dominio_sa.email
}
