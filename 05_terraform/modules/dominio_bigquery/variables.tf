variable "dominio"       { type = string; description = "Nombre del dominio (ej: finanzas)" }
variable "project_id"   { type = string }
variable "location"     { type = string; default = "US" }
variable "crear_curated" { type = bool; default = false; description = "Crear dataset Curated propio (false = usa el compartido)" }

output "raw_dataset_id"     { value = google_bigquery_dataset.raw.dataset_id }
output "curated_dataset_id" {
  value = var.crear_curated ? google_bigquery_dataset.curated[0].dataset_id : "lake_ecommerce_curated"
}
