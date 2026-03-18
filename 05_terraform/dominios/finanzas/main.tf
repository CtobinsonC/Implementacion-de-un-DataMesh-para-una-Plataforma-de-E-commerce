# ===========================================================================
# Dominio: Finanzas — Ejemplo de autoservicio con módulos Data Mesh
# Para levantar un nuevo dominio solo se modifican estas variables.
# ===========================================================================

locals {
  dominio    = "finanzas"
  project_id = "gcp-implementacion-datamesh"
  region     = "us-central1"
}

# 1. Service Account e IAM mínimo
module "finanzas_iam" {
  source     = "../../modules/dominio_iam"
  dominio    = local.dominio
  project_id = local.project_id
}

# 2. Cloud SQL PostgreSQL para el dominio
module "finanzas_db" {
  source      = "../../modules/dominio_cloudsql"
  dominio     = local.dominio
  engine      = "POSTGRES_15"
  tier        = "db-f1-micro"       # Cambiar a db-g1-small en producción
  region      = local.region
  project_id  = local.project_id
  db_password = var.db_password_finanzas

  depends_on = [module.finanzas_iam]
}

# 3. Dataset BigQuery Raw para el dominio
module "finanzas_bq" {
  source     = "../../modules/dominio_bigquery"
  dominio    = local.dominio
  project_id = local.project_id
  location   = "US"
  crear_curated = false  # Usa lake_ecommerce_curated compartido

  depends_on = [module.finanzas_iam]
}

# ---------------------------------------------------------------------------
# Outputs: información clave del dominio creado
# ---------------------------------------------------------------------------
output "sa_email" {
  description = "Email del Service Account del dominio Finanzas"
  value       = module.finanzas_iam.service_account_email
}

output "cloudsql_connection" {
  description = "Connection name de la BD PostgreSQL para BigQuery External Connection"
  value       = module.finanzas_db.instance_connection_name
}

output "bq_raw_dataset" {
  description = "Dataset BigQuery Raw del dominio"
  value       = module.finanzas_bq.raw_dataset_id
}
