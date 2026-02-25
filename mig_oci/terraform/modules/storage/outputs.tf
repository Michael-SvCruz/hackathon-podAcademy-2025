# ============================================
# Outputs do Módulo Storage
# ============================================
# Estes valores são usados por:
# - Compute: Data Flow precisa dos nomes dos buckets para ler/escrever
# - Upload scripts: precisa do namespace para construir paths
# - Airflow: precisa do namespace + bucket names para configurar DAGs

output "namespace" {
  description = "Namespace do Object Storage (identificador unico da tenancy)"
  value       = data.oci_objectstorage_namespace.current.namespace
}

output "bucket_landing_zone" {
  description = "Nome do bucket landing-zone (apenas dados brutos — source/)"
  value       = oci_objectstorage_bucket.landing_zone.name
}

output "bucket_pipeline_ops" {
  description = "Nome do bucket pipeline-ops (scripts/, libs/, logs/)"
  value       = oci_objectstorage_bucket.pipeline_ops.name
}

output "bucket_bronze_layer" {
  description = "Nome do bucket bronze-layer"
  value       = oci_objectstorage_bucket.bronze_layer.name
}

output "bucket_silver_layer" {
  description = "Nome do bucket silver-layer"
  value       = oci_objectstorage_bucket.silver_layer.name
}

output "bucket_gold_layer" {
  description = "Nome do bucket gold-layer"
  value       = oci_objectstorage_bucket.gold_layer.name
}

output "bucket_models" {
  description = "Nome do bucket models (artefatos do modelo)"
  value       = oci_objectstorage_bucket.models.name
}

output "bucket_tfstate" {
  description = "Nome do bucket tfstate (state remoto Terraform)"
  value       = oci_objectstorage_bucket.tfstate.name
}
