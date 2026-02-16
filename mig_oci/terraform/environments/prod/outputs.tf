# ============================================
# Outputs do Environment Prod
# ============================================
# Estes outputs são exibidos ao executar:
#   terraform output
#   terraform output -json > config.json
#
# São usados para:
# 1. Verificar se os recursos foram criados corretamente
# 2. Obter IDs para configurar Airflow
# 3. Passar informações entre fases


# --- Fase 1: IAM ---

output "compartment_ids" {
  description = "OCIDs dos compartments criados"
  value = {
    project  = module.iam.project_compartment_id
    network  = module.iam.network_compartment_id
    storage  = module.iam.storage_compartment_id
    compute  = module.iam.compute_compartment_id
    data     = module.iam.data_compartment_id
    security = module.iam.security_compartment_id
  }
}

output "group_ids" {
  description = "OCIDs dos grupos IAM criados"
  value = {
    administrators  = module.iam.administrators_group_id
    data_engineers  = module.iam.data_engineers_group_id
    data_scientists = module.iam.data_scientists_group_id
  }
}


# --- Fase 2: Network ---

output "network_ids" {
  description = "IDs dos recursos de rede"
  value = {
    vcn_id                 = module.network.vcn_id
    public_subnet_id       = module.network.public_subnet_id
    private_data_subnet_id = module.network.private_data_subnet_id
    private_app_subnet_id  = module.network.private_app_subnet_id
  }
}


# --- Fase 3: Storage ---

output "storage_info" {
  description = "Informacoes dos buckets"
  value = {
    namespace      = module.storage.namespace
    bucket_landing = module.storage.bucket_landing_zone
    bucket_bronze  = module.storage.bucket_bronze_layer
    bucket_silver  = module.storage.bucket_silver_layer
    bucket_gold    = module.storage.bucket_gold_layer
    bucket_models  = module.storage.bucket_models
    bucket_tfstate = module.storage.bucket_tfstate
  }
}


# --- Fase 4: Compute ---
# Descomentar quando iniciar Fase 4:
#
# output "dataflow_applications" {
#   description = "IDs das aplicacoes Data Flow"
#   value = {
#     bronze_id = module.compute.dataflow_bronze_id
#     silver_id = module.compute.dataflow_silver_id
#     gold_id   = module.compute.dataflow_gold_id
#     abt_id    = module.compute.dataflow_abt_id
#   }
# }
