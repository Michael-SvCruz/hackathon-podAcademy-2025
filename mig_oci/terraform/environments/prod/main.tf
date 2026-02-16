# ============================================
# Main.tf - Orquestrador de Módulos
# ============================================
# Este arquivo é o "maestro" que chama cada módulo na ordem correta.
# Cada módulo é como uma "receita" independente que recebe inputs e gera outputs.
#
# Fluxo de dependências:
# Fase 1: IAM (este apply) → compartment IDs
# Fase 2: Network          → VCN, subnet IDs       (precisa de IAM)
# Fase 3: Storage          → bucket names, namespace (precisa de IAM)
# Fase 4: Compute          → Data Flow app IDs      (precisa de IAM + Network + Storage)


# ============================================
# FASE 1: IAM (Compartments + Groups + Policies)
# ============================================
module "iam" {
  source = "../../modules/iam"

  tenancy_ocid = var.tenancy_ocid
  project_name = var.project_name
  tags         = var.tags
}


# ============================================
# FASE 2: Network (VCN + Subnets + Gateways)
# ============================================
module "network" {
  source = "../../modules/network"

  network_compartment_id   = module.iam.network_compartment_id
  project_name             = var.project_name
  vcn_cidr_block           = var.vcn_cidr_block
  public_subnet_cidr       = var.public_subnet_cidr
  private_data_subnet_cidr = var.private_data_subnet_cidr
  private_app_subnet_cidr  = var.private_app_subnet_cidr
  tags                     = var.tags
}


# ============================================
# FASE 3: Storage (Buckets Object Storage)
# ============================================
module "storage" {
  source = "../../modules/storage"

  storage_compartment_id    = module.iam.storage_compartment_id
  project_name              = var.project_name
  enable_lifecycle_policies = var.enable_lifecycle_policies
  tags                      = var.tags
}


# ============================================
# FASE 4: Compute (Data Flow Applications)
# ============================================
# Descomentar quando iniciar Fase 4:
#
# module "compute" {
#   source = "../../modules/compute"
#
#   compute_compartment_id    = module.iam.compute_compartment_id
#   bucket_landing_zone       = module.storage.bucket_landing_zone
#   bucket_bronze_layer       = module.storage.bucket_bronze_layer
#   bucket_silver_layer       = module.storage.bucket_silver_layer
#   bucket_gold_layer         = module.storage.bucket_gold_layer
#   dataflow_bronze_ocpu      = var.dataflow_bronze_ocpu
#   dataflow_silver_ocpu      = var.dataflow_silver_ocpu
#   dataflow_gold_ocpu        = var.dataflow_gold_ocpu
#   dataflow_abt_ocpu         = var.dataflow_abt_ocpu
#   dataflow_bronze_executors = var.dataflow_bronze_executors
#   dataflow_silver_executors = var.dataflow_silver_executors
#   dataflow_gold_executors   = var.dataflow_gold_executors
#   dataflow_abt_executors    = var.dataflow_abt_executors
#   tags                      = var.tags
# }
