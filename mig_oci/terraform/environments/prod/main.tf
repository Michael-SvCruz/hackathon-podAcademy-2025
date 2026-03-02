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
# FASE 5: Airflow VM (Docker Compose self-hosted)
# ============================================
module "airflow" {
  source = "../../modules/airflow"

  tenancy_ocid             = var.tenancy_ocid
  compartment_id           = module.iam.compute_compartment_id
  project_compartment_name = var.project_name
  public_subnet_id         = module.network.public_subnet_id
  vcn_id                   = module.network.vcn_id
  ssh_public_key           = var.airflow_ssh_public_key
  project_name             = var.project_name
  shape                    = "VM.Standard.E3.Flex"
  ocpus                    = 1
  memory_in_gbs            = 16
  tags                     = var.tags
}


# ============================================
# FASE 4: Compute (Data Flow Applications)
# ============================================
module "compute" {
  source = "../../modules/compute"

  compute_compartment_id = module.iam.compute_compartment_id
  namespace              = module.storage.namespace
  bucket_pipeline_ops    = module.storage.bucket_pipeline_ops
  dataflow_applications  = var.dataflow_applications
  project_name           = var.project_name
  spark_version          = "3.5.0"
  tags                   = var.tags
}
