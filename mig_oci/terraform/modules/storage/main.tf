# ============================================
# Provider - Módulo precisa declarar qual provider usa
# ============================================
terraform {
  required_providers {
    oci = {
      source = "oracle/oci"
    }
  }
}

# ============================================
# Módulo Storage - Object Storage Buckets
# ============================================
# Cria 6 buckets seguindo o Medallion Architecture:
#
# ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
# │ landing-zone│ →  │   bronze    │ →  │   silver    │ →  │    gold     │
# │ (dados raw) │    │ (metadados) │    │ (tipado)    │    │ (ABT v1-v6) │
# └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
#
# + models (artefatos do modelo LightGBM)
# + tfstate (state remoto do Terraform - futuro)
#
# Equivalência com Databricks:
# /Volumes/hackathon_2025/default/bronze/ → bucket bronze-layer
# /Volumes/hackathon_2025/default/silver/ → bucket silver-layer
# /Volumes/hackathon_2025/default/gold/   → bucket gold-layer


# ============================================
# 1. NAMESPACE (identificador único da tenancy)
# ============================================
# O namespace é um identificador globalmente único atribuído à sua tenancy.
# Necessário para construir URLs de acesso aos buckets:
#   oci://<bucket>@<namespace>/path/to/object

data "oci_objectstorage_namespace" "current" {
  compartment_id = var.storage_compartment_id
}


# ============================================
# 2. BUCKETS - Medallion Architecture
# ============================================

# Landing Zone - Dados brutos + scripts Python
# Primeiro ponto de entrada: dados CSV/Parquet vindos do Databricks
resource "oci_objectstorage_bucket" "landing_zone" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-landing-zone"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}

# Bronze Layer - Dados com metadados (schema-on-read)
resource "oci_objectstorage_bucket" "bronze_layer" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-bronze-layer"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}

# Silver Layer - Dados tipados e validados
resource "oci_objectstorage_bucket" "silver_layer" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-silver-layer"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}

# Gold Layer - ABT v1-v6 (feature tables + ABT final)
resource "oci_objectstorage_bucket" "gold_layer" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-gold-layer"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}

# Models - Artefatos do modelo (LightGBM, métricas, feature importance)
resource "oci_objectstorage_bucket" "models" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-models"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}

# Terraform State - State remoto para colaboração em time
# Após criar este bucket, migrar state local → remoto
resource "oci_objectstorage_bucket" "tfstate" {
  compartment_id = var.storage_compartment_id
  namespace      = data.oci_objectstorage_namespace.current.namespace
  name           = "${var.project_name}-tfstate"
  access_type    = "NoPublicAccess"
  versioning     = "Enabled"

  freeform_tags = var.tags
}


# ============================================
# 3. LIFECYCLE POLICIES (opcional)
# ============================================
# Arquiva dados antigos para reduzir custos.
# Desabilitado por padrão (enable_lifecycle_policies = false).
# No Hackathon, janela é de 30 dias, então não precisa.

resource "oci_objectstorage_object_lifecycle_policy" "landing_lifecycle" {
  count     = var.enable_lifecycle_policies ? 1 : 0
  namespace = data.oci_objectstorage_namespace.current.namespace
  bucket    = oci_objectstorage_bucket.landing_zone.name

  rules {
    name        = "archive-after-60-days"
    action      = "ARCHIVE"
    time_amount = 60
    time_unit   = "DAYS"
    is_enabled  = true
    target      = "objects"
  }
}
