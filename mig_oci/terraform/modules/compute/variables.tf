# ============================================
# Variáveis do Módulo Compute
# ============================================
# Recebe IDs dos módulos IAM e Storage,
# além de configurações de recursos do pipeline.


# --- Dependências de outros módulos ---

variable "compute_compartment_id" {
  description = "OCID do compartment de compute (vem do módulo IAM)"
  type        = string
}

variable "namespace" {
  description = "Namespace do Object Storage (vem do módulo Storage)"
  type        = string
}

variable "bucket_pipeline_ops" {
  description = "Nome do bucket pipeline-ops (scripts/, libs/, logs/ do Data Flow)"
  type        = string
}


# --- Mapa de aplicações Data Flow ---
# Cada entrada define uma aplicação com sua própria configuração de shape.
# Apps em grupos paralelos usam shapes de famílias diferentes para evitar
# competição de quota no OCI free tier.
#
# Shapes fixos (Standard2.1, Standard2.2): não precisam de ocpus/memory.
# Shapes Flex (E3, E4, Standard3, A1): precisam de ocpus e memory.

variable "dataflow_applications" {
  description = "Mapa de aplicações Data Flow com configuração de shape por app"
  type = map(object({
    display_name    = string
    script_name     = string
    driver_shape    = string
    executor_shape  = string
    driver_ocpus    = optional(number)
    driver_memory   = optional(number)
    executor_ocpus  = optional(number)
    executor_memory = optional(number)
    min_executors   = number
    max_executors   = number
  }))
}


# --- Configuração geral ---

variable "spark_version" {
  description = "Versão do Apache Spark (OCI Data Flow suporta 3.2.1 e 3.5.0)"
  type        = string
  default     = "3.5.0"
}

variable "project_name" {
  description = "Nome do projeto (prefixo para nomes de aplicações)"
  type        = string
  default     = "hackathon-2025"
}

variable "tags" {
  description = "Tags para organização e controle de custos"
  type        = map(string)
  default     = {}
}
