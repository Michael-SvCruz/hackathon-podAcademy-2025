# ============================================
# Variáveis do Módulo Compute
# ============================================
# Recebe IDs dos módulos IAM e Storage,
# além de configurações de recursos por etapa do pipeline.


# --- Dependências de outros módulos ---

variable "compute_compartment_id" {
  description = "OCID do compartment de compute (vem do módulo IAM)"
  type        = string
}

variable "namespace" {
  description = "Namespace do Object Storage (vem do módulo Storage)"
  type        = string
}

variable "bucket_landing_zone" {
  description = "Nome do bucket landing-zone (scripts Python ficam aqui)"
  type        = string
}


# --- Configuração Data Flow: Mapa de aplicações ---
# Cada entrada define uma aplicação Data Flow com seus recursos.
# Memória é calculada automaticamente: OCPU * 16 GB

variable "dataflow_applications" {
  description = "Mapa de aplicações Data Flow (key = nome, value = config)"
  type = map(object({
    display_name  = string
    script_name   = string
    ocpu          = number
    num_executors = number
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
