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


# --- Shapes Data Flow ---
# Shapes fixos (não Flex) devido a limitação de quota no OCI free tier.
# VM.Standard2.1: 1 OCPU, 15 GB RAM
# VM.Standard2.2: 2 OCPU, 30 GB RAM

variable "driver_shape" {
  description = "Shape da VM do driver Spark"
  type        = string
  default     = "VM.Standard2.1"
}

variable "executor_shape" {
  description = "Shape das VMs dos executors Spark"
  type        = string
  default     = "VM.Standard2.2"
}


# --- Autoscaling (Spark Dynamic Allocation) ---
# O Data Flow ajusta executors entre min e max conforme a carga do job.
# Ref: https://docs.oracle.com/en-us/iaas/data-flow/using/autoscaling.htm

variable "min_executors" {
  description = "Número mínimo de executors (usado como num_executors inicial)"
  type        = number
  default     = 3
}

variable "max_executors" {
  description = "Número máximo de executors (autoscaling via Spark Dynamic Allocation)"
  type        = number
  default     = 8
}


# --- Mapa de aplicações Data Flow ---
# Cada entrada define uma aplicação. Shape e autoscaling são padronizados
# para todas as apps (configurados nas variáveis acima).

variable "dataflow_applications" {
  description = "Mapa de aplicações Data Flow (key = nome, value = config)"
  type = map(object({
    display_name = string
    script_name  = string
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
