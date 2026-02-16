# ============================================
# Variáveis do Módulo Storage
# ============================================

variable "storage_compartment_id" {
  description = "OCID do compartment de storage (vem do módulo IAM)"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto (prefixo para nomes de buckets)"
  type        = string
  default     = "hackathon-2025"
}

variable "enable_lifecycle_policies" {
  description = "Habilitar lifecycle policies (arquivar dados antigos após 60 dias)"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags para organizacao e controle de custos"
  type        = map(string)
  default     = {}
}
