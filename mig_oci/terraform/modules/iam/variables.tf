# ============================================
# Variáveis do Módulo IAM
# ============================================
# Estas são as "entradas" que o módulo precisa para funcionar.
# Quem chama o módulo (main.tf do environment) deve fornecer esses valores.

variable "tenancy_ocid" {
  description = "OCID do Tenancy OCI (identificador único da sua conta)"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto (usado como prefixo em todos os recursos)"
  type        = string
  default     = "hackathon-2025"
}

variable "tags" {
  description = "Tags para organização e controle de custos"
  type        = map(string)
  default     = {}
}
