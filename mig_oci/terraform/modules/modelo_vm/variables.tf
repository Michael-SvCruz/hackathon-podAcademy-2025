# ============================================================
# Variáveis — Módulo Modelo VM
# ============================================================

variable "tenancy_ocid" {
  description = "OCID do Tenancy (necessário para criar Dynamic Group no nível do Tenancy)"
  type        = string
}

variable "compartment_id" {
  description = "OCID do compartment de compute (vem do módulo IAM)"
  type        = string
}

variable "project_compartment_name" {
  description = "Nome do compartment raiz do projeto (para referência nas policies)"
  type        = string
  default     = "hackathon-2025"
}

variable "private_data_subnet_id" {
  description = "OCID da subnet privada de dados (vem do módulo Network)"
  type        = string
}

variable "ssh_public_key" {
  description = "Chave SSH pública para acesso à VM (via Airflow como jump host)"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto (prefixo para recursos)"
  type        = string
  default     = "hackathon-2025"
}

variable "shape" {
  description = "Shape da VM (E5.Flex evita conflito de quota com shapes do pipeline ETL)"
  type        = string
  default     = "VM.Standard.E5.Flex"
}

variable "ocpus" {
  description = "Número de OCPUs da VM"
  type        = number
  default     = 1
}

variable "memory_in_gbs" {
  description = "Memória em GB da VM"
  type        = number
  default     = 16
}

variable "tags" {
  description = "Tags para organização"
  type        = map(string)
  default     = {}
}
