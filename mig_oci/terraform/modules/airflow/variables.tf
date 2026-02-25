# ============================================================
# Variáveis — Módulo Airflow
# ============================================================

variable "compartment_id" {
  description = "OCID do compartment de compute (vem do módulo IAM)"
  type        = string
}

variable "public_subnet_id" {
  description = "OCID da subnet pública (vem do módulo Network)"
  type        = string
}

variable "vcn_id" {
  description = "OCID da VCN (vem do módulo Network)"
  type        = string
}

variable "ssh_public_key" {
  description = "Chave SSH pública para acesso administrativo à VM"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto (prefixo para recursos)"
  type        = string
  default     = "hackathon-2025"
}

variable "shape" {
  description = "Shape da VM (trocar se Out of host capacity: E4.Flex → E3.Flex → A1.Flex)"
  type        = string
  default     = "VM.Standard.E3.Flex"
}

variable "ocpus" {
  description = "Número de OCPUs da VM (VM.Standard.E4.Flex)"
  type        = number
  default     = 2
}

variable "memory_in_gbs" {
  description = "Memória em GB da VM"
  type        = number
  default     = 32
}

variable "tags" {
  description = "Tags para organização"
  type        = map(string)
  default     = {}
}
