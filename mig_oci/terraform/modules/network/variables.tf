# ============================================
# Variáveis do Módulo Network
# ============================================

variable "network_compartment_id" {
  description = "OCID do compartment de network (vem do módulo IAM)"
  type        = string
}

variable "project_name" {
  description = "Nome do projeto (prefixo para recursos)"
  type        = string
}

variable "vcn_cidr_block" {
  description = "CIDR block da VCN (rede principal)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR da subnet publica (Load Balancer, acesso externo)"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_data_subnet_cidr" {
  description = "CIDR da subnet privada de dados (Data Flow, Data Science)"
  type        = string
  default     = "10.0.2.0/24"
}

variable "private_app_subnet_cidr" {
  description = "CIDR da subnet privada de aplicacao (Scoring API)"
  type        = string
  default     = "10.0.3.0/24"
}

variable "tags" {
  description = "Tags para organizacao e controle de custos"
  type        = map(string)
  default     = {}
}
