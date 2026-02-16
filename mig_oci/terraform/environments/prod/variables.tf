# ============================================
# Variáveis do Environment Prod
# ============================================
# Estas variáveis recebem valores do terraform.tfvars.
# O fluxo é:
#   terraform.tfvars (seus valores) → variables.tf (declaração) → main.tf (uso)


# --- Autenticação OCI ---

variable "tenancy_ocid" {
  description = "OCID do Tenancy (Console OCI > Tenancy Details)"
  type        = string
}

variable "user_ocid" {
  description = "OCID do usuário (Console OCI > Identity > Users)"
  type        = string
}

variable "fingerprint" {
  description = "Fingerprint da API Key (Console OCI > Identity > Users > API Keys)"
  type        = string
}

variable "private_key_path" {
  description = "Caminho para a chave privada .pem"
  type        = string
}

variable "region" {
  description = "Regiao OCI (default: Sao Paulo)"
  type        = string
  default     = "sa-saopaulo-1"
}


# --- Configuração do Projeto ---

variable "project_name" {
  description = "Nome do projeto (prefixo para todos os recursos)"
  type        = string
  default     = "hackathon-2025"
}

variable "environment" {
  description = "Nome do ambiente"
  type        = string
  default     = "prod"
}

variable "tags" {
  description = "Tags para organizacao e controle de custos"
  type        = map(string)
  default = {
    Project     = "Hackathon-PodAcademy-2025"
    Environment = "Production"
    ManagedBy   = "Terraform"
  }
}


# --- Network (Fase 2) ---

variable "vcn_cidr_block" {
  description = "CIDR block da VCN (rede principal)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR da subnet publica (Load Balancer)"
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


# --- Storage (Fase 3) ---

variable "enable_lifecycle_policies" {
  description = "Habilitar lifecycle policies nos buckets (arquivar após 60 dias)"
  type        = bool
  default     = false
}


# --- Compute (Fase 4) ---

variable "dataflow_bronze_ocpu" {
  description = "OCPU por executor do Data Flow Bronze"
  type        = number
  default     = 2
}

variable "dataflow_silver_ocpu" {
  description = "OCPU por executor do Data Flow Silver"
  type        = number
  default     = 4
}

variable "dataflow_gold_ocpu" {
  description = "OCPU por executor do Data Flow Gold"
  type        = number
  default     = 4
}

variable "dataflow_abt_ocpu" {
  description = "OCPU por executor do Data Flow ABT"
  type        = number
  default     = 4
}

variable "dataflow_bronze_executors" {
  description = "Numero de executors do Data Flow Bronze"
  type        = number
  default     = 4
}

variable "dataflow_silver_executors" {
  description = "Numero de executors do Data Flow Silver"
  type        = number
  default     = 8
}

variable "dataflow_gold_executors" {
  description = "Numero de executors do Data Flow Gold"
  type        = number
  default     = 16
}

variable "dataflow_abt_executors" {
  description = "Numero de executors do Data Flow ABT"
  type        = number
  default     = 8
}
