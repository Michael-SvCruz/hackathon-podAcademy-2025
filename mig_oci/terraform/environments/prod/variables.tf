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

variable "dataflow_applications" {
  description = "Mapa de aplicações Data Flow"
  type = map(object({
    display_name  = string
    script_name   = string
    ocpu          = number
    num_executors = number
  }))
  default = {
    # Bronze (leve: 2 OCPU, 4 executors)
    bronze-bureau    = { display_name = "bronze-bureau",    script_name = "bronze_bureau.py",    ocpu = 2, num_executors = 4 }
    bronze-telco     = { display_name = "bronze-telco",     script_name = "bronze_telco.py",     ocpu = 2, num_executors = 4 }
    bronze-cadastro  = { display_name = "bronze-cadastro",  script_name = "bronze_cadastro.py",  ocpu = 2, num_executors = 4 }
    bronze-recarga   = { display_name = "bronze-recarga",   script_name = "bronze_recarga.py",   ocpu = 2, num_executors = 4 }
    bronze-pagamento = { display_name = "bronze-pagamento", script_name = "bronze_pagamento.py", ocpu = 2, num_executors = 4 }
    bronze-atraso    = { display_name = "bronze-atraso",    script_name = "bronze_atraso.py",    ocpu = 2, num_executors = 4 }

    # Silver (médio: 4 OCPU, 8 executors)
    silver-bureau    = { display_name = "silver-bureau",    script_name = "silver_bureau.py",    ocpu = 4, num_executors = 8 }
    silver-telco     = { display_name = "silver-telco",     script_name = "silver_telco.py",     ocpu = 4, num_executors = 8 }
    silver-cadastro  = { display_name = "silver-cadastro",  script_name = "silver_cadastro.py",  ocpu = 4, num_executors = 8 }
    silver-recarga   = { display_name = "silver-recarga",   script_name = "silver_recarga.py",   ocpu = 4, num_executors = 8 }
    silver-pagamento = { display_name = "silver-pagamento", script_name = "silver_pagamento.py", ocpu = 4, num_executors = 8 }
    silver-atraso    = { display_name = "silver-atraso",    script_name = "silver_atraso.py",    ocpu = 4, num_executors = 8 }

    # Gold Features (pesado: 4 OCPU, 16 executors)
    gold-recarga     = { display_name = "gold-recarga",     script_name = "gold_recarga.py",     ocpu = 4, num_executors = 16 }
    gold-pagamento   = { display_name = "gold-pagamento",   script_name = "gold_pagamento.py",   ocpu = 4, num_executors = 16 }
    gold-atraso      = { display_name = "gold-atraso",      script_name = "gold_atraso.py",      ocpu = 4, num_executors = 16 }

    # ABT Builders (médio: 4 OCPU, 8 executors)
    abt-v1           = { display_name = "abt-v1",           script_name = "abt_v1_builder.py",   ocpu = 4, num_executors = 8 }
    abt-v2           = { display_name = "abt-v2",           script_name = "abt_v2_builder.py",   ocpu = 4, num_executors = 8 }
    abt-v3           = { display_name = "abt-v3",           script_name = "abt_v3_builder.py",   ocpu = 4, num_executors = 8 }
    abt-v4           = { display_name = "abt-v4",           script_name = "abt_v4_builder.py",   ocpu = 4, num_executors = 8 }
    abt-v5           = { display_name = "abt-v5",           script_name = "abt_v5_builder.py",   ocpu = 4, num_executors = 8 }
    abt-v6           = { display_name = "abt-v6",           script_name = "abt_v6_builder.py",   ocpu = 4, num_executors = 8 }
  }
}
