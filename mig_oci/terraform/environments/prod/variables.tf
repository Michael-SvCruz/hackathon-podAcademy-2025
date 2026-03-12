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


# --- Airflow VM (Fase 5) ---

variable "airflow_ssh_public_key" {
  description = "Chave SSH pública para acesso à VM do Airflow (cat ~/.ssh/id_rsa.pub)"
  type        = string
  sensitive   = true
}


# --- Modelo VM (Fase 6) ---

variable "modelo_ssh_public_key" {
  description = "Chave SSH pública para a VM do Modelo (pode ser a mesma da Airflow)"
  type        = string
  sensitive   = true
}


# --- Storage (Fase 3) ---

variable "enable_lifecycle_policies" {
  description = "Habilitar lifecycle policies nos buckets (arquivar após 60 dias)"
  type        = bool
  default     = false
}


# --- Compute (Fase 4) ---
# Cada app tem sua própria configuração de shape.
# Apps em grupos paralelos usam famílias de shapes diferentes para evitar
# competição de quota no OCI free tier.
#
# Shapes fixos: driver_ocpus/memory = null (não precisa de shape_config)
# Shapes Flex:  driver_ocpus/memory informados (gera shape_config no módulo)

variable "dataflow_applications" {
  description = "Mapa de aplicações Data Flow com shape por app"
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
  default = {

    # ============================================================
    # BRONZE (6 apps — ingestão, paralelo)
    # Cada app usa uma família de shape diferente para rodar em paralelo.
    # Configuração validada em teste manual (dataflow_vms.txt).
    # ============================================================

    bronze-bureau = {
      display_name    = "bronze-bureau"
      script_name     = "bronze_bureau.py"
      driver_shape    = "VM.Standard.E5.Flex"
      executor_shape  = "VM.Standard.E5.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    bronze-telco = {
      display_name   = "bronze-telco"
      script_name    = "bronze_telco.py"
      driver_shape   = "VM.Standard2.2"
      executor_shape = "VM.Standard2.2"
      min_executors  = 2
      max_executors  = 8
    }

    bronze-cadastro = {
      display_name    = "bronze-cadastro"
      script_name     = "bronze_cadastro.py"
      driver_shape    = "VM.Standard.A1.Flex"
      executor_shape  = "VM.Standard.A1.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 1
      max_executors   = 8
    }

    bronze-atraso = {
      display_name    = "bronze-atraso"
      script_name     = "bronze_atraso.py"
      driver_shape    = "VM.Standard3.Flex"
      executor_shape  = "VM.Standard3.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    bronze-pagamento = {
      display_name    = "bronze-pagamento"
      script_name     = "bronze_pagamento.py"
      driver_shape    = "VM.Standard.E3.Flex"
      executor_shape  = "VM.Standard.E3.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    bronze-recarga = {
      display_name    = "bronze-recarga"
      script_name     = "bronze_recarga.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    # ============================================================
    # SILVER (6 apps — tipagem, validação, dedup, paralelo)
    # Cada app usa uma família de shape diferente (mesma estratégia do Bronze).
    # Configs mais potentes: Silver faz tipagem, validação e dedup.
    # ============================================================

    silver-bureau = {
      display_name    = "silver-bureau"
      script_name     = "silver_bureau.py"
      driver_shape    = "VM.Standard.E5.Flex"
      executor_shape  = "VM.Standard.E5.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    silver-telco = {
      display_name   = "silver-telco"
      script_name    = "silver_telco.py"
      driver_shape   = "VM.Standard2.2"
      executor_shape = "VM.Standard2.2"
      min_executors  = 2
      max_executors  = 8
    }

    silver-cadastro = {
      display_name    = "silver-cadastro"
      script_name     = "silver_cadastro.py"
      driver_shape    = "VM.Standard.A1.Flex"
      executor_shape  = "VM.Standard.A1.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 1
      max_executors   = 8
    }

    silver-recarga = {
      display_name    = "silver-recarga"
      script_name     = "silver_recarga.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    silver-pagamento = {
      display_name    = "silver-pagamento"
      script_name     = "silver_pagamento.py"
      driver_shape    = "VM.Standard.E3.Flex"
      executor_shape  = "VM.Standard.E3.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    silver-atraso = {
      display_name    = "silver-atraso"
      script_name     = "silver_atraso.py"
      driver_shape    = "VM.Standard3.Flex"
      executor_shape  = "VM.Standard3.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    # ============================================================
    # GOLD FEATURES (3 apps — feature engineering, paralelo)
    # Cada app usa uma família diferente (mesma estratégia do Bronze).
    # Shapes mais potentes: 2 OCPU driver, 2 OCPU/32 GB executor.
    # ============================================================

    gold-recarga = {
      display_name    = "gold-recarga"
      script_name     = "gold_recarga.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    gold-pagamento = {
      display_name    = "gold-pagamento"
      script_name     = "gold_pagamento.py"
      driver_shape    = "VM.Standard3.Flex"
      executor_shape  = "VM.Standard3.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    gold-atraso = {
      display_name    = "gold-atraso"
      script_name     = "gold_atraso.py"
      driver_shape    = "VM.Standard.A1.Flex"
      executor_shape  = "VM.Standard.A1.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    # ============================================================
    # ABT BUILDERS (6 apps — joins + builder, sequencial)
    # Rodam sequencialmente (v1→v2→...→v6), podem usar o mesmo shape.
    # Shape mais potente disponível: E4.Flex com 2 OCPU/32 GB.
    # ============================================================

    abt-v1 = {
      display_name    = "abt-v1"
      script_name     = "abt_v1_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    abt-v2 = {
      display_name    = "abt-v2"
      script_name     = "abt_v2_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    abt-v3 = {
      display_name    = "abt-v3"
      script_name     = "abt_v3_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    abt-v4 = {
      display_name    = "abt-v4"
      script_name     = "abt_v4_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    abt-v5 = {
      display_name    = "abt-v5"
      script_name     = "abt_v5_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    abt-v6 = {
      display_name    = "abt-v6"
      script_name     = "abt_v6_builder.py"
      driver_shape    = "VM.Standard.E4.Flex"
      executor_shape  = "VM.Standard.E4.Flex"
      driver_ocpus    = 2
      driver_memory   = 16
      executor_ocpus  = 2
      executor_memory = 32
      min_executors   = 2
      max_executors   = 8
    }

    # MODELO removido do Data Flow — migrado para VM dedicada (modules/modelo_vm)
    # Motivo: Data Flow não suporta dependências customizadas (lightgbm, scikit-learn)
    # O scoring agora roda em VM.Standard.E5.Flex com Start/Stop via Airflow
  }
}
