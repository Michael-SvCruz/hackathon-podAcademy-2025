# ============================================
# Provider - Módulo precisa declarar qual provider usa
# ============================================
terraform {
  required_providers {
    oci = {
      source = "oracle/oci"
    }
  }
}

# ============================================
# Módulo IAM - Compartments, Groups e Policies
# ============================================
# Este módulo cria a estrutura organizacional da OCI:
# - Compartments: "pastas" para organizar recursos
# - Groups: agrupamentos de usuários
# - Policies: regras de acesso (quem pode fazer o quê)
#
# Hierarquia criada:
# Tenancy Root
# └── hackathon-2025 (root do projeto)
#     ├── network
#     ├── storage
#     ├── compute
#     ├── data
#     ├── security
#     └── dev-teste (sandbox isolado para o time)


# ============================================
# 1. COMPARTMENTS
# ============================================
# Compartment raiz do projeto - todos os recursos ficam dentro dele.
# "enable_delete = true" permite destruir o compartment via terraform destroy
resource "oci_identity_compartment" "project" {
  compartment_id = var.tenancy_ocid
  name           = var.project_name
  description    = "Hackathon PodAcademy 2025 - Modelo de Risco FPD"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Network
# Isola recursos de rede (VCN, subnets, gateways)
resource "oci_identity_compartment" "network" {
  compartment_id = oci_identity_compartment.project.id
  name           = "network"
  description    = "Recursos de rede: VCN, Subnets, Gateways, Security Lists"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Storage
# Isola recursos de armazenamento (buckets Object Storage)
resource "oci_identity_compartment" "storage" {
  compartment_id = oci_identity_compartment.project.id
  name           = "storage"
  description    = "Object Storage: landing-zone, bronze, silver, gold, models"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Compute
# Isola recursos de processamento (Data Flow, Data Science, VMs)
resource "oci_identity_compartment" "compute" {
  compartment_id = oci_identity_compartment.project.id
  name           = "compute"
  description    = "Processamento: Data Flow (Spark), Data Science, VMs"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Data
# Reservado para Data Catalog e metadados (futuro)
resource "oci_identity_compartment" "data" {
  compartment_id = oci_identity_compartment.project.id
  name           = "data"
  description    = "Servicos de dados: Data Catalog, metadados (futuro)"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Security
# Isola recursos de segurança (Vault, Keys, Secrets)
resource "oci_identity_compartment" "security" {
  compartment_id = oci_identity_compartment.project.id
  name           = "security"
  description    = "Seguranca: Vault, Keys, Secrets"
  enable_delete  = true

  freeform_tags = var.tags
}

# Sub-compartment: Dev-Teste
# Sandbox isolado para os integrantes do time explorarem a OCI.
# Recursos criados aqui NÃO afetam os compartments de produção.
resource "oci_identity_compartment" "dev_teste" {
  compartment_id = oci_identity_compartment.project.id
  name           = "dev-teste"
  description    = "Sandbox isolado para o time testar e explorar a OCI sem afetar producao"
  enable_delete  = true

  freeform_tags = merge(var.tags, {
    "ambiente" = "dev-teste"
  })
}


# ============================================
# 2. GROUPS (Grupos de Usuários)
# ============================================
# Groups são criados no nível do Tenancy (não dentro de compartments).
# Cada grupo terá políticas diferentes definindo o que podem fazer.

# Administradores - acesso total ao projeto
resource "oci_identity_group" "administrators" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-administrators"
  description    = "Administradores do projeto - acesso total"

  freeform_tags = var.tags
}

# Engenheiros de Dados - gerenciam pipeline Bronze/Silver/Gold
resource "oci_identity_group" "data_engineers" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-data-engineers"
  description    = "Engenheiros de dados - pipeline e Data Flow"

  freeform_tags = var.tags
}

# Cientistas de Dados - modelagem e análise
resource "oci_identity_group" "data_scientists" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-data-scientists"
  description    = "Cientistas de dados - notebooks e modelos"

  freeform_tags = var.tags
}

# Developers - integrantes do time com acesso ao sandbox dev-teste
resource "oci_identity_group" "developers" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-developers"
  description    = "Integrantes do time - acesso ao sandbox dev-teste para explorar a OCI"

  freeform_tags = var.tags
}


# ============================================
# 3. POLICIES (Regras de Acesso)
# ============================================
# Policies usam linguagem natural da OCI:
# "Allow group <GRUPO> to <VERBO> <RECURSO> in compartment <COMPARTMENT>"
#
# Verbos principais:
# - manage = criar, ler, atualizar, deletar (acesso total)
# - use    = ler e atualizar (sem criar/deletar)
# - read   = apenas leitura
# - inspect = listar (sem ver conteúdo)

# Policy: Administradores - acesso total ao projeto
resource "oci_identity_policy" "administrators" {
  compartment_id = oci_identity_compartment.project.id
  name           = "${var.project_name}-admin-policy"
  description    = "Acesso total para administradores"

  statements = [
    "Allow group ${oci_identity_group.administrators.name} to manage all-resources in compartment ${var.project_name}"
  ]
}

# Policy: Engenheiros de Dados
# - Gerenciam Storage (buckets, objetos)
# - Gerenciam Data Flow (criar/executar jobs Spark)
# - Leem Network (precisam das subnets para Data Flow)
resource "oci_identity_policy" "data_engineers" {
  compartment_id = oci_identity_compartment.project.id
  name           = "${var.project_name}-data-engineers-policy"
  description    = "Acesso para engenheiros de dados ao pipeline"

  statements = [
    # Storage: gerenciar buckets e objetos (upload/download dados)
    "Allow group ${oci_identity_group.data_engineers.name} to manage object-family in compartment ${var.project_name}",

    # Data Flow: criar e executar jobs Spark
    "Allow group ${oci_identity_group.data_engineers.name} to manage dataflow-family in compartment ${var.project_name}",

    # Network: ler subnets (necessário para Data Flow se conectar)
    "Allow group ${oci_identity_group.data_engineers.name} to read virtual-network-family in compartment ${var.project_name}",
    "Allow group ${oci_identity_group.data_engineers.name} to use subnets in compartment ${var.project_name}",
  ]
}

# Policy: Cientistas de Dados
# - Gerenciam Data Science (notebooks, modelos)
# - Leem Storage (acessar dados da ABT)
# - Escrevem no bucket models (salvar modelos treinados)
resource "oci_identity_policy" "data_scientists" {
  compartment_id = oci_identity_compartment.project.id
  name           = "${var.project_name}-data-scientists-policy"
  description    = "Acesso para cientistas de dados a notebooks e modelos"

  statements = [
    # Data Science: gerenciar notebooks e modelos
    "Allow group ${oci_identity_group.data_scientists.name} to manage data-science-family in compartment ${var.project_name}",

    # Storage: ler dados (ABT, features)
    "Allow group ${oci_identity_group.data_scientists.name} to read object-family in compartment ${var.project_name}",

    # Storage: escrever apenas no bucket de modelos
    "Allow group ${oci_identity_group.data_scientists.name} to manage objects in compartment ${var.project_name} where target.bucket.name = 'models'",

    # Network: ler subnets (necessário para Data Science Notebook)
    "Allow group ${oci_identity_group.data_scientists.name} to read virtual-network-family in compartment ${var.project_name}",
  ]
}

# Policy: Serviço Data Flow
# O próprio serviço Data Flow precisa de permissão para acessar buckets.
# Essa policy é obrigatória - sem ela, os jobs Spark não conseguem
# ler/escrever nos buckets Object Storage.
resource "oci_identity_policy" "dataflow_service" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-dataflow-service-policy"
  description    = "Permite ao servico Data Flow acessar recursos do projeto"

  # Dependência explícita: o compartment precisa existir e estar propagado
  # antes de ser referenciado pelo nome nas statements
  depends_on = [oci_identity_compartment.project]

  statements = [
    # Data Flow precisa ler scripts Python dos buckets
    "Allow service dataflow to read object-family in compartment ${var.project_name}",

    # Data Flow precisa escrever resultados nos buckets (bronze, silver, gold)
    "Allow service dataflow to manage objects in compartment ${var.project_name}",

    # Data Flow precisa acessar a rede (subnets)
    "Allow service dataflow to use virtual-network-family in compartment ${var.project_name}",
  ]
}


# ============================================
# 4. POLICIES DEV-TESTE (Sandbox Isolado)
# ============================================
# Developers têm acesso total DENTRO do dev-teste, mas apenas
# leitura nos recursos de produção. Isso permite que explorem
# a OCI livremente sem risco de afetar o pipeline principal.

# Policy: Developers - acesso total ao sandbox dev-teste
# Criada no nível do projeto para que "dev-teste" seja visível como sub-compartment
resource "oci_identity_policy" "developers_sandbox" {
  compartment_id = oci_identity_compartment.project.id
  name           = "${var.project_name}-developers-sandbox-policy"
  description    = "Acesso total para developers no sandbox dev-teste"

  statements = [
    # Acesso total dentro do dev-teste: criar VMs, buckets, redes, etc.
    "Allow group ${oci_identity_group.developers.name} to manage all-resources in compartment dev-teste"
  ]
}

# Policy: Developers - leitura nos buckets de produção
# Permite que consultem dados da ABT para testes, sem poder modificar
resource "oci_identity_policy" "developers_read_prod" {
  compartment_id = oci_identity_compartment.project.id
  name           = "${var.project_name}-developers-read-prod-policy"
  description    = "Leitura nos buckets de producao para developers"

  statements = [
    # Ler dados dos buckets de producao (bronze, silver, gold, models)
    "Allow group ${oci_identity_group.developers.name} to read object-family in compartment storage",

    # Inspecionar rede (necessario para entender a topologia)
    "Allow group ${oci_identity_group.developers.name} to read virtual-network-family in compartment network",
  ]
}
