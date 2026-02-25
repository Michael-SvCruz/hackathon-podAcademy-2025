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
# Módulo Network - VCN, Subnets, Gateways
# ============================================
# Cria a rede virtual completa para o projeto:
#
# Arquitetura de rede:
# ┌──────────────────────────────────────────────────┐
# │  VCN 10.0.0.0/16                                │
# │                                                  │
# │  ┌──────────────────┐  Internet Gateway          │
# │  │ Public Subnet    │──────────────► Internet    │
# │  │ 10.0.1.0/24      │  (Load Balancer futuro)   │
# │  └──────────────────┘                            │
# │                                                  │
# │  ┌──────────────────┐  NAT Gateway               │
# │  │ Private Data     │──────────────► Internet    │
# │  │ 10.0.2.0/24      │  (Data Flow, Data Science) │
# │  └──────────────────┘  Service Gateway            │
# │                        ──────────────► OCI Services│
# │  ┌──────────────────┐                            │
# │  │ Private App      │  (Scoring API futuro)      │
# │  │ 10.0.3.0/24      │                            │
# │  └──────────────────┘                            │
# └──────────────────────────────────────────────────┘


# ============================================
# 1. VCN (Virtual Cloud Network)
# ============================================
# A VCN é a "rede local virtual" do projeto na OCI.
# Todos os recursos (subnets, gateways) ficam dentro dela.

resource "oci_core_vcn" "main" {
  compartment_id = var.network_compartment_id
  cidr_blocks    = [var.vcn_cidr_block]
  display_name   = "${var.project_name}-vcn"
  dns_label      = "hackathonvcn"

  freeform_tags = var.tags
}


# ============================================
# 2. GATEWAYS (Portas de saída da rede)
# ============================================

# Internet Gateway - Permite acesso direto à internet (entrada e saída)
# Usado pela subnet pública (Load Balancer, SSH)
resource "oci_core_internet_gateway" "main" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-igw"
  enabled        = true

  freeform_tags = var.tags
}

# NAT Gateway - Permite que subnets privadas acessem a internet
# (somente saída, sem acesso de entrada - mais seguro)
# Data Flow usa para baixar dependências, acessar APIs externas
resource "oci_core_nat_gateway" "main" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-natgw"

  freeform_tags = var.tags
}

# Service Gateway - Acesso direto aos serviços OCI (Object Storage, etc.)
# Tráfego fica dentro da rede Oracle, sem passar pela internet (mais rápido e seguro)
# Crítico para Data Flow ler/escrever nos buckets Object Storage
data "oci_core_services" "all_services" {
  filter {
    name   = "name"
    values = ["All .* Services In Oracle Services Network"]
    regex  = true
  }
}

resource "oci_core_service_gateway" "main" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-sgw"

  services {
    service_id = data.oci_core_services.all_services.services[0].id
  }

  freeform_tags = var.tags
}


# ============================================
# 3. ROUTE TABLES (Tabelas de roteamento)
# ============================================
# Definem "para onde vai o tráfego" de cada subnet.

# Route Table pública: todo tráfego externo vai pelo Internet Gateway
resource "oci_core_route_table" "public" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-public-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.main.id
  }

  freeform_tags = var.tags
}

# Route Table privada: internet vai pelo NAT, serviços OCI pelo Service Gateway
resource "oci_core_route_table" "private" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-private-rt"

  # Tráfego internet → NAT Gateway (somente saída)
  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_nat_gateway.main.id
  }

  # Tráfego para serviços OCI → Service Gateway (rede interna Oracle)
  route_rules {
    destination       = data.oci_core_services.all_services.services[0].cidr_block
    destination_type  = "SERVICE_CIDR_BLOCK"
    network_entity_id = oci_core_service_gateway.main.id
  }

  freeform_tags = var.tags
}


# ============================================
# 4. SECURITY LISTS (Firewall)
# ============================================
# Definem quais portas e protocolos são permitidos.

# Security List pública: permite SSH (22), HTTP (80), HTTPS (443)
resource "oci_core_security_list" "public" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-public-sl"

  # Regras de SAÍDA: todo tráfego de saída permitido
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  # SSH (porta 22) - acesso remoto
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6" # TCP
    stateless = false
    tcp_options {
      min = 22
      max = 22
    }
  }

  # HTTP (porta 80)
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6"
    stateless = false
    tcp_options {
      min = 80
      max = 80
    }
  }

  # HTTPS (porta 443)
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6"
    stateless = false
    tcp_options {
      min = 443
      max = 443
    }
  }

  # Airflow UI (porta 8080)
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6"
    stateless = false
    tcp_options {
      min = 8080
      max = 8080
    }
  }

  freeform_tags = var.tags
}

# Security List privada: permite tráfego apenas dentro da VCN
resource "oci_core_security_list" "private" {
  compartment_id = var.network_compartment_id
  vcn_id         = oci_core_vcn.main.id
  display_name   = "${var.project_name}-private-sl"

  # Saída: todo tráfego permitido (NAT Gateway filtra)
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  # Entrada: apenas tráfego da própria VCN (recursos internos se comunicam)
  ingress_security_rules {
    source    = var.vcn_cidr_block
    protocol  = "all"
    stateless = false
  }

  freeform_tags = var.tags
}


# ============================================
# 5. SUBNETS
# ============================================

# Subnet Pública - Load Balancer, acesso externo (futuro)
resource "oci_core_subnet" "public" {
  compartment_id             = var.network_compartment_id
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = var.public_subnet_cidr
  display_name               = "${var.project_name}-public-subnet"
  dns_label                  = "pubsubnet"
  prohibit_public_ip_on_vnic = false # Permite IPs públicos
  route_table_id             = oci_core_route_table.public.id
  security_list_ids          = [oci_core_security_list.public.id]

  freeform_tags = var.tags
}

# Subnet Privada de Dados - Data Flow e Data Science
# Esta é a subnet principal: os jobs Spark e notebooks rodam aqui
resource "oci_core_subnet" "private_data" {
  compartment_id             = var.network_compartment_id
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = var.private_data_subnet_cidr
  display_name               = "${var.project_name}-private-data-subnet"
  dns_label                  = "datasubnet"
  prohibit_public_ip_on_vnic = true # Sem IPs públicos (mais seguro)
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = [oci_core_security_list.private.id]

  freeform_tags = var.tags
}

# Subnet Privada de Aplicação - Scoring API (futuro)
resource "oci_core_subnet" "private_app" {
  compartment_id             = var.network_compartment_id
  vcn_id                     = oci_core_vcn.main.id
  cidr_block                 = var.private_app_subnet_cidr
  display_name               = "${var.project_name}-private-app-subnet"
  dns_label                  = "appsubnet"
  prohibit_public_ip_on_vnic = true
  route_table_id             = oci_core_route_table.private.id
  security_list_ids          = [oci_core_security_list.private.id]

  freeform_tags = var.tags
}
