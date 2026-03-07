# ============================================================
# Módulo Airflow — VM OCI para hospedar o Airflow (Docker Compose)
# ============================================================
# Cria uma Compute Instance na subnet pública com:
#   - Docker + Docker Compose instalados via cloud-init
#   - IP público para acesso à UI (porta 8080)
#   - Dynamic Group + Policy para Instance Principal (Data Flow + Storage)
#
# Shape: VM.Standard.E3.Flex (1 OCPU, 16 GB)

terraform {
  required_providers {
    oci = { source = "oracle/oci" }
  }
}

# ============================================================
# Availability Domain (busca automaticamente — evita hardcode)
# ============================================================
data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_id
}

# ============================================================
# Imagem Oracle Linux 8 (mais recente disponível na região)
# ============================================================
data "oci_core_images" "oracle_linux_8" {
  compartment_id           = var.compartment_id
  operating_system         = "Oracle Linux"
  operating_system_version = "8"
  shape                    = var.shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

# ============================================================
# Compute Instance — VM do Airflow
# ============================================================
resource "oci_core_instance" "airflow" {
  compartment_id      = var.compartment_id
  display_name        = "${var.project_name}-airflow-vm"
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name

  shape = var.shape
  shape_config {
    ocpus         = var.ocpus
    memory_in_gbs = var.memory_in_gbs
  }

  source_details {
    source_type = "image"
    source_id   = data.oci_core_images.oracle_linux_8.images[0].id
  }

  create_vnic_details {
    subnet_id        = var.public_subnet_id
    assign_public_ip = true
    display_name     = "${var.project_name}-airflow-vnic"
    hostname_label   = "airflow"
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data           = base64encode(file("${path.module}/cloud-init.yaml"))
  }

  freeform_tags = merge(var.tags, {
    role = "airflow"
  })

  # Ignorar mudanças de imagem após criação da VM.
  # Sem isso, o Terraform tenta trocar o boot volume quando uma nova
  # imagem Oracle Linux 8 é publicada, causando erro de kmsKeyId.
  lifecycle {
    ignore_changes = [source_details]
  }
}

# ============================================================
# Security List — porta 8080 (Airflow UI)
# ============================================================
resource "oci_core_security_list" "airflow_ingress" {
  compartment_id = var.compartment_id
  vcn_id         = var.vcn_id
  display_name   = "${var.project_name}-airflow-sl"

  ingress_security_rules {
    protocol    = "6"
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    stateless   = false

    tcp_options {
      min = 8080
      max = 8080
    }
  }

  egress_security_rules {
    protocol         = "all"
    destination      = "0.0.0.0/0"
    destination_type = "CIDR_BLOCK"
    stateless        = false
  }

  freeform_tags = var.tags
}


# ============================================================
# Dynamic Group — Instance Principal para a VM do Airflow
# ============================================================
# Dynamic Groups são o mecanismo da OCI para que instâncias (VMs)
# se autentiquem automaticamente sem credenciais de usuário.
# A matching rule identifica a VM pelo OCID — quando o código
# Python na VM usa `oci.auth.signers.InstancePrincipalsSecurityTokenSigner()`,
# a OCI verifica se a VM pertence a algum Dynamic Group e aplica as policies.
#
# Dynamic Groups são recursos de Tenancy-level (como Identity Groups).

resource "oci_identity_dynamic_group" "airflow_vm" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-airflow-dynamic-group"
  description    = "Dynamic Group para a VM do Airflow — permite Instance Principal auth"

  matching_rule = "instance.id = '${oci_core_instance.airflow.id}'"

  freeform_tags = var.tags
}


# ============================================================
# Policy — Permissões da VM do Airflow via Instance Principal
# ============================================================
# Sem esta policy, a VM autentica (Instance Principal) mas recebe
# 404 NotAuthorizedOrNotFound ao chamar APIs da OCI.
#
# Permissões necessárias:
# 1. Data Flow: criar e monitorar runs (o Airflow dispara jobs Spark)
# 2. Storage: ler scripts e escrever resultados nos buckets
# 3. Network: Data Flow precisa de acesso às subnets

resource "oci_identity_policy" "airflow_instance_principal" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-airflow-instance-principal-policy"
  description    = "Permite à VM do Airflow gerenciar Data Flow runs e acessar Storage via Instance Principal"

  depends_on = [oci_identity_dynamic_group.airflow_vm]

  statements = [
    # Data Flow: criar runs, monitorar status, cancelar jobs
    "Allow dynamic-group ${oci_identity_dynamic_group.airflow_vm.name} to manage dataflow-family in compartment ${var.project_compartment_name}",

    # Storage: ler scripts do pipeline-ops, ler/escrever dados nos buckets
    "Allow dynamic-group ${oci_identity_dynamic_group.airflow_vm.name} to manage object-family in compartment ${var.project_compartment_name}",

    # Network: Data Flow precisa das subnets para executar
    "Allow dynamic-group ${oci_identity_dynamic_group.airflow_vm.name} to read virtual-network-family in compartment ${var.project_compartment_name}",

    # Compute: Start/Stop da VM do Modelo de Scoring (orquestrada pelo Airflow)
    "Allow dynamic-group ${oci_identity_dynamic_group.airflow_vm.name} to manage instance-family in compartment ${var.project_compartment_name}",
  ]
}
