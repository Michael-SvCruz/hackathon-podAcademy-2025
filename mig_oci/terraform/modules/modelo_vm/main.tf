# ============================================================
# Módulo Modelo VM — VM dedicada para scoring LightGBM
# ============================================================
# Cria uma Compute Instance na subnet privada com:
#   - Python 3.11 + LightGBM + scikit-learn via cloud-init
#   - Sem IP público (acesso via Airflow VM como jump host)
#   - Dynamic Group + Policy para Instance Principal (Object Storage)
#   - Padrão Start/Stop: VM fica STOPPED e só liga durante scoring
#
# Shape: VM.Standard.E5.Flex (1 OCPU, 16 GB)
# Evita conflito de quota com shapes do pipeline ETL (E3, E4, Standard2/3, A1)

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
# Compute Instance — VM do Modelo de Scoring
# ============================================================
resource "oci_core_instance" "modelo" {
  compartment_id      = var.compartment_id
  display_name        = "${var.project_name}-modelo-scoring-vm"
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
    subnet_id        = var.private_data_subnet_id
    assign_public_ip = false
    display_name     = "${var.project_name}-modelo-vnic"
    hostname_label   = "modelo-scoring"
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data           = base64encode(file("${path.module}/cloud-init.yaml"))
  }

  freeform_tags = merge(var.tags, {
    role = "modelo-scoring"
  })

  # Ignorar mudanças de imagem após criação da VM.
  # Sem isso, o Terraform tenta trocar o boot volume quando uma nova
  # imagem Oracle Linux 8 é publicada, causando erro de kmsKeyId.
  lifecycle {
    ignore_changes = [source_details]
  }
}


# ============================================================
# Dynamic Group — Instance Principal para a VM do Modelo
# ============================================================
# A VM do modelo precisa acessar Object Storage (ler ABT, ler PKL,
# salvar predições e métricas) sem credenciais de usuário.

resource "oci_identity_dynamic_group" "modelo_vm" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-modelo-scoring-dynamic-group"
  description    = "Dynamic Group para a VM do Modelo de Scoring — permite Instance Principal auth"

  matching_rule = "instance.id = '${oci_core_instance.modelo.id}'"

  freeform_tags = var.tags
}


# ============================================================
# Policy — Permissões da VM do Modelo via Instance Principal
# ============================================================
# Permissões:
# 1. Storage: ler ABT v6 (gold-layer), ler PKL e escrever resultados (models)
# 2. Network: necessário para acessar Object Storage via Service Gateway

resource "oci_identity_policy" "modelo_instance_principal" {
  compartment_id = var.tenancy_ocid
  name           = "${var.project_name}-modelo-scoring-instance-principal-policy"
  description    = "Permite à VM do Modelo acessar Object Storage (leitura ABT + escrita resultados) via Instance Principal"

  depends_on = [oci_identity_dynamic_group.modelo_vm]

  statements = [
    # Storage: ler ABT v6, ler PKL, salvar predições e métricas
    "Allow dynamic-group ${oci_identity_dynamic_group.modelo_vm.name} to manage object-family in compartment ${var.project_compartment_name}",

    # Network: acesso ao Service Gateway para Object Storage
    "Allow dynamic-group ${oci_identity_dynamic_group.modelo_vm.name} to read virtual-network-family in compartment ${var.project_compartment_name}",
  ]
}
