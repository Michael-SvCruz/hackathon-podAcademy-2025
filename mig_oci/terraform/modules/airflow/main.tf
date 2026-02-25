# ============================================================
# Módulo Airflow — VM OCI para hospedar o Airflow (Astro CLI)
# ============================================================
# Cria uma Compute Instance na subnet pública com:
#   - Docker + Astro CLI instalados via cloud-init
#   - IP público para acesso à UI (porta 8080)
#
# Shape: VM.Standard.E4.Flex (2 OCPU, 32 GB)

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
