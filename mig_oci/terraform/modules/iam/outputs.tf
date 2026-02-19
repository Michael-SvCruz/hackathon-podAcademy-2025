# ============================================
# Outputs do Módulo IAM
# ============================================
# Outputs são os "resultados" do módulo - os IDs dos recursos criados.
# Esses IDs são necessários para as próximas fases:
# - network_compartment_id → Fase 2 (criar VCN dentro deste compartment)
# - storage_compartment_id → Fase 3 (criar buckets dentro deste compartment)
# - compute_compartment_id → Fase 4 (criar Data Flow dentro deste compartment)

# --- Compartment IDs ---

output "project_compartment_id" {
  description = "OCID do compartment raiz do projeto"
  value       = oci_identity_compartment.project.id
}

output "network_compartment_id" {
  description = "OCID do compartment de rede (VCN, subnets)"
  value       = oci_identity_compartment.network.id
}

output "storage_compartment_id" {
  description = "OCID do compartment de storage (buckets)"
  value       = oci_identity_compartment.storage.id
}

output "compute_compartment_id" {
  description = "OCID do compartment de compute (Data Flow, VMs)"
  value       = oci_identity_compartment.compute.id
}

output "data_compartment_id" {
  description = "OCID do compartment de dados (Data Catalog)"
  value       = oci_identity_compartment.data.id
}

output "security_compartment_id" {
  description = "OCID do compartment de seguranca (Vault, Keys)"
  value       = oci_identity_compartment.security.id
}

output "dev_teste_compartment_id" {
  description = "OCID do compartment dev-teste (sandbox isolado para o time)"
  value       = oci_identity_compartment.dev_teste.id
}

# --- Group IDs ---

output "administrators_group_id" {
  description = "OCID do grupo de administradores"
  value       = oci_identity_group.administrators.id
}

output "data_engineers_group_id" {
  description = "OCID do grupo de engenheiros de dados"
  value       = oci_identity_group.data_engineers.id
}

output "data_scientists_group_id" {
  description = "OCID do grupo de cientistas de dados"
  value       = oci_identity_group.data_scientists.id
}

output "developers_group_id" {
  description = "OCID do grupo de developers (sandbox dev-teste)"
  value       = oci_identity_group.developers.id
}
