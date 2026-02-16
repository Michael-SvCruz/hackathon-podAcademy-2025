# ============================================
# Outputs do Módulo Network
# ============================================
# Estes IDs são usados por outros módulos:
# - Compute: precisa da subnet para Data Flow
# - Storage: Service Gateway para acesso interno

output "vcn_id" {
  description = "OCID da VCN"
  value       = oci_core_vcn.main.id
}

output "public_subnet_id" {
  description = "OCID da subnet publica (Load Balancer)"
  value       = oci_core_subnet.public.id
}

output "private_data_subnet_id" {
  description = "OCID da subnet privada de dados (Data Flow, Data Science)"
  value       = oci_core_subnet.private_data.id
}

output "private_app_subnet_id" {
  description = "OCID da subnet privada de aplicacao (Scoring API)"
  value       = oci_core_subnet.private_app.id
}

output "service_gateway_id" {
  description = "OCID do Service Gateway"
  value       = oci_core_service_gateway.main.id
}
