# ============================================================
# Outputs — Módulo Airflow
# ============================================================

output "airflow_vm_id" {
  description = "OCID da VM do Airflow"
  value       = oci_core_instance.airflow.id
}

output "airflow_public_ip" {
  description = "IP público da VM do Airflow (acesso SSH e UI :8080)"
  value       = oci_core_instance.airflow.public_ip
}

output "airflow_ui_url" {
  description = "URL da interface web do Airflow"
  value       = "http://${oci_core_instance.airflow.public_ip}:8080"
}

output "airflow_security_list_id" {
  description = "OCID da Security List do Airflow (porta 8080) — associar à subnet pública"
  value       = oci_core_security_list.airflow_ingress.id
}
