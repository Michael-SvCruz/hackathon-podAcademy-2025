# ============================================================
# Outputs — Módulo Modelo VM
# ============================================================

output "modelo_vm_id" {
  description = "OCID da VM do Modelo (usado pelo Airflow para Start/Stop)"
  value       = oci_core_instance.modelo.id
}

output "modelo_private_ip" {
  description = "IP privado da VM do Modelo (usado pelo Airflow para SSH)"
  value       = oci_core_instance.modelo.private_ip
}
