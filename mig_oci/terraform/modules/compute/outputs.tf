# ============================================
# Outputs do Módulo Compute
# ============================================
# Estes IDs são usados para:
# - Airflow: disparar execuções via oci_dataflow_run
# - Monitoramento: consultar status dos jobs
# - Outputs do terraform: verificar que os recursos foram criados

output "dataflow_application_ids" {
  description = "Mapa de IDs das aplicações Data Flow (key = app_key, value = OCID)"
  value       = { for k, v in oci_dataflow_application.apps : k => v.id }
}
