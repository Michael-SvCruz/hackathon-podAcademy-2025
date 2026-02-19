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
# Módulo Compute - OCI Data Flow Applications
# ============================================
# Cria 17 aplicações Data Flow (Apache Spark gerenciado)
# que implementam o pipeline Medallion Architecture:
#
# ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐    ┌───────────────┐
# │   Bronze (6 apps) │ →  │  Silver (6 apps)  │ →  │  Gold (3 apps)   │ →  │  ABT (2 apps) │
# │ bureau, telco,    │    │ bureau, telco,    │    │ recarga,         │    │ v5, v6        │
# │ cadastro, recarga,│    │ cadastro, recarga,│    │ pagamento,       │    │               │
# │ pagamento, atraso │    │ pagamento, atraso │    │ atraso           │    │               │
# └───────────────────┘    └───────────────────┘    └───────────────────┘    └───────────────┘
#
# Cada application é uma "definição de job" - as execuções são disparadas
# via API, CLI ou Airflow (oci_dataflow_run).
#
# Shape VM.Standard.E4.Flex permite configurar OCPUs e memória por etapa:
# - Bronze: leve (2 OCPU, 4 executors) - apenas ingestão
# - Silver: médio (4 OCPU, 8 executors) - validação + tipagem
# - Gold:   pesado (4 OCPU, 16 executors) - feature engineering
# - ABT:    médio (4 OCPU, 8 executors) - joins + builder final


# ============================================
# Data Flow Applications (for_each)
# ============================================
# Uma única definição de recurso cria todas as 17 aplicações
# com base no mapa var.dataflow_applications.

resource "oci_dataflow_application" "apps" {
  for_each       = var.dataflow_applications
  compartment_id = var.compute_compartment_id
  display_name   = "${var.project_name}-dataflow-${each.value.display_name}"
  language       = "PYTHON"
  type           = "BATCH"
  spark_version  = var.spark_version

  driver_shape = "VM.Standard.E4.Flex"
  driver_shape_config {
    ocpus         = each.value.ocpu
    memory_in_gbs = each.value.ocpu * 16
  }

  executor_shape = "VM.Standard.E4.Flex"
  executor_shape_config {
    ocpus         = each.value.ocpu
    memory_in_gbs = each.value.ocpu * 16
  }

  num_executors = each.value.num_executors

  file_uri = "oci://${var.bucket_landing_zone}@${var.namespace}/scripts/${each.value.script_name}"

  # Dependências Python carregadas via addPyFile() nos scripts (não archive_uri)
  # archive_uri exige conda pack - desnecessário para 2 arquivos .py utilitários

  # Logs e warehouse no bucket landing-zone (evita criar bucket dataflow-logs separado)
  logs_bucket_uri      = "oci://${var.bucket_landing_zone}@${var.namespace}/dataflow-logs/"
  warehouse_bucket_uri = "oci://${var.bucket_landing_zone}@${var.namespace}/dataflow-warehouse/"

  configuration = {
    "spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }

  arguments = [var.namespace]

  # Tags: globais (var.tags) + tag "layer" derivada automaticamente do nome
  # Ex: key "bronze-bureau" → layer = "bronze", "gold-recarga" → layer = "gold"
  freeform_tags = merge(var.tags, {
    layer  = split("-", each.key)[0]
    source = length(split("-", each.key)) > 1 ? split("-", each.key)[1] : each.key
  })
}
