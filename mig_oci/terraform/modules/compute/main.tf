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
# Shape VM.Standard2.1 (driver: 1 OCPU/15GB) + VM.Standard2.2 (executor: 2 OCPU/30GB)
# Autoscaling habilitado: Data Flow ajusta executors entre min/max conforme carga.
# Configuração padronizada para todas as 21 apps (limitação de quota OCI free tier).


# ============================================
# Data Flow Applications (for_each)
# ============================================
# Uma única definição de recurso cria todas as 21 aplicações
# com base no mapa var.dataflow_applications.

resource "oci_dataflow_application" "apps" {
  for_each       = var.dataflow_applications
  compartment_id = var.compute_compartment_id
  display_name   = "${var.project_name}-dataflow-${each.value.display_name}"
  language       = "PYTHON"
  type           = "BATCH"
  spark_version  = var.spark_version

  # Driver: VM.Standard2.1 (1 OCPU, 15 GB RAM) — shape fixo
  driver_shape = var.driver_shape

  # Executor: VM.Standard2.2 (2 OCPU, 30 GB RAM) — shape fixo
  # num_executors define o valor inicial; autoscaling ajusta entre min/max via Spark Dynamic Allocation
  executor_shape = var.executor_shape
  num_executors  = var.min_executors

  file_uri = "oci://${var.bucket_pipeline_ops}@${var.namespace}/scripts/${each.value.script_name}"

  # Scripts self-contained: sem archive_uri, sem addPyFile.
  # Funcoes utilitarias inline em cada script (evita race condition com Resource Principal).

  # Logs e warehouse no bucket pipeline-ops (separado da landing-zone de dados brutos)
  logs_bucket_uri      = "oci://${var.bucket_pipeline_ops}@${var.namespace}/logs/"
  warehouse_bucket_uri = "oci://${var.bucket_pipeline_ops}@${var.namespace}/warehouse/"

  # Delta Lake + Autoscaling (Spark Dynamic Allocation)
  # Ref: https://docs.oracle.com/en-us/iaas/data-flow/using/autoscaling.htm
  configuration = {
    # Delta Lake
    "spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    # Autoscaling via Spark Dynamic Allocation
    "spark.dynamicAllocation.enabled"                 = "true"
    "spark.dynamicAllocation.shuffleTracking.enabled"  = "true"
    "spark.dynamicAllocation.minExecutors"             = tostring(var.min_executors)
    "spark.dynamicAllocation.maxExecutors"             = tostring(var.max_executors)
    "spark.dynamicAllocation.executorIdleTimeout"      = "60"
    "spark.dynamicAllocation.schedulerBacklogTimeout"  = "60"
    "spark.dataflow.dynamicAllocation.quotaPolicy"     = "min"
  }

  arguments = [var.namespace]

  # Tags: globais (var.tags) + tag "layer" derivada automaticamente do nome
  # Ex: key "bronze-bureau" → layer = "bronze", "gold-recarga" → layer = "gold"
  freeform_tags = merge(var.tags, {
    layer  = split("-", each.key)[0]
    source = length(split("-", each.key)) > 1 ? split("-", each.key)[1] : each.key
  })
}
