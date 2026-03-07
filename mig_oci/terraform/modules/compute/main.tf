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
# Cria 21 aplicações Data Flow (Apache Spark gerenciado)
# que implementam o pipeline Medallion Architecture:
#
# ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐    ┌───────────────┐
# │   Bronze (6 apps) │ →  │  Silver (6 apps)  │ →  │  Gold (3 apps)   │ →  │  ABT (6 apps) │
# │ bureau, telco,    │    │ bureau, telco,    │    │ recarga,         │    │ v1→v2→...→v6  │
# │ cadastro, recarga,│    │ cadastro, recarga,│    │ pagamento,       │    │               │
# │ pagamento, atraso │    │ pagamento, atraso │    │ atraso           │    │               │
# └───────────────────┘    └───────────────────┘    └───────────────────┘    └───────────────┘
#
# Cada application é uma "definição de job" - as execuções são disparadas
# via API, CLI ou Airflow (oci_dataflow_run).
#
# Estratégia de shapes: apps em grupos paralelos usam famílias de shapes
# diferentes para evitar competição de quota no OCI free tier.
# Shapes fixos: VM.Standard2.1, VM.Standard2.2 (sem shape_config)
# Shapes Flex: E3, E4, Standard3, A1 (com shape_config: ocpus + memory)


# ============================================
# Data Flow Applications (for_each)
# ============================================

resource "oci_dataflow_application" "apps" {
  for_each       = var.dataflow_applications
  compartment_id = var.compute_compartment_id
  display_name   = "${var.project_name}-dataflow-${each.value.display_name}"
  language       = "PYTHON"
  type           = "BATCH"
  spark_version  = var.spark_version

  # Driver shape (per-app)
  driver_shape = each.value.driver_shape

  # Driver shape config — apenas para shapes Flex (E3, E4, Standard3, A1)
  dynamic "driver_shape_config" {
    for_each = each.value.driver_ocpus != null ? [1] : []
    content {
      ocpus         = each.value.driver_ocpus
      memory_in_gbs = each.value.driver_memory
    }
  }

  # Executor shape (per-app)
  executor_shape = each.value.executor_shape
  num_executors  = each.value.min_executors

  # Executor shape config — apenas para shapes Flex
  dynamic "executor_shape_config" {
    for_each = each.value.executor_ocpus != null ? [1] : []
    content {
      ocpus         = each.value.executor_ocpus
      memory_in_gbs = each.value.executor_memory
    }
  }

  file_uri = "oci://${var.bucket_pipeline_ops}@${var.namespace}/scripts/${each.value.script_name}"

  # Logs e warehouse no bucket pipeline-ops
  logs_bucket_uri      = "oci://${var.bucket_pipeline_ops}@${var.namespace}/logs/"
  warehouse_bucket_uri = "oci://${var.bucket_pipeline_ops}@${var.namespace}/warehouse/"

  # Delta Lake + Autoscaling (Spark Dynamic Allocation)
  configuration = merge(
    {
      # Delta Lake
      "spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
      "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

      # Autoscaling via Spark Dynamic Allocation (per-app min/max)
      "spark.dynamicAllocation.enabled"                 = "true"
      "spark.dynamicAllocation.shuffleTracking.enabled"  = "true"
      "spark.dynamicAllocation.minExecutors"             = tostring(each.value.min_executors)
      "spark.dynamicAllocation.maxExecutors"             = tostring(each.value.max_executors)
      "spark.dynamicAllocation.executorIdleTimeout"      = "60"
      "spark.dynamicAllocation.schedulerBacklogTimeout"  = "60"
      "spark.dataflow.dynamicAllocation.quotaPolicy"     = "min"
    }
  )

  arguments = [var.namespace]

  # Tags: globais + layer/source derivados do nome
  freeform_tags = merge(var.tags, {
    layer  = split("-", each.key)[0]
    source = length(split("-", each.key)) > 1 ? split("-", each.key)[1] : each.key
  })
}
