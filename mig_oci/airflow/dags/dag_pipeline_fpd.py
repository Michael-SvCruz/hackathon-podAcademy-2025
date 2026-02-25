"""
DAG: pipeline_credit_risk_fpd
=================================
Orquestra o pipeline completo de risco de crédito FPD no OCI Data Flow.

Arquitetura Medallion (Medallion Architecture):
    Bronze (6 apps, paralelo)
    → Silver (6 apps, paralelo)
    → Gold Features (3 apps, paralelo)
    → ABT (6 apps, sequencial: v1→v2→v3→v4→v5→v6)

Cada task dispara uma aplicação OCI Data Flow (criada via Terraform Fase 4)
e aguarda a conclusão por polling (lifecycle_state = SUCCEEDED | FAILED).

Agendamento:
    - @monthly: executa no dia 1 de cada mês às 02:00 (batch mensal)
    - Catchup desabilitado (não reprocessa meses anteriores)

Configuração necessária (Airflow Variables):
    Ver config/airflow_variables.json para o template completo.
    Valores obtidos via: cd mig_oci/terraform/environments/prod && terraform output -json

Autenticação OCI:
    - Airflow em OCI: Instance Principal (automático, sem config file)
    - Airflow local/dev: ~/.oci/config (fallback automático)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ============================================================
# Configurações da DAG
# ============================================================

DAG_ID = "pipeline_credit_risk_fpd"
SCHEDULE = "0 2 1 * *"   # Todo dia 1 do mês às 02:00
START_DATE = datetime(2026, 2, 1)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Tempo máximo de espera por run (em segundos): 3 horas
RUN_TIMEOUT_SECONDS = 3 * 60 * 60
POLL_INTERVAL_SECONDS = 60  # verifica status a cada 1 minuto

# Estados terminais do Data Flow
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "DELETED"}
SUCCESS_STATE = "SUCCEEDED"


# ============================================================
# Helpers OCI Data Flow
# ============================================================

def _get_oci_client():
    """
    Retorna um DataFlowClient autenticado.

    Tenta Instance Principal (para Airflow rodando em OCI) primeiro.
    Faz fallback para ~/.oci/config (para desenvolvimento local).
    """
    import oci

    try:
        # Instance Principal — usado quando o Airflow roda em OCI (Compute, K8s, etc.)
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.data_flow.DataFlowClient({}, signer=signer)
        log.info("Autenticado via Instance Principal.")
        return client
    except Exception as e:
        log.warning("Instance Principal falhou (%s). Tentando ~/.oci/config...", e)
        config = oci.config.from_file()
        return oci.data_flow.DataFlowClient(config)


def _trigger_and_wait(app_id: str, display_name: str, compartment_id: str) -> None:
    """
    Dispara um run do OCI Data Flow e aguarda até conclusão.

    Args:
        app_id:         OCID da aplicação Data Flow (criada pelo Terraform).
        display_name:   Nome descritivo para o run (aparece no Console OCI).
        compartment_id: OCID do compartment de compute.

    Raises:
        AirflowException se o run não terminar com SUCCEEDED.
    """
    import oci
    from airflow.exceptions import AirflowException

    client = _get_oci_client()

    # --- Disparar o run ---
    run_details = oci.data_flow.models.CreateRunDetails(
        application_id=app_id,
        compartment_id=compartment_id,
        display_name=display_name,
    )
    response = client.create_run(run_details)
    run_id = response.data.id
    log.info("Run criado: %s (ID: %s)", display_name, run_id)

    # --- Polling até estado terminal ---
    elapsed = 0
    while elapsed < RUN_TIMEOUT_SECONDS:
        run = client.get_run(run_id).data
        state = run.lifecycle_state
        log.info("[%s] Estado: %s (elapsed: %ds)", display_name, state, elapsed)

        if state in TERMINAL_STATES:
            break

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS
    else:
        raise AirflowException(
            f"Timeout após {RUN_TIMEOUT_SECONDS}s aguardando run '{display_name}' (ID: {run_id})"
        )

    if state != SUCCESS_STATE:
        raise AirflowException(
            f"Run '{display_name}' terminou com estado '{state}' (esperado: SUCCEEDED). "
            f"Verifique os logs em: Console OCI > Data Flow > Runs > {run_id}"
        )

    log.info("Run '%s' concluído com SUCESSO.", display_name)


def make_task(task_id: str, app_var: str, run_label: str):
    """
    Factory que cria um PythonOperator para disparar uma app Data Flow.

    Args:
        task_id:   ID da task no Airflow (ex: "silver_recarga").
        app_var:   Nome da Airflow Variable com o OCID da app (ex: "oci_app_id_silver_recarga").
        run_label: Rótulo descritivo do run (aparece no Console OCI).

    Returns:
        PythonOperator configurado.
    """
    def _run(**context):
        app_id = Variable.get(app_var)
        compartment_id = Variable.get("oci_compute_compartment_id")
        execution_date = context["ds"]  # ex: "2026-02-01"
        _trigger_and_wait(
            app_id=app_id,
            display_name=f"{run_label} [{execution_date}]",
            compartment_id=compartment_id,
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=_run,
    )


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description="Pipeline FPD completo: Bronze → Silver → Gold Features → ABT v6",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fpd", "credit-risk", "oci", "data-flow", "medallion"],
    doc_md=__doc__,
) as dag:

    # --------------------------------------------------------
    # BRONZE — 6 apps em paralelo
    # Lê de: landing-zone (CSVs/Parquets brutos)
    # Escreve em: bronze-layer (Delta + metadados)
    # --------------------------------------------------------
    with TaskGroup("bronze", tooltip="Ingestão Bronze (paralelo)") as tg_bronze:
        b_bureau    = make_task("bureau",    "oci_app_id_bronze_bureau",    "bronze-bureau")
        b_telco     = make_task("telco",     "oci_app_id_bronze_telco",     "bronze-telco")
        b_cadastro  = make_task("cadastro",  "oci_app_id_bronze_cadastro",  "bronze-cadastro")
        b_recarga   = make_task("recarga",   "oci_app_id_bronze_recarga",   "bronze-recarga")
        b_pagamento = make_task("pagamento", "oci_app_id_bronze_pagamento", "bronze-pagamento")
        b_atraso    = make_task("atraso",    "oci_app_id_bronze_atraso",    "bronze-atraso")

    # --------------------------------------------------------
    # SILVER — 6 apps em paralelo
    # Lê de: bronze-layer
    # Escreve em: silver-layer (tipagem, validação, dedup)
    # --------------------------------------------------------
    with TaskGroup("silver", tooltip="Transformação Silver (paralelo)") as tg_silver:
        s_bureau    = make_task("bureau",    "oci_app_id_silver_bureau",    "silver-bureau")
        s_telco     = make_task("telco",     "oci_app_id_silver_telco",     "silver-telco")
        s_cadastro  = make_task("cadastro",  "oci_app_id_silver_cadastro",  "silver-cadastro")
        s_recarga   = make_task("recarga",   "oci_app_id_silver_recarga",   "silver-recarga")
        s_pagamento = make_task("pagamento", "oci_app_id_silver_pagamento", "silver-pagamento")
        s_atraso    = make_task("atraso",    "oci_app_id_silver_atraso",    "silver-atraso")

    # --------------------------------------------------------
    # GOLD FEATURES — 3 apps em paralelo
    # Lê de: silver-layer
    # Escreve em: gold-layer (features por janelas M1/M3/M6)
    # --------------------------------------------------------
    with TaskGroup("gold_features", tooltip="Feature Engineering Gold (paralelo)") as tg_gold:
        g_recarga   = make_task("recarga",   "oci_app_id_gold_recarga",   "gold-recarga")
        g_pagamento = make_task("pagamento", "oci_app_id_gold_pagamento", "gold-pagamento")
        g_atraso    = make_task("atraso",    "oci_app_id_gold_atraso",    "gold-atraso")

    # --------------------------------------------------------
    # ABT — 6 apps sequenciais (v1 → v2 → v3 → v4 → v5 → v6)
    # Cada versão consome a ABT anterior como base.
    # Escreve: gold-layer/abt_v6/ (~614 features, 3.79M registros)
    # --------------------------------------------------------
    with TaskGroup("abt", tooltip="ABT Builders (sequencial v1→v6)") as tg_abt:
        abt_v1 = make_task("v1", "oci_app_id_abt_v1", "abt-v1")
        abt_v2 = make_task("v2", "oci_app_id_abt_v2", "abt-v2")
        abt_v3 = make_task("v3", "oci_app_id_abt_v3", "abt-v3")
        abt_v4 = make_task("v4", "oci_app_id_abt_v4", "abt-v4")
        abt_v5 = make_task("v5", "oci_app_id_abt_v5", "abt-v5")
        abt_v6 = make_task("v6", "oci_app_id_abt_v6", "abt-v6")

        # Cadeia estrita: cada ABT precisa da anterior
        abt_v1 >> abt_v2 >> abt_v3 >> abt_v4 >> abt_v5 >> abt_v6

    # --------------------------------------------------------
    # Dependências entre grupos (ordem do pipeline)
    # --------------------------------------------------------
    tg_bronze >> tg_silver >> tg_gold >> tg_abt
