"""
DAG: pipeline_fpd_sequential
=================================
Versao sequencial do pipeline FPD para ambientes com quota limitada.

Executa uma aplicacao Data Flow por vez (sem paralelismo),
respeitando a ordem do pipeline Medallion:

    Bronze:  bureau → telco → cadastro → recarga → pagamento → atraso
    Silver:  bureau → telco → cadastro → recarga → pagamento → atraso
    Gold:    recarga → pagamento → atraso
    ABT:     v1 → v2 → v3 → v4 → v5 → v6

Total: 21 apps executadas sequencialmente.

Uso: trigger manual para teste do Airflow + Data Flow em ambiente OCI free tier.
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
log.info("Carregando DAG %s v%s", "pipeline_fpd_sequential", "2.1.0")

# ============================================================
# Configuracoes
# ============================================================

DAG_ID = "pipeline_fpd_sequential"
DAG_VERSION = "2.1.0"  # Incrementar a cada deploy para confirmar reload no Airflow
START_DATE = datetime(2026, 2, 1)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=4),
}

RUN_TIMEOUT_SECONDS = 3 * 60 * 60
POLL_INTERVAL_SECONDS = 60

CREATE_RUN_MAX_RETRIES = 3
CREATE_RUN_BASE_DELAY = 30

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "DELETED"}
ACTIVE_STATES = {"ACCEPTED", "IN_PROGRESS", "CANCELING"}
SUCCESS_STATE = "SUCCEEDED"


# ============================================================
# Helpers OCI Data Flow (identicos a DAG principal)
# ============================================================

def _get_oci_client():
    import oci
    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.data_flow.DataFlowClient({}, signer=signer)
        log.info("Autenticado via Instance Principal.")
        return client
    except Exception as e:
        log.warning("Instance Principal falhou (%s). Tentando ~/.oci/config...", e)
        config = oci.config.from_file()
        return oci.data_flow.DataFlowClient(config)


def _find_active_run(client, compartment_id: str, app_id: str, display_name: str):
    """
    Busca um run ativo (ACCEPTED ou IN_PROGRESS) com o mesmo display_name.
    Previne duplicatas quando o Airflow faz retry apos SIGTERM.

    Nota: list_runs so aceita UM filtro alem de compartment_id, entao
    filtramos por application_id e checamos lifecycle_state em Python.
    """
    try:
        response = client.list_runs(
            compartment_id,
            application_id=app_id,
            sort_by="timeCreated",
            sort_order="DESC",
            limit=20,
        )
        for run in response.data:
            if run.lifecycle_state in ACTIVE_STATES and run.display_name == display_name:
                log.info(
                    "[%s] Encontrado run existente em estado %s (ID: %s)",
                    display_name, run.lifecycle_state, run.id,
                )
                return run.id
    except Exception as e:
        log.warning("[%s] Erro ao buscar runs ativos: %s. Prosseguindo com create_run.", display_name, e)

    return None


def _trigger_and_wait(app_id: str, display_name: str, compartment_id: str) -> None:
    import oci
    from airflow.exceptions import AirflowException

    client = _get_oci_client()

    # --- Verificar se ja existe run ativo com mesmo display_name ---
    run_id = _find_active_run(client, compartment_id, app_id, display_name)

    if run_id:
        log.info(
            "[%s] Run ativo encontrado (ID: %s). Monitorando em vez de criar novo.",
            display_name, run_id,
        )
    else:
        # --- Disparar novo run (com retry para API rate limiting 429) ---
        run_details = oci.data_flow.models.CreateRunDetails(
            application_id=app_id,
            compartment_id=compartment_id,
            display_name=display_name,
        )

        for attempt in range(1, CREATE_RUN_MAX_RETRIES + 1):
            try:
                response = client.create_run(run_details)
                break
            except (oci.exceptions.TransientServiceError, oci.exceptions.ServiceError) as e:
                is_limit = (
                    isinstance(e, oci.exceptions.ServiceError)
                    and getattr(e, "code", "") == "LimitExceeded"
                )
                is_transient = isinstance(e, oci.exceptions.TransientServiceError)
                if not is_limit and not is_transient:
                    raise
                if attempt == CREATE_RUN_MAX_RETRIES:
                    raise AirflowException(
                        f"create_run falhou apos {CREATE_RUN_MAX_RETRIES} tentativas "
                        f"(ultimo erro: {e})"
                    )
                delay = CREATE_RUN_BASE_DELAY * (2 ** (attempt - 1))
                reason = "LimitExceeded (quota)" if is_limit else "429 TooManyRequests"
                log.warning(
                    "[%s] %s (tentativa %d/%d). Aguardando %ds antes de retry...",
                    display_name, reason, attempt, CREATE_RUN_MAX_RETRIES, delay,
                )
                time.sleep(delay)

        run_id = response.data.id
        log.info("Run criado: %s (ID: %s)", display_name, run_id)

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
            f"Timeout apos {RUN_TIMEOUT_SECONDS}s aguardando run '{display_name}' (ID: {run_id})"
        )

    if state != SUCCESS_STATE:
        raise AirflowException(
            f"Run '{display_name}' terminou com estado '{state}' (esperado: SUCCEEDED). "
            f"Verifique os logs em: Console OCI > Data Flow > Runs > {run_id}"
        )

    log.info("Run '%s' concluido com SUCESSO.", display_name)


def make_task(task_id: str, app_var: str, run_label: str):
    def _run(**context):
        app_id = Variable.get(app_var)
        compartment_id = Variable.get("oci_compute_compartment_id")
        execution_date = context["ds"]
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
# Definicao sequencial das 21 apps
# ============================================================
# Cada layer mantem TaskGroup para organizacao visual na UI,
# mas dentro de cada grupo as tasks sao encadeadas sequencialmente.

BRONZE_APPS = [
    ("bureau",    "oci_app_id_bronze_bureau",    "bronze-bureau"),
    ("telco",     "oci_app_id_bronze_telco",     "bronze-telco"),
    ("cadastro",  "oci_app_id_bronze_cadastro",  "bronze-cadastro"),
    ("recarga",   "oci_app_id_bronze_recarga",   "bronze-recarga"),
    ("pagamento", "oci_app_id_bronze_pagamento", "bronze-pagamento"),
    ("atraso",    "oci_app_id_bronze_atraso",    "bronze-atraso"),
]

SILVER_APPS = [
    ("bureau",    "oci_app_id_silver_bureau",    "silver-bureau"),
    ("telco",     "oci_app_id_silver_telco",     "silver-telco"),
    ("cadastro",  "oci_app_id_silver_cadastro",  "silver-cadastro"),
    ("recarga",   "oci_app_id_silver_recarga",   "silver-recarga"),
    ("pagamento", "oci_app_id_silver_pagamento", "silver-pagamento"),
    ("atraso",    "oci_app_id_silver_atraso",    "silver-atraso"),
]

GOLD_APPS = [
    ("recarga",   "oci_app_id_gold_recarga",   "gold-recarga"),
    ("pagamento", "oci_app_id_gold_pagamento", "gold-pagamento"),
    ("atraso",    "oci_app_id_gold_atraso",    "gold-atraso"),
]

ABT_APPS = [
    ("v1", "oci_app_id_abt_v1", "abt-v1"),
    ("v2", "oci_app_id_abt_v2", "abt-v2"),
    ("v3", "oci_app_id_abt_v3", "abt-v3"),
    ("v4", "oci_app_id_abt_v4", "abt-v4"),
    ("v5", "oci_app_id_abt_v5", "abt-v5"),
    ("v6", "oci_app_id_abt_v6", "abt-v6"),
]


def _build_sequential_chain(apps_config, group_id, tooltip):
    """Cria TaskGroup com tasks encadeadas sequencialmente."""
    with TaskGroup(group_id, tooltip=tooltip) as tg:
        tasks = [make_task(*cfg) for cfg in apps_config]
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]
    return tg


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description="Pipeline FPD sequencial (1 app por vez) para teste com quota limitada",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fpd", "credit-risk", "oci", "data-flow", "sequential", "test"],
    doc_md=f"**DAG Version: {DAG_VERSION}**\n\n{__doc__}",
) as dag:

    tg_bronze = _build_sequential_chain(
        BRONZE_APPS, "bronze", "Bronze sequencial: bureau→telco→cadastro→recarga→pagamento→atraso"
    )
    tg_silver = _build_sequential_chain(
        SILVER_APPS, "silver", "Silver sequencial: bureau→telco→cadastro→recarga→pagamento→atraso"
    )
    tg_gold = _build_sequential_chain(
        GOLD_APPS, "gold_features", "Gold sequencial: recarga→pagamento→atraso"
    )
    tg_abt = _build_sequential_chain(
        ABT_APPS, "abt", "ABT sequencial: v1→v2→v3→v4→v5→v6"
    )

    tg_bronze >> tg_silver >> tg_gold >> tg_abt
