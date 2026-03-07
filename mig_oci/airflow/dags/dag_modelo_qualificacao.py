"""
DAG: pipeline_modelo_qualificacao (v2.1.0)
==========================================
Executa o modelo de qualificação FPD em VM dedicada OCI.

Fluxo:
    1. start_vm: Liga a VM do modelo (ComputeClient.instance_action START)
    2. run_modelo: Executa o script via SSH (python3.11 modelo_qualificacao.py)
    3. stop_vm: Desliga a VM (trigger_rule=ALL_DONE — sempre executa)

A VM usa shape VM.Standard.E5.Flex e fica STOPPED entre execuções
para reduzir custos. O Airflow VM (subnet pública 10.0.1.x) conecta
via SSH na Modelo VM (subnet privada 10.0.2.x) pela VCN interna.

Modos de execução:
    - Automático: Disparada via TriggerDagRunOperator ao final da DAG
      pipeline_credit_risk_fpd (ETL completo → Modelo scoring)
    - Manual: Trigger manual via UI ou CLI para re-scoring sem re-ETL

Agendamento:
    - None: sem schedule próprio. Execução via trigger automático (ETL)
      ou trigger manual (re-scoring independente)

Configuração necessária (Airflow Variables):
    - oci_modelo_vm_id: OCID da VM do modelo (terraform output modelo_vm_info.vm_id)
    - oci_modelo_vm_ip: IP privado da VM (terraform output modelo_vm_info.private_ip)
    - oci_compute_compartment_id: OCID do compartment de compute (já existe)

SSH:
    - Chave privada: /opt/airflow-fpd/config/modelo_vm_key
    - Usuário: opc (Oracle Linux default)

Autenticação OCI:
    - Airflow em OCI: Instance Principal (automático via Dynamic Group)
    - Airflow local/dev: ~/.oci/config (fallback)
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)
log.info("Carregando DAG %s v%s", "pipeline_modelo_qualificacao", "2.1.0")

# ============================================================
# Configurações da DAG
# ============================================================

DAG_ID = "pipeline_modelo_qualificacao"
DAG_VERSION = "2.1.0"
SCHEDULE = None  # Trigger manual
START_DATE = datetime(2026, 2, 1)

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Timeouts e polling
VM_START_TIMEOUT_SECONDS = 10 * 60   # 10 min para a VM ligar
VM_START_POLL_SECONDS = 15
SSH_TIMEOUT_SECONDS = 3 * 60 * 60    # 3h para o script
SSH_KEY_PATH = "/opt/airflow/config/modelo_vm_key"
SSH_USER = "opc"
SCRIPT_PATH = "/opt/modelo-fpd/modelo_qualificacao.py"


# ============================================================
# Helpers OCI Compute
# ============================================================

def _get_compute_client():
    """Retorna ComputeClient autenticado (Instance Principal ou ~/.oci/config)."""
    import oci

    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.core.ComputeClient({}, signer=signer)
        log.info("Autenticado via Instance Principal.")
        return client
    except Exception as e:
        log.warning("Instance Principal falhou (%s). Tentando ~/.oci/config...", e)
        config = oci.config.from_file()
        return oci.core.ComputeClient(config)


def _get_vm_state(client, vm_id: str) -> str:
    """Retorna o lifecycle_state da VM."""
    response = client.get_instance(vm_id)
    return response.data.lifecycle_state


# ============================================================
# Task 1: Start VM
# ============================================================

def _start_vm(**context):
    """Liga a VM do modelo e aguarda estado RUNNING."""
    from airflow.exceptions import AirflowException

    vm_id = Variable.get("oci_modelo_vm_id")
    client = _get_compute_client()

    current_state = _get_vm_state(client, vm_id)
    log.info("Estado atual da VM: %s", current_state)

    if current_state == "RUNNING":
        log.info("VM já está RUNNING. Prosseguindo.")
        return

    if current_state == "STOPPED":
        log.info("Enviando comando START para a VM...")
        client.instance_action(vm_id, "START")
    elif current_state == "STOPPING":
        log.info("VM está STOPPING. Aguardando STOPPED antes de iniciar...")
        elapsed = 0
        while elapsed < VM_START_TIMEOUT_SECONDS:
            state = _get_vm_state(client, vm_id)
            if state == "STOPPED":
                break
            time.sleep(VM_START_POLL_SECONDS)
            elapsed += VM_START_POLL_SECONDS
        else:
            raise AirflowException(
                f"Timeout aguardando VM sair de STOPPING (elapsed: {elapsed}s)"
            )
        client.instance_action(vm_id, "START")
    elif current_state == "STARTING":
        log.info("VM já está STARTING. Aguardando RUNNING...")
    else:
        raise AirflowException(
            f"VM em estado inesperado: {current_state}. Esperado: STOPPED, RUNNING ou STARTING."
        )

    # Poll até RUNNING
    elapsed = 0
    while elapsed < VM_START_TIMEOUT_SECONDS:
        state = _get_vm_state(client, vm_id)
        log.info("  VM state: %s (elapsed: %ds)", state, elapsed)

        if state == "RUNNING":
            log.info("VM está RUNNING.")
            # Aguardar SSH ficar disponível (cloud-init + sshd startup)
            time.sleep(30)
            return

        time.sleep(VM_START_POLL_SECONDS)
        elapsed += VM_START_POLL_SECONDS

    raise AirflowException(
        f"Timeout: VM não ficou RUNNING em {VM_START_TIMEOUT_SECONDS}s"
    )


# ============================================================
# Task 2: Run Modelo (SSH)
# ============================================================

def _run_modelo(**context):
    """Executa o script de scoring via SSH na VM do modelo."""
    from airflow.exceptions import AirflowException

    vm_ip = Variable.get("oci_modelo_vm_ip")

    ssh_cmd = [
        "ssh",
        "-i", SSH_KEY_PATH,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=30",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=10",
        f"{SSH_USER}@{vm_ip}",
        f"python3.11 -u {SCRIPT_PATH}",
    ]

    log.info("Executando: %s", " ".join(ssh_cmd))

    result = subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=SSH_TIMEOUT_SECONDS,
    )

    # Log stdout e stderr
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            log.info("[modelo-stdout] %s", line)

    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            log.warning("[modelo-stderr] %s", line)

    if result.returncode != 0:
        raise AirflowException(
            f"Script falhou com exit code {result.returncode}. "
            f"Verifique logs acima."
        )

    log.info("Script de scoring concluído com sucesso.")


# ============================================================
# Task 3: Stop VM (sempre executa)
# ============================================================

def _stop_vm(**context):
    """Desliga a VM do modelo. Sempre executa (trigger_rule=ALL_DONE)."""
    vm_id = Variable.get("oci_modelo_vm_id")
    client = _get_compute_client()

    current_state = _get_vm_state(client, vm_id)
    log.info("Estado atual da VM: %s", current_state)

    if current_state in ("STOPPED", "STOPPING"):
        log.info("VM já está %s. Nada a fazer.", current_state)
        return

    if current_state == "RUNNING":
        log.info("Enviando comando STOP para a VM...")
        client.instance_action(vm_id, "STOP")
        log.info("Comando STOP enviado. VM será desligada.")
    else:
        log.warning("VM em estado %s. Tentando STOP mesmo assim...", current_state)
        try:
            client.instance_action(vm_id, "STOP")
        except Exception as e:
            log.error("Erro ao enviar STOP: %s", e)


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description="Modelo de qualificação FPD: VM Start → SSH scoring → VM Stop",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fpd", "credit-risk", "oci", "vm", "modelo", "scoring"],
    doc_md=f"**DAG Version: {DAG_VERSION}**\n\n{__doc__}",
) as dag:

    start_vm = PythonOperator(
        task_id="start_vm",
        python_callable=_start_vm,
    )

    run_modelo = PythonOperator(
        task_id="run_modelo",
        python_callable=_run_modelo,
    )

    stop_vm = PythonOperator(
        task_id="stop_vm",
        python_callable=_stop_vm,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_vm >> run_modelo >> stop_vm
