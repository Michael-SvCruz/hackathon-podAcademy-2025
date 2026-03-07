#!/bin/bash
# ============================================================
# deploy_modelo.sh — Deploy do script de scoring para a VM do Modelo
# ============================================================
# A VM do Modelo está na subnet privada (sem IP público).
# Este script usa a VM do Airflow como jump host para:
#   1. Copiar o script para a VM do Modelo via Airflow
#   2. Copiar a chave SSH para o Airflow (para a DAG usar)
#
# Executar LOCALMENTE (WSL/Linux/Mac).
#
# Uso:
#   cd mig_oci/airflow
#   chmod +x deploy_modelo.sh
#   ./deploy_modelo.sh <AIRFLOW_IP> <MODELO_PRIVATE_IP> [AIRFLOW_SSH_KEY] [MODELO_SSH_KEY]
#
# Exemplo:
#   ./deploy_modelo.sh 137.131.199.10 10.0.2.5 ~/.ssh/airflow_vm ~/.ssh/modelo_vm
# ============================================================

set -euo pipefail

# ---- Parâmetros ----
AIRFLOW_IP="${1:?Uso: ./deploy_modelo.sh <AIRFLOW_IP> <MODELO_PRIVATE_IP> [AIRFLOW_SSH_KEY] [MODELO_SSH_KEY]}"
MODELO_IP="${2:?Uso: ./deploy_modelo.sh <AIRFLOW_IP> <MODELO_PRIVATE_IP> [AIRFLOW_SSH_KEY] [MODELO_SSH_KEY]}"
AIRFLOW_KEY="${3:-~/.ssh/airflow_vm}"
MODELO_KEY="${4:-~/.ssh/modelo_vm}"
VM_USER="opc"

# Diretório deste script (raiz do airflow/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODELO_SCRIPT="${SCRIPT_DIR}/../data_science/scripts/modelo_qualificacao.py"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

echo ""
echo "========================================"
echo "  Deploy Modelo → VM OCI (via jump host)"
echo "========================================"
echo "  Airflow VM:  $VM_USER@$AIRFLOW_IP (jump host)"
echo "  Modelo VM:   $VM_USER@$MODELO_IP (destino)"
echo "  Airflow key: $AIRFLOW_KEY"
echo "  Modelo key:  $MODELO_KEY"
echo ""

# ---- Verificar arquivo do script ----
if [ ! -f "$MODELO_SCRIPT" ]; then
    echo "ERRO: Script não encontrado: $MODELO_SCRIPT"
    echo "  Caminho esperado: mig_oci/data_science/scripts/modelo_qualificacao.py"
    exit 1
fi

# ---- Verificar chave do modelo ----
if [ ! -f "$MODELO_KEY" ]; then
    echo "ERRO: Chave SSH do modelo não encontrada: $MODELO_KEY"
    echo "  Gere com: ssh-keygen -t ed25519 -f ~/.ssh/modelo_vm -N ''"
    exit 1
fi

# ---- Step 1: Verificar conectividade com Airflow ----
echo "--- [1/5] Verificando conexão SSH com Airflow VM ---"
ssh -i "$AIRFLOW_KEY" $SSH_OPTS "$VM_USER@$AIRFLOW_IP" "echo '  Conectado ao Airflow!'" || {
    echo "ERRO: Não foi possível conectar à VM do Airflow."
    exit 1
}

# ---- Step 2: Copiar chave SSH do modelo para Airflow ----
echo ""
echo "--- [2/5] Copiando chave SSH do modelo para Airflow ---"
scp -i "$AIRFLOW_KEY" $SSH_OPTS "$MODELO_KEY" "$VM_USER@$AIRFLOW_IP:/tmp/modelo_vm_key"
ssh -i "$AIRFLOW_KEY" $SSH_OPTS "$VM_USER@$AIRFLOW_IP" \
    "mkdir -p /opt/airflow-fpd/config && \
     mv /tmp/modelo_vm_key /opt/airflow-fpd/config/modelo_vm_key && \
     chmod 600 /opt/airflow-fpd/config/modelo_vm_key"
echo "  Chave copiada para /opt/airflow-fpd/config/modelo_vm_key"

# ---- Step 3: Verificar conectividade Airflow → Modelo ----
echo ""
echo "--- [3/5] Verificando conexão Airflow → Modelo VM ---"
ssh -i "$AIRFLOW_KEY" $SSH_OPTS "$VM_USER@$AIRFLOW_IP" \
    "ssh -i /opt/airflow-fpd/config/modelo_vm_key $SSH_OPTS $VM_USER@$MODELO_IP 'echo \"  Conectado ao Modelo!\"'" || {
    echo "ERRO: Airflow não consegue conectar na VM do Modelo."
    echo "  Verifique: Security List da subnet privada permite tráfego VCN interno (porta 22)"
    exit 1
}

# ---- Step 4: Copiar script para Airflow (staging) ----
echo ""
echo "--- [4/5] Copiando script para Airflow (staging) ---"
scp -i "$AIRFLOW_KEY" $SSH_OPTS "$MODELO_SCRIPT" "$VM_USER@$AIRFLOW_IP:/tmp/modelo_qualificacao.py"
echo "  Script copiado para Airflow:/tmp/"

# ---- Step 5: Copiar script do Airflow para Modelo VM ----
echo ""
echo "--- [5/5] Copiando script para VM do Modelo ---"
ssh -i "$AIRFLOW_KEY" $SSH_OPTS "$VM_USER@$AIRFLOW_IP" \
    "scp -i /opt/airflow-fpd/config/modelo_vm_key $SSH_OPTS \
     /tmp/modelo_qualificacao.py $VM_USER@$MODELO_IP:/opt/modelo-fpd/modelo_qualificacao.py && \
     rm /tmp/modelo_qualificacao.py"
echo "  Script copiado para Modelo VM:/opt/modelo-fpd/"

# ---- Verificação ----
echo ""
echo "--- Verificando instalação ---"
ssh -i "$AIRFLOW_KEY" $SSH_OPTS "$VM_USER@$AIRFLOW_IP" \
    "ssh -i /opt/airflow-fpd/config/modelo_vm_key $SSH_OPTS $VM_USER@$MODELO_IP \
     'ls -la /opt/modelo-fpd/modelo_qualificacao.py && python3.11 -c \"import lightgbm; print(f\\\"LightGBM {lightgbm.__version__}\\\")\"'"

echo ""
echo "========================================"
echo "  Deploy concluído com sucesso!"
echo "========================================"
echo ""
echo "Próximos passos:"
echo "  1. Parar a VM do modelo (para iniciar via DAG):"
echo "     Executar LOCAL: oci compute instance action --action STOP --instance-id <MODELO_VM_OCID>"
echo "  2. Deploy da DAG no Airflow:"
echo "     Executar LOCAL: scp -i $AIRFLOW_KEY $SCRIPT_DIR/dags/dag_modelo_qualificacao.py $VM_USER@$AIRFLOW_IP:/opt/airflow-fpd/dags/"
echo "  3. Importar variáveis no Airflow (na VM Airflow):"
echo "     Executar VM AIRFLOW: docker compose exec airflow-scheduler airflow variables import /opt/airflow-fpd/config/airflow_variables_filled.json"
echo "  4. Trigger manual da DAG no Airflow UI:"
echo "     Executar BROWSER: http://$AIRFLOW_IP:8080 → DAGs → pipeline_modelo_qualificacao → Trigger"
echo ""
