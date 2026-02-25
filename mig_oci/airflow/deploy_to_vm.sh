#!/bin/bash
# ============================================================
# deploy_to_vm.sh — Copia arquivos do Airflow para a VM OCI
# ============================================================
# Executar LOCALMENTE (WSL/Linux/Mac) para enviar todos os
# arquivos necessários para a VM e iniciar o setup.
#
# Uso:
#   cd mig_oci/airflow
#   chmod +x deploy_to_vm.sh
#   ./deploy_to_vm.sh <VM_IP> [SSH_KEY_PATH]
#
# Exemplo:
#   ./deploy_to_vm.sh 137.131.199.10 ~/.ssh/airflow_vm
# ============================================================

set -euo pipefail

# ---- Parâmetros ----
VM_IP="${1:?Uso: ./deploy_to_vm.sh <VM_IP> [SSH_KEY_PATH]}"
SSH_KEY="${2:-~/.ssh/airflow_vm}"
VM_USER="opc"
REMOTE_DIR="/opt/airflow-fpd"

# Diretório deste script (raiz do airflow/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

echo ""
echo "========================================"
echo "  Deploy Airflow → VM OCI"
echo "========================================"
echo "  VM:     $VM_USER@$VM_IP"
echo "  Key:    $SSH_KEY"
echo "  Remote: $REMOTE_DIR"
echo ""

# ---- Verificar conectividade ----
echo "--- [1/4] Verificando conexão SSH ---"
ssh $SSH_OPTS "$VM_USER@$VM_IP" "echo '  Conectado!'" || {
    echo "ERRO: Não foi possível conectar via SSH."
    echo "  Verifique: IP correto, chave SSH, Security List porta 22"
    exit 1
}

# ---- Criar estrutura remota ----
echo ""
echo "--- [2/4] Criando estrutura de diretórios ---"
ssh $SSH_OPTS "$VM_USER@$VM_IP" "mkdir -p $REMOTE_DIR/{dags,plugins,config,logs}"
echo "  Diretórios criados."

# ---- Copiar arquivos ----
echo ""
echo "--- [3/4] Copiando arquivos ---"

copy_file() {
    local src="$1"
    local dst="$2"
    if [ -f "$src" ]; then
        scp -q $SSH_OPTS "$src" "$VM_USER@$VM_IP:$dst"
        echo "  ✔ $(basename "$src") → $dst"
    else
        echo "  ⚠ $(basename "$src") não encontrado (skip)"
    fi
}

# Docker Compose
copy_file "$SCRIPT_DIR/docker/docker-compose.yml" "$REMOTE_DIR/docker-compose.yml"

# Setup script
copy_file "$SCRIPT_DIR/docker/setup_vm.sh" "$REMOTE_DIR/setup_vm.sh"

# DAG
copy_file "$SCRIPT_DIR/dags/dag_pipeline_fpd.py" "$REMOTE_DIR/dags/dag_pipeline_fpd.py"

# Variables (preenchido)
copy_file "$SCRIPT_DIR/config/airflow_variables_filled.json" "$REMOTE_DIR/config/airflow_variables_filled.json"

# Tornar setup_vm.sh executável
ssh $SSH_OPTS "$VM_USER@$VM_IP" "chmod +x $REMOTE_DIR/setup_vm.sh"

# ---- Executar setup ----
echo ""
echo "--- [4/4] Executando setup na VM ---"
echo ""
ssh -t $SSH_OPTS "$VM_USER@$VM_IP" "cd $REMOTE_DIR && ./setup_vm.sh"
