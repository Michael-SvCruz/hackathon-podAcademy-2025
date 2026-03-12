#!/bin/bash
# ============================================================
# Limpar buckets Bronze, Silver e Gold para medição de storage
# ============================================================
# Execução: LOCAL ou VM com OCI CLI configurado
#
# Uso:
#   chmod +x limpar_buckets_medallion.sh
#   ./limpar_buckets_medallion.sh
#
# O que faz:
#   1. Lista tamanho atual de cada bucket (antes)
#   2. Apaga TODOS os objetos dos 3 buckets
#   3. Confirma que estão vazios
#
# ATENÇÃO: Isso é DESTRUTIVO. Os dados serão removidos.
#          A landing-zone, pipeline-ops e models NÃO são tocados.
# ============================================================

set -euo pipefail

# --- Configuração ---
BUCKETS=(
    "hackathon-2025-bronze-layer"
    "hackathon-2025-silver-layer"
    "hackathon-2025-gold-layer"
)

# Detectar namespace automaticamente
NAMESPACE=$(oci os ns get --query 'data' --raw-output 2>/dev/null)
if [ -z "$NAMESPACE" ]; then
    echo "[ERRO] Não foi possível obter o namespace. Verifique a configuração do OCI CLI."
    echo "       Execute: oci setup config"
    exit 1
fi
echo "Namespace: $NAMESPACE"
echo ""

# --- Etapa 1: Mostrar tamanho atual ---
echo "============================================"
echo " TAMANHO ATUAL DOS BUCKETS (ANTES)"
echo "============================================"
for BUCKET in "${BUCKETS[@]}"; do
    echo ""
    echo "--- $BUCKET ---"

    # Contar objetos e tamanho total
    OBJECTS=$(oci os object list \
        --namespace "$NAMESPACE" \
        --bucket-name "$BUCKET" \
        --all \
        --query 'data[*].size' \
        --output json 2>/dev/null || echo "[]")

    if [ "$OBJECTS" = "[]" ] || [ -z "$OBJECTS" ]; then
        echo "  Objetos: 0"
        echo "  Tamanho: 0 bytes"
    else
        COUNT=$(echo "$OBJECTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))")
        TOTAL_BYTES=$(echo "$OBJECTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(d))")
        TOTAL_MB=$(echo "$OBJECTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{sum(d)/1024/1024:.2f}')")
        TOTAL_GB=$(echo "$OBJECTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{sum(d)/1024/1024/1024:.2f}')")
        echo "  Objetos: $COUNT"
        echo "  Tamanho: ${TOTAL_MB} MB (${TOTAL_GB} GB)"
    fi
done

echo ""
echo "============================================"
echo " CONFIRMAÇÃO"
echo "============================================"
echo ""
echo "Os seguintes buckets serão ESVAZIADOS:"
for BUCKET in "${BUCKETS[@]}"; do
    echo "  - $BUCKET"
done
echo ""
echo "Buckets NÃO afetados:"
echo "  - hackathon-2025-landing-zone"
echo "  - hackathon-2025-pipeline-ops"
echo "  - hackathon-2025-models"
echo "  - hackathon-2025-tfstate"
echo ""
read -p "Deseja continuar? (digite 'SIM' para confirmar): " CONFIRM
if [ "$CONFIRM" != "SIM" ]; then
    echo "Operação cancelada."
    exit 0
fi

# --- Etapa 2: Apagar objetos ---
echo ""
echo "============================================"
echo " LIMPANDO BUCKETS"
echo "============================================"
for BUCKET in "${BUCKETS[@]}"; do
    echo ""
    echo "--- Limpando: $BUCKET ---"

    # Listar todos os objetos
    OBJECT_NAMES=$(oci os object list \
        --namespace "$NAMESPACE" \
        --bucket-name "$BUCKET" \
        --all \
        --query 'data[*].name' \
        --output json 2>/dev/null || echo "[]")

    if [ "$OBJECT_NAMES" = "[]" ] || [ -z "$OBJECT_NAMES" ]; then
        echo "  Bucket já está vazio."
        continue
    fi

    # Usar bulk-delete para performance
    echo "  Deletando objetos..."
    oci os object bulk-delete \
        --namespace "$NAMESPACE" \
        --bucket-name "$BUCKET" \
        --force \
        2>&1 | tail -1

    echo "  Concluído."
done

# --- Etapa 3: Verificar ---
echo ""
echo "============================================"
echo " VERIFICAÇÃO (DEPOIS)"
echo "============================================"
for BUCKET in "${BUCKETS[@]}"; do
    COUNT=$(oci os object list \
        --namespace "$NAMESPACE" \
        --bucket-name "$BUCKET" \
        --limit 1 \
        --query 'length(data)' \
        --output json 2>/dev/null || echo "0")
    echo "  $BUCKET: $COUNT objetos"
done

echo ""
echo "============================================"
echo " PRONTO"
echo "============================================"
echo ""
echo "Buckets bronze, silver e gold estão vazios."
echo ""
echo "Próximos passos:"
echo "  1. Ligar a Airflow VM"
echo "  2. Disparar a DAG: pipeline_credit_risk_fpd"
echo "  3. Após conclusão, rodar o script de medição:"
echo "     ./medir_buckets.sh"
echo ""
