#!/bin/bash
# ============================================================
# Medir tamanho de TODOS os buckets do projeto
# ============================================================
# Execução: LOCAL ou VM com OCI CLI configurado
#
# Uso:
#   chmod +x medir_buckets.sh
#   ./medir_buckets.sh
#
# Saída: tabela com objetos, tamanho em MB e GB por bucket
# ============================================================

set -euo pipefail

BUCKETS=(
    "hackathon-2025-landing-zone"
    "hackathon-2025-pipeline-ops"
    "hackathon-2025-bronze-layer"
    "hackathon-2025-silver-layer"
    "hackathon-2025-gold-layer"
    "hackathon-2025-models"
    "hackathon-2025-tfstate"
)

NAMESPACE=$(oci os ns get --query 'data' --raw-output 2>/dev/null)
if [ -z "$NAMESPACE" ]; then
    echo "[ERRO] Não foi possível obter o namespace."
    exit 1
fi

echo "============================================"
echo " MEDIÇÃO DE BUCKETS — $(date '+%Y-%m-%d %H:%M')"
echo " Namespace: $NAMESPACE"
echo "============================================"
echo ""

printf "%-40s %10s %12s %10s\n" "BUCKET" "OBJETOS" "MB" "GB"
printf "%-40s %10s %12s %10s\n" "----------------------------------------" "----------" "------------" "----------"

TOTAL_OBJECTS=0
TOTAL_BYTES=0

for BUCKET in "${BUCKETS[@]}"; do
    OBJECTS=$(oci os object list \
        --namespace "$NAMESPACE" \
        --bucket-name "$BUCKET" \
        --all \
        --query 'data[*].size' \
        --output json 2>/dev/null || echo "[]")

    if [ "$OBJECTS" = "[]" ] || [ -z "$OBJECTS" ]; then
        printf "%-40s %10d %12s %10s\n" "$BUCKET" 0 "0.00" "0.00"
    else
        RESULT=$(echo "$OBJECTS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
count = len(d)
total = sum(d)
mb = total / 1024 / 1024
gb = total / 1024 / 1024 / 1024
print(f'{count} {total} {mb:.2f} {gb:.2f}')
")
        COUNT=$(echo "$RESULT" | awk '{print $1}')
        BYTES=$(echo "$RESULT" | awk '{print $2}')
        MB=$(echo "$RESULT" | awk '{print $3}')
        GB=$(echo "$RESULT" | awk '{print $4}')

        printf "%-40s %10s %12s %10s\n" "$BUCKET" "$COUNT" "$MB" "$GB"

        TOTAL_OBJECTS=$((TOTAL_OBJECTS + COUNT))
        TOTAL_BYTES=$((TOTAL_BYTES + BYTES))
    fi
done

TOTAL_MB=$(python3 -c "print(f'{$TOTAL_BYTES/1024/1024:.2f}')")
TOTAL_GB=$(python3 -c "print(f'{$TOTAL_BYTES/1024/1024/1024:.2f}')")

printf "%-40s %10s %12s %10s\n" "----------------------------------------" "----------" "------------" "----------"
printf "%-40s %10d %12s %10s\n" "TOTAL" "$TOTAL_OBJECTS" "$TOTAL_MB" "$TOTAL_GB"

echo ""
echo "============================================"
echo ""
