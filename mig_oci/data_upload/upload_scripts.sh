#!/bin/bash
set -e

# ============================================
# Upload de scripts PySpark para OCI Object Storage
# ============================================
# Faz upload dos 17 scripts adaptados + utils.zip para o bucket landing-zone.
# Necessário ANTES de executar a Fase 4 do Terraform (Data Flow).
#
# Pré-requisitos:
#   - OCI CLI configurado (oci setup config)
#   - Bucket landing-zone criado (Fase 3 do Terraform)
#
# Uso:
#   ./upload_scripts.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$SCRIPT_DIR/scripts"
LIBS_DIR="$SCRIPT_DIR/libs"

# Configuração
BUCKET_NAME="hackathon-2025-landing-zone"
NAMESPACE=$(oci os ns get --query 'data' --raw-output 2>/dev/null)

if [ -z "$NAMESPACE" ]; then
  echo "ERRO: Não foi possível obter o namespace. Verifique a configuração do OCI CLI."
  echo "Execute: oci setup config"
  exit 1
fi

echo "=== Upload de Scripts para OCI ==="
echo ""
echo "Namespace: $NAMESPACE"
echo "Bucket:    $BUCKET_NAME"
echo "Scripts:   $SCRIPTS_DIR/"
echo "Libs:      $LIBS_DIR/"
echo ""

# --- Passo 1: Empacotar utils.zip ---
# OCI Data Flow exige que arquivos .py fiquem em python/lib/ dentro do ZIP
# Ref: https://docs.oracle.com/en-us/iaas/data-flow/using/third-party-libraries.htm
echo "--- Empacotando utils.zip (estrutura python/lib/) ---"
mkdir -p "$LIBS_DIR"
rm -f "$LIBS_DIR/utils.zip"
TEMP_ZIP_DIR=$(mktemp -d)
mkdir -p "$TEMP_ZIP_DIR/python/lib"
cp "$SCRIPTS_DIR/spark_utils.py" "$TEMP_ZIP_DIR/python/lib/"
cp "$SCRIPTS_DIR/validate_abt.py" "$TEMP_ZIP_DIR/python/lib/"
cd "$TEMP_ZIP_DIR"
zip -r "$LIBS_DIR/utils.zip" python/
cd "$SCRIPT_DIR"
rm -rf "$TEMP_ZIP_DIR"
echo "  utils.zip criado com estrutura: python/lib/spark_utils.py, python/lib/validate_abt.py"
echo ""

# --- Passo 2: Upload dos scripts ---
echo "--- Upload de scripts (21 arquivos) ---"
SCRIPTS=(
  bronze_bureau.py bronze_telco.py bronze_cadastro.py
  bronze_recarga.py bronze_pagamento.py bronze_atraso.py
  silver_bureau.py silver_telco.py silver_cadastro.py
  silver_recarga.py silver_pagamento.py silver_atraso.py
  gold_recarga.py gold_pagamento.py gold_atraso.py
  abt_v1_builder.py abt_v2_builder.py abt_v3_builder.py abt_v4_builder.py
  abt_v5_builder.py abt_v6_builder.py
)

uploaded=0
for script in "${SCRIPTS[@]}"; do
  if [ -f "$SCRIPTS_DIR/$script" ]; then
    echo "  Uploading $script..."
    oci os object put \
      --bucket-name "$BUCKET_NAME" \
      --file "$SCRIPTS_DIR/$script" \
      --name "scripts/$script" \
      --force
    uploaded=$((uploaded + 1))
  else
    echo "  AVISO: $script não encontrado em $SCRIPTS_DIR/"
  fi
done

echo ""
echo "Scripts enviados: $uploaded/${#SCRIPTS[@]}"

# --- Passo 3: Upload do utils.zip ---
echo ""
echo "--- Upload de libs ---"
if [ -f "$LIBS_DIR/utils.zip" ]; then
  echo "  Uploading utils.zip..."
  oci os object put \
    --bucket-name "$BUCKET_NAME" \
    --file "$LIBS_DIR/utils.zip" \
    --name "libs/utils.zip" \
    --force
  echo "  utils.zip enviado!"
else
  echo "  ERRO: utils.zip não encontrado em $LIBS_DIR/"
  exit 1
fi

echo ""
echo "=== Upload concluído! ==="
echo ""
echo "Verifique os objetos no bucket:"
echo "  oci os object list --bucket-name $BUCKET_NAME --prefix scripts/ --query 'data[].name'"
echo "  oci os object list --bucket-name $BUCKET_NAME --prefix libs/ --query 'data[].name'"
echo ""
echo "Próximo passo:"
echo "  cd ../terraform/scripts && ./apply_phase.sh 4"
