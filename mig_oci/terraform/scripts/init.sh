#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../environments/prod"

echo "=== Terraform Init para OCI ==="
echo "Diretório: $TERRAFORM_DIR"
echo ""

# Verificar se terraform.tfvars existe
if [ ! -f "$TERRAFORM_DIR/terraform.tfvars" ]; then
  echo "❌ ERRO: terraform.tfvars não encontrado!"
  echo ""
  echo "Ação necessária:"
  echo "1. Copie o template: cp terraform.tfvars.example terraform.tfvars"
  echo "2. Edite terraform.tfvars com seus valores OCI:"
  echo "   - tenancy_ocid (Console OCI > Tenancy Details)"
  echo "   - user_ocid (Console OCI > Identity > Users)"
  echo "   - fingerprint (Console OCI > Identity > Users > API Keys)"
  echo "   - private_key_path (caminho para .pem gerado com API Key)"
  echo ""
  exit 1
fi

# Verificar se chave privada existe
PRIVATE_KEY=$(grep private_key_path "$TERRAFORM_DIR/terraform.tfvars" | cut -d'"' -f2)
PRIVATE_KEY_EXPANDED="${PRIVATE_KEY/#\~/$HOME}"

if [ ! -f "$PRIVATE_KEY_EXPANDED" ]; then
  echo "❌ ERRO: Chave privada OCI não encontrada: $PRIVATE_KEY_EXPANDED"
  echo ""
  echo "Ação necessária:"
  echo "1. Gere uma API Key no Console OCI:"
  echo "   Identity > Users > seu usuário > API Keys > Add API Key"
  echo "2. Baixe a chave privada (.pem)"
  echo "3. Salve em: $PRIVATE_KEY_EXPANDED"
  echo "4. Defina permissões: chmod 600 $PRIVATE_KEY_EXPANDED"
  echo ""
  exit 1
fi

# Verificar permissões da chave privada (deve ser 600)
KEY_PERMS=$(stat -c "%a" "$PRIVATE_KEY_EXPANDED" 2>/dev/null || stat -f "%OLp" "$PRIVATE_KEY_EXPANDED" 2>/dev/null)
if [ "$KEY_PERMS" != "600" ]; then
  echo "⚠️  AVISO: Permissões da chave privada não são 600 (atual: $KEY_PERMS)"
  echo "Corrigindo para 600..."
  chmod 600 "$PRIVATE_KEY_EXPANDED"
fi

# Init
echo "Executando terraform init..."
cd "$TERRAFORM_DIR"
terraform init

# Validar
echo ""
echo "Validando configuração..."
terraform validate

echo ""
echo "✅ Terraform inicializado com sucesso!"
echo ""
echo "Próximos passos:"
echo "1. Revisar plano: cd $TERRAFORM_DIR && terraform plan"
echo "2. Aplicar Fase 1 (IAM): cd ../../scripts && ./apply_phase.sh 1"
echo ""
