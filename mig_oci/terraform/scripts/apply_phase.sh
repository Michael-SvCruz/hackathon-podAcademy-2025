#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../environments/prod"

PHASE=$1
if [ -z "$PHASE" ]; then
  echo "=== Terraform Apply - Por Fase ==="
  echo ""
  echo "Uso: $0 <fase>"
  echo ""
  echo "Fases disponiveis:"
  echo "  1  IAM (Compartments + Groups + Policies)"
  echo "  2  Network (VCN + Subnets + Gateways)"
  echo "  3  Storage (Buckets Object Storage)"
  echo "  4  Compute (Data Flow Applications)"
  echo "  5  Security (Vault + Keys) - opcional"
  echo ""
  echo "Exemplo: $0 1"
  exit 1
fi

echo "=== Terraform Apply - Fase $PHASE ==="
echo ""

cd "$TERRAFORM_DIR"

# Sempre executar init (idempotente, detecta novos modulos)
echo "Inicializando Terraform (detectando modulos)..."
terraform init -input=false

# Plan primeiro (mostra o que sera criado)
echo "Gerando plano de execucao..."
echo ""
terraform plan

# Confirmar
echo ""
read -p "Deseja aplicar as mudancas da Fase $PHASE? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
  echo "Cancelado pelo usuario."
  exit 0
fi

# Apply
echo ""
echo "Aplicando..."
terraform apply -auto-approve

echo ""
echo "Verificando outputs..."
terraform output

echo ""
echo "====================================="
echo "Fase $PHASE aplicada com sucesso!"
echo "====================================="
echo ""
echo "Proximos passos:"

case $PHASE in
  1)
    echo "1. Valide no Console OCI:"
    echo "   - Identity > Compartments > Ver 'hackathon-2025' + 5 sub-compartments"
    echo "   - Identity > Groups > Ver 3 grupos criados"
    echo "   - Identity > Policies > Ver 4 policies"
    echo ""
    echo "2. Para a proxima fase (Network):"
    echo "   - Descomente o modulo 'network' em main.tf e outputs.tf"
    echo "   - Execute: $0 2"
    ;;
  2)
    echo "1. Valide no Console OCI:"
    echo "   - Networking > Virtual Cloud Networks"
    echo "   - Ver VCN, 3 subnets, 3 gateways"
    echo ""
    echo "2. Para a proxima fase (Storage):"
    echo "   - Descomente o modulo 'storage' em main.tf e outputs.tf"
    echo "   - Execute: $0 3"
    ;;
  3)
    echo "1. Valide no Console OCI:"
    echo "   - Storage > Buckets > Ver 6 buckets"
    echo ""
    echo "2. Migrar state para remoto (opcional):"
    echo "   - Execute: ./migrate_state_to_remote.sh"
    echo ""
    echo "3. Para a proxima fase (Compute):"
    echo "   - Descomente o modulo 'compute' em main.tf e outputs.tf"
    echo "   - Execute: $0 4"
    ;;
  4)
    echo "1. Valide no Console OCI:"
    echo "   - Analytics > Data Flow > Applications"
    echo "   - Ver 4 applications criadas"
    echo ""
    echo "2. Upload de dados e scripts:"
    echo "   - Execute: cd ../../data_upload && ./upload_landing.sh"
    ;;
  5)
    echo "1. Valide no Console OCI:"
    echo "   - Security > Vault > Ver vault e master key"
    ;;
  *)
    echo "Verifique o Console OCI para validar os recursos criados."
    ;;
esac
