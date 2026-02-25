#!/bin/bash
# ============================================================
# setup_vm.sh — Setup completo do Airflow na VM OCI
# ============================================================
# Automatiza toda a configuração do Airflow via Docker Compose.
# Executar como usuário opc na VM OCI (Oracle Linux 8).
#
# Pré-requisitos (provisionados pelo cloud-init):
#   - Docker CE + Docker Compose plugin instalados
#   - Diretório /opt/airflow-fpd/ criado com permissões do opc
#
# Uso:
#   # No WSL (local), copiar arquivos para a VM:
#   ./deploy_to_vm.sh
#
#   # Na VM (via SSH):
#   cd /opt/airflow-fpd
#   chmod +x setup_vm.sh
#   ./setup_vm.sh
# ============================================================

set -euo pipefail

AIRFLOW_DIR="/opt/airflow-fpd"
COMPOSE_FILE="$AIRFLOW_DIR/docker-compose.yml"
VARIABLES_FILE="$AIRFLOW_DIR/config/airflow_variables_filled.json"
DAG_FILE="$AIRFLOW_DIR/dags/dag_pipeline_fpd.py"

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✔${NC} $1"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }
fail() { echo -e "  ${RED}✘${NC} $1"; exit 1; }

echo ""
echo "========================================"
echo "  Airflow Setup — Hackathon FPD Pipeline"
echo "========================================"
echo ""

# ---- Passo 1: Verificar pré-requisitos ----
echo "--- [1/6] Verificando pré-requisitos ---"

command -v docker &>/dev/null || fail "Docker não instalado. Execute cloud-init ou instale manualmente."
ok "Docker: $(docker --version | cut -d' ' -f3 | tr -d ',')"

docker compose version &>/dev/null || fail "Docker Compose plugin não instalado."
ok "Docker Compose: $(docker compose version --short)"

[ -f "$COMPOSE_FILE" ] || fail "docker-compose.yml não encontrado em $AIRFLOW_DIR/"
ok "docker-compose.yml encontrado"

[ -f "$DAG_FILE" ] || warn "DAG não encontrada em $DAG_FILE (copie antes de ativar)"
[ -f "$DAG_FILE" ] && ok "DAG encontrada: dag_pipeline_fpd.py"

[ -f "$VARIABLES_FILE" ] || warn "airflow_variables_filled.json não encontrado (importe manualmente depois)"
[ -f "$VARIABLES_FILE" ] && ok "Variables JSON encontrado: $(wc -l < "$VARIABLES_FILE") linhas"

# ---- Passo 2: Estrutura de diretórios ----
echo ""
echo "--- [2/6] Verificando estrutura de diretórios ---"

for dir in dags plugins config logs; do
    mkdir -p "$AIRFLOW_DIR/$dir"
done
ok "Diretórios: dags/ plugins/ config/ logs/"

# .env com AIRFLOW_UID
if ! grep -q "AIRFLOW_UID" "$AIRFLOW_DIR/.env" 2>/dev/null; then
    echo "AIRFLOW_UID=$(id -u)" > "$AIRFLOW_DIR/.env"
fi
ok "AIRFLOW_UID=$(grep AIRFLOW_UID "$AIRFLOW_DIR/.env" | cut -d= -f2)"

# ---- Passo 3: Subir PostgreSQL ----
echo ""
echo "--- [3/6] Subindo PostgreSQL ---"
cd "$AIRFLOW_DIR"

docker compose up -d postgres
echo "  Aguardando PostgreSQL ficar healthy..."
for i in $(seq 1 30); do
    if docker compose exec postgres pg_isready -U airflow &>/dev/null; then
        ok "PostgreSQL pronto"
        break
    fi
    [ "$i" -eq 30 ] && fail "PostgreSQL não ficou pronto em 30s"
    sleep 1
done

# ---- Passo 4: Migrar banco + criar usuário ----
echo ""
echo "--- [4/6] Migrando banco de dados ---"

docker compose run --rm airflow-scheduler airflow db migrate
ok "Banco migrado"

docker compose run --rm airflow-scheduler \
    airflow users create \
        --username airflow --firstname Airflow --lastname Admin \
        --role Admin --email admin@hackathon.local --password airflow \
    2>/dev/null || true
ok "Usuário admin criado"

# ---- Passo 5: Subir webserver + scheduler ----
echo ""
echo "--- [5/6] Subindo Airflow (webserver + scheduler) ---"

docker compose up -d
echo "  Aguardando containers ficarem saudáveis..."

for i in $(seq 1 60); do
    WS_STATUS=$(docker compose ps airflow-webserver --format json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || echo "")
    if [ "$WS_STATUS" = "healthy" ]; then
        ok "Webserver healthy"
        break
    fi
    [ "$i" -eq 60 ] && warn "Webserver não ficou healthy em 60s (pode precisar de mais tempo)"
    sleep 2
done

docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# ---- Passo 6: Importar variáveis + ativar DAG ----
echo ""
echo "--- [6/6] Configurando variáveis e DAG ---"

if [ -f "$VARIABLES_FILE" ]; then
    docker compose exec -T airflow-scheduler \
        airflow variables import /opt/airflow/config/airflow_variables_filled.json
    COUNT=$(docker compose exec -T airflow-scheduler airflow variables list 2>/dev/null | grep -c "oci_" || echo "0")
    ok "Variáveis importadas: $COUNT de 22"
else
    warn "Pule: airflow_variables_filled.json não encontrado"
fi

# Aguardar scheduler parsear DAGs
echo "  Aguardando scheduler parsear DAGs (até 90s)..."
for i in $(seq 1 30); do
    DAG_EXISTS=$(docker compose exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -c "pipeline_credit_risk_fpd" || echo "0")
    if [ "$DAG_EXISTS" -gt 0 ]; then
        docker compose exec -T airflow-scheduler \
            airflow dags unpause pipeline_credit_risk_fpd &>/dev/null
        ok "DAG pipeline_credit_risk_fpd ativada"
        break
    fi
    [ "$i" -eq 30 ] && warn "DAG não detectada em 90s. Verifique: docker compose logs airflow-scheduler"
    sleep 3
done

# ---- Resumo final ----
VM_IP=$(curl -s --connect-timeout 3 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

echo ""
echo "========================================"
echo -e "  ${GREEN}Setup concluído!${NC}"
echo "========================================"
echo ""
echo "  UI Airflow:  http://${VM_IP}:8080"
echo "  Usuário:     airflow"
echo "  Senha:       airflow"
echo ""
echo "  Comandos úteis:"
echo "    docker compose ps                          # status"
echo "    docker compose logs -f airflow-scheduler   # logs scheduler"
echo "    docker compose exec airflow-scheduler \\    "
echo "      airflow dags trigger pipeline_credit_risk_fpd  # trigger manual"
echo "    docker compose down                        # parar tudo"
echo ""
