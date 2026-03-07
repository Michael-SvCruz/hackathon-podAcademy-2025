# Guia de Reprodução — Migração OCI

> **Objetivo:** Permitir que qualquer membro da equipe (ou avaliador da banca) reproduza o projeto completo a partir do repositório Git, em uma nova tenancy OCI ou na mesma.

---

## Sumário

1. [Visão Geral da Reprodução](#1-visão-geral-da-reprodução)
2. [Pré-requisitos](#2-pré-requisitos)
3. [Passo a Passo Completo](#3-passo-a-passo-completo)
   - [Etapa 1: Clonar o Repositório](#etapa-1-clonar-o-repositório)
   - [Etapa 2: Configurar Credenciais OCI](#etapa-2-configurar-credenciais-oci)
   - [Etapa 3: Gerar Chaves SSH](#etapa-3-gerar-chaves-ssh)
   - [Etapa 4: Provisionar Infraestrutura (Terraform)](#etapa-4-provisionar-infraestrutura-terraform)
   - [Etapa 5: Upload dos Dados Brutos (Landing Zone)](#etapa-5-upload-dos-dados-brutos-landing-zone)
   - [Etapa 6: Upload dos Scripts PySpark](#etapa-6-upload-dos-scripts-pyspark)
   - [Etapa 7: Configurar Airflow](#etapa-7-configurar-airflow)
   - [Etapa 8: Deploy do Modelo de Scoring](#etapa-8-deploy-do-modelo-de-scoring)
   - [Etapa 9: Executar Pipeline Completo](#etapa-9-executar-pipeline-completo)
   - [Etapa 10: Validar Resultados](#etapa-10-validar-resultados)
4. [Diagrama de Reprodução](#4-diagrama-de-reprodução)
5. [Checklist de Validação](#5-checklist-de-validação)
6. [Gaps Conhecidos e Limitações](#6-gaps-conhecidos-e-limitações)
7. [Troubleshooting](#7-troubleshooting)
8. [Tempo Estimado](#8-tempo-estimado)

---

## 1. Visão Geral da Reprodução

O projeto é reprodutível via **Infrastructure as Code (Terraform)** + **scripts automatizados** + **orquestração Airflow**. O fluxo completo:

```
                       LOCAL (máquina do desenvolvedor)
┌──────────────────────────────────────────────────────────────────────┐
│  1. git clone                                                        │
│  2. terraform.tfvars (credenciais)                                   │
│  3. ssh-keygen (2 pares de chaves)                                   │
│  4. terraform init → plan → apply     (provisiona tudo na OCI)       │
│  5. upload_landing.sh                  (dados brutos → landing-zone) │
│  6. upload_scripts.sh                  (PySpark → pipeline-ops)      │
│  7. populate_variables.sh              (OCIDs → Airflow JSON)        │
│  8. deploy_to_vm.sh                   (Airflow → VM pública)        │
│  9. deploy_modelo.sh                  (scoring → VM privada)        │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                       OCI (Oracle Cloud)
┌──────────────────────────────────────────────────────────────────────┐
│  Airflow UI (porta 8080) → Trigger DAG pipeline_fpd                  │
│    └→ 21 Data Flow runs (Bronze → Silver → Gold → ABT v1-v6)        │
│    └→ TriggerDagRunOperator → DAG pipeline_modelo_qualificacao       │
│         └→ VM Start → SSH scoring → VM Stop                          │
│                                                                      │
│  Resultado: KS OOT ≈ 34% (+1.29 p.p. acima do benchmark)           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Pré-requisitos

### 2.1 Ferramentas (máquina local)

| Ferramenta | Versão Mínima | Instalação | Verificar |
|------------|---------------|------------|-----------|
| **Terraform** | >= 1.5.0 | [terraform.io/downloads](https://www.terraform.io/downloads) | `terraform version` |
| **OCI CLI** | >= 3.0 | `pip install oci-cli` ou [docs.oracle.com](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) | `oci --version` |
| **jq** | >= 1.6 | `sudo apt install jq` (Linux) / `brew install jq` (Mac) | `jq --version` |
| **SSH** | qualquer | Pré-instalado no Linux/Mac/WSL | `ssh -V` |
| **Git** | qualquer | Pré-instalado | `git --version` |
| **zip** | qualquer | `sudo apt install zip` | `zip --version` |

### 2.2 Conta OCI

| Requisito | Onde Obter |
|-----------|------------|
| **Tenancy** com compartment raiz | Console OCI > Governance > Tenancy Details |
| **Usuário** com permissões de Admin (ou policy equivalente) | Console OCI > Identity > Users |
| **API Key** (.pem) gerada para o usuário | Console OCI > Identity > Users > API Keys > Add API Key |
| **Fingerprint** da API Key | Exibido ao criar a API Key |
| **Região** | `sa-saopaulo-1` (São Paulo) — ou outra se necessário |

### 2.3 Dados Brutos

Os 6 datasets de origem (proprietários da Claro) devem estar disponíveis localmente:

| Fonte | Formato | Tamanho Aproximado |
|-------|---------|-------------------|
| `bureau` | Parquet | ~2 GB |
| `telco` | Parquet | ~5 GB |
| `cadastro` | Parquet | ~1 GB |
| `recarga` | Parquet | ~8 GB |
| `pagamento` | Parquet | ~3 GB |
| `atraso` | Parquet | ~2 GB |

> **Nota:** Os dados brutos não estão no repositório por serem confidenciais. Devem ser fornecidos separadamente pela equipe.

---

## 3. Passo a Passo Completo

### Etapa 1: Clonar o Repositório

**Executar em: Local**

```bash
git clone <URL_DO_REPOSITORIO>
cd hackathon-podAcademy-2025
```

### Etapa 2: Configurar Credenciais OCI

**Executar em: Local**

```bash
# 2.1 Configurar OCI CLI
oci setup config
# Informar: tenancy OCID, user OCID, região, caminho da chave .pem

# 2.2 Testar conectividade
oci os ns get
# Deve retornar o namespace (ex: "grfjr7baehkj")

# 2.3 Configurar Terraform
cd mig_oci/terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars
```

Editar `terraform.tfvars` com os valores reais:

```hcl
# Obter no Console OCI > Governance > Tenancy Details
tenancy_ocid     = "ocid1.tenancy.oc1..aaaaaaa..."

# Obter no Console OCI > Identity > Users > seu usuário
user_ocid        = "ocid1.user.oc1..aaaaaaa..."

# Exibido ao criar a API Key
fingerprint      = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"

# Caminho absoluto para a chave .pem
private_key_path = "/home/<usuario>/.oci/oci_api_key.pem"

# Região OCI
region           = "sa-saopaulo-1"

# SSH keys (geradas na Etapa 3)
ssh_public_key_airflow = "ssh-rsa AAAAB3..."
ssh_public_key_modelo  = "ssh-rsa AAAAB3..."
```

### Etapa 3: Gerar Chaves SSH

**Executar em: Local**

São necessários **dois pares** de chaves SSH — um para cada VM:

```bash
# Par 1: VM Airflow (subnet pública — acesso direto)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/airflow_vm -N "" -C "airflow-vm"

# Par 2: VM Modelo (subnet privada — acesso via jump host)
ssh-keygen -t rsa -b 4096 -f ~/.ssh/modelo_vm -N "" -C "modelo-vm"
```

Copiar as chaves **públicas** para o `terraform.tfvars`:

```bash
# Exibir chave pública do Airflow
cat ~/.ssh/airflow_vm.pub
# Copiar o conteúdo para ssh_public_key_airflow em terraform.tfvars

# Exibir chave pública do Modelo
cat ~/.ssh/modelo_vm.pub
# Copiar o conteúdo para ssh_public_key_modelo em terraform.tfvars
```

### Etapa 4: Provisionar Infraestrutura (Terraform)

**Executar em: Local**

O Terraform cria **toda a infraestrutura** em um único apply:
- IAM (6 compartments, 3 grupos, 4 políticas)
- Network (VCN, 3 subnets, 3 gateways, route tables, security lists)
- Storage (7 buckets Object Storage)
- Compute (21 Data Flow Applications com per-app shapes)
- Airflow VM (E3.Flex, cloud-init com Docker)
- Modelo VM (E5.Flex, cloud-init com Python + LightGBM)

```bash
cd mig_oci/terraform/scripts

# 4.1 Inicializar (valida credenciais e chave .pem)
./init.sh

# 4.2 Aplicar toda a infraestrutura
./apply_phase.sh 4
# Revisar o plan e confirmar com "yes"
# Tempo: ~5-10 minutos

# 4.3 Verificar outputs (anotar IPs e OCIDs)
cd ../environments/prod
terraform output
```

**Outputs importantes:**
- `airflow_info.public_ip` — IP da VM Airflow (para SSH e UI)
- `modelo_vm_info.private_ip` — IP da VM Modelo (acesso via Airflow)
- `dataflow_applications` — OCIDs das 21 apps Data Flow

> **Aguardar ~5 minutos** após o apply para as VMs concluírem o cloud-init (instalar Docker, Python, etc.).

### Etapa 5: Upload dos Dados Brutos (Landing Zone)

**Executar em: Local**

Os dados brutos devem ser enviados para o bucket `landing-zone` seguindo a estrutura esperada pelos scripts Bronze:

```
hackathon-2025-landing-zone/
└── source/
    ├── bureau/       ← arquivos .parquet
    ├── telco/        ← arquivos .parquet
    ├── cadastro/     ← arquivos .parquet
    ├── recarga/      ← arquivos .parquet
    ├── pagamento/    ← arquivos .parquet
    └── atraso/       ← arquivos .parquet
```

Upload via OCI CLI:

```bash
# Obter namespace
NAMESPACE=$(oci os ns get --query 'data' --raw-output)
BUCKET="hackathon-2025-landing-zone"

# Upload de cada fonte (substituir <caminho_local> pelo diretório dos dados)
for FONTE in bureau telco cadastro recarga pagamento atraso; do
    echo "=== Upload $FONTE ==="
    oci os object bulk-upload \
        --bucket-name "$BUCKET" \
        --src-dir "<caminho_local>/$FONTE/" \
        --object-prefix "source/$FONTE/" \
        --overwrite \
        --content-type "application/octet-stream"
done

# Verificar upload
for FONTE in bureau telco cadastro recarga pagamento atraso; do
    echo "--- $FONTE ---"
    oci os object list \
        --bucket-name "$BUCKET" \
        --prefix "source/$FONTE/" \
        --query 'data[].{name:name, size:size}' \
        --output table
done
```

> **Tempo:** Depende da banda e volume — pode levar 30-60 minutos para ~20 GB total.

### Etapa 6: Upload dos Scripts PySpark

**Executar em: Local**

```bash
cd mig_oci/data_upload

# Upload dos 21 scripts + utils.zip para o bucket pipeline-ops
./upload_scripts.sh

# Verificar
oci os object list \
    --bucket-name "hackathon-2025-pipeline-ops" \
    --prefix "scripts/" \
    --query 'data[].name'
```

Este script:
1. Empacota `spark_utils.py` + `validate_abt.py` em `libs/utils.zip` (estrutura `python/lib/`)
2. Faz upload dos 21 scripts `.py` para `pipeline-ops/scripts/`
3. Faz upload do `utils.zip` para `pipeline-ops/libs/`

### Etapa 7: Configurar Airflow

**Executar em: Local**

```bash
cd mig_oci/airflow

# 7.1 Gerar airflow_variables_filled.json a partir do Terraform
cd config
./populate_variables.sh
cd ..

# 7.2 Deploy do Airflow na VM (copia DAGs + docker-compose + setup)
AIRFLOW_IP=$(cd ../terraform/environments/prod && terraform output -json airflow_info | jq -r '.public_ip')
echo "Airflow VM IP: $AIRFLOW_IP"

./deploy_to_vm.sh "$AIRFLOW_IP" ~/.ssh/airflow_vm
```

O `deploy_to_vm.sh` executa automaticamente:
1. Copia `docker-compose.yml`, `setup_vm.sh`, 3 DAGs, `airflow_variables_filled.json`
2. Na VM: inicia PostgreSQL → `airflow db migrate` → cria admin → sobe webserver + scheduler
3. Importa variáveis → unpause das DAGs

**Verificar:**
```bash
# Acessar a UI do Airflow
echo "http://$AIRFLOW_IP:8080"
# Login: airflow / airflow
```

### Etapa 8: Deploy do Modelo de Scoring

**Executar em: Local**

O script do modelo (`modelo_qualificacao.py`) deve ser copiado para a VM Modelo via jump host (Airflow VM), pois a VM Modelo está em subnet privada sem IP público.

```bash
cd mig_oci/airflow

AIRFLOW_IP=$(cd ../terraform/environments/prod && terraform output -json airflow_info | jq -r '.public_ip')
MODELO_IP=$(cd ../terraform/environments/prod && terraform output -json modelo_vm_info | jq -r '.private_ip')

./deploy_modelo.sh "$AIRFLOW_IP" "$MODELO_IP" ~/.ssh/airflow_vm ~/.ssh/modelo_vm
```

> **Nota:** A VM Modelo fica STOPPED por padrão. O script `deploy_modelo.sh` precisa que a VM esteja RUNNING. Se necessário, iniciar manualmente:
> ```bash
> MODELO_VM_ID=$(cd ../terraform/environments/prod && terraform output -json modelo_vm_info | jq -r '.vm_id')
> oci compute instance action --instance-id "$MODELO_VM_ID" --action START
> # Aguardar ~60 segundos até RUNNING
> ```

### Etapa 9: Executar Pipeline Completo

**Executar em: Airflow UI (navegador)**

1. Acessar `http://<AIRFLOW_IP>:8080` (login: `airflow` / `airflow`)
2. Localizar a DAG `pipeline_fpd`
3. Clicar em **Trigger DAG** (botão play)
4. Acompanhar execução no Graph View

**O que acontece automaticamente:**
```
DAG pipeline_fpd (21 tarefas)
├── Grupo Bronze (6 apps em paralelo)    → ~10 min
├── Grupo Silver (6 apps em paralelo)    → ~15 min
├── Grupo Gold Features (3 apps em paralelo) → ~10 min
├── ABT v1 → v2 → v3 → v4 → v5 → v6 (sequencial) → ~40 min
└── TriggerDagRunOperator → DAG pipeline_modelo_qualificacao
    ├── start_modelo_vm        → Liga a VM
    ├── run_scoring_ssh        → Executa modelo via SSH
    └── stop_modelo_vm         → Desliga a VM
```

**Tempo total estimado:** ~90-120 minutos (primeira execução)

> **Alternativa (quota limitada):** Usar a DAG `pipeline_fpd_sequential` que executa 1 app por vez.

### Etapa 10: Validar Resultados

**Executar em: Airflow UI + OCI Console**

#### 10.1 Verificar Logs do Modelo

Na Airflow UI → DAG `pipeline_modelo_qualificacao` → Task `run_scoring_ssh` → Logs

Procurar por:
```
>>> [Resultado] KS OOT: 34.XX%
>>> [Resultado] AUC OOT: 0.73XX
>>> [Resultado] GINI OOT: 46.XX%
```

#### 10.2 Verificar Artefatos no Object Storage

**Executar em: Local**

```bash
BUCKET="hackathon-2025-models"

# Modelo treinado (PKL)
oci os object list --bucket-name "$BUCKET" --prefix "pkl/" --query 'data[].{name:name, size:size}' --output table

# Predições OOT
oci os object list --bucket-name "$BUCKET" --prefix "resultados_modelo/" --query 'data[].{name:name, size:size}' --output table

# Métricas
oci os object list --bucket-name "$BUCKET" --prefix "metricas/" --query 'data[].{name:name, size:size}' --output table
```

---

## 4. Diagrama de Reprodução

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FLUXO DE REPRODUÇÃO                              │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌───────────┐    ┌──────────────────┐ │
│  │ git clone│───▶│terraform │───▶│upload data│───▶│upload scripts    │ │
│  │          │    │  apply   │    │(landing)  │    │(pipeline-ops)    │ │
│  └──────────┘    └──────────┘    └───────────┘    └──────────────────┘ │
│       │               │                                    │           │
│       │               ▼                                    ▼           │
│       │         ┌───────────┐                     ┌──────────────┐    │
│       │         │ populate  │────────────────────▶│ deploy_to_vm │    │
│       │         │ variables │                     │  (Airflow)   │    │
│       │         └───────────┘                     └──────────────┘    │
│       │                                                    │           │
│       │                                                    ▼           │
│       │                                           ┌──────────────┐    │
│       └──────────────────────────────────────────▶│deploy_modelo │    │
│                                                   │(via jump)    │    │
│                                                   └──────────────┘    │
│                                                            │           │
│                                                            ▼           │
│                                                   ┌──────────────┐    │
│                                                   │ Trigger DAG  │    │
│                                                   │ (Airflow UI) │    │
│                                                   └──────────────┘    │
│                                                            │           │
│                                                            ▼           │
│                                                   ┌──────────────┐    │
│                                                   │  KS ≈ 34%    │    │
│                                                   │  Validado!   │    │
│                                                   └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Checklist de Validação

### Infraestrutura

- [ ] `terraform output` retorna todos os OCIDs sem erro
- [ ] Console OCI > Compartments: 6 sub-compartments visíveis
- [ ] Console OCI > VCN: VCN + 3 subnets + gateways
- [ ] Console OCI > Buckets: 7 buckets criados
- [ ] Console OCI > Data Flow > Applications: 21 apps
- [ ] VM Airflow acessível via SSH (`ssh -i ~/.ssh/airflow_vm opc@<AIRFLOW_IP>`)
- [ ] VM Modelo acessível via jump host (ver Etapa 8)

### Dados

- [ ] Landing zone: 6 prefixos (`source/bureau/`, `source/telco/`, etc.) com dados
- [ ] Pipeline-ops: 21 scripts + `libs/utils.zip`

### Airflow

- [ ] UI acessível em `http://<AIRFLOW_IP>:8080`
- [ ] 3 DAGs visíveis: `pipeline_fpd`, `pipeline_fpd_sequential`, `pipeline_modelo_qualificacao`
- [ ] Variables importadas: `oci_app_id_*`, `oci_modelo_vm_id`, `oci_modelo_vm_ip`
- [ ] DAGs unpaused (toggle ativo)

### Pipeline

- [ ] DAG `pipeline_fpd` executa sem falha (todas as tasks verdes)
- [ ] DAG `pipeline_modelo_qualificacao` executa com sucesso
- [ ] Logs do modelo mostram KS OOT ≈ 34%
- [ ] Bucket `models` contém: `pkl/modelo_fpd.pkl`, `resultados_modelo/`, `metricas/`

### Modelo

- [ ] KS OOT >= 33.1% (benchmark)
- [ ] Métricas JSON contém: KS, AUC, GINI, confusion matrix
- [ ] Predições OOT em `resultados_modelo/predicoes_oot_*.parquet`

---

## 6. Gaps Conhecidos e Limitações

### 6.1 Dados Brutos Não Incluídos no Repositório

Os dados da Claro são **confidenciais** e não estão no Git. Para reproduzir, é necessário obter os dados brutos e fazer upload manual para a landing zone (Etapa 5).

**Impacto:** Sem os dados, o pipeline não roda. Porém, toda a infraestrutura pode ser provisionada e validada independentemente.

### 6.2 Bucket Names Hardcoded nos Scripts

Os 21 scripts PySpark e o script do modelo usam o prefixo `hackathon-2025-` nos nomes de bucket. Se o `project_name` no Terraform for alterado, os scripts **não acompanham automaticamente**.

**Impacto:** Nenhum, desde que `project_name = "hackathon-2025"` seja mantido no `terraform.tfvars`.

### 6.3 Credenciais Default do Airflow

O Airflow usa `airflow`/`airflow` como admin e `SECRET_KEY: 'hackathon-2025-fpd'`. Aceitável para hackathon com VMs temporárias (30 dias), mas não para produção.

---

## 7. Troubleshooting

| Problema | Causa Provável | Solução |
|----------|---------------|---------|
| `terraform init` falha | Chave .pem não encontrada | Verificar `private_key_path` no `terraform.tfvars` |
| `terraform apply` erro de quota | Free tier limitado | Usar DAG sequencial (`pipeline_fpd_sequential`) |
| SSH timeout na VM Airflow | Security list ou VM não pronta | Aguardar 5 min após apply; verificar porta 22 na security list |
| SSH timeout na VM Modelo | VM está STOPPED | Iniciar via `oci compute instance action --action START` |
| `upload_scripts.sh` falha | OCI CLI não configurado | Executar `oci setup config` |
| Data Flow run falha com X509 | Dados não existem no bucket | Verificar upload da landing zone (Etapa 5) |
| Airflow Variables vazias | `populate_variables.sh` não executado | Executar antes do `deploy_to_vm.sh` |
| OOM no modelo scoring | Parquets duplicados (Delta orphans) | VACUUM automático no ABT v6; dedup incremental como fallback |
| `\r': command not found` | CRLF em scripts editados no Windows | `sed -i 's/\r$//' <arquivo>` |
| Terraform `kmsKeyId` erro | Imagem Oracle Linux atualizada | `lifecycle { ignore_changes = [source_details] }` (já implementado) |

**Documentação detalhada:** `mig_oci/docs/FASE_8_TROUBLESHOOTING.md` (13 problemas documentados com soluções).

---

## 8. Tempo Estimado

| Etapa | Tempo | Notas |
|-------|-------|-------|
| Clonar + configurar credenciais | 15 min | Requer OCI CLI e API Key prontos |
| Terraform apply | 10 min | Provisiona toda infraestrutura |
| Aguardar cloud-init das VMs | 5 min | Docker, Python, LightGBM |
| Upload dados brutos | 30-60 min | Depende da banda (~20 GB) |
| Upload scripts PySpark | 2 min | 21 scripts + utils.zip |
| Deploy Airflow + modelo | 5 min | Scripts automatizados |
| Pipeline ETL completo | 60-90 min | 21 Data Flow runs |
| Modelo scoring | 10-15 min | VM Start → scoring → VM Stop |
| **Total** | **~2.5-3.5 horas** | Da primeira vez (inclui upload de dados) |

> **Re-execução** (dados já no landing): ~75-110 min (apenas pipeline ETL + modelo)

---

## Referências

| Documento | Descrição |
|-----------|-----------|
| `mig_oci/README.md` | Visão geral da migração com status de cada fase |
| `mig_oci/docs/FASE_0_1_IMPLEMENTACAO.md` | Setup inicial, IAM, conceitos OCI |
| `mig_oci/docs/FASE_2_3_IMPLEMENTACAO.md` | Network, Storage, lições aprendidas |
| `mig_oci/docs/FASE_4_IMPLEMENTACAO.md` | Compute, Data Flow, upload scripts |
| `mig_oci/docs/FASE_6A_LANDING_BRONZE.md` | Pipeline Landing → Bronze (as-built) |
| `mig_oci/docs/FASE_6A_TROUBLESHOOTING.md` | Lições aprendidas (problemas/soluções) |
| `mig_oci/docs/FASE_6B_SILVER.md` | Silver Layer (6 scripts, dedup, calibração) |
| `mig_oci/docs/FASE_6B_GOLD_FEATURES.md` | Gold Features (3 scripts, 2 actions) |
| `mig_oci/docs/FASE_7_DATA_SCIENCE_NOTEBOOK.md` | Data Science notebook, IAM granular |
| `mig_oci/docs/FASE_8_MODELO_VM_IMPLEMENTACAO.md` | Modelo scoring em VM dedicada |
| `mig_oci/docs/FASE_8_TROUBLESHOOTING.md` | Troubleshooting completo (13 problemas) |
| `mig_oci/docs/MODELO_ARTEFATOS_GUIDE.md` | Artefatos do modelo (PKL, predições, métricas) |
| `mig_oci/airflow/README.md` | Guia completo Airflow (deploy, checklist) |
| `mig_oci/terraform/environments/prod/terraform.tfvars.example` | Template de credenciais |
