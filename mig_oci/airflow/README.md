# Orquestração Airflow — Pipeline FPD

Pasta de configuração do Apache Airflow responsável por orquestrar o pipeline de risco de crédito FPD no OCI Data Flow.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Estrutura de Arquivos](#2-estrutura-de-arquivos)
3. [Pré-requisitos](#3-pré-requisitos)
4. [Deploy Automatizado (Recomendado)](#4-deploy-automatizado-recomendado)
   - 4.1 [Checklist Pré-Deploy](#41-checklist-pré-deploy)
   - 4.2 [deploy_to_vm.sh — O que faz](#42-deploy_to_vmsh--o-que-faz)
   - 4.3 [setup_vm.sh — O que faz](#43-setup_vmsh--o-que-faz)
   - 4.4 [Executando o Deploy](#44-executando-o-deploy)
   - 4.5 [Verificação Pós-Deploy](#45-verificação-pós-deploy)
5. [Deploy Manual (Alternativo)](#5-deploy-manual-alternativo)
6. [Variáveis Airflow](#6-variáveis-airflow)
   - 6.1 [Gerar variáveis do Terraform](#61-gerar-variáveis-do-terraform)
   - 6.2 [Tabela de variáveis](#62-tabela-de-variáveis)
7. [Autenticação OCI](#7-autenticação-oci)
8. [Execução e Monitoramento](#8-execução-e-monitoramento)
9. [Troubleshooting](#9-troubleshooting)
10. [Referências](#10-referências)

---

## 1. Visão Geral

O Airflow atua como **maestro** do pipeline: dispara as 21 aplicações OCI Data Flow na ordem correta, aguarda a conclusão de cada etapa e encadeia as dependências automaticamente, **incluindo o disparo do modelo de scoring ao final**.

```
Bronze (6 apps, paralelo)
  └── bureau, telco, cadastro, recarga, pagamento, atraso

Silver (6 apps, paralelo)
  └── bureau, telco, cadastro, recarga, pagamento, atraso

Gold Features (3 apps, paralelo)
  └── recarga, pagamento, atraso

ABT (6 apps, sequencial)
  └── v1 → v2 → v3 → v4 → v5 → v6 (~614 features, 3.79M registros)

Modelo Scoring (TriggerDagRunOperator)
  └── pipeline_modelo_qualificacao: VM Start → SSH scoring → VM Stop
```

**Agendamento:** Todo dia 1 do mês às 02:00 (UTC-3 / Horário de Brasília).

**Infraestrutura:** Apache Airflow 2.8.0 via Docker Compose (LocalExecutor) em VM OCI.

### Arquitetura de Encadeamento (ETL → Modelo)

O pipeline utiliza **duas DAGs separadas** conectadas via `TriggerDagRunOperator`:

| DAG | Responsabilidade | Schedule |
|-----|------------------|----------|
| `pipeline_credit_risk_fpd` | ETL completo (Bronze→ABT) + trigger do modelo | `0 2 1 * *` (mensal) |
| `pipeline_modelo_qualificacao` | Scoring (VM Start→SSH→Stop) | `None` (trigger automático ou manual) |

**Por que duas DAGs em vez de uma?**

1. **Isolamento de logs e retries:** Cada DAG tem seus próprios logs, contadores de retry e SLAs. Um problema no modelo não polui o histórico do ETL e vice-versa.
2. **Re-scoring independente:** O modelo pode ser re-executado manualmente (via UI ou CLI) sem re-processar os dados. Útil para ajuste de hiperparâmetros ou atualização do `.pkl`.
3. **Re-ETL independente:** O pipeline de dados pode ser re-executado sem disparar o modelo (basta desativar o trigger ou usar a DAG sequencial).
4. **Monitoramento granular:** Na UI do Airflow, cada DAG aparece como uma linha separada no Grid View, facilitando a identificação de gargalos.
5. **Separação de responsabilidades:** Engenharia de dados (ETL) e Ciência de dados (Modelo) são domínios distintos com equipes e ciclos de vida diferentes.

O `TriggerDagRunOperator` garante que a DAG do modelo **só é disparada se todo o ETL completar com sucesso** (`wait_for_completion=True`, `allowed_states=["success"]`).

---

## 2. Estrutura de Arquivos

```
mig_oci/airflow/
├── deploy_to_vm.sh                    # [LOCAL] Copia arquivos + executa setup na VM
├── deploy_modelo.sh                   # [LOCAL] Deploy script scoring (via jump host)
├── dags/
│   ├── dag_pipeline_fpd.py            # DAG principal: ETL completo + trigger modelo
│   ├── dag_pipeline_fpd_sequential.py # DAG alternativa: 1 app por vez (teste quota)
│   └── dag_modelo_qualificacao.py     # DAG modelo: VM Start → SSH scoring → VM Stop
├── plugins/
│   └── oci_operators/                 # Reservado para operators customizados OCI
├── config/
│   ├── .gitignore                     # Ignora airflow_variables_filled.json
│   ├── airflow_variables.json         # Template de variáveis (OCIDs placeholder)
│   ├── airflow_variables_filled.json  # Gerado por populate_variables.sh (NÃO comitar)
│   └── populate_variables.sh          # Popula OCIDs do terraform output
├── docker/
│   ├── docker-compose.yml             # Airflow + PostgreSQL (LocalExecutor)
│   └── setup_vm.sh                    # [VM] Setup completo automatizado
├── requirements.txt                   # Dependências Python (oci SDK)
└── README.md                          # Este arquivo
```

---

## 3. Pré-requisitos

### Na máquina local (WSL/Linux/Mac)

| Requisito | Verificação |
|-----------|-------------|
| SSH Key para a VM OCI | `ls ~/.ssh/airflow_vm` |
| Terraform aplicado (Fases 1-4) | `cd mig_oci/terraform/environments/prod && terraform output dataflow_applications` |
| `jq` instalado | `jq --version` |
| Variáveis preenchidas | `cat mig_oci/airflow/config/airflow_variables_filled.json` |

### Na VM OCI (provisionada pelo Terraform módulo `airflow`)

| Requisito | Provisionado por |
|-----------|------------------|
| Docker CE | cloud-init (automático) |
| Docker Compose plugin | cloud-init (automático) |
| Diretório `/opt/airflow-fpd/` | cloud-init (automático) |
| Porta 8080 aberta | Security List no módulo `network` |
| Porta 22 (SSH) aberta | Security List no módulo `network` |

---

## 4. Deploy Automatizado (Recomendado)

O fluxo é composto por **dois scripts** que automatizam todo o processo:

### 4.1 Checklist Pré-Deploy

Antes de executar o `deploy_to_vm.sh`, verifique:

- [ ] **VM OCI criada e acessível via SSH:**
  ```bash
  ssh -i ~/.ssh/airflow_vm opc@<VM_IP> "echo OK"
  ```

- [ ] **cloud-init concluído** (Docker instalado):
  ```bash
  ssh -i ~/.ssh/airflow_vm opc@<VM_IP> "docker --version && docker compose version"
  ```
  > Se o cloud-init ainda estiver executando, aguarde ~3 minutos.
  > Verificar: `ssh opc@<VM_IP> "cat /var/log/airflow-setup.log"`

- [ ] **Terraform Fase 4 aplicada** (21 apps Data Flow criadas):
  ```bash
  cd mig_oci/terraform/environments/prod
  terraform output dataflow_applications | head -5
  ```

- [ ] **Variáveis Airflow preenchidas** (OCIDs reais, não placeholders):
  ```bash
  cat mig_oci/airflow/config/airflow_variables_filled.json | head -5
  # Deve mostrar OCIDs reais (ocid1.dataflowapplication.oc1...)
  ```
  > Se estiver com placeholders, execute primeiro:
  > ```bash
  > cd mig_oci/airflow/config && ./populate_variables.sh
  > ```

- [ ] **Porta 8080 liberada** na Security List da subnet pública:
  ```bash
  # Se não estiver, aplicar via Terraform:
  cd mig_oci/terraform/environments/prod
  terraform apply -target=module.network.oci_core_security_list.public
  ```

### 4.2 deploy_to_vm.sh — O que faz

**Executa localmente** no WSL/Linux/Mac. Responsável por copiar os arquivos e iniciar o setup remoto.

| Passo | Ação | Verificação |
|-------|------|-------------|
| [1/4] | Testa conexão SSH com a VM | Falha se SSH não conectar |
| [2/4] | Cria estrutura de diretórios remota (`dags/`, `plugins/`, `config/`, `logs/`) | `mkdir -p` idempotente |
| [3/4] | Copia 4 arquivos via SCP | docker-compose.yml, setup_vm.sh, DAG, variables JSON |
| [4/4] | Executa `setup_vm.sh` na VM via SSH | Passa o controle para o script remoto |

**Parâmetros:**
```bash
./deploy_to_vm.sh <VM_IP> [SSH_KEY_PATH]

# VM_IP:        IP público da VM (obrigatório)
# SSH_KEY_PATH: Caminho da chave SSH (default: ~/.ssh/airflow_vm)
```

### 4.3 setup_vm.sh — O que faz

**Executa na VM OCI.** Sobe todos os serviços Docker e configura o Airflow.

| Passo | Ação | Detalhes |
|-------|------|---------|
| [1/6] | Verifica pré-requisitos | Docker, Docker Compose, docker-compose.yml, DAG, variables JSON |
| [2/6] | Verifica estrutura de diretórios | Cria `dags/`, `plugins/`, `config/`, `logs/` se não existirem. Configura `AIRFLOW_UID` |
| [3/6] | Sobe PostgreSQL | `docker compose up -d postgres` + healthcheck (até 30s) |
| [4/6] | Migra banco de dados | `airflow db migrate` + cria usuário admin (`airflow`/`airflow`) |
| [5/6] | Sobe webserver + scheduler | `docker compose up -d` + healthcheck do webserver (até 120s) |
| [6/6] | Importa variáveis + ativa DAG | 22 variáveis OCI + `airflow dags unpause pipeline_credit_risk_fpd` |

**Importante:** O setup sobe o PostgreSQL **primeiro** e espera ele ficar healthy antes de migrar o banco. Isso evita o erro `relation "variable" does not exist` que ocorre quando o banco não está pronto.

### 4.4 Executando o Deploy

```bash
# 1. Ir para o diretório do Airflow
cd mig_oci/airflow

# 2. Garantir permissão de execução
chmod +x deploy_to_vm.sh

# 3. Executar (substitua pelo IP real da VM)
./deploy_to_vm.sh 137.131.199.10 ~/.ssh/airflow_vm
```

**Output esperado (resumido):**
```
========================================
  Deploy Airflow → VM OCI
========================================
--- [1/4] Verificando conexão SSH ---
  Conectado!
--- [2/4] Criando estrutura de diretórios ---
  Diretórios criados.
--- [3/4] Copiando arquivos ---
  ✔ docker-compose.yml
  ✔ setup_vm.sh
  ✔ dag_pipeline_fpd.py
  ✔ airflow_variables_filled.json
--- [4/4] Executando setup na VM ---
  ...
  ✔ PostgreSQL pronto
  ✔ Banco migrado
  ✔ Usuário admin criado
  ✔ Webserver healthy
  ✔ Variáveis importadas: 22 de 22
  ✔ DAG pipeline_credit_risk_fpd ativada
========================================
  Setup concluído!
========================================
  UI Airflow:  http://137.131.199.10:8080
  Usuário:     airflow
  Senha:       airflow
```

### 4.5 Verificação Pós-Deploy

Após o deploy concluir com sucesso:

1. **Acessar a UI:** `http://<VM_IP>:8080` (user: `airflow`, senha: `airflow`)

2. **Verificar DAG ativa:** Na tela principal, a DAG `pipeline_credit_risk_fpd` deve aparecer com o toggle ON

3. **Verificar variáveis (via SSH):**
   ```bash
   ssh -i ~/.ssh/airflow_vm opc@<VM_IP> \
     "cd /opt/airflow-fpd && docker compose exec -T airflow-scheduler airflow variables list | grep -c oci_"
   # Esperado: 22
   ```

4. **Verificar containers saudáveis:**
   ```bash
   ssh -i ~/.ssh/airflow_vm opc@<VM_IP> \
     "cd /opt/airflow-fpd && docker compose ps"
   ```

---

## 5. Deploy Manual (Alternativo)

Se preferir controle granular, execute os passos manualmente:

```bash
# 1. Copiar arquivos para a VM
VM_IP=<IP_DA_VM>
KEY=~/.ssh/airflow_vm
scp -i $KEY docker/docker-compose.yml opc@$VM_IP:/opt/airflow-fpd/
scp -i $KEY docker/setup_vm.sh opc@$VM_IP:/opt/airflow-fpd/
scp -i $KEY dags/dag_pipeline_fpd.py opc@$VM_IP:/opt/airflow-fpd/dags/
scp -i $KEY config/airflow_variables_filled.json opc@$VM_IP:/opt/airflow-fpd/config/

# 2. SSH para a VM
ssh -i $KEY opc@$VM_IP

# 3. Na VM:
cd /opt/airflow-fpd
echo "AIRFLOW_UID=$(id -u)" > .env

# 4. Subir PostgreSQL e migrar banco
docker compose up -d postgres
sleep 10
docker compose run --rm airflow-scheduler airflow db migrate
docker compose run --rm airflow-scheduler airflow users create \
  --username airflow --firstname Airflow --lastname Admin \
  --role Admin --email admin@hackathon.local --password airflow

# 5. Subir Airflow
docker compose up -d

# 6. Importar variáveis e ativar DAG
docker compose exec airflow-scheduler \
  airflow variables import /opt/airflow/config/airflow_variables_filled.json
docker compose exec airflow-scheduler \
  airflow dags unpause pipeline_credit_risk_fpd
```

---

## 6. Variáveis Airflow

### 6.1 Gerar variáveis do Terraform

As variáveis contêm os OCIDs das 21 aplicações Data Flow (criadas pelo Terraform Fase 4):

```bash
cd mig_oci/airflow/config
./populate_variables.sh
```

Isso lê `terraform output -json` e gera `airflow_variables_filled.json` com os 22 valores reais.

> **Atenção:** `airflow_variables_filled.json` contém OCIDs sensíveis. Não comitar no Git (já no `.gitignore`).

### 6.2 Tabela de variáveis

| Variável | Descrição |
|----------|-----------|
| `oci_compute_compartment_id` | OCID do compartment de compute |
| `oci_app_id_bronze_bureau` | App Data Flow bronze-bureau |
| `oci_app_id_bronze_telco` | App Data Flow bronze-telco |
| `oci_app_id_bronze_cadastro` | App Data Flow bronze-cadastro |
| `oci_app_id_bronze_recarga` | App Data Flow bronze-recarga |
| `oci_app_id_bronze_pagamento` | App Data Flow bronze-pagamento |
| `oci_app_id_bronze_atraso` | App Data Flow bronze-atraso |
| `oci_app_id_silver_bureau` | App Data Flow silver-bureau |
| `oci_app_id_silver_telco` | App Data Flow silver-telco |
| `oci_app_id_silver_cadastro` | App Data Flow silver-cadastro |
| `oci_app_id_silver_recarga` | App Data Flow silver-recarga |
| `oci_app_id_silver_pagamento` | App Data Flow silver-pagamento |
| `oci_app_id_silver_atraso` | App Data Flow silver-atraso |
| `oci_app_id_gold_recarga` | App Data Flow gold-recarga |
| `oci_app_id_gold_pagamento` | App Data Flow gold-pagamento |
| `oci_app_id_gold_atraso` | App Data Flow gold-atraso |
| `oci_app_id_abt_v1` | App Data Flow abt-v1 |
| `oci_app_id_abt_v2` | App Data Flow abt-v2 |
| `oci_app_id_abt_v3` | App Data Flow abt-v3 |
| `oci_app_id_abt_v4` | App Data Flow abt-v4 |
| `oci_app_id_abt_v5` | App Data Flow abt-v5 |
| `oci_app_id_abt_v6` | App Data Flow abt-v6 |

Verificar variáveis importadas:
```bash
docker compose exec airflow-scheduler airflow variables list | grep oci_
```

---

## 7. Autenticação OCI

A DAG tenta **Instance Principal** primeiro (para Airflow rodando em VM OCI) e faz fallback para `~/.oci/config` (desenvolvimento local).

### Produção (VM OCI)

Nenhuma configuração adicional necessária. A VM deve pertencer a uma Dynamic Group com policy que permite gerenciar runs do Data Flow:

```
Allow dynamic-group airflow-instance-group to manage data-flow-run in compartment hackathon-2025-compute
```

> **Nota:** Se a Dynamic Group não estiver configurada, o Airflow faz fallback para `~/.oci/config` montado como volume read-only no Docker Compose.

### Desenvolvimento local

```bash
# Verificar que ~/.oci/config aponta para a tenancy correta
oci iam compartment list --query 'data[0].name' --raw-output
```

---

## 8. Execução e Monitoramento

### Pipeline completo (ETL + Modelo)

O trigger da DAG `pipeline_credit_risk_fpd` executa **automaticamente** o pipeline end-to-end:

```
Bronze (paralelo) → Silver (paralelo) → Gold Features (paralelo)
  → ABT v1→v6 (sequencial) → Trigger Modelo (TriggerDagRunOperator)
      → VM Start → SSH scoring → VM Stop
```

```bash
# Via CLI na VM — Executar na VM Airflow
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_credit_risk_fpd

# Com run_id customizado — Executar na VM Airflow
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_credit_risk_fpd --run-id manual_2026_03_05
```

Ou via UI: DAG > Trigger DAG (botão ▶).

### Re-scoring independente (sem re-ETL)

Para re-executar apenas o modelo (ex: após atualizar o `.pkl` ou ajustar hiperparâmetros):

```bash
# Via CLI na VM — Executar na VM Airflow
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_modelo_qualificacao
```

Ou via UI: DAG `pipeline_modelo_qualificacao` > Trigger DAG (botão ▶).

### Monitoramento na UI

- **Grid View:** Histórico de execuções por task — verde = sucesso, vermelho = falha
- **Graph View:** Dependências visuais entre grupos (Bronze → Silver → Gold → ABT → Modelo)
- **Logs:** Clique na task > View Log para ver o polling de status do Data Flow run
- **Task `trigger_modelo_qualificacao`:** Mostra o status da DAG do modelo (aguarda conclusão)

### Monitoramento no Console OCI

Os runs disparados pelo Airflow aparecem em: `Console OCI > Data Flow > Runs`

Cada run é nomeado como: `{layer}-{fonte} [YYYY-MM-DD]`
Exemplo: `silver-recarga [2026-02-01]`

### Comandos úteis na VM

```bash
cd /opt/airflow-fpd

docker compose ps                          # Status dos containers
docker compose logs -f airflow-scheduler   # Logs do scheduler (tempo real)
docker compose logs -f airflow-webserver   # Logs do webserver
docker compose down                        # Parar tudo
docker compose up -d                       # Reiniciar tudo
docker compose down -v                     # Parar + remover volumes (reset completo)
```

---

## 9. Troubleshooting

### Task falhou — como investigar?

1. No Airflow UI: clique na task vermelha > **View Log**
2. Procure pela linha `Run terminou com estado 'FAILED'` — ela inclui o Run ID
3. No Console OCI: `Data Flow > Runs > {Run ID} > Logs` para ver o erro Spark

### Erro: `Variable 'oci_app_id_...' not found`

A Airflow Variable não foi importada:
```bash
docker compose exec airflow-scheduler \
  airflow variables import /opt/airflow/config/airflow_variables_filled.json
```

### Erro: `relation "variable" does not exist`

O banco não foi migrado. Execute:
```bash
docker compose exec airflow-scheduler airflow db migrate
```

### Erro: `Instance Principal falhou`

A VM não tem permissão via Dynamic Group. Verifique:
1. A VM está na Dynamic Group `airflow-instance-group`?
2. A policy `manage data-flow-run` existe no compartment correto?

Fallback: configure `~/.oci/config` na VM para autenticar via API Key.

### Timeout após 3 horas

O `RUN_TIMEOUT_SECONDS = 10800` foi atingido. Possíveis causas:
- Job travado no Data Flow (verificar no Console OCI)
- Shape insuficiente para o volume de dados

Aumentar timeout editando `dag_pipeline_fpd.py`:
```python
RUN_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 horas
```

### Task ABT v5 falhou com erro X509

Causa conhecida: dados do passo anterior (`abt_v4/` ou `gold_recarga_features/`) não existem no bucket. O erro X509 é enganoso — na realidade é um path inválido.

Solução: re-executar a partir da task que falhou via UI (clique na task > Clear > Downstream).

### deploy_to_vm.sh falha com "Permission denied"

```bash
chmod +x deploy_to_vm.sh
# Se persistir, verificar permissões da chave SSH:
chmod 600 ~/.ssh/airflow_vm
```

### Containers não sobem (CRLF)

Se os scripts foram editados no Windows, podem ter line endings CRLF:
```bash
# No WSL, antes do deploy:
sed -i 's/\r//' docker/docker-compose.yml docker/setup_vm.sh deploy_to_vm.sh
```

---

## 10. Referências

| Documento | Descrição |
|-----------|-----------|
| `mig_oci/docs/FASE_4_IMPLEMENTACAO.md` | Compute: criação das 21 apps Data Flow |
| `mig_oci/docs/FASE_6A_LANDING_BRONZE.md` | Pipeline Landing → Bronze (as-built) |
| `mig_oci/docs/FASE_6B_SILVER.md` | Silver Layer: 6 scripts, dedup, BYTES calibração |
| `mig_oci/docs/FASE_6B_GOLD_FEATURES.md` | Gold Features: 3 scripts, 2 actions, quality gates |
| `mig_oci/docs/FASE_8_MODELO_VM_IMPLEMENTACAO.md` | Modelo scoring em VM dedicada (as-built) |
| `mig_oci/docs/FASE_8_TROUBLESHOOTING.md` | Troubleshooting: OOM, SSH, deploy, CRLF |
| `mig_oci/terraform/modules/airflow/` | Módulo Terraform da VM Airflow (cloud-init, Security List) |
| `mig_oci/terraform/modules/modelo_vm/` | Módulo Terraform da VM Modelo (E5.Flex, Dynamic Group) |
| `mig_oci/terraform/environments/prod/` | IaC que criou os 21 apps (Terraform Fase 4) |
| `mig_oci/data_science/scripts/modelo_qualificacao.py` | Script scoring: pandas + LightGBM, Instance Principal |
| `docs/architecture/OCI_TERRAFORM_AIRFLOW.md` | Arquitetura geral OCI + Airflow |
