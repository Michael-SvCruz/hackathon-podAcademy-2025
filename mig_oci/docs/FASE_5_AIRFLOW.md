# Migração OCI - Fase 5: Airflow (Orquestração do Pipeline)

## Contexto

Com as Fases 1-4 concluídas (IAM, Network, Storage, Compute) e as 21 Data Flow Applications criadas, a Fase 5 implementa a **orquestração** — o Apache Airflow que dispara e monitora os jobs Spark automaticamente, seguindo a ordem correta do pipeline Medallion.

**Pré-requisitos:**
- Fases 1-4 aplicadas (IAM, Network, Storage, Compute)
- 21 Data Flow Applications criadas (Fase 4)
- Scripts PySpark enviados para o bucket `pipeline-ops` (prefixo `scripts/`)
- Chave SSH para acesso à VM

---

## O que é o Apache Airflow?

Apache Airflow é um orquestrador de workflows — ele define **quando** e **em que ordem** os jobs devem executar. Não executa os jobs Spark diretamente; ele dispara as execuções (Runs) no OCI Data Flow via API e monitora o progresso.

**Analogia:** O Airflow é como um "maestro de orquestra". Ele não toca nenhum instrumento (jobs Spark), mas sabe a partitura (DAG) e diz a cada músico (Data Flow Application) quando começar e parar. Se um músico erra, o maestro pode pedir que tente de novo (retry).

```
Airflow (orquestrador)                   OCI Data Flow (execução)
┌──────────────────────┐                ┌──────────────────────────┐
│  DAG: pipeline_fpd   │                │                          │
│                      │   create_run   │  Run bronze-bureau       │
│  Task: bronze.bureau │───────────────→│  Status: IN_PROGRESS     │
│  Task: bronze.telco  │   (API call)   │  VMs: 1 driver + 2 exec │
│  Task: bronze.cadastro│               │  Logs: bucket/logs/      │
│  ...                 │   get_run      │                          │
│                      │←──────────────→│  Status: SUCCEEDED ✓     │
│  (polling a cada 60s)│   (polling)    │                          │
└──────────────────────┘                └──────────────────────────┘
  VM E3.Flex (1 OCPU, 16 GB)             VMs provisionadas sob demanda
  Docker Compose                          (desligadas após execução)
```

### DAG vs Task vs TaskGroup

| Conceito | Analogia | No Projeto |
|----------|----------|------------|
| **DAG** | Partitura completa | Pipeline FPD inteiro (21 tasks) |
| **TaskGroup** | Seção da orquestra | Bronze (6), Silver (6), Gold (3), ABT (6) |
| **Task** | Músico individual | Uma Data Flow Application (ex: `bronze.bureau`) |
| **Run** | Apresentação | Uma execução completa da DAG |

---

## Arquitetura na OCI

### Infraestrutura Terraform (Módulo `airflow`)

O módulo Terraform cria 4 recursos:

```
Módulo Airflow
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  1. Compute Instance (VM)                                       │
│     ┌──────────────────────────────────┐                        │
│     │  hackathon-2025-airflow-vm       │                        │
│     │  Shape: VM.Standard.E3.Flex      │                        │
│     │  1 OCPU, 16 GB RAM              │                        │
│     │  Oracle Linux 8                  │                        │
│     │  IP Público (para UI :8080)      │                        │
│     │  cloud-init: Docker + Compose    │                        │
│     └──────────────────────────────────┘                        │
│                                                                 │
│  2. Security List                                               │
│     ┌──────────────────────────────────┐                        │
│     │  Porta 8080 → Airflow Web UI     │                        │
│     │  Ingress: 0.0.0.0/0 (TCP 8080)  │                        │
│     │  Egress:  0.0.0.0/0 (all)       │                        │
│     └──────────────────────────────────┘                        │
│                                                                 │
│  3. Dynamic Group (Instance Principal)                          │
│     ┌──────────────────────────────────┐                        │
│     │  hackathon-2025-airflow-dg       │                        │
│     │  Matching: instance.id = <OCID>  │                        │
│     └──────────────────────────────────┘                        │
│                                                                 │
│  4. IAM Policy (Instance Principal)                             │
│     ┌──────────────────────────────────┐                        │
│     │  manage dataflow-family          │                        │
│     │  manage object-family            │                        │
│     │  read virtual-network-family     │                        │
│     │  in compartment hackathon-2025   │                        │
│     └──────────────────────────────────┘                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Instance Principal — Autenticação sem Credenciais

A VM do Airflow autentica na OCI **sem** arquivo de credenciais (`~/.oci/config`). Isso funciona via **Instance Principal**: a OCI reconhece a identidade da VM pelo OCID e aplica as permissões do Dynamic Group.

```
Autenticação Instance Principal
                                              OCI Identity
VM Airflow                                    ┌───────────────────────┐
┌──────────────┐   "Sou instance X"           │                       │
│              │──────────────────────────────→│  Dynamic Group:       │
│  Python:     │                              │   instance.id = X     │
│  signer =    │   "OK, você pertence ao DG"  │                       │
│  InstancePri │←──────────────────────────────│  Policy:              │
│  ncipals...  │                              │   manage dataflow     │
│              │   Token temporário (assinado) │   manage storage      │
│              │──────────────────────────────→│   read network        │
│              │   "Pode criar Data Flow Run"  │                       │
└──────────────┘                              └───────────────────────┘
```

**Sem Dynamic Group + Policy:** A VM autentica, mas recebe `404 NotAuthorizedOrNotFound` ao chamar APIs. Este erro é enganoso — não é "recurso não encontrado", mas "não autorizado".

---

## Software Stack na VM

```
VM OCI (Oracle Linux 8)
├── Docker CE + Docker Compose plugin (instalados via cloud-init)
│
└── /opt/airflow-fpd/
    ├── docker-compose.yml          ← 3 serviços + 1 init
    ├── setup_vm.sh                 ← Script de setup automatizado
    ├── .env                        ← AIRFLOW_UID
    ├── dags/
    │   ├── dag_pipeline_fpd.py             ← DAG principal (paralelo por grupo)
    │   └── dag_pipeline_fpd_sequential.py  ← DAG sequencial (teste)
    ├── config/
    │   └── airflow_variables_filled.json   ← 22 OCIDs do Terraform
    ├── plugins/                    ← (vazio — sem plugins customizados)
    └── logs/                       ← Logs das execuções (persistido)
```

### Docker Compose — 3 Serviços

| Serviço | Imagem | Função | Healthcheck |
|---------|--------|--------|-------------|
| **postgres** | `postgres:15` | Banco de metadados do Airflow | `pg_isready -U airflow` |
| **airflow-webserver** | `airflow:2.8.0-python3.10` | UI web (porta 8080) | `curl localhost:8080/health` |
| **airflow-scheduler** | `airflow:2.8.0-python3.10` | Dispara tasks, polling | `airflow jobs check --job-type SchedulerJob` |

**Escolhas técnicas:**
- **LocalExecutor** (sem Redis/Celery): suficiente para pipeline batch mensal. As tasks do Airflow apenas fazem chamadas API ao Data Flow — o trabalho pesado (Spark) acontece em VMs separadas.
- **OCI SDK** instalado via `_PIP_ADDITIONAL_REQUIREMENTS`: `oci>=2.126.0`
- **Volume `~/.oci:/opt/airflow/.oci:ro`**: fallback para desenvolvimento local (credenciais OCI). Na VM OCI, usa Instance Principal.

---

## As Duas DAGs

### DAG Principal: `pipeline_credit_risk_fpd`

Pipeline de produção com **paralelismo** dentro dos grupos:

```
Bronze (6 em paralelo)
  bureau ──┐
  telco ───┤
  cadastro─┤
  recarga ─┤──→ Silver (6 em paralelo) ──→ Gold (3 em paralelo) ──→ ABT (sequencial)
  pagamento┤      bureau, telco, ...        recarga, pagamento,      v1→v2→v3→v4→v5→v6
  atraso ──┘                                atraso
```

| Configuração | Valor |
|--------------|-------|
| **schedule** | `0 2 1 * *` (dia 1 de cada mês, 02:00) |
| **catchup** | Desabilitado |
| **retries** | 1 (com 5 min de delay) |
| **timeout por run** | 3 horas |
| **polling** | A cada 60 segundos |

### DAG Sequencial: `pipeline_fpd_sequential`

Pipeline de teste que executa **1 app por vez**, mesmo dentro dos grupos:

```
bureau → telco → cadastro → recarga → pagamento → atraso → (Silver) → ... → abt-v6
  (21 tasks encadeadas sequencialmente)
```

| Configuração | Valor |
|--------------|-------|
| **schedule** | `None` (trigger manual apenas) |
| **execution_timeout** | 4 horas por task |
| **uso** | Teste do pipeline em ambiente com quota limitada |

### Estratégia de Shapes por Grupo (Evitar Conflito de Quota)

Apps em grupos paralelos usam **famílias de shapes diferentes** para não competir pela mesma quota pool no OCI free tier:

| App | Bronze | Silver | Gold |
|-----|--------|--------|------|
| **bureau** | VM.Standard2.1 | VM.Standard2.1 | — |
| **telco** | VM.Standard2.2 | VM.Standard2.2 | — |
| **cadastro** | VM.Standard.A1.Flex | VM.Standard.A1.Flex | — |
| **recarga** | VM.Standard.E4.Flex | VM.Standard.E4.Flex | VM.Standard.E4.Flex |
| **pagamento** | VM.Standard.E3.Flex | VM.Standard.E3.Flex | VM.Standard3.Flex |
| **atraso** | VM.Standard3.Flex | VM.Standard3.Flex | VM.Standard.A1.Flex |

**ABT** (sequencial): todas usam `VM.Standard.E4.Flex` (shape mais potente, sem conflito pois rodam uma por vez).

### Retry com Backoff para Rate Limiting (HTTP 429)

Quando múltiplas tasks criam runs simultaneamente, a API do Data Flow pode retornar `429 TooManyRequests`. A DAG trata isso automaticamente com retry e backoff exponencial:

```python
# Comportamento do retry:
#   Tentativa 1: imediata
#   Se 429: espera 30s
#   Tentativa 2: retry
#   Se 429: espera 60s
#   Tentativa 3: retry (última — falha se ainda retornar 429)
```

| Parâmetro | Valor |
|-----------|-------|
| `CREATE_RUN_MAX_RETRIES` | 3 |
| `CREATE_RUN_BASE_DELAY` | 30s (backoff: 30s, 60s, 120s) |
| Exceção capturada | `oci.exceptions.TransientServiceError` |

**Diferença entre rate limiting e retry do Airflow:**
- **Rate limiting (429):** Tratado na DAG com delay curto (30-120s). Sem perder estado.
- **Retry do Airflow:** Espera 5 minutos, recria a task do zero. Mais pesado e lento.

---

## Deploy Automatizado

O deploy é feito em **dois scripts**: um local que copia arquivos para a VM, e um na VM que configura tudo.

### Fluxo Completo

```
   Máquina Local (WSL/Linux/Mac)             VM OCI (Oracle Linux 8)
   ┌───────────────────────────┐             ┌───────────────────────────┐
   │                           │             │                           │
   │  1. cd mig_oci/airflow    │             │  /opt/airflow-fpd/        │
   │                           │     SCP     │                           │
   │  2. ./deploy_to_vm.sh     │────────────→│  docker-compose.yml       │
   │     <IP> <SSH_KEY>        │   4 files   │  setup_vm.sh              │
   │                           │             │  dags/dag_pipeline_fpd.py │
   │                           │     SSH     │  config/variables.json    │
   │                           │────────────→│                           │
   │                           │  run setup  │  3. ./setup_vm.sh         │
   │                           │             │     [1/6] Pré-requisitos  │
   │                           │             │     [2/6] Diretórios      │
   │                           │             │     [3/6] PostgreSQL ↑    │
   │                           │             │     [4/6] DB migrate      │
   │                           │             │     [5/6] Webserver ↑     │
   │                           │             │     [6/6] Variables + DAG │
   │                           │             │                           │
   │                           │             │  ✔ http://<IP>:8080       │
   └───────────────────────────┘             └───────────────────────────┘
```

### `deploy_to_vm.sh` (executa localmente)

```bash
cd mig_oci/airflow
./deploy_to_vm.sh <VM_IP> [SSH_KEY_PATH]
# Exemplo:
./deploy_to_vm.sh 137.131.199.10 ~/.ssh/airflow_vm
```

O script:
1. Verifica conexão SSH
2. Cria diretórios remotos (`dags/`, `plugins/`, `config/`, `logs/`)
3. Copia 4 arquivos via SCP
4. Executa `setup_vm.sh` na VM via SSH

### `setup_vm.sh` (executa na VM)

O script faz 6 passos:

| Passo | Ação | Detalhe |
|-------|------|---------|
| **1/6** | Verificar pré-requisitos | Docker, Compose, docker-compose.yml, DAG, Variables |
| **2/6** | Verificar diretórios | `dags/`, `plugins/`, `config/`, `logs/`, `.env` |
| **3/6** | Subir PostgreSQL | `docker compose up -d postgres` + aguardar `pg_isready` |
| **4/6** | Migrar banco | `airflow db migrate` + `airflow users create` |
| **5/6** | Subir Airflow | `docker compose up -d` + aguardar healthcheck |
| **6/6** | Importar variáveis | `airflow variables import` + `airflow dags unpause` |

**Detalhe importante (Passo 4):** O `airflow db migrate` é executado via `docker compose run --rm airflow-scheduler` (não via `airflow-init`). Isso evita dois problemas:
- O serviço `airflow-init` usa `entrypoint: /bin/bash`, que pode causar "cannot execute binary file" se o comando não for passado corretamente
- O `airflow-scheduler` tem o entrypoint padrão que instala o `_PIP_ADDITIONAL_REQUIREMENTS` (OCI SDK) antes de executar

### Checklist Pré-Deploy

Antes de executar `deploy_to_vm.sh`, verificar:

- [ ] VM ligada e acessível via SSH (`ssh -i <key> opc@<IP>`)
- [ ] Security List com porta 8080 aberta (Terraform cria automaticamente)
- [ ] Security List com porta 22 aberta (SSH — na security list pública)
- [ ] `config/airflow_variables_filled.json` preenchido com OCIDs reais (22 variáveis)
- [ ] `dags/dag_pipeline_fpd.py` presente no diretório local

---

## Airflow Variables (22 variáveis)

As variáveis conectam a DAG do Airflow aos recursos da OCI criados pelo Terraform. Cada variável é um OCID.

### Geração automática (via Terraform)

```bash
cd mig_oci/terraform/environments/prod
terraform output -json > /tmp/tf_output.json

# Usar populate_variables.sh para gerar airflow_variables_filled.json
cd ../../airflow/config
./populate_variables.sh
```

### Lista de variáveis

| Variável | Tipo | Origem (Terraform output) |
|----------|------|---------------------------|
| `oci_compute_compartment_id` | OCID | `compartment_ids.compute` |
| `oci_app_id_bronze_bureau` | OCID | `dataflow_applications.bronze-bureau` |
| `oci_app_id_bronze_telco` | OCID | `dataflow_applications.bronze-telco` |
| `oci_app_id_bronze_cadastro` | OCID | `dataflow_applications.bronze-cadastro` |
| `oci_app_id_bronze_recarga` | OCID | `dataflow_applications.bronze-recarga` |
| `oci_app_id_bronze_pagamento` | OCID | `dataflow_applications.bronze-pagamento` |
| `oci_app_id_bronze_atraso` | OCID | `dataflow_applications.bronze-atraso` |
| `oci_app_id_silver_*` (6) | OCID | `dataflow_applications.silver-*` |
| `oci_app_id_gold_recarga` | OCID | `dataflow_applications.gold-recarga` |
| `oci_app_id_gold_pagamento` | OCID | `dataflow_applications.gold-pagamento` |
| `oci_app_id_gold_atraso` | OCID | `dataflow_applications.gold-atraso` |
| `oci_app_id_abt_v1` a `_v6` (6) | OCID | `dataflow_applications.abt-v*` |

---

## O que foi criado (Fase 5)

### Terraform (módulo `airflow`)

| Arquivo | Propósito |
|---------|-----------|
| `terraform/modules/airflow/main.tf` | VM + Security List + Dynamic Group + Policy |
| `terraform/modules/airflow/variables.tf` | Inputs: tenancy_ocid, compartment_id, subnet, SSH key, shape |
| `terraform/modules/airflow/outputs.tf` | Outputs: vm_id, public_ip, ui_url, security_list_id |
| `terraform/modules/airflow/cloud-init.yaml` | Instalação automática de Docker + estrutura de diretórios |

### Airflow (pasta `mig_oci/airflow/`)

| Arquivo | Propósito |
|---------|-----------|
| `deploy_to_vm.sh` | Deploy automatizado: copia 4 arquivos + executa setup na VM |
| `docker/docker-compose.yml` | 3 serviços (PostgreSQL, Webserver, Scheduler) + LocalExecutor |
| `docker/setup_vm.sh` | Setup completo: PostgreSQL → migrate → Airflow → variables → DAG |
| `dags/dag_pipeline_fpd.py` | DAG principal: 21 apps, 4 TaskGroups paralelos, retry 429 |
| `dags/dag_pipeline_fpd_sequential.py` | DAG de teste: 21 apps sequenciais (1 por vez) |
| `config/airflow_variables.json` | Template de variáveis (placeholders) |
| `config/airflow_variables_filled.json` | Variáveis preenchidas com OCIDs reais |
| `config/populate_variables.sh` | Gera o JSON preenchido a partir do `terraform output` |

### Recursos criados na OCI (4 recursos)

| Recurso | Nome | Tipo |
|---------|------|------|
| Compute Instance | `hackathon-2025-airflow-vm` | `VM.Standard.E3.Flex` (1 OCPU, 16 GB) |
| Security List | `hackathon-2025-airflow-sl` | Porta 8080 (Airflow UI) |
| Dynamic Group | `hackathon-2025-airflow-dynamic-group` | Matching rule no OCID da VM |
| IAM Policy | `hackathon-2025-airflow-instance-principal-policy` | 3 statements (dataflow, storage, network) |

---

## Verificação Pós-Deploy

### 1. Verificar containers

```bash
ssh -i ~/.ssh/airflow_vm opc@<VM_IP>
cd /opt/airflow-fpd
docker compose ps
```

**Saída esperada:**
```
NAME                              STATUS                    PORTS
airflow-fpd-airflow-scheduler-1   Up X minutes (healthy)    8080/tcp
airflow-fpd-airflow-webserver-1   Up X minutes (healthy)    0.0.0.0:8080->8080/tcp
airflow-fpd-postgres-1            Up X minutes (healthy)    5432/tcp
```

Os 3 serviços devem estar `healthy`.

### 2. Verificar variáveis

```bash
docker compose exec airflow-scheduler airflow variables list 2>/dev/null | grep -c "oci_"
# Esperado: 22
```

### 3. Acessar UI

Abrir no navegador: `http://<VM_IP>:8080`
- Usuário: `airflow`
- Senha: `airflow`
- Verificar: DAG `pipeline_credit_risk_fpd` visível e ativa

### 4. Trigger manual de teste

Na UI do Airflow: DAG `pipeline_credit_risk_fpd` → botão "Trigger DAG" (▶).

Ou via CLI:
```bash
docker compose exec airflow-scheduler airflow dags trigger pipeline_credit_risk_fpd
```

### 5. Após reinício da VM

Quando a VM for parada e reiniciada (novo IP possível):

```bash
ssh -i ~/.ssh/airflow_vm opc@<NOVO_IP>
cd /opt/airflow-fpd
docker compose ps     # Verificar se containers subiram automaticamente (restart: unless-stopped)
```

Se os containers não subiram:
```bash
docker compose up -d
```

---

## Erros Comuns e Soluções

### Deploy

| Erro | Causa | Solução |
|------|-------|---------|
| `ssh: Connection refused` | VM desligada ou porta 22 bloqueada | Verificar VM no Console OCI + Security List |
| `Permission denied (publickey)` | Chave SSH errada ou não corresponde | Verificar que a chave pública está no `terraform.tfvars` |
| `cloud-init` não executou | VM antiga (antes da alteração do cloud-init) | Destruir e recriar VM via Terraform |

### Setup (setup_vm.sh)

| Erro | Causa | Solução |
|------|-------|---------|
| `Docker não instalado` | cloud-init ainda não terminou | Aguardar: `tail -f /var/log/cloud-init-output.log` |
| `pg_isready` timeout | PostgreSQL não iniciou | `docker compose logs postgres` |
| `relation "variable" does not exist` | `airflow db migrate` não executou | Executar manualmente: `docker compose run --rm airflow-scheduler airflow db migrate` |
| `No module named 'airflow'` | Usando `airflow-init` com `--entrypoint ""` | Usar `airflow-scheduler` em vez de `airflow-init` |
| `cannot execute binary file` | `airflow-init` tem `entrypoint: /bin/bash` | Usar `airflow-scheduler` (entrypoint correto) |

### DAG em execução

| Erro | Causa | Solução |
|------|-------|---------|
| `404 NotAuthorizedOrNotFound` | Dynamic Group ou Policy não existe | Verificar Terraform: `terraform state list \| grep dynamic` |
| `429 TooManyRequests` | Rate limiting da API (muitos `create_run` simultâneos) | Automático: retry com backoff (30s/60s/120s) |
| `SIGTERM` / `externally set to restarting` | Scheduler reiniciou enquanto task rodava | O Data Flow run continua na OCI; o Airflow re-tenta via retry |
| `Out of host capacity` | Sem VMs disponíveis para o shape solicitado | Trocar shape ou aguardar disponibilidade |
| Task fica em `ACCEPTED` por muito tempo | OCI provisionando VMs Spark | Normal (2-5 min). Se > 10 min, verificar quota |

---

## Lições Aprendidas

### 1. Astro CLI não é adequado para deploy em VM OCI

**Tentativa inicial:** Usar Astro CLI (Astronomer) para gerenciar o Airflow na VM.

**Problemas encontrados:**
- `astro dev run` cria containers **temporários** que não persistem estado (variáveis importadas desaparecem)
- Astro só monta `dags/`, `plugins/`, `include/` — não monta a raiz do projeto (arquivos de config não acessíveis)
- `astro dev run variables import` reporta sucesso mas as variáveis não ficam no banco

**Solução:** Docker Compose padrão com `docker compose exec` (executa dentro do container ativo, persistindo estado no PostgreSQL).

### 2. `airflow db migrate` deve ser explícito (não depender do `airflow-init`)

**Problema:** O serviço `airflow-init` no Docker Compose pede confirmação interativa durante a migração, que expira em 4 segundos no modo não-interativo. Resultado: tabelas não são criadas, e o `variables import` falha com `relation "variable" does not exist`.

**Solução:** Executar `airflow db migrate` explicitamente via `docker compose run --rm airflow-scheduler` antes de qualquer operação.

### 3. Alteração no `cloud-init.yaml` destrói e recria a VM

**Problema:** O `cloud-init.yaml` é passado como `metadata.user_data` no Terraform. Qualquer alteração neste arquivo força a **substituição** (destroy + create) da VM, gerando um novo IP público.

**Impacto:** Todos os dados da VM são perdidos (containers, volumes, configurações). O deploy precisa ser refeito do zero.

**Prevenção:** Alterações pós-deploy devem ser feitas via SSH (não via cloud-init). Usar `deploy_to_vm.sh` para reconfigurar.

### 4. Shapes diferentes por grupo paralelo evitam conflito de quota

**Problema:** No OCI free tier, cada família de shape tem uma quota limitada. Ao rodar 6 apps Bronze em paralelo com o mesmo shape, apenas 1-2 conseguiam provisionar; as demais falhavam com `Out of host capacity`.

**Solução:** Distribuir apps em 6 famílias de shapes diferentes (Standard2.1, Standard2.2, A1.Flex, Standard3.Flex, E3.Flex, E4.Flex). Cada app consome de um pool de quota independente, permitindo paralelismo real.

### 5. Rate limiting (429) é diferente de quota de compute

**429 TooManyRequests:** A API do Data Flow limita chamadas `create_run` por segundo por tenant. Acontece quando múltiplas tasks fazem `create_run` simultaneamente. Solução: retry com backoff (30s).

**Out of host capacity:** Não há VMs disponíveis para o shape solicitado. É um problema de infraestrutura, não de API. Solução: trocar shape ou aguardar.

### 6. Instance Principal requer Dynamic Group + Policy no nível do Tenancy

**Problema:** A VM autenticava com sucesso (`InstancePrincipalsSecurityTokenSigner`) mas recebia `404 NotAuthorizedOrNotFound` ao criar runs.

**Causa:** Dynamic Groups e suas policies são recursos de **Tenancy level** (não de compartment). Sem a policy, a VM tem identidade mas zero permissões.

**Solução:** Criar Dynamic Group com matching rule no OCID da VM + Policy com `manage dataflow-family` + `manage object-family` no compartment do projeto.

---

## Custo da Fase 5

### VM do Airflow (mensal)

| Recurso | Shape | OCPUs | RAM | Custo/Hora | Custo/Mês (24/7) |
|---------|-------|:-----:|:---:|:----------:|:-----------------:|
| VM Airflow | E3.Flex | 1 | 16 GB | ~$0.03 | ~$22 |
| Block Volume | 50 GB | — | — | — | ~$3 |
| **Total** | — | — | — | — | **~$25** |

**Nota:** O custo principal do pipeline é o Data Flow (Fase 4), não o Airflow. A VM do Airflow apenas faz chamadas API — o processamento pesado acontece nas VMs Spark provisionadas sob demanda.

---

## Estrutura Atual do Projeto (Após Fase 5)

```
mig_oci/
├── terraform/
│   ├── modules/
│   │   ├── iam/           # ✅ Fase 1 (compartments, groups, policies)
│   │   ├── network/       # ✅ Fase 2 (VCN, subnets, gateways)
│   │   ├── storage/       # ✅ Fase 3 (7 buckets Object Storage)
│   │   ├── compute/       # ✅ Fase 4 (21 Data Flow apps, per-app shapes)
│   │   └── airflow/       # ✅ Fase 5 (VM + Dynamic Group + Policy)
│   │       ├── main.tf              # VM + Security List + DG + Policy
│   │       ├── variables.tf         # tenancy_ocid, compartment_id, SSH key
│   │       ├── outputs.tf           # vm_id, public_ip, ui_url
│   │       └── cloud-init.yaml      # Docker + Compose + estrutura
│   ├── environments/prod/
│   │   ├── main.tf                  # ✅ Orquestra 5 módulos
│   │   ├── variables.tf             # ✅ Variáveis Fases 1-5
│   │   └── outputs.tf               # ✅ Outputs Fases 1-5
│   └── scripts/
│       ├── init.sh                  # ✅ Validação inicial
│       └── apply_phase.sh           # ✅ Apply incremental (Fases 1-5)
│
├── airflow/                         # ✅ Fase 5 (orquestração)
│   ├── deploy_to_vm.sh              # Deploy local → VM (único comando)
│   ├── docker/
│   │   ├── docker-compose.yml       # Airflow 2.8.0 + PostgreSQL 15
│   │   └── setup_vm.sh              # Setup automatizado (6 passos)
│   ├── dags/
│   │   ├── dag_pipeline_fpd.py      # DAG principal (paralelo por grupo)
│   │   └── dag_pipeline_fpd_sequential.py  # DAG sequencial (teste)
│   ├── config/
│   │   ├── airflow_variables.json            # Template (placeholders)
│   │   ├── airflow_variables_filled.json     # OCIDs reais (22 vars)
│   │   └── populate_variables.sh             # Gerador automático
│   └── README.md                    # Guia completo com troubleshooting
│
├── data_upload/                     # ✅ Fase 4/6 (scripts + upload)
│   ├── scripts/                     # 21 scripts PySpark self-contained
│   └── upload_scripts.sh            # Upload para Object Storage
│
└── docs/
    ├── FASE_0_1_IMPLEMENTACAO.md    # ✅ Setup + IAM
    ├── FASE_2_3_IMPLEMENTACAO.md    # ✅ Network + Storage
    ├── FASE_4_IMPLEMENTACAO.md      # ✅ Compute (Data Flow)
    ├── FASE_5_AIRFLOW.md            # ✅ Este documento
    ├── FASE_6A_LANDING_BRONZE.md    # ✅ Pipeline Bronze
    ├── FASE_6A_TROUBLESHOOTING.md   # ✅ Lições aprendidas
    ├── FASE_6B_SILVER.md            # ✅ Silver Layer
    └── FASE_6B_GOLD_FEATURES.md     # ✅ Gold Features
```

---

## Próximos Passos

| Fase | Status | O que faz | Dependência |
|------|--------|-----------|-------------|
| **Fase 0** | ✅ Concluída | Setup + validação credenciais | — |
| **Fase 1** | ✅ Aplicada | IAM (6 compartments, 4 groups, 6 policies) | Fase 0 |
| **Fase 2** | ✅ Aplicada | Network (VCN, 3 subnets, 3 gateways) | Fase 1 |
| **Fase 3** | ✅ Aplicada | Storage (7 buckets Object Storage) | Fase 1 |
| **Fase 4** | ✅ Aplicada | Compute (21 Data Flow apps, per-app shapes) | Fases 1, 3 |
| **Fase 5** | ✅ Aplicada | Airflow (VM + Docker Compose + Instance Principal) | Fases 1, 2 |
| **Fase 6A** | ✅ Concluída | Pipeline Landing → Bronze (6 scripts) | Fases 3, 4 |
| **Fase 6B** | ✅ Concluída | Silver + Gold Features + ABT v1-v6 | Fase 6A |
| **Teste** | 🔄 Em andamento | Pipeline completo via Airflow (DAG trigger) | Fase 5, 6B |

**Para testar o pipeline completo:**
1. Verificar VM ligada e containers saudáveis
2. Na UI do Airflow, trigger manual da DAG `pipeline_credit_risk_fpd`
3. Monitorar progresso: UI Airflow (tasks) + Console OCI (Data Flow Runs)
4. Se quota limitada: usar DAG `pipeline_fpd_sequential` (1 app por vez)

---

## Glossário (Novos Termos)

| Termo | Significado |
|-------|-------------|
| **DAG** | Directed Acyclic Graph — definição do workflow (quais tasks, em que ordem) |
| **TaskGroup** | Agrupamento visual de tasks na UI do Airflow (Bronze, Silver, Gold, ABT) |
| **Instance Principal** | Mecanismo OCI para VMs se autenticarem sem credenciais de usuário |
| **Dynamic Group** | Grupo de recursos OCI (VMs) identificados por regras de matching (OCID, compartment, tag) |
| **LocalExecutor** | Executor do Airflow que roda tasks em processos locais (sem Redis/Celery) |
| **cloud-init** | Ferramenta para configuração automática de VMs na primeira inicialização |
| **Rate Limiting (429)** | Limite de chamadas API por segundo. Diferente de quota de compute |
| **Backoff Exponencial** | Estratégia de retry onde o delay dobra a cada tentativa (30s, 60s, 120s) |
| **TransientServiceError** | Erro temporário da OCI (429, 503) que pode ser resolvido com retry |
| **`user_data`** | Atributo do Terraform para passar cloud-init à VM. Alteração força substituição da VM |
| **`AIRFLOW_PROJ_DIR`** | Variável de ambiente que define o diretório raiz para volumes do Docker Compose |
| **`_PIP_ADDITIONAL_REQUIREMENTS`** | Variável do container Airflow para instalar pacotes Python adicionais no startup |
