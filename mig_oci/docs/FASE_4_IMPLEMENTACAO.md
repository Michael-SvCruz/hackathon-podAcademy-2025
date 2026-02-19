# Migração OCI - Fase 4: Compute (Data Flow Applications)

## Contexto

Com as Fases 1-3 concluídas, temos a estrutura organizacional (IAM), a rede (VCN) e o armazenamento (Object Storage) prontos na OCI. A Fase 4 cria o **processamento** — as aplicações OCI Data Flow que implementam o pipeline Medallion Architecture usando Apache Spark.

**Pré-requisitos:**
- Fase 0 concluída (credenciais validadas)
- Fase 1 aplicada (compartments, groups, policies — incluindo `dataflow-service-policy`)
- Fase 3 aplicada (buckets Object Storage existem na OCI)
- Scripts Python enviados para o bucket `landing-zone` (ver seção Upload)

---

## O que é OCI Data Flow?

OCI Data Flow é o serviço gerenciado da Oracle para executar jobs **Apache Spark**. É equivalente ao **Databricks Jobs** ou ao **AWS Glue** — você envia um script PySpark e a OCI cuida de provisionar servidores, executar o job e desligar tudo ao final.

**Analogia:** Data Flow funciona como um "Uber para Spark". Você define o destino (script), quantos passageiros (executors) e o tamanho do carro (OCPUs/memória). Quando pede uma corrida (Run), a OCI provisiona tudo, executa e desliga. Você paga apenas pelo tempo de uso.

### Application vs Run

O Data Flow separa dois conceitos fundamentais:

| Conceito | Analogia | O que define | Quem cria |
|----------|----------|--------------|-----------|
| **Application** | Receita de bolo | Configuração: script, shape, executors | Terraform (Fase 4) |
| **Run** | Bolo sendo assado | Execução real do job Spark | Airflow, CLI ou Console |

Uma Application é criada **uma vez** e pode ser executada **infinitas vezes** (Runs). Cada Run gera logs, métricas e artefatos independentes.

```
Application (definição)              Run (execução)
┌─────────────────────┐          ┌─────────────────────┐
│ • Script: bronze.py │          │ • Run ID: ocid1...  │
│ • 2 OCPUs           │────→     │ • Status: SUCCEEDED │
│ • 4 executors       │  criar   │ • Duração: 12 min   │
│ • Spark 3.5.0       │  runs    │ • Logs: bucket/logs │
└─────────────────────┘          └─────────────────────┘
                                 ┌─────────────────────┐
                          ────→  │ • Run ID: ocid2...  │
                           outra │ • Status: RUNNING   │
                           run   │ • Duração: 5 min... │
                                 └─────────────────────┘
```

**No Terraform:** Criamos apenas Applications (a "receita"). Os Runs (execuções) serão disparados pelo Airflow ou manualmente via CLI/Console.

---

## Arquitetura: 4 Applications do Pipeline

As 4 aplicações Data Flow implementam o pipeline Medallion Architecture, espelhando os jobs que rodam no Databricks:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  dataflow-bronze │    │  dataflow-silver │    │  dataflow-gold  │    │  dataflow-abt   │
│                  │    │                  │    │                  │    │                  │
│  landing-zone ──→│──→ │  bronze ────────→│──→ │  silver ────────→│──→ │  gold ──→ ABT   │
│  bronze-layer    │    │  silver-layer    │    │  gold-layer      │    │  v6 final       │
│                  │    │                  │    │                  │    │                  │
│  2 OCPU × 4 exec│    │  4 OCPU × 8 exec│    │  4 OCPU × 16 exec│    │  4 OCPU × 8 exec│
│  (leve)          │    │  (médio)         │    │  (pesado)        │    │  (médio)         │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

| Application | Função | Entrada | Saída | Equivalente Databricks |
|-------------|--------|---------|-------|------------------------|
| **Bronze** | Ingestão + metadados | CSVs/Parquets brutos | Delta com schema | Jobs de ingestão |
| **Silver** | Tipagem + validação + sentinelas | Bronze (raw) | Dados tipados | `to_int_safe()`, `to_date_safe()` |
| **Gold** | Feature engineering (M1/M3/M6) | Silver (tipado) | Recarga, Pagamento, Atraso features | `gold_*_features_v2.py` |
| **ABT** | Join + builder final | Gold features + Scores | ABT v6 (614 cols) | `05_gold_abt_v6_builder_v2.py` |

### Por que o Gold é o mais pesado?

O Gold usa 16 executors (vs 4 ou 8 dos outros) porque o feature engineering é a etapa mais intensiva computacionalmente:
- Calcula features em **3 janelas temporais** (M1, M3, M6) para cada CPF
- Processa **3 blocos de features** (Recarga: 74, Pagamento: 56, Atraso: 19)
- Envolve window functions e aggregações complexas (percentis, coeficientes de variação)
- Dataset base: ~3.79M registros × múltiplas tabelas de lookup

---

## Configuração das Applications

### Shape Flex: VM.Standard.E4.Flex

Todas as applications usam o shape flexível `VM.Standard.E4.Flex`, que permite ajustar OCPUs e memória de forma independente por etapa:

```
Shape Flex (configurável)           vs    Shape Fixo (desperdício)
┌───────────────────────┐                ┌───────────────────────┐
│  Bronze: 2 OCPU, 32GB │                │  Todos: 4 OCPU, 64GB │
│  Silver: 4 OCPU, 64GB │                │  Bronze desperdiça    │
│  Gold:   4 OCPU, 64GB │                │  50% dos recursos     │
│  ABT:    4 OCPU, 64GB │                │                       │
└───────────────────────┘                └───────────────────────┘
  Paga só o que precisa                    Paga pelo máximo sempre
```

**Regra de memória:** 16 GB por OCPU (padrão OCI Data Flow). O Terraform calcula automaticamente:

```hcl
# No módulo compute/main.tf:
memory_in_gbs = var.dataflow_bronze_ocpu * 16
# 2 OCPUs × 16 = 32 GB de RAM por executor
```

### Recursos por Application

| Application | OCPUs/Executor | RAM/Executor | Executors | Total OCPUs | Total RAM |
|-------------|:-:|:-:|:-:|:-:|:-:|
| **Bronze** | 2 | 32 GB | 4 | 8 | 128 GB |
| **Silver** | 4 | 64 GB | 8 | 32 | 512 GB |
| **Gold** | 4 | 64 GB | 16 | 64 | 1024 GB |
| **ABT** | 4 | 64 GB | 8 | 32 | 512 GB |

**Nota:** O driver Spark usa as mesmas configurações de OCPU/memória que os executors. Então cada Application aloca `num_executors + 1` (driver + executors) máquinas virtuais durante a execução.

### URLs dos Scripts

Os scripts Python ficam no bucket `landing-zone`, prefixo `scripts/`:

```
oci://hackathon-2025-landing-zone@grfjr7baehkj/
├── scripts/
│   ├── bronze_job.py      ← dataflow-bronze aponta aqui
│   ├── silver_job.py      ← dataflow-silver aponta aqui
│   ├── gold_job.py        ← dataflow-gold aponta aqui
│   └── abt_job.py         ← dataflow-abt aponta aqui
├── dataflow-logs/         ← logs de todas as execuções
└── dataflow-warehouse/    ← warehouse temporário do Spark
```

**Importante:** A OCI valida que o script existe no bucket **no momento da criação** da Application (ver Lições Aprendidas).

---

## Upload de Scripts (Pré-requisito)

Antes de executar `./apply_phase.sh 4`, os scripts devem existir no bucket. Criamos scripts placeholder (mínimos) que serão substituídos pelos scripts reais na Fase 6.

### Estrutura Local

```
mig_oci/data_upload/
├── scripts/
│   ├── bronze_job.py        # Placeholder PySpark
│   ├── silver_job.py        # Placeholder PySpark
│   ├── gold_job.py          # Placeholder PySpark
│   └── abt_job.py           # Placeholder PySpark
└── upload_scripts.sh        # Script de upload via OCI CLI
```

### Como fazer o upload

```bash
cd mig_oci/data_upload
./upload_scripts.sh
```

**Saída esperada:**
```
=== Upload de Scripts para OCI ===

Namespace: grfjr7baehkj
Bucket:    hackathon-2025-landing-zone

Uploading bronze_job.py...
Uploading object  [####################################]  100%
Uploading silver_job.py...
Uploading object  [####################################]  100%
Uploading gold_job.py...
Uploading object  [####################################]  100%
Uploading abt_job.py...
Uploading object  [####################################]  100%

=== Upload concluído! ===
```

**Pré-requisito:** OCI CLI configurado (`oci setup config`). O CLI usa as mesmas credenciais do `terraform.tfvars`:
- `user_ocid` → User OCID
- `tenancy_ocid` → Tenancy OCID
- `fingerprint` → Fingerprint
- `private_key_path` → Caminho para o `.pem`
- `region` → `sa-saopaulo-1`

**Verificação:** Listar objetos no bucket após o upload:
```bash
oci os object list \
  --bucket-name hackathon-2025-landing-zone \
  --prefix scripts/ \
  --query 'data[].name'
```

---

## O que foi criado (Fase 4)

| Arquivo | Propósito |
|---------|-----------|
| `terraform/modules/compute/main.tf` | 4 aplicações `oci_dataflow_application` (Bronze, Silver, Gold, ABT) |
| `terraform/modules/compute/variables.tf` | Entradas: compartment_id, namespace, buckets, OCPUs, executors, spark_version, project_name, tags |
| `terraform/modules/compute/outputs.tf` | Saídas: 4 application IDs (para Airflow) |
| `data_upload/scripts/*.py` | 4 scripts PySpark placeholder |
| `data_upload/upload_scripts.sh` | Script de upload para OCI Object Storage |

**Arquivos atualizados:**

| Arquivo | Mudança |
|---------|---------|
| `terraform/environments/prod/main.tf` | Bloco `module "compute"` descomentado + parâmetros `namespace`, `project_name`, `spark_version` adicionados |
| `terraform/environments/prod/outputs.tf` | Bloco `output "dataflow_applications"` descomentado |

**Recursos criados na OCI (4 recursos):**
- 4 Data Flow Applications (Python, Spark 3.5.0, BATCH)

---

## Custo da Fase 4

**Applications não custam nada.** O custo ocorre apenas durante as **execuções (Runs)**.

### Custo por execução (estimativa)

O preço do Data Flow é baseado em OCPUs × horas de execução:
- **OCPU/hora:** ~$0.0638 (preço OCI Data Flow, região São Paulo)

| Application | Total OCPUs | Duração Est. | Custo/Run |
|-------------|:-:|:-:|:-:|
| **Bronze** | 8 + 2 (driver) = 10 | ~15 min | ~$0.16 |
| **Silver** | 32 + 4 (driver) = 36 | ~30 min | ~$1.15 |
| **Gold** | 64 + 4 (driver) = 68 | ~60 min | ~$4.34 |
| **ABT** | 32 + 4 (driver) = 36 | ~30 min | ~$1.15 |
| **Total (1 run completo)** | - | ~2h15 | **~$6.80** |

**Custo mensal estimado:**
- Pipeline executado **1x por mês** (batch mensal): ~$6.80/mês
- Pipeline executado **1x por semana** (desenvolvimento): ~$27.20/mês

**Dica:** Durante o Hackathon (30 dias), o custo total de Data Flow será mínimo porque são poucas execuções. O custo principal é de desenvolvimento/testes, não de produção.

---

## Fluxo de Dependências: IAM + Storage → Compute

```
Módulo IAM                    Módulo Storage               Módulo Compute
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│                  │         │                  │         │                  │
│ compute_         │────────→│                  │         │ compute_         │
│ compartment_id   │    ┌───→│ namespace        │────────→│ compartment_id   │
│                  │    │    │                  │         │ namespace        │
│ (onde criar)     │    │    │ bucket_landing   │────────→│ bucket_*         │
└──────────────────┘    │    │ bucket_bronze    │         │                  │
                        │    │ bucket_silver    │         │ (cria 4 Data     │
                        │    │ bucket_gold      │         │  Flow apps aqui) │
                        │    └──────────────────┘         └──────────────────┘
                        │
                        └─── O Compute precisa do namespace
                             para construir URLs oci://
```

**Por que Compute depende de Storage?**
As URLs de `file_uri`, `logs_bucket_uri` e `warehouse_bucket_uri` são construídas usando o **namespace** e os **nomes dos buckets** do módulo Storage. Sem essas informações, o Data Flow não sabe onde encontrar os scripts nem onde salvar logs.

**Por que Compute NÃO depende de Network?**
Na configuração atual, as Applications não especificam subnet. Quando executadas (Run), o Data Flow pode usar a rede padrão da OCI ou ser configurado para usar subnets específicas. Para o Hackathon, a rede padrão é suficiente.

---

## Validação da Fase 4

### Fluxo completo

```bash
# 1. Upload dos scripts (pré-requisito)
cd mig_oci/data_upload
./upload_scripts.sh

# 2. Aplicar Fase 4
cd ../terraform/scripts
./apply_phase.sh 4
```

### Validação no Console OCI

1. Menu ☰ > Analytics & AI > Data Flow
2. Selecionar compartment `hackathon-2025` > `compute`
3. Ver 4 applications:

| Name | Language | Spark version | Application type |
|------|----------|:-:|:-:|
| hackathon-2025-dataflow-bronze | PYTHON | 3.5.0 | BATCH |
| hackathon-2025-dataflow-silver | PYTHON | 3.5.0 | BATCH |
| hackathon-2025-dataflow-gold | PYTHON | 3.5.0 | BATCH |
| hackathon-2025-dataflow-abt | PYTHON | 3.5.0 | BATCH |

### Validação via Terraform

```bash
cd mig_oci/terraform/environments/prod
terraform output dataflow_applications
# Mostra: bronze_id, silver_id, gold_id, abt_id (OCIDs)
```

### Validação via OCI CLI

```bash
# Listar applications no compartment compute
oci data-flow application list \
  --compartment-id $(terraform output -raw compartment_ids | jq -r '.compute') \
  --query 'data[].{"name":"display-name","id":"id"}' \
  --output table
```

---

## Erros Comuns e Soluções

### Fase 4

| Erro | Causa | Solução |
|------|-------|---------|
| `FILE_URL_INVALID` | Script não existe no bucket landing-zone | Executar `./upload_scripts.sh` antes de `./apply_phase.sh 4` |
| `NotAuthorizedOrNotFound` (compartment) | Compartment compute não existe | Verificar que Fase 1 foi aplicada |
| `NotAuthorizedOrNotFound` (bucket) | Bucket landing-zone não existe ou sem policy | Verificar Fase 3 + policy `dataflow-service` (Fase 1) |
| `InvalidParameter` (spark_version) | Versão Spark não suportada na região | Usar `3.5.0` ou `3.2.1` (verificar disponibilidade) |
| `ServiceLimitExceeded` | OCPUs acima do limite da tenancy | Reduzir `dataflow_*_ocpu` ou `dataflow_*_executors` no `terraform.tfvars` |
| Warning `Permissions too open` (.pem) | Chave privada com permissões amplas (WSL + NTFS) | Inofensivo no WSL. Ignorar ou `export OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True` |
| `upload_scripts.sh` sem output | OCI CLI não instalado + `2>/dev/null` | Instalar OCI CLI: `oci setup config` |

### Problema: `apply_phase.sh 4` deu "sucesso" mas sem recursos novos

**Sintoma:** O Terraform reportou `Apply complete! Resources: 0 added, 0 changed, 0 destroyed.`

**Causa:** O bloco `module "compute"` estava **comentado** no `main.tf`. O Terraform não tinha nada novo para criar — apenas re-aplicou as Fases 1-3 existentes.

**Solução:** Verificar que o bloco `module "compute" { ... }` está **descomentado** no `main.tf`:
```bash
grep -A2 'module "compute"' mig_oci/terraform/environments/prod/main.tf
# Deve mostrar linhas SEM o prefixo "#"
```

---

## Lições Aprendidas

### 1. OCI Data Flow valida `file_uri` na criação (não no Run)

**Expectativa:** O Data Flow aceitaria qualquer URL no `file_uri` e só validaria quando executasse um Run (validação "lazy").

**Realidade:** A API faz validação **eager** — verifica se o objeto existe no bucket no momento de criar a Application. Sem o script, retorna `FILE_URL_INVALID`.

**Impacto:** Os scripts precisam ser enviados para o bucket **ANTES** de rodar `terraform apply`. Isso cria uma dependência entre upload (OCI CLI) e infra (Terraform):

```
Fluxo correto:
  Upload scripts → terraform apply → Airflow runs

Fluxo errado (falha):
  terraform apply (FILE_URL_INVALID!) → Upload scripts → ???
```

**Diferença com outros clouds:**
- **AWS Glue:** Aceita qualquer S3 path na definição, valida só na execução (lazy)
- **OCI Data Flow:** Valida na criação (eager) — mais seguro, mas requer scripts antes da infra
- **Databricks Jobs:** Valida na criação se o notebook existe no workspace (eager)

### 2. OCI CLI é necessário além do Terraform

Até a Fase 3, apenas o Terraform era necessário para gerenciar a infraestrutura. Na Fase 4, o **OCI CLI** se tornou necessário para fazer upload de scripts antes da criação das Applications.

**Credenciais são as mesmas:** O OCI CLI usa os mesmos valores do `terraform.tfvars` (user_ocid, tenancy_ocid, fingerprint, private_key_path, region), configurados via `oci setup config`.

### 3. Warnings de permissão da chave .pem no WSL são inofensivos

O OCI CLI emite warnings sobre permissões da chave `.pem` serem "too open". No WSL, arquivos em `/mnt/d/` (filesystem Windows/NTFS) não suportam permissões Unix granulares. O `chmod 600` não tem efeito real.

**Soluções:**
- Ignorar (não afeta funcionamento)
- Suprimir: `export OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True`
- Mover a chave para filesystem Linux: `cp /mnt/d/.../chave.pem ~/.oci/` e ajustar os caminhos

### 4. Scripts placeholder permitem desacoplar infra e código

A estratégia de usar scripts placeholder (mínimos) permite:
1. **Fase 4:** Criar a infra (Applications) com placeholders
2. **Fase 6:** Substituir placeholders pelos scripts reais
3. **Testes:** Validar que o Data Flow funciona executando um Run do placeholder

Isso segue a mesma filosofia incremental das fases anteriores: cada etapa é validada antes de avançar.

---

## Estrutura Atual do Projeto (Após Fase 4)

```
mig_oci/
├── .gitignore                                    # Proteção de credenciais
├── README.md                                     # Guia rápido
│
├── terraform/
│   ├── modules/
│   │   ├── iam/                                  # ✅ Fase 1 (aplicado)
│   │   │   ├── main.tf                           # 6 compartments + 4 groups + 6 policies
│   │   │   ├── variables.tf                      # tenancy_ocid, project_name, tags
│   │   │   └── outputs.tf                        # 6 compartment IDs + 4 group IDs
│   │   ├── network/                              # ✅ Fase 2 (aplicado)
│   │   │   ├── main.tf                           # VCN + 3 gateways + 2 RTs + 2 SLs + 3 subnets
│   │   │   ├── variables.tf                      # compartment_id, CIDRs, project_name
│   │   │   └── outputs.tf                        # vcn_id, 3 subnet IDs, sgw_id
│   │   ├── storage/                              # ✅ Fase 3 (aplicado)
│   │   │   ├── main.tf                           # 6 buckets + namespace data source
│   │   │   ├── variables.tf                      # compartment_id, project_name, lifecycle
│   │   │   └── outputs.tf                        # namespace + 7 bucket names
│   │   ├── compute/                              # ✅ Fase 4 (aplicado)
│   │   │   ├── main.tf                           # 4 Data Flow applications
│   │   │   ├── variables.tf                      # compartment_id, namespace, buckets, OCPUs
│   │   │   └── outputs.tf                        # 4 application IDs
│   │   └── security/                             # ⏳ Fase 5 (opcional)
│   │
│   ├── environments/prod/
│   │   ├── versions.tf                           # ✅ Provider OCI v5.47.0
│   │   ├── backend.tf                            # ✅ State local
│   │   ├── main.tf                               # ✅ Orquestra IAM + Network + Storage + Compute
│   │   ├── variables.tf                          # ✅ Variáveis Fases 1-4
│   │   ├── outputs.tf                            # ✅ Outputs Fases 1-4
│   │   └── terraform.tfvars.example              # ✅ Template configuração
│   │
│   └── scripts/
│       ├── init.sh                               # ✅ Validação inicial
│       └── apply_phase.sh                        # ✅ Apply incremental (Fases 1-4)
│
├── data_upload/                                  # ✅ Fase 4 (criado)
│   ├── scripts/
│   │   ├── bronze_job.py                         # Placeholder PySpark (Bronze)
│   │   ├── silver_job.py                         # Placeholder PySpark (Silver)
│   │   ├── gold_job.py                           # Placeholder PySpark (Gold)
│   │   └── abt_job.py                            # Placeholder PySpark (ABT)
│   └── upload_scripts.sh                         # Upload via OCI CLI
│
├── airflow/                                      # ⏳ Futuro
└── docs/
    ├── FASE_0_1_IMPLEMENTACAO.md                 # ✅ Documentação Fases 0-1
    ├── FASE_2_3_IMPLEMENTACAO.md                 # ✅ Documentação Fases 2-3
    └── FASE_4_IMPLEMENTACAO.md                   # ✅ Este documento
```

---

## Próximos Passos

| Fase | Status | O que faz | Dependência |
|------|--------|-----------|-------------|
| **Fase 0** | ✅ Concluída | Setup + validação credenciais | - |
| **Fase 1** | ✅ Aplicada | IAM (6 compartments, 4 groups, 6 policies) | Fase 0 |
| **Fase 2** | ✅ Aplicada | Network (VCN, 3 subnets, 3 gateways) | Fase 1 |
| **Fase 3** | ✅ Aplicada | Storage (6 buckets Object Storage) | Fase 1 |
| **Fase 4** | ✅ Aplicada | Compute (4 Data Flow applications) | Fases 1, 3 |
| **Fase 5** | ⏳ Opcional | Security (Vault + Master Key) | Fase 1 |
| **Fase 6** | ⏳ Aguardando | Upload de dados + scripts reais | Fases 3, 4 |

**Para avançar (Fase 6 — Upload de dados e scripts reais):**
1. Adaptar scripts PySpark do Databricks para OCI:
   - Substituir paths `/Volumes/hackathon_2025/default/` → `oci://hackathon-2025-*@namespace/`
   - Substituir `spark.read.table()` → `spark.read.parquet("oci://...")`
   - Remover dependências de Unity Catalog
2. Upload dos scripts reais: `./upload_scripts.sh` (sobrescreve placeholders)
3. Upload dos dados brutos para o bucket landing-zone
4. Testar um Run via Console OCI (Data Flow > Applications > Bronze > Create Run)

**Opcional (Airflow):**
```bash
# Usar os IDs das applications para configurar DAGs
terraform output -json dataflow_applications
# {
#   "bronze_id": "ocid1.dataflowapplication...",
#   "silver_id": "ocid1.dataflowapplication...",
#   "gold_id":   "ocid1.dataflowapplication...",
#   "abt_id":    "ocid1.dataflowapplication..."
# }
```

---

## Glossário (Novos Termos)

| Termo | Significado |
|-------|-------------|
| **Data Flow** | Serviço OCI gerenciado para executar jobs Apache Spark (equivalente ao Databricks Jobs) |
| **Application** | Definição/configuração de um job Data Flow (script, shape, executors) — não executa nada |
| **Run** | Execução real de uma Application. Cada Run provisiona recursos, executa e desliga |
| **Shape** | Tipo de máquina virtual. `VM.Standard.E4.Flex` permite configurar OCPUs/memória |
| **OCPU** | Oracle CPU — equivalente a 1 vCPU. Unidade de processamento na OCI |
| **Executor** | Worker node do Spark que processa dados em paralelo. Mais executors = mais paralelismo |
| **Driver** | Nó coordenador do Spark. Distribui tarefas para os executors e coleta resultados |
| **Spark Version** | Versão do Apache Spark. OCI Data Flow suporta 3.2.1 e 3.5.0 |
| **file_uri** | URL do script PySpark no Object Storage. Formato: `oci://bucket@namespace/path/script.py` |
| **logs_bucket_uri** | Onde os logs do Spark são salvos após a execução |
| **warehouse_bucket_uri** | Armazenamento temporário usado pelo Spark durante a execução |
| **Placeholder** | Script mínimo que satisfaz a validação do Data Flow. Substituído pelo real na Fase 6 |
| **OCI CLI** | Command Line Interface da Oracle Cloud. Usado para upload de arquivos e operações manuais |
| **Validação eager** | Verificação que acontece imediatamente (na criação), não na execução posterior |
