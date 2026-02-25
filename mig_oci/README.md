# Migração OCI - Hackathon PodAcademy 2025

Migração do projeto de Databricks para Oracle Cloud Infrastructure (OCI) usando Terraform e OCI Data Flow (Spark gerenciado).

## Status das Fases

| Fase | Status | Recursos |
|------|--------|----------|
| **Fase 0** | ✅ Concluída | Setup: Provider OCI v5.47.0, backend local, credenciais validadas |
| **Fase 1** | ✅ Aplicada | IAM: 6 compartments, 4 grupos, políticas de acesso |
| **Fase 2** | ✅ Aplicada | Network: VCN, 3 subnets, 3 gateways, 2 route tables, 2 security lists |
| **Fase 3** | ✅ Re-aplicada | Storage: 7 buckets — `pipeline-ops` criado (scripts/libs/logs separados da landing-zone) |
| **Fase 4** | ✅ Re-aplicada | Compute: 21 Data Flow Applications com `file_uri`, logs e warehouse → `pipeline-ops` |
| **Fase 5** | ⏳ Opcional | Security: Vault + Master Key |
| **Fase 6A** | ✅ Concluída | Landing → Bronze: 6 scripts adaptados, testados e executados no Data Flow |
| **Fase 6B** | ⏳ Em andamento | Silver: todos os 6 scripts com padrão opt_z (principal) + opt_optimize (standby). Gold/ABT aguardando |

---

## Quick Start

### 1. Configurar Credenciais OCI

```bash
cd mig_oci/terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars
# Editar terraform.tfvars com suas credenciais OCI
```

### 2. Inicializar e Aplicar

```bash
cd mig_oci/terraform/scripts
./init.sh              # Validar credenciais + inicializar
./apply_phase.sh 1     # Aplicar fase específica
```

### 3. Upload de Scripts

```bash
cd mig_oci/data_upload
./upload_scripts.sh    # Envia 21 scripts + utils.zip para o bucket pipeline-ops
```

> **Nota:** Executar `./apply_phase.sh 3` antes para criar o bucket `pipeline-ops`.

---

## Estrutura do Projeto

```
mig_oci/
├── terraform/
│   ├── modules/
│   │   ├── iam/        # Compartments, grupos, políticas
│   │   ├── network/    # VCN, subnets, gateways, security lists
│   │   ├── storage/    # 6 buckets Object Storage
│   │   ├── compute/    # 21 Data Flow Applications (for_each dinâmico)
│   │   └── security/   # Vault + Keys (opcional)
│   ├── environments/prod/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── terraform.tfvars.example
│   └── scripts/
│       ├── init.sh              # Inicializar + validar credenciais
│       └── apply_phase.sh       # Aplicar fase específica (1-5)
├── data_upload/
│   ├── scripts/                 # 21 scripts PySpark (versão principal — padrão opt_z)
│   │   ├── bronze_*.py          # 6 scripts Bronze (Landing → Bronze)
│   │   ├── silver_*.py          # 6 scripts Silver opt_z (sem cache, coalesce dinâmico)
│   │   ├── gold_*.py            # 3 scripts Gold (Silver → Gold features)
│   │   ├── abt_*.py             # 6 scripts ABT (Gold → ABT v1-v6)
│   │   └── opc_standby/         # Versões alternativas por script:
│   │       ├── silver_*_original.py     # Versão pré-otimização (referência)
│   │       ├── silver_*_opt_z.py        # Cópia da versão promovida
│   │       └── silver_*_opt_optimize.py # Alternativa com Delta OPTIMIZE + VACUUM
│   └── upload_scripts.sh        # Upload scripts para bucket pipeline-ops
├── airflow/                     # Orquestração (configurar após Terraform)
└── docs/
    ├── FASE_0_1_IMPLEMENTACAO.md       # Setup, IAM, conceitos OCI
    ├── FASE_2_3_IMPLEMENTACAO.md       # Network, Storage, lições aprendidas
    ├── FASE_4_IMPLEMENTACAO.md         # Compute, Data Flow, upload scripts
    ├── FASE_6A_LANDING_BRONZE.md       # Pipeline Landing → Bronze (as-built)
    ├── FASE_6A_TROUBLESHOOTING.md      # Lições aprendidas (problemas/soluções)
    └── IAM_USUARIOS_EQUIPE.md          # Grupos, acessos e integrantes da equipe
```

---

## Pipeline de Dados (21 Data Flow Applications)

```
Landing Zone (OCI Object Storage)
    │
    ├── bronze_bureau.py    ─┐
    ├── bronze_telco.py      │
    ├── bronze_cadastro.py   ├─ Bronze Layer (6 apps) ✅ Testado
    ├── bronze_recarga.py    │
    ├── bronze_pagamento.py  │
    └── bronze_atraso.py    ─┘
           │
    ├── silver_bureau.py    ─┐
    ├── silver_telco.py      │
    ├── silver_cadastro.py   ├─ Silver Layer (6 apps) ⏳ recarga ✅ testado; demais prontos (opt_z)
    ├── silver_recarga.py    │
    ├── silver_pagamento.py  │
    └── silver_atraso.py    ─┘
           │
    ├── gold_recarga.py     ─┐
    ├── gold_pagamento.py    ├─ Gold Features (3 apps) ⏳ Aguardando
    └── gold_atraso.py      ─┘
           │
    ├── abt_v1_builder.py   ─┐
    ├── abt_v2_builder.py    │
    ├── abt_v3_builder.py    ├─ ABT Builders (6 apps) ⏳ Aguardando
    ├── abt_v4_builder.py    │
    ├── abt_v5_builder.py    │
    └── abt_v6_builder.py   ─┘
```

**Padrão dos scripts (self-contained):**
- Sem `addPyFile` / sem `archive_uri` — funções utilitárias inline em cada script
- Namespace OCI recebido via `sys.argv[1]` (passado pelo Terraform via `arguments`)
- Paths: `oci://hackathon-2025-{layer}-layer@{namespace}/{fonte}/`
- `SparkSession.builder.appName("...").getOrCreate()` direto (sem configurar `fs.oci.*`)

---

## IAM — Equipe e Acessos

**Grupos criados (Fase 1):**

| Grupo | Acesso Principal |
|-------|-----------------|
| `hackathon-2025-administrators` | Acesso total ao projeto |
| `hackathon-2025-data-engineers` | Object Storage (manage) + Data Flow (manage) |
| `hackathon-2025-data-scientists` | Data Science Notebooks + leitura buckets |
| `hackathon-2025-developers` | Sandbox dev-teste (manage) + leitura produção |

> Detalhes completos, integrantes e matriz de acessos: `docs/IAM_USUARIOS_EQUIPE.md`

---

## Divisão de Responsabilidades

| Ferramenta | O que faz | Quando |
|------------|-----------|--------|
| **Terraform** | Cria infraestrutura OCI (compartments, VCN, buckets, Data Flow apps) | Fases 0-5 |
| **upload_scripts.sh** | Envia scripts Python para o bucket **pipeline-ops** | Antes de cada deploy |
| **OCI Data Flow** | Executa jobs Spark gerenciados (Bronze/Silver/Gold/ABT) | Fase 6 |
| **Airflow** | Orquestra execução dos Data Flow jobs em sequência | Após Terraform |
| **Console OCI** | Testes manuais, monitoramento de runs, gestão de usuários | Durante toda migração |

---

## Lições Aprendidas (principais)

| Problema | Solução |
|----------|---------|
| `FILE_URL_INVALID` no Terraform | Scripts devem existir no bucket **antes** do `terraform apply` |
| `archive_uri` com ZIP simples falha | Exige `conda pack` — usar scripts self-contained no lugar |
| `addPyFile("oci://...")` não funciona | Race condition com Resource Principal — funções inline |
| `spark.hadoop.fs.oci.*` é propriedade reservada | Data Flow configura internamente — não incluir no Terraform |
| `KryoSerializer` quebra autenticação OCI | Incompatível com `X509FederationClient` — não usar no Data Flow |
| `logs_bucket_uri` obrigatório | Sem ele, Data Flow procura bucket `dataflow-logs` inexistente |
| Erro X509 pode indicar dados ausentes | Verificar se os dados existem no bucket antes de debugar auth |
| `executeCompaction()` gera arquivos de ~1GB | Default Delta open-source é 1GB — setar `spark.databricks.delta.targetFileSize` para 128MB antes do OPTIMIZE |
| Delta `mode("overwrite")` acumula ghost files | Executar `VACUUM` após cada overwrite para limpar arquivos antigos |
| Silver opt_z — arquivo muito grande (coalesce=1) | `BYTES_PER_ROW_ESTIMATE` muito baixo — calibrar com `BYTES = tamanho_MB * 1024*1024 / count` |

> Documentação detalhada: `docs/FASE_6A_TROUBLESHOOTING.md`

---

## Custos Estimados (30 dias desenvolvimento)

| Serviço | Custo |
|---------|-------|
| Storage (295 GB) | $29 |
| Data Flow (3 runs completos) | $715 |
| Data Science Notebooks | $34 |
| Network | $54 |
| Airflow (self-hosted VM) | $183 |
| **Total desenvolvimento** | **~$1,015** |
| **Operacional mensal (pós-hackathon)** | **~$350** |

---

## Documentação

| Documento | Conteúdo |
|-----------|----------|
| `docs/FASE_0_1_IMPLEMENTACAO.md` | Setup, IAM, conceitos OCI (compartments, groups, policies) |
| `docs/FASE_2_3_IMPLEMENTACAO.md` | Network (VCN, subnets), Storage (buckets), lições aprendidas |
| `docs/FASE_4_IMPLEMENTACAO.md` | Compute (Data Flow applications), upload de scripts |
| `docs/FASE_6A_LANDING_BRONZE.md` | Pipeline Landing → Bronze — as-built, funcionando |
| `docs/FASE_6A_TROUBLESHOOTING.md` | Problemas encontrados e soluções definitivas |
| `docs/IAM_USUARIOS_EQUIPE.md` | Grupos, acessos, integrantes e matriz de permissões |
| `data_upload/scripts/opc_standby/COMPARATIVO_VERSOES_SILVER_RECARGA.md` | Benchmark das versões do silver_recarga (vencedor: opt_z, 9m44s) |
| `docs/IAM_USUARIOS_EQUIPE.md` | Grupos, acessos, integrantes e matriz de permissões da equipe |

---

## Troubleshooting Rápido

### Erro: `FILE_URL_INVALID` no Terraform
```bash
cd mig_oci/data_upload
./upload_scripts.sh   # Fazer upload dos scripts ANTES do terraform apply
```

### Erro: Quota insuficiente (E4/E3 = 0 no free tier)
> No Console OCI: Data Flow → Application → Run → Enable Autoscaling

### Erro: Permissão negada no Data Flow
> Verificar se a policy `dataflow-service-policy` foi aplicada (Fase 1)

### Warning de permissões da chave `.pem` no WSL
> Inofensivo no WSL/NTFS. `oci setup repair-file-permissions` não funciona em `/mnt/d` — ignorar.

---

## Referências

- **CLAUDE.md:** `.claude/CLAUDE.md` (quick reference geral do projeto)
- **Guia OCI para o time:** `docs/architecture/GUIA_ARQUITETURA_OCI.md`
- **Arquitetura completa:** `docs/architecture/OCI_ARCHITECTURE.md`
