# Inventário de Recursos OCI — Entregável E (Complemento)

Levantamento completo dos recursos Oracle Cloud Infrastructure utilizados no projeto.

**Região:** `sa-saopaulo-1` (São Paulo)
**IaC:** Terraform (OCI Provider v5.x)
**Estimador:** [OCI Cost Estimator](https://www.oracle.com/br/cloud/costestimator.html)

---

## 1. Resumo Geral

| Categoria | Quantidade | Recursos |
|-----------|-----------|----------|
| **IAM** | 16 | 7 compartments, 4 groups, 5 policies, 2 dynamic groups |
| **Network** | 12 | 1 VCN, 3 gateways, 2 route tables, 3 security lists, 3 subnets |
| **Storage** | 7 | 7 buckets Object Storage |
| **Compute** | 23 | 21 Data Flow apps (Spark) + 2 VMs (Start/Stop) |
| **Total** | **~58 recursos** | Todos gerenciados via Terraform |

> **Nota:** Recursos de Data Science (Notebook Sessions) não estão incluídos neste levantamento por serem exclusivamente de teste/exploração, não compondo o ambiente de produção.

---

## 2. Compute — VMs (Start/Stop)

Ambas as VMs operam em modo **Start/Stop** — são ligadas sob demanda e desligadas após uso. Custo zero quando STOPPED.

| VM | Shape | OCPUs | RAM (GB) | Subnet | Runs/mês | Tempo/run | Horas/mês | Custo/mês (USD) |
|----|-------|-------|----------|--------|----------|-----------|-----------|-----------------|
| **Airflow** (orquestração) | VM.Standard.E3.Flex | 1 | 16 | Pública | 1 | ~2h | **2** | R$ 0,81 |
| **Modelo Scoring** | VM.Standard.E5.Flex | 2 | 32 | Privada | 1 | ~10min | **1** | R$ 0,70 |
| | | | | | | **Subtotal VMs** | **3 h/mês** | **R$ 1,51** |

### Detalhes das VMs

**Airflow VM:**
- SO: Oracle Linux 8
- Software: Docker Compose (Airflow 2.8.0 + PostgreSQL, LocalExecutor)
- Autenticação OCI: Instance Principal (Dynamic Group)
- Acesso: SSH (22) + Airflow UI (8080)
- Boot: Docker Compose sobe automaticamente via systemd (~1-2 min)

**Modelo Scoring VM:**
- SO: Oracle Linux 8
- Software: Python 3.11, LightGBM 3.3.5, pandas, OCI SDK
- Autenticação OCI: Instance Principal (Dynamic Group)
- Acesso: Via jump host (Airflow VM → SSH interno pela VCN)
- Sem IP público (subnet privada)

### Ciclo de vida operacional

```
VM STOPPED ($0)
    │  Operador liga via OCI Console/CLI (ou OCI Functions futuro)
    ▼
VM START (~2 min boot)
    │  Docker/Python sobem automaticamente
    ▼
EXECUÇÃO (Airflow: ~3h | Modelo: ~1h)
    │  Pipeline ETL + Scoring
    ▼
VM STOP (automático via DAG ou manual)
    │
    ▼
VM STOPPED ($0)
```

---

## 3. Compute — Data Flow (Apache Spark Gerenciado)

**21 aplicações** organizadas em 4 grupos. Spark 3.5.0 + Delta Lake.

### 3.1 Bronze Layer — 6 apps (execução paralela)

| App | Script | Driver Shape | Driver OCPUs | Executor Shape | Executor OCPUs | Min/Max Exec | Tempo est. |
|-----|--------|-------------|-------------|----------------|---------------|-------------|-----------|
| bronze-bureau | `bronze_bureau.py` | VM.Standard2.1 | 1 | VM.Standard2.1 | 1 | 2–8 | ~10 min |
| bronze-telco | `bronze_telco.py` | VM.Standard2.2 | 2 | VM.Standard2.2 | 2 | 2–8 | ~10 min |
| bronze-cadastro | `bronze_cadastro.py` | VM.Standard.A1.Flex | 2 | VM.Standard.A1.Flex | 2 | 1–8 | ~10 min |
| bronze-atraso | `bronze_atraso.py` | VM.Standard3.Flex | 1 | VM.Standard3.Flex | 2 | 2–8 | ~10 min |
| bronze-pagamento | `bronze_pagamento.py` | VM.Standard.E3.Flex | 1 | VM.Standard.E3.Flex | 2 | 2–8 | ~10 min |
| bronze-recarga | `bronze_recarga.py` | VM.Standard.E4.Flex | 1 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |

### 3.2 Silver Layer — 6 apps (execução paralela)

| App | Script | Driver Shape | Driver OCPUs | Executor Shape | Executor OCPUs | Min/Max Exec | Tempo est. |
|-----|--------|-------------|-------------|----------------|---------------|-------------|-----------|
| silver-bureau | `silver_bureau.py` | VM.Standard2.1 | 1 | VM.Standard2.1 | 1 | 2–8 | ~10 min |
| silver-telco | `silver_telco.py` | VM.Standard2.2 | 2 | VM.Standard2.2 | 2 | 2–8 | ~10 min |
| silver-cadastro | `silver_cadastro.py` | VM.Standard.A1.Flex | 2 | VM.Standard.A1.Flex | 2 | 1–8 | ~10 min |
| silver-recarga | `silver_recarga.py` | VM.Standard.E4.Flex | 1 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |
| silver-pagamento | `silver_pagamento.py` | VM.Standard.E3.Flex | 1 | VM.Standard.E3.Flex | 2 | 2–8 | ~10 min |
| silver-atraso | `silver_atraso.py` | VM.Standard3.Flex | 1 | VM.Standard3.Flex | 2 | 2–8 | ~10 min |

### 3.3 Gold Features — 3 apps (execução paralela)

| App | Script | Driver Shape | Driver OCPUs | Executor Shape | Executor OCPUs | Min/Max Exec | Tempo est. |
|-----|--------|-------------|-------------|----------------|---------------|-------------|-----------|
| gold-recarga | `gold_recarga.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~15 min |
| gold-pagamento | `gold_pagamento.py` | VM.Standard3.Flex | 2 | VM.Standard3.Flex | 2 | 2–8 | ~15 min |
| gold-atraso | `gold_atraso.py` | VM.Standard.A1.Flex | 2 | VM.Standard.A1.Flex | 2 | 2–8 | ~15 min |

### 3.4 ABT Builders — 6 apps (execução sequencial v1→v6)

| App | Script | Driver Shape | Driver OCPUs | Executor Shape | Executor OCPUs | Min/Max Exec | Tempo est. |
|-----|--------|-------------|-------------|----------------|---------------|-------------|-----------|
| abt-v1 | `abt_v1_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |
| abt-v2 | `abt_v2_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |
| abt-v3 | `abt_v3_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |
| abt-v4 | `abt_v4_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~10 min |
| abt-v5 | `abt_v5_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~20 min |
| abt-v6 | `abt_v6_builder.py` | VM.Standard.E4.Flex | 2 | VM.Standard.E4.Flex | 2 | 2–8 | ~20 min |

### 3.5 Resumo Data Flow — Estimativa de Consumo Mensal

| Métrica | Valor |
|---------|-------|
| Total de apps | 21 |
| Shapes utilizados | 6 famílias (evitar conflito de quota) |
| Execuções/mês | 1 |
| OCPU-horas estimadas/run | ~3 |
| **OCPU-horas/mês** | **~3** |
| **Custo/mês** | **R$71,00** |

> **Estratégia de shapes:** Cada grupo paralelo (Bronze, Silver, Gold) usa famílias de shapes diferentes (Standard2.1, Standard2.2, A1.Flex, Standard3.Flex, E3.Flex, E4.Flex) para que múltiplos apps rodem simultaneamente sem conflito de quota no free tier. Apps sequenciais (ABT) compartilham o mesmo shape.

---

## 4. Storage — Object Storage Standard

Todos os buckets: **NoPublicAccess**, **Versioning ON**, compartment `storage`.

| Bucket | Conteúdo | Tamanho Estimado | Custo/mês (USD) |
|--------|----------|-----------------|-----------------|
| `hackathon-2025-landing-zone` | CSVs/Parquet brutos (entrada do pipeline) | ~10 GB | R$ _______ |
| `hackathon-2025-pipeline-ops` | 21 scripts PySpark + utils.zip + logs | ~250 Mb | R$ _______ |
| `hackathon-2025-bronze-layer` | Delta tables (schema-on-read) | ~10 GB | R$ _______ |
| `hackathon-2025-silver-layer` | Delta tables (tipado, dedup) | ~10 GB | R$ _______ |
| `hackathon-2025-gold-layer` | ABT v1-v6 + feature tables (Recarga, Pagamento, Atraso) | ~5 GB | R$ _______ |
| `hackathon-2025-models` | PKL, predições OOT, métricas JSON | ~200 Mb | R$ _______ |
| `hackathon-2025-tfstate` | Terraform state (standby) | ~1 MB | R$ _______ |
| **Total atual** | | **~35 GB** | **R$32,00** |


> **Nota sobre limpeza:** O pipeline usa Delta Lake com `.mode("overwrite")`, que gera arquivos órfãos. O `abt_v6_builder.py` já executa `VACUUM(retentionHours=0)` após o write. Para manter o storage próximo de ~87 GB, recomenda-se adicionar VACUUM nas demais camadas (Bronze, Silver, Gold Features).

---

## 5. Network

| Recurso | Quantidade | Cobrança | Horas/mês | Custo/mês (USD) |
|---------|-----------|----------|-----------|-----------------|
| VCN (`10.0.0.0/16`) | 1 | Gratuito | — | R$0 |
| Internet Gateway | 1 | Gratuito (cobra egress) | — | R$0 |
| **NAT Gateway** | 1 | **Por hora** | 730 (sempre ativo) | **R$ _______** |
| Service Gateway | 1 | Gratuito | — | R$0 |
| Route Tables | 2 | Gratuito | — | R$0 |
| Security Lists | 3 | Gratuito | — | R$0 |
| Subnets | 3 | Gratuito | — | R$0 |
| **Subtotal Network** | | | | **R$ _______** |

> **NAT Gateway:** Necessário para que as subnets privadas (Data Flow, Modelo VM) acessem internet (pip install, APIs externas). Cobra por hora mesmo sem tráfego. Alternativa futura: desligar NAT quando não houver execução (economia de ~$22/mês), mas exige automação.

> **Service Gateway:** Permite acesso direto ao Object Storage pela rede interna da Oracle, sem passar pela internet. Tráfego gratuito — é por onde o Data Flow e o Modelo VM leem/escrevem nos buckets.

---

## 6. IAM — Identidade e Segurança

Sem custo direto, mas essencial para a arquitetura de segurança.

### 6.1 Compartments (7)

| Compartment | Hierarquia | Propósito |
|-------------|-----------|-----------|
| `hackathon-2025` | Raiz | Compartment raiz do projeto |
| `network` | hackathon-2025/ | VCN, Subnets, Gateways |
| `storage` | hackathon-2025/ | Object Storage (7 buckets) |
| `compute` | hackathon-2025/ | Data Flow, VMs |
| `data` | hackathon-2025/ | Reservado (Data Catalog futuro) |
| `security` | hackathon-2025/ | Reservado (Vault futuro) |
| `dev-teste` | hackathon-2025/ | Sandbox isolado para o time |

### 6.2 Groups (4)

| Group | Permissões |
|-------|-----------|
| `hackathon-2025-administrators` | Acesso total ao projeto |
| `hackathon-2025-data-engineers` | Gerenciam pipeline (Data Flow + Storage) |
| `hackathon-2025-data-scientists` | Notebooks e modelos (somente leitura em gold) |
| `hackathon-2025-developers` | Sandbox dev-teste |

### 6.3 Dynamic Groups (2) — Instance Principal

| Dynamic Group | Matching Rule | Permissões |
|--------------|--------------|-----------|
| `airflow-dynamic-group` | `instance.id = '<Airflow VM OCID>'` | manage dataflow-family, object-family, instance-family |
| `modelo-scoring-dynamic-group` | `instance.id = '<Modelo VM OCID>'` | manage object-family, read virtual-network-family |

> **Nota:** Dynamic Groups de Data Science Notebooks (cientistas e engenheiros) existem no Terraform mas são exclusivamente para teste/exploração, não incluídos neste levantamento de produção.

### 6.4 Policies (5 de produção)

| Policy | Nível | Escopo |
|--------|-------|--------|
| Admin | Compartment | manage all-resources |
| Data Engineers | Compartment | manage object-family + dataflow-family |
| DataFlow Service | Tenancy | Serviço Data Flow acessa rede e storage |
| Airflow Instance Principal | Tenancy | VM Airflow gerencia Data Flow + Storage + Compute |
| Modelo Instance Principal | Tenancy | VM Modelo acessa Object Storage |

---

## 7. Topologia de Rede

```
                        INTERNET
                           │
                    ┌──────▼──────┐
                    │ Internet    │
                    │ Gateway     │
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │  VCN: 10.0.0.0/16       │
              │  hackathon-2025-vcn      │
              │                          │
              │  ┌─── public-subnet ───┐ │        ┌──────────┐
              │  │  10.0.1.0/24        │ │        │ NAT      │
              │  │                     │ │        │ Gateway  │
              │  │  Airflow VM         │ │        └────┬─────┘
              │  │  E3.Flex 1/16GB     │ │             │
              │  │  :8080 (UI)         │ │        ┌────▼─────┐
              │  │  :22 (SSH)          │ │        │ Service  │
              │  └─────────────────────┘ │        │ Gateway  │
              │                          │        └────┬─────┘
              │  ┌── private-data ─────┐ │             │
              │  │  10.0.2.0/24        │◄├─────────────┘
              │  │                     │ │
              │  │  Data Flow (21 apps)│ │
              │  │  Modelo VM          │ │
              │  │  E5.Flex 2/32GB     │ │
              │  └─────────────────────┘ │
              │                          │
              │  ┌── private-app ──────┐ │
              │  │  10.0.3.0/24        │ │
              │  │  (reservada)        │ │
              │  └─────────────────────┘ │
              └──────────────────────────┘

              Object Storage (7 buckets)
              ┌───────────────────────────┐
              │ landing │ bronze │ silver  │
              │ gold    │ models │ ops     │
              │ tfstate                    │
              └───────────────────────────┘
              Acesso via Service Gateway
              (tráfego interno, gratuito)
```

---

## 8. Custo Total Mensal — Produção

| Componente | Recurso | Horas ou GB | Custo/mês (USD) |
|-----------|---------|-------------|-----------------|
| Compute | Airflow VM (E3.Flex 1 OCPU/16 GB) | 2 h/mês | R$ 0,81 |
| Compute | Modelo VM (E5.Flex 2 OCPUs/32 GB) | 1 h/mês | R$ 0,70 |
| Data Flow | 21 apps × 1 run (Spark gerenciado) | ~50 OCPU-h/mês | R$71,00 |
| Storage | Object Storage Standard | 35 GB (atual) | R$32,00 |
| Storage | Object Storage Standard (com margem) | 350 GB | R$ 76,00 |
| Network | NAT Gateway | 730 h/mês | R$ _______ |
| Network | Tráfego egress (estimado) | ~10 GB/mês | R$ _______ |
| **TOTAL (atual)** | | | **R$ 105,00** |
| **TOTAL (com margem 350GB)** | | | **R$ 150,00** |

---

## 9. Referências Terraform

| Módulo | Path | Recursos |
|--------|------|----------|
| IAM | `mig_oci/terraform/modules/iam/` | Compartments, Groups, Policies |
| Network | `mig_oci/terraform/modules/network/` | VCN, Subnets, Gateways, Security Lists |
| Storage | `mig_oci/terraform/modules/storage/` | 7 Buckets Object Storage |
| Compute | `mig_oci/terraform/modules/compute/` | 21 Data Flow Applications |
| Airflow | `mig_oci/terraform/modules/airflow/` | VM E3.Flex + Dynamic Group + Policy |
| Modelo VM | `mig_oci/terraform/modules/modelo_vm/` | VM E5.Flex + Dynamic Group + Policy |
| **Prod** | `mig_oci/terraform/environments/prod/` | main.tf, variables.tf, outputs.tf |

**Comandos:**
```bash
# Executar LOCAL — Inicializar e aplicar
cd mig_oci/terraform/scripts
./init.sh                # Validar credenciais + inicializar
./apply_phase.sh <FASE>  # Aplicar fase específica (1-8)
```

---

*Documento gerado em Março/2026 — Hackathon PodAcademy 2025*
*Fonte: Terraform IaC em `mig_oci/terraform/` (6 módulos, ~60 recursos)*
*Valores: consultar [OCI Cost Estimator](https://www.oracle.com/br/cloud/costestimator.html)*
