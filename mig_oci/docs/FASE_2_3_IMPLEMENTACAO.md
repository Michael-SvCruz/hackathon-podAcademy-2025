# Migração OCI - Fases 2 e 3: Network e Storage

## Contexto

Com a Fase 1 (IAM) concluída, temos os compartments organizacionais criados na OCI. As Fases 2 e 3 criam a **infraestrutura física** onde os dados e processamentos vão residir: a rede virtual (VCN) e os buckets de armazenamento (Object Storage).

**Pré-requisitos:**
- Fase 0 concluída (credenciais validadas)
- Fase 1 aplicada (compartments, groups, policies existem na OCI)

---

## Fase 2: Network (VCN + Subnets + Gateways)

**Objetivo:** Criar a rede virtual completa onde os recursos OCI vão se comunicar.

### Por que precisamos de rede na OCI?

Diferente do Databricks onde a rede é transparente, na OCI o Data Flow e o Data Science **precisam de subnets** para executar. Sem VCN configurada, os jobs Spark não rodam.

**Analogia:** A VCN é como o "prédio" do seu projeto na nuvem. As subnets são os "andares" (público, dados, aplicação), os gateways são as "portas de saída" (internet, serviços OCI), e as security lists são os "seguranças" que controlam quem entra e sai.

### Arquitetura de Rede

```
┌──────────────────────────────────────────────────────────┐
│  VCN: hackathon-2025-vcn (10.0.0.0/16)                  │
│                                                          │
│  ┌────────────────────────┐                              │
│  │ Public Subnet          │  Internet    ┌─────────────┐ │
│  │ 10.0.1.0/24            │─────────────→│   Internet  │ │
│  │ (Load Balancer futuro) │  Gateway     │   Gateway   │ │
│  └────────────────────────┘              └─────────────┘ │
│                                                          │
│  ┌────────────────────────┐              ┌─────────────┐ │
│  │ Private Data Subnet    │  NAT         │     NAT     │ │
│  │ 10.0.2.0/24            │─────────────→│   Gateway   │ │
│  │ (Data Flow + Data Sci) │  (só saída)  └─────────────┘ │
│  └────────────────────────┘                              │
│           │                              ┌─────────────┐ │
│           └──────────────────────────────│   Service   │ │
│                                          │   Gateway   │ │
│  ┌────────────────────────┐  (rede       └─────────────┘ │
│  │ Private App Subnet     │   interna                    │
│  │ 10.0.3.0/24            │   Oracle)                    │
│  │ (Scoring API futuro)   │                              │
│  └────────────────────────┘                              │
└──────────────────────────────────────────────────────────┘
```

### Os 3 Tipos de Gateway

| Gateway | Analogia | Direção | Para que serve |
|---------|----------|---------|----------------|
| **Internet Gateway** | Porta da frente | Entrada e saída | Acesso direto à internet (subnet pública) |
| **NAT Gateway** | Porta dos fundos | Somente saída | Subnets privadas acessam internet sem serem acessíveis de fora |
| **Service Gateway** | Túnel interno | Rede Oracle | Acesso direto ao Object Storage e outros serviços OCI, sem passar pela internet |

**Dica importante:** O Service Gateway é crítico para performance. Quando o Data Flow lê/escreve nos buckets Object Storage, o tráfego passa pelo Service Gateway (rede interna Oracle) — mais rápido e sem custo de tráfego NAT. Sem ele, o tráfego de dados iria pela internet pública, mais lento e mais caro.

### Subnets: Pública vs Privada

| Característica | Subnet Pública | Subnet Privada |
|----------------|:-:|:-:|
| IP público nos recursos | Sim | Não |
| Acessível da internet | Sim (via security list) | Não |
| Acessa a internet | Sim (Internet Gateway) | Sim (NAT Gateway, só saída) |
| Uso no projeto | Load Balancer (futuro) | Data Flow, Data Science, Scoring API |

**Por que subnets privadas para dados?** Segurança. Os jobs Spark processam dados sensíveis (CPF, scores de crédito). Colocá-los em subnet privada significa que ninguém da internet consegue acessá-los diretamente — apenas outros recursos dentro da VCN.

### Security Lists (Firewall)

As security lists controlam o tráfego de rede como um firewall:

**Security List Pública:**
- Entrada: SSH (porta 22), HTTP (80), HTTPS (443) — de qualquer origem
- Saída: todo tráfego permitido

**Security List Privada:**
- Entrada: apenas tráfego da própria VCN (10.0.0.0/16) — recursos internos se comunicam
- Saída: todo tráfego permitido (NAT Gateway filtra)

**Dica:** A security list privada bloqueia todo tráfego externo de entrada. Isso significa que ninguém fora da VCN consegue acessar os jobs Data Flow ou notebooks diretamente. O acesso é feito via Console OCI ou API (que usam autenticação IAM).

### Route Tables (Tabelas de Roteamento)

Route tables definem "para onde vai o tráfego":

**Route Table Pública:**
- `0.0.0.0/0` → Internet Gateway (todo tráfego externo vai direto)

**Route Table Privada:**
- `0.0.0.0/0` → NAT Gateway (tráfego internet, somente saída)
- `OCI Services CIDR` → Service Gateway (tráfego para serviços OCI, rede interna)

**Dica:** A route table privada tem 2 regras porque precisa distinguir entre tráfego internet (NAT) e tráfego para serviços OCI (Service Gateway). Quando o Data Flow acessa o Object Storage, o roteamento usa o Service Gateway automaticamente.

### O que foi criado (Fase 2)

| Arquivo | Propósito |
|---------|-----------|
| `terraform/modules/network/main.tf` | VCN + 3 gateways + 2 route tables + 2 security lists + 3 subnets |
| `terraform/modules/network/variables.tf` | Entradas: compartment_id, CIDRs, project_name, tags |
| `terraform/modules/network/outputs.tf` | Saídas: vcn_id, 3 subnet IDs, service_gateway_id |

**Recursos criados na OCI (11 recursos):**
- 1 VCN (`hackathon-2025-vcn`, CIDR 10.0.0.0/16)
- 3 Gateways (Internet, NAT, Service)
- 2 Route Tables (pública, privada)
- 2 Security Lists (pública, privada)
- 3 Subnets (pública, privada-dados, privada-app)

### Custo da Fase 2

**Subnets e security lists não custam nada.** Gateways têm custo mínimo:
- Internet Gateway: gratuito
- NAT Gateway: ~$0.028/hora (~$20/mês)
- Service Gateway: gratuito

**Dica:** Criamos as 3 subnets agora mesmo sem usar todas imediatamente porque subnets vazias têm custo zero. Alterar topologia de rede depois é muito mais trabalhoso do que criar a estrutura completa desde o início.

### Validação da Fase 2

```bash
cd mig_oci/terraform/scripts
./apply_phase.sh 2
```

**Validação no Console OCI:**
1. Menu ☰ > Networking > Virtual Cloud Networks
2. Selecionar compartment `hackathon-2025` > `network`
3. Ver `hackathon-2025-vcn` com 3 subnets, 3 gateways

**Validação via Terraform:**
```bash
cd mig_oci/terraform/environments/prod
terraform output network_ids
# Mostra: vcn_id, public_subnet_id, private_data_subnet_id, private_app_subnet_id
```

### Fluxo de Dependências: IAM → Network

```
Módulo IAM                    Módulo Network
┌──────────────────┐         ┌──────────────────┐
│                  │         │                  │
│ network_         │────────→│ network_         │
│ compartment_id   │         │ compartment_id   │
│                  │         │                  │
│ (onde criar)     │         │ (cria VCN aqui)  │
└──────────────────┘         └──────────────────┘
```

O módulo Network recebe o `network_compartment_id` do IAM para saber **em qual compartment** criar a VCN e os recursos de rede.

---

## Fase 3: Storage (Object Storage Buckets)

**Objetivo:** Criar os buckets Object Storage que armazenam os dados do pipeline Medallion.

### O que é Object Storage?

Object Storage é o serviço de armazenamento de arquivos da OCI — equivalente ao **S3 da AWS** ou ao **Azure Blob Storage**. Funciona como um "HD na nuvem" onde você armazena arquivos (objetos) organizados em "buckets" (pastas de nível superior).

**Equivalência com o Databricks:**
```
Databricks:  /Volumes/hackathon_2025/default/bronze/
OCI:         oci://hackathon-2025-bronze-layer@<namespace>/

Databricks:  /Volumes/hackathon_2025/default/silver/
OCI:         oci://hackathon-2025-silver-layer@<namespace>/

Databricks:  /Volumes/hackathon_2025/default/gold/
OCI:         oci://hackathon-2025-gold-layer@<namespace>/
```

### Namespace: Identificador Único da Tenancy

Cada tenancy OCI tem um **namespace** — um identificador globalmente único (ex: `grtu5abcd123`). Ele é necessário para construir as URLs de acesso aos buckets:

```
oci://<bucket-name>@<namespace>/path/to/file.parquet
```

**Dica:** O namespace é obtido automaticamente pelo Terraform usando um `data source` (consulta, não cria recurso). Por isso o plan mostrou "6 to add" e não "7 to add" — o data source não conta como recurso criado.

### Resource vs Data Source no Terraform

| Tipo | O que faz | Conta no plan? | Exemplo |
|------|-----------|:-:|---------|
| `resource` | **Cria** infraestrutura na OCI | Sim ("to add") | `oci_objectstorage_bucket` |
| `data` | **Lê** informação existente | Não | `oci_objectstorage_namespace` |

O `data source` é como um `SELECT` no banco — apenas lê, não modifica nada. No caso do storage, usamos `data.oci_objectstorage_namespace.current` para descobrir o namespace da tenancy.

### Estrutura de Buckets (Medallion Architecture)

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ landing-zone    │ →  │ bronze-layer    │ →  │ silver-layer    │ →  │ gold-layer      │
│                 │    │                 │    │                 │    │                 │
│ • Dados brutos  │    │ • Schema-on-read│    │ • Tipado        │    │ • ABT v1-v6     │
│ • Scripts .py   │    │ • Metadados     │    │ • Validado      │    │ • Features eng. │
│ • ~50 GB        │    │ • ~60 GB        │    │ • ~80 GB        │    │ • ~100 GB       │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘

┌─────────────────┐    ┌─────────────────┐
│ models          │    │ tfstate         │
│                 │    │                 │
│ • LightGBM     │    │ • State remoto  │
│ • Métricas     │    │ • Colaboração   │
│ • ~5 GB        │    │ • ~1 MB         │
└─────────────────┘    └─────────────────┘
```

| Bucket | Nome na OCI | Volume Est. | Conteúdo |
|--------|-------------|-------------|----------|
| **Landing Zone** | `hackathon-2025-landing-zone` | 50 GB | Dados brutos CSV/Parquet + scripts Python |
| **Bronze** | `hackathon-2025-bronze-layer` | 60 GB | Dados com metadados (Delta Lake) |
| **Silver** | `hackathon-2025-silver-layer` | 80 GB | Dados tipados e validados |
| **Gold** | `hackathon-2025-gold-layer` | 100 GB | ABT v1-v6 (614 colunas, 3.79M registros) |
| **Models** | `hackathon-2025-models` | 5 GB | Modelo LightGBM, métricas, feature importance |
| **TFState** | `hackathon-2025-tfstate` | ~1 MB | State remoto do Terraform |

### Configurações dos Buckets

Todos os buckets foram criados com:

- **`access_type = "NoPublicAccess"`** — Ninguém da internet consegue acessar. Apenas usuários autenticados via IAM ou serviços com policy (como Data Flow).

- **`versioning = "Enabled"`** — Cada vez que um arquivo é sobrescrito, a versão anterior é mantida. Funciona como "git para arquivos".

**Dica sobre Versioning:** Se durante a execução do pipeline algo der errado (ex: o job Gold Features gera dados corrompidos e sobrescreve o bucket), o versioning permite recuperar a versão anterior. No Hackathon com janela de 30 dias, essa é uma proteção muito importante — evita ter que re-processar todo o pipeline do zero.

### Lifecycle Policies (Opcional)

O módulo suporta lifecycle policies para arquivar dados antigos automaticamente (economizar custos). Está **desabilitado por padrão** (`enable_lifecycle_policies = false`).

**Por que desabilitado?** No Hackathon (30 dias), não faz sentido arquivar dados. Lifecycle policies são úteis em produção contínua, onde dados de meses atrás podem ser movidos para storage mais barato (Archive Storage).

### O que foi criado (Fase 3)

| Arquivo | Propósito |
|---------|-----------|
| `terraform/modules/storage/main.tf` | 6 buckets + 1 data source (namespace) + lifecycle opcional |
| `terraform/modules/storage/variables.tf` | Entradas: compartment_id, project_name, lifecycle flag, tags |
| `terraform/modules/storage/outputs.tf` | Saídas: namespace + 7 nomes de buckets |

**Recursos criados na OCI (6 recursos):**
- 6 buckets Object Storage (todos com versioning, sem acesso público)

### Custo da Fase 3

Object Storage na OCI custa ~$0.0255/GB/mês (Standard tier):

| Bucket | Volume Est. | Custo Mensal |
|--------|-------------|-------------|
| Landing Zone | 50 GB | ~$1.28 |
| Bronze | 60 GB | ~$1.53 |
| Silver | 80 GB | ~$2.04 |
| Gold | 100 GB | ~$2.55 |
| Models | 5 GB | ~$0.13 |
| TFState | ~0 GB | ~$0.00 |
| **Total** | **295 GB** | **~$7.53/mês** |

**Dica:** O custo do versioning é adicional — cada versão antiga ocupa espaço. Para o período do Hackathon (30 dias), o impacto é mínimo. Em produção, configure políticas para deletar versões antigas após X dias.

### Validação da Fase 3

```bash
cd mig_oci/terraform/scripts
./apply_phase.sh 3
```

**Validação no Console OCI:**
1. Menu ☰ > Storage > Buckets
2. Selecionar compartment `hackathon-2025` > `storage`
3. Ver 6 buckets com versioning habilitado

**Validação via Terraform:**
```bash
cd mig_oci/terraform/environments/prod
terraform output storage_info
# Mostra: namespace + 7 nomes de buckets
```

### Fluxo de Dependências: IAM → Storage

```
Módulo IAM                    Módulo Storage
┌──────────────────┐         ┌──────────────────┐
│                  │         │                  │
│ storage_         │────────→│ storage_         │
│ compartment_id   │         │ compartment_id   │
│                  │         │                  │
│ (onde criar)     │         │ (cria buckets    │
│                  │         │  aqui)           │
└──────────────────┘         └──────────────────┘
```

**Nota:** Storage não depende de Network (Fase 2). Teoricamente, Storage poderia ser aplicada em paralelo com Network, ambas dependem apenas da Fase 1 (IAM). Optamos pela sequência linear por simplicidade.

---

## Erros Comuns e Soluções

### Fase 2

| Erro | Causa | Solução |
|------|-------|---------|
| `Module not installed` | Novo módulo adicionado sem `terraform init` | O script `apply_phase.sh` já roda `init` automaticamente |
| `hashicorp/oci` provider instalado | Módulo sem `required_providers` | Adicionamos `terraform { required_providers { oci = { source = "oracle/oci" } } }` em cada módulo |
| `Service not available` | Service Gateway precisa de serviços disponíveis na região | São Paulo (`sa-saopaulo-1`) tem todos os serviços necessários |

### Fase 3

| Erro | Causa | Solução |
|------|-------|---------|
| `BucketAlreadyExists` | Nome de bucket já existe globalmente | Nomes são prefixados com `hackathon-2025-` para evitar conflitos |
| `NotAuthorizedOrNotFound` | Compartment não existe ou sem permissão | Verificar que Fase 1 foi aplicada com sucesso |
| Plan mostra "6 to add" (não 7) | `data source` (namespace) não conta como resource | Normal — `data` é consulta, não criação |

---

## Lições Aprendidas

### 1. Sempre declarar `required_providers` nos módulos

Na Fase 1, o módulo IAM não tinha `required_providers` declarado. O Terraform interpretou `oci_*` resources como vindos de `hashicorp/oci` (namespace antigo) em vez de `oracle/oci`. Solução: todo módulo agora inclui:

```hcl
terraform {
  required_providers {
    oci = {
      source = "oracle/oci"
    }
  }
}
```

### 2. `depends_on` para dependências implícitas

Na Fase 1, a policy `dataflow_service` foi criada no tenancy (nível superior) mas referencia o compartment `hackathon-2025` pelo **nome no texto** da statement. O Terraform não detectou essa dependência implícita e tentou criar a policy antes do compartment estar propagado. Solução:

```hcl
resource "oci_identity_policy" "dataflow_service" {
  # ...
  depends_on = [oci_identity_compartment.project]
}
```

**Regra:** Se um recurso referencia outro apenas em strings (não via `.id`), adicione `depends_on` explícito.

### 3. `terraform init` deve rodar sempre, não apenas na primeira vez

O script `apply_phase.sh` originalmente só rodava `init` se `.terraform/` não existisse. Mas ao adicionar novos módulos (como network, storage), o init precisa rodar novamente para baixar o código do módulo. Solução: o script agora sempre executa `terraform init -input=false` (idempotente).

---

## Estrutura Atual do Projeto (Após Fase 3)

```
mig_oci/
├── .gitignore                                    # Proteção de credenciais
├── README.md                                     # Guia rápido
│
├── terraform/
│   ├── modules/
│   │   ├── iam/                                  # ✅ Fase 1 (aplicado)
│   │   │   ├── main.tf                           # 6 compartments + 3 groups + 4 policies
│   │   │   ├── variables.tf                      # tenancy_ocid, project_name, tags
│   │   │   └── outputs.tf                        # 6 compartment IDs + 3 group IDs
│   │   ├── network/                              # ✅ Fase 2 (aplicado)
│   │   │   ├── main.tf                           # VCN + 3 gateways + 2 RTs + 2 SLs + 3 subnets
│   │   │   ├── variables.tf                      # compartment_id, CIDRs, project_name
│   │   │   └── outputs.tf                        # vcn_id, 3 subnet IDs, sgw_id
│   │   ├── storage/                              # ✅ Fase 3 (aplicado)
│   │   │   ├── main.tf                           # 6 buckets + namespace data source
│   │   │   ├── variables.tf                      # compartment_id, project_name, lifecycle
│   │   │   └── outputs.tf                        # namespace + 7 bucket names
│   │   ├── compute/                              # ⏳ Fase 4 (vazio)
│   │   └── security/                             # ⏳ Fase 5 (vazio)
│   │
│   ├── environments/prod/
│   │   ├── versions.tf                           # ✅ Provider OCI v5.47.0
│   │   ├── backend.tf                            # ✅ State local (migrar para remoto após Fase 3)
│   │   ├── main.tf                               # ✅ Orquestra IAM + Network + Storage
│   │   ├── variables.tf                          # ✅ Variáveis Fases 1-4
│   │   ├── outputs.tf                            # ✅ Outputs Fases 1-3
│   │   └── terraform.tfvars.example              # ✅ Template configuração
│   │
│   └── scripts/
│       ├── init.sh                               # ✅ Validação inicial
│       └── apply_phase.sh                        # ✅ Apply incremental (com init automático)
│
├── airflow/                                      # ⏳ Futuro
├── data_upload/                                  # ⏳ Futuro
└── docs/
    ├── FASE_0_1_IMPLEMENTACAO.md                 # ✅ Documentação Fases 0-1
    └── FASE_2_3_IMPLEMENTACAO.md                 # ✅ Este documento
```

---

## Próximos Passos

| Fase | Status | O que faz | Dependência |
|------|--------|-----------|-------------|
| **Fase 0** | ✅ Concluída | Setup + validação credenciais | - |
| **Fase 1** | ✅ Aplicada | IAM (6 compartments, 3 groups, 4 policies) | Fase 0 |
| **Fase 2** | ✅ Aplicada | Network (VCN, 3 subnets, 3 gateways) | Fase 1 |
| **Fase 3** | ✅ Aplicada | Storage (6 buckets Object Storage) | Fase 1 |
| **Fase 4** | ⏳ Aguardando | Compute (4 Data Flow applications) | Fases 1-3 |
| **Fase 5** | ⏳ Opcional | Security (Vault + Master Key) | Fase 1 |
| **Fase 6** | ⏳ Aguardando | Upload de dados + scripts Python | Fase 3 |

**Para avançar:**
1. Descomentar módulo `compute` em `main.tf` e `outputs.tf`
2. Executar `./apply_phase.sh 4`
3. Validar 4 Data Flow applications no Console OCI

**Opcional:** Migrar state para remoto (bucket `hackathon-2025-tfstate`):
```bash
cd mig_oci/terraform/scripts
./migrate_state_to_remote.sh
```

---

## Glossário (Novos Termos)

| Termo | Significado |
|-------|-------------|
| **VCN** | Virtual Cloud Network - rede virtual isolada na OCI (equivalente à VPC da AWS) |
| **Subnet** | Subdivisão da VCN com regras próprias de roteamento e segurança |
| **Internet Gateway** | Porta de acesso direto à internet (entrada e saída) |
| **NAT Gateway** | Network Address Translation - permite saída para internet sem expor IP interno |
| **Service Gateway** | Túnel direto para serviços OCI (Object Storage), sem passar pela internet |
| **Route Table** | Tabela que define para onde o tráfego de rede é direcionado |
| **Security List** | Conjunto de regras de firewall (portas, protocolos, origens permitidas) |
| **CIDR** | Classless Inter-Domain Routing - notação de faixa de IPs (ex: 10.0.0.0/16 = 65.536 IPs) |
| **Object Storage** | Serviço de armazenamento de arquivos na OCI (equivalente ao S3 da AWS) |
| **Bucket** | Container de nível superior no Object Storage (como uma "pasta raiz") |
| **Namespace** | Identificador globalmente único da tenancy para Object Storage |
| **Versioning** | Funcionalidade que mantém versões anteriores dos arquivos ao sobrescrever |
| **Lifecycle Policy** | Regra automática para arquivar ou deletar objetos após X dias |
| **Data Source** | No Terraform, consulta que lê informação existente (não cria recursos) |
