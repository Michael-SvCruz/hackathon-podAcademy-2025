# Arquitetura OCI - Hackathon PodAcademy 2025

## 1. Resumo Executivo

### 1.1 Contexto do Projeto

| Item | Valor |
|------|-------|
| **Projeto** | Modelo de Risco de Crédito - FPD (First Payment Default) |
| **Cliente** | Claro Telecom |
| **Arquitetura** | Medallion (Bronze → Silver → Gold → Modeling) |
| **ABT Final** | 3,795,310 registros × 614 colunas |
| **Benchmark** | KS = 33.1% OOT |
| **Resultado** | KS = 33.94% (+0.84 p.p. acima do benchmark) |
| **Janela OCI** | 30 dias para defesa final |

### 1.2 Volumes de Dados

| Fonte | Volume de Eventos | Output (Client-Month) | Compressão |
|-------|-------------------|----------------------|------------|
| **Recarga** | 95,210,519 | 32,882,218 | 2.9x |
| **Atraso** | 31,611,316 | 15,023,012 | 2.1x |
| **Pagamento** | 21,821,465 | 12,634,799 | 1.7x |
| **Bureau/Telco/Cadastro** | ~3.8M cada | - | Snapshot |

---

## 2. Tenancy (Locação)

### 2.1 Configuração Recomendada

```
Tenancy: hackathon-podacademy-2025
├── Região Principal: sa-saopaulo-1 (São Paulo)
├── Região Secundária: Não necessária (projeto hackathon)
├── Tipo de Conta: Pay-as-you-go
└── Home Region: Brazil East (São Paulo)
```

### 2.2 Justificativas

| Decisão | Justificativa |
|---------|---------------|
| **São Paulo** | Menor latência para dados brasileiros; conformidade LGPD |
| **Pay-as-you-go** | Otimização para janela curta de 30 dias; sem compromisso longo prazo |
| **Sem DR** | Projeto de hackathon; dados podem ser recriados; custo/benefício |

### 2.3 Configurações Iniciais

```bash
# Após criar a tenancy, configurar:
1. Habilitar MFA para usuário admin
2. Definir orçamento mensal (~$1,000)
3. Criar alertas de custo (50%, 80%, 100%)
4. Configurar Cloud Guard (segurança básica)
```

---

## 3. Compartments (Compartimentos)

### 3.1 Estrutura Hierárquica

```
Tenancy Root
└── hackathon-2025 (Compartment Raiz do Projeto)
    │
    ├── network
    │   ├── Descrição: VCN, Subnets, Gateways, Security Lists
    │   └── Recursos: VCN, Internet Gateway, NAT Gateway, Service Gateway
    │
    ├── storage
    │   ├── Descrição: Object Storage, Buckets por camada
    │   └── Recursos: 5 buckets (landing, bronze, silver, gold, models)
    │
    ├── compute
    │   ├── Descrição: Data Flow Applications, Data Science, VMs
    │   └── Recursos: Spark jobs, Notebooks, Scoring API (opcional)
    │
    ├── data
    │   ├── Descrição: Data Catalog, metadados
    │   └── Recursos: Catálogo de dados, schemas
    │
    └── security
        ├── Descrição: Vault, Keys, Secrets
        └── Recursos: Master encryption key, credenciais
```

### 3.2 Criação via OCI CLI

```bash
# Criar compartimento raiz
oci iam compartment create \
    --compartment-id <tenancy-ocid> \
    --name "hackathon-2025" \
    --description "Projeto Hackathon PodAcademy 2025 - Risco de Crédito"

# Criar sub-compartimentos
for comp in network storage compute data security; do
    oci iam compartment create \
        --compartment-id <hackathon-2025-ocid> \
        --name "$comp" \
        --description "Recursos de $comp para o projeto"
done
```

### 3.3 Tags para Organização

```hcl
# Tags recomendadas para todos os recursos
freeform-tags = {
    "projeto"     = "hackathon-2025"
    "ambiente"    = "producao"
    "responsavel" = "data-team"
    "custo"       = "hackathon"
}
```

---

## 4. Groups (Grupos)

### 4.1 Estrutura de Grupos

| Grupo | Descrição | Permissões Principais |
|-------|-----------|----------------------|
| **administrators** | Administradores do projeto | Full access a todos recursos |
| **data-engineers** | Engenheiros de dados | Storage, Data Flow, Data Catalog |
| **data-scientists** | Cientistas de dados | Notebooks, Models, Read storage |
| **viewers** | Stakeholders, revisores | Read-only em dashboards e métricas |

### 4.2 Criação dos Grupos

```bash
# Criar grupos
oci iam group create --name "administrators" \
    --description "Administradores do projeto Hackathon 2025"

oci iam group create --name "data-engineers" \
    --description "Engenheiros de dados - pipeline Bronze/Silver/Gold"

oci iam group create --name "data-scientists" \
    --description "Cientistas de dados - modelagem e análise"

oci iam group create --name "viewers" \
    --description "Visualizadores - stakeholders e revisores"
```

### 4.3 Políticas IAM por Grupo

```hcl
# Políticas para administrators
Allow group administrators to manage all-resources in compartment hackathon-2025

# Políticas para data-engineers
Allow group data-engineers to manage objects in compartment storage
Allow group data-engineers to manage buckets in compartment storage
Allow group data-engineers to manage dataflow-family in compartment compute
Allow group data-engineers to manage data-catalog-family in compartment data
Allow group data-engineers to use virtual-network-family in compartment network

# Políticas para data-scientists
Allow group data-scientists to manage data-science-family in compartment compute
Allow group data-scientists to read objects in compartment storage
Allow group data-scientists to manage objects in compartment storage where target.bucket.name='models'
Allow group data-scientists to use virtual-network-family in compartment network

# Políticas para viewers
Allow group viewers to read all-resources in compartment hackathon-2025

# Políticas para serviços (Data Flow)
Allow service dataflow to read objects in compartment storage
Allow service dataflow to manage objects in compartment storage
Allow service dataflow to read logs in compartment compute
```

---

## 5. Users (Usuários)

### 5.1 Estrutura de Usuários

| Usuário | Grupo | Função | Responsabilidades |
|---------|-------|--------|-------------------|
| `admin@projeto` | administrators | Admin geral | Setup, IAM, orçamento |
| `eng1@projeto` | data-engineers | Eng. Dados Sr | Pipeline Bronze/Silver |
| `eng2@projeto` | data-engineers | Eng. Dados Jr | Pipeline Gold/Features |
| `ds1@projeto` | data-scientists | Cientista Sr | Modelagem LightGBM |
| `ds2@projeto` | data-scientists | Cientista Jr | Análise, validação |
| `stakeholder@projeto` | viewers | PO/Gerente | Acompanhamento |

### 5.2 Criação de Usuários

```bash
# Criar usuários
oci iam user create --name "admin@projeto" \
    --description "Administrador do projeto"

# Adicionar usuário ao grupo
oci iam group add-user \
    --group-id <administrators-group-ocid> \
    --user-id <admin-user-ocid>
```

### 5.3 Configuração de Acesso

```bash
# Para cada usuário:
1. Criar usuário no IAM
2. Adicionar ao grupo apropriado
3. Gerar API Key (para acesso programático)
4. Configurar MFA (obrigatório para administrators)
5. Enviar credenciais temporárias por canal seguro
```

### 5.4 API Keys para Automação

```bash
# Gerar API Key para Data Flow
oci iam user api-key upload \
    --user-id <user-ocid> \
    --key-file ~/.oci/oci_api_key_public.pem

# Configurar ~/.oci/config
[DEFAULT]
user=ocid1.user.oc1..xxxxx
fingerprint=xx:xx:xx:xx:xx
tenancy=ocid1.tenancy.oc1..xxxxx
region=sa-saopaulo-1
key_file=~/.oci/oci_api_key.pem
```

---

## 6. Storage (Armazenamento)

### 6.1 Opções de Storage na OCI

| Serviço | Uso Recomendado | Custo (São Paulo) |
|---------|-----------------|-------------------|
| **Object Storage Standard** | Dados ativos (pipeline) | $0.0255/GB/mês |
| **Object Storage Archive** | Dados históricos (pós-defesa) | $0.0026/GB/mês |
| **Block Volume** | Notebooks, VMs | $0.0255/GB/mês |
| **File Storage** | Compartilhamento (não necessário) | $0.30/GB/mês |

### 6.2 Arquitetura de Buckets por Etapa

```
Object Storage Namespace: <tenancy-namespace>
│
├── Bucket: landing-zone
│   ├── Tier: Standard
│   ├── Conteúdo: Arquivos Parquet originais
│   ├── Tamanho estimado: 50 GB
│   ├── Versioning: Disabled
│   ├── Retention: 30 dias
│   └── Acesso: data-engineers (RW)
│
├── Bucket: bronze-layer
│   ├── Tier: Standard
│   ├── Conteúdo: Parquet + metadados de ingestão
│   ├── Tamanho estimado: 60 GB
│   ├── Versioning: Enabled
│   ├── Retention: 30 dias
│   └── Acesso: data-engineers (RW)
│
├── Bucket: silver-layer
│   ├── Tier: Standard
│   ├── Conteúdo: Delta Lake (tipado, validado)
│   ├── Tamanho estimado: 80 GB
│   ├── Versioning: Enabled
│   ├── Retention: 30 dias
│   └── Acesso: data-engineers (RW), data-scientists (R)
│
├── Bucket: gold-layer
│   ├── Tier: Standard
│   ├── Conteúdo: ABTs + features agregadas
│   ├── Tamanho estimado: 100 GB
│   ├── Versioning: Enabled
│   ├── Retention: 90 dias (manter pós-defesa)
│   └── Acesso: data-engineers (RW), data-scientists (R)
│
└── Bucket: models
    ├── Tier: Standard
    ├── Conteúdo: Modelos LightGBM, artefatos
    ├── Tamanho estimado: 5 GB
    ├── Versioning: Enabled
    ├── Retention: 90 dias
    └── Acesso: data-scientists (RW)
```

### 6.3 Criação dos Buckets

```bash
# Criar buckets
for bucket in landing-zone bronze-layer silver-layer gold-layer models; do
    oci os bucket create \
        --compartment-id <storage-compartment-ocid> \
        --name "$bucket" \
        --storage-tier Standard \
        --public-access-type NoPublicAccess
done

# Habilitar versioning nos buckets críticos
for bucket in bronze-layer silver-layer gold-layer models; do
    oci os bucket update \
        --bucket-name "$bucket" \
        --versioning Enabled
done
```

### 6.4 Lifecycle Policies (Otimização de Custo)

```json
{
  "name": "archive-after-60-days",
  "action": "ARCHIVE",
  "timeAmount": 60,
  "timeUnit": "DAYS",
  "isEnabled": true,
  "objectNameFilter": {
    "inclusionPatterns": ["*.parquet", "*.delta"]
  }
}
```

### 6.5 Paths de Acesso (Spark/PySpark)

```python
# Formato de path para OCI Object Storage
# oci://<bucket>@<namespace>/<path>

# Exemplos:
LANDING_PATH = "oci://landing-zone@<namespace>/bureau/"
BRONZE_PATH = "oci://bronze-layer@<namespace>/bureau_bronze/"
SILVER_PATH = "oci://silver-layer@<namespace>/bureau_silver_delta/"
GOLD_PATH = "oci://gold-layer@<namespace>/abt_v6_v2_delta/"
MODELS_PATH = "oci://models@<namespace>/modelo_final/"

# Configuração Spark para OCI
spark.conf.set("fs.oci.client.auth.tenantId", "<tenancy-ocid>")
spark.conf.set("fs.oci.client.auth.userId", "<user-ocid>")
spark.conf.set("fs.oci.client.auth.fingerprint", "<fingerprint>")
spark.conf.set("fs.oci.client.auth.pemfilepath", "/path/to/key.pem")
```

### 6.6 Estimativa de Custos de Storage

| Bucket | Tamanho | Tier | Custo/Mês |
|--------|---------|------|-----------|
| landing-zone | 50 GB | Standard | $1.28 |
| bronze-layer | 60 GB | Standard | $1.53 |
| silver-layer | 80 GB | Standard | $2.04 |
| gold-layer | 100 GB | Standard | $2.55 |
| models | 5 GB | Standard | $0.13 |
| **TOTAL** | **295 GB** | - | **$7.53** |

---

## 7. Compute (Processamento)

### 7.1 Opções de Compute na OCI

| Serviço | Uso Recomendado | Modelo de Cobrança |
|---------|-----------------|-------------------|
| **OCI Data Flow** | Jobs Spark (pipeline) | OCPU-hora |
| **OCI Data Science** | Notebooks, modelagem | OCPU-hora + storage |
| **Compute Instance** | VMs (API, custom) | OCPU-hora |
| **Container Instances** | Containers stateless | OCPU-hora |

### 7.2 OCI Data Flow (Apache Spark Gerenciado)

#### Configuração por Etapa do Pipeline

**Etapa 1: Landing → Bronze (Ingestão)**

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Spark Version | 3.5.0 | Compatibilidade com código atual |
| Driver Shape | VM.Standard.E4.Flex | Custo-benefício |
| Driver OCPU | 2 | Suficiente para coordenação |
| Driver Memory | 16 GB | Metadata handling |
| Executor Shape | VM.Standard.E4.Flex | Custo-benefício |
| Executor OCPU | 2 | Ingestão simples |
| Executor Memory | 16 GB | Buffer adequado |
| Num Executors | 4 | 6 fontes paralelas |
| Duração | 30-45 min | Estimativa |

```bash
# Criar Data Flow Application para Bronze
oci data-flow application create \
    --compartment-id <compute-compartment-ocid> \
    --display-name "bronze-ingestion" \
    --driver-shape VM.Standard.E4.Flex \
    --driver-shape-config '{"ocpus": 2, "memoryInGBs": 16}' \
    --executor-shape VM.Standard.E4.Flex \
    --executor-shape-config '{"ocpus": 2, "memoryInGBs": 16}' \
    --num-executors 4 \
    --spark-version 3.5.0 \
    --file-uri "oci://models@<namespace>/scripts/bronze_ingestion.py" \
    --language PYTHON
```

**Etapa 2: Bronze → Silver (Transformação)**

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Driver OCPU | 4 | Agregações moderadas |
| Driver Memory | 32 GB | Metadata + broadcast |
| Executor OCPU | 4 | Transformações pesadas |
| Executor Memory | 32 GB | Cache para joins |
| Num Executors | 8 | Volume: 95M eventos (Recarga) |
| Duração | 1-2 horas | Recarga mais pesado |

**Etapa 3: Silver → Gold Features (Feature Engineering)**

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Driver OCPU | 4 | Coordenação de agregações |
| Driver Memory | 32 GB | Broadcast de lookup tables |
| Executor OCPU | 4 | Agregações pesadas |
| Executor Memory | 32 GB | Window functions |
| Num Executors | 16 | Volume alto + agregações |
| Duração | 2-3 horas | 3 feature generators |

**Etapa 4: Gold → ABT Builder**

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Driver OCPU | 4 | JOINs múltiplos |
| Driver Memory | 32 GB | Broadcast de features |
| Executor OCPU | 4 | JOINs M1/M3/M6 |
| Executor Memory | 32 GB | Cache intermediário |
| Num Executors | 8 | JOINs sequenciais |
| Duração | 1-2 horas | ABT v1 a v6 |

#### Configuração Delta Lake para OCI Data Flow

```python
# Configurações necessárias para Delta Lake
spark_conf = {
    # Delta Lake
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",

    # OCI Object Storage
    "fs.oci.client.auth.tenantId": "<tenancy-ocid>",
    "fs.oci.client.auth.userId": "<user-ocid>",
    "fs.oci.client.auth.fingerprint": "<fingerprint>",
    "fs.oci.client.auth.pemfilepath": "/opt/spark/work-dir/oci_key.pem",

    # Performance
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}
```

### 7.3 OCI Data Science (Notebooks e Modelagem)

#### Configuração do Notebook Session

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Shape | VM.Standard.E4.Flex | Custo-benefício |
| OCPU | 8 | LightGBM training |
| Memory | 64 GB | Dataset em memória |
| Block Storage | 100 GB | Cache, checkpoints |
| Frameworks | Python 3.10, Spark, LightGBM, SHAP | Stack atual |

```bash
# Criar Notebook Session
oci data-science notebook-session create \
    --compartment-id <compute-compartment-ocid> \
    --project-id <project-ocid> \
    --display-name "modeling-notebook" \
    --notebook-session-config-details '{
        "shape": "VM.Standard.E4.Flex",
        "shapeConfigDetails": {
            "ocpus": 8,
            "memoryInGBs": 64
        },
        "blockStorageSizeInGBs": 100
    }'
```

#### Ambiente Conda Recomendado

```yaml
# environment.yml
name: hackathon-fpd
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.10
  - pandas=2.0
  - numpy=1.24
  - scikit-learn=1.3
  - lightgbm=4.1
  - shap=0.43
  - pyspark=3.5
  - delta-spark=3.0
  - matplotlib=3.8
  - seaborn=0.13
  - jupyter=1.0
  - pip:
    - oci-sdk
    - oracle-ads
```

### 7.4 Compute Instance (Scoring API - Opcional)

Se necessário deploy de API de scoring:

| Parâmetro | Valor | Justificativa |
|-----------|-------|---------------|
| Shape | VM.Standard.E4.Flex | Custo-benefício |
| OCPU | 2 | Inferência leve |
| Memory | 16 GB | Modelo em memória |
| OS | Oracle Linux 8 | Suporte OCI nativo |
| Uptime | 24/7 | Disponibilidade |

```bash
# Criar instância para API
oci compute instance launch \
    --compartment-id <compute-compartment-ocid> \
    --availability-domain "SA-SAOPAULO-1-AD-1" \
    --shape "VM.Standard.E4.Flex" \
    --shape-config '{"ocpus": 2, "memoryInGBs": 16}' \
    --image-id <oracle-linux-8-image-ocid> \
    --subnet-id <private-compute-subnet-ocid> \
    --display-name "scoring-api"
```

### 7.5 Estimativa de Custos de Compute

| Serviço | Config | Horas | Custo/OCPU-h | Total |
|---------|--------|-------|--------------|-------|
| **Data Flow - Bronze** | 10 OCPU | 4h × 3 runs | $0.20 | $24.00 |
| **Data Flow - Silver** | 36 OCPU | 8h × 3 runs | $0.20 | $172.80 |
| **Data Flow - Gold** | 68 OCPU | 12h × 3 runs | $0.20 | $489.60 |
| **Data Flow - ABT** | 36 OCPU | 8h × 3 runs | $0.20 | $172.80 |
| **Data Science** | 8 OCPU | 100h | $0.127 | $101.60 |
| **Block Storage** | 100 GB | 720h | $0.0255/GB | $2.55 |
| **Scoring API (opc)** | 2 OCPU | 720h | $0.0425 | $61.20 |
| **TOTAL** | - | - | - | **~$1,024.55** |

---

## 8. VCN (Virtual Cloud Network)

### 8.1 Arquitetura de Rede

```
┌─────────────────────────────────────────────────────────────────────┐
│                           INTERNET                                   │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │     Internet Gateway      │
                    │     (hackathon-igw)       │
                    └─────────────┬─────────────┘
                                  │
┌─────────────────────────────────┴───────────────────────────────────┐
│                    VCN: hackathon-vcn                                │
│                    CIDR: 10.0.0.0/16                                 │
│                    DNS: hackathon                                    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    public-subnet                                │ │
│  │                    10.0.1.0/24                                  │ │
│  │                                                                  │ │
│  │  ┌─────────────────────┐    ┌─────────────────────┐            │ │
│  │  │   Load Balancer     │    │    NAT Gateway      │            │ │
│  │  │   (opcional)        │    │   (hackathon-nat)   │            │ │
│  │  └─────────────────────┘    └──────────┬──────────┘            │ │
│  └─────────────────────────────────────────┼────────────────────────┘ │
│                                            │                          │
│  ┌─────────────────────────────────────────┼────────────────────────┐ │
│  │                    private-data-subnet  │                        │ │
│  │                    10.0.10.0/24         │                        │ │
│  │                                         │                        │ │
│  │  ┌─────────────────────┐    ┌─────────────────────┐            │ │
│  │  │   OCI Data Flow     │    │  OCI Data Science   │            │ │
│  │  │   (Spark Jobs)      │    │    (Notebooks)      │            │ │
│  │  └─────────────────────┘    └─────────────────────┘            │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                            │                          │
│  ┌─────────────────────────────────────────┼────────────────────────┐ │
│  │                    private-compute-subnet                        │ │
│  │                    10.0.20.0/24                                  │ │
│  │                                                                  │ │
│  │  ┌─────────────────────┐                                        │ │
│  │  │   Scoring API       │                                        │ │
│  │  │   (FastAPI)         │                                        │ │
│  │  └─────────────────────┘                                        │ │
│  └──────────────────────────────────────────────────────────────────┘ │
│                                            │                          │
│                              ┌─────────────┴─────────────┐           │
│                              │    Service Gateway        │           │
│                              │   (hackathon-sgw)         │           │
│                              └─────────────┬─────────────┘           │
└────────────────────────────────────────────┼────────────────────────┘
                                             │
                               ┌─────────────┴─────────────┐
                               │   OCI Object Storage      │
                               │   (All Buckets)           │
                               └───────────────────────────┘
```

### 8.2 Configuração da VCN

```bash
# Criar VCN
oci network vcn create \
    --compartment-id <network-compartment-ocid> \
    --cidr-blocks '["10.0.0.0/16"]' \
    --display-name "hackathon-vcn" \
    --dns-label "hackathon"
```

### 8.3 Subnets

| Subnet | CIDR | Tipo | Uso |
|--------|------|------|-----|
| public-subnet | 10.0.1.0/24 | Public | LB, NAT Gateway, Bastion |
| private-data-subnet | 10.0.10.0/24 | Private | Data Flow, Data Science |
| private-compute-subnet | 10.0.20.0/24 | Private | VMs, Scoring API |

```bash
# Criar subnet pública
oci network subnet create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --cidr-block "10.0.1.0/24" \
    --display-name "public-subnet" \
    --dns-label "public" \
    --prohibit-public-ip-on-vnic false

# Criar subnets privadas
oci network subnet create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --cidr-block "10.0.10.0/24" \
    --display-name "private-data-subnet" \
    --dns-label "data" \
    --prohibit-public-ip-on-vnic true

oci network subnet create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --cidr-block "10.0.20.0/24" \
    --display-name "private-compute-subnet" \
    --dns-label "compute" \
    --prohibit-public-ip-on-vnic true
```

### 8.4 Gateways

| Gateway | Tipo | Função |
|---------|------|--------|
| hackathon-igw | Internet Gateway | Acesso externo para LB |
| hackathon-nat | NAT Gateway | Saída internet para subnets privadas |
| hackathon-sgw | Service Gateway | Acesso ao Object Storage sem internet |

```bash
# Internet Gateway
oci network internet-gateway create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --display-name "hackathon-igw" \
    --is-enabled true

# NAT Gateway
oci network nat-gateway create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --display-name "hackathon-nat"

# Service Gateway (para Object Storage)
oci network service-gateway create \
    --compartment-id <network-compartment-ocid> \
    --vcn-id <vcn-ocid> \
    --services '[{"serviceId": "<object-storage-service-ocid>"}]' \
    --display-name "hackathon-sgw"
```

### 8.5 Security Lists

**public-sl (para public-subnet):**

| Direção | Protocolo | Porta | Origem/Destino | Descrição |
|---------|-----------|-------|----------------|-----------|
| Ingress | TCP | 443 | 0.0.0.0/0 | HTTPS externo |
| Ingress | TCP | 22 | <IP-corporativo>/32 | SSH (restrito) |
| Egress | All | All | 0.0.0.0/0 | Saída total |

**private-data-sl (para private-data-subnet):**

| Direção | Protocolo | Porta | Origem/Destino | Descrição |
|---------|-----------|-------|----------------|-----------|
| Ingress | All | All | 10.0.0.0/16 | Tráfego interno |
| Egress | TCP | 443 | OCI Services | Object Storage |
| Egress | All | All | 10.0.0.0/16 | Tráfego interno |

**private-compute-sl (para private-compute-subnet):**

| Direção | Protocolo | Porta | Origem/Destino | Descrição |
|---------|-----------|-------|----------------|-----------|
| Ingress | TCP | 8000 | 10.0.1.0/24 | API do LB |
| Egress | TCP | 443 | OCI Services | Object Storage |
| Egress | All | All | 10.0.0.0/16 | Tráfego interno |

### 8.6 Route Tables

**public-rt:**
```
Destination: 0.0.0.0/0 → Internet Gateway
```

**private-rt:**
```
Destination: 0.0.0.0/0 → NAT Gateway
Destination: OCI Services → Service Gateway
```

### 8.7 Estimativa de Custos de Rede

| Recurso | Custo/Mês |
|---------|-----------|
| NAT Gateway | $32.40 |
| Load Balancer (10 Mbps) | $21.60 |
| Data Transfer (100 GB) | $0.00 (intra-region) |
| **TOTAL** | **~$54.00** |

---

## 9. Segurança

### 9.1 Encryption (Criptografia)

| Recurso | Tipo | Gerenciamento |
|---------|------|---------------|
| Object Storage | SSE-S3 | OCI-managed |
| Block Storage | SSE | OCI-managed |
| Data em trânsito | TLS 1.2+ | Automático |

### 9.2 OCI Vault

```bash
# Criar Vault
oci kms vault create \
    --compartment-id <security-compartment-ocid> \
    --display-name "hackathon-vault" \
    --vault-type DEFAULT

# Criar Master Encryption Key
oci kms key create \
    --compartment-id <security-compartment-ocid> \
    --display-name "master-encryption-key" \
    --key-shape '{"algorithm": "AES", "length": 256}'
```

### 9.3 Secrets (Credenciais)

| Secret | Conteúdo | Uso |
|--------|----------|-----|
| oci-api-key | API Key PEM | Autenticação programática |
| spark-config | Spark properties | Configuração Data Flow |

### 9.4 Auditoria

```bash
# Habilitar Audit Logs
oci audit config update \
    --compartment-id <tenancy-ocid> \
    --retention-period-days 90
```

---

## 10. Orçamento Total Estimado (30 dias)

### 10.1 Resumo por Categoria

| Categoria | Serviço | Custo Estimado |
|-----------|---------|----------------|
| **Storage** | Object Storage (295 GB) | $7.53 |
| **Storage** | Block Storage (100 GB) | $2.55 |
| **Compute** | Data Flow (todas etapas) | $859.20 |
| **Compute** | Data Science Notebook | $101.60 |
| **Compute** | Scoring API (opcional) | $61.20 |
| **Network** | NAT Gateway | $32.40 |
| **Network** | Load Balancer (opcional) | $21.60 |
| **SUBTOTAL** | - | **$1,086.08** |
| **Contingência** | 15% | $162.91 |
| **TOTAL** | - | **~$1,249.00** |

### 10.2 Otimizações de Custo

1. **Usar Always Free Tier** onde disponível
2. **Parar notebooks** quando não em uso (economia de 50%+)
3. **Auto-scaling** em Data Flow (executors dinâmicos)
4. **Archive Tier** para dados após defesa (redução de 90%)
5. **Spot instances** para notebooks de teste
6. **Evitar LB** se API de scoring não for necessária

### 10.3 Monitoramento de Custos

```bash
# Criar Budget Alert
oci budgets budget create \
    --compartment-id <hackathon-2025-ocid> \
    --amount 1000 \
    --reset-period MONTHLY \
    --display-name "hackathon-budget" \
    --target-type COMPARTMENT \
    --targets '["<hackathon-2025-ocid>"]'

# Criar alertas em 50%, 80%, 100%
```

---

## 11. Cronograma de Migração (30 dias)

### Semana 1: Setup Infraestrutura

| Dia | Atividade | Responsável |
|-----|-----------|-------------|
| 1 | Criar tenancy, compartments | Admin |
| 2 | Configurar IAM (groups, users, policies) | Admin |
| 3 | Criar VCN, subnets, gateways | Admin |
| 4 | Criar buckets, configurar versioning | Eng. Dados |
| 5 | Upload dados para landing-zone | Eng. Dados |

### Semana 2: Pipeline Bronze/Silver

| Dia | Atividade | Responsável |
|-----|-----------|-------------|
| 6-7 | Adaptar scripts Bronze para OCI | Eng. Dados |
| 8 | Configurar Data Flow Applications | Eng. Dados |
| 9-10 | Executar e validar Bronze | Eng. Dados |
| 11-12 | Executar e validar Silver | Eng. Dados |

### Semana 3: Pipeline Gold/ABT

| Dia | Atividade | Responsável |
|-----|-----------|-------------|
| 13-14 | Executar Feature Generators | Eng. Dados |
| 15-16 | Executar ABT Builders (v1-v5) | Eng. Dados |
| 17-18 | Executar ABT v6, validar 614 colunas | Eng. Dados |
| 19 | Buffer para correções | Eng. Dados |

### Semana 4: Modelagem e Defesa

| Dia | Atividade | Responsável |
|-----|-----------|-------------|
| 20-21 | Configurar Data Science Notebook | Cientista |
| 22-23 | Executar notebooks de modelagem | Cientista |
| 24-25 | Validar modelo (KS >= 33.1% OOT) | Cientista |
| 26-27 | Deploy Scoring API (opcional) | Eng. Dados |
| 28-29 | Preparar evidências para defesa | Time |
| 30 | Defesa final | Time |

---

## 12. Checklist de Migração

### 12.1 Pré-Migração

```
□ Revisar código para dependências Databricks
□ Identificar uso de dbutils (substituir por oci-sdk)
□ Mapear paths /Volumes/ para oci://
□ Verificar compatibilidade Delta Lake
□ Documentar configurações atuais
```

### 12.2 Infraestrutura

```
□ Criar tenancy e configurar billing
□ Criar compartimentos (5)
□ Criar grupos e políticas IAM
□ Criar usuários e API Keys
□ Configurar VCN completa
□ Criar buckets de storage
□ Habilitar versioning e lifecycle
□ Configurar Vault e secrets
□ Habilitar audit logs
```

### 12.3 Pipeline

```
□ Upload dados landing-zone
□ Adaptar scripts Bronze (6 fontes)
□ Adaptar scripts Silver (6 fontes)
□ Adaptar Feature Generators (3)
□ Adaptar ABT Builders (6 versões)
□ Configurar Data Flow Applications
□ Executar pipeline completo
□ Validar ABT v6 (3.79M × 614 cols)
```

### 12.4 Modelagem

```
□ Configurar Data Science Notebook
□ Instalar ambiente conda
□ Migrar notebooks de modelagem
□ Executar diagnóstico de features
□ Treinar modelo final
□ Validar KS OOT >= 33.1%
□ Exportar artefatos para bucket models
```

### 12.5 Opcional (Scoring API)

```
□ Criar VM para API
□ Configurar Docker/FastAPI
□ Criar Load Balancer
□ Testar endpoints
□ Monitorar latência
```

---

## 13. Scripts Prioritários para Migração

### 13.1 Alta Prioridade (Volume/Criticidade)

| Script | Path | Volume | Adaptações |
|--------|------|--------|------------|
| `gold_recarga_features_v2.py` | `/src/jobs/02_gold/` | 95M eventos | Paths OCI, Delta config |
| `05_gold_abt_v6_builder_v2.py` | `/src/jobs/02_gold/` | ABT final | Paths OCI |
| `gold_pagamento_features_v2.py` | `/src/jobs/02_gold/` | 21.8M eventos | Paths OCI |
| `gold_atraso_features_v2.py` | `/src/jobs/02_gold/` | 31.6M eventos | Paths OCI |

### 13.2 Média Prioridade

| Script | Path | Adaptações |
|--------|------|------------|
| `02_bronze_silver_cadastro.py` | `/src/jobs/01_silver/` | Paths OCI (já usa F.to_date) |
| `spark_utils.py` | `/src/utils/` | Remover dbutils, adicionar oci-sdk |

### 13.3 Notebooks de Modelagem

| Notebook | Path | Adaptações |
|----------|------|------------|
| `20260202 - Modelo Final FPD.ipynb` | `/src/jobs/04_modeling/` | Paths OCI, spark session |
| `20260202 - Diagnostico Features Comportamentais.ipynb` | `/src/jobs/04_modeling/` | Paths OCI |

---

## 14. Adaptações de Código

### 14.1 Substituição de Paths

```python
# ANTES (Databricks)
BRONZE_PATH = "/Volumes/hackathon_2025/default/bronze/"
df = spark.read.format("delta").load(BRONZE_PATH)

# DEPOIS (OCI)
BRONZE_PATH = "oci://bronze-layer@<namespace>/bureau_bronze_delta/"
df = spark.read.format("delta").load(BRONZE_PATH)
```

### 14.2 Substituição de dbutils

```python
# ANTES (Databricks)
dbutils.fs.ls("/Volumes/hackathon_2025/")
dbutils.fs.head("/Volumes/.../modelo.txt", max_bytes=10000)

# DEPOIS (OCI)
import oci
from oci.object_storage import ObjectStorageClient

client = ObjectStorageClient(config)
objects = client.list_objects(namespace, bucket_name)
content = client.get_object(namespace, bucket, object_name).data.content
```

### 14.3 Configuração Spark Session

```python
# OCI Data Flow Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hackathon-pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("fs.oci.client.auth.tenantId", "<tenancy-ocid>") \
    .config("fs.oci.client.auth.userId", "<user-ocid>") \
    .config("fs.oci.client.auth.fingerprint", "<fingerprint>") \
    .config("fs.oci.client.auth.pemfilepath", "/opt/spark/work-dir/key.pem") \
    .getOrCreate()
```

---

## 15. Referências

### 15.1 Documentação OCI

- [OCI Data Flow](https://docs.oracle.com/en-us/iaas/data-flow/using/home.htm)
- [OCI Data Science](https://docs.oracle.com/en-us/iaas/data-science/using/overview.htm)
- [OCI Object Storage](https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm)
- [Delta Lake on OCI](https://docs.oracle.com/en-us/iaas/data-flow/using/spark-delta-lake.htm)

### 15.2 Documentação do Projeto

- `docs/00_project/target_definition.md` - Regras anti-leakage
- `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md` - Dicionário de variáveis
- `docs/08_team_preparation/` - Documentação para apresentações

---

*Documento gerado em: 2026-02-03*
*Projeto: Hackathon PodAcademy 2025 - Modelo de Risco de Crédito*
