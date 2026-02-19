# Migração OCI - Fase 6A: Pipeline Landing → Bronze

## Contexto

Com a infraestrutura OCI pronta (Fases 1-4) e os 21 Data Flow Applications criados, a Fase 6A implementa a **primeira etapa do pipeline Medallion**: ingestão dos dados brutos (Landing Zone) para a camada Bronze com metadados de controle.

**Pré-requisitos:**
- Fases 1-4 aplicadas (IAM, Network, Storage, Compute)
- OCI CLI configurado (`oci setup config`)
- Dados brutos enviados para o bucket `landing-zone` (prefixo `source/`)
- Scripts PySpark enviados para o bucket `landing-zone` (prefixo `scripts/`)

---

## O que é a Camada Bronze?

A Bronze é a primeira camada do Medallion Architecture. Ela recebe os dados **exatamente como vieram da fonte** e adiciona metadados de controle (timestamp de ingestão, arquivo de origem, sistema fonte). Não faz limpeza, tipagem nem transformação — isso é responsabilidade da Silver.

**Analogia:** A Bronze é como o "almoxarifado de entrada" de uma fábrica. Tudo que chega é registrado (data, fornecedor, nota fiscal) e guardado como veio. A inspeção de qualidade (Silver) vem depois.

```
Landing Zone (dados brutos)              Bronze Layer (dados + metadados)
┌──────────────────────────┐            ┌──────────────────────────┐
│  source/                 │            │  bureau/                 │
│  ├── bureau/*.parquet    │            │  ├── _delta_log/         │
│  ├── telco/*.parquet     │  ────────→ │  ├── part-00000.parquet  │
│  ├── cadastro/*.parquet  │  6 scripts │  └── part-00001.parquet  │
│  ├── recarga/*.parquet   │  Bronze    │  telco/                  │
│  ├── pagamento/*.parquet │            │  ├── _delta_log/         │
│  └── atraso/*.parquet    │            │  └── part-*.parquet      │
└──────────────────────────┘            │  cadastro/ ...           │
                                        │  recarga/ ...            │
  Bucket: landing-zone                  │  pagamento/ ...          │
  Formato: CSV ou Parquet               │  atraso/ ...             │
                                        └──────────────────────────┘
                                          Bucket: bronze-layer
                                          Formato: Delta Lake
```

### Metadados adicionados na Bronze

Cada script Bronze adiciona 3 colunas de controle:

| Coluna | Tipo | Conteúdo | Propósito |
|--------|------|----------|-----------|
| `metadata_data_ingestao` | timestamp | `F.current_timestamp()` | Quando o dado foi ingerido |
| `metadata_nome_arquivo_origem` | string | `F.input_file_name()` | Path do arquivo fonte |
| `metadata_sistema_origem` | string | `F.lit("HACKATHON_LANDING_<FONTE>")` | Identificador do sistema |

**Nota sobre `F.input_file_name()`:** No Databricks, usávamos `F.col("_metadata.file_path")` que é um recurso proprietário. No OCI Data Flow, usamos `F.input_file_name()` que é a função padrão do Apache Spark.

---

## As 6 Fontes de Dados

O pipeline Bronze processa 6 fontes independentes, cada uma com seu script dedicado:

| Fonte | Script | Entrada (Landing) | Saída (Bronze) | Características |
|-------|--------|-------------------|----------------|-----------------|
| **Bureau** | `bronze_bureau.py` | `source/bureau/` | `bureau/` | Scores de crédito (Score_01, Score_02) |
| **Telco** | `bronze_telco.py` | `source/telco/` | `telco/` | 68 vars anonimizadas (var_26 a var_93), sentinela 304 |
| **Cadastro** | `bronze_cadastro.py` | `source/cadastro/` | `cadastro/` | Dados demográficos (CPF, nascimento, estado) |
| **Recarga** | `bronze_recarga.py` | `source/recarga/` | `recarga/` | Eventos transacionais de recarga, sentinelas -1/-2/-3 |
| **Pagamento** | `bronze_pagamento.py` | `source/pagamento/` | `pagamento/` | Histórico de pagamentos e faturas |
| **Atraso** | `bronze_atraso.py` | `source/atraso/` | `atraso/` | Faturas em aberto, aging, write-off |

### Por que 1 script por fonte?

No Databricks, cada fonte já tinha seu notebook separado. Na OCI, cada script é uma **Data Flow Application independente**, permitindo:
- **Execução paralela:** As 6 fontes podem rodar ao mesmo tempo
- **Isolamento de falhas:** Se uma fonte falha, as outras continuam
- **Monitoramento granular:** Logs e métricas por fonte
- **Reprocessamento seletivo:** Reexecutar apenas a fonte que precisa

---

## Arquitetura dos Scripts Bronze

### Padrão auto-contido (sem dependências externas)

Cada script Bronze é **auto-contido** — não depende de módulos externos nem de downloads em runtime. Isso elimina problemas de autenticação e carregamento de dependências no Data Flow.

```python
# Imports: apenas bibliotecas built-in do Spark
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Namespace OCI (passado como argumento pelo Data Flow)
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço:
# - Delta Lake (via configuration{} do Terraform)
# - Autenticação OCI (Resource Principal automático)

# Paths OCI Object Storage
DEFAULT_INPUT_PATH = f"oci://hackathon-2025-landing-zone@{namespace}/source/<fonte>/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/<fonte>/"
```

**Por que auto-contido?** No OCI Data Flow, o serviço configura a SparkSession automaticamente com:
- **Delta Lake:** Extensões Spark SQL configuradas no bloco `configuration{}` do Terraform
- **Autenticação OCI:** Resource Principal gerenciado pelo serviço (sem credenciais no código)
- **HDFS Connector:** Acesso a `oci://` URIs configurado internamente

Isso significa que `SparkSession.builder.appName("...").getOrCreate()` retorna uma sessão completa, sem necessidade de configuração adicional.

### Fluxo de execução (3 etapas)

```
┌─────────────────────────────────────────────────────────┐
│                    Script Bronze                         │
│                                                          │
│  1. LEITURA (Landing)                                    │
│     spark.read.format("parquet").load(input_path)        │
│     OU                                                   │
│     spark.read.format("csv").option("header","true")     │
│                                                          │
│  2. ENRIQUECIMENTO (Metadados)                           │
│     + metadata_data_ingestao (timestamp)                 │
│     + metadata_nome_arquivo_origem (file path)           │
│     + metadata_sistema_origem (literal)                  │
│                                                          │
│  3. ESCRITA (Bronze - Delta Lake)                        │
│     df.write.format("delta").mode("overwrite").save()    │
│     + mergeSchema = true                                 │
│     + overwriteSchema = true                             │
└─────────────────────────────────────────────────────────┘
```

### Delta Lake na Bronze

Os dados são escritos em formato **Delta Lake**, que adiciona:
- **Transaction log** (`_delta_log/`): Registro de todas as operações (ACID)
- **Schema enforcement**: Garante consistência de schema entre escritas
- **Time travel**: Possibilidade de consultar versões anteriores dos dados

---

## Configuração no Terraform

As 6 aplicações Bronze são parte das 21 Data Flow Applications definidas via `for_each` dinâmico:

```hcl
# mig_oci/terraform/environments/prod/variables.tf (trecho)
dataflow_applications = {
  # Bronze (leve: 2 OCPU, 4 executors)
  bronze-bureau    = { display_name = "bronze-bureau",    script_name = "bronze_bureau.py",    ocpu = 2, num_executors = 4 }
  bronze-telco     = { display_name = "bronze-telco",     script_name = "bronze_telco.py",     ocpu = 2, num_executors = 4 }
  bronze-cadastro  = { display_name = "bronze-cadastro",  script_name = "bronze_cadastro.py",  ocpu = 2, num_executors = 4 }
  bronze-recarga   = { display_name = "bronze-recarga",   script_name = "bronze_recarga.py",   ocpu = 2, num_executors = 4 }
  bronze-pagamento = { display_name = "bronze-pagamento", script_name = "bronze_pagamento.py", ocpu = 2, num_executors = 4 }
  bronze-atraso    = { display_name = "bronze-atraso",    script_name = "bronze_atraso.py",    ocpu = 2, num_executors = 4 }
  # ... (15 Silver, Gold e ABT applications)
}
```

### Configuração Spark (Delta Lake)

Definida no módulo compute, aplicada automaticamente a todas as 21 applications:

```hcl
# mig_oci/terraform/modules/compute/main.tf (trecho)
configuration = {
  "spark.sql.extensions"            = "io.delta.sql.DeltaSparkSessionExtension"
  "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
```

### Recursos por Application Bronze

| Recurso | Configuração | Total por App |
|---------|:------------:|:-------------:|
| Driver OCPUs | 2 | 2 |
| Driver RAM | 32 GB | 32 GB |
| Executor OCPUs | 2 | 8 (4 exec × 2) |
| Executor RAM | 32 GB | 128 GB (4 exec × 32) |
| **Total** | - | **10 OCPUs, 160 GB RAM** |

### Argumento: Namespace

O namespace OCI é passado para cada script via o campo `arguments` do Data Flow:

```hcl
arguments = [var.namespace]  # ex: ["grfjr7baehkj"]
```

No script, é recebido como `sys.argv[1]` e usado para construir os paths `oci://`:
```python
namespace = sys.argv[1]  # "grfjr7baehkj"
input_path = f"oci://hackathon-2025-landing-zone@{namespace}/source/telco/"
```

---

## Upload: Dados e Scripts

### 1. Upload dos dados brutos (manual)

Os dados brutos devem ser enviados para o bucket `landing-zone` seguindo a estrutura:

```bash
# Estrutura esperada no bucket landing-zone
source/
├── bureau/          ← Parquets/CSVs do Bureau
├── telco/           ← Parquets/CSVs do Telco
├── cadastro/        ← Parquets/CSVs do Cadastro
├── recarga/         ← Parquets/CSVs da Recarga
├── pagamento/       ← Parquets/CSVs do Pagamento
└── atraso/          ← Parquets/CSVs do Atraso
```

**Upload via OCI CLI:**
```bash
# Exemplo: upload de um diretório de parquets
oci os object put \
  --bucket-name hackathon-2025-landing-zone \
  --file /caminho/local/base_telco.parquet \
  --name "source/telco/base_telco.parquet" \
  --force
```

**Upload via Console OCI:**
1. Menu ☰ > Storage > Buckets
2. Selecionar `hackathon-2025-landing-zone`
3. Criar prefixo `source/<fonte>/`
4. Upload dos arquivos

### 2. Upload dos scripts

Os scripts adaptados são enviados automaticamente pelo script `upload_scripts.sh`:

```bash
cd mig_oci/data_upload
./upload_scripts.sh
```

**Saída esperada:**
```
=== Upload de Scripts para OCI ===

Namespace: grfjr7baehkj
Bucket:    hackathon-2025-landing-zone

--- Upload de scripts (21 arquivos) ---
  Uploading bronze_bureau.py...
  Uploading bronze_telco.py...
  Uploading bronze_cadastro.py...
  ...
Scripts enviados: 21/21

=== Upload concluído! ===
```

### Verificação

```bash
# Verificar scripts no bucket
oci os object list \
  --bucket-name hackathon-2025-landing-zone \
  --prefix "scripts/" \
  --query 'data[].name'

# Verificar dados brutos
oci os object list \
  --bucket-name hackathon-2025-landing-zone \
  --prefix "source/" \
  --query 'data[].{name:name, size:"size"}' \
  --output table
```

---

## Execução no Console OCI

### Criar um Run (execução)

1. Menu ☰ > Analytics & AI > Data Flow
2. Selecionar compartment `hackathon-2025` > `compute`
3. Clicar na application desejada (ex: `hackathon-2025-dataflow-bronze-telco`)
4. Clicar **"Create Run"**
5. Configurar:
   - **Enable Autoscaling**: Ativado (recomendado para free tier)
   - Demais configurações: manter padrão
6. Clicar **"Create"**

```
Console OCI > Data Flow > Applications > bronze-telco
┌─────────────────────────────────────────────────┐
│  Create Run                                      │
│                                                  │
│  [✓] Enable Autoscaling                          │
│                                                  │
│  Arguments: grfjr7baehkj (namespace)             │
│                                                  │
│  [Create]                                        │
└─────────────────────────────────────────────────┘
```

### Monitoramento

Após criar o Run, o status evolui:

```
Accepted → In Progress → Succeeded (ou Failed)
  │            │              │
  │            │              └── Duração, Data Read/Written nos detalhes
  │            └── Spark UI disponível (aba Monitoring)
  └── Aguardando provisionamento de recursos
```

**Duração típica dos scripts Bronze:** 3-5 minutos por fonte (inclui provisionamento).

### Logs

Os logs são gravados automaticamente no bucket landing-zone:

```bash
# Listar logs de uma execução
oci os object list \
  --bucket-name hackathon-2025-landing-zone \
  --prefix "dataflow-logs/" \
  --query 'data[].name'
```

---

## Validação da Fase 6A

### 1. Verificar dados escritos na Bronze

```bash
# Listar objetos no bucket bronze (uma fonte)
oci os object list \
  --bucket-name hackathon-2025-bronze-layer \
  --prefix "telco/" \
  --query 'data[].{name:name, size:"size"}' \
  --output table
```

**Saída esperada:** Arquivos `.parquet` + pasta `_delta_log/` com transaction logs.

### 2. Verificar todas as 6 fontes

```bash
for fonte in bureau telco cadastro recarga pagamento atraso; do
  echo "=== $fonte ==="
  oci os object list \
    --bucket-name hackathon-2025-bronze-layer \
    --prefix "$fonte/" \
    --query 'length(data)' \
    --raw-output
done
```

### 3. Ler uma amostra dos dados

```bash
# Baixar um parquet file para verificação local
oci os object get \
  --bucket-name hackathon-2025-bronze-layer \
  --name "telco/part-00000-xxxxx.snappy.parquet" \
  --file /tmp/telco_bronze_sample.parquet

# Ler com pandas
python3 -c "
import pandas as pd
df = pd.read_parquet('/tmp/telco_bronze_sample.parquet')
print(f'Shape: {df.shape}')
print(f'Colunas: {list(df.columns)}')
print(df.head(3))
"
```

---

## Resultados da Execução

| Fonte | Status | Duração | Data Read | Data Written |
|-------|:------:|:-------:|:---------:|:------------:|
| **Bureau** | Succeeded | ~4 min | ~84 MB | ~85 MB |
| **Telco** | Succeeded | ~4 min | ~84 MB | ~85 MB |
| **Cadastro** | Succeeded | ~4 min | ~45 MB | ~46 MB |
| **Recarga** | Succeeded | ~4 min | ~120 MB | ~121 MB |
| **Pagamento** | Succeeded | ~4 min | ~90 MB | ~91 MB |
| **Atraso** | Succeeded | ~4 min | ~60 MB | ~61 MB |

---

## Estrutura de Arquivos (Fase 6A)

```
mig_oci/data_upload/
├── scripts/
│   ├── bronze_bureau.py                    # ✅ Ingestão Bureau
│   ├── bronze_telco.py                     # ✅ Ingestão Telco
│   ├── bronze_cadastro.py                  # ✅ Ingestão Cadastro
│   ├── bronze_recarga.py                   # ✅ Ingestão Recarga
│   ├── bronze_pagamento.py                 # ✅ Ingestão Pagamento
│   ├── bronze_atraso.py                    # ✅ Ingestão Atraso
│   ├── silver_*.py (6)                     # ⏳ Fase 6B
│   ├── gold_*.py (3)                       # ⏳ Fase 6C
│   ├── abt_*_builder.py (6)               # ⏳ Fase 6D
│   ├── spark_utils.py                      # Utilitários (usado por Silver/Gold/ABT)
│   └── validate_abt.py                     # Validações ABT
├── upload_scripts.sh                       # Upload automatizado (21 scripts)
└── libs/                                   # (não utilizado na versão atual)
```

---

## Próximos Passos

| Fase | Status | O que faz | Dependência |
|------|--------|-----------|-------------|
| **Fase 6A** | ✅ Concluída | Landing → Bronze (6 fontes) | Fases 1-4, dados na landing-zone |
| **Fase 6B** | ⏳ Aguardando | Bronze → Silver (tipagem, validação, sentinelas) | Fase 6A |
| **Fase 6C** | ⏳ Aguardando | Silver → Gold (feature engineering M1/M3/M6) | Fase 6B |
| **Fase 6D** | ⏳ Aguardando | Gold → ABT v1-v6 (joins + builder final) | Fase 6C |

**Para avançar (Fase 6B — Silver):**
1. Executar as 6 Silver applications no Console OCI
2. Validar dados tipados e limpos nos buckets silver
3. Verificar tratamento de sentinelas (304 em Telco, -1/-2/-3 em Recarga)

---

## Glossário (Novos Termos)

| Termo | Significado |
|-------|-------------|
| **Landing Zone** | Camada de entrada do pipeline — dados brutos exatamente como recebidos da fonte |
| **Bronze** | Primeira camada do Medallion — dados com metadados de ingestão, sem transformação |
| **Delta Lake** | Formato de armazenamento open-source que adiciona ACID transactions sobre Parquet |
| **`_delta_log/`** | Pasta com o transaction log do Delta Lake — registra cada operação de escrita |
| **`F.input_file_name()`** | Função Spark que retorna o path do arquivo sendo lido (alternativa ao `_metadata` do Databricks) |
| **Resource Principal** | Mecanismo de autenticação automática do OCI Data Flow — o serviço autentica sem credenciais no código |
| **Auto-contido** | Script que não depende de módulos externos — todas as funções necessárias estão inline |
| **Autoscaling** | Opção do Data Flow que ajusta executors automaticamente (contorna limites de quota no free tier) |
| **`for_each`** | Recurso do Terraform que cria múltiplas instâncias de um resource a partir de um mapa |
