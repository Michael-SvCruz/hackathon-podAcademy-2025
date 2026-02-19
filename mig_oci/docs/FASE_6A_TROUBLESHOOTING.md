# Fase 6A - Troubleshooting: Lições Aprendidas na Migração Landing → Bronze

## Resumo

Este documento registra os **problemas técnicos encontrados** durante a adaptação dos scripts PySpark do Databricks para o OCI Data Flow, as **tentativas de solução**, e o **padrão final que funcionou**. É um guia de referência para evitar os mesmos erros nas próximas etapas (Silver, Gold, ABT).

**Cronologia resumida:**

```
Tentativa 1: archive_uri + utils.zip  ──→  FALHOU (conda pack, versão Python)
Tentativa 2: addPyFile("oci://...")    ──→  FALHOU (X509FederationClient)
Tentativa 3: Scripts self-contained    ──→  SUCESSO ✅
```

---

## Problema 1: Propriedade Spark Reservada

### Sintoma

Ao executar qualquer script no OCI Data Flow, o job falhava imediatamente com erro interno do serviço, sem mensagem clara nos logs do usuário.

### Causa Raiz

O `spark_utils.py` adaptado para OCI continha a seguinte configuração:

```python
# ❌ ERRADO - propriedade reservada no OCI Data Flow
def get_spark_session(app_name="Hackathon_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.oci.client.hostname",
                "https://objectstorage.sa-saopaulo-1.oraclecloud.com") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
```

A propriedade `spark.hadoop.fs.oci.client.hostname` é **gerenciada internamente** pelo OCI Data Flow. Quando o usuário tenta defini-la, o serviço rejeita a configuração.

### Solução

Remover a propriedade reservada. O OCI Data Flow configura automaticamente:
- `spark.hadoop.fs.oci.client.hostname` (endpoint do Object Storage)
- Autenticação via Resource Principal
- HDFS Connector para `oci://` URIs

```python
# ✅ CORRETO - deixar o Data Flow configurar o HDFS Connector
def get_spark_session(app_name="Hackathon_App"):
    # No OCI Data Flow, Delta Lake e OCI HDFS Connector já são configurados
    # pelo serviço (via configuration{} do Terraform e Resource Principal).
    # spark.hadoop.fs.oci.client.hostname é RESERVADA - não pode ser setada.
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
```

### Regra Aprendida

> **No OCI Data Flow, NUNCA configurar propriedades `spark.hadoop.fs.oci.*` no código Python.** O serviço gerencia essas propriedades via Resource Principal. A configuração de Delta Lake (via `configuration{}` no Terraform) é a única que deve ser explícita.

---

## Problema 2: utils.zip e archive_uri

### Sintoma

```
ArchiveWarn: Incorrect python version used to package utils.zip
```

O OCI Data Flow rejeitou o `utils.zip` por incompatibilidade de versão Python usada no empacotamento.

### Contexto

A estratégia original era empacotar `spark_utils.py` e `validate_abt.py` em um ZIP e distribuí-lo via `archive_uri` no Terraform:

```
mig_oci/data_upload/libs/utils.zip
└── python/lib/
    ├── spark_utils.py
    └── validate_abt.py
```

O `archive_uri` do OCI Data Flow **exige** que o ZIP seja criado com `conda pack` usando a mesma versão de Python do cluster Spark. Um simples `zip -r` não é suficiente.

### Tentativas

| Tentativa | Método | Resultado |
|-----------|--------|-----------|
| 1 | `zip -r utils.zip python/lib/*.py` | `ArchiveWarn: Incorrect python version` |
| 2 | Remover `archive_uri` do Terraform, usar `addPyFile()` nos scripts | Erro X509 (ver Problema 3) |
| 3 | **Scripts self-contained (sem dependências externas)** | **SUCESSO** ✅ |

### Solução Final

Abandonar `archive_uri` e `utils.zip` completamente. Cada script contém tudo o que precisa inline (ver Problema 4 para detalhes).

### Regra Aprendida

> **`archive_uri` no OCI Data Flow exige `conda pack`** para gerar o ZIP com o ambiente Python correto. Para projetos com poucas dependências (2 arquivos .py), o custo de configurar conda pack não compensa. Preferir scripts self-contained.

---

## Problema 3: addPyFile e X509FederationClient

### Sintoma

```
INFO X509FederationClient: Cannot renew security token.
java.lang.NullPointerException
    at com.oracle.bmc.auth.internal.X509FederationClient.refreshAndGetSecurityTokenInner
```

O script falhava ao tentar carregar um arquivo `.py` do Object Storage via `addPyFile()` no início da execução.

### Contexto

Após abandonar `archive_uri`, tentamos carregar as dependências diretamente no código dos scripts:

```python
# ❌ FALHOU - addPyFile antes do Resource Principal estar pronto
def _bootstrap_spark():
    spark = SparkSession.builder.appName("...").getOrCreate()
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"
    utils_uri = f"oci://hackathon-2025-landing-zone@{namespace}/libs/utils.zip"
    spark.sparkContext.addPyFile(utils_uri)
    return spark

spark = _bootstrap_spark()
from spark_utils import get_spark_session
```

### Causa Raiz

O `addPyFile("oci://...")` precisa acessar o Object Storage via HDFS Connector, que usa **Resource Principal** para autenticação. O problema é uma **race condition**: no momento em que `addPyFile()` é chamado (início do script, antes de qualquer operação Spark), o token de autenticação do Resource Principal pode ainda não estar totalmente inicializado.

O fluxo de inicialização do Data Flow:

```
1. Data Flow provisiona containers Spark
2. SparkSession é criada (parcial)
3. Resource Principal token é obtido (X509)  ← pode não estar pronto
4. Script do usuário começa a executar
5. addPyFile("oci://...") tenta ler Object Storage  ← FALHA se (3) não completou
6. Primeira operação Spark real (read/write)  ← aqui o token já está pronto
```

### Por que o read/write funciona mas addPyFile não?

As operações `spark.read.format(...).load("oci://...")` são **lazy** — elas são avaliadas apenas quando uma action (como `.count()` ou `.write()`) é chamada. Nesse ponto, o Resource Principal já está totalmente inicializado. O `addPyFile()` é **eager** — tenta baixar o arquivo imediatamente, antes que o token esteja pronto.

### Solução Final

Eliminar `addPyFile()` completamente. Cada script é auto-suficiente (ver Problema 4).

### Regra Aprendida

> **`addPyFile("oci://...")` não é confiável no OCI Data Flow** devido à race condition com Resource Principal. Funciona com paths locais ou HDFS, mas **não funciona de forma consistente com URIs `oci://`** no início do script. Preferir scripts self-contained ou usar `--py-files` via API de submissão (não testado).

---

## Problema 4: A Solução — Scripts Self-Contained

### Estratégia

Ao invés de compartilhar código via módulos Python (`from spark_utils import ...`), cada script contém **inline** apenas as funções que realmente usa:

```
┌─────────────────────────────────────────────────────────────────┐
│  ANTES (Databricks)                                             │
│                                                                 │
│  bronze_telco.py ──import──→ src/utils/spark_utils.py           │
│  silver_telco.py ──import──→ src/utils/spark_utils.py           │
│  abt_v5_builder.py ──import──→ src/utils/validate_abt.py       │
│                                                                 │
│  1 módulo compartilhado, N scripts dependentes                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  DEPOIS (OCI Data Flow)                                         │
│                                                                 │
│  bronze_telco.py ──→ SparkSession inline (3 linhas)             │
│  silver_telco.py ──→ standardize_column_names + to_int_safe     │
│                      + to_double_safe + treat_sentinel_value    │
│                      (funções copiadas inline)                  │
│  abt_v5_builder.py ──→ SparkSession inline                     │
│                        + validate_abt comentado (TODO)          │
│                                                                 │
│  0 dependências externas, cada script é independente            │
└─────────────────────────────────────────────────────────────────┘
```

### Categorias de Scripts

A quantidade de código inlinado varia por camada:

| Camada | O que precisa | Ação Tomada |
|--------|---------------|-------------|
| **Bronze** (6 scripts) | Apenas `get_spark_session()` | Substituído por `SparkSession.builder.appName(...).getOrCreate()` — 1 linha |
| **Gold** (3 scripts) | Apenas `get_spark_session()` | Idem Bronze — 1 linha |
| **ABT v6** (1 script) | Apenas `get_spark_session()` | Idem Bronze — 1 linha |
| **Silver** (6 scripts) | `get_spark_session()` + funções de tipagem/limpeza | Funções copiadas inline no topo do script (cada script copia só as que usa) |
| **ABT v1-v5** (5 scripts) | `get_spark_session()` + `validate_abt()` | SparkSession inline + `validate_abt` comentado com `# TODO` |

### Exemplo: Bronze (Antes vs Depois)

**ANTES (Databricks):**

```python
# src/jobs/00_bronze/01_ingest_telco.py
from src.utils.spark_utils import get_spark_session

DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/source/base_telco/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/bronze/telco_delta/"

def add_metadata(df):
    return df \
        .withColumn("metadata_nome_arquivo_origem", F.col("_metadata.file_path"))
        #                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        #                                           Proprietário Databricks/Unity Catalog

def main():
    spark = get_spark_session("Bronze_Ingestion_Telco")
    #       ^^^^^^^^^^^^^^^^^
    #       Import externo (src.utils.spark_utils)
    ...
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    df_bronze.write.saveAsTable("hackathon_2025.default.bronze_telco")
    #               ^^^^^^^^^^^
    #               Unity Catalog - não existe no OCI
```

**DEPOIS (OCI Data Flow):**

```python
# mig_oci/data_upload/scripts/bronze_telco.py
from pyspark.sql import SparkSession  # Import direto, sem módulo externo

namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-landing-zone@{namespace}/source/telco/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/telco/"
#                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#                      URIs OCI com namespace dinâmico (via argumento CLI)

def add_metadata(df):
    return df \
        .withColumn("metadata_nome_arquivo_origem", F.input_file_name())
        #                                           ^^^^^^^^^^^^^^^^^^^^
        #                                           Padrão Apache Spark

def main():
    spark = SparkSession.builder.appName("Bronze_Ingestion_Telco").getOrCreate()
    #       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #       Direto, sem import externo. Data Flow já pré-configura Delta Lake.
    ...
    # saveAsTable REMOVIDO (sem Unity Catalog no OCI)
```

### Exemplo: Silver (Funções Inlinadas)

Os scripts Silver precisam de funções utilitárias para tipagem e limpeza. Cada script copia inline apenas as funções que usa:

**ANTES (Databricks):**

```python
# src/jobs/01_silver/01_bronze_silver_telco.py
from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe,
    treat_sentinel_value
)
```

**DEPOIS (OCI Data Flow):**

```python
# mig_oci/data_upload/scripts/silver_telco.py

# =============================================================================
# FUNÇÕES UTILITÁRIAS (inline - adaptado de spark_utils.py)
# No OCI Data Flow, não é possível importar módulos externos de forma confiável.
# Cada script inclui apenas as funções que utiliza.
# =============================================================================

def standardize_column_names(df):
    new_cols = []
    for col in df.columns:
        clean_col = col.lower().strip() \
            .replace(" ", "_").replace("/", "_").replace(".", "") \
            .replace("ç", "c").replace("ã", "a")
        new_cols.append(clean_col)
    return df.toDF(*new_cols)

def to_int_safe(colname):
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

def to_double_safe(colname):
    return F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)
    ).when(
        F.trim(F.col(colname)).rlike("^[+-]?([0-9]*[.,])?[0-9]+$"),
        F.col(colname).cast("double")
    ).otherwise(F.lit(None))

def treat_sentinel_value(colname, sentinel_values=[304]):
    sentinel_str_values = [str(s) for s in sentinel_values]
    sentinel_condition = F.col(colname).isin(sentinel_str_values)
    expr_treated = F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == "") | sentinel_condition,
        F.lit(None)
    ).otherwise(F.col(colname).cast("double"))
    expr_flag = F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == "") | sentinel_condition,
        F.lit(1)
    ).otherwise(F.lit(0))
    return {
        "colname_treated": f"{colname}_adj",
        "flag_name": f"flag_{colname}_missing",
        "expr_treated": expr_treated, "expr_flag": expr_flag
    }
```

### Trade-off

| Aspecto | Módulo Compartilhado | Scripts Self-Contained |
|---------|---------------------|----------------------|
| **DRY** (Don't Repeat Yourself) | ✅ Uma cópia só | ❌ Código duplicado em N scripts |
| **Facilidade de deploy** | ❌ ZIP, conda pack, addPyFile | ✅ Upload direto do .py |
| **Confiabilidade no OCI** | ❌ Race condition, versão Python | ✅ Zero dependências |
| **Manutenção** | ✅ Alterar 1 arquivo | ❌ Alterar N arquivos |
| **Debugging** | ❌ Erro pode ser do loader | ✅ Erro é sempre do script |

**Decisão:** Para o contexto do hackathon (21 scripts, 5 funções utilitárias), a duplicação é aceitável. A confiabilidade e simplicidade de deploy compensam a violação do DRY. Se o projeto crescer, considerar `--py-files` via API de submissão do Data Flow (não via `addPyFile` no código).

---

## Problema 5: Dados Ausentes no Bucket

### Sintoma

```
INFO X509FederationClient: Cannot renew security token.
java.lang.NullPointerException
```

O script `bronze_bureau.py` falhava com o mesmo erro X509 que havia sido corrigido nos Problemas 1-3. Porém, o script já estava atualizado (sem `addPyFile`, sem propriedade reservada).

### Investigação

1. Verificamos que o script no bucket estava atualizado (sem `addPyFile`)
2. O mesmo script funcionou na segunda tentativa? Não — falhou novamente
3. Hipótese: erro transiente do Resource Principal? Descartado após 2 falhas consecutivas

### Causa Raiz

Os dados da base bureau **não existiam** no bucket `landing-zone`. Apenas a base telco havia sido enviada. O erro X509 era uma **mensagem enganosa** — o Data Flow falhou ao tentar listar/ler arquivos de um prefixo vazio (`source/bureau/`), e reportou como erro de autenticação.

### Solução

Upload de todos os 6 conjuntos de dados brutos para o bucket:

```
oci://hackathon-2025-landing-zone@namespace/
└── source/
    ├── bureau/*.parquet     ← faltava
    ├── telco/*.parquet      ← já existia
    ├── cadastro/*.parquet   ← faltava
    ├── recarga/*.parquet    ← faltava
    ├── pagamento/*.parquet  ← faltava
    └── atraso/*.parquet     ← faltava
```

Após o upload, todos os 6 scripts Bronze executaram com sucesso.

### Regra Aprendida

> **Erros X509/NullPointerException no OCI Data Flow podem indicar dados ausentes**, não necessariamente problemas de autenticação. Quando o Resource Principal não consegue listar objetos em um prefixo vazio, a mensagem de erro é enganosa. **Sempre verificar se os dados de entrada existem no bucket** antes de debugar autenticação.

---

## Problema 6: Namespace Dinâmico

### Contexto

Os paths OCI Object Storage seguem o formato `oci://bucket@namespace/path`. O namespace é específico da tenancy OCI e não deve ser hardcoded nos scripts.

### Solução Adotada

O namespace é passado como **primeiro argumento posicional** do script, configurado no Terraform:

**Terraform (`compute/main.tf`):**

```hcl
resource "oci_dataflow_application" "apps" {
  ...
  arguments = [var.namespace]   # Primeiro argumento = namespace da tenancy
}
```

**Python (todos os scripts):**

```python
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH  = f"oci://hackathon-2025-landing-zone@{namespace}/source/telco/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/telco/"
```

O fallback `default_namespace` permite rodar o script localmente para debug (embora os paths OCI não funcionem sem autenticação).

---

## Problema 7: Quota do Free Tier e Shapes

### Sintoma

```
Error: 400-LimitExceeded - Limits for shape VM.Standard.E4.Flex have been exceeded
```

Ao aplicar o Terraform com 21 Data Flow Applications, a quota de OCPUs disponíveis na conta free tier foi excedida.

### Causa Raiz

A configuração original alocava OCPUs fixas por camada:

| Camada | OCPUs/app | Apps | Driver | Executors | Total OCPUs |
|--------|-----------|------|--------|-----------|-------------|
| Bronze | 2 | 6 | 2 | 4×2=8 | 60 |
| Silver | 4 | 6 | 4 | 8×4=32 | 216 |
| Gold | 4 | 3 | 4 | 16×4=64 | 204 |
| ABT | 4 | 6 | 4 | 8×4=32 | 216 |
| **Total** | | **21** | | | **696** |

A conta free tier tem limite de ~96 OCPUs para VM.Standard.E4.Flex.

### Solução

Reduzir para configuração mínima viável:

```hcl
# Todas as apps: 1 OCPU, 1 executor (mínimo do Data Flow)
ocpu          = 1
num_executors = 1
```

**Nota:** Isso é suficiente para o hackathon (datasets < 500MB). Para produção, usar shapes maiores conforme necessidade e quota disponível.

---

## Tabela de Mudanças: Databricks → OCI Data Flow

### Mudanças por Categoria

| # | Mudança | Databricks | OCI Data Flow | Motivo |
|---|---------|------------|---------------|--------|
| 1 | **Paths** | `/Volumes/hackathon_2025/default/...` | `oci://bucket@namespace/...` | Object Storage vs Unity Catalog Volumes |
| 2 | **SparkSession** | `get_spark_session()` (módulo externo) | `SparkSession.builder.appName(...).getOrCreate()` (inline) | Evitar addPyFile/archive_uri |
| 3 | **Metadados** | `F.col("_metadata.file_path")` | `F.input_file_name()` | `_metadata` é proprietário Databricks |
| 4 | **saveAsTable** | `.saveAsTable("hackathon_2025.default.xxx")` | Removido | Unity Catalog não existe no OCI |
| 5 | **try_cast** | `F.expr("try_cast(x as int)")` | `to_int_safe()` (inline) | `try_cast` é proprietário Databricks |
| 6 | **Imports** | `from src.utils.spark_utils import ...` | Funções copiadas inline | addPyFile não funciona com `oci://` |
| 7 | **Namespace** | Hardcoded (Databricks gerencia) | `sys.argv[1]` (argumento CLI via Terraform) | Namespace da tenancy OCI |
| 8 | **Delta Lake** | Configurado pelo cluster Databricks | `configuration{}` no Terraform | Data Flow não tem cluster persistente |

### Mudanças por Script

| Script | Mudanças Aplicadas (referência #) |
|--------|----------------------------------|
| `bronze_*.py` (6) | #1, #2, #3, #4, #7 |
| `silver_*.py` (6) | #1, #2, #3, #4, #5, #6, #7 |
| `gold_*.py` (3) | #1, #2, #4, #7 |
| `abt_v6_builder.py` | #1, #2, #4, #7 |
| `abt_v1-v5_builder.py` (5) | #1, #2, #4, #7 + validate_abt comentado |

---

## Checklist de Verificação (para próximas etapas)

Antes de executar qualquer script no OCI Data Flow, verificar:

- [ ] **Script não contém `addPyFile("oci://...")`** — usar funções inline
- [ ] **Script não configura `spark.hadoop.fs.oci.*`** — propriedades reservadas
- [ ] **Script não usa `F.col("_metadata.*")`** — usar `F.input_file_name()`
- [ ] **Script não usa `F.expr("try_cast(...)")`** — usar `to_int_safe()` / `to_double_safe()`
- [ ] **Script não usa `.saveAsTable()`** — apenas `.save()` com Delta
- [ ] **Paths usam formato `oci://bucket@namespace/path`** com namespace dinâmico
- [ ] **Dados de entrada existem no bucket** (prefixo correto)
- [ ] **Script foi enviado ao bucket** via `upload_scripts.sh`
- [ ] **Terraform foi aplicado** após alterações no script (`./apply_phase.sh 4`)

---

## Glossário de Erros

| Erro | Causa Provável | Solução |
|------|----------------|---------|
| `ArchiveWarn: Incorrect python version` | `utils.zip` criado com `zip` em vez de `conda pack` | Usar scripts self-contained |
| `X509FederationClient: Cannot renew security token` | addPyFile antes do RP estar pronto OU dados ausentes no bucket | Remover addPyFile + verificar dados |
| `NullPointerException` em `X509FederationClient` | Race condition na inicialização do Resource Principal | Remover addPyFile |
| `FILE_URL_INVALID` no `terraform apply` | Script referenciado no `file_uri` não existe no bucket | Upload scripts ANTES do apply |
| `LimitExceeded` para `VM.Standard.E4.Flex` | Quota de OCPUs excedida no free tier | Reduzir OCPUs e executors |
| `spark.hadoop.fs.oci.client.hostname` erro silencioso | Propriedade reservada configurada no código | Remover do `get_spark_session()` |

---

## Referências

- [FASE_6A_LANDING_BRONZE.md](FASE_6A_LANDING_BRONZE.md) — Documentação funcional do pipeline Landing → Bronze
- [FASE_4_IMPLEMENTACAO.md](FASE_4_IMPLEMENTACAO.md) — Implementação do módulo Compute (Data Flow)
- [OCI Data Flow - Third Party Libraries](https://docs.oracle.com/en-us/iaas/data-flow/using/third-party-libraries.htm) — Documentação oficial sobre dependências
- [OCI Data Flow - Spark Configuration](https://docs.oracle.com/en-us/iaas/data-flow/using/spark-config.htm) — Propriedades reservadas
