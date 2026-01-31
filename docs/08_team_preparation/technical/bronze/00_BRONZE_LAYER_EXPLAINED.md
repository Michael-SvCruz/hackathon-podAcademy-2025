# Camada Bronze - Documentação Técnica Detalhada

## Visão Geral

A camada **Bronze** é a primeira camada de dados processados na arquitetura Medallion. Sua função é:
- Ler dados brutos da Landing Zone (arquivos Parquet/CSV)
- Adicionar metadados de auditoria
- Salvar em formato Delta Lake

**Princípio fundamental:** Nenhuma transformação de negócio é feita na Bronze. Apenas metadados são adicionados.

---

## Scripts da Camada Bronze

| Script | Fonte | Destino |
|--------|-------|---------|
| `00_ingest_bureau_full.py` | base_score_bureau_movel_full/ | bronze_bureau_full_delta/ |
| `01_ingest_telco.py` | base_telco/ | bronze_telco_delta/ |
| `02_ingest_cadastro.py` | base_cadastro/ | bronze_cadastro_delta/ |
| `03_ingest_recarga.py` | base_recarga/ | bronze_recarga_delta/ |
| `04_ingest_pagamento.py` | base_pagamento/ | bronze_pagamento_delta/ |
| `05_ingest_atraso.py` | base_atraso/ | bronze_atraso_delta/ |

Todos seguem o **mesmo padrão**. Abaixo explicamos cada parte do código usando `00_ingest_bureau_full.py` como exemplo.

---

## Estrutura do Script (Explicação Linha a Linha)

### 1. Docstring Inicial (Linhas 1-17)

```python
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 00_ingest_bureau.py
OBJETIVO: Ingestão da camada Landing (Raw) para Bronze.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê os dados brutos (Parquet) do volume de origem (Landing),
adiciona metadados de auditoria (data de ingestão e origem) e salva
em formato Delta Lake na camada Bronze.

AJUSTES UNITY CATALOG:
- Utiliza caminhos no formato /Volumes/...
- Utiliza colunas ocultas (_metadata) para rastreio de origem.
- Trata argumentos de sistema do Databricks Notebook.
--------------------------------------------------------------------------------
"""
```

**Por que assim?**
- Docstring no topo é padrão Python (PEP 257)
- Facilita entendimento rápido do propósito do script
- Documenta peculiaridades do ambiente (Unity Catalog)

**Alternativa comum:** Não ter docstring ou ter apenas uma linha.
**Por que não usamos:** Equipe mista (júnior a sênior) precisa de contexto claro.

---

### 2. Imports (Linhas 19-23)

```python
import sys
import argparse
from pyspark.sql import functions as F
from src.utils.spark_utils import get_spark_session
```

**Explicação de cada import:**

| Import | Propósito |
|--------|-----------|
| `sys` | Permite `sys.exit(1)` para encerrar com erro |
| `argparse` | Parseamento de argumentos de linha de comando |
| `functions as F` | Funções do Spark (F.col, F.lit, F.current_timestamp) |
| `get_spark_session` | Função utilitária para criar SparkSession |

**Por que `functions as F`?**
- Convenção padrão da comunidade Spark
- Código mais curto: `F.col("x")` vs `functions.col("x")`
- Evita conflito com `col` como variável local

**Alternativa comum:** `from pyspark.sql.functions import col, lit, current_timestamp`
**Por que não usamos:** Importar tudo com `F` é mais flexível e evita imports longos.

---

### 3. Configuração de Caminhos (Linhas 25-32)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/source/base_score_bureau_movel_full/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/bronze/bureau_full_delta/"
DEFAULT_FORMAT = "parquet"
# =============================================================================
```

**Por que caminhos no topo?**
- Facilita mudança rápida durante desenvolvimento
- Visível imediatamente ao abrir o arquivo
- Funciona como "configuração" do script

**Por que `/Volumes/`?**
- É o padrão do Unity Catalog (Databricks)
- Substitui o antigo `dbfs:/` que tinha problemas de permissão
- Formato: `/Volumes/<catalog>/<schema>/<volume>/`

**Alternativa comum:** Usar variáveis de ambiente ou arquivo de config.
**Por que não usamos:** Para scripts simples de ETL, config no topo é suficiente e mais transparente.

---

### 4. Função de Metadados (Linhas 34-47)

```python
def add_metadata(df):
    """
    Adiciona colunas de controle exigidas na camada Bronze.

    NOTA SOBRE UNITY CATALOG:
    A função F.input_file_name() é bloqueada em alguns modos do UC.
    A forma correta é acessar a coluna oculta "_metadata.file_path".
    """
    print(">>> [Transform] Adicionando metadados de ingestão...")

    return df \
        .withColumn("metadata_data_ingestao", F.current_timestamp()) \
        .withColumn("metadata_nome_arquivo_origem", F.col("_metadata.file_path")) \
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING"))
```

**Explicação de cada coluna adicionada:**

| Coluna | Função | Valor |
|--------|--------|-------|
| `metadata_data_ingestao` | Quando o dado foi ingerido | Timestamp atual |
| `metadata_nome_arquivo_origem` | De qual arquivo veio | Path do arquivo Parquet |
| `metadata_sistema_origem` | Sistema de origem | Literal "HACKATHON_LANDING" |

**Por que `_metadata.file_path` e não `F.input_file_name()`?**

```python
# ❌ BLOQUEADO no Unity Catalog
.withColumn("arquivo", F.input_file_name())

# ✓ FUNCIONA no Unity Catalog
.withColumn("arquivo", F.col("_metadata.file_path"))
```

O Unity Catalog bloqueia `input_file_name()` por segurança. A coluna oculta `_metadata` é a alternativa oficial.

**Por que usar `\` para quebra de linha?**
- Permite encadear `.withColumn()` de forma legível
- Alternativa é usar parênteses: `(df.withColumn(...).withColumn(...))`
- Usamos `\` por ser padrão em muitos projetos Spark

---

### 5. Tratamento de Argumentos (Linhas 49-74)

```python
def main():
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze")
    parser.add_argument("--input_path", help="Caminho do arquivo na Landing Zone")
    parser.add_argument("--output_path", help="Caminho de destino na Bronze Zone")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem")

    # TRATAMENTO DE ARGUMENTOS (FIX PARA NOTEBOOK DATABRICKS)
    # O Databricks injeta argumentos como '-f /kernel/...' que quebram o parser.
    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()
```

**Por que `parse_known_args()` e não `parse_args()`?**

```python
# ❌ QUEBRA no Databricks Notebook
args = parser.parse_args()
# Erro: unrecognized arguments: -f /kernel/xyz

# ✓ FUNCIONA em qualquer lugar
args, unknown = parser.parse_known_args()
# Ignora argumentos desconhecidos
```

O Databricks injeta argumentos internos (`-f`, `-k`) que quebram o parser padrão.

**Por que criar uma classe Args inline?**

```python
class Args:
    input_path = DEFAULT_INPUT_PATH
    output_path = DEFAULT_OUTPUT_PATH
    format = DEFAULT_FORMAT
args = Args()
```

Isso permite usar `args.input_path` da mesma forma que usaríamos `args_parsed.input_path`.

**Alternativa comum:** Usar um dicionário.
```python
args = {"input_path": DEFAULT_INPUT_PATH, ...}
# Mas aí seria args["input_path"], não args.input_path
```
**Por que não usamos:** Consistência - queremos `args.input_path` em ambos os modos.

---

### 6. Leitura dos Dados (Linhas 76-97)

```python
    spark = get_spark_session("Bronze_Ingestion_Bureau")

    print(f">>> [Leitura] Lendo dados da Landing: {args.input_path}")

    try:
        if args.format == "csv":
            df_landing = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(args.input_path)
        else:
            df_landing = spark.read.format(args.format).load(args.input_path)

    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)
```

**Por que `get_spark_session()` customizada?**
- Centraliza configurações (Delta Lake, Unity Catalog)
- Código localizado em `src/utils/spark_utils.py`
- Evita duplicação de config em cada script

**Por que `inferSchema = "false"` para CSV?**

```python
# ❌ PERIGOSO: Spark infere tipos automaticamente
.option("inferSchema", "true")
# Problema: "123" pode virar int em um arquivo e string em outro

# ✓ SEGURO: Tudo vem como string
.option("inferSchema", "false")
# Silver layer faz a tipagem correta depois
```

Na Bronze, queremos preservar os dados exatamente como vieram. Tipagem é responsabilidade da Silver.

**Por que try/except com sys.exit(1)?**
- Falha explícita: se não conseguir ler, para o processo
- Código de saída 1: indica erro para sistemas de orquestração (Airflow, etc.)
- Print do erro: facilita debug

---

### 7. Transformação (Linha 102-103)

```python
    # Apenas adicionamos colunas de controle. Sem limpeza de negócio na Bronze.
    df_bronze = add_metadata(df_landing)
```

**Por que apenas uma linha?**
- Bronze é pass-through: dados entram, metadados são adicionados, dados saem
- Nenhuma limpeza, nenhuma transformação de negócio
- Filosofia: "dados brutos + rastreabilidade"

---

### 8. Escrita em Delta Lake (Linhas 105-121)

```python
    print(f">>> [Escrita] Salvando na camada Bronze (Delta): {args.output_path}")

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(f">>> [Sucesso] Processo finalizado. Total de registros: {df_bronze.count()}")
```

**Explicação das opções Delta:**

| Opção | Valor | Propósito |
|-------|-------|-----------|
| `format("delta")` | Delta Lake | Formato transacional com versionamento |
| `mode("overwrite")` | Sobrescreve | Carga full, não incremental |
| `mergeSchema` | true | Aceita novas colunas se origem mudar |
| `overwriteSchema` | true | Atualiza schema se tipos mudarem |

**Por que Delta e não Parquet puro?**

| Feature | Parquet | Delta |
|---------|---------|-------|
| ACID transactions | ❌ | ✓ |
| Time travel | ❌ | ✓ |
| Schema evolution | ❌ | ✓ |
| Upserts (MERGE) | ❌ | ✓ |

Delta adiciona camada transacional sobre Parquet. Essencial para pipelines confiáveis.

**Por que `mode("overwrite")` e não `mode("append")`?**
- Dados de origem são estáticos (hackathon)
- Overwrite garante consistência total
- Para dados incrementais, usaríamos append ou merge

---

### 9. Registro no Unity Catalog (Linhas 124-131)

```python
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    target_table = "hackathon_2025.default.bronze_bureau"
    df_bronze.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog, destino: {target_table}. ")
```

**Por que salvar duas vezes (Delta + Table)?**
- `.save(path)`: Salva arquivos Delta no Volume
- `.saveAsTable(table)`: Registra no catálogo como tabela SQL

**Por que isso importa?**
```sql
-- Com saveAsTable, podemos fazer:
SELECT * FROM hackathon_2025.default.bronze_bureau;

-- Sem saveAsTable, teríamos que:
SELECT * FROM delta.`/Volumes/.../bronze_bureau_full_delta/`;
```

**Nota:** O comentário diz "RETIRAR QUANDO PASSAR PARA OCI" porque Oracle Cloud tem catálogo diferente.

---

### 10. Ponto de Entrada (Linhas 134-135)

```python
if __name__ == "__main__":
    main()
```

**Por que usar `if __name__ == "__main__"`?**
- Permite executar o script diretamente: `python script.py`
- Permite importar funções sem executar: `from script import add_metadata`
- Padrão Python universal

---

## Resumo do Fluxo

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   LANDING       │      │   TRANSFORM     │      │   BRONZE        │
│   (Parquet/CSV) │ ──▶  │   (+ metadata)  │ ──▶  │   (Delta Lake)  │
│                 │      │                 │      │                 │
│ - Dados brutos  │      │ + data_ingestao │      │ + Versionamento │
│ - Sem controle  │      │ + arquivo_origem│      │ + ACID          │
│                 │      │ + sistema_origem│      │ + Catálogo      │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

---

## Checklist de Validação (Bronze)

Ao revisar ou criar um script Bronze, verifique:

- [ ] Docstring explicando o propósito
- [ ] Imports organizados (stdlib, third-party, local)
- [ ] Caminhos padrão no topo do arquivo
- [ ] `parse_known_args()` (não `parse_args()`)
- [ ] `_metadata.file_path` (não `input_file_name()`)
- [ ] `inferSchema = false` para CSV
- [ ] Apenas metadados adicionados (sem transformação de negócio)
- [ ] Delta Lake com mergeSchema e overwriteSchema
- [ ] Registro no Unity Catalog (saveAsTable)
- [ ] Logs com print (>>> prefixo)
- [ ] `if __name__ == "__main__"`

---

## Perguntas Frequentes

### "Por que não usar logging ao invés de print?"
Para scripts de ETL simples, print é suficiente e mais transparente no Databricks. Logging é recomendado para aplicações mais complexas.

### "Por que não parametrizar o nome da tabela?"
Para manter consistência. Cada script tem uma tabela específica. Parametrização excessiva dificulta manutenção.

### "Posso rodar este script localmente?"
Sim, desde que tenha Spark instalado e acesso aos volumes (ou mude os paths para locais).
