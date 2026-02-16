# Script 00: Bronze → Silver Bureau (Spine)

**Arquivo:** `src/jobs/01_silver/00_bronze_silver_bureau.py`
**Ordem no Pipeline:** 1º (deve ser executado primeiro)
**Função:** Criar o Spine oficial do projeto (universo de clientes)

---

## Visão Geral

O Bureau é a **base principal** do projeto. Contém:
- Chaves de identificação (NUM_CPF, SAFRA)
- Labels de decisão (FLAG_INSTALACAO) e risco (FPD)
- Scores de crédito (SCORE_01, SCORE_02)

**Grão:** 1 linha por NUM_CPF + SAFRA (cliente-mês)

---

## Código Completo Explicado

### Bloco 1: Docstring (Linhas 1-23)

```python
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 01_silver_bureau.py
OBJETIVO: Transformação da camada Bronze para Silver (Bureau Full / Spine).
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (bureau_full), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA), trata sentinelas de score e
garante o grão 1:1 por NUM_CPF + SAFRA através de deduplicação controlada.

REGRAS DE NEGÓCIO (BASEADO EM bureau.pdf):
- Grain esperado: 1 linha por NUM_CPF + SAFRA (Spine oficial).
- FLAG_INSTALACAO é label de decisão/política (0/1).
- FPD é label de risco (0/1) e pode ser nulo quando FLAG_INSTALACAO=0.
- SCORE_01 e SCORE_02 são features potenciais; inicialmente usaremos apenas SCORE_01.
- SCORE_01 = 0 deve ser tratado como sentinela/missing (criar flag e opcionalmente converter para NULL).

AJUSTES UNITY CATALOG:
- Leitura/escrita em Delta nos caminhos /Volumes/...
- Mantém colunas de auditoria da Bronze (opcional).
--------------------------------------------------------------------------------
"""
```

**Por que docstring detalhada?**
- Equipe mista (júnior a sênior) precisa de contexto claro
- Documenta regras de negócio diretamente no código
- Facilita manutenção futura

---

### Bloco 2: Imports (Linhas 25-30)

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session, standardize_column_names
```

**Explicação de cada import:**

| Import | Propósito |
|--------|-----------|
| `sys` | `sys.exit(1)` para encerrar com erro |
| `argparse` | Parseamento de argumentos CLI |
| `functions as F` | Funções Spark (F.col, F.when, F.lit) |
| `Window` | Funções de janela para deduplicação |
| `get_spark_session` | Cria SparkSession configurada |
| `standardize_column_names` | Converte colunas para snake_case |

**Por que `Window` é importado aqui?**
A deduplicação usa `row_number().over(Window)` para manter apenas o registro mais recente.

---

### Bloco 3: Configuração (Linhas 32-38)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/bureau_full_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
DEFAULT_FORMAT = "delta"
# =============================================================================
```

**Por que caminhos hardcoded no topo?**
- Facilita execução interativa (Notebook)
- Visível imediatamente ao abrir o arquivo
- Pode ser sobrescrito via argumentos CLI

---

### Bloco 4: Funções de Conversão Segura (Linhas 40-48)

```python
def to_int_safe(colname):
    """Converte string -> int de forma segura (vazio/null vira null)."""
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

def to_double_safe(colname):
    """Converte string -> double de forma segura (vazio/null vira null)."""
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("double"))
```

**Explicação linha a linha:**

```python
def to_int_safe(colname):
```
Define função que recebe nome da coluna como string.

```python
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None))
```
- `F.col(colname).isNull()` → Verifica se é NULL
- `F.trim(F.col(colname)) == ""` → Verifica se é string vazia (ou só espaços)
- `|` → Operador OR (se qualquer condição for verdadeira)
- `F.lit(None)` → Retorna NULL explícito

```python
            .otherwise(F.col(colname).cast("int"))
```
Se não for NULL nem vazio, converte para inteiro.

**Por que não usar apenas `.cast("int")` diretamente?**

```python
# ❌ PROBLEMA: string vazia vira 0 ou erro
F.col("score").cast("int")

# ✓ SOLUÇÃO: string vazia vira NULL
to_int_safe("score")
```

O `.cast()` direto pode converter "" para 0 ou gerar erro, dependendo do Spark. Nossa função garante comportamento consistente.

---

### Bloco 5: Função build_silver (Linhas 50-134)

Esta é a função principal de transformação. Vamos analisar cada parte.

#### 5.1: Tipagem Básica (Linhas 53-65)

```python
def build_silver(df_bronze):
    print(">>> [Transform] Tipagem + regras Silver (bureau_full)...")

    # 1) Tipagem básica
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
        .withColumn("score_01_dbl", to_double_safe("score_01"))
        .withColumn("score_02_dbl", to_double_safe("score_02"))
    )
```

**Explicação:**

| Coluna Original | Coluna Tipada | Tipo | Por quê? |
|-----------------|---------------|------|----------|
| `num_cpf` | `num_cpf` | string | CPF é identificador, não número matemático |
| `safra` | `safra` | string | YYYYMM é código, não número |
| `flag_instalacao` | `flag_instalacao_int` | int | Label binário (0/1) |
| `fpd` | `fpd_int` | int | Label binário (0/1) |
| `score_01` | `score_01_dbl` | double | Score numérico contínuo |
| `score_02` | `score_02_dbl` | double | Score numérico contínuo |

**Por que criar novas colunas (ex: `_int`, `_dbl`) ao invés de sobrescrever?**
- Preserva coluna original para debug
- Evita perda de dados se conversão falhar
- Padrão de imutabilidade (dados originais intocados)

**Alternativa comum:**
```python
# ❌ Sobrescreve coluna original
df = df.withColumn("flag_instalacao", F.col("flag_instalacao").cast("int"))
```

**Por que não usamos:** Se a conversão falhar, perdemos o dado original.

#### 5.2: Derivação de DT_SAFRA (Linhas 67-72)

```python
    # 2) DT_SAFRA (primeiro dia do mês)
    # SAFRA vem como YYYYMM -> YYYY-MM-01
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )
```

**Explicação passo a passo:**

1. `F.col("safra")` → Pega valor "202401"
2. `F.lit("01")` → Literal "01"
3. `F.concat(...)` → Concatena: "20240101"
4. `F.to_date(..., "yyyyMMdd")` → Converte para date: 2024-01-01

**Por que criar DT_SAFRA?**
- Operações de data são mais fáceis com tipo date
- Permite `F.months_between()`, `F.add_months()`, etc.
- Joins temporais ficam mais simples

#### 5.3: Tratamento de Sentinela SCORE_01 (Linhas 74-81)

```python
    # 3) Tratamento de sentinela (SCORE_01 = 0)
    df = df.withColumn(
        "flag_score01_missing",
        F.when(F.col("score_01_dbl").isNull() | (F.col("score_01_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "score_01_adj",
        F.when(F.col("score_01_dbl") == 0, F.lit(None)).otherwise(F.col("score_01_dbl"))
    )
```

**O que é sentinela?**
Valor especial que significa "não informado" ou "não aplicável". No SCORE_01:
- **Valor 0** = cliente sem score no bureau (não é score real de zero)

**Tratamento:**

| Coluna | Propósito |
|--------|-----------|
| `flag_score01_missing` | 1 se score é NULL ou 0, senão 0 |
| `score_01_adj` | Score real ou NULL (0 convertido para NULL) |

**Por que criar flag + coluna ajustada?**
- O modelo pode usar a flag como feature (ausência de score pode ser preditiva)
- A coluna ajustada evita que 0 seja tratado como score válido em médias/agregações

**Alternativa comum:** Apenas converter 0 para NULL.
**Por que não usamos:** Perdemos a informação de que o cliente não tinha score.

#### 5.4: Quality Gates (Linhas 89-96)

```python
    # 4) Quality gates simples (domínio)
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when(~F.col("flag_instalacao_int").isin(0, 1) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when(~F.col("fpd_int").isin(0, 1) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
```

**Explicação:**

```python
~F.col("flag_instalacao_int").isin(0, 1)
```
- `isin(0, 1)` → Verifica se está em {0, 1}
- `~` → Operador NOT (negação)
- Resultado: True se valor NÃO está em {0, 1}

```python
& F.col("flag_instalacao_int").isNotNull()
```
- `&` → Operador AND
- Só marca como inválido se não for NULL (NULL é válido = ausência de info)

**Por que criar flags de validação?**
- Permite identificar dados problemáticos sem removê-los
- Análise posterior pode filtrar por qualidade
- Evita decisões precipitadas sobre o que descartar

#### 5.5: Metadados (Linhas 98-100)

```python
    # Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
                     .withColumn("metadata_versao_regra", F.lit("silver_bureau_full_v1"))
```

**Por que adicionar metadados?**
- Rastreabilidade: quando este registro foi transformado?
- Versionamento: qual versão das regras foi aplicada?
- Debug: se algo der errado, sabemos qual pipeline processou

#### 5.6: Seleção Final de Colunas (Linhas 102-132)

```python
    # 5) Seleção de colunas finais (Silver "clean")
    df_silver = df.select(
        "num_cpf",
        "safra",
        "dt_safra",

        "flag_instalacao_int",
        "fpd_int",

        # Features principais
        "score_01_adj",
        "flag_score01_missing",
        "score_02_dbl",
        "flag_score02_missing",

        "prod",
        "flag_mig2",

        "flag_instalacao_invalida",
        "fpd_invalido",

        # auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    return df_silver
```

**Por que selecionar colunas explicitamente?**
- Remove colunas originais desnecessárias
- Documenta quais colunas saem da Silver
- Garante ordem consistente
- Evita arrastar colunas de debug para produção

**Alternativa comum:** `df.drop("col1", "col2", ...)`
**Por que não usamos:** Com muitas colunas, é mais seguro listar o que QUER do que o que NÃO quer.

---

### Bloco 6: Função de Deduplicação (Linhas 136-148)

```python
def dedupe_by_key(df_silver):
    """
    Garante grão 1:1 por NUM_CPF + SAFRA.
    Critério de desempate: metadata_data_ingestao DESC (mais recente).
    """
    print(">>> [Transform] Deduplicação por num_cpf + safra (se necessário)...")

    w = Window.partitionBy("num_cpf", "safra").orderBy(F.col("metadata_data_ingestao").desc())

    df_ranked = df_silver.withColumn("rn", F.row_number().over(w))
    df_out = df_ranked.filter(F.col("rn") == 1).drop("rn")

    return df_out
```

**Explicação linha a linha:**

```python
w = Window.partitionBy("num_cpf", "safra").orderBy(F.col("metadata_data_ingestao").desc())
```
- `partitionBy("num_cpf", "safra")` → Agrupa por chave composta
- `orderBy(...desc())` → Ordena do mais recente para mais antigo

```python
df_ranked = df_silver.withColumn("rn", F.row_number().over(w))
```
- `row_number().over(w)` → Numera linhas dentro de cada grupo (1, 2, 3...)
- Linha mais recente recebe `rn = 1`

```python
df_out = df_ranked.filter(F.col("rn") == 1).drop("rn")
```
- `filter(rn == 1)` → Mantém apenas a primeira linha de cada grupo
- `.drop("rn")` → Remove coluna auxiliar

**Por que usar row_number e não dropDuplicates?**

```python
# ❌ dropDuplicates: não controla qual linha manter
df.dropDuplicates(["num_cpf", "safra"])

# ✓ row_number: mantém linha mais recente
Window.partitionBy(...).orderBy(F.col("metadata_data_ingestao").desc())
```

`dropDuplicates` é não-determinístico (mantém uma linha arbitrária). Com `row_number`, controlamos qual manter.

---

### Bloco 7: Função main() (Linhas 150-228)

#### 7.1: Parseamento de Argumentos (Linhas 150-166)

```python
def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Bureau Full")
    parser.add_argument("--input_path", help="Caminho da Bronze (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino na Silver (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem (delta)")

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
O Databricks injeta argumentos internos (`-f`, `-k`) que quebram `parse_args()`.

#### 7.2: Leitura e Transformação (Linhas 168-193)

```python
    spark = get_spark_session("Silver_Bureau_Full")

    # 1) Leitura Bronze
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # 1.5) Padronização de nomes de coluna
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # 2) Transform Silver
    df_silver = build_silver(df_bronze)

    # 3) Dedup por chave do spine
    df_silver_dedup = dedupe_by_key(df_silver)

    count_out = df_silver_dedup.count()
    print(f">>> [Info] Registros na Silver (após dedupe): {count_out}")
    print(f">>> [Info] Linhas removidas no dedupe: {count_in - count_out}")
```

**Por que `standardize_column_names` antes de `build_silver`?**
A função `build_silver` espera colunas em snake_case. Se a Bronze vier com `NUM_CPF` (maiúsculo), quebraria.

#### 7.3: Escrita (Linhas 195-213)

```python
    # 4) Escrita Silver
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")

    df_silver_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # ESCRITA TABLE PARA DATABRICKS
    target_table = "hackathon_2025.default.silver_bureau"
    df_silver_dedup.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog, destino: {target_table}. ")
```

**Dupla escrita: por quê?**
- `.save(path)` → Arquivos Delta no Volume (persistência)
- `.saveAsTable(table)` → Registro no catálogo (SQL acessível)

#### 7.4: Quality Checks (Linhas 216-228)

```python
    # 5) Quality checks (rápidos)
    print(">>> [Quality] Checando domínios e unicidade...")

    invalid_flag = df_silver_dedup.filter(F.col("flag_instalacao_invalida") == 1).count()
    invalid_fpd = df_silver_dedup.filter(F.col("fpd_invalido") == 1).count()
    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] distinct num_cpf+safra: {distinct_key} | total_out: {count_out}")

    print(f">>> [Sucesso] Silver bureau_full concluído.")
```

**O que esses checks validam?**
1. `invalid_flag` → Quantos registros têm FLAG fora de {0, 1}?
2. `invalid_fpd` → Quantos registros têm FPD fora de {0, 1}?
3. `distinct_key == count_out` → Grão 1:1 está garantido?

---

## Resumo do Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  bureau_full    │
│  (raw + meta)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ standardize     │  ← snake_case
│ column_names    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ build_silver()  │  ← Tipagem, sentinela, derivadas, flags
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ dedupe_by_key() │  ← Garante 1:1 por CPF+SAFRA
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  bureau_full    │
│  (typed, clean) │
└─────────────────┘
```

---

## Checklist de Validação

- [x] Docstring completa com regras de negócio
- [x] `parse_known_args()` para compatibilidade Databricks
- [x] `standardize_column_names()` aplicado
- [x] Tipagem explícita (int, double, string, date)
- [x] Sentinela SCORE_01=0 tratado (NULL + flag)
- [x] DT_SAFRA derivada
- [x] Quality gates (flag_invalida, fpd_invalido)
- [x] Deduplicação por row_number (não dropDuplicates)
- [x] Metadados de transformação
- [x] Dupla escrita (Delta + Unity Catalog)
- [x] Logs com prefixo >>>
