# Script 01: Bronze → Silver Telco

**Arquivo:** `src/jobs/01_silver/01_bronze_silver_telco.py`
**Ordem no Pipeline:** 2º (após Bureau)
**Função:** Processar 68 variáveis anônimas de uso de telefonia (var_26 a var_93)

---

## Visão Geral

A base Telco contém **variáveis anônimas** de comportamento de uso de telefonia. O principal desafio é o tratamento do **sentinela 304** que aparece em muitas variáveis.

**Grão:** 1 linha por NUM_CPF + SAFRA (cliente-mês)

**Particularidades:**
- 68 variáveis anônimas (var_26 a var_93)
- Sentinela 304 = "não informado/não aplicável"
- ~50% dos registros têm 304 em algumas variáveis

---

## Código Completo Explicado

### Bloco 1: Docstring (Linhas 1-29)

```python
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 01_bronze_silver_telco.py
OBJETIVO: Transformação da camada Bronze para Silver - Base TELCO.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (telco), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA), trata sentinelas de valor 304
em variáveis anonimizadas (var_26 a var_93) e garante o grão 1:1 por
NUM_CPF + SAFRA através de deduplicação controlada.

REGRAS DE NEGÓCIO (BASEADO EM telco.md):
- Grain esperado: 1 linha por NUM_CPF + SAFRA (confirmado em EDA).
- FLAG_INSTALACAO é label de decisão/política (0/1).
- FPD é label de risco (0/1) com ~3,36% de missing (usar com cautela).
- Variáveis var_26 a var_93: features anonimizadas (cast para double).
- SENTINELA 304: muito frequente em var_* (não informado/não aplicável).
  → Trata convertendo 304 para NULL e criando flag de missing.
- PROD e flag_mig2: metadados de origem (manter como string).

ANTI-LEAKAGE:
- FPD e FLAG_INSTALACAO não devem ser usados como features (apenas labels/auditoria).
--------------------------------------------------------------------------------
"""
```

**Destaques da docstring:**
- Documenta o sentinela 304 (regra de negócio crítica)
- Alerta sobre FPD com ~3.36% missing
- Explicita regra de anti-leakage

---

### Bloco 2: Imports (Linhas 31-42)

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe,
    treat_sentinel_value
)
```

**Diferença do Bureau:**
Importa `treat_sentinel_value`, função específica para tratamento de sentinelas.

**O que faz `treat_sentinel_value`?**
Recebe nome da coluna e lista de sentinelas, retorna:
- Coluna ajustada (sentinela → NULL)
- Flag indicando se era sentinela

---

### Bloco 3: Configuração (Linhas 44-53)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/telco_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/telco_silver_delta/"
DEFAULT_FORMAT = "delta"

# Lista de colunas var_* esperadas (var_26 a var_93 = 68 colunas)
VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# =============================================================================
```

**Por que gerar lista de colunas dinamicamente?**

```python
# ✓ Dinâmico: fácil de manter
VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# Resultado: ["var_26", "var_27", ..., "var_93"]

# ❌ Manual: propenso a erro, difícil de manter
VAR_COLUMNS = ["var_26", "var_27", "var_28", ...]  # 68 itens!
```

---

### Bloco 4: Função build_silver (Linhas 55-146)

#### 4.1: Tipagem Básica (Linhas 67-79)

```python
def build_silver(df_bronze):
    """
    Aplica tipagem explícita, cria derivadas e trata sentinelas da Telco.
    """
    print(">>> [Transform] Tipagem + regras Silver (telco)...")

    # 1) Tipagem básica
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
    )
```

**Idêntico ao Bureau:** Tipagem das colunas-chave e labels.

#### 4.2: Derivação de DT_SAFRA (Linhas 81-86)

```python
    # 2) DT_SAFRA (primeiro dia do mês)
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )
```

**Idêntico ao Bureau:** YYYYMM → YYYY-MM-01.

#### 4.3: Tratamento de Sentinela 304 (Linhas 88-98) ⭐

```python
    # 3) Casting de var_* para double com tratamento de sentinela 304
    print(">>> [Transform] Tratando sentinela 304 em var_*...")

    for var_col in VAR_COLUMNS:
        if var_col in df.columns:
            treatment = treat_sentinel_value(var_col, sentinel_values=[304])
            df = df \
                .withColumn(treatment["colname_treated"], treatment["expr_treated"]) \
                .withColumn(treatment["flag_name"], treatment["expr_flag"])
        else:
            print(f"!!! AVISO: Coluna {var_col} não encontrada no DataFrame.")
```

**Explicação linha a linha:**

```python
for var_col in VAR_COLUMNS:
```
Itera sobre as 68 variáveis (var_26 a var_93).

```python
    if var_col in df.columns:
```
Verifica se a coluna existe (proteção contra schema incompleto).

```python
        treatment = treat_sentinel_value(var_col, sentinel_values=[304])
```
Chama função utilitária que retorna dicionário com:
- `colname_treated`: nome da coluna ajustada (ex: "var_26_adj")
- `expr_treated`: expressão Spark para criar coluna ajustada
- `flag_name`: nome da flag (ex: "flag_var_26_missing")
- `expr_flag`: expressão Spark para criar flag

```python
        df = df \
            .withColumn(treatment["colname_treated"], treatment["expr_treated"]) \
            .withColumn(treatment["flag_name"], treatment["expr_flag"])
```
Adiciona duas colunas ao DataFrame:
1. `var_XX_adj`: valor original ou NULL se era 304
2. `flag_var_XX_missing`: 1 se era 304 ou NULL, senão 0

**O que acontece internamente em `treat_sentinel_value`?**

```python
def treat_sentinel_value(colname, sentinel_values):
    """
    Trata sentinelas: valor vira NULL + flag de missing.
    """
    col_adj = f"{colname}_adj"
    flag_name = f"flag_{colname}_missing"

    # Expressão para coluna ajustada
    expr_treated = F.when(
        F.col(colname).isin(*sentinel_values) | F.col(colname).isNull(),
        F.lit(None)
    ).otherwise(F.col(colname).cast("double"))

    # Expressão para flag
    expr_flag = F.when(
        F.col(colname).isin(*sentinel_values) | F.col(colname).isNull(),
        F.lit(1)
    ).otherwise(F.lit(0))

    return {
        "colname_treated": col_adj,
        "expr_treated": expr_treated,
        "flag_name": flag_name,
        "expr_flag": expr_flag
    }
```

**Por que usar função separada ao invés de código inline?**

```python
# ❌ Código inline: repetitivo, difícil de manter
for var_col in VAR_COLUMNS:
    df = df.withColumn(
        f"{var_col}_adj",
        F.when(F.col(var_col) == 304, None).otherwise(F.col(var_col).cast("double"))
    ).withColumn(
        f"flag_{var_col}_missing",
        F.when(F.col(var_col) == 304, 1).otherwise(0)
    )

# ✓ Função reutilizável: DRY, testável
treatment = treat_sentinel_value(var_col, sentinel_values=[304])
```

#### 4.4: Quality Gates (Linhas 100-107)

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

**Idêntico ao Bureau:** Valida domínio de labels.

#### 4.5: Seleção Final com Unpacking (Linhas 113-144)

```python
    # 6) Seleção de colunas finais (Silver "clean")
    df_silver = df.select(
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels/Política (não usar como features)
        "flag_instalacao_int",
        "fpd_int",

        # Metadados de origem
        "prod",
        "flag_mig2",

        # Features var_* (versão ajustada com tratamento de 304)
        *[f"var_{i}_adj" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],

        # Flags de missing para var_*
        *[f"flag_var_{i}_missing" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],

        # Quality flags
        "flag_instalacao_invalida",
        "fpd_invalido",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
```

**Destaque: Unpacking com asterisco (*)**

```python
*[f"var_{i}_adj" for i in range(26, 94) if f"var_{i}" in df_bronze.columns]
```

**O que isso faz?**

1. `[f"var_{i}_adj" for i in range(26, 94)]` → Lista: ["var_26_adj", "var_27_adj", ...]
2. `if f"var_{i}" in df_bronze.columns` → Filtra apenas colunas que existem
3. `*[...]` → Unpacking: expande lista como argumentos individuais

**Equivalente sem unpacking:**

```python
# ❌ Sem unpacking: muito verboso
df_silver = df.select(
    "num_cpf",
    "safra",
    "var_26_adj",
    "var_27_adj",
    # ... mais 66 colunas ...
    "var_93_adj",
)
```

**Por que o `if` dentro da list comprehension?**
Proteção: se alguma coluna var_XX não existir no Bronze, não tenta selecioná-la.

---

### Bloco 5: Deduplicação (Linhas 148-160)

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

**Idêntico ao Bureau:** Mesma lógica de deduplicação.

---

### Bloco 6: Quality Checks Específicos (Linhas 228-241)

```python
    # 5) Quality checks (rápidos)
    print(">>> [Quality] Checando domínios e unicidade...")

    invalid_flag = df_silver_dedup.filter(F.col("flag_instalacao_invalida") == 1).count()
    invalid_fpd = df_silver_dedup.filter(F.col("fpd_invalido") == 1).count()
    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()
    fpd_null = df_silver_dedup.filter(F.col("fpd_int").isNull()).count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] fpd null: {fpd_null} ({fpd_null*100/count_out:.2f}%)")
    print(f">>> [Quality] distinct num_cpf+safra: {distinct_key} | total_out: {count_out}")
```

**Diferença do Bureau:**
Adiciona check de `fpd_null` porque Telco tem ~3.36% de FPD missing (documentado na docstring).

---

## Sentinela 304: Análise de Negócio

### O Que É 304?

Na base Telco, o valor **304** significa:
- "Não informado"
- "Não determinado"
- "Não aplicável"

### Por Que Tratar?

Se não tratarmos, 304 seria considerado um valor numérico válido:

```python
# ❌ Sem tratamento: média distorcida
df.agg(F.avg("var_26")).show()
# Resultado: 250.5 (304 está inflando a média!)

# ✓ Com tratamento: média correta
df.agg(F.avg("var_26_adj")).show()
# Resultado: 150.2 (304 virou NULL e foi ignorado)
```

### Impacto Esperado

| Variável | % com 304 | Ação |
|----------|-----------|------|
| var_26 | ~45% | 304 → NULL + flag |
| var_27 | ~50% | 304 → NULL + flag |
| ... | ... | ... |
| var_93 | ~30% | 304 → NULL + flag |

---

## Diagrama de Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  telco          │
│  (68 vars raw)  │
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
│ build_silver()  │
│                 │
│ ├─ Tipagem      │
│ ├─ DT_SAFRA     │
│ ├─ 304 → NULL   │  ← PRINCIPAL
│ ├─ Flags        │
│ └─ Select       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ dedupe_by_key() │  ← Garante 1:1
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  telco          │
│  (136 cols)     │  ← 68 adj + 68 flags
└─────────────────┘
```

---

## Colunas de Saída

| Categoria | Colunas | Quantidade |
|-----------|---------|------------|
| Chaves | num_cpf, safra, dt_safra | 3 |
| Labels | flag_instalacao_int, fpd_int | 2 |
| Metadados origem | prod, flag_mig2 | 2 |
| Features ajustadas | var_26_adj ... var_93_adj | 68 |
| Flags de missing | flag_var_26_missing ... flag_var_93_missing | 68 |
| Quality flags | flag_instalacao_invalida, fpd_invalido | 2 |
| Auditoria | metadata_* | 5 |
| **Total** | | **~150** |

---

## Diferenças do Bureau

| Aspecto | Bureau | Telco |
|---------|--------|-------|
| Sentinela | Score=0 | 304 |
| Tratamento | Manual (2 colunas) | Loop (68 colunas) |
| Função | Inline | `treat_sentinel_value()` |
| FPD missing | Não mencionado | ~3.36% |
| Colunas saída | ~20 | ~150 |

---

## Checklist de Validação

- [x] Docstring com regra do sentinela 304
- [x] Lista dinâmica de VAR_COLUMNS
- [x] `treat_sentinel_value()` para cada var_*
- [x] Unpacking (*) na seleção de colunas
- [x] Check de coluna existente antes de tratar
- [x] Flag de FPD null monitorado
- [x] Deduplicação por row_number
- [x] Metadados de transformação
