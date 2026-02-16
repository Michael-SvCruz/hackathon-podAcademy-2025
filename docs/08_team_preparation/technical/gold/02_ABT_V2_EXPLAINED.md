# 02 - ABT v2 Builder Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/01_gold_abt_v2_builder.py` |
| **Função** | Construir ABT v2 - adiciona Score_02 |
| **Input** | Silver Bureau (spine) |
| **Output** | Gold ABT v2 |
| **Registros** | 3,795,310 (1:1 com spine) |
| **Colunas** | ~18 |
| **Feature Blocks** | Score_01 + Score_02 |

---

## Contexto de Negócio

A ABT v2 **estende a v1** adicionando o segundo score de bureau:

1. **Mantém tudo de v1:** Chaves, labels, Score_01, metadados
2. **Adiciona Score_02:** Segundo score de bureau com tratamento de sentinela
3. **Permite medir incremento:** KS(v2) - KS(v1) = contribuição do Score_02

**Por que dois scores separados?**
- Score_01 e Score_02 são calculados com metodologias diferentes
- Podem capturar aspectos complementares do risco
- A ordem incremental permite isolar a contribuição de cada um

**Diferença entre Score_01 e Score_02:**
| Score | Característica | Cobertura Típica |
|-------|----------------|------------------|
| Score_01 | Score principal de bureau | ~98% |
| Score_02 | Score secundário/histórico | ~99% |

---

## O Que Muda em Relação à v1

| Aspecto | ABT v1 | ABT v2 |
|---------|--------|--------|
| **Features** | score_01_adj, flag_score01_missing | + score_02_adj, flag_score02_missing |
| **Colunas** | ~15 | ~18 (+3) |
| **Feature Blocks** | "score_01" | "score_01,score_02" |
| **Validação** | validate_abt_v1 | validate_abt_v2 |

---

## Código Explicado Linha por Linha

### 1. Imports e Configuração (Linhas 48-63)

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v2  # MUDOU: v2 em vez de v1

# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_SILVER_BUREAU_PATH = "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v2_delta/"  # MUDOU: v2
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v2"  # MUDOU: v2
```

**Diferenças em relação à v1:**
- Import de `validate_abt_v2` (validação específica para v2)
- Output path para `abt_v2_delta/`
- `GOLD_VERSION` atualizado para "gold_abt_v2"

---

### 2. Função build_abt_v2 - Seleção Inicial (Linhas 65-115)

```python
def build_abt_v2(df_bureau):
    """
    Constrói ABT v2: estende v1 com SCORE_02 como feature adicional.
    """
    print(">>> [Transform] Selecionando colunas para ABT v2...")

    # Seleção de colunas ordenadas logicamente
    df_abt = df_bureau.select(
        # CHAVES (obrigatórias para identificação)
        "num_cpf",
        "safra",
        "dt_safra",

        # LABELS (para auditoria e análise de impacto - NÃO usar como features)
        "flag_instalacao_int",
        "fpd_int",

        # FEATURES v1 (SCORE_01) - MANTIDAS DE V1
        "score_01_adj",
        "flag_score01_missing",

        # FEATURES v2 (SCORE_02) - NOVAS
        "score_02_dbl",            # Score 2 tipado (já vem como double da Silver)
        "flag_score02_missing",    # Flag de missing para score_02

        # METADADOS DE ORIGEM
        "prod",
        "flag_mig2",

        # AUDITORIA (rastreabilidade)
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
```

**Por que `score_02_dbl` e não `score_02_adj`?**
- Na Silver Bureau, Score_02 vem como `score_02_dbl` (tipado como double)
- O ajuste de sentinela (0 → NULL) é feito NESTE script, não na Silver
- Isso difere de Score_01, que já vem ajustado da Silver

**Padrão de nomenclatura:**
| Sufixo | Significado | Exemplo |
|--------|-------------|---------|
| `_dbl` | Tipado como double, sem ajuste | score_02_dbl |
| `_adj` | Ajustado (sentinela → NULL) | score_02_adj |
| `_missing` | Flag indicando missing/sentinela | flag_score02_missing |

---

### 3. Tratamento de Sentinela do Score_02 (Linhas 117-125)

```python
    # Tratar sentinela em SCORE_02: valor 0 é sentinela (não informado)
    # Converter para NULL e manter flag de missing
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    )
```

**Explicação do tratamento de sentinela:**

O valor `0` para Score_02 significa "não informado" (sentinela), não um score real de zero.

**Lógica do `score_02_adj`:**
```python
F.when(F.col("score_02_dbl") == 0, F.lit(None))  # Se 0, converte para NULL
 .otherwise(F.col("score_02_dbl"))                # Senão, mantém o valor original
```

**Lógica do `flag_score02_missing`:**
```python
F.when(
    F.col("score_02_dbl").isNull() |     # Se é NULL original
    (F.col("score_02_dbl") == 0),        # OU se é sentinela (0)
    F.lit(1)                             # → Flag = 1 (missing)
).otherwise(F.lit(0))                    # → Flag = 0 (tem valor)
```

**Por que criar uma flag separada?**
- Permite distinguir "NULL porque não informado" de "NULL porque erro"
- O modelo pode usar a flag como feature (missing pattern pode ser informativo)
- Preserva informação que seria perdida apenas com NULL

**Diagrama do tratamento:**
```
score_02_dbl    →    score_02_adj    |    flag_score02_missing
─────────────────────────────────────┼────────────────────────
     NULL       →        NULL        |           1
       0        →        NULL        |           1
     750        →        750         |           0
     500        →        500         |           0
```

---

### 4. Remoção da Coluna Original (Linha 127-128)

```python
    # Remover coluna score_02_dbl (usar apenas ajustada)
    df_abt = df_abt.drop("score_02_dbl")
```

**Por que remover `score_02_dbl`?**
- Evita confusão entre coluna original e ajustada
- O modelo deve usar apenas `score_02_adj`
- Reduz número de colunas desnecessárias

**Por que não usar apenas rename?**
```python
# ALTERNATIVA DESCARTADA: apenas renomear
df_abt = df_abt.withColumnRenamed("score_02_dbl", "score_02_adj")
```
Motivo: O rename não trataria o sentinela. Precisamos criar uma NOVA coluna com a lógica de ajuste.

---

### 5. Reordenação de Colunas (Linhas 130-159)

```python
    # Reordenar colunas para clareza (features agrupadas)
    df_abt = df_abt.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels
        "flag_instalacao_int",
        "fpd_int",

        # Features Score_01 (v1)
        "score_01_adj",
        "flag_score01_missing",

        # Features Score_02 (v2 novo)
        "score_02_adj",
        "flag_score02_missing",

        # Metadados
        "prod",
        "flag_mig2",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
```

**Por que fazer um segundo select()?**
- O primeiro select trouxe `score_02_dbl`
- Após o withColumn, temos `score_02_adj` (nova) E `score_02_dbl` (original)
- O drop removeu `score_02_dbl`
- O segundo select **garante a ordem lógica** das colunas

**Alternativa descartada:**
```python
# Fazer tudo em um único select com expressões inline
df_abt = df_bureau.select(
    "num_cpf",
    ...,
    F.when(F.col("score_02_dbl") == 0, None).otherwise(F.col("score_02_dbl")).alias("score_02_adj"),
    ...
)
```
Motivo: Código menos legível. Preferimos clareza sobre concisão.

---

### 6. Metadados Gold Atualizados (Linhas 161-167)

```python
    # Adicionar metadados de gold
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02"))  # MUDOU: agora tem 2 blocos

    return df_abt
```

**Mudança em `gold_feature_blocks`:**
- v1: `"score_01"`
- v2: `"score_01,score_02"`

**Por que listar os blocos como string separada por vírgula?**
- Simples de parsear
- Fácil de ler em queries SQL
- Documenta a evolução incremental

---

### 7. Relatório Final - Análise de Incremento (Linhas 265-290)

```python
    # Completude de features
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()

    score01_coverage = (count_out - score01_null) * 100 / count_out
    score02_coverage = (count_out - score02_null) * 100 / count_out

    print(f"\n>>> [Features] Completude:")
    print(f"    SCORE_01_ADJ: {score01_coverage:.2f}%")
    print(f"    SCORE_02_ADJ: {score02_coverage:.2f}%")
```

**Por que calcular completude de ambos os scores?**
- Permite comparar cobertura entre scores
- Identifica se Score_02 adiciona cobertura incremental
- Útil para entender o impacto potencial no modelo

---

#### 7.1 Análise de Sobreposição (Linhas 277-280)

```python
    # Incremento vs v1
    print(f"\n>>> [ΔKS] Impacto potencial do Score_02:")
    print(f"    Registros apenas com Score_01: {df_abt.filter((F.col('score_01_adj').isNotNull()) & (F.col('score_02_adj').isNull())).count()}")
    print(f"    Registros com ambos Scores: {df_abt.filter((F.col('score_01_adj').isNotNull()) & (F.col('score_02_adj').isNotNull())).count()}")
    print(f"    Registros com Score_02 mas não Score_01: {df_abt.filter((F.col('score_01_adj').isNull()) & (F.col('score_02_adj').isNotNull())).count()}")
```

**Explicação da análise de sobreposição:**

| Cenário | Filtro | Significado |
|---------|--------|-------------|
| Apenas Score_01 | `S1 NOT NULL AND S2 NULL` | Score_02 não adiciona info |
| Ambos Scores | `S1 NOT NULL AND S2 NOT NULL` | Informação complementar |
| Apenas Score_02 | `S1 NULL AND S2 NOT NULL` | Score_02 cobre gaps do Score_01 |

**Por que essa análise é importante?**
- Se Score_02 cobre registros onde Score_01 é NULL, adiciona valor
- Se são 100% sobrepostos, Score_02 pode ser redundante
- Ajuda a justificar a inclusão do Score_02 na apresentação

---

## Diagrama de Fluxo

```
┌─────────────────────────────────────────────────────────────────┐
│                    01_gold_abt_v2_builder.py                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐                                                │
│  │   LEITURA   │     Silver Bureau (spine)                      │
│  │             │     3,795,310 registros                        │
│  └──────┬──────┘                                                │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │  SELECT 1   │────▶│ Seleciona colunas v1 + score_02_dbl  │   │
│  │             │     │ (score_02 ainda com sentinela)       │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │  TRATAMENTO │────▶│ score_02_dbl == 0 → NULL             │   │
│  │  SENTINELA  │     │ Cria score_02_adj                    │   │
│  │             │     │ Atualiza flag_score02_missing        │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │    DROP     │────▶│ Remove score_02_dbl (usa só _adj)    │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │  SELECT 2   │────▶│ Reordena colunas logicamente         │   │
│  │             │     │ Chaves → Labels → Features → Meta    │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │  METADADOS  │────▶│ gold_version = "gold_abt_v2"         │   │
│  │    GOLD     │     │ gold_feature_blocks = "score_01,     │   │
│  │             │     │                        score_02"     │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  VALIDAÇÃO  │     validate_abt_v2()                          │
│  └──────┬──────┘                                                │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │   ESCRITA   │────▶│ Delta Lake + Unity Catalog           │   │
│  └──────┬──────┘     │ gold_abt_v2_delta/                   │   │
│         │            └──────────────────────────────────────┘   │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  RELATÓRIO  │     Completude + Análise de Sobreposição       │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Colunas de Saída (ABT v2)

| # | Coluna | Tipo | Origem | Papel | Novo em v2? |
|---|--------|------|--------|-------|-------------|
| 1 | num_cpf | string | Bureau | Chave | |
| 2 | safra | string | Bureau | Chave | |
| 3 | dt_safra | date | Bureau | Chave derivada | |
| 4 | flag_instalacao_int | int | Bureau | **LABEL** | |
| 5 | fpd_int | int | Bureau | **TARGET** | |
| 6 | score_01_adj | double | Bureau | Feature | |
| 7 | flag_score01_missing | int | Bureau | Flag missing | |
| 8 | **score_02_adj** | double | Bureau | **Feature** | **SIM** |
| 9 | **flag_score02_missing** | int | Bureau | **Flag missing** | **SIM** |
| 10 | prod | string | Bureau | Metadado | |
| 11 | flag_mig2 | int | Bureau | Metadado | |
| 12-16 | metadata_* | various | Bureau | Auditoria | |
| 17 | gold_version | string | Script | Versão | Atualizado |
| 18 | gold_build_date | timestamp | Script | Data build | |
| 19 | gold_feature_blocks | string | Script | Blocos | Atualizado |

---

## Tratamento de Sentinela - Comparação

| Score | Onde é Tratado | Valor Sentinela | Resultado |
|-------|----------------|-----------------|-----------|
| Score_01 | **Silver Bureau** | 0 | score_01_adj (já vem ajustado) |
| Score_02 | **Gold ABT v2** | 0 | score_02_adj (ajustado aqui) |

**Por que a diferença?**
- Decisão de design: Score_01 era prioritário, tratado mais cedo
- Score_02 foi adicionado depois, tratamento ficou no Gold
- Ambos têm o mesmo resultado final (sentinela → NULL + flag)

**Consistência futura:**
Para novos scores, recomenda-se tratar sentinela na Silver para manter padrão.

---

## Validações Específicas de v2

A função `validate_abt_v2()` adiciona:

```python
# Gate 6: Score_02 coverage razoável (>90%)
score02_coverage = df.filter(F.col("score_02_adj").isNotNull()).count() / total
assert score02_coverage > 0.90, f"Score_02 coverage muito baixa: {score02_coverage}"

# Gate 7: Flag_score02_missing consistente
inconsistent = df.filter(
    (F.col("score_02_adj").isNotNull()) &
    (F.col("flag_score02_missing") == 1)
).count()
assert inconsistent == 0, "Flag inconsistente com valor"
```

---

## Lições Aprendidas

### 1. Tratamento de Sentinela no Lugar Certo

**Ideal:** Tratar na Silver (padronizado, único lugar)
**Realidade deste projeto:** Score_01 na Silver, Score_02 no Gold
**Aprendizado:** Documentar a inconsistência e manter funcionando

### 2. Análise de Sobreposição

**Novo em v2:** Relatório mostra quantos registros têm ambos os scores
**Valor:** Justifica inclusão incremental na apresentação

### 3. Dois Selects para Clareza

**Escolha:** Separar seleção inicial de reordenação final
**Motivo:** Código mais legível que expressões inline complexas

---

## Exemplo de Saída do Relatório

```
================================================================================
RELATÓRIO FINAL - ABT v2 (Score_01 + Score_02)
================================================================================

>>> [Stats] FLAG_INSTALACAO (decisão observada):
    FLAG=0:    1161410 (30.60%)
    FLAG=1:    2633900 (69.40%)

>>> [Stats] FPD (target, observado SÓ em FLAG_INSTALACAO=1):
    FPD=0:    2074671 (54.66%)
    FPD=1:     559229 (14.73%)

>>> [Features] Completude:
    SCORE_01_ADJ: 98.18%
    SCORE_02_ADJ: 99.95%

>>> [ΔKS] Impacto potencial do Score_02:
    Registros apenas com Score_01: 1,234
    Registros com ambos Scores: 3,725,432
    Registros com Score_02 mas não Score_01: 68,644

================================================================================
✓ ABT v2 PRONTA PARA MODELAGEM
  - Versão: gold_abt_v2
  - Feature blocks: Score_01, Score_02
  - Total registros: 3,795,310
  - Grão: 1:1 NUM_CPF + SAFRA
  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)
  - Status: Incremental (Score_02 adiciona 99.9% de cobertura)
================================================================================
```

---

## Checklist de Revisão

- [x] Herda todas as colunas de v1
- [x] Adiciona score_02_adj com tratamento de sentinela
- [x] Cria flag_score02_missing consistente
- [x] Remove coluna intermediária (score_02_dbl)
- [x] Atualiza gold_feature_blocks para "score_01,score_02"
- [x] Usa validate_abt_v2 (não v1)
- [x] Relatório inclui análise de sobreposição
- [x] Output path correto (abt_v2_delta)

---

## Próximo Passo

A ABT v2 serve como base conceitual para a ABT v3, que adiciona as variáveis Telco:

```
ABT v2 (Score_01 + Score_02) → 02_gold_abt_v3_builder.py → ABT v3 (+ Telco 68 vars)
```

**Nota:** A v3 faz JOIN com Silver Telco, diferente de v1 e v2 que usam apenas Bureau.

Ver [03_ABT_V3_EXPLAINED.md](03_ABT_V3_EXPLAINED.md).
