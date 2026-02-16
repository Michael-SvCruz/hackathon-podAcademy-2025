# Camada Gold - Índice de Documentação Técnica

## Visão Geral

A camada **Gold** é a terceira e última camada na arquitetura Medallion. Sua função é:
- Construir a **ABT (Analytical Base Table)** para modelagem
- Realizar **engenharia de features** (agregações, janelas temporais, métricas derivadas)
- Garantir **anti-leakage** (target e decisão NÃO são features)
- Aplicar **validações de qualidade** (gates obrigatórios)
- Manter **rastreabilidade** incremental (v1 → v2 → ... → v6)

**Princípio fundamental:** Dados prontos para modelagem de Machine Learning, com features derivadas e validações anti-leakage.

---

## Arquitetura da Camada Gold

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CAMADA GOLD                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   GERADORES DE FEATURES           ABT BUILDERS (INCREMENTAIS)               │
│   ━━━━━━━━━━━━━━━━━━━━━           ━━━━━━━━━━━━━━━━━━━━━━━━━━━               │
│                                                                             │
│   gold_recarga_features_v2        00_gold_abt_builder (v1)                  │
│   (Silver Recarga → 60+ features) (Bureau → Score_01)                       │
│           │                              │                                  │
│           │                              ▼                                  │
│           │                       01_gold_abt_v2_builder                    │
│           │                       (v1 + Score_02)                           │
│           │                              │                                  │
│           │                              ▼                                  │
│           │                       02_gold_abt_v3_builder                    │
│           │                       (v2 + Telco 68 vars)                      │
│           │                              │                                  │
│           │                              ▼                                  │
│           │                       03_gold_abt_v4_builder                    │
│           │                       (v3 + Cadastro 33 vars)                   │
│           │                              │                                  │
│           └──────────────────────────────┼──────────────────────────────────┤
│                                          ▼                                  │
│   gold_pagamento_features_v2      04_gold_abt_v5_builder_v2                 │
│   (Silver Pagamento → 50+ feat)   (v4 + Recarga M1/M3/M6)                   │
│           │                              │                                  │
│           │                              ▼                                  │
│   gold_atraso_features_v2         05_gold_abt_v6_builder_v2                 │
│   (Silver Atraso → 60+ features)  (v5 + Pagamento + Atraso M1/M3/M6)        │
│           │                              │                                  │
│           └──────────────────────────────┘                                  │
│                                          │                                  │
│                                          ▼                                  │
│                              ┌───────────────────────┐                      │
│                              │    ABT v6 FINAL       │                      │
│                              │   3.79M registros     │                      │
│                              │    614 colunas        │                      │
│                              │  ~250+ features       │                      │
│                              └───────────────────────┘                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Scripts e Documentação

### Geradores de Features (Silver → Gold Features)

| # | Script | Documentação | Descrição |
|---|--------|--------------|-----------|
| F1 | `gold_recarga_features_v2.py` | [07_RECARGA_FEATURES_EXPLAINED.md](07_RECARGA_FEATURES_EXPLAINED.md) | 95M eventos → 60+ features comportamentais (SOS, temporal) |
| F2 | `gold_pagamento_features_v2.py` | [08_PAGAMENTO_FEATURES_EXPLAINED.md](08_PAGAMENTO_FEATURES_EXPLAINED.md) | 21M eventos → 50+ features de pagamento (juros, descontos) |
| F3 | `gold_atraso_features_v2.py` | [09_ATRASO_FEATURES_EXPLAINED.md](09_ATRASO_FEATURES_EXPLAINED.md) | 31M eventos → 60+ features de atraso (aging, risco) |

### ABT Builders (Incrementais)

| # | Script | Documentação | Descrição |
|---|--------|--------------|-----------|
| 00 | `00_gold_abt_builder.py` | [01_ABT_V1_EXPLAINED.md](01_ABT_V1_EXPLAINED.md) | ABT v1: Bureau (spine) + Score_01 |
| 01 | `01_gold_abt_v2_builder.py` | [02_ABT_V2_EXPLAINED.md](02_ABT_V2_EXPLAINED.md) | ABT v2: v1 + Score_02 |
| 02 | `02_gold_abt_v3_builder.py` | [03_ABT_V3_EXPLAINED.md](03_ABT_V3_EXPLAINED.md) | ABT v3: v2 + Telco (68 variáveis) |
| 03 | `03_gold_abt_v4_builder.py` | [04_ABT_V4_EXPLAINED.md](04_ABT_V4_EXPLAINED.md) | ABT v4: v3 + Cadastro (33 variáveis) |
| 04 | `04_gold_abt_v5_builder_v2.py` | [05_ABT_V5_EXPLAINED.md](05_ABT_V5_EXPLAINED.md) | ABT v5: v4 + Recarga M1/M3/M6 (~120 features) |
| 05 | `05_gold_abt_v6_builder_v2.py` | [06_ABT_V6_EXPLAINED.md](06_ABT_V6_EXPLAINED.md) | ABT v6: v5 + Pagamento + Atraso M1/M3/M6 (FINAL) |

---

## Ordem de Execução do Pipeline

```
FASE 1: GERADORES DE FEATURES (podem rodar em paralelo)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    gold_recarga_features_v2.py     ← Silver Recarga (95M) → Gold Features (32.9M)
    gold_pagamento_features_v2.py   ← Silver Pagamento (21M) → Gold Features (12.6M)
    gold_atraso_features_v2.py      ← Silver Atraso (31M) → Gold Features (15M)


FASE 2: ABT BUILDERS (sequencial - cada versão depende da anterior)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    00_gold_abt_builder.py          ← Silver Bureau → ABT v1 (spine + Score_01)
            │
            ▼
    01_gold_abt_v2_builder.py       ← ABT v1 + Score_02 → ABT v2
            │
            ▼
    02_gold_abt_v3_builder.py       ← ABT v2 + Silver Telco → ABT v3
            │
            ▼
    03_gold_abt_v4_builder.py       ← ABT v3 + Silver Cadastro → ABT v4
            │
            ▼
    04_gold_abt_v5_builder_v2.py    ← ABT v4 + Gold Recarga Features → ABT v5
            │
            ▼
    05_gold_abt_v6_builder_v2.py    ← ABT v5 + Gold Pagamento + Gold Atraso → ABT v6 (FINAL)
```

**Nota:** Os geradores de features (Fase 1) podem rodar em paralelo pois são independentes. Os ABT builders (Fase 2) devem rodar em sequência pois cada versão depende da anterior.

---

## Evolução Incremental das ABTs

| Versão | Feature Blocks Adicionados | Colunas | Incremento KS |
|--------|----------------------------|---------|---------------|
| **v1** | Score_01 (baseline) | ~15 | baseline |
| **v2** | + Score_02 | ~18 | + score bureau |
| **v3** | + Telco (var_26 a var_93) | ~85 | + comportamento telecom |
| **v4** | + Cadastro (33 vars) | ~185 | + dados demográficos |
| **v5** | + Recarga M1/M3/M6 (120+ vars) | ~311 | + stress financeiro |
| **v6** | + Pagamento + Atraso M1/M3/M6 | **614** | + histórico pagamentos |

**Benchmark a bater:** KS = 33.1 (OOT Feb/Mar)

---

## Padrões Comuns (Todos os Scripts)

### Imports Padrão

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_vX  # X = versão
```

### Configuração de Caminhos

```python
# ABT Builders
DEFAULT_PREV_ABT_PATH = "/Volumes/hackathon_2025/default/gold/abt_vX_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_vY_delta/"

# Feature Generators
DEFAULT_SILVER_PATH = "/Volumes/hackathon_2025/default/silver/<fonte>_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/<fonte>_features_v2_delta/"
```

### Estrutura do main()

```python
def main():
    # 1. Parse de argumentos (parse_known_args para compatibilidade Databricks)
    # 2. Leitura da ABT anterior (ou Silver para geradores)
    # 3. Construção da ABT/Features (build_abt_vX ou build_features)
    # 4. Validações (quality gates)
    # 5. Escrita em Delta + Unity Catalog
    # 6. Relatório final (stats, distribuições)
```

---

## Regras Anti-Leakage (CRÍTICAS)

### Colunas que NUNCA são Features

| Coluna | Papel | Regra |
|--------|-------|-------|
| `fpd_int` | **Target** (label de risco) | NUNCA usar como feature. É a variável que o modelo deve predizer |
| `flag_instalacao_int` | **Decisão observada** | NUNCA usar como feature. Incluir apenas para auditoria/análise de swap |

### Verificação de Leakage em Features

Ao adicionar novas features, verificar:
1. A feature é observada ANTES da decisão (safra)? ✓
2. A feature não contém informação do futuro? ✓
3. Para recarga/pagamento/atraso: SAFRA_FONTE < SAFRA_ABT? ✓

---

## Janelas Temporais (M1, M3, M6)

Para Recarga, Pagamento e Atraso, features são calculadas em 3 janelas:

| Janela | Período | Significado |
|--------|---------|-------------|
| **M1** | 1 mês antes | Comportamento recente |
| **M3** | 3 meses antes | Tendência curto prazo |
| **M6** | 6 meses antes | Padrão estabelecido |

**Exemplo:** `freq_sos_m1` = frequência de SOS no último mês antes da safra.

**Anti-leakage temporal:**
```python
# Correto: só eventos ANTES da safra
df_features = df_silver.filter(F.col("safra_recarga") < F.col("safra"))

# Errado: incluiria eventos futuros (LEAKAGE!)
df_features = df_silver  # sem filtro temporal
```

---

## Quality Gates (Validações Obrigatórias)

Todos os scripts Gold implementam gates:

### Gates Universais (todas as versões)

```python
# Gate 1: Unicidade (1:1 por NUM_CPF + SAFRA)
distinct_key = df.select("num_cpf", "safra").distinct().count()
assert distinct_key == df.count(), "Duplicatas detectadas!"

# Gate 2: Chaves não nulas
assert df.filter(F.col("num_cpf").isNull()).count() == 0
assert df.filter(F.col("safra").isNull()).count() == 0

# Gate 3: Labels em domínio válido
assert df.filter(~F.col("flag_instalacao_int").isin(0, 1)).count() == 0

# Gate 4: FPD observado apenas em aprovados
fpd_em_reprovados = df.filter(
    (F.col("flag_instalacao_int") == 0) &
    (F.col("fpd_int").isNotNull())
).count()
assert fpd_em_reprovados == 0, "FPD observado em reprovados (leakage!)"
```

### Gates Específicos por Versão

| Versão | Gate Adicional | Validação |
|--------|----------------|-----------|
| v1-v2 | Score coverage | score_01_adj > 90% não-nulo |
| v3 | Telco coverage | var_26_adj > 30% não-nulo |
| v4 | Cadastro coverage | idade_anos > 30% não-nulo |
| v5 | Recarga coverage | freq_recarga_m1 > 5% não-nulo |
| v6 | Pagamento coverage | qtd_pagamentos_m1 > 5% não-nulo |

---

## Metadados Adicionados

Cada ABT adiciona metadados de rastreabilidade:

```python
df = df.withColumn("abt_version", F.lit("v6"))
df = df.withColumn("build_date", F.current_timestamp())
df = df.withColumn("spine_version", F.lit("bureau_full_silver"))
df = df.withColumn("gold_version", F.lit("gold_abt_v6_v2"))
```

---

## Diferenças Entre Scripts

### Feature Generators vs ABT Builders

| Aspecto | Feature Generators | ABT Builders |
|---------|-------------------|--------------|
| **Input** | Silver (eventos) | ABT anterior + Silver/Gold |
| **Output** | Gold Features (agregado) | ABT versão N |
| **Grão entrada** | Evento-level | Cliente-mês |
| **Grão saída** | Cliente-mês | Cliente-mês |
| **Operação** | Agregação/Window | LEFT JOIN |
| **Colunas** | 50-60 features | Incremental (+100-300) |

### Tipos de JOIN

| Script | Tipo JOIN | Motivo |
|--------|-----------|--------|
| ABT v1-v4 | SELECT direto ou LEFT JOIN | Bureau é o spine |
| ABT v5 | LEFT JOIN | Nem todos têm recarga |
| ABT v6 | LEFT JOIN | Nem todos têm pagamento/atraso |

---

## Checklist de Revisão (Gold)

Ao revisar ou criar um script Gold, verifique:

- [ ] `parse_known_args()` (não `parse_args()`)
- [ ] Anti-leakage: FPD e FLAG_INSTALACAO NÃO usados como features
- [ ] Janela temporal correta (SAFRA_FONTE < SAFRA_ABT)
- [ ] LEFT JOIN preserva o spine (todos os registros da ABT anterior)
- [ ] FLAGS de missing criados para colunas nullable
- [ ] Quality gates implementados e passando
- [ ] Metadados de versão adicionados
- [ ] Escrita em Delta + Unity Catalog
- [ ] Relatório final com distribuições
- [ ] Logs com print (>>> prefixo)

---

## Resultados da Execução

### Feature Generators

| Gerador | Input (Eventos) | Output (Cliente-Mês) | Features |
|---------|-----------------|---------------------|----------|
| Recarga v2 | 95,210,519 | 32,882,218 | 51 |
| Pagamento v2 | 21,821,465 | 12,634,799 | 49 |
| Atraso v2 | 31,611,316 | 15,023,012 | 58 |

### ABT Builders

| ABT | Registros | Colunas | Validação |
|-----|-----------|---------|-----------|
| v1 | 3,795,310 | ~15 | PASSED |
| v2 | 3,795,310 | ~18 | PASSED |
| v3 | 3,795,310 | ~85 | PASSED |
| v4 | 3,795,310 | ~185 | PASSED |
| v5 | 3,795,310 | 311 | 11/11 gates |
| **v6** | **3,795,310** | **614** | **PASSED** |

---

## Próximo Passo

Após construir a ABT v6, o pipeline de modelagem pode iniciar:

```
ABT v6 (614 colunas)
        │
        ├── Feature Selection (reduzir para top features)
        │
        ├── Train/Test/OOT Split (por SAFRA)
        │
        ├── Baseline Model (Logistic Regression)
        │
        ├── XGBoost/LightGBM
        │
        ├── Avaliação (KS, Gini, ROC)
        │
        └── Interpretação (SHAP)
```

Ver documentação de modelagem em `docs/06_modeling/` (quando disponível).
