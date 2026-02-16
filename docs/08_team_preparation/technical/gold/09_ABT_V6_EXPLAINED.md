# Gold ABT v6 Builder v2 - Documentacao Tecnica Detalhada

## Informacoes do Arquivo

| Item | Valor |
|------|-------|
| **Script** | `src/jobs/02_gold/05_gold_abt_v6_builder_v2.py` |
| **Tipo** | ABT Builder (Gold + Gold → Gold) |
| **Inputs** | ABT v5 v2 + Pagamento Features v2 + Atraso Features v2 |
| **Output** | `gold/abt_v6_v2_delta/` (3.79M registros, 614 colunas) |
| **Grao** | 1 linha por NUM_CPF + SAFRA |
| **Janelas Temporais** | M1 (1 mes), M3 (3 meses), M6 (6 meses) |

---

## Posicao no Roadmap Incremental

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ROADMAP DE EVOLUCAO DA ABT                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ABT v1 ──► ABT v2 ──► ABT v3 ──► ABT v4 ──► ABT v5 v2 ──► ABT v6 v2      │
│    │          │          │          │           │             │             │
│    │          │          │          │           │             │             │
│  Score_01   +Score_02  +Telco    +Cadastro   +Recarga      +Pagamento      │
│  (baseline)            (68 vars)  (33 vars)   (60+ feat)   +Atraso         │
│                                               M1/M3/M6     M1/M3/M6        │
│                                                             │             │
│                                                             ▼             │
│                                                     ┌─────────────────┐   │
│                                                     │   ABT FINAL     │   │
│                                                     │   614 colunas   │   │
│                                                     │   3.79M regs    │   │
│                                                     │   ~250 features │   │
│                                                     └─────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Este e o script FINAL da pipeline de Data Engineering.**

---

## Arquitetura do Join

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ABT v5 v2                                       │
│                                                                             │
│  Grao: NUM_CPF + SAFRA (1:1)                                               │
│  Registros: 3,795,310                                                       │
│  Colunas: 311                                                               │
│  Features: Score_01/02 + Telco + Cadastro + Recarga (M1/M3/M6)             │
│                                                                             │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         │                           │                           │
         ▼                           │                           ▼
┌─────────────────────┐              │              ┌─────────────────────┐
│  Pagamento Feat v2  │              │              │   Atraso Feat v2    │
│                     │              │              │                     │
│  Grao: CPF+SAFRA_P  │              │              │  Grao: CPF+SAFRA_A  │
│  Registros: 12.6M   │              │              │  Registros: 15M     │
│  Features: 49       │              │              │  Features: 58       │
└──────────┬──────────┘              │              └──────────┬──────────┘
           │                         │                         │
           │    ┌────────────────────┴────────────────────┐    │
           │    │                                         │    │
           │    │         AGREGACAO POR JANELA            │    │
           │    │         (M1 / M3 / M6)                  │    │
           │    │                                         │    │
           │    │   JOIN por NUM_CPF                      │    │
           │    │   + Filtro: SAFRA_* < SAFRA             │    │
           │    │     (anti-leakage)                      │    │
           │    │                                         │    │
           └────┴─────────────────────────────────────────┴────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ABT v6 v2                                       │
│                                                                             │
│  Grao: NUM_CPF + SAFRA (1:1)                                               │
│  Registros: 3,795,310 (mesmo que v5)                                        │
│  Colunas: 614                                                               │
│                                                                             │
│  Feature Blocks:                                                            │
│  ├── Score_01, Score_02                (~10 colunas)                       │
│  ├── Telco (var_26 - var_93)           (~68 colunas)                       │
│  ├── Cadastro (var_02 - var_25)        (~33 colunas)                       │
│  ├── Recarga v2 (M1/M3/M6)             (~180 colunas)                      │
│  ├── Pagamento v2 (M1/M3/M6)           (~150 colunas)                      │
│  └── Atraso v2 (M1/M3/M6)              (~174 colunas)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Conceito Chave: Janelas Temporais (Lookback)

### O que sao M1, M3, M6?

```
Para um cliente com SAFRA = 202403 (Marco 2024):

                    M6                M3           M1
              ◄─────────────────────────────────────►
              │                      │              │
    Set/23    Out/23   Nov/23   Dez/23   Jan/24   Fev/24   Mar/24
      │         │        │        │        │        │        │
      └─────────┴────────┴────────┴────────┴────────┴────────┼──── SAFRA
                                                             │
                                                     (Data do evento
                                                      no spine)

M1 = 1 mes anterior (Fev/24)
M3 = 3 meses anteriores (Dez/23, Jan/24, Fev/24)
M6 = 6 meses anteriores (Set/23 a Fev/24)

IMPORTANTE: Nunca inclui o proprio mes da SAFRA (anti-leakage)
```

### Por que 3 janelas?

| Janela | Periodo | Captura |
|--------|---------|---------|
| **M1** | 1 mes | Comportamento **recente** (mais preditivo, mais volatil) |
| **M3** | 3 meses | Comportamento **medio prazo** (balanceado) |
| **M6** | 6 meses | Comportamento **estavel** (menos volatil, menos atual) |

**Trade-off:**
- M1: Mais sensivel a mudancas recentes, pode ser ruido
- M6: Mais estavel, pode perder mudancas recentes

**Modelo pode escolher** qual janela e mais preditiva por feature.

### Anti-Leakage: Por que SAFRA_* < SAFRA?

```python
# ERRADO: Incluiria dados do proprio mes ou futuro
df_filtered = df_joined.filter(
    F.col(dt_safra_col) <= F.col("dt_safra")  # VAZAMENTO!
)

# CORRETO: Apenas dados anteriores
df_filtered = df_joined.filter(
    (F.col(dt_safra_col) >= F.add_months(F.col("dt_safra"), -num_meses)) &
    (F.col(dt_safra_col) < F.col("dt_safra"))  # ESTRITAMENTE MENOR
)
```

```
Cenario de Vazamento (ERRADO):
  Spine SAFRA = Marco/2024
  Pagamento SAFRA_PAGAMENTO = Marco/2024  ← Inclui dados do proprio mes!

  O modelo saberia que o cliente pagou em Marco antes de decidir
  se aprova em Marco. Na producao, essa info nao existe ainda.

Correto:
  Apenas SAFRA_PAGAMENTO = Fev/2024 ou anterior
```

---

## Codigo Explicado Linha por Linha

### 1. Configuracao e Constantes

```python
DEFAULT_GOLD_ABT_V5_PATH = "/Volumes/hackathon_2025/default/gold/abt_v5_v2_delta/"
DEFAULT_GOLD_PAGAMENTO_PATH = "/Volumes/hackathon_2025/default/gold/pagamento_features_v2_delta/"
DEFAULT_GOLD_ATRASO_PATH = "/Volumes/hackathon_2025/default/gold/atraso_features_v2_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v6_v2_delta/"
```

**3 inputs Gold + 1 output Gold:**
- ABT v5 v2: Spine com features anteriores
- Pagamento Features v2: Features de pagamento (50+)
- Atraso Features v2: Features de atraso (60+)
- Output: ABT final para modelagem

```python
TEMPORAL_WINDOWS = {"m1": 1, "m3": 3, "m6": 6}
```

**Dicionario de janelas temporais:**
- Chave: Sufixo para nome da coluna (`_m1`, `_m3`, `_m6`)
- Valor: Numero de meses de lookback

---

### 2. Funcao Principal: agregar_features_por_janela()

Esta e a funcao mais importante do script - implementa a logica de janelas temporais.

#### 2.1 Assinatura e Parametros

```python
def agregar_features_por_janela(
    df_spine: DataFrame,
    df_features: DataFrame,
    safra_col: str,
    prefix: str
) -> dict:
```

| Parametro | Tipo | Exemplo | Descricao |
|-----------|------|---------|-----------|
| `df_spine` | DataFrame | ABT v5 | Spine com NUM_CPF, SAFRA, DT_SAFRA |
| `df_features` | DataFrame | Pagamento Features | Features com NUM_CPF, SAFRA_* |
| `safra_col` | str | "safra_pagamento" | Nome da coluna SAFRA na fonte |
| `prefix` | str | "pag" | Prefixo para sufixar colunas |

**Retorno:** `dict` com DataFrames agregados `{m1: df, m3: df, m6: df}`

#### 2.2 Preparar Chaves do Spine

```python
    df_keys = df_spine.select("num_cpf", "safra", "dt_safra").distinct()
```

**Por que select + distinct?**
- **select**: Apenas colunas necessarias para o join (menos memoria)
- **distinct**: Garante unicidade (spine ja e 1:1, mas e uma garantia extra)

**Por que nao usar df_spine diretamente?**
```python
# ERRADO: Arrastaria todas as 311 colunas do ABT v5 no join
df_joined = df_spine.join(df_features, ...)

# CORRETO: Apenas chaves necessarias
df_keys = df_spine.select("num_cpf", "safra", "dt_safra").distinct()
df_joined = df_keys.join(df_features, ...)
```
- Economia de memoria e shuffle
- Join mais eficiente

#### 2.3 Preparar Coluna de Data

```python
    dt_safra_col = f"dt_{safra_col}"  # Ex: "dt_safra_pagamento"
    if dt_safra_col not in df_features.columns:
        df_features = df_features.withColumn(
            dt_safra_col,
            F.to_date(F.concat(F.col(safra_col), F.lit("01")), "yyyyMMdd")
        )
```

**Por que verificar `if not in columns`?**
- Feature generators ja criam `dt_safra_*`
- Verificacao evita recriar (idempotencia)
- Se nao existe, cria a partir de `safra_*` string

**Conversao:**
```
safra_pagamento = "202401" (string)
         │
         ▼
concat("202401", "01") = "20240101"
         │
         ▼
to_date("20240101", "yyyyMMdd") = 2024-01-01 (date)
```

#### 2.4 Identificar Colunas de Features

```python
    meta_cols = ["num_cpf", safra_col, dt_safra_col, "gold_version", "gold_build_date"]
    feature_cols = [c for c in df_features.columns if c not in meta_cols]
```

**Por que excluir meta_cols?**
- `num_cpf`, `safra_*`: Chaves de join, nao features
- `gold_version`, `gold_build_date`: Metadados, nao features

**List comprehension:**
```python
# Equivalente expandido:
feature_cols = []
for c in df_features.columns:
    if c not in meta_cols:
        feature_cols.append(c)
```

#### 2.5 Loop sobre Janelas Temporais

```python
    for janela, num_meses in TEMPORAL_WINDOWS.items():
        print(f"    → Janela {janela.upper()} ({num_meses} mês(es))...")

        sfx = f"_{prefix}_{janela}"  # Ex: "_pag_m1"
```

**Sufixo dinamico:**
- `prefix` = "pag" ou "atr"
- `janela` = "m1", "m3", "m6"
- Resultado: `_pag_m1`, `_pag_m3`, `_atr_m6`, etc.

#### 2.6 JOIN por NUM_CPF (Sem SAFRA!)

```python
        df_joined = df_keys.join(df_features, on="num_cpf", how="left")
```

**POR QUE JOIN APENAS POR NUM_CPF?**

Esta e uma decisao arquitetural importante:

```
Cenario: Cliente 123 com SAFRA=202403 no spine

Se JOIN por (NUM_CPF, SAFRA):
  ├── Pagamento 123, 202403  ← Match direto
  └── Pagamento 123, 202402  ← NAO match (SAFRA diferente)

  Resultado: So pegaria dados do proprio mes (vazamento!)

Se JOIN por (NUM_CPF) + Filtro temporal:
  ├── Pagamento 123, 202403  ← Filtrado (>= dt_safra)
  ├── Pagamento 123, 202402  ← INCLUSO (< dt_safra)
  ├── Pagamento 123, 202401  ← INCLUSO (< dt_safra)
  └── ...

  Resultado: Pega todos os meses anteriores para agregar
```

**A janela temporal e aplicada no FILTRO, nao no JOIN.**

#### 2.7 Filtro de Janela Temporal (Anti-Leakage)

```python
        df_filtered = df_joined.filter(
            (F.col(dt_safra_col) >= F.add_months(F.col("dt_safra"), -num_meses)) &
            (F.col(dt_safra_col) < F.col("dt_safra"))
        )
```

**Decompondo:**

```python
# Limite inferior: N meses antes da SAFRA
F.add_months(F.col("dt_safra"), -num_meses)
# Para M3: dt_safra - 3 meses

# Limite superior: ESTRITAMENTE antes da SAFRA
F.col(dt_safra_col) < F.col("dt_safra")
# Nunca inclui o proprio mes
```

**Exemplo concreto (M3, SAFRA=202403):**
```
dt_safra = 2024-03-01

Limite inferior: 2024-03-01 - 3 meses = 2023-12-01
Limite superior: < 2024-03-01

Meses inclusos: Dez/23, Jan/24, Fev/24 (3 meses)
Mes EXCLUIDO: Mar/24 (seria vazamento)
```

#### 2.8 Agregacoes Dinamicas por Tipo de Coluna

```python
        agg_exprs = [F.count("*").alias(f"qtd_meses_dados{sfx}")]
```

**Primeira agregacao:** Quantos meses de dados existem na janela.

```python
        for col in feature_cols:
            # Colunas de contagem/soma: SUM
            if col.startswith(("qtd_", "sum_")):
                agg_exprs.append(F.sum(F.coalesce(F.col(col), F.lit(0))).alias(f"{col}{sfx}"))
```

**Logica por prefixo:**

| Prefixo | Agregacao | Justificativa |
|---------|-----------|---------------|
| `qtd_*`, `sum_*` | `SUM` | Contagens/valores acumulam |
| `flag_*` | `MAX` | Se teve em algum mes = 1 |
| `pct_*`, `ratio_*`, `avg_*` | `AVG` | Medias/percentuais fazem media |
| `max_*` | `MAX` | Maximo dos maximos |
| `min_*` | `MIN` | Minimo dos minimos |
| outros | `AVG` | Default seguro |

**Por que F.coalesce(col, 0) em SUM?**
```python
F.sum(F.coalesce(F.col(col), F.lit(0)))

# Sem coalesce:
# NULL + 100 + NULL = NULL (Spark SUM ignora NULLs, mas...)

# Com coalesce:
# 0 + 100 + 0 = 100 (resultado esperado)
```

**Flags com MAX:**
```python
            elif col.startswith("flag_"):
                agg_exprs.append(F.max(F.coalesce(F.col(col), F.lit(0))).alias(f"{col}{sfx}"))
```

```
flag_teve_wo em 3 meses: [0, 0, 1]

F.max([0, 0, 1]) = 1  → "Teve WO em algum mes da janela"
F.sum([0, 0, 1]) = 1  → Funcionaria, mas semanticamente MAX e mais claro
```

#### 2.9 GroupBy e Agregacao

```python
        df_agg = df_filtered.groupBy("num_cpf", "safra").agg(*agg_exprs)
```

**Por que groupBy("num_cpf", "safra")?**
- `num_cpf`: Identificador do cliente
- `safra`: Mes de referencia do spine (para reconectar depois)

**Asterisco em `*agg_exprs`:**
```python
# agg_exprs e uma lista: [expr1, expr2, expr3, ...]
# *agg_exprs "desempacota" a lista

# Equivalente:
df_filtered.groupBy(...).agg(expr1, expr2, expr3, ...)
```

#### 2.10 Retorno do Dicionario

```python
        agg_por_janela[janela] = df_agg

    return agg_por_janela
```

**Retorno:**
```python
{
    "m1": DataFrame com features agregadas M1,
    "m3": DataFrame com features agregadas M3,
    "m6": DataFrame com features agregadas M6
}
```

---

### 3. Funcao build_abt_v6()

#### 3.1 Agregar Pagamento por Janela

```python
    pag_por_janela = agregar_features_por_janela(
        df_abt_v5, df_pagamento_features,
        safra_col="safra_pagamento", prefix="pag"
    )
```

**Resultado:** Dicionario com 3 DataFrames (M1, M3, M6) de features de Pagamento.

#### 3.2 Combinar Janelas de Pagamento

```python
    df_pag_all = pag_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_pag_all = df_pag_all.join(pag_por_janela[janela], on=["num_cpf", "safra"], how="outer")
```

**Por que OUTER JOIN?**

```
Cenario:
  Cliente 123 tem dados em M1 e M3, mas NAO em M6
  Cliente 456 tem dados em M6 apenas

OUTER JOIN:
  123 | feat_pag_m1 | feat_pag_m3 | NULL (m6)
  456 | NULL (m1)   | NULL (m3)   | feat_pag_m6

LEFT JOIN perderia cliente 456!
```

**Evolucao do DataFrame:**
```
Iteracao 1: df_pag_all = M1
Iteracao 2: df_pag_all = M1 + M3 (outer)
Iteracao 3: df_pag_all = M1 + M3 + M6 (outer)
```

#### 3.3 Repetir para Atraso

```python
    atr_por_janela = agregar_features_por_janela(
        df_abt_v5, df_atraso_features,
        safra_col="safra_atraso", prefix="atr"
    )

    df_atr_all = atr_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_atr_all = df_atr_all.join(atr_por_janela[janela], on=["num_cpf", "safra"], how="outer")
```

**Mesma logica**, mas para features de Atraso.

#### 3.4 JOIN Final com ABT v5

```python
    df_abt_v6 = df_abt_v5.join(df_pag_all, on=["num_cpf", "safra"], how="left")
    df_abt_v6 = df_abt_v6.join(df_atr_all, on=["num_cpf", "safra"], how="left")
```

**Por que LEFT JOIN aqui?**
- ABT v5 e o **spine** (todos os clientes devem estar no resultado)
- LEFT preserva todos os registros do ABT v5
- Se cliente nao tem Pagamento/Atraso, features ficam NULL

```
ABT v5:      3,795,310 registros  (todos preservados)
+ Pagamento: Enriquece com features (onde existir)
+ Atraso:    Enriquece com features (onde existir)
= ABT v6:    3,795,310 registros  (mesmo count do spine)
```

#### 3.5 Preenchimento de NULLs

```python
    for col in df_abt_v6.columns:
        if any(x in col for x in ["_pag_", "_atr_"]):
            if col.startswith(("qtd_", "sum_", "flag_")):
                df_abt_v6 = df_abt_v6.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
            elif col.startswith(("pct_", "ratio_")):
                df_abt_v6 = df_abt_v6.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
```

**Por que preencher NULLs?**

| Tipo de Coluna | NULL Significa | Valor Default | Justificativa |
|----------------|----------------|---------------|---------------|
| `qtd_*` | Sem eventos | 0 | Zero contagem |
| `sum_*` | Sem valores | 0 | Zero soma |
| `flag_*` | Nao teve | 0 | Flag desligado |
| `pct_*` | Nao calculavel | 0.0 | Zero percentual |
| `ratio_*` | Nao calculavel | 0.0 | Zero ratio |
| `avg_*`, outros | Nao calculavel | **Manter NULL** | Media de nada e NULL, nao 0 |

**Por que NAO preencher avg_* com zero?**
```
avg_val_pago = NULL  → Cliente sem pagamentos, media indefinida
avg_val_pago = 0     → Cliente pagou media de R$0? ERRADO!
```

#### 3.6 Flags de Cobertura

```python
    for janela in TEMPORAL_WINDOWS.keys():
        df_abt_v6 = df_abt_v6.withColumn(
            f"flag_sem_pagamento_{janela}",
            F.when(
                F.col(f"qtd_meses_dados_pag_{janela}").isNull() |
                (F.col(f"qtd_meses_dados_pag_{janela}") == 0),
                1
            ).otherwise(0)
        )
```

**Para que servem esses flags?**
- Indicam **ausencia de dados** na janela
- Modelo pode tratar diferentemente:
  - `flag_sem_pagamento_m1 = 1`: Pode ser cliente novo ou sem historico
  - `flag_sem_pagamento_m1 = 0`: Tem dados de pagamento

**Por que criar flag explicito em vez de usar NULL?**
- Alguns modelos (XGBoost) tratam NULL como valor especial
- Flag explicito da controle ao modelador
- Facilita feature engineering adicional

---

### 4. Funcao validate_abt_v6()

#### Gate 1: Unicidade

```python
    count_distinct = df.select("num_cpf", "safra").distinct().count()
    if count_distinct != count_atual:
        errors.append(f"Gate 1 FALHOU: duplicatas encontradas")
```

**Garante grao 1:1** (NUM_CPF + SAFRA).

#### Gate 2: Anti-Leakage FPD

```python
    fpd_em_flag_0 = df.filter(
        (F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())
    ).count()
```

**FPD so pode existir onde FLAG_INSTALACAO = 1:**
- `FLAG = 1`: Aprovado e contratou → FPD observavel
- `FLAG = 0`: Reprovado ou nao contratou → FPD impossivel de observar

#### Gate 3: Cobertura Score_01

```python
    score_cov = df.filter(F.col("score_01_adj").isNotNull()).count() * 100 / count_atual
    if score_cov < 90:
        errors.append(f"Gate 3 FALHOU: Score_01 {score_cov:.1f}% < 90%")
```

**Score_01 e o baseline** - deve ter alta cobertura.

#### Gates 4-5: Cobertura Informativa

```python
    if pag_col in df.columns:
        pag_cov = df.filter(F.col(pag_col) > 0).count() * 100 / count_atual
        print(f"    ✓ INFO: Pagamento M1 {pag_cov:.1f}%")
```

**Nao falha, apenas informa** - cobertura de Pagamento/Atraso pode ser < 100%.

---

### 5. Funcao main() e Escrita

```python
    df_abt_v6.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
```

**Opcoes de escrita:**
- `mergeSchema`: Permite adicionar colunas novas
- `overwriteSchema`: Permite alterar tipos de colunas existentes
- **Sem partitionBy**: ABT final nao precisa particao (sera lida inteira para treino)

**Por que nao particionar ABT final?**
- Particao e util para queries filtradas
- Modelo le ABT inteira para treino
- Evita overhead de muitos arquivos pequenos

---

## Fluxo Completo de Dados

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          PIPELINE COMPLETO                                 │
│                                                                           │
│  SILVER                                                                   │
│  ├── bureau_silver (spine)                                               │
│  ├── telco_silver (68 vars)                                              │
│  ├── cadastro_silver (33 vars)                                           │
│  ├── recarga_silver (95M eventos)                                        │
│  ├── pagamento_silver (22M eventos)                                      │
│  └── atraso_silver (32M snapshots)                                       │
│                                                                           │
│       │            │            │                                         │
│       ▼            ▼            ▼                                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                                   │
│  │ gold_   │  │ gold_   │  │ gold_   │                                   │
│  │ recarga │  │ pagam.  │  │ atraso  │                                   │
│  │ _v2.py  │  │ _v2.py  │  │ _v2.py  │                                   │
│  └────┬────┘  └────┬────┘  └────┬────┘                                   │
│       │            │            │                                         │
│       ▼            │            │                                         │
│  ┌─────────────────────────────────────────┐                             │
│  │              ABT v1 → v4                 │                             │
│  │  (Score_01 + Score_02 + Telco + Cad)   │                             │
│  └───────────────────┬─────────────────────┘                             │
│                      │                                                    │
│                      ▼                                                    │
│  ┌─────────────────────────────────────────┐                             │
│  │           ABT v5 v2 Builder              │                             │
│  │     (v4 + Recarga M1/M3/M6)             │                             │
│  └───────────────────┬─────────────────────┘                             │
│                      │                                                    │
│                      ▼                                                    │
│  ┌─────────────────────────────────────────┐                             │
│  │           ABT v6 v2 Builder              │  ← ESTE SCRIPT             │
│  │  (v5 + Pagamento + Atraso M1/M3/M6)     │                             │
│  └───────────────────┬─────────────────────┘                             │
│                      │                                                    │
│                      ▼                                                    │
│  ┌─────────────────────────────────────────┐                             │
│  │             ABT v6 v2 FINAL              │                             │
│  │                                          │                             │
│  │   3,795,310 registros                   │                             │
│  │   614 colunas                           │                             │
│  │   ~250+ features para modelagem         │                             │
│  │                                          │                             │
│  │   Pronto para:                          │                             │
│  │   - Feature selection                   │                             │
│  │   - Train/Test/OOT split                │                             │
│  │   - Modelagem (Logistic, XGBoost, etc)  │                             │
│  └─────────────────────────────────────────┘                             │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Resultados da Execucao

### Volumetria

| Metrica | Valor |
|---------|-------|
| ABT v5 (input) | 3,795,310 |
| ABT v6 (output) | 3,795,310 |
| Colunas totais | 614 |

### Cobertura por Fonte (M1)

| Feature Block | Cobertura |
|---------------|-----------|
| Score_01 | 98.18% |
| Score_02 | 99.95% |
| Telco | 35.46% |
| Cadastro | 35-40% |
| Recarga M1 | 56.12% |
| Pagamento M1 | 16.13% |
| Atraso M1 | 21.79% |

### Distribuicao de Labels

| Label | Count | % |
|-------|-------|---|
| FLAG=1 (Aprovado) | 2,633,900 | 69.40% |
| FLAG=0 (Reprovado) | 1,161,410 | 30.60% |
| FPD=1 (em FLAG=1) | 559,229 | 21.23% |

---

## Decisoes de Design Importantes

### 1. Por que Nao Fazer Tudo em Um Script?

```
Opcao A (Monolitico):
  Um script que le todas as Silvers e gera ABT final

Opcao B (Modular - ESCOLHIDO):
  Feature generators + ABT builders separados
```

**Vantagens da abordagem modular:**
- **Reprocessamento parcial**: Se Recarga mudar, so roda recarga + ABT v5+
- **Debug mais facil**: Isola problemas em etapas especificas
- **Paralelismo**: Feature generators podem rodar em paralelo
- **Versionamento**: Cada feature generator tem sua versao

### 2. Por que Janelas Fixas (M1/M3/M6)?

```
Opcao A: Janelas fixas (M1, M3, M6)
Opcao B: Janelas dinamicas (parametrizaveis)
Opcao C: Features cumulativas (tudo ate SAFRA)
```

**Escolhemos janelas fixas porque:**
- **Simplicidade**: Modelo recebe conjunto previsivel de features
- **Interpretabilidade**: "Comportamento no ultimo mes" e claro
- **Comparabilidade**: Todos os clientes avaliados com mesmas janelas
- **Feature selection**: Modelo escolhe qual janela e mais preditiva

### 3. Por que Agregacao Dinamica por Prefixo?

```python
if col.startswith(("qtd_", "sum_")):
    SUM
elif col.startswith("flag_"):
    MAX
elif col.startswith(("pct_", "ratio_")):
    AVG
```

**Vantagens:**
- **Automatico**: Nao precisa listar 60+ colunas manualmente
- **Consistente**: Todas as `qtd_*` tratadas igual
- **Extensivel**: Adicionar nova feature segue a convencao

**Risco:**
- Coluna com prefixo errado seria agregada incorretamente
- Mitigado por: convencao de nomenclatura nos feature generators

---

## Erros Comuns e Solucoes

### 1. Duplicatas apos JOIN

**Sintoma:**
```
ABT v5: 3.79M → ABT v6: 7.58M (dobrou!)
```

**Causa:** JOIN criou produto cartesiano por chave ambigua.

**Solucao:** Verificar unicidade das chaves antes do JOIN.
```python
df_keys = df_spine.select("num_cpf", "safra").distinct()
assert df_keys.count() == df_spine.count()
```

### 2. Vazamento Temporal

**Sintoma:** Modelo com KS muito alto em treino, cai drasticamente em OOT.

**Causa:** Filtro temporal com `<=` em vez de `<`.

**Solucao:** Sempre usar `<` (estritamente menor):
```python
F.col(dt_safra_col) < F.col("dt_safra")  # CORRETO
```

### 3. Todos os Features NULL

**Sintoma:** Apos JOIN, todas as features de Pagamento/Atraso sao NULL.

**Causa:** Coluna `dt_safra_*` nao existe ou formato incompativel.

**Solucao:** Verificar se feature generator criou a coluna corretamente.

---

## Checklist de Revisao

- [ ] Entendi por que JOIN e por NUM_CPF (nao por SAFRA)
- [ ] Sei a diferenca entre M1, M3, M6 e quando usar cada uma
- [ ] Compreendo por que o filtro usa `<` (estritamente menor)
- [ ] Entendi a logica de agregacao por prefixo (qtd_→SUM, flag_→MAX, pct_→AVG)
- [ ] Sei por que ABT v6 tem mesmo count que ABT v5 (LEFT JOIN)
- [ ] Compreendo a diferenca entre OUTER JOIN (janelas) e LEFT JOIN (ABT final)
- [ ] Entendi por que NAO preencher avg_* com zero
- [ ] Sei interpretar os flags de cobertura (flag_sem_pagamento_m1)

---

## Proximos Passos (Modelagem)

Apos ABT v6 v2 pronta:

1. **Feature Selection**
   - Remover colunas com >95% NULL
   - Remover features colineares (correlacao > 0.95)
   - Importancia por Random Forest / XGBoost

2. **Split Temporal**
   ```python
   df_train = df.filter(F.col("safra") < "202402")
   df_test = df.filter(F.col("safra") == "202402")
   df_oot = df.filter(F.col("safra") == "202403")
   ```

3. **Modelagem**
   - Baseline: Logistic Regression
   - Champion: XGBoost / LightGBM
   - Avaliar: KS incremental por feature block

4. **Apresentacao**
   - Mostrar KS incremental: Score_01 → +Score_02 → +Telco → +Cadastro → +Recarga → +Pagamento/Atraso
   - Confusion matrix com swap-in/swap-out
   - Top features por SHAP

---

## Referencias

- **Documentacao ABT v5:** `05_ABT_V5_EXPLAINED.md`
- **Pagamento Features:** `08_PAGAMENTO_FEATURES_EXPLAINED.md`
- **Atraso Features:** `09_ATRASO_FEATURES_EXPLAINED.md`
- **Variable Book:** `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md`
