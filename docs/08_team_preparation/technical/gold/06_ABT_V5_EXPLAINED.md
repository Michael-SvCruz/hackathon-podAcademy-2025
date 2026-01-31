# 05 - ABT v5 Builder v2 Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/04_gold_abt_v5_builder_v2.py` |
| **Função** | Construir ABT v5 - adiciona Recarga com janelas M1/M3/M6 |
| **Input** | Gold ABT v4 (spine) + Gold Recarga Features v2 |
| **Output** | Gold ABT v5 v2 |
| **Registros** | 3,795,310 (1:1 com spine) |
| **Colunas** | ~311 |
| **Feature Blocks** | Score_01 + Score_02 + Telco + Cadastro + Recarga |

---

## Contexto de Negócio

A ABT v5 é o **primeiro script com janelas temporais**:

1. **Consome Gold Recarga Features:** Lê features pré-calculadas (não Silver)
2. **Aplica janelas M1/M3/M6:** Agrega features por período de lookback
3. **Garante anti-leakage:** SAFRA_RECARGA sempre < SAFRA (olha para o passado)
4. **Triplica features de recarga:** Cada feature existe em 3 versões temporais

### Arquitetura do JOIN

```
ABT v4 (gold/abt_v4_delta)
├── Grão: NUM_CPF + SAFRA (cliente-mês do spine)
├── ~3.8M registros
└── Features: Score_01/02 + Telco (68) + Cadastro (33)
                │
                │ LEFT JOIN com filtro temporal
                │ Condição: SAFRA_RECARGA < SAFRA (anti-leakage)
                │
Gold Recarga Features v2 (gold/recarga_features_v2_delta)
├── Grão: NUM_CPF + SAFRA_RECARGA (cliente-mês da recarga)
├── Features: 51 features comportamentais
└── Múltiplos registros por cliente (um por mês com atividade)
                │
                ▼
ABT v5 (gold/abt_v5_v2_delta)
├── Grão: NUM_CPF + SAFRA (mantido do spine)
├── ~3.8M registros (mesmo count do v4)
└── Features: v4 + Recarga (M1/M3/M6 agregações) = ~311 colunas
```

---

## Janelas Temporais (Lookback)

O conceito de **janela temporal** é central neste script:

| Janela | Lookback | Significado | Exemplo (SAFRA=202502) |
|--------|----------|-------------|------------------------|
| **M1** | 1 mês | Comportamento recente | Janeiro 2025 (202501) |
| **M3** | 3 meses | Tendência curto prazo | Nov-Jan 2024/25 (202411-202501) |
| **M6** | 6 meses | Padrão estabelecido | Ago-Jan 2024/25 (202408-202501) |

### Regra Anti-Leakage Temporal

```
SAFRA_RECARGA < SAFRA (OBRIGATÓRIO)

Cliente com SAFRA = 202502:
  ✓ Pode usar recargas de 202501 (janeiro)     → passado
  ✓ Pode usar recargas de 202412 (dezembro)   → passado
  ✗ NÃO pode usar recargas de 202502 (fevereiro) → futuro/presente (LEAKAGE!)
```

**Por que isso importa?**
- Na data da decisão (SAFRA), só temos informação do passado
- Usar dados do mesmo mês ou futuros seria "ver o futuro" (leakage)
- O modelo ficaria artificialmente bom em treino mas ruim em produção

---

## O Que Muda em Relação à v4

| Aspecto | ABT v4 | ABT v5 |
|---------|--------|--------|
| **Input adicional** | Silver Cadastro | **Gold Recarga Features** |
| **Operação** | LEFT JOIN direto | LEFT JOIN + filtro temporal + agregação |
| **Features novas** | ~57 (Cadastro) | **~120** (Recarga × 3 janelas) |
| **Colunas** | ~185 | **~311** (+126) |
| **Complexidade** | Simples | **Alta** (janelas temporais) |

---

## Código Explicado Linha por Linha

### 1. Configuração de Features (Linhas 159-204)

```python
# Features do Gold Recarga a serem agregadas por janela
FEATURES_SOMA = [
    "qtd_recargas_mes",
    "qtd_recargas_validas_mes",
    "sum_val_credito_mes",
    "sum_val_bonus_mes",
    "sum_val_real_mes",
    "sum_val_real_ajustado_mes",
    "qtd_sos_mes",
    "sum_valor_sos_mes",
    # ...
]

FEATURES_MEDIA = [
    "avg_val_real_mes",
    "ticket_medio_mes",
    "dias_medio_entre_recargas_mes",
    "coef_variacao_val_mes",
    # ...
]

FEATURES_MAX = [
    "max_val_real_mes",
    "dias_max_entre_recargas_mes",
    "flag_teve_sos_mes",
    # ...
]

FEATURES_MIN = [
    "min_val_real_mes",
    "dias_min_entre_recargas_mes",
]
```

**Por que categorizar features por tipo de agregação?**

| Tipo | Agregação | Exemplo | Motivo |
|------|-----------|---------|--------|
| **SOMA** | F.sum() | qtd_recargas | Acumula ao longo da janela |
| **MEDIA** | F.avg() | ticket_medio | Média ponderada pelo tempo |
| **MAX** | F.max() | flag_teve_sos | Basta ter 1 para ser verdade |
| **MIN** | F.min() | dias_min | Menor valor é o que importa |

**Exemplo prático para janela M3:**
```
Mês 1: qtd_recargas=5, flag_teve_sos=0
Mês 2: qtd_recargas=3, flag_teve_sos=1
Mês 3: qtd_recargas=4, flag_teve_sos=0

Resultado M3:
- qtd_recargas_m3 = SUM(5,3,4) = 12
- flag_teve_sos_m3 = MAX(0,1,0) = 1 (teve SOS em algum mês)
```

---

### 2. Função agregar_recarga_por_janela (Linhas 211-308)

Esta é a função central que aplica as janelas temporais.

#### 2.1 Preparação para JOIN (Linhas 239-252)

```python
    # Preparar spine: apenas chaves necessárias
    df_keys = df_spine.select(
        "num_cpf",
        "safra",
        "dt_safra"
    ).distinct()

    # Preparar recarga: garantir dt_recarga_safra existe
    df_rec = df_recarga_features
    if "dt_recarga_safra" not in df_rec.columns:
        df_rec = df_rec.withColumn(
            "dt_recarga_safra",
            F.to_date(F.concat(F.col("safra_recarga"), F.lit("01")), "yyyyMMdd")
        )
```

**Por que select apenas chaves do spine?**
- Reduz dados no shuffle do JOIN
- Evita conflitos de colunas
- Melhora performance significativamente

**Por que distinct()?**
- Remove possíveis duplicatas nas chaves
- Garante grão 1:1 para o JOIN

---

#### 2.2 JOIN por NUM_CPF (Linha 254-259)

```python
    # JOIN por NUM_CPF (cross join parcial para pegar todas as combinações)
    df_joined = df_keys.join(
        df_rec,
        on="num_cpf",
        how="left"
    )
```

**Por que JOIN apenas por NUM_CPF (sem SAFRA)?**

Este é um **JOIN especial** que gera todas as combinações cliente × mês de recarga:

```
ANTES DO JOIN:
Spine (chaves)           Recarga Features
┌────────┬────────┐      ┌────────┬──────────────┐
│ cpf    │ safra  │      │ cpf    │ safra_recarga│
├────────┼────────┤      ├────────┼──────────────┤
│ AAA    │ 202502 │      │ AAA    │ 202501       │
│ BBB    │ 202502 │      │ AAA    │ 202412       │
└────────┴────────┘      │ AAA    │ 202411       │
                         │ BBB    │ 202501       │
                         └────────┴──────────────┘

APÓS JOIN POR NUM_CPF:
┌────────┬────────┬──────────────┐
│ cpf    │ safra  │ safra_recarga│
├────────┼────────┼──────────────┤
│ AAA    │ 202502 │ 202501       │  ← AAA pode ver suas 3 safras de recarga
│ AAA    │ 202502 │ 202412       │
│ AAA    │ 202502 │ 202411       │
│ BBB    │ 202502 │ 202501       │  ← BBB vê sua 1 safra de recarga
└────────┴────────┴──────────────┘
```

---

#### 2.3 Filtro Temporal (Linhas 261-266)

```python
    # Filtrar: SAFRA_RECARGA dentro da janela temporal
    # Condição: dt_recarga_safra >= (dt_safra - N meses) AND dt_recarga_safra < dt_safra
    df_filtered = df_joined.filter(
        (F.col("dt_recarga_safra") >= F.add_months(F.col("dt_safra"), -num_meses)) &
        (F.col("dt_recarga_safra") < F.col("dt_safra"))
    )
```

**Explicação da condição de filtro:**

```python
# Para M3 (num_meses=3) e cliente com safra=2025-02-01:

dt_recarga_safra >= add_months(dt_safra, -3)  # >= 2024-11-01
AND
dt_recarga_safra < dt_safra                    # < 2025-02-01

# Resultado: recargas de Nov, Dez, Jan (3 meses anteriores)
```

**Por que `<` e não `<=` para dt_safra?**
- `< dt_safra` exclui o mês da decisão
- Garante anti-leakage: só vemos o passado

**Por que F.add_months() e não aritmética de datas?**
```python
# CORRETO: F.add_months() trata meses corretamente
F.add_months(F.col("dt_safra"), -3)  # 2025-02-01 → 2024-11-01

# INCORRETO: aritmética de dias não funciona para meses
F.col("dt_safra") - 90  # Problemas com meses de 28/30/31 dias
```

---

#### 2.4 Agregação com Expressões Dinâmicas (Linhas 271-306)

```python
    # Agregar por NUM_CPF + SAFRA
    agg_exprs = []

    # Contagem de meses com dados
    agg_exprs.append(F.countDistinct("safra_recarga").alias(f"qtd_meses_com_recarga{sfx}"))

    # Somas
    for col in FEATURES_SOMA:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.sum(F.coalesce(F.col(col), F.lit(0))).alias(f"{col_clean}{sfx}")
        )

    # Médias
    for col in FEATURES_MEDIA:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.avg(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Máximos
    for col in FEATURES_MAX:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.max(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Mínimos
    for col in FEATURES_MIN:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.min(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Executar agregação
    df_agg = df_filtered.groupBy("num_cpf", "safra").agg(*agg_exprs)
```

**Padrão de renomeação de colunas:**
```python
col.replace("_mes", "")
# "qtd_recargas_mes" → "qtd_recargas"
# + sufixo da janela → "qtd_recargas_m1"
```

**Por que F.coalesce(col, lit(0)) nas somas?**
- Trata NULLs como 0 para não propagar NULL na soma
- Sem isso, SUM de [5, NULL, 3] = NULL (não 8)

**Por que construir lista de expressões dinamicamente?**
```python
# ABORDAGEM ESCOLHIDA: lista dinâmica
agg_exprs = []
for col in FEATURES_SOMA:
    agg_exprs.append(F.sum(...).alias(...))
df.groupBy(...).agg(*agg_exprs)

# ALTERNATIVA: hardcoded (não escalável)
df.groupBy(...).agg(
    F.sum("qtd_recargas_mes").alias("qtd_recargas_m1"),
    F.sum("sum_val_credito_mes").alias("sum_val_credito_m1"),
    # ... mais 50 linhas ...
)
```

---

### 3. Função criar_features_derivadas_janela (Linhas 311-388)

```python
def criar_features_derivadas_janela(df: DataFrame, janela: str) -> DataFrame:
    sfx = f"_{janela}"

    # Ticket médio global da janela (recalculado)
    df = df.withColumn(
        f"ticket_medio_global{sfx}",
        F.when(
            F.col(f"qtd_recargas_validas{sfx}") > 0,
            F.round(F.col(f"sum_val_real_ajustado{sfx}") / F.col(f"qtd_recargas_validas{sfx}"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre crédito (recalculado para a janela)
    df = df.withColumn(
        f"pct_sos_credito_janela{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round((F.col(f"sum_valor_sos{sfx}") / F.col(f"sum_val_credito{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequência SOS na janela
    df = df.withColumn(
        f"freq_sos_janela{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round(F.col(f"qtd_sos{sfx}") / F.col(f"qtd_recargas{sfx}"), 4)
        ).otherwise(0.0)
    )

    # ... mais features derivadas ...

    # Flag sem recarga na janela
    df = df.withColumn(
        f"flag_sem_recarga{sfx}",
        F.when(F.col(f"qtd_recargas{sfx}") == 0, 1).otherwise(0)
    )

    # Flag baixa atividade
    limiar = {"m1": 2, "m3": 5, "m6": 10}.get(janela, 2)
    df = df.withColumn(
        f"flag_baixa_atividade{sfx}",
        F.when(F.col(f"qtd_recargas_validas{sfx}") < limiar, 1).otherwise(0)
    )

    return df
```

**Por que recalcular features derivadas para a janela?**

As features mensais do Gold Recarga foram calculadas por mês. Ao agregar M3 (3 meses), precisamos recalcular:

| Feature | No Gold Recarga (mensal) | Na ABT v5 (M3) |
|---------|--------------------------|----------------|
| ticket_medio | soma_mes / qtd_mes | soma_3_meses / qtd_3_meses |
| pct_sos | sos_mes / credito_mes | sos_3_meses / credito_3_meses |

**Por que limiares diferentes por janela?**
```python
limiar = {"m1": 2, "m3": 5, "m6": 10}.get(janela, 2)
```
- M1: < 2 recargas/mês = baixa atividade
- M3: < 5 recargas/trimestre = baixa atividade (~ 1.7/mês)
- M6: < 10 recargas/semestre = baixa atividade (~ 1.7/mês)

---

### 4. Função build_abt_v5 - Orquestração (Linhas 391-493)

```python
def build_abt_v5(df_abt_v4: DataFrame, df_recarga_features: DataFrame) -> DataFrame:
    # Dicionário para armazenar agregações por janela
    agg_por_janela = {}

    # Agregar para cada janela temporal
    for janela, num_meses in TEMPORAL_WINDOWS.items():
        print(f"\n>>> [Window] Processando janela {janela.upper()}...")

        # Agregar features
        df_agg = agregar_recarga_por_janela(
            df_abt_v4, df_recarga_features, janela, num_meses
        )

        # Criar features derivadas
        df_agg = criar_features_derivadas_janela(df_agg, janela)

        agg_por_janela[janela] = df_agg
```

**Padrão de processamento por janela:**
1. Loop por cada janela (M1, M3, M6)
2. Agrega features para a janela
3. Cria features derivadas
4. Armazena no dicionário

---

#### 4.1 Combinação das Janelas (Linhas 432-441)

```python
    # Combinar todas as janelas em um único DataFrame
    print("\n>>> [Join] Combinando janelas M1, M3, M6...")

    df_recarga_all = agg_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_recarga_all = df_recarga_all.join(
            agg_por_janela[janela],
            on=["num_cpf", "safra"],
            how="outer"
        )
```

**Por que OUTER JOIN entre janelas?**
- Cliente pode ter dados em M3 mas não em M1 (inativo recentemente)
- Cliente pode ter dados em M6 mas não em M3 (muito inativo)
- OUTER preserva todos os casos

---

#### 4.2 JOIN Final com ABT v4 (Linhas 443-450)

```python
    # JOIN com ABT v4 (LEFT JOIN para manter todos os registros do spine)
    print(">>> [Join] JOIN ABT v4 + Recarga Features...")

    df_abt_v5 = df_abt_v4.join(
        df_recarga_all,
        on=["num_cpf", "safra"],
        how="left"
    )
```

**Por que LEFT JOIN com ABT v4?**
- ABT v4 é o spine (universo oficial)
- Nem todos os clientes têm recarga
- LEFT preserva todos do spine, NULLs onde não há recarga

---

#### 4.3 Preenchimento de NULLs (Linhas 452-483)

```python
    # Preencher NULLs com 0 para features de contagem/soma
    print(">>> [Clean] Preenchendo NULLs com valores default...")

    recarga_cols = [c for c in df_abt_v5.columns if any(
        c.endswith(f"_{j}") for j in TEMPORAL_WINDOWS.keys()
    )]

    for col in recarga_cols:
        # Colunas de contagem/soma: default 0
        if any(x in col for x in ["qtd_", "sum_", "flag_"]):
            df_abt_v5 = df_abt_v5.withColumn(
                col, F.coalesce(F.col(col), F.lit(0))
            )
        # Colunas de percentual: default 0.0
        elif any(x in col for x in ["pct_", "freq_"]):
            df_abt_v5 = df_abt_v5.withColumn(
                col, F.coalesce(F.col(col), F.lit(0.0))
            )
        # Colunas de média/valores: manter NULL (indica ausência de dados)
```

**Regras de preenchimento de NULL:**

| Tipo de Coluna | Default | Motivo |
|----------------|---------|--------|
| qtd_*, sum_*, flag_* | 0 | Sem dados = zero |
| pct_*, freq_* | 0.0 | Sem dados = zero por cento |
| avg_*, dias_*, ticket_* | **NULL** | Média de nada não é zero |

**Por que manter NULL para médias?**
- Se cliente não tem recarga, a média não é 0
- É "desconhecida" ou "não aplicável"
- NULL indica ausência de informação (mais correto semanticamente)

---

### 5. Validação de 11 Gates (Linhas 496-641)

```python
def validate_abt_v5_enhanced(df: DataFrame, count_expected: int) -> bool:
    """
    Gates:
    1-8: Herdados (unicidade, anti-leakage, labels, scores)
    9: Cobertura Recarga M1 > 5%
    10: Distribuição SOS sensata
    11: Valores não-negativos
    """
```

**Gates específicos de v5:**

| Gate | Validação | Threshold |
|------|-----------|-----------|
| 9 | Recarga M1 cobertura | > 5% |
| 10 | Média pct_sos_credito | < 50% |
| 11 | Valores negativos | < 1% |

**Por que threshold baixo (5%) para Recarga M1?**
- Nem todos os clientes têm recarga no último mês
- 5% garante que o pipeline funcionou
- Cobertura real é ~56%

---

## Diagrama de Fluxo Completo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    04_gold_abt_v5_builder_v2.py                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐              ┌──────────────────┐                     │
│  │ LEITURA ABT v4   │              │ LEITURA RECARGA  │                     │
│  │ (Gold - spine)   │              │ FEATURES v2      │                     │
│  │ 3,795,310 reg    │              │ 32,882,218 reg   │                     │
│  │ ~185 colunas     │              │ 51 colunas       │                     │
│  └────────┬─────────┘              └────────┬─────────┘                     │
│           │                                 │                               │
│           │                 ┌───────────────┼───────────────┐               │
│           │                 │               │               │               │
│           │                 ▼               ▼               ▼               │
│           │         ┌─────────────┐ ┌─────────────┐ ┌─────────────┐         │
│           │         │ AGREGAR M1  │ │ AGREGAR M3  │ │ AGREGAR M6  │         │
│           │         │ (1 mês)     │ │ (3 meses)   │ │ (6 meses)   │         │
│           │         │             │ │             │ │             │         │
│           │         │ JOIN + FILT │ │ JOIN + FILT │ │ JOIN + FILT │         │
│           │         │ + AGG       │ │ + AGG       │ │ + AGG       │         │
│           │         │ + DERIVADAS │ │ + DERIVADAS │ │ + DERIVADAS │         │
│           │         └──────┬──────┘ └──────┬──────┘ └──────┬──────┘         │
│           │                │               │               │                │
│           │                └───────────────┼───────────────┘                │
│           │                                │                                │
│           │                                ▼                                │
│           │                ┌───────────────────────────┐                    │
│           │                │ COMBINAR M1 + M3 + M6     │                    │
│           │                │ (OUTER JOIN entre janelas)│                    │
│           │                └─────────────┬─────────────┘                    │
│           │                              │                                  │
│           └──────────────────────────────┤                                  │
│                                          │                                  │
│                                          ▼                                  │
│                          ┌───────────────────────────┐                      │
│                          │ LEFT JOIN ABT v4 + RECARGA│                      │
│                          │ (spine preservado)        │                      │
│                          └─────────────┬─────────────┘                      │
│                                        │                                    │
│                                        ▼                                    │
│                          ┌───────────────────────────┐                      │
│                          │ PREENCHER NULLs           │                      │
│                          │ qtd/sum/flag → 0          │                      │
│                          │ pct/freq → 0.0            │                      │
│                          │ avg/dias → manter NULL    │                      │
│                          └─────────────┬─────────────┘                      │
│                                        │                                    │
│                                        ▼                                    │
│                          ┌───────────────────────────┐                      │
│                          │ VALIDAÇÃO (11 GATES)      │                      │
│                          └─────────────┬─────────────┘                      │
│                                        │                                    │
│                                        ▼                                    │
│                          ┌───────────────────────────┐                      │
│                          │ ESCRITA ABT v5            │                      │
│                          │ ~311 colunas              │                      │
│                          │ 3,795,310 registros       │                      │
│                          └───────────────────────────┘                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Colunas de Saída (ABT v5)

### Resumo por Grupo

| Grupo | Colunas | Quantidade |
|-------|---------|------------|
| Herdadas v4 | Chaves, Labels, Scores, Telco, Cadastro | ~185 |
| **Recarga M1** | Volume, Valores, SOS, Temporal, Flags | ~42 |
| **Recarga M3** | Volume, Valores, SOS, Temporal, Flags | ~42 |
| **Recarga M6** | Volume, Valores, SOS, Temporal, Flags | ~42 |
| **TOTAL** | | **~311** |

### Features de Recarga por Janela

Cada janela (M1, M3, M6) contém:

| Categoria | Features | Exemplo M1 |
|-----------|----------|------------|
| Volume | qtd_recargas, qtd_validas, qtd_telefones | qtd_recargas_m1 |
| Valores | sum_val_credito, sum_val_real_ajustado, avg, min, max | sum_val_credito_m1 |
| SOS | qtd_sos, sum_valor_sos, flag_teve_sos, pct_sos, freq_sos | freq_sos_m1 |
| Temporal | dias_medio, dias_max, qtd_meses_com_recarga | dias_max_entre_recargas_m1 |
| Horário | qtd_madrugada, pct_fim_semana | pct_recargas_madrugada_m1 |
| Flags | flag_sem_recarga, flag_baixa_atividade | flag_sem_recarga_m1 |

---

## Lições Aprendidas

### 1. JOIN por NUM_CPF + Filtro Temporal

**Padrão:** JOIN amplo (por CPF) seguido de filtro temporal.
**Motivo:** Permite flexibilidade para diferentes janelas.

### 2. Agregação com Expressões Dinâmicas

**Padrão:** Construir lista de agregações em loop.
**Benefício:** Código mais manutenível para muitas features.

### 3. Preenchimento Seletivo de NULLs

**Regra:** Contagens/somas → 0, médias → manter NULL.
**Motivo:** Semântica correta (média de nada não é zero).

### 4. F.add_months() para Aritmética de Datas

**Padrão:** Usar `F.add_months()` em vez de aritmética de dias.
**Motivo:** Trata meses de tamanhos diferentes corretamente.

---

## Exemplo de Saída do Relatório

```
╔==============================================================================╗
║                           ABT V5 BUILDER V2                                  ║
║              ABT v4 + Recarga Features v2 (M1/M3/M6)                        ║
╚==============================================================================╝

>>> [Leitura] Carregando ABT v4 (spine): /Volumes/.../gold/abt_v4_delta/
>>> [Info] Registros no ABT v4: 3,795,310
>>> [Info] Colunas no ABT v4: 185

>>> [Leitura] Carregando Gold Recarga Features v2
>>> [Info] Registros no Recarga Features: 32,882,218

================================================================================
CONSTRUINDO ABT v5 (v4 + Recarga M1/M3/M6)
================================================================================

>>> [Window] Processando janela M1...
    → 2,130,456 registros com dados para M1
>>> [Window] Processando janela M3...
    → 2,567,890 registros com dados para M3
>>> [Window] Processando janela M6...
    → 2,890,123 registros com dados para M6

>>> [Join] Combinando janelas M1, M3, M6...
>>> [Join] JOIN ABT v4 + Recarga Features...
>>> [Clean] Preenchendo NULLs com valores default...

>>> [Info] Registros no ABT v5: 3,795,310
>>> [Info] Colunas no ABT v5: 311

================================================================================
VALIDAÇÃO ABT v5 (11 GATES)
================================================================================

>>> [Gate 1] Verificando unicidade... ✓ PASS
>>> [Gate 9] Verificando cobertura Recarga M1...
    Recarga M1 cobertura: 56.12%
    ✓ PASS
>>> [Gate 10] Verificando distribuição SOS...
    Média pct_sos_credito M1: 12.34%
    ✓ PASS

✓ TODOS OS GATES PASSARAM!
================================================================================
```

---

## Checklist de Revisão

- [x] JOIN por NUM_CPF (não por safra) para permitir filtro temporal
- [x] Filtro `dt_recarga_safra < dt_safra` (anti-leakage)
- [x] F.add_months() para cálculo de janelas
- [x] Agregações separadas por tipo (SUM, AVG, MAX, MIN)
- [x] OUTER JOIN entre janelas (M1, M3, M6)
- [x] LEFT JOIN final com ABT v4 (preserva spine)
- [x] Preenchimento seletivo de NULLs
- [x] 11 gates de validação
- [x] Metadados gold_feature_blocks atualizados

---

## Próximo Passo

A ABT v5 serve como base para a ABT v6, que adiciona Pagamento e Atraso:

```
ABT v5 (Scores + Telco + Cadastro + Recarga M1/M3/M6)
        │
        │ + Gold Pagamento Features v2
        │ + Gold Atraso Features v2
        ▼
ABT v6 (614 colunas - FINAL)
```

Ver [06_ABT_V6_EXPLAINED.md](06_ABT_V6_EXPLAINED.md).
