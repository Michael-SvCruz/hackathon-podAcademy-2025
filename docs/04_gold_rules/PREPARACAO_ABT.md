# Preparação da Tabela Analítica de Modelagem (ABT)

> **Projeto:** Hackathon PodAcademy 2025 - Modelo de Risco de Crédito
>
> **ABT Final:** `hackathon_2025.default.gold_abt_v6_v2` (614 colunas, 3.79M registros)

---

## Índice

1. [Visão Geral do Processo](#1-visão-geral-do-processo)
2. [Fontes de Dados Utilizadas](#2-fontes-de-dados-utilizadas)
3. [Estratégia de Integração](#3-estratégia-de-integração)
4. [Transformações Realizadas](#4-transformações-realizadas)
5. [Evolução Incremental da ABT](#5-evolução-incremental-da-abt)
6. [Tratamento de Valores Ausentes](#6-tratamento-de-valores-ausentes)
7. [Validações Implementadas](#7-validações-implementadas-gates)
8. [Resultado Final](#8-resultado-final)

---

## 1. Visão Geral do Processo

A ABT foi construída utilizando a **Medallion Architecture** (Landing → Bronze → Silver → Gold), com versionamento incremental que permitiu adicionar blocos de features de forma controlada e rastreável.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FLUXO DE CONSTRUÇÃO DA ABT                          │
│                                                                             │
│   LANDING        BRONZE         SILVER           GOLD                      │
│   (Parquet)   (+ Metadata)   (Tipado/Validado)  (Features/ABT)            │
│                                                                             │
│   Bureau ────▶ Bureau ────▶ Bureau ────┐                                   │
│   Telco  ────▶ Telco  ────▶ Telco  ────┤                                   │
│   Cadastro ──▶ Cadastro ──▶ Cadastro ──┼───▶ ABT v1 → v2 → v3 → v4        │
│   Recarga ───▶ Recarga ───▶ Recarga ───┼───▶ Recarga Features ──▶ ABT v5  │
│   Pagamento ─▶ Pagamento ─▶ Pagamento ─┼───▶ Pagamento Features ─┐        │
│   Atraso ────▶ Atraso ────▶ Atraso ────┴───▶ Atraso Features ────┴▶ ABT v6│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Princípios de Design

| Princípio | Descrição |
|-----------|-----------|
| **Versionamento Incremental** | Cada versão adiciona um bloco de features, permitindo medir o ganho incremental de KS |
| **Anti-Leakage** | Features comportamentais usam apenas dados anteriores à SAFRA |
| **Reprodutibilidade** | Metadados de versão e data de build em cada registro |
| **Validação Automática** | Gates de qualidade executados a cada build |

---

## 2. Fontes de Dados Utilizadas

| Fonte | Registros | Descrição | Granularidade |
|-------|-----------|-----------|---------------|
| **Bureau** | 3.79M | Scores de crédito (Score_01, Score_02) e variáveis target | CPF + SAFRA |
| **Telco** | 3.79M | Variáveis de comportamento telefônico (var_26 a var_93) | CPF + SAFRA |
| **Cadastro** | 3.79M | Dados demográficos (idade, UF, tempo de relacionamento) | CPF + SAFRA |
| **Recarga** | 95.2M | Histórico de recargas (valores, frequência, SOS) | CPF + MÊS (evento) |
| **Pagamento** | 21.8M | Histórico de pagamentos (valores, juros, descontos) | CPF + MÊS (evento) |
| **Atraso** | 31.6M | Snapshot de faturas em aberto (aging, status) | CPF + MÊS (snapshot) |

### Descrição das Fontes

**Bureau (Spine):**
- Fonte principal que define a população da ABT
- Contém `Score_01` e `Score_02` (scores de crédito externos)
- Contém variáveis target: `FPD_INT` (First Payment Default) e `FLAG_INSTALACAO_INT` (aprovação)

**Telco:**
- 68 variáveis comportamentais (var_26 a var_93)
- Informações sobre uso de telefonia
- Valor sentinela 304 = dado não disponível

**Cadastro:**
- Dados demográficos e de relacionamento
- Idade, UF, tempo como cliente
- Corrigido em Jan/2026 (substituição de UDF por funções nativas Spark)

**Recarga:**
- Histórico de todas as recargas do cliente
- Inclui indicador SOS (empréstimo emergencial - indicador de estresse financeiro)
- Requer agregação temporal (evento → cliente-mês)

**Pagamento:**
- Histórico de pagamentos de faturas
- Inclui valores de juros (indicador de atraso passado) e descontos (negociação)
- Requer agregação temporal

**Atraso:**
- Snapshot mensal de faturas em aberto
- Distribuição de aging (0-30, 31-60, 61-90, 90+ dias)
- Flags de status crítico (WO, PDD, Fraude)

---

## 3. Estratégia de Integração

### 3.1 Spine (Tabela Base)

O **Bureau** foi utilizado como spine por conter:
- A população completa de clientes elegíveis
- As variáveis target (`FPD_INT`) e decisão (`FLAG_INSTALACAO_INT`)
- A granularidade correta (CPF + SAFRA)

### 3.2 Chave de Junção

```
Chave: NUM_CPF + SAFRA
```

| Campo | Descrição |
|-------|-----------|
| `NUM_CPF` | Identificador único do cliente (11 dígitos) |
| `SAFRA` | Mês de referência no formato YYYYMM (ex: 202503) |

**Garantia de unicidade:** Cada combinação CPF + SAFRA aparece exatamente uma vez na ABT (1:1).

### 3.3 Tipo de JOIN

```python
# LEFT JOIN para preservar todos os registros da spine
df_abt = df_spine.join(df_features, on=["num_cpf", "safra"], how="left")
```

**Justificativa:** LEFT JOIN garante que todos os clientes da população original sejam mantidos, mesmo quando não possuem dados em alguma fonte comportamental.

### 3.4 Diagrama de Junções

```
                    ┌──────────────┐
                    │    Bureau    │
                    │   (Spine)    │
                    │   3.79M      │
                    └──────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │  Telco   │    │ Cadastro │    │ Features │
    │  LEFT    │    │  LEFT    │    │  LEFT    │
    └──────────┘    └──────────┘    └──────────┘
                                          │
                           ┌──────────────┼──────────────┐
                           │              │              │
                           ▼              ▼              ▼
                    ┌──────────┐   ┌──────────┐   ┌──────────┐
                    │ Recarga  │   │Pagamento │   │  Atraso  │
                    │ M1/M3/M6 │   │ M1/M3/M6 │   │ M1/M3/M6 │
                    └──────────┘   └──────────┘   └──────────┘
```

---

## 4. Transformações Realizadas

### 4.1 Camada Bronze → Silver

| Transformação | Descrição | Exemplo |
|---------------|-----------|---------|
| **Type Casting** | Conversão de strings para tipos apropriados | `"650"` → `650` (int) |
| **Padronização** | Nomes de colunas em snake_case | `DataNascimento` → `data_nascimento` |
| **Deduplicação** | Remoção de registros duplicados | Via hash MD5 ou row_number() |
| **Tratamento de Sentinelas** | Valores especiais convertidos para NULL | `0` → NULL + `flag_score_missing` |

**Valores Sentinela por Fonte:**

| Fonte | Valor | Significado | Tratamento |
|-------|-------|-------------|------------|
| Bureau (Score) | 0 | Score não calculado | NULL + flag |
| Telco | 304 | Dado não disponível | NULL + flag |
| Recarga | -1, -2, -3 | Não aplica, não determinado, não informado | NULL |

### 4.2 Camada Silver → Gold (Feature Engineering)

#### Recarga Features (60+ variáveis)

```python
# ═══════════════════════════════════════════════════════════════
# INDICADOR DE ESTRESSE FINANCEIRO (SOS)
# ═══════════════════════════════════════════════════════════════
# SOS é um empréstimo emergencial (R$3-20) descontado na próxima recarga
# Alta frequência de SOS indica dificuldade financeira

freq_sos_m1                # Frequência de uso do SOS no último mês
pct_sos_sobre_credito_m1   # Proporção SOS/crédito total

# ═══════════════════════════════════════════════════════════════
# PADRÕES TEMPORAIS
# ═══════════════════════════════════════════════════════════════
dias_medio_entre_recargas_m1   # Regularidade do comportamento
dias_max_entre_recargas_m1     # Maior intervalo (possível dificuldade)

# ═══════════════════════════════════════════════════════════════
# MÉTRICAS DE VALOR
# ═══════════════════════════════════════════════════════════════
ticket_medio_m1            # Valor médio por recarga
coef_variacao_val_m1       # Estabilidade (desvio/média)
sum_val_credito_m1         # Volume total de crédito

# ═══════════════════════════════════════════════════════════════
# COMPORTAMENTO
# ═══════════════════════════════════════════════════════════════
pct_recargas_madrugada_m1     # Recargas entre 00h-06h
pct_recargas_fim_semana_m1    # Recargas sáb/dom
qtd_canais_distintos_m1       # Diversificação de canais
```

#### Pagamento Features (50+ variáveis)

```python
# ═══════════════════════════════════════════════════════════════
# VOLUME E VALORES
# ═══════════════════════════════════════════════════════════════
qtd_pagamentos_validos_m1     # Quantidade de pagamentos
sum_val_pago_m1               # Valor total pago
ticket_medio_pag_m1           # Valor médio por pagamento

# ═══════════════════════════════════════════════════════════════
# INDICADORES DE NEGOCIAÇÃO
# ═══════════════════════════════════════════════════════════════
pct_com_desconto_m1           # % de pagamentos com desconto
ratio_desconto_pago_m1        # Desconto/Valor pago

# ═══════════════════════════════════════════════════════════════
# INDICADORES DE ATRASO PASSADO
# ═══════════════════════════════════════════════════════════════
# Juros positivo = cliente pagou com atraso em fatura anterior
pct_com_juros_m1              # % pagamentos com juros
sum_val_juros_pos_m1          # Total de juros pagos
ratio_juros_pago_m1           # Juros/Valor pago
```

#### Atraso Features (60+ variáveis)

```python
# ═══════════════════════════════════════════════════════════════
# EXPOSIÇÃO ATUAL
# ═══════════════════════════════════════════════════════════════
qtd_faturas_abertas_m1        # Número de faturas em aberto
sum_val_aberto_m1             # Valor total em aberto

# ═══════════════════════════════════════════════════════════════
# DISTRIBUIÇÃO DE AGING
# ═══════════════════════════════════════════════════════════════
pct_aging_0_30_m1             # % em atraso leve (até 30 dias)
pct_aging_31_60_m1            # % em atraso moderado
pct_aging_61_90_m1            # % em atraso alto
pct_aging_90_plus_m1          # % em atraso severo (90+ dias)

# ═══════════════════════════════════════════════════════════════
# FLAGS DE RISCO CRÍTICO
# ═══════════════════════════════════════════════════════════════
flag_teve_wo_m1               # Write-off (perda contábil)
flag_teve_pdd_m1              # Provisão para devedores duvidosos
flag_teve_fraude_m1           # Indicação de fraude
flag_teve_aca_m1              # Acordo de cobrança amigável
flag_teve_pccr_m1             # Processo de cobrança/recuperação
```

### 4.3 Janelas Temporais (Anti-Leakage)

Para features comportamentais (Recarga, Pagamento, Atraso), aplicamos janelas temporais com regra estrita de anti-leakage:

```python
# ═══════════════════════════════════════════════════════════════
# REGRA DE ANTI-LEAKAGE
# ═══════════════════════════════════════════════════════════════
# SAFRA_FEATURE < SAFRA_ABT (apenas dados do passado)
# Nunca usar dados do mesmo mês ou futuro

TEMPORAL_WINDOWS = {
    "m1": 1,   # 1 mês anterior
    "m3": 3,   # 3 meses anteriores
    "m6": 6    # 6 meses anteriores
}

# Filtro aplicado na agregação
df_filtered = df_joined.filter(
    (F.col("dt_safra_feature") >= F.add_months(F.col("dt_safra"), -num_meses)) &
    (F.col("dt_safra_feature") < F.col("dt_safra"))  # ESTRITAMENTE anterior
)
```

**Exemplo Visual:**

```
SAFRA ABT: 202503 (Março 2025)

Janela M1: usa apenas 202502 (Fevereiro)
Janela M3: usa 202502, 202501, 202412 (Fev, Jan, Dez)
Janela M6: usa 202502, 202501, 202412, 202411, 202410, 202409

NÃO USA: 202503 (mesmo mês) nem futuros
```

### 4.4 Regras de Agregação por Tipo de Coluna

| Prefixo da Coluna | Agregação | Justificativa | Exemplo |
|-------------------|-----------|---------------|---------|
| `qtd_*`, `sum_*` | **SUM** | Acumular totais | `sum_val_pago_m3` = soma dos 3 meses |
| `flag_*` | **MAX** | Se ocorreu em qualquer mês | `flag_teve_wo_m6` = 1 se teve WO |
| `pct_*`, `ratio_*`, `avg_*` | **AVG** | Média do comportamento | `pct_com_juros_m6` = média |
| `max_*` | **MAX** | Pior caso no período | `max_aging_m1` = maior aging |
| `min_*` | **MIN** | Melhor caso no período | `min_val_pago_m3` |

```python
# Implementação no código
for col in feature_cols:
    if col.startswith(("qtd_", "sum_")):
        agg_exprs.append(F.sum(F.col(col)).alias(f"{col}{sfx}"))
    elif col.startswith("flag_"):
        agg_exprs.append(F.max(F.col(col)).alias(f"{col}{sfx}"))
    elif col.startswith(("pct_", "ratio_", "avg_")):
        agg_exprs.append(F.avg(F.col(col)).alias(f"{col}{sfx}"))
```

---

## 5. Evolução Incremental da ABT

### 5.1 Histórico de Versões

| Versão | Fontes Adicionadas | Novas Variáveis | Total Colunas | KS OOT |
|--------|-------------------|-----------------|---------------|--------|
| **v1** | Bureau (Score_01) | Score_01 + metadados | 15 | ~28% |
| **v2** | + Score_02 | Score_02 | 20 | ~29% |
| **v3** | + Telco | var_26 a var_93 (68 vars) | 95 | ~30% |
| **v4** | + Cadastro | idade, UF, tempo_relacionamento | 185 | ~31% |
| **v5** | + Recarga | Features M1/M3/M6 (60+ vars) | 311 | ~32% |
| **v6** | + Pagamento + Atraso | Features M1/M3/M6 (170+ vars) | **614** | **33.94%** |

### 5.2 Ganho Incremental de KS

```
┌─────────────────────────────────────────────────────────────────┐
│                    GANHO INCREMENTAL DE KS                      │
│                                                                 │
│   v1 (Score_01)         ████████████████████████████  28%      │
│   v2 (+Score_02)        █████████████████████████████ 29% +1   │
│   v3 (+Telco)           ██████████████████████████████ 30% +1  │
│   v4 (+Cadastro)        ███████████████████████████████ 31% +1 │
│   v5 (+Recarga)         ████████████████████████████████ 32% +1│
│   v6 (+Pag+Atraso)      █████████████████████████████████ 34%+2│
│   ───────────────────────────────────────────────────────────  │
│   BENCHMARK             ████████████████████████████████ 33.1% │
│   MODELO FINAL          █████████████████████████████████33.94%│
│                                                   +0.84 p.p.   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Tratamento de Valores Ausentes

### 6.1 Estratégia por Tipo de Feature

```python
# ═══════════════════════════════════════════════════════════════
# FEATURES DE CONTAGEM/SOMA: Preencher com 0
# ═══════════════════════════════════════════════════════════════
# Justificativa: Ausência de dados = 0 ocorrências
for col in df.columns:
    if col.startswith(("qtd_", "sum_", "flag_")):
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# ═══════════════════════════════════════════════════════════════
# FEATURES DE PROPORÇÃO/PERCENTUAL: Preencher com 0.0
# ═══════════════════════════════════════════════════════════════
for col in df.columns:
    if col.startswith(("pct_", "ratio_")):
        df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
```

### 6.2 Flags de Cobertura

Para cada janela temporal, criamos flags indicando ausência de dados:

```python
# Flag indica que cliente não tem dados naquela janela
df = df.withColumn(
    f"flag_sem_recarga_{janela}",
    F.when(
        F.col(f"qtd_meses_dados_rec_{janela}").isNull() |
        (F.col(f"qtd_meses_dados_rec_{janela}") == 0),
        1
    ).otherwise(0)
)

# Variáveis criadas:
# - flag_sem_recarga_m1, flag_sem_recarga_m3, flag_sem_recarga_m6
# - flag_sem_pagamento_m1, flag_sem_pagamento_m3, flag_sem_pagamento_m6
# - flag_sem_atraso_m1, flag_sem_atraso_m3, flag_sem_atraso_m6
```

### 6.3 Cobertura por Bloco de Features

| Bloco | Cobertura M1 | Observação |
|-------|--------------|------------|
| Score_01 | 98.18% | Muito alta (bureau completo) |
| Score_02 | 99.95% | Muito alta |
| Telco | 35.46% | Moderada (nem todos têm dados telco) |
| Cadastro | 35-40% | Corrigido Jan/2026 |
| Recarga | 56.12% | Boa cobertura |
| Pagamento | 16.13% | Baixa (clientes novos) |
| Atraso | 21.79% | Baixa (clientes novos) |

---

## 7. Validações Implementadas (Gates)

### 7.1 Gates de Qualidade

Cada build da ABT executa validações automáticas que bloqueiam o pipeline em caso de falha:

| Gate | Validação | Critério | Ação se Falhar |
|------|-----------|----------|----------------|
| 1 | **Unicidade** | 1 registro por NUM_CPF + SAFRA | `AssertionError` |
| 2 | **Integridade de Chaves** | Sem NULLs em NUM_CPF, SAFRA | `AssertionError` |
| 3 | **Anti-Leakage** | FPD só observado quando FLAG_INSTALACAO=1 | `AssertionError` |
| 4 | **Cobertura Score** | Score_01 > 90% preenchido | `Warning` |
| 5 | **Distribuição Target** | FPD ∈ {0, 1} | `AssertionError` |
| 6 | **Distribuição Flag** | FLAG_INSTALACAO ∈ {0, 1} | `AssertionError` |
| 7 | **Cobertura Features** | Features M1 > 5% preenchidas | `Warning` |
| 8 | **Valores Não-Negativos** | Somas e contagens >= 0 | `Warning` |

### 7.2 Implementação

```python
def validate_abt_v6(df: DataFrame, count_expected: int) -> bool:
    """Validação para ABT v6."""

    # Gate 1: Unicidade
    count_distinct = df.select("num_cpf", "safra").distinct().count()
    assert count_distinct == df.count(), "Duplicatas encontradas!"

    # Gate 2: Integridade
    nulls_cpf = df.filter(F.col("num_cpf").isNull()).count()
    assert nulls_cpf == 0, "NULLs em NUM_CPF!"

    # Gate 3: Anti-leakage
    leak_check = df.filter(
        (F.col("flag_instalacao_int") == 0) &
        (F.col("fpd_int").isNotNull())
    ).count()
    assert leak_check == 0, "Vazamento de dados detectado!"

    # ... demais gates

    return True
```

---

## 8. Resultado Final

### 8.1 Características da ABT v6

| Característica | Valor |
|----------------|-------|
| **Tabela** | `hackathon_2025.default.gold_abt_v6_v2` |
| **Registros** | 3.795.310 |
| **Colunas Totais** | 614 |
| **Features Engineered** | ~250 |
| **Granularidade** | NUM_CPF + SAFRA (1:1) |
| **Target** | `fpd_int` (First Payment Default) |
| **Período** | Out/2024 a Mar/2025 |

### 8.2 Distribuição do Target

```
┌─────────────────────────────────────────────────────────────┐
│                 DISTRIBUIÇÃO DO TARGET                      │
│                                                             │
│   FLAG_INSTALACAO = 1 (Aprovados): 2.633.900 (69.40%)      │
│   FLAG_INSTALACAO = 0 (Reprovados): 1.161.410 (30.60%)     │
│                                                             │
│   Entre os aprovados (FLAG=1):                              │
│   ├── FPD = 0 (Bom pagador): 2.074.671 (78.77%)            │
│   └── FPD = 1 (Inadimplente): 559.229 (21.23%)             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 8.3 Performance do Modelo

| Métrica | Valor |
|---------|-------|
| **KS OOT** | 33.94% |
| **AUC** | 0.7327 |
| **Gini** | 46.54% |
| **Benchmark** | 33.10% |
| **Ganho vs Benchmark** | **+0.84 p.p.** |

### 8.4 Metadados Incluídos

Cada registro da ABT contém metadados para rastreabilidade:

```python
# Colunas de metadados
abt_version          # "v6.2"
build_date           # Timestamp do build
spine_version        # Versão do spine (bureau)
gold_version         # Versão das features gold
gold_build_date      # Timestamp do build gold
gold_feature_blocks  # "score_01,score_02,telco,cadastro,recarga_v2,pagamento_v2,atraso_v2"
```

---

## Referências

| Documento | Caminho |
|-----------|---------|
| Variable Book (ABT v6) | `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md` |
| Target Definition | `docs/00_project/target_definition.md` |
| ABT v6 Explained | `docs/08_team_preparation/technical/gold/09_ABT_V6_EXPLAINED.md` |
| Recarga Features | `docs/05_abt_v5_docs/GOLD_RECARGA_FEATURES_V2.md` |

---

*Documento criado em: Fevereiro 2026*
*Projeto: Hackathon PodAcademy 2025 - Modelo de Risco de Crédito*
