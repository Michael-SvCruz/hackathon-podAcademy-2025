# 08 - Gold Pagamento Features v2 Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/gold_pagamento_features_v2.py` |
| **Função** | Gerar features comportamentais de Pagamento |
| **Input** | Silver Pagamento (event-level, ~22M transações) |
| **Output** | Gold Pagamento Features (cliente-mês, ~12.6M registros) |
| **Compressão** | ~1.7x (transações → agregado) |
| **Colunas** | 49 features por safra mensal |
| **Grão** | NUM_CPF + SAFRA_PAGAMENTO (1:1) |

---

## Contexto de Negócio

Este script gera **features de comportamento de pagamento** que revelam:

1. **Capacidade de pagamento:** Valores pagos, ticket médio
2. **Comportamento de negociação:** Descontos obtidos
3. **Histórico de atraso:** Juros pagos = atraso em pagamentos anteriores
4. **Padrões de uso:** Formas de pagamento preferidas

### Por que Pagamento é Importante para Risco?

| Indicador | Feature | Significado |
|-----------|---------|-------------|
| **Juros pagos** | sum_val_juros_pos | Cliente já atrasou no passado |
| **% com juros** | pct_pagamentos_com_juros | Frequência de atrasos |
| **Descontos** | ratio_desconto_pago | Capacidade de negociação |
| **Multas** | sum_val_multa_* | Quebra de fidelidade/equipamento |

### Regra de Negócio: Juros = Indicador de Atraso

```
Se val_juros_pos > 0 → Cliente pagou com ATRASO
   (juros é cobrado quando pagamento ocorre após vencimento)

Portanto:
- flag_com_juros = 1 indica atraso passado
- pct_pagamentos_com_juros = proxy de histórico de inadimplência
```

---

## Arquitetura

```
SILVER PAGAMENTO (transacional)
     │ ~22M registros
     │ Grão: fatura + item + pagamento
     │
     ▼
┌─────────────────────────────────────────────────────┐
│        GOLD PAGAMENTO FEATURES (este script)        │
│                                                     │
│  1. Preparação (flags de validade, desconto, juros) │
│  2. Agregação por NUM_CPF + SAFRA_PAGAMENTO         │
│  3. Features derivadas (ratios, percentuais)        │
│  4. Flags de comportamento                          │
│                                                     │
└─────────────────────────────────────────────────────┘
     │
     │ ~12.6M registros (cliente-mês)
     │ 49 features
     ▼
GOLD PAGAMENTO FEATURES
     │
     │ LEFT JOIN com ABT v5
     │ + agregação M1/M3/M6
     ▼
ABT v6
```

---

## Código Explicado Linha por Linha

### 1. Preparação de Valores (Linhas 138-159)

```python
    # Garantir valores numéricos
    val_pago = F.coalesce(F.col("val_atual_pagamento"), F.lit(0.0))
    val_desconto = F.coalesce(F.col("val_desconto_item"), F.lit(0.0))
    val_juros_pos = F.coalesce(F.col("val_juros_pos"), F.lit(0.0))
    val_juros_neg = F.coalesce(F.col("val_juros_neg_abs"), F.lit(0.0))

    # Flag de pagamento válido (valor > 0)
    df = df.withColumn(
        "flag_pagamento_valido",
        F.when(val_pago > 0, 1).otherwise(0)
    )

    # Flag com desconto
    df = df.withColumn(
        "flag_com_desconto",
        F.when(val_desconto > 0, 1).otherwise(0)
    )

    # Flag com juros (indicador de atraso passado)
    df = df.withColumn(
        "flag_com_juros",
        F.when(val_juros_pos > 0, 1).otherwise(0)
    )
```

**Explicação das colunas de origem:**

| Coluna Silver | Significado | Uso |
|---------------|-------------|-----|
| `val_atual_pagamento` | Valor efetivamente pago | Feature principal |
| `val_desconto_item` | Desconto obtido no item | Negociação |
| `val_juros_pos` | Juros/multa por atraso | **Indicador de risco** |
| `val_juros_neg_abs` | Juros negativo (estorno) | Ajustes |

**Por que F.coalesce() com 0.0?**
```python
F.coalesce(F.col("val_atual_pagamento"), F.lit(0.0))
# Se val_atual_pagamento é NULL → retorna 0.0
# Se val_atual_pagamento tem valor → retorna o valor
```
Isso evita NULLs que propagariam nas agregações.

---

### 2. Agregação Mensal - Volume e Valores (Linhas 167-181)

```python
    df_gold = df.groupBy("num_cpf", "safra_pagamento", "dt_safra_pagamento").agg(

        # === VOLUME ===
        F.count("*").alias("qtd_transacoes_mes"),
        F.sum("flag_pagamento_valido").alias("qtd_pagamentos_validos_mes"),
        F.countDistinct("seq_fatura").alias("qtd_faturas_distintas_mes"),
        F.countDistinct("contrato").alias("qtd_contratos_distintos_mes"),

        # === VALORES PAGOS ===
        F.sum(val_pago).alias("sum_val_pago_mes"),
        F.avg(F.when(F.col("flag_pagamento_valido") == 1, val_pago)).alias("avg_val_pago_mes"),
        F.max(val_pago).alias("max_val_pago_mes"),
        F.min(F.when(val_pago > 0, val_pago)).alias("min_val_pago_mes"),
        F.stddev(F.when(F.col("flag_pagamento_valido") == 1, val_pago)).alias("std_val_pago_mes"),
        # ...
    )
```

**Features de volume:**

| Feature | Agregação | Significado |
|---------|-----------|-------------|
| qtd_transacoes_mes | COUNT(*) | Total de registros |
| qtd_pagamentos_validos_mes | SUM(flag) | Pagamentos com valor > 0 |
| qtd_faturas_distintas_mes | COUNT DISTINCT | Faturas diferentes pagas |
| qtd_contratos_distintos_mes | COUNT DISTINCT | Contratos ativos |

**Padrão de média condicional:**
```python
F.avg(F.when(F.col("flag_pagamento_valido") == 1, val_pago))
# Calcula média APENAS dos pagamentos válidos
# Ignora transações com valor = 0
```

---

### 3. Agregação de Descontos e Juros (Linhas 182-193)

```python
        # === DESCONTOS ===
        F.sum(val_desconto).alias("sum_val_desconto_mes"),
        F.sum("flag_com_desconto").alias("qtd_com_desconto_mes"),
        F.avg(F.when(F.col("flag_com_desconto") == 1, val_desconto)).alias("avg_val_desconto_mes"),
        F.max(val_desconto).alias("max_val_desconto_mes"),

        # === JUROS E MULTAS (indicador de atraso passado) ===
        F.sum(val_juros_pos).alias("sum_val_juros_pos_mes"),
        F.sum(val_juros_neg).alias("sum_val_juros_neg_mes"),
        F.sum("flag_com_juros").alias("qtd_com_juros_mes"),
        F.avg(F.when(F.col("flag_com_juros") == 1, val_juros_pos)).alias("avg_val_juros_mes"),
        F.max(val_juros_pos).alias("max_val_juros_mes"),
```

**Por que separar juros positivo e negativo?**
- `val_juros_pos`: Juros cobrados (cliente atrasou)
- `val_juros_neg`: Juros estornados (ajuste a favor do cliente)

**Importância para risco:**
- `sum_val_juros_pos` alto = cliente frequentemente atrasa
- `qtd_com_juros` / `qtd_transacoes` = frequência de atrasos

---

### 4. Agregação por Forma de Pagamento (Linhas 199-209)

```python
        # === FORMAS DE PAGAMENTO ===
        F.countDistinct(F.when(F.col("cod_forma_pagamento").isNotNull(), F.col("cod_forma_pagamento")))
            .alias("qtd_formas_pagamento_distintas_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "01", val_pago).otherwise(0)).alias("sum_pago_forma_01_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "02", val_pago).otherwise(0)).alias("sum_pago_forma_02_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "03", val_pago).otherwise(0)).alias("sum_pago_forma_03_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento").isNull(), val_pago).otherwise(0)).alias("sum_pago_forma_missing_mes"),

        # === MÉTODOS DE PAGAMENTO ===
        F.countDistinct(F.when(F.col("cod_metodo_pagamento").isNotNull(), F.col("cod_metodo_pagamento")))
            .alias("qtd_metodos_pagamento_distintos_mes"),
```

**Códigos de forma de pagamento (típicos):**

| Código | Forma | Significado |
|--------|-------|-------------|
| 01 | Boleto | Tradicional |
| 02 | Débito automático | Organizado |
| 03 | Cartão de crédito | Alavancado |
| NULL | Missing | Não informado |

**Por que capturar diversidade de formas?**
- Cliente com muitas formas = comportamento diversificado
- Cliente concentrado em uma forma = padrão previsível
- `pct_forma_dominante` indica concentração

---

### 5. Status de Pagamento (Linhas 211-218)

```python
        # === STATUS DE PAGAMENTO ===
        F.sum(F.when(F.col("ind_status_pagamento") == "P", 1).otherwise(0)).alias("qtd_status_p_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "R", 1).otherwise(0)).alias("qtd_status_r_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "C", 1).otherwise(0)).alias("qtd_status_c_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "B", 1).otherwise(0)).alias("qtd_status_b_mes"),
```

**Códigos de status:**

| Status | Significado | Risco |
|--------|-------------|-------|
| P | Pago | Neutro |
| R | Rejeitado | Alto (falha) |
| C | Cancelado | Médio |
| B | Baixado | Variável |

---

### 6. Features Derivadas - Percentuais e Ratios (Linhas 223-278)

```python
    # Ticket médio
    df_gold = df_gold.withColumn(
        "ticket_medio_pagamento_mes",
        F.when(
            F.col("qtd_pagamentos_validos_mes") > 0,
            F.round(F.col("sum_val_pago_mes") / F.col("qtd_pagamentos_validos_mes"), 2)
        ).otherwise(0.0)
    )

    # Percentual com desconto
    df_gold = df_gold.withColumn(
        "pct_pagamentos_com_desconto_mes",
        F.when(
            F.col("qtd_transacoes_mes") > 0,
            F.round((F.col("qtd_com_desconto_mes") / F.col("qtd_transacoes_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Ratio desconto/pago
    df_gold = df_gold.withColumn(
        "ratio_desconto_pago_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round(F.col("sum_val_desconto_mes") / F.col("sum_val_pago_mes"), 4)
        ).otherwise(0.0)
    )

    # Percentual com juros (indicador de atraso)
    df_gold = df_gold.withColumn(
        "pct_pagamentos_com_juros_mes",
        F.when(
            F.col("qtd_transacoes_mes") > 0,
            F.round((F.col("qtd_com_juros_mes") / F.col("qtd_transacoes_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Ratio juros/pago
    df_gold = df_gold.withColumn(
        "ratio_juros_pago_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round(F.col("sum_val_juros_pos_mes") / F.col("sum_val_pago_mes"), 4)
        ).otherwise(0.0)
    )
```

**Features derivadas importantes:**

| Feature | Fórmula | Interpretação |
|---------|---------|---------------|
| ticket_medio_pagamento | sum_pago / qtd_pagamentos | Valor médio por pagamento |
| pct_pagamentos_com_desconto | (qtd_desconto / qtd_total) × 100 | % com negociação |
| ratio_desconto_pago | sum_desconto / sum_pago | Intensidade do desconto |
| **pct_pagamentos_com_juros** | (qtd_juros / qtd_total) × 100 | **% com atraso** |
| **ratio_juros_pago** | sum_juros / sum_pago | **Intensidade do atraso** |

---

### 7. Concentração em Forma de Pagamento (Linhas 286-301)

```python
    # Concentração na forma de pagamento dominante
    df_gold = df_gold.withColumn(
        "max_forma_pagamento_mes",
        F.greatest(
            F.col("sum_pago_forma_01_mes"),
            F.col("sum_pago_forma_02_mes"),
            F.col("sum_pago_forma_03_mes"),
            F.col("sum_pago_forma_missing_mes")
        )
    ).withColumn(
        "pct_forma_dominante_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round((F.col("max_forma_pagamento_mes") / F.col("sum_val_pago_mes")) * 100, 2)
        ).otherwise(0.0)
    ).drop("max_forma_pagamento_mes")
```

**Explicação do F.greatest():**
```python
F.greatest(col1, col2, col3, col4)
# Retorna o MAIOR valor entre as 4 colunas

# Exemplo:
# sum_pago_forma_01 = 100
# sum_pago_forma_02 = 500  ← maior
# sum_pago_forma_03 = 200
# sum_pago_forma_missing = 0
# → max_forma_pagamento = 500
```

**Por que medir concentração?**
- `pct_forma_dominante = 100%` → usa apenas uma forma
- `pct_forma_dominante = 33%` → diversificado entre 3 formas
- Diversificação pode indicar comportamento mais planejado

---

### 8. Flags de Comportamento (Linhas 303-340)

```python
    # Sem pagamento no mês
    df_gold = df_gold.withColumn(
        "flag_sem_pagamento_mes",
        F.when(F.col("qtd_pagamentos_validos_mes") == 0, 1).otherwise(0)
    )

    # Sempre com juros (>80% dos pagamentos)
    df_gold = df_gold.withColumn(
        "flag_sempre_com_juros_mes",
        F.when(F.col("pct_pagamentos_com_juros_mes") > 80, 1).otherwise(0)
    )

    # Alto desconto (>10%)
    df_gold = df_gold.withColumn(
        "flag_alto_desconto_mes",
        F.when(F.col("ratio_desconto_pago_mes") > 0.10, 1).otherwise(0)
    )

    # Baixo volume de pagamentos (<2)
    df_gold = df_gold.withColumn(
        "flag_baixo_volume_pagamento_mes",
        F.when(F.col("qtd_pagamentos_validos_mes") < 2, 1).otherwise(0)
    )

    # Alta multa (multa > 5% do valor pago)
    df_gold = df_gold.withColumn(
        "flag_alta_multa_mes",
        F.when(
            (F.col("sum_val_pago_mes") > 0) &
            ((F.col("sum_val_multa_equip_mes") + F.col("sum_val_multa_fid_mes")) / F.col("sum_val_pago_mes") > 0.05),
            1
        ).otherwise(0)
    )
```

**Flags e seus significados:**

| Flag | Condição | Risco |
|------|----------|-------|
| flag_sem_pagamento | qtd_pagamentos = 0 | Alto |
| **flag_sempre_com_juros** | pct_juros > 80% | **Muito Alto** |
| flag_alto_desconto | ratio_desconto > 10% | Médio (negociação) |
| flag_baixo_volume | qtd_pagamentos < 2 | Médio |
| flag_alta_multa | multas > 5% do pago | Alto |

**Por que flag_sempre_com_juros é crítico?**
- Se >80% dos pagamentos têm juros, cliente é **cronicamente atrasado**
- Forte preditor de FPD (First Payment Default)

---

## Diagrama de Fluxo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    gold_pagamento_features_v2.py                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ LEITURA: Silver Pagamento (~22M transações)                          │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PREPARAÇÃO                                                           │   │
│  │                                                                      │   │
│  │ 1. Garantir safra_pagamento (derivar de ts_status_fatura)           │   │
│  │ 2. Criar dt_safra_pagamento para joins                              │   │
│  │ 3. F.coalesce() para valores numéricos                              │   │
│  │ 4. Criar flags: flag_pagamento_valido, flag_com_desconto,           │   │
│  │                 flag_com_juros                                       │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ AGREGAÇÃO (groupBy NUM_CPF + SAFRA_PAGAMENTO)                        │   │
│  │                                                                      │   │
│  │ • Volume: count, countDistinct                                      │   │
│  │ • Valores pagos: sum, avg, max, min, stddev                         │   │
│  │ • Descontos: sum, qtd, avg, max                                     │   │
│  │ • Juros/multas: sum, qtd, avg, max                                  │   │
│  │ • Formas de pagamento: countDistinct, sum por código                │   │
│  │ • Status: qtd por código (P, R, C, B)                               │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ FEATURES DERIVADAS                                                   │   │
│  │                                                                      │   │
│  │ • ticket_medio_pagamento = sum_pago / qtd_pagamentos                │   │
│  │ • pct_pagamentos_com_desconto = (qtd_desc / qtd_total) × 100        │   │
│  │ • ratio_desconto_pago = sum_desconto / sum_pago                     │   │
│  │ • pct_pagamentos_com_juros = (qtd_juros / qtd_total) × 100          │   │
│  │ • ratio_juros_pago = sum_juros / sum_pago                           │   │
│  │ • coef_variacao = std / avg                                         │   │
│  │ • pct_forma_dominante = max_forma / sum_pago × 100                  │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ FLAGS DE COMPORTAMENTO                                               │   │
│  │                                                                      │   │
│  │ • flag_sem_pagamento = 1 se qtd_pagamentos = 0                      │   │
│  │ • flag_sempre_com_juros = 1 se pct_juros > 80%                      │   │
│  │ • flag_alto_desconto = 1 se ratio_desconto > 10%                    │   │
│  │ • flag_baixo_volume = 1 se qtd_pagamentos < 2                       │   │
│  │ • flag_alta_multa = 1 se multas > 5% do pago                        │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ ESCRITA: Gold Pagamento Features (~12.6M cliente-mês)                │   │
│  │ Particionado por safra_pagamento                                     │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Features de Saída (49 colunas)

### Chaves e Metadados

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| num_cpf | string | Identificador do cliente |
| safra_pagamento | string | Mês do pagamento (YYYYMM) |
| dt_safra_pagamento | date | Primeiro dia do mês |
| gold_version | string | Versão do script |
| gold_build_date | timestamp | Data de execução |

### Features de Volume

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| qtd_transacoes_mes | long | Total de transações |
| qtd_pagamentos_validos_mes | long | Pagamentos com valor > 0 |
| qtd_faturas_distintas_mes | long | Faturas distintas |
| qtd_contratos_distintos_mes | long | Contratos distintos |

### Features de Valor

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| sum_val_pago_mes | double | Soma total pago |
| avg_val_pago_mes | double | Média por pagamento |
| max_val_pago_mes | double | Maior pagamento |
| min_val_pago_mes | double | Menor pagamento |
| std_val_pago_mes | double | Desvio padrão |
| ticket_medio_pagamento_mes | double | sum / qtd |

### Features de Desconto

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| sum_val_desconto_mes | double | Total de descontos |
| qtd_com_desconto_mes | long | Quantidade com desconto |
| pct_pagamentos_com_desconto_mes | double | % com desconto |
| ratio_desconto_pago_mes | double | desconto / pago |

### Features de Juros (Indicador de Atraso)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| **sum_val_juros_pos_mes** | double | **Total de juros pagos** |
| **qtd_com_juros_mes** | long | **Quantidade com juros** |
| **pct_pagamentos_com_juros_mes** | double | **% com juros** |
| **ratio_juros_pago_mes** | double | **juros / pago** |

### Features de Forma de Pagamento

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| qtd_formas_pagamento_distintas_mes | long | Diversidade |
| sum_pago_forma_01_mes | double | Valor pago via forma 01 |
| sum_pago_forma_02_mes | double | Valor pago via forma 02 |
| sum_pago_forma_03_mes | double | Valor pago via forma 03 |
| pct_forma_dominante_mes | double | Concentração |

### Flags de Comportamento

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| flag_sem_pagamento_mes | int | Sem pagamento |
| **flag_sempre_com_juros_mes** | int | **>80% com juros** |
| flag_alto_desconto_mes | int | Desconto > 10% |
| flag_baixo_volume_pagamento_mes | int | < 2 pagamentos |
| flag_alta_multa_mes | int | Multas > 5% |

---

## Comparação com Recarga Features

| Aspecto | Recarga | Pagamento |
|---------|---------|-----------|
| **Grão origem** | Evento de recarga | Transação de pagamento |
| **Volume típico** | 95M eventos | 22M transações |
| **Compressão** | 2.9x | 1.7x |
| **Indicador principal de risco** | SOS (empréstimo) | **Juros (atraso)** |
| **Comportamento capturado** | Stress financeiro | Histórico de pagamento |

---

## Lições Aprendidas

### 1. Juros = Proxy de Atraso Histórico

**Insight:** `val_juros_pos > 0` indica que o cliente pagou COM ATRASO.
**Uso:** Feature forte para prever FPD (já atrasou antes = pode atrasar de novo).

### 2. Descontos Podem Indicar Negociação

**Insight:** Clientes que negociam descontos podem ter perfil diferente.
**Uso:** `flag_alto_desconto` pode segmentar clientes em dificuldade.

### 3. Concentração em Forma de Pagamento

**Insight:** Diversidade de formas pode indicar organização financeira.
**Uso:** `pct_forma_dominante` como proxy de comportamento.

---

## Exemplo de Saída

```
╔==============================================================================╗
║                      GOLD PAGAMENTO FEATURES V2                              ║
╚==============================================================================╝

>>> [Leitura] Carregando Silver Pagamento
>>> [Info] Registros Silver: 21,821,465

================================================================================
GOLD PAGAMENTO FEATURES - PIPELINE PRINCIPAL
================================================================================

>>> [Prep] Preparando dados de Pagamento...
>>> [Agg] Agregando por NUM_CPF + SAFRA_PAGAMENTO...
>>> [Deriv] Criando features derivadas...
>>> [Flags] Criando flags de comportamento...
>>> [Info] Features geradas: 49 colunas

>>> [Info] Registros Gold: 12,634,799

================================================================================
PAGAMENTO FEATURES V2 CONCLUÍDO!
  Silver: 21,821,465 → Gold: 12,634,799
  Compressão: 1.7x
================================================================================
```

---

## Checklist de Revisão

- [x] F.coalesce() para tratar NULLs em valores
- [x] Flags de validade, desconto, juros criados
- [x] Agregação por NUM_CPF + SAFRA_PAGAMENTO
- [x] Features de juros como indicador de atraso
- [x] Concentração em forma de pagamento (F.greatest)
- [x] Flags de comportamento (sempre_com_juros, alto_desconto)
- [x] Particionamento por safra_pagamento
- [x] Metadados gold_version e gold_build_date

---

## Próximo Passo

As features de Pagamento serão consumidas pelo ABT v6 junto com Atraso:

```
Gold Pagamento Features (cliente-mês)
        │
        │ + Gold Atraso Features
        │
        │ LEFT JOIN com ABT v5
        │ + agregação M1/M3/M6
        ▼
ABT v6 (614 colunas - FINAL)
```

Ver [09_ATRASO_FEATURES_EXPLAINED.md](09_ATRASO_FEATURES_EXPLAINED.md).
