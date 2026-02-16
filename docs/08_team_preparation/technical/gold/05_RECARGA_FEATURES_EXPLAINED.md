# 07 - Gold Recarga Features v2 Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/gold_recarga_features_v2.py` |
| **Função** | Gerar features comportamentais de Recarga para modelagem |
| **Input** | Silver Recarga (event-level, ~95M eventos) |
| **Output** | Gold Recarga Features (cliente-mês, ~33M registros) |
| **Compressão** | ~2.9x (eventos → agregado) |
| **Colunas** | 51 features por safra mensal |
| **Grão** | NUM_CPF + SAFRA_RECARGA (1:1) |

---

## Contexto de Negócio

Este script é o **gerador de features de Recarga** que alimenta a ABT v5+. É um dos scripts mais importantes do pipeline porque:

1. **Transforma eventos em features:** 95M eventos → 33M registros cliente-mês
2. **Implementa regras de negócio críticas:** Ajuste de SOS e bônus conforme Fernando (Claro)
3. **Cria indicadores de risco:** SOS é forte indicador de estresse financeiro
4. **Prepara dados para janelas temporais:** M1, M3, M6 serão aplicadas no ABT builder

### Arquitetura do Pipeline

```
SILVER RECARGA (event-level)
     │ ~95M registros
     │ Grão: 1 linha por evento de recarga
     │
     ▼
┌─────────────────────────────────────────────────────┐
│         GOLD RECARGA FEATURES (este script)         │
│                                                     │
│  1. Preparação (tipos, classificação, ajustes)      │
│  2. Cálculo de métricas temporais                   │
│  3. Agregação por NUM_CPF + SAFRA_RECARGA           │
│  4. Features derivadas (ratios, percentuais)        │
│  5. Flags de cobertura                              │
│                                                     │
└─────────────────────────────────────────────────────┘
     │
     │ ~33M registros (cliente-mês)
     │ 51 features
     ▼
GOLD RECARGA FEATURES
     │
     │ LEFT JOIN por (NUM_CPF, SAFRA)
     │ com filtro temporal (SAFRA_RECARGA < SAFRA)
     ▼
ABT v5+ (abt_v5_delta, abt_v6_delta)
```

---

## Regras de Negócio (Reunião Fernando/Claro - 07/01/2026)

### 1. SOS - Serviço de Empréstimo de Crédito

| Aspecto | Descrição |
|---------|-----------|
| **O que é** | Adiantamento/empréstimo de R$3-20 (geralmente R$5) |
| **Como funciona** | Descontado da próxima recarga tradicional |
| **Regra crucial** | SOS e bônus NÃO contam como "dinheiro real" |
| **Exemplo** | Recarga R$20 + SOS R$5 = R$20 real, não R$25 |
| **Significado** | Alta frequência de SOS = **estresse financeiro** |

### 2. Valores Negativos

- Indicam ajustes, estornos ou sentinelas
- Tratados com colunas `*_CLEAN` (NULL se negativo)
- Flags indicam presença de negativos

### 3. Sentinelas em Códigos Dimensionais

| Valor | Significado |
|-------|-------------|
| -1 | "Não se aplica" |
| -2 | "Não determinado" |
| -3 | "Não informado" |

### 4. Anti-Leakage

- `SAFRA_RECARGA` deve ser **ANTERIOR** à `SAFRA` do cliente no spine
- Janelas temporais (M1, M3, M6) garantem lookback correto
- `FPD` e `FLAG_INSTALACAO` NÃO são features

---

## Código Explicado Linha por Linha

### 1. Configurações e Constantes (Linhas 149-184)

```python
# Configuração das janelas temporais (meses de lookback)
TEMPORAL_WINDOWS = {
    "m1": 1,   # Último mês
    "m3": 3,   # Últimos 3 meses
    "m6": 6    # Últimos 6 meses
}

# Limiar para "baixa atividade" (recargas por período)
LIMIAR_BAIXA_ATIVIDADE = {
    "m1": 2,   # Menos de 2 recargas/mês = baixa atividade
    "m3": 5,   # Menos de 5 recargas/trimestre
    "m6": 10   # Menos de 10 recargas/semestre
}
```

**Por que dicionários para configuração?**
- Centraliza parâmetros de negócio
- Fácil ajustar sem modificar código
- Documenta regras implicitamente

**Por que esses limiares de baixa atividade?**
- Baseados em conhecimento do negócio
- Cliente com <2 recargas/mês é atípico para pré-pago
- Permite identificar clientes inativos ou com comportamento irregular

---

### 2. Função preparar_recarga_para_agregacao (Linhas 191-379)

Esta função prepara os dados event-level para agregação.

#### 2.1 Criação de dt_recarga_safra (Linhas 215-219)

```python
    # SAFRA_RECARGA (YYYYMM) → DT_RECARGA_SAFRA (primeiro dia do mês)
    df = df.withColumn(
        "dt_recarga_safra",
        F.to_date(F.concat(F.col("safra_recarga"), F.lit("01")), "yyyyMMdd")
    )
```

**Por que criar dt_recarga_safra?**
- `safra_recarga` é string "YYYYMM" (ex: "202401")
- `dt_recarga_safra` é date (ex: 2024-01-01)
- Date permite cálculos temporais (datediff, months_between)

**Lógica:**
```
"202401" + "01" = "20240101" → to_date → 2024-01-01
```

---

#### 2.2 Classificação de Tipo de Transação (Linhas 233-258)

```python
    # Usar colunas clean se existirem, senão usar originais
    val_cred = F.coalesce(F.col("val_credito_inserido_clean"), F.col("val_credito_inserido"), F.lit(0.0))
    val_bonus = F.coalesce(F.col("val_bonus_clean"), F.col("val_bonus"), F.lit(0.0))
    val_real = F.coalesce(F.col("val_real_clean"), F.col("val_real"), F.lit(0.0))
    valor_sos = F.coalesce(F.col("valor_sos"), F.lit(0.0))
    flag_sos = F.coalesce(F.col("flag_sos"), F.lit(0))

    df = df.withColumn(
        "tipo_transacao",
        F.when((val_cred > 0) & (val_bonus == 0), "PAGO_PURO")
         .when((val_cred == 0) & (val_bonus > 0), "BONUS_PURO")
         .when((val_cred > 0) & (val_bonus > 0), "COMBO_PAGO_BONUS")
         .when((val_cred == 0) & (val_bonus == 0) & (val_real == 0), "ZERO_TOTAL")
         .when(val_real < 0, "VALOR_NEGATIVO")
         .otherwise("OUTROS")
    )
```

**Por que F.coalesce() para valores?**
```python
F.coalesce(col_clean, col_original, default)
# Retorna o primeiro valor não-nulo:
# 1. Se col_clean existe e não é NULL → usa col_clean
# 2. Senão, se col_original não é NULL → usa col_original
# 3. Senão → usa default (0.0)
```

**Tipos de Transação:**

| Tipo | Condição | Significado |
|------|----------|-------------|
| PAGO_PURO | crédito>0, bônus=0 | Recarga 100% paga |
| BONUS_PURO | crédito=0, bônus>0 | Apenas bônus promocional |
| COMBO_PAGO_BONUS | crédito>0, bônus>0 | Recarga com bônus adicional |
| ZERO_TOTAL | tudo=0 | Evento sem valor (problema) |
| VALOR_NEGATIVO | val_real<0 | Estorno ou ajuste |
| OUTROS | default | Casos não classificados |

---

#### 2.3 Ajuste de SOS - Lógica Central (Linhas 262-303)

```python
    # Conforme explicação de Fernando (Claro):
    # - SOS é empréstimo que será descontado da próxima recarga
    # - Bônus não é dinheiro real
    # - VAL_REAL_AJUSTADO = crédito real desconsiderando SOS pendente e bônus

    # Etapa 1: Ajustar por SOS
    df = df.withColumn(
        "val_real_ajustado_sos",
        F.when(
            (flag_sos == 1) & (valor_sos == val_cred),
            -valor_sos  # Toda a recarga foi SOS, valor real negativo
        )
        .when(
            (flag_sos == 1) & (valor_sos != val_cred),
            val_cred - valor_sos  # Desconta o SOS do crédito
        )
        .otherwise(val_real)  # Sem SOS, mantém valor original
    )

    # Etapa 2: Ajustar por bônus (bônus não é dinheiro real)
    df = df.withColumn(
        "val_real_ajustado_final",
        F.when(
            F.col("tipo_transacao").isin(["COMBO_PAGO_BONUS", "BONUS_PURO"]),
            F.col("val_real_ajustado_sos") - val_bonus
        ).otherwise(
            F.col("val_real_ajustado_sos")
        )
    )
```

**Explicação do ajuste de SOS:**

| Cenário | Condição | Resultado |
|---------|----------|-----------|
| Toda recarga é SOS | flag_sos=1 AND valor_sos=val_cred | val_real = -valor_sos |
| SOS parcial | flag_sos=1 AND valor_sos≠val_cred | val_real = val_cred - valor_sos |
| Sem SOS | flag_sos=0 | val_real = val_real (original) |

**Exemplo prático:**
```
Recarga: val_credito=20, valor_sos=5, flag_sos=1
→ valor_sos (5) ≠ val_cred (20)
→ val_real_ajustado = 20 - 5 = R$15 (dinheiro real)
```

**Por que valor negativo quando toda recarga é SOS?**
- Se cliente fez recarga apenas para pagar SOS pendente
- O "dinheiro real" é negativo (ele devolveu, não adicionou)
- Isso é um indicador forte de estresse financeiro

---

#### 2.4 Cálculo de Tempo Entre Recargas (Linhas 305-334)

```python
    # Window para calcular tempo entre recargas (por CPF, ordenado por timestamp)
    window_tempo = Window.partitionBy("num_cpf").orderBy("ts_recarga")

    # Timestamp da recarga anterior
    df = df.withColumn(
        "ts_recarga_anterior",
        F.lag("ts_recarga", 1).over(window_tempo)
    )

    # Dias desde a recarga anterior
    df = df.withColumn(
        "dias_desde_recarga_anterior",
        F.when(
            F.col("ts_recarga_anterior").isNotNull() & F.col("ts_recarga").isNotNull(),
            F.datediff(F.col("ts_recarga"), F.col("ts_recarga_anterior"))
        ).otherwise(None)
    )
```

**Explicação do Window + lag():**

```
Window.partitionBy("num_cpf").orderBy("ts_recarga")
│
├── Particiona por CPF (cada cliente é um grupo)
└── Ordena por timestamp dentro do grupo

F.lag("ts_recarga", 1).over(window)
│
└── Para cada linha, pega o ts_recarga da linha ANTERIOR no grupo
```

**Diagrama:**
```
CPF: AAA, ordenado por ts_recarga
┌────────────┬─────────────────┬─────────────────────┬───────────────────┐
│ ts_recarga │ ts_anterior     │ dias_desde_anterior │ Cálculo           │
├────────────┼─────────────────┼─────────────────────┼───────────────────┤
│ 2024-01-05 │ NULL            │ NULL                │ Primeira recarga  │
│ 2024-01-12 │ 2024-01-05      │ 7                   │ 12-05 = 7 dias    │
│ 2024-01-20 │ 2024-01-12      │ 8                   │ 20-12 = 8 dias    │
│ 2024-02-01 │ 2024-01-20      │ 12                  │ 01-20 = 12 dias   │
└────────────┴─────────────────┴─────────────────────┴───────────────────┘
```

---

#### 2.5 Features de Horário e Período (Linhas 338-375)

```python
    # Extrair hora do dia
    df = df.withColumn(
        "hora_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.hour("ts_recarga"))
         .otherwise(None)
    )

    # Classificar período do dia
    df = df.withColumn(
        "periodo_dia",
        F.when(F.col("hora_recarga").between(6, 11), "MANHA")
         .when(F.col("hora_recarga").between(12, 17), "TARDE")
         .when(F.col("hora_recarga").between(18, 23), "NOITE")
         .otherwise("MADRUGADA")
    )

    # Flag fim de semana
    df = df.withColumn(
        "flag_fim_semana",
        F.when(F.col("dia_semana").isin(1, 7), 1).otherwise(0)
    )
```

**Por que capturar período do dia?**
- Recargas na madrugada podem indicar comportamento atípico
- Padrão de fim de semana vs dia de semana
- Features comportamentais para o modelo

**Classificação de períodos:**

| Período | Horas | Significado |
|---------|-------|-------------|
| MANHA | 06-11 | Início do dia |
| TARDE | 12-17 | Horário comercial |
| NOITE | 18-23 | Pós-trabalho |
| MADRUGADA | 00-05 | Atípico |

**Por que dia_semana.isin(1, 7)?**
- `F.dayofweek()` retorna 1=Domingo, 7=Sábado
- Fim de semana = Domingo (1) ou Sábado (7)

---

### 3. Função criar_features_recarga_completas (Linhas 608-838)

Esta função executa a agregação principal.

#### 3.1 Agregação por NUM_CPF + SAFRA_RECARGA (Linhas 642-707)

```python
    df_gold = df_prep.groupBy("num_cpf", "safra_recarga", "dt_recarga_safra").agg(

        # === VOLUME ===
        F.count("*").alias("qtd_recargas_mes"),
        F.sum("flag_recarga_valida").alias("qtd_recargas_validas_mes"),
        F.countDistinct("dw_num_ntc").alias("qtd_telefones_distintos_mes"),

        # === VALORES BRUTOS ===
        F.sum(F.coalesce(F.col("val_credito_inserido_clean"), F.col("val_credito_inserido"), F.lit(0.0)))
            .alias("sum_val_credito_mes"),
        # ... mais agregações ...

        # === SOS ===
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("qtd_sos_mes"),
        F.sum(F.when(F.col("flag_sos") == 1, F.col("valor_sos")).otherwise(0)).alias("sum_valor_sos_mes"),
        F.max(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("flag_teve_sos_mes"),

        # === TEMPO ENTRE RECARGAS ===
        F.avg("dias_desde_recarga_anterior").alias("dias_medio_entre_recargas_mes"),
        F.min(F.when(F.col("dias_desde_recarga_anterior") > 0, F.col("dias_desde_recarga_anterior")))
            .alias("dias_min_entre_recargas_mes"),
        F.max("dias_desde_recarga_anterior").alias("dias_max_entre_recargas_mes"),
        # ...
    )
```

**Categorias de agregações:**

| Categoria | Exemplos | Propósito |
|-----------|----------|-----------|
| **Volume** | qtd_recargas, qtd_telefones | Frequência de uso |
| **Valores** | sum_val_credito, avg_val_real | Capacidade financeira |
| **SOS** | qtd_sos, sum_valor_sos | **Estresse financeiro** |
| **Temporal** | dias_medio_entre, dias_max | Regularidade |
| **Horário** | qtd_recargas_madrugada | Comportamento atípico |
| **Transação** | qtd_pago_puro, qtd_bonus | Tipo de uso |

**Padrão de agregação condicional:**
```python
# Soma condicional: apenas quando flag_sos = 1
F.sum(F.when(F.col("flag_sos") == 1, F.col("valor_sos")).otherwise(0))

# Flag binária: 1 se teve pelo menos um evento SOS
F.max(F.when(F.col("flag_sos") == 1, 1).otherwise(0))
```

---

#### 3.2 Features Derivadas (Linhas 714-814)

```python
    # Ticket médio
    df_gold = df_gold.withColumn(
        "ticket_medio_mes",
        F.when(
            F.col("qtd_recargas_validas_mes") > 0,
            F.round(F.col("sum_val_real_ajustado_mes") / F.col("qtd_recargas_validas_mes"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre crédito (indicador de estresse)
    df_gold = df_gold.withColumn(
        "pct_sos_sobre_credito_mes",
        F.when(
            F.col("sum_val_credito_mes") > 0,
            F.round((F.col("sum_valor_sos_mes") / F.col("sum_val_credito_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequência de SOS
    df_gold = df_gold.withColumn(
        "freq_sos_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round(F.col("qtd_sos_mes") / F.col("qtd_recargas_mes"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variação (estabilidade financeira)
    df_gold = df_gold.withColumn(
        "coef_variacao_val_mes",
        F.when(
            (F.col("avg_val_real_mes").isNotNull()) & (F.col("avg_val_real_mes") > 0),
            F.round(F.col("std_val_real_mes") / F.col("avg_val_real_mes"), 4)
        ).otherwise(None)
    )
```

**Features derivadas importantes:**

| Feature | Fórmula | Significado |
|---------|---------|-------------|
| `ticket_medio` | sum_val / qtd_recargas | Valor médio por recarga |
| `pct_sos_sobre_credito` | (sum_sos / sum_credito) × 100 | % do crédito que foi SOS |
| `freq_sos` | qtd_sos / qtd_recargas | Frequência de uso de SOS |
| `coef_variacao_val` | std / avg | Instabilidade de valores |
| `ratio_max_min_val` | max / min | Amplitude de valores |
| `recargas_por_semana` | qtd_recargas / 4.33 | Frequência semanal |

**Por que 4.33 semanas por mês?**
```
52 semanas/ano ÷ 12 meses = 4.33 semanas/mês
```

---

### 4. Escrita com Particionamento (Linhas 996-1002)

```python
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("safra_recarga") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(args.output_path)
```

**Por que partitionBy("safra_recarga")?**
- Recarga tem muitos registros (~33M)
- Particionamento por safra permite leitura eficiente por período
- Queries filtradas por safra leem apenas partições relevantes

**Estrutura de diretórios criada:**
```
recarga_features_v2_delta/
├── safra_recarga=202301/
│   └── part-00000-*.parquet
├── safra_recarga=202302/
│   └── part-00000-*.parquet
├── safra_recarga=202303/
│   └── part-00000-*.parquet
└── ...
```

---

## Diagrama de Fluxo Completo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    gold_recarga_features_v2.py                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ LEITURA: Silver Recarga (~95M eventos)                               │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PREPARAÇÃO (preparar_recarga_para_agregacao)                         │   │
│  │                                                                      │   │
│  │ 1. Criar dt_recarga_safra (YYYYMM → date)                           │   │
│  │ 2. Classificar tipo_transacao (PAGO, BONUS, COMBO, etc)             │   │
│  │ 3. Ajustar valores por SOS (regra Fernando/Claro)                   │   │
│  │ 4. Ajustar valores por bônus (não é dinheiro real)                  │   │
│  │ 5. Calcular dias_desde_recarga_anterior (Window + lag)              │   │
│  │ 6. Extrair hora_recarga, periodo_dia, flag_fim_semana               │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ AGREGAÇÃO (groupBy NUM_CPF + SAFRA_RECARGA)                          │   │
│  │                                                                      │   │
│  │ • Volume: count, countDistinct                                      │   │
│  │ • Valores: sum, avg, min, max, stddev                               │   │
│  │ • SOS: sum condicional, flag_teve_sos                               │   │
│  │ • Temporal: avg/min/max dias entre recargas                         │   │
│  │ • Horário: qtd por período, pct madrugada/fim de semana            │   │
│  │ • Transação: qtd por tipo                                           │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ FEATURES DERIVADAS                                                   │   │
│  │                                                                      │   │
│  │ • ticket_medio = sum_val / qtd_recargas                             │   │
│  │ • pct_sos_sobre_credito = (sum_sos / sum_credito) × 100             │   │
│  │ • freq_sos = qtd_sos / qtd_recargas                                 │   │
│  │ • coef_variacao_val = std / avg                                     │   │
│  │ • ratio_max_min_val = max / min                                     │   │
│  │ • recargas_por_semana = qtd / 4.33                                  │   │
│  │ • pct_semanas_com_recarga = (semanas_ativas / 4.33) × 100           │   │
│  │ • val_liquido = sum_credito - sum_sos                               │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ FLAGS DE COBERTURA                                                   │   │
│  │                                                                      │   │
│  │ • flag_sem_recarga_mes = 1 se qtd_recargas = 0                      │   │
│  │ • flag_baixa_atividade_mes = 1 se qtd_recargas < 2                  │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ ESCRITA: Gold Recarga Features (~33M cliente-mês)                    │   │
│  │ Particionado por safra_recarga                                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Features de Saída (51 colunas)

### Chaves e Metadados

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| num_cpf | string | Identificador do cliente |
| safra_recarga | string | Mês da recarga (YYYYMM) |
| dt_recarga_safra | date | Primeiro dia do mês |
| gold_version | string | Versão do script |
| gold_build_date | timestamp | Data de execução |

### Features de Volume

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| qtd_recargas_mes | long | Total de eventos |
| qtd_recargas_validas_mes | long | Eventos com valor > 0 |
| qtd_telefones_distintos_mes | long | Linhas distintas |

### Features de Valor

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| sum_val_credito_mes | double | Soma crédito inserido |
| sum_val_bonus_mes | double | Soma bônus |
| sum_val_real_mes | double | Soma valor real (original) |
| sum_val_real_ajustado_mes | double | **Soma após ajuste SOS/bônus** |
| avg_val_real_mes | double | Média |
| min_val_real_mes | double | Mínimo |
| max_val_real_mes | double | Máximo |
| std_val_real_mes | double | Desvio padrão |
| ticket_medio_mes | double | Valor médio por recarga |

### Features de SOS (Estresse Financeiro)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| qtd_sos_mes | long | Quantidade de SOS |
| sum_valor_sos_mes | double | Soma valores SOS |
| flag_teve_sos_mes | int | 1 se usou SOS |
| pct_sos_sobre_credito_mes | double | % SOS / crédito |
| freq_sos_mes | double | qtd_sos / qtd_recargas |
| val_liquido_mes | double | crédito - SOS |

### Features Temporais

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| dias_medio_entre_recargas_mes | double | Média de dias entre recargas |
| dias_min_entre_recargas_mes | double | Menor intervalo |
| dias_max_entre_recargas_mes | double | Maior intervalo (inatividade) |
| std_dias_entre_recargas_mes | double | Variabilidade do padrão |
| dt_ultima_recarga_mes | date | Data da última recarga |
| dt_primeira_recarga_mes | date | Data da primeira recarga |

### Features de Padrão Comportamental

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| qtd_recargas_madrugada_mes | long | Recargas 00-05h |
| pct_recargas_madrugada_mes | double | % na madrugada |
| qtd_recargas_fim_semana_mes | long | Recargas sáb/dom |
| pct_recargas_fim_semana_mes | double | % no fim de semana |
| qtd_semanas_com_recarga_mes | long | Semanas ativas |
| pct_semanas_com_recarga_mes | double | % de semanas ativas |
| recargas_por_semana_mes | double | Frequência semanal |

### Features de Estabilidade

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| coef_variacao_val_mes | double | CV = std/avg (instabilidade) |
| ratio_max_min_val_mes | double | Amplitude de valores |
| ratio_bonus_credito_mes | double | % bônus / crédito |

### Flags de Cobertura

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| flag_sem_recarga_mes | int | 1 se qtd_recargas = 0 |
| flag_baixa_atividade_mes | int | 1 se qtd_recargas < 2 |

---

## Lições Aprendidas

### 1. Ajuste de SOS é Crítico

**Regra de negócio fundamental:** SOS não é dinheiro real, deve ser descontado.
**Impacto:** Sem ajuste, modelo superestimaria capacidade financeira do cliente.

### 2. Window Functions para Métricas Temporais

**Padrão:** `Window.partitionBy().orderBy()` + `F.lag()`
**Uso:** Calcular tempo entre eventos sequenciais do mesmo cliente.

### 3. Agregação Condicional

**Padrão:** `F.sum(F.when(condição, valor).otherwise(0))`
**Uso:** Somar apenas eventos que atendem critério (ex: flag_sos = 1).

### 4. Particionamento para Grandes Volumes

**Padrão:** `.partitionBy("safra_recarga")`
**Benefício:** Leitura eficiente por período temporal.

---

## Exemplo de Saída do Relatório

```
╔==============================================================================╗
║                         GOLD RECARGA FEATURES V2                             ║
║      Features Comportamentais para Modelagem de Risco de Crédito            ║
╚==============================================================================╝

>>> [Leitura] Carregando Silver Recarga: /Volumes/.../silver/recarga_silver_delta/
>>> [Info] Registros na Silver: 95,210,519
>>> [Info] Colunas disponíveis: 28

================================================================================
GOLD RECARGA FEATURES - PIPELINE PRINCIPAL
================================================================================

>>> [Prep] Preparando Recarga para agregação Gold...
>>> [Prep] Criando classificação de tipo de transação...
>>> [Prep] Aplicando ajuste de SOS e bônus...
>>> [Prep] Calculando métricas de tempo entre recargas...
>>> [Prep] Extraindo features de horário e período...
>>> [Prep] Preparação concluída. Colunas: 42

>>> [Agg] Agregando por NUM_CPF + SAFRA_RECARGA (mensal)...
>>> [Agg] Criando features derivadas mensais...
>>> [Info] Registros no Gold: 32,882,218
>>> [Info] Colunas geradas: 51

================================================================================
RELATÓRIO DE QUALIDADE - GOLD RECARGA FEATURES
================================================================================

>>> [Stats] Volumetria:
    Silver (eventos):      95,210,519
    Gold (cliente-mês):    32,882,218
    Compressão: 2.9x

>>> [Stats] Features principais:
    QTD_RECARGAS_MES (média): 2.89
    SUM_VAL_REAL_AJUSTADO_MES (média): R$ 45.23
    TICKET_MEDIO_MES (média): R$ 18.50
    Registros com SOS: 5,234,567 (15.92%)
    DIAS_MEDIO_ENTRE_RECARGAS (média): 10.5 dias

>>> [Schema] Total de colunas: 51
================================================================================
```

---

## Checklist de Revisão

- [x] F.coalesce() para tratar NULLs em valores
- [x] Ajuste de SOS implementado conforme regra Fernando
- [x] Ajuste de bônus (não é dinheiro real)
- [x] Window + lag() para tempo entre recargas
- [x] Classificação de período do dia
- [x] Agregação por NUM_CPF + SAFRA_RECARGA
- [x] Features derivadas (ratios, percentuais)
- [x] Flags de cobertura
- [x] Particionamento por safra_recarga
- [x] Relatório de qualidade

---

## Próximo Passo

As features geradas por este script são consumidas pelo ABT v5 builder:

```
Gold Recarga Features (cliente-mês)
        │
        │ LEFT JOIN por (NUM_CPF, SAFRA)
        │ com filtro: SAFRA_RECARGA < SAFRA
        │ + agregação M1/M3/M6
        ▼
ABT v4 + Recarga M1/M3/M6 → ABT v5
```

Ver [05_ABT_V5_EXPLAINED.md](05_ABT_V5_EXPLAINED.md).
