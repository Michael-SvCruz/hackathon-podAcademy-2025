# ABT v6 — Feature Enhancement Analysis
## Avaliação de Variáveis Adicionais para Melhoria de Performance

**Data:** 25 de janeiro de 2026  
**Status:** 🔍 Análise Recomendatória  
**Objetivo:** Identificar oportunidades de novos sinais discriminantes em Pagamento e Atraso

---

## 📊 Sumário Executivo

### Status Atual v6
- **Total features:** 72 (Pagamento: 36 + Atraso: 36)
- **Estrutura:** Muito boa — cobertura bem distribuída
- **KS esperado:** 44-45% (baseline v4: 40.2%, delta: +3-5pp)

### Oportunidades Identificadas: ✅ SIM
- 📈 **Alto potencial:** 12 novos sinais em Pagamento/Atraso
- 🎯 **Aumento esperado:** +0.5-1.5pp adicional em KS (45-46%)
- ⚠️ **Effort:** Médio (20 linhas Spark por feature)
- 💡 **Prioridade:** 3 features críticas (High Impact/Low Effort)

---

## 1️⃣ Análise das Features Atuais

### Pagamento (36 features)

#### ✅ Bem coberto:
| Aspecto | Features | Coverage | Status |
|---------|----------|----------|--------|
| **Quantidade transações** | 3 (QTD M1/M3/M6) | Todos | ✅ |
| **Valores pagos** | 6 (SUM, AVG, MAX) | M1/M3/M6 | ✅ |
| **Juros/Descontos** | 6 (SUM juros_pos, juros_neg, desconto) | M1/M3/M6 | ✅ |
| **Flags indicadores** | 12 (TEVE_DESCONTO, MISSING_TS) | M1/M3/M6 | ✅ |

#### ❌ Gaps Identificados:

**Gap 1: Ratio de Valores (Eficiência de Pagamento)**
- ✅ Temos: SUM_VAL_PAGO, SUM_VAL_DESCONTO (absolutos)
- ❌ Falta: Proporção de descontos vs pagamento realizado
  - **Intuição:** Cliente que recebe muitos descontos pode estar negociando (comportamento risco)
  - **Sinal:** `desconto_rate = sum_val_desconto / (sum_val_pago + sum_val_desconto)`

**Gap 2: Volatilidade de Pagamentos (Consistency)**
- ✅ Temos: AVG, MAX de valores pagos
- ❌ Falta: Variância ou coeficiente de variação
  - **Intuição:** Cliente com pagamentos muito irregulares pode ser instável
  - **Sinal:** `std_val_pago_m1` ou `coef_variacao = stddev(val_pago) / avg(val_pago)`

**Gap 3: Juros Negativos (Abatimentos)**
- ✅ Temos: SUM_VAL_JUROS_NEG_ABS
- ❌ Falta: Proporção vs total pago
  - **Intuição:** Abatimentos podem indicar renegociação ou cortesia (sinais mistos)
  - **Sinal:** `ratio_juros_neg = sum_val_juros_neg_abs / sum_val_pago`

**Gap 4: Incremento Temporal (Trend)**
- ✅ Temos: Valores M1, M3, M6 (absolutos)
- ❌ Falta: Direção de mudança (cliente pagando mais ou menos?)
  - **Intuição:** Trend crescente em pagamentos = comportamento positivo
  - **Sinal:** `trend_qtd = qtd_m6 / qtd_m3` e `trend_valor = sum_val_pago_m6 / sum_val_pago_m3`

**Gap 5: Status Payment (Missing data pattern)**
- ✅ Temos: FLAG_MISSING_TS_STATUS_PAGAMENTO (binário)
- ❌ Falta: **Proporção de cada status** (P, R, C, B)
  - **Intuição:** Distribuição entre status pode indicar qualidade/comportamento
  - **Sinal:** `pct_status_p_m1`, `pct_status_c_m1` (processado, cancelado)

---

### Atraso (36 features)

#### ✅ Bem coberto:
| Aspecto | Features | Coverage | Status |
|---------|----------|----------|--------|
| **Quantidade faturas abertas** | 3 (QTD M1/M3/M6) | Todos | ✅ |
| **Saldo aberto** | 6 (SUM, AVG, MAX) | M1/M3/M6 | ✅ |
| **Multa+Juros** | 3 (SUM M1/M3/M6) | Todos | ✅ |
| **Flags indicadores** | 6 (WO, PDD, FRAUDE, ACA, PCCR) | M1+M1 | ✅ |

#### ❌ Gaps Identificados:

**Gap 1: Taxa de Inadimplência (Delinquency Rate)**
- ✅ Temos: QTD_FATURAS_ABERTAS (count), SUM_VAL_ABERTO (value)
- ❌ Falta: Proporção de faturas abertas vs total de faturas
  - **Intuição:** Cliente com alta % de faturas abertas = risco claro
  - **Sinal:** `pct_fat_aberta_m1 = qtd_fat_abertas / qtd_total_fat` (precisa COUNT de TODAS)

**Gap 2: Aging Distribution (Idade da Dívida)**
- ✅ Temos: Datas de vencimento/status
- ❌ Falta: Distribuição de dias em atraso
  - **Intuição:** Dívida antiga é mais risco que recente
  - **Sinal:** 
    - `qtd_fat_atraso_0_30` (0-30 dias em atraso)
    - `qtd_fat_atraso_30_60` (30-60 dias)
    - `qtd_fat_atraso_60_plus` (60+ dias)
    - `max_dias_atraso` (pior situação)

**Gap 3: Multa+Juros Rate (Accumulation)**
- ✅ Temos: SUM_VAL_MULTA_JUROS
- ❌ Falta: Proporção vs saldo original
  - **Intuição:** Cliente acumulando muita multa = comportamento deteriorado
  - **Sinal:** `ratio_multa_juros = sum_val_multa_juros / sum_val_aberto` (quando > 0)

**Gap 4: Cobertura de Pagamento (Payment Coverage)**
- ✅ Temos: SUM_VAL_PAGAMENTO (do atraso, não de atual!)
- ❌ Falta: Cobertura = pagamento realizado / saldo aberto
  - **Intuição:** Cliente pagando 100% do saldo = bom, <50% = risco
  - **Sinal:** `coverage_ratio = sum_val_pagamento_m1 / sum_val_aberto_m1`

**Gap 5: Incremento Temporal (Trend Dívida)**
- ✅ Temos: Valores M1, M3, M6 (absolutos)
- ❌ Falta: Direção de mudança em saldo aberto
  - **Intuição:** Saldo aumentando = cliente não está pagando
  - **Sinal:** `trend_saldo = sum_val_aberto_m6 / sum_val_aberto_m3` (deve ser < 1 se pagando)

**Gap 6: Persistência de Atraso (Chronic Delinquency)**
- ✅ Temos: FLAG_TEVE_WO, FLAG_TEVE_PDD (presença em M1)
- ❌ Falta: Persistência entre períodos (estava em atraso M3 E M6 também?)
  - **Intuição:** Cliente em atraso crônico é alto risco
  - **Sinal:** `flag_persistente_atraso = case when (qtd_fat_abertas_m3 > 0) AND (qtd_fat_abertas_m6 > 0) then 1`

---

## 2️⃣ Recomendações: 12 Novas Features

### 🔴 CRÍTICAS (High Impact, Low Effort) — Implementar Agora

#### **Feature 1: Desconto Rate (Pagamento)**
```python
# Lógica:
DESCONTO_RATE_M1 = SUM(val_desconto_item) / (SUM(val_atual_pagamento) + SUM(val_desconto_item))
DESCONTO_RATE_M3 = idem
DESCONTO_RATE_M6 = idem

# Interpretação:
# 0% = nenhum desconto (cliente paga cheio)
# 5-10% = desconto normal (renegociação ocasional)
# >20% = muitos descontos (cliente negociador agressivo = risco)

# SQL pattern (exemplo M1):
MAX(CASE WHEN 
    SUM(val_desconto_item) + SUM(val_atual_pagamento) > 0
    THEN SUM(val_desconto_item) / (SUM(val_desconto_item) + SUM(val_atual_pagamento))
    ELSE 0
END)

# Esperado: +0.3pp KS (sinais claros de clientes negociadores)
```

**Impacto KS esperado:** +0.3pp  
**Implementação:** 5 linhas por período  
**Prioridade:** 🔴 MÁXIMA

---

#### **Feature 2: Delinquency Rate (Atraso)**
```python
# Lógica: Proporção de faturas abertas vs total
DEL_RATE_M1 = QTD_FATURAS_ABERTAS_M1 / QTD_TOTAL_FATURAS_M1

# Interpretação:
# 0% = nenhuma fatura aberta (cliente OK)
# 10-30% = alguns atrasos (normal)
# >50% = maioria das faturas abertas (alto risco)

# Desafio: contar TODAS as faturas (não só abertas)
# Spark: window function para contar distintos num_fatura_hash por NUM_CPF+safra

# SQL pattern:
COUNT(DISTINCT CASE WHEN val_fat_aberto > 0 THEN num_fatura_hash END) /
COUNT(DISTINCT num_fatura_hash)

# Esperado: +0.4pp KS (altamente discriminante)
```

**Impacto KS esperado:** +0.4pp  
**Implementação:** 8 linhas (COUNT DISTINCT é simples)  
**Prioridade:** 🔴 MÁXIMA

---

#### **Feature 3: Max Days in Arrears (Atraso)**
```python
# Lógica: Quantos dias a fatura mais antiga está em atraso?
MAX_DIAS_ATRASO_M1 = MAX(SNAPSHOT_DATE - DAT_VENCIMENTO_FAT)

# Interpretação:
# 0 dias = nada em atraso
# 1-30 dias = atraso pequeno
# 60+ dias = atraso grave (alto risco)

# SQL pattern:
MAX(CASE WHEN val_fat_aberto > 0 
    THEN datediff(to_date(dat_referencia), to_date(dat_vencimento_fat))
    ELSE 0
END)

# Esperado: +0.4pp KS (muito discriminante)
```

**Impacto KS esperado:** +0.4pp  
**Implementação:** 6 linhas  
**Prioridade:** 🔴 MÁXIMA

---

### 🟡 RECOMENDADAS (Medium Impact, Low Effort)

#### Feature 4: Pagamento Trend (Pagamento)
```python
# Lógica: Cliente está pagando mais ou menos ao longo do tempo?
TREND_QTD_PAGAMENTO = QTD_ITENS_PAGAMENTO_M6 / QTD_ITENS_PAGAMENTO_M3
TREND_VALOR_PAGAMENTO = SUM_VAL_PAGO_M6 / SUM_VAL_PAGO_M3

# Interpretação:
# > 1.0 = cliente pagando mais (bom)
# < 0.8 = cliente pagando menos (risco)
# Null/NaN quando denominador = 0 (cliente não pagava em M3)

# SQL: CASE WHEN SUM_VAL_PAGO_M3 > 0 THEN ...
```

**Impacto KS esperado:** +0.2pp  
**Implementação:** 4 linhas  
**Prioridade:** 🟡 ALTA

---

#### Feature 5: Arrears Trend (Atraso)
```python
# Lógica: Saldo devedor está crescendo ou diminuindo?
TREND_SALDO = SUM_VAL_ABERTO_M6 / SUM_VAL_ABERTO_M3

# Interpretação:
# < 0.9 = cliente pagando, saldo diminuindo (bom)
# > 1.1 = cliente não pagando, saldo crescendo (risco)
# 0.9-1.1 = saldo estável (cliente não age)

# SQL: CASE WHEN SUM_VAL_ABERTO_M3 > 0 THEN ...
```

**Impacto KS esperado:** +0.2pp  
**Implementação:** 4 linhas  
**Prioridade:** 🟡 ALTA

---

#### Feature 6: Chronic Arrears Indicator (Atraso)
```python
# Lógica: Cliente estava em atraso M3 E em M6? (persistente)
FLAG_ARREARS_PERSISTENT = CASE WHEN 
    (QTD_FATURAS_ABERTAS_M3 > 0) AND (QTD_FATURAS_ABERTAS_M6 > 0)
    THEN 1 ELSE 0

# Interpretação:
# 1 = cliente em atraso crônico (muito risco)
# 0 = atraso ocasional ou resolvido

# SQL: Simples lógica booleana
```

**Impacto KS esperado:** +0.15pp  
**Implementação:** 3 linhas  
**Prioridade:** 🟡 ALTA

---

### 🟢 SECUNDÁRIAS (Low Impact, Medium Effort)

#### Feature 7: Payment Volatility (Pagamento)
```python
# Lógica: Desvio padrão dos pagamentos em M1
STDDEV_VAL_PAGO_M1 = STDDEV(val_atual_pagamento)
COEF_VAR_M1 = STDDEV / AVG (normalizado)

# Interpretação:
# Baixo CV = pagamentos consistentes (bom)
# Alto CV = pagamentos muito variáveis (cliente instável)

# SQL: STDDEV_POP(val_atual_pagamento) over (partition by num_cpf, safra)
```

**Impacto KS esperado:** +0.15pp  
**Implementação:** 6 linhas  
**Prioridade:** 🟢 MÉDIA

---

#### Feature 8: Fees Accumulation Rate (Atraso)
```python
# Lógica: Proporção de multa/juros acumulados
RATIO_MULTA_JUROS_M1 = SUM_VAL_MULTA_JUROS / SUM_VAL_ABERTO

# Interpretação:
# < 10% = multa normal (ocasional)
# 10-30% = cliente acumulando juros (risco moderado)
# > 30% = cliente deteriorado (muito risco)

# SQL: CASE WHEN SUM_VAL_ABERTO > 0 THEN ...
```

**Impacto KS esperado:** +0.1pp  
**Implementação:** 4 linhas  
**Prioridade:** 🟢 MÉDIA

---

#### Features 9-12: Payment Status Distribution (Pagamento)
```python
# Lógica: Distribuição entre os 5 status (P, R, C, B, NULL)
PCT_STATUS_P_M1 = COUNT(STATUS=P) / COUNT(*)
PCT_STATUS_R_M1 = COUNT(STATUS=R) / COUNT(*)
PCT_STATUS_C_M1 = COUNT(STATUS=C) / COUNT(*)
PCT_STATUS_B_M1 = COUNT(STATUS=B) / COUNT(*)

# Interpretação:
# P = Processado (em andamento)
# R = Recebido (confirmado)
# C = Cancelado (problema)
# B = Bloqueado (problema)

# SQL: SUM(CASE WHEN status='P' THEN 1 ELSE 0) / COUNT(*) OVER (...)
```

**Impacto KS esperado:** +0.1pp (acumulado)  
**Implementação:** 8 linhas (4 features)  
**Prioridade:** 🟢 BAIXA (apenas se tempo permitir)

---

## 3️⃣ Roadmap de Implementação

### Fase 1: CRÍTICAS (1-2 dias de desenvolvimento)
```
1. ✅ Desconto Rate (Pagamento)      → ABT v6.1
2. ✅ Delinquency Rate (Atraso)      → ABT v6.1
3. ✅ Max Days in Arrears (Atraso)   → ABT v6.1

Ganho esperado: +1.1pp KS
Novo target: 45-46%
```

### Fase 2: RECOMENDADAS (1-2 dias)
```
4. ✅ Pagamento Trend
5. ✅ Arrears Trend
6. ✅ Chronic Arrears Indicator

Ganho esperado: +0.55pp KS
Novo target: 45.5-46.5%
```

### Fase 3: SECUNDÁRIAS (2-3 dias, opcional)
```
7-12: Payment Volatility, Fees Rate, Status Distribution

Ganho esperado: +0.35pp KS
Novo target: 45.8-46.8%
```

---

## 4️⃣ Estimativa de Impacto Total

| Scenario | Features | KS v6 | vs v5 | vs v4 |
|----------|----------|-------|-------|-------|
| **Baseline (atual)** | 72 | 44-45% | +2-3pp | +4-5pp |
| **+Críticas (Fase 1)** | 75 | 45-46% | +3-4pp | +5-6pp |
| **+Recomendadas** | 78 | 45.5-46.5% | +3.5-4.5pp | +5.5-6.5pp |
| **+Secundárias** | 90 | 45.8-46.8% | +3.8-4.8pp | +5.8-6.8pp |

---

## 5️⃣ Recomendação Final

### ✅ Implementar AGORA (Fase 1 + 2):
- **Tempo:** 3-4 dias
- **Features:** 6 novas (desconto, delinquência, aging, trends, persistência)
- **Total v6 estendido:** 78 features
- **Ganho esperado:** +1.55pp KS (44.5% → 46%)
- **ROI:** Alto (6 features simples, ganho significativo)

### 🟡 Considerar depois (Fase 3):
- Volatilidade e status distribution (maior complexidade, menor impacto)
- Implementar se KS real for < 45% (ajuste fino)

### ❌ Não recomendar:
- Polinômios/interações (complexidade sem ganho comprovado)
- Features de domínios outros (Telco, Cadastro já muito coberto)

---

## 6️⃣ Validação Anti-Leakage

Todas as 12 features propostas respeitam anti-leakage:

✅ **Pagamento:**
- Usa dados históricos (transações passadas)
- Não usa data futura
- Não usa resultado de aprovação

✅ **Atraso:**
- Snapshot mensal (sem eventos futuros)
- DAT_REFERENCIA é sempre primeiro dia do mês
- Não usa status pós-análise

---

## 7️⃣ SQL Execution Patterns

### Pattern Pagamento (Desconto Rate)
```sql
WITH pag_agg AS (
  SELECT 
    num_cpf, 
    safra_pagamento as safra,
    SUM(val_desconto_item) as sum_desc,
    SUM(val_atual_pagamento) as sum_pago,
    SUM(val_juros_pos) as sum_juros_pos,
    SUM(val_juros_neg_abs) as sum_juros_neg
  FROM silver_pagamento
  WHERE safra_pagamento BETWEEN (safra - 1) AND safra  -- M1 window
  GROUP BY num_cpf, safra_pagamento
)
SELECT 
  num_cpf, 
  safra,
  CASE WHEN (sum_desc + sum_pago) > 0 
    THEN ROUND(sum_desc / (sum_desc + sum_pago), 4) 
    ELSE 0 
  END as desconto_rate_m1
FROM pag_agg
```

### Pattern Atraso (Delinquency Rate)
```sql
WITH atraso_dist AS (
  SELECT 
    num_cpf,
    safra_atraso as safra,
    COUNT(DISTINCT num_fatura_hash) as total_fats,
    COUNT(DISTINCT CASE WHEN val_fat_aberto > 0 THEN num_fatura_hash END) as open_fats
  FROM silver_atraso
  WHERE safra_atraso = safra  -- M1 snapshot
  GROUP BY num_cpf, safra
)
SELECT 
  num_cpf, 
  safra,
  CASE WHEN total_fats > 0 
    THEN ROUND(open_fats / total_fats, 4) 
    ELSE 0 
  END as delinquency_rate_m1
FROM atraso_dist
```

---

## 📋 Checklist para Implementação

- [ ] Fase 1: Desconto Rate, Delinquency Rate, Max Days
  - [ ] Desenvolver lógica em notebook
  - [ ] Testar em amostra 1M linhas
  - [ ] Validar contra data dictionary
  - [ ] Integrar em 05_gold_abt_v6_builder.py
  - [ ] Rodar v6.1 completo
  - [ ] Medir KS (esperado 45-46%)

- [ ] Fase 2: Trends e Persistência
  - [ ] Idem acima
  - [ ] Testar NULLs quando denominador = 0

- [ ] Geral
  - [ ] Atualizar abt_v6.md com novas features
  - [ ] Atualizar validation gates se necessário
  - [ ] Documentar em ABT_V6_QUICK_REFERENCE.md
  - [ ] Executar v6.1 end-to-end
  - [ ] Gerar relatório de cobertura

---

## 🎯 Conclusão

**ABT v6 atual é sólida**, mas há 12 oportunidades claras para aumentar poder discriminante do modelo. As 3 features críticas (Desconto Rate, Delinquency Rate, Max Days Arrears) têm **alto impacto esperado (+1.1pp KS) com baixo esforço (1-2 dias)**.

**Recomendação:** Implementar Fase 1 + Fase 2 antes de treinar modelo final. Isso levará KS esperado de 44-45% → 45.5-46.5%, colocando v6 em nível profissional de credit risk modeling.

---

**Próximos passos:**
1. Validar recomendações com time de negócio
2. Começar Fase 1 (desconto rate + delinquency rate)
3. Testar em ambiente de desenvolvimento
4. Medir KS real (validar +1.1pp)
5. Se confirmado, prosseguir Fase 2
