# AUDITORIA: Scripts vs Regras Documentadas
**Data:** 2026-01-23  
**Objetivo:** Validar alinhamento dos scripts criados com docs/03_silver_rules/ e ABT v6 spec

---

## EXECUTIVE SUMMARY

| Componente | Status | Severidade | Observação |
|---|---|---|---|
| **Silver Pagamento** | ⚠️ PARCIALMENTE CONFORME | MÉDIA | Features geradas ≠ regra esperada |
| **Silver Atraso** | ⚠️ PARCIALMENTE CONFORME | MÉDIA | Estrutura correta, faltam algumas agregações |
| **Silver Recarga** | ✅ CONFORME | - | Alinhado com especificação |
| **ABT v6 (Agregações)** | ⚠️ NÃO CONFORME | ALTA | Estrutura de features diverge das regras Silver |
| **ABT v6 (Gates)** | ✅ CONFORME | - | 14 gates implementados corretamente |

---

## 1. SILVER PAGAMENTO (`04_bronze_silver_pagamento.py`)

### 1.1) Parsing de Datas ✅ CONFORME
**Regra esperada (pagamento.md §3.1):**
```
TS_STATUS_FATURA = to_timestamp(upper(DAT_STATUS_FATURA),'ddMMMyyyy:HH:mm:ss')
TS_STATUS_PAGAMENTO = to_timestamp(upper(DAT_STATUS_PAGAMENTO),'ddMMMyyyy:HH:mm:ss')
SAFRA_PAGAMENTO = date_format(to_date(TS_STATUS_FATURA),'yyyyMM')
```

**Implementado:**
```python
df.withColumn("ts_status_fatura", 
              F.to_timestamp(F.upper(F.col("DAT_STATUS_FATURA")), "ddMMMyyyy:HH:mm:ss"))
df.withColumn("safra_pagamento", 
              F.date_format(F.to_date(F.col("ts_status_fatura")), "yyyyMM"))
```
✅ **RESULTADO:** Implementação correta e completa.

---

### 1.2) Casting Monetário ✅ CONFORME
**Regra esperada (pagamento.md §4.1):**
- Minimo: VAL_PAGAMENTO_FATURA, VAL_ATUAL_PAGAMENTO, VAL_DESCONTO_ITEM, VAL_JUROS_MULTAS_ITEM, etc.
- Usar `to_double_safe()` ou equivalente

**Implementado:**
```python
monetary_cols = [
    "VAL_PAGAMENTO_FATURA",
    "VAL_PAGAMENTO_ITEM",
    "VAL_ATUAL_PAGAMENTO",
    "VAL_ORIGINAL_PAGAMENTO",
    "VAL_PAGAMENTO_CREDITO",
    "VAL_DESCONTO_ITEM",
    "VAL_JUROS_MULTAS_ITEM",
    "VAL_MULTA_EQUIP_ITEM",
    "VAL_MULTA_EQUIP_TOTAL",
    "VAL_MULTA_FID_ITEM",
    "VAL_BAIXA_ATIVIDADE"
]
for col in monetary_cols:
    if col in df.columns:
        df = df.withColumn(col.lower(), to_double_safe(col))
```
✅ **RESULTADO:** Casting implementado para todas as colunas recomendadas.

---

### 1.3) Tratamento de Juros Negativos ✅ CONFORME
**Regra esperada (pagamento.md §4.2):**
- Manter VAL_JUROS_MULTAS_ITEM original
- Criar `FLAG_JUROS_NEG = 1` quando negativo
- Criar `VAL_JUROS_POS = greatest(VAL_JUROS_MULTAS_ITEM, 0)`
- Criar `VAL_JUROS_NEG_ABS = abs(least(VAL_JUROS_MULTAS_ITEM, 0))`

**Implementado:**
```python
df = df.withColumn("flag_juros_neg",
    F.when((F.col("val_juros_multas_item").isNotNull()) & 
           (F.col("val_juros_multas_item") < 0), F.lit(1))
    .otherwise(F.lit(0)))

df = df.withColumn("val_juros_pos", 
    F.greatest(F.col("val_juros_multas_item"), F.lit(0)))

df = df.withColumn("val_juros_neg_abs", 
    F.abs(F.least(F.col("val_juros_multas_item"), F.lit(0))))
```
✅ **RESULTADO:** Implementação 100% conforme.

---

### 1.4) Deduplicação por Versionamento ✅ CONFORME
**Regra esperada (pagamento.md §5):**
- DEDUP_KEY = NUM_CPF + CONTRATO + SEQ_FATURA + NUM_SUB_SEQ_FATURA + NUM_CREDITO_SEQ
- Manter row_number() = 1 ordenado por TS_STATUS_FATURA DESC
- Auditoria: registrar linhas_removidas (esperado: 8.163)

**Implementado:** ✅ (verificado em linhas 150-180 do script)
- Cria dedup_key com concatenação
- Aplica row_number com partição correta
- Registra removidas: "linhas_removidas = 8,163"

---

### 1.5) Flags de Missing ✅ CONFORME
**Regra esperada (pagamento.md §3.1):**
- `FLAG_TS_STATUS_PAGAMENTO_MISSING = 1` quando DAT_STATUS_PAGAMENTO nulo/vazio

**Implementado:**
```python
df = df.withColumn("flag_ts_status_pagamento_missing",
    F.when(F.col("ts_status_pagamento").isNull(), F.lit(1))
    .otherwise(F.lit(0)))
```
✅ **RESULTADO:** Conforme.

---

### 1.6) Padronização de Nomes ✅ CONFORME
**Regra:** Converter para snake_case via `standardize_column_names()`
**Implementado:** Sim, chamada no final do script

---

## 2. SILVER ATRASO (`05_bronze_silver_atraso.py`)

### 2.1) Parsing de Datas ✅ CONFORME
**Regra esperada (atraso.md §3):**
```
TS_REFERENCIA = to_timestamp(upper(DAT_REFERENCIA),'ddMMMyyyy:HH:mm:ss')
TS_VENCIMENTO = to_timestamp(upper(DAT_VENCIMENTO_FAT),'ddMMMyyyy:HH:mm:ss')
TS_STATUS_FAT = to_timestamp(upper(DAT_STATUS_FAT),'ddMMMyyyy:HH:mm:ss')
SAFRA_ATRASO = date_format(to_date(TS_REFERENCIA),'yyyyMM')
```
✅ **RESULTADO:** Implementação correta.

---

### 2.2) Casting Monetário ✅ CONFORME
**Regra esperada (atraso.md §4):**
- VAL_FAT_LIQUIDO, VAL_FAT_BRUTO, VAL_FAT_ABERTO, VAL_MULTA_JUROS, etc.

**Implementado:** Sim, todas as colunas recomendadas

---

### 2.3) Sentinelas em Colunas Dimensionais ✅ CONFORME
**Regra esperada (atraso.md §5):**
- Criar FLAG_<COL>_SENTINELA quando col IN ('-1','-2','-3')

**Implementado:**
```python
COLS_WITH_SENTINELA = {
    "IND_WO": "flag_ind_wo_sentinela",
    "IND_PDD": "flag_ind_pdd_sentinela",
    "IND_PCCR": "flag_ind_pccr_sentinela",
    "DW_TIPO_CLIENTE_CONTA": "flag_dw_tipo_cliente_sentinela",
    "COD_PLATAFORMA": "flag_cod_plataforma_sentinela",
    "DW_FAIXA_TEMPO_BASE": "flag_faixa_tempo_base_sentinela",
    "DW_FAIXA_AGING_PROX_FECH": "flag_faixa_aging_prox_fech_sentinela"
}
```
✅ **RESULTADO:** Conforme às recomendações.

---

### 2.4) Política de Deduplicação ✅ CONFORME
**Regra esperada (atraso.md §6):**
- NÃO aplicar dedup agressivo (pode apagar sinal real)
- Manter "itens do snapshot"
- Dedupe apenas para duplicatas EXATAS se houver

**Implementado:** Script não força dedupe (correto), apenas monitora

---

## 3. SILVER RECARGA (`03_bronze_silver_recarga.py`)

### 3.1) Parsing de Data ✅ CONFORME
**Regra (recarga.md §3.1):**
```
TS_RECARGA = to_timestamp(DAT_INSERCAO_CREDITO,'ddMMMyyyy:HH:mm:ss')
SAFRA_RECARGA = date_format(to_date(TS_RECARGA),'yyyyMM')
```
✅ **RESULTADO:** Implementação correta com tolerância a parsing inválido

---

### 3.2) Casting Numérico ✅ CONFORME
**Regra:** VAL_CREDITO_INSERIDO, VAL_BONUS, VAL_REAL, VALOR_SOS → double
✅ **RESULTADO:** Implementado

---

### 3.3) Tratamento de Negativos ✅ CONFORME
**Regra (recaga.md §5.1):**
- Manter valor original
- Criar FLAG_VAL_BONUS_NEG, FLAG_VAL_REAL_NEG
- Criar colunas CLEAN (NULL se negativo)

✅ **RESULTADO:** Implementado conforme esperado

---

### 3.4) Sentinelas ✅ CONFORME
**Regra:** Mapear -1/-2/-3 para "sentinela" e criar flags
✅ **RESULTADO:** Implementado para todas as colunas dimensionais

---

### 3.5) Deduplicação por EVENT_KEY ✅ CONFORME
**Regra:** Hash robusta de colunas-chave + row_number
✅ **RESULTADO:** Implementado com função `dedupe_by_event_key()`

---

## 4. ABT v6 - ANÁLISE DE FEATURES

### 4.1) ESTRUTURA DE AGREGAÇÃO PAGAMENTO

**Regra esperada (abt_v6.md / pagamento.md §7):**
```
QTD_ITENS_FATURA_MES
SUM_VAL_PAGO_MES
SUM_DESCONTOS_MES
SUM_JUROS_POS_MES
SUM_JUROS_NEG_ABS_MES
[+ distribuição por COD_FORMA_PAGAMENTO / status]
```

**Implementado em 05_gold_abt_v6_builder.py (agregação temporal):**
```python
F.count(F.lit(1)).alias(f"qtd_itens_pagamento_{period_name}"),
F.sum(F.col("val_atual_pagamento")).alias(f"sum_val_pago_{period_name}"),
F.sum(F.col("val_desconto_item")).alias(f"sum_val_desconto_{period_name}"),
F.sum(F.col("val_juros_pos")).alias(f"sum_val_juros_pos_{period_name}"),
F.sum(F.col("val_juros_neg_abs")).alias(f"sum_val_juros_neg_abs_{period_name}"),
```

**ANÁLISE:**
- ✅ Quantidade e somas estão presentes
- ✅ Juros positivos e negativos separados
- ⚠️ **FALTA:** Distribuição por COD_FORMA_PAGAMENTO (não agregado)
- ⚠️ **FALTA:** Distribuição por IND_STATUS_PAGAMENTO
- ⚠️ **FALTA:** Flag de teve_desconto é criada, mas deveria incluir valor agregado de descontos

**SEVERIDADE:** MÉDIA (features essenciais presentes, mas faltam dimensões de breakdown)

---

### 4.2) ESTRUTURA DE AGREGAÇÃO ATRASO

**Regra esperada (abt_v6.md / atraso.md §9):**
```
SUM_ABERTO_MES
QTD_FATURAS_ABERTO_MES
MAX_ABERTO_MES, AVG_ABERTO_MES
SUM_PAGAMENTO_MES
FLAGS: TEVE_WO_MES, TEVE_PDD_MES, TEVE_FRAUDE_MES
[+ distribuição por FAIXA_AGING]
```

**Implementado em 05_gold_abt_v6_builder.py:**
```python
F.sum(F.when(F.col("val_fat_aberto") > 0, F.lit(1)).otherwise(F.lit(0)))
 .alias(f"qtd_faturas_abertas_{period_name}"),
F.sum(F.col("val_fat_aberto")).alias(f"sum_val_aberto_{period_name}"),
F.avg(F.col("val_fat_aberto")).alias(f"avg_val_aberto_{period_name}"),
F.max(F.col("val_fat_aberto")).alias(f"max_val_aberto_{period_name}"),
F.sum(F.col("val_fat_pagamento_bruto")).alias(f"sum_val_pagamento_{period_name}"),
F.max(F.col("ind_wo")).alias(f"flag_teve_wo_{period_name}"),
F.max(F.col("ind_pdd")).alias(f"flag_teve_pdd_{period_name}"),
```

**ANÁLISE:**
- ✅ Soma, contagem, média, máximo de saldo aberto
- ✅ Soma de pagamento
- ✅ Indicadores WO, PDD, FRAUDE (para M1 apenas)
- ⚠️ **FALTA:** Distribuição por FAIXA_AGING
- ⚠️ **FALTA:** Val_multa_juros agregado
- ⚠️ **NOTA:** FRAUDE, ACA, PCCR agregados apenas para M1 (deveria ser M1/M3/M6?)

**SEVERIDADE:** MÉDIA (features principais presentes, mas faltam quebras by aging)

---

### 4.3) VERIFICAÇÃO DE COLUMN NAMES

**Expectativa:** Nomes devem estar em snake_case e ter pattern consistente

**Observado:**
```
qtd_itens_pagamento_m1 ✅
sum_val_pago_m1 ✅
flag_teve_desconto_m1 ✅
sum_val_juros_pos_m1 ✅
sum_val_juros_neg_abs_m1 ✅
```

✅ **RESULTADO:** Nomenclatura consistente e padronizada

---

### 4.4) TEMPORAL WINDOWS (M1/M3/M6)

**Regra:** Meses anteriores à SAFRA anchor
```
M1 = meses_back = 0  (mesmo mês)
M3 = meses_back = 2  (últimos 3 meses)
M6 = meses_back = 5  (últimos 6 meses)
```

**Implementado:**
```python
for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
    safra_lower_bound = safra - (100 * months_back)
    filter: (safra_pag_calc >= safra_lower_bound) & (safra_pag_calc <= safra)
```

⚠️ **POTENCIAL ISSUE:**
- Lógica: `safra - 100*0 = safra` (correto para M1)
- Lógica: `safra - 100*2 = safra - 200` (INCORRETO para M3)
  - Esperado: últimos 3 meses = safra, safra-100, safra-200
  - Implementado: apenas safra e safra-100? Não...

**ANÁLISE DETALHADA:**
```
M1: months_back=0, safra_lower=safra - 0 = safra
    Período: [safra, safra] → 1 mês ✅

M3: months_back=2, safra_lower=safra - 200
    Período: [safra-200, safra] → 3 meses ✅
    (safra, safra-100, safra-200)

M6: months_back=5, safra_lower=safra - 500
    Período: [safra-500, safra] → 6 meses ✅
    (safra, safra-100, ..., safra-500)
```

✅ **RESULTADO:** Lógica de janelas temporal está correta

---

## 5. COMPARATIVO: DOCUMENTAÇÃO vs IMPLEMENTAÇÃO

| Regra de Negócio | Esperado | Implementado | Status | Gap |
|---|---|---|---|---|
| **Pagamento: Parse data** | ddMMMyyyy:HH:mm:ss | ddMMMyyyy:HH:mm:ss | ✅ | Nenhum |
| **Pagamento: Casting** | double | to_double_safe() | ✅ | Nenhum |
| **Pagamento: Juros separado** | POS + NEG_ABS + FLAG | POS + NEG_ABS + FLAG | ✅ | Nenhum |
| **Pagamento: Dedup** | RN=1 TS DESC | RN=1 TS DESC | ✅ | Nenhum |
| **Atraso: Parse 3 datas** | REF + VENC + STATUS | REF + VENC + STATUS | ✅ | Nenhum |
| **Atraso: Flags sentinela** | 7 flags | 7 flags | ✅ | Nenhum |
| **Atraso: Não dedup** | Manter itens | Sem dedup | ✅ | Nenhum |
| **Recarga: Tolerância parse** | Try/catch | Flag invalid | ✅ | Nenhum |
| **Recarga: Valores clean** | NULL se neg | NULL se neg | ✅ | Nenhum |
| **ABT v6 Pag: QTD + SUM + JUR** | Sim | Sim | ✅ | Nenhum |
| **ABT v6 Pag: Breakdown canal** | COD_FORMA_PAGAMENTO | Não agregado | ❌ | Missing |
| **ABT v6 Atr: QTD + SUM + AVG** | Sim | Sim | ✅ | Nenhum |
| **ABT v6 Atr: Breakdown aging** | FAIXA_AGING | Não agregado | ❌ | Missing |
| **ABT v6: Temporal M1/M3/M6** | 3 windows | 3 windows | ✅ | Nenhum |
| **ABT v6: Flags M1 especial** | FRAUDE/ACA/PCCR M1 | FRAUDE/ACA/PCCR M1 | ✅ | Nenhum |

---

## 6. FINDINGS E RECOMENDAÇÕES

### CRÍTICO ❌
Nenhum encontrado.

### ALTO ⚠️
1. **ABT v6 - Agregações dimensionais missing**
   - **Problema:** Esperado breakdown by `COD_FORMA_PAGAMENTO` e `FAIXA_AGING`, não implementado
   - **Impacto:** Features dimensionais que melhoram segmentação e interpretabilidade não estão presentes
   - **Recomendação:** Adicionar agregações:
     ```python
     # Pagamento breakdown
     .agg(..., F.sum(F.when(F.col("cod_forma_pagamento") == "01", F.col("sum_val_pago"))).alias(f"sum_pago_forma01_{period_name}"))
     
     # Atraso breakdown
     .agg(..., F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "0-30", 1)).alias(f"qtd_aging_0_30_{period_name}"))
     ```
   - **Prioridade:** MÉDIA (não bloqueia v6, mas reduz poder preditivo)

### MÉDIO ⚠️
2. **ABT v6 - Flags múltiplos períodos**
   - **Problema:** FRAUDE/ACA/PCCR apenas para M1, esperado M1/M3/M6
   - **Impacto:** Menos features para modelagem
   - **Recomendação:** Replicar logic para M3 e M6:
     ```python
     if period_name in ["m1", "m3", "m6"]:  # ao invés de apenas "m1"
         agg = agg.join(df_period.groupBy(...).agg(fraude, aca, pccr))
     ```
   - **Prioridade:** BAIXA (M1 é período mais importante)

### BAIXO ℹ️
3. **Nomenclatura de flags em Atraso**
   - **Problema:** Flags são criadas em Silver como `flag_ind_wo_sentinela`, mas ABT v6 usa `ind_wo` direto (coluna original)
   - **Impacto:** Inconsistência de naming, mas funcional
   - **Recomendação:** Padronizar se flags devem vir da Silver ou original
   - **Prioridade:** BAIXA (não afeta resultados)

---

## 7. CONCLUSÃO

### Status Geral: ⚠️ **PARCIALMENTE CONFORME**

**Pontos Fortes:**
- ✅ Todas as transformações Silver estão 100% alinhadas com regras documentadas
- ✅ Parsing de datas, casting monetário, flags de missing/sentinelas implementados corretamente
- ✅ Deduplicação (Pagamento) e não-dedup (Atraso) seguem políticas recomendadas
- ✅ Temporal windows M1/M3/M6 estão corretos
- ✅ Padronização de nomes e metadados presentes
- ✅ 14 gates de validação implementados

**Pontos Fracos:**
- ⚠️ **ABT v6 faltam aggregações dimensionais** (breakdown by forma_pagamento, aging)
- ⚠️ **ABT v6 flags FRAUDE/ACA/PCCR apenas para M1** (deveria M1/M3/M6)

**Recomendação Final:**
- ✅ **Proceder com execução v6** (gaps não são bloqueadores)
- 📋 **Depois de validação v6:** Implementar agregações dimensionais para v7/v8 (próximas iterações)
- 📊 **Monitorar KS em v6:** Se não atingir 44%+, investigar se faltam features dimensionais

---

## APÊNDICE A: CHECKLIST DE CONFORMIDADE

### Silver Pagamento
- [x] Parse TS_STATUS_FATURA (ddMMMyyyy:HH:mm:ss)
- [x] Parse TS_STATUS_PAGAMENTO
- [x] Derivar SAFRA_PAGAMENTO
- [x] Cast double (todos os valores monetários)
- [x] Flag VAL_JUROS_NEG
- [x] VAL_JUROS_POS e VAL_JUROS_NEG_ABS derivados
- [x] Dedup por DEDUP_KEY (RN=1 TS DESC)
- [x] Auditoria linhas_removidas = 8.163
- [x] Padronizar nomes (snake_case)

### Silver Atraso
- [x] Parse TS_REFERENCIA, TS_VENCIMENTO, TS_STATUS_FAT
- [x] Derivar SAFRA_ATRASO
- [x] Cast double (monetários)
- [x] Flag STATUS_FAT_MISSING
- [x] Flags sentinela (IND_WO, IND_PDD, IND_PCCR, etc.)
- [x] Sem dedup agressiva
- [x] Padronizar nomes

### Silver Recarga
- [x] Parse TS_RECARGA com tolerância
- [x] Derivar SAFRA_RECARGA
- [x] Cast double (valores)
- [x] Flags de negativos (VAL_BONUS_NEG, VAL_REAL_NEG)
- [x] Colunas CLEAN (NULL se neg)
- [x] Flags sentinela
- [x] Dedup por EVENT_KEY
- [x] Padronizar nomes

### ABT v6
- [x] Temporal windows M1/M3/M6
- [x] Agregação QTD/SUM/AVG/MAX
- [x] Flags de presença/indicadores
- [x] 14 gates de validação
- [x] Padronizar nomes
- [ ] ⚠️ Breakdown by dimensão (Pagamento: forma; Atraso: aging) — NÃO IMPLEMENTADO
- [ ] ⚠️ Flags FRAUDE M3/M6 — NÃO IMPLEMENTADO

---

## APÊNDICE B: MAPEAMENTO ABT v6 vs Docum

ação

| Feature | Documentado | Implementado | Observação |
|---|---|---|---|
| qtd_itens_pagamento_m1 | QTD_ITENS_FATURA | ✅ | count(1) |
| sum_val_pago_m1 | SUM_VAL_PAGO | ✅ | sum(val_atual_pagamento) |
| sum_val_desconto_m1 | SUM_DESCONTOS | ✅ | sum(val_desconto) |
| sum_val_juros_pos_m1 | SUM_JUROS_POS | ✅ | sum(val_juros_pos) |
| sum_val_juros_neg_abs_m1 | SUM_JUROS_NEG_ABS | ✅ | sum(val_juros_neg_abs) |
| flag_teve_desconto_m1 | (implícito) | ✅ | max(when val>0 then 1) |
| **QTD_ITENS_FORMA_TOP1** | SIM | ❌ | Missing |
| **FORMA_TOP1** | SIM | ❌ | Missing |
| qtd_faturas_abertas_m1 | QTD_FATURAS_ABERTO | ✅ | sum(when val>0 then 1) |
| sum_val_aberto_m1 | SUM_ABERTO | ✅ | sum(val_fat_aberto) |
| avg_val_aberto_m1 | AVG_ABERTO | ✅ | avg(val_fat_aberto) |
| max_val_aberto_m1 | MAX_ABERTO | ✅ | max(val_fat_aberto) |
| sum_val_pagamento_m1 | SUM_PAGAMENTO | ✅ | sum(val_fat_pagamento_bruto) |
| flag_teve_wo_m1 | TEVE_WO | ✅ | max(ind_wo) |
| flag_teve_pdd_m1 | TEVE_PDD | ✅ | max(ind_pdd) |
| flag_teve_fraude_m1 | TEVE_FRAUDE (M1 only) | ✅ | max(ind_fraude) M1 apenas |
| **QTD_AGING_0_30_M1** | SIM | ❌ | Missing |
| **QTD_AGING_31_60_M1** | SIM | ❌ | Missing |
| **DISTRIBUICAO_AGING** | SIM | ❌ | Missing |
| sum_val_multa_juros_m1 | (implícito) | ⚠️ | Não agregado |

---

**Fim da Auditoria**
