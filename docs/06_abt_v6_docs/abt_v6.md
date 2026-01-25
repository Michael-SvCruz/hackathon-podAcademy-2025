# ABT v6 — Analytical Base Table Final (Score + Telco + Cadastro + Recarga + Pagamento + Atraso)

**Roadmap:** v1 (Score_01) → v2 (Score_02) → v3 (Telco) → v4 (Cadastro) → v5 (Recarga) → **v6 (Pagamento + Atraso) ✨**

**Status:** Design completo, pronto para implementação  
**Data de Criação:** 23 de janeiro de 2026  
**Versão:** 1.0

---

## 📋 Índice

1. [Objetivo](#objetivo)
2. [Roadmap Incremental](#roadmap-incremental)
3. [Definições Críticas](#definições-críticas)
4. [Estrutura de Dados](#estrutura-de-dados)
5. [Features por Bloco](#features-por-bloco)
6. [Lógica de Join](#lógica-de-join)
7. [Validações (10+ Gates)](#validações-gates)
8. [Anti-Leakage Rules](#anti-leakage-rules)
9. [Próximos Passos](#próximos-passos)

---

## Objetivo

Construir a **tabela final de modelagem (ABT v6)** que combina todos os dados históricos e comportamentais do cliente por safra (cliente-mês):

```
SPINE: ABT v5 (3.795.310 registros, 195+ colunas)
  ├─ Score_01, Score_02 (aprovação + risco)
  ├─ Telco (68 variáveis comportamento móvel)
  ├─ Cadastro (33 variáveis perfil)
  ├─ Recarga (18 variáveis prepago M1/M3/M6)
  │
  └─ NOVO EM v6:
      ├─ Pagamento (features transacionais M1/M3/M6)
      └─ Atraso (features inadimplência M1/M3/M6)
      
RESULTADO: ABT v6 com ~250 colunas → pronto para cientistas de dados
```

---

## Roadmap Incremental

| Versão | Bloco | Features | Status | KS Esperado |
|--------|-------|----------|--------|-------------|
| v1 | SCORE_01 | 2 (score + flag) | ✅ | ~33.1% |
| v2 | SCORE_02 | +2 (score + flag) | ✅ | ~35% |
| v3 | TELCO | +68 (var_26-93) | ✅ | ~38% |
| v4 | CADASTRO | +33 (age, cep, var_02-25) | ✅ | ~40.2% |
| v5 | RECARGA | +18 (M1/M3/M6 prepago) | ✅ | ~42% |
| **v6** | **PAGAMENTO + ATRASO** | **+36 (M1/M3/M6 cada)** | 🔄 | **~44-45%** |

---

## Definições Críticas

### Anchor Event (Cliente-Mês)
- **Unidade de análise:** `NUM_CPF + SAFRA` (1:1 grão, sem duplicatas)
- **Referência temporal:** `DT_SAFRA = primeiro dia do mês`
- **Safra derivada:**
  - Pagamento: `SAFRA_PAGAMENTO = YYYYMM(TS_STATUS_FATURA)`
  - Atraso: `SAFRA_ATRASO = YYYYMM(TS_REFERENCIA)` (snapshot mensal)

### Labels (Auditoria, Não-Features)
| Label | Type | Observabilidade | Validação |
|-------|------|-----------------|-----------|
| `FPD_INT` | Target (risco) | Observado **SÓ** em `FLAG_INSTALACAO_INT=1` | Gate 2 |
| `FLAG_INSTALACAO_INT` | Decision (aprovação) | Sempre presente | Gate 4 |

### Anti-Leakage Crítico
- ✅ **FPD_INT**: Usado apenas para análise/auditoria, NUNCA como feature
- ✅ **FLAG_INSTALACAO_INT**: Usado para análise de swaps, NUNCA como feature
- ✅ **Pagamento**: Agregação transacional (puro histórico) — seguro
- ✅ **Atraso**: Snapshot mensal (sem eventos futuros) — seguro
- ✅ **Recarga**: Histórico prepago (1-6 meses atrás) — seguro

---

## Estrutura de Dados

### Entrada: ABT v5 (Spine)
```
Grain: NUM_CPF + SAFRA (3.795.310 linhas)
Colunas:
  - Chaves: num_cpf, safra, dt_safra
  - Labels: flag_instalacao_int, fpd_int
  - Features v1-v5: score_01/02, var_26-93, var_02-25, qtd_recargas_m1/m3/m6, etc
  - Metadados: prod, flag_mig2, gold_*
```

### Entrada: Silver Pagamento (Eventos)
```
Grain: Transacional (múltiplas linhas por NUM_CPF+SAFRA)
21.821.465 linhas após dedup
Colunas principais:
  - Chaves: num_cpf, contrato, seq_fatura, ...
  - Datas: ts_status_fatura, safra_pagamento, ts_status_pagamento
  - Valores: val_atual_pagamento, val_desconto_item, val_juros_pos, val_juros_neg_abs
  - Flags: flag_ts_status_pagamento_missing, flag_juros_neg
```

### Entrada: Silver Atraso (Snapshot Mensal)
```
Grain: Transacional no snapshot (múltiplas linhas por NUM_CPF+SAFRA_ATRASO)
31.611.316 linhas
Colunas principais:
  - Chaves: num_cpf, safra_atraso, num_fatura_hash, ...
  - Datas: ts_referencia, ts_vencimento, ts_status_fat
  - Valores: val_fat_aberto, val_fat_pagamento_bruto, val_multa_juros, ...
  - Flags: flag_status_fat_missing, flag_cod_plataforma_sentinela, etc
  - Indicadores: ind_wo, ind_pdd, ind_pccr, ind_aca, ind_fraude
```

### Saída: ABT v6
```
Grain: NUM_CPF + SAFRA (3.795.310 linhas = v5)
Colunas esperadas: ~250 (v5 + 36 Pagamento + 36 Atraso + metadados)
Joins:
  - ABT v5 (spine)
    ↓ LEFT JOIN
  - Pagamento agregado (M1/M3/M6)
    ↓ LEFT JOIN
  - Atraso agregado (M1/M3/M6)
```

---

## Features por Bloco

### Pagamento (Novo em v6) — 36 features

#### M1 (1 mês antes de DT_SAFRA)
| Feature | Descrição | Tipo | Lógica |
|---------|-----------|------|--------|
| `QTD_ITENS_PAGAMENTO_M1` | Quantidade de itens/transações | INT | COUNT(*) |
| `SUM_VAL_PAGO_M1` | Soma de valores pagos | DOUBLE | SUM(val_atual_pagamento) |
| `SUM_VAL_DESCONTO_M1` | Soma de descontos | DOUBLE | SUM(val_desconto_item) |
| `SUM_VAL_JUROS_POS_M1` | Soma juros positivos (charges) | DOUBLE | SUM(val_juros_pos) |
| `SUM_VAL_JUROS_NEG_ABS_M1` | Soma juros negativos (abatimentos) | DOUBLE | SUM(val_juros_neg_abs) |
| `AVG_VAL_PAGO_M1` | Média valores pagos | DOUBLE | AVG(val_atual_pagamento) |
| `MAX_VAL_PAGO_M1` | Máximo valor pago | DOUBLE | MAX(val_atual_pagamento) |
| `FLAG_TEVE_DESCONTO_M1` | Indicador presença desconto | INT | CASE WHEN SUM > 0 THEN 1 |

#### M3 (3 meses) + M6 (6 meses)
- Mesmas 8 features, para períodos M3 e M6
- **Total Pagamento: 24 features** (8 × 3 períodos)

#### Flags de Qualidade (Pagamento)
- `FLAG_MISSING_TS_STATUS_PAGAMENTO_M1/M3/M6`: % missing > 50%
- **Total: 12 features** (4 flags × 3 períodos)

**Total Pagamento: 24 + 12 = 36 features**

---

### Atraso (Novo em v6) — 36 features

#### M1 (1 mês)
| Feature | Descrição | Tipo | Lógica |
|---------|-----------|------|--------|
| `QTD_FATURAS_ABERTAS_M1` | Quantidade faturas com saldo > 0 | INT | COUNT(CASE WHEN val_fat_aberto > 0) |
| `SUM_VAL_ABERTO_M1` | Soma saldo devedor | DOUBLE | SUM(val_fat_aberto) |
| `AVG_VAL_ABERTO_M1` | Média saldo por fatura | DOUBLE | AVG(val_fat_aberto) |
| `MAX_VAL_ABERTO_M1` | Máximo saldo | DOUBLE | MAX(val_fat_aberto) |
| `SUM_VAL_PAGAMENTO_M1` | Soma pagamentos realizados | DOUBLE | SUM(val_fat_pagamento_bruto) |
| `SUM_VAL_MULTA_JUROS_M1` | Soma multas + juros | DOUBLE | SUM(val_multa_juros) |
| `FLAG_TEVE_WO_M1` | Write-off no período | INT | MAX(ind_wo) |
| `FLAG_TEVE_PDD_M1` | Problematic Delinquent Debt | INT | MAX(ind_pdd) |

#### M3 (3 meses) + M6 (6 meses)
- Mesmas 8 features

#### Indicadores Agregados (M1 only)
- `FLAG_TEVE_FRAUDE_M1`: Fraude detectada
- `FLAG_TEVE_ACA_M1`: Ação de cobrança
- `FLAG_TEVE_PCCR_M1`: PCCR (programa de compromisso)

**Total Atraso: 24 + 12 = 36 features**

**Total Novo em v6: 72 features** (36 Pagamento + 36 Atraso)

---

## Lógica de Join

```
ABT v5 (spine: 3.795.310 registros)
  │
  ├─ LEFT JOIN aggregated_pagamento
  │   ON: num_cpf = num_cpf AND safra_pagamento = safra
  │   Tipo: LEFT (preserva todos os v5, adiciona pagamento como enriquecimento)
  │
  └─ LEFT JOIN aggregated_atraso
      ON: num_cpf = num_cpf AND safra_atraso = safra
      Tipo: LEFT (preserva todos os v5, adiciona atraso como enriquecimento)
      
RESULTADO: 3.795.310 linhas com todas as features
```

### Coalesce Strategy (Quando NULL)
```
QTD_RECARGAS_M1 → 0
SUM_VAL_PAGO_M1 → 0.0
AVG_VAL_PAGO_M1 → 0.0
FLAG_* → 0
```

---

## Validações (Gates)

### Gates 1-8 (Herdados de v5, validar continuam OK)
1. ✅ Unicidade: 1:1 NUM_CPF + SAFRA (sem duplicatas)
2. ✅ Anti-leakage FPD: nulo em FLAG_INSTALACAO=0
3. ✅ Integridade chaves: sem NULLs
4. ✅ Distribuição FLAG_INSTALACAO: ambos 0 e 1
5. ✅ Score_01 cobertura ≥90%
6. ✅ Score_02 cobertura ≥40%
7. ✅ Telco cobertura ≥20%
8. ✅ Cadastro cobertura ≥20%

### Gates 9-10 (Novos em v5, validar continuam OK)
9. ✅ Recarga cobertura ≥5%
10. ✅ QTD_RECARGAS_M1 sanidade (sem NaN/Inf)

### Gates 11-12 (Novos em v6)
11. **Pagamento cobertura ≥2%**
    - Espera-se: ~5-10% dos clientes com transações de pagamento
    - Mínimo aceitável: 2%
    
12. **Atraso cobertura ≥10%**
    - Espera-se: ~20-30% dos clientes com saldo aberto
    - Mínimo aceitável: 10%

### Gates 13-14 (Sanidade v6)
13. **QTD_ITENS_PAGAMENTO_M1 sanidade**
    - Sem NaN, Inf, negativos
    - Min=0, Max=razoável (< 1000)
    
14. **QTD_FATURAS_ABERTAS_M1 sanidade**
    - Sem NaN, Inf, negativos
    - Min=0, Max=razoável (< 500)

---

## Anti-Leakage Rules

### ✅ Pagamento — Seguro
- **Dados:** histórico transacional de pagamentos efetuados
- **Janelas:** M1/M3/M6 = 1-6 meses ANTES de DT_SAFRA
- **Risco:** ZERO — nenhuma transação futura está em Pagamento
- **Validação:** TS_STATUS_FATURA sempre ≤ DT_SAFRA

### ✅ Atraso — Seguro (Snapshot)
- **Dados:** fotografia mensal (TS_REFERENCIA = dia 01)
- **Estado:** tudo medido NA DATA DO SNAPSHOT (não há eventos futuros)
- **Janelas:** M1/M3/M6 = snapshots de 1-6 meses ANTES de DT_SAFRA
- **Risco:** ZERO — snapshot é fotografia, não predição
- **Validação:** SAFRA_ATRASO sempre ≤ SAFRA

### Comparação com Target
```
DT_SAFRA (cliente-mês):           2024-06-01
↑
Pagamento M1:                      [2024-05-01, 2024-06-01)
Atraso M1 (snapshot):              2024-05-01 (fotografia)
↓
~60 dias depois, mede FPD_INT:     2024-08-01
→ Gap de 60 dias (seguro!)
```

---

## Próximos Passos

### Fase 1: Build + Validate (AGORA)
- [ ] Criar `05_gold_abt_v6_builder.py`
- [ ] Rodar agregações (Pagamento + Atraso M1/M3/M6)
- [ ] Validar todos 14 gates
- [ ] Medir KS final (target: 44-45%)

### Fase 2: Variable Book (Após v6 ✅)
- [ ] Dicionário técnico (36 features novas)
- [ ] Distribuições e estatísticas (missing %, ranges)
- [ ] KS individual por variável
- [ ] Lineage e transformações
- [ ] Recomendações para modelo

### Fase 3: Entrega (Final)
- **Artefatos:**
  1. `abt_v1` a `abt_v6` (7 tabelas para cientistas)
  2. `variable_book.md` (documentação completa)
  3. Notebooks EDA (distribuições, correlações)

---

## Referências

- `docs/target_definition.md` — Anchor event, labels, anti-leakage
- `docs/03_silver_rules/pagamento.md` — Transformações Silver Pagamento
- `docs/03_silver_rules/atraso.md` — Transformações Silver Atraso
- `docs/04_gold_rules/abt_v5.md` — ABT v5 (base para v6)
- `src/jobs/02_gold/04_gold_abt_v5_builder.py` — Padrão para v6

---

**Próximo:** Script `05_gold_abt_v6_builder.py` ⚡
