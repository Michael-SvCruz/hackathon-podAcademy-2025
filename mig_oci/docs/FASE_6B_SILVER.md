# Fase 6B — Silver Layer (Bronze → Silver)

> **Status:** ✅ Scripts prontos. `silver_recarga` testado e funcionando (9m44s). Demais aguardando execução e calibração de `BYTES_PER_ROW_ESTIMATE`.

---

## Visão Geral

A camada Silver transforma os dados Bronze (metadados + raw) em dados tipados, validados e padronizados. Cada fonte tem particularidades distintas de dedup, schema e sentinelas.

```
Bronze Layer (6 fontes Delta)
    │
    ├── silver_bureau.py    ─┐
    ├── silver_telco.py      │
    ├── silver_cadastro.py   ├─ Silver Layer (6 scripts) ✅ Prontos
    ├── silver_recarga.py    │  silver_recarga ✅ Testado (9m44s)
    ├── silver_pagamento.py  │  silver_pagamento ✅ Testado (12m54s)
    └── silver_atraso.py    ─┘
```

---

## Padrão dos Scripts (opt_z — Principal)

Todos os scripts Silver seguem o padrão **opt_z**: sem cache, coalesce dinâmico, quality check no arquivo já gravado.

### Arquitetura (3 actions)

```
action 1: count()              → calcula num_output_files (coalesce dinâmico)
action 2: coalesce(N).write()  → grava Silver em Delta
action 3: agg(Silver escrita)  → quality check no arquivo real
```

### Fórmula do Coalesce Dinâmico

```python
estimated_size_mb = count_pre * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
num_output_files  = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
```

**Calibração após 1º run:**
```
BYTES_PER_ROW_ESTIMATE = tamanho_real_MB * 1024 * 1024 / count_registros
```

---

## Scripts e Particularidades por Fonte

### silver_bureau.py — Spine Oficial

| Item | Detalhe |
|------|---------|
| **Grain** | 1:1 — 1 linha por NUM_CPF + SAFRA |
| **Dedup** | `dropDuplicates(["num_cpf", "safra"])` |
| **Sentinela** | SCORE_01 = 0 → `flag_score01_missing` + `score_01_adj` (NULL) |
| **BYTES_PER_ROW** | 80 (estimativa inicial — schema enxuto, spine) |
| **Coverage esperada** | SCORE_01: ~98.18% \| SCORE_02: ~99.95% |
| **appName** | `silver_bureau_optz` |

### silver_telco.py

| Item | Detalhe |
|------|---------|
| **Grain** | 1:1 — confirmado em EDA |
| **Dedup** | `dropDuplicates(["num_cpf", "safra"])` |
| **Sentinela** | Código 304 em variáveis dimensionais → `flag_*_sentinela` |
| **BYTES_PER_ROW** | 200 (schema largo: 68 var_* → ~136 colunas derivadas) |
| **Coverage esperada** | FPD nulo: ~3.36% |
| **appName** | `silver_telco_optz` |

### silver_cadastro.py

| Item | Detalhe |
|------|---------|
| **Grain** | 1:1 — confirmado em EDA |
| **Dedup** | `dropDuplicates(["num_cpf", "safra"])` |
| **Crítico** | `F.coalesce(F.to_date(...))` com 4 formatos — **NUNCA Python UDF** (fix Jan/2026) |
| **BYTES_PER_ROW** | 150 (schema intermediário) |
| **Coverage esperada** | `idade_anos`: ~99.57% (pós fix UDF → F.to_date) |
| **appName** | `silver_cadastro_optz` |

### silver_recarga.py ✅ Testado

| Item | Detalhe |
|------|---------|
| **Grain** | 1:1 — 1 evento por NUM_CPF + SAFRA_RECARGA + DW_NUM_NTC |
| **Dedup** | `dropDuplicates(["num_cpf", "safra_recarga", "dw_num_ntc"])` |
| **Sentinela** | -1, -2, -3 em colunas dimensionais → `flag_*_sentinela` |
| **BYTES_PER_ROW** | **40** ✅ Calibrado após 1º run |
| **Tempo de execução** | **9m44s** (opt_z vencedor vs opt_optimize) |
| **appName** | `silver_recarga_optz` |

### silver_pagamento.py ✅ Testado

| Item | Detalhe |
|------|---------|
| **Grain** | N:1 — múltiplas faturas/itens por CPF no mês |
| **Dedup** | `Window + row_number()` — critério de negócio: `ts_status_fatura DESC` |
| **BYTES_PER_ROW** | **85** ✅ Calibrado após 1º run |
| **Tempo de execução** | **12m54s** (opt_z) |
| **appName** | `silver_pagamento_optz` |

> **Nota:** Pagamento usa `Window+row_number` em vez de `dropDuplicates` porque há critério de negócio para desempate (fatura mais recente prevalece). É a única Silver com esse padrão.

### silver_atraso.py

| Item | Detalhe |
|------|---------|
| **Grain** | **Múltiplo por design** — várias faturas/itens por CPF (snapshot mensal) |
| **Dedup** | **NENHUM** — remover linhas = perder sinal de risco |
| **BYTES_PER_ROW** | 120 (estimativa inicial — grain múltiplo, mais linhas/CPF) |
| **Quality diferenciado** | `countDistinct("num_cpf")` + "média linhas por CPF" |
| **appName** | `silver_atraso_optz` |

---

## Estrutura de Arquivos

```
mig_oci/data_upload/scripts/
├── silver_bureau.py          ← principal (opt_z)
├── silver_telco.py           ← principal (opt_z)
├── silver_cadastro.py        ← principal (opt_z)
├── silver_recarga.py         ← principal (opt_z) ✅ Testado
├── silver_pagamento.py       ← principal (opt_z) ✅ Testado
├── silver_atraso.py          ← principal (opt_z)
└── opc_standby/
    ├── silver_bureau_original.py          ← pré-otimização (referência)
    ├── silver_bureau_opt_z.py             ← cópia da versão promovida
    ├── silver_bureau_opt_optimize.py      ← alternativa com OPTIMIZE + VACUUM
    ├── silver_telco_original.py
    ├── silver_telco_opt_z.py
    ├── silver_telco_opt_optimize.py
    ├── silver_cadastro_original.py
    ├── silver_cadastro_opt_z.py
    ├── silver_cadastro_opt_optimize.py
    ├── silver_recarga_original.py
    ├── silver_recarga_opt_z.py
    ├── silver_recarga_opt_optimize.py
    ├── silver_pagamento_original.py
    ├── silver_pagamento_opt_z.py
    ├── silver_pagamento_opt_optimize.py
    ├── silver_atraso_original.py
    ├── silver_atraso_opt_z.py
    └── silver_atraso_opt_optimize.py
```

---

## Versões Alternativas (opc_standby)

### opt_optimize (standby para fontes com volume alto ou BYTES difícil de estimar)

Arquitetura com 2 actions + OPTIMIZE:
```
action 1: coalesce(INITIAL_COALESCE).write()  → grava Silver (sem calibrar BYTES)
OPTIMIZE (targetFileSize=128MB)               → reorganiza para ~128MB por arquivo
action 2: agg(Silver pós-OPTIMIZE)           → quality check
```

**Vantagem:** Não precisa calibrar `BYTES_PER_ROW_ESTIMATE` — OPTIMIZE garante ~128MB.
**Desvantagem:** Passo extra (OPTIMIZE leva alguns minutos adicionais).

> **Atenção:** Sempre setar `spark.conf.set("spark.databricks.delta.targetFileSize", str(128*1024*1024))` antes do `executeCompaction()`. O default do Delta open-source é 1GB.

---

## Convenção de Nomes

| Item | Padrão |
|------|--------|
| **appName (principal)** | `silver_X_optz` |
| **appName (original)** | `silver_X_original` |
| **appName (opt_optimize)** | `silver_X_optimize` |
| **Output path** | `oci://hackathon-2025-silver-layer@{namespace}/X/` |

O `appName` aparece nos logs do Data Flow Console → permite identificar imediatamente qual versão rodou em cada execução.

---

## Estratégia de Dedup (resumo)

| Fonte | Estratégia | Motivo |
|-------|-----------|--------|
| bureau | `dropDuplicates` | Grain 1:1 confirmado em EDA — spine oficial |
| telco | `dropDuplicates` | Grain 1:1 confirmado em EDA |
| cadastro | `dropDuplicates` | Grain 1:1 confirmado em EDA |
| recarga | `dropDuplicates` | Grain 1:1 por CPF + SAFRA + NTC |
| pagamento | `Window + row_number` | Critério de negócio: fatura mais recente (`ts_status_fatura DESC`) |
| atraso | **Nenhum** | Grain múltiplo por design — cada linha é sinal de risco |

---

## Próximos Passos

1. Executar `silver_bureau`, `silver_telco`, `silver_cadastro`, `silver_atraso` no Data Flow
2. Calibrar `BYTES_PER_ROW_ESTIMATE` após 1ª execução de cada uma
3. Ajustar constante e re-executar se necessário

---

## Lições Aprendidas

| Problema | Solução |
|----------|---------|
| `executeCompaction()` gera ~1GB em vez de ~128MB | Setar `spark.databricks.delta.targetFileSize` para `128*1024*1024` antes do OPTIMIZE |
| `mode("overwrite")` acumula ghost files | Executar `VACUUM(0h)` após overwrite (setar `retentionDurationCheck.enabled=false`) |
| `BYTES_PER_ROW_ESTIMATE` muito baixo → arquivo grande | Calibrar após 1º run: `BYTES = total_MB * 1024*1024 / count` |
| Python UDF falha silenciosamente no cadastro | Usar `F.coalesce(F.to_date(...))` com múltiplos formatos (fix Jan/2026) |
| Dedup Window no pagamento gera shuffle extra | Aceito: é necessário para critério de negócio (fatura mais recente) |
