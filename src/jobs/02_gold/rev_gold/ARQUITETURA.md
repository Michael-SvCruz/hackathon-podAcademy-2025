# 🏗️ ARQUITETURA: rev_gold v1

**Data:** 27 de janeiro de 2026  
**Versão:** v1.0

---

## 📐 Diagrama Geral

```
LANDING (Raw Parquet)
  └─ atraso_landing/ (faturas)
  └─ pagamento_landing/ (transações)
       ↓
BRONZE (Ingest + Metadata)
  └─ atraso_delta/
  └─ pagamento_delta/
       ↓
SILVER (Type Cast + Standardize)
  ├─ 05_bronze_silver_atraso.py
  │  └─ atraso_silver_delta/ ✅ (pronto)
  │
  └─ 04_bronze_silver_pagamento.py
     └─ pagamento_silver_delta/ ✅ (pronto)
          ↓
GOLD (rev_gold - ALTERNATIVA)
  └─ 00_gold_abt_v1_base.py ✅ (NOVO - este)
     ├─ Agrega Atraso (12 features)
     ├─ Agrega Pagamento (8 features)
     ├─ Cria Derivadas (3 features)
     ├─ Valida (4 gates)
     └─ Escreve:
        ├─ Delta: /abt_v1_rev_delta/
        └─ Table: gold_abt_v1_rev (UC)
             ↓
MODELO
  └─ LGBMClassifier (prox. fase)
     ├─ Train: abt_v1_rev (23 features)
     ├─ Medir: KS (esperado 40-42%)
     └─ Comparar: vs v1_orig (33.1%)
```

---

## 🔄 Fluxo de Dados (v1 rev_gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                    ENTRADA (Silver)                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  atraso_silver_delta/          pagamento_silver_delta/         │
│  ├─ num_cpf (string)           ├─ num_cpf (string)            │
│  ├─ safra_atraso (YYYYMM)      ├─ safra_pagamento (YYYYMM)    │
│  ├─ [12 colunas raw]           └─ [15 colunas raw]            │
│  └─ [flags sentinela]                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
           ↓                              ↓
    AGREGAÇÃO 1                   AGREGAÇÃO 2
    ├─ Dedup por                  ├─ Dedup por
    │  CPF+SAFRA                  │  CPF+SAFRA
    └─ Seleção 12 feat            └─ Seleção 8 feat
           ↓                              ↓
    atraso_agg (12f)            pagto_agg (8f)
           └──────────────┬───────────────┘
                          ↓
           ┌──────────────────────────┐
           │  JOIN: CPF + SAFRA       │
           │  Tipo: LEFT (manter atr) │
           └──────────────────────────┘
                      ↓
    ┌────────────────────────────────────┐
    │  FEATURE ENGINEERING               │
    ├────────────────────────────────────┤
    │  ✅ delinquency_rate               │
    │  ✅ risk_score_delinquency         │
    │  ✅ flag_cliente_em_risco          │
    └────────────────────────────────────┘
                      ↓
    ┌────────────────────────────────────┐
    │  PREENCHIMENTO NULLs               │
    │  • Somas → 0                       │
    │  • Flags → 0 (safe default)        │
    │  • Preserve missing flags          │
    └────────────────────────────────────┘
                      ↓
    ┌────────────────────────────────────┐
    │  VALIDAÇÕES (4 Gates)              │
    ├────────────────────────────────────┤
    │  ✅ Gate 1: Grain 1:1              │
    │  ✅ Gate 2: Zero NULLs chaves      │
    │  ✅ Gate 3: Distribuição OK        │
    │  ✅ Gate 4: Completude >70%        │
    └────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SAÍDA (Gold)                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Delta Lake: /Volumes/.../gold/rev_abt/abt_v1_rev_delta/      │
│  ├─ Format: Parquet compresso                                 │
│  ├─ Grain: 1:1 NUM_CPF + SAFRA                                │
│  ├─ Registros: ~100K-500K (estimado)                          │
│  ├─ Features: 23 + 6 metadados                                │
│  └─ Tamanho: ~100-500 MB (estimado)                           │
│                                                                 │
│  Unity Catalog: hackathon_2025.rev_gold.gold_abt_v1_rev        │
│  ├─ Queryable como tabela SQL                                 │
│  └─ Documentada com metadados                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🧩 Estrutura de Componentes

```
src/jobs/02_gold/rev_gold/
│
├── __init__.py
│   └─ Módulo Python vazio (permite import)
│
├── 00_gold_abt_v1_base.py ⭐ (MAIN)
│   ├─ Imports: Spark, Pandas, etc
│   ├─ Config: paths, versions
│   ├─ aggregate_atraso()
│   ├─ aggregate_pagamento()
│   ├─ build_abt_v1_base()
│   ├─ validate_abt_v1_rev()
│   └─ main()
│
├── validators/
│   └── __init__.py
│
└── Documentação (7 arquivos MD)
    ├─ README.md (specs técnicas)
    ├─ FEATURE_ENGINEERING_V1.md (design features)
    ├─ IMPLEMENTACAO_V1.md (sumário)
    ├─ FEATURES_QUICK_REFERENCE.md (tabelas)
    ├─ SUMARIO_FINAL.md (executivo)
    ├─ INDEX.md (índice + FAQs)
    ├─ EXECUTIVO.md (stakeholders)
    ├─ CHECKLIST_COMPLETO.md (QA)
    └─ ARQUITETURA.md (este)
```

---

## 🌊 Fluxo de Execução (Sequência)

```
START
  ↓
[1] Ler argumentos (ou defaults)
  ├─ silver_atraso: /Volumes/.../silver/atraso_silver_delta/
  ├─ silver_pagamento: /Volumes/.../silver/pagamento_silver_delta/
  └─ output_path: /Volumes/.../gold/abt_v1_rev_delta/
  ↓
[2] Inicializar Spark
  ├─ Session ID: "Gold_ABT_rev_v1"
  └─ Configurações standard
  ↓
[3] Leitura Silver Atraso
  ├─ Read Delta: atraso_silver_delta/
  ├─ Count: N atraso
  └─ Schema: check
  ↓
[4] Leitura Silver Pagamento
  ├─ Read Delta: pagamento_silver_delta/
  ├─ Count: M pagamento
  └─ Schema: check
  ↓
[5] Agregação Atraso
  ├─ Dedup: 1:1 CPF+SAFRA
  ├─ Select: 12 features
  └─ Output: df_atraso_agg
  ↓
[6] Agregação Pagamento
  ├─ Dedup: row_number by CPF+SAFRA order by TS desc
  ├─ Select: 8 features
  └─ Output: df_pagamento_agg
  ↓
[7] Build ABT v1
  ├─ JOIN: LEFT df_atraso_agg ← df_pagamento_agg on [CPF, SAFRA]
  ├─ Fill NULLs: somas→0, bools→0
  ├─ Derivadas: delinquency_rate, risk_score, flag_risco
  └─ Output: df_abt
  ↓
[8] Validações (4 Gates)
  ├─ Gate 1: Grain 1:1? (assert count == unique_keys)
  ├─ Gate 2: NULL chaves? (assert count=0)
  ├─ Gate 3: Distribuição ok? (report)
  └─ Gate 4: Completude >70%? (report)
  ↓
[9] Escrita Delta Lake
  ├─ Path: output_path
  ├─ Format: Delta
  ├─ Mode: Overwrite
  └─ Options: mergeSchema=true
  ↓
[10] Escrita Unity Catalog
  ├─ Table: hackathon_2025.default.gold_abt_v1_rev
  ├─ Mode: Overwrite
  └─ Options: overwriteSchema=true
  ↓
[11] Relatório Final
  ├─ Count totals
  ├─ Distribuição flags
  ├─ Completude features
  └─ Print report
  ↓
END ✅
```

---

## 🎯 Mapeamento de Features (Input → Output)

```
ATRASO (Silver → Gold)
├─ IND_WO → flag_write_off (0/1)
├─ IND_PDD → flag_pdd (0/1)
├─ IND_ACA → flag_aca (0/1)
├─ DW_FAIXA_AGING_FATURA → atraso_faixa_aging (int)
├─ DW_FAIXA_TEMPO_BASE → atraso_faixa_tempo_base (int)
├─ VAL_FAT_ABERTO → atraso_valor_aberto (double)
├─ VAL_MULTA_JUROS → atraso_valor_multa_juros (double)
└─ FLAGS → flag_*_sentinela (0/1)

PAGAMENTO (Silver → Gold)
├─ VAL_ATUAL_PAGAMENTO → pagto_valor_atual (double)
├─ VAL_ORIGINAL_PAGAMENTO → pagto_valor_original (double)
├─ VAL_PAGAMENTO_FATURA → pagto_valor_fatura (double)
├─ VAL_DESCONTO_ITEM → pagto_desconto_total (double)
├─ VAL_JUROS_POS → pagto_juros_total (double)
├─ IND_STATUS_PAGAMENTO → flag_pagto_pendente (0/1)
├─ FLAG_JUROS_NEG → flag_juros_incidido (0/1)
└─ COD_METODO_PAGAMENTO → cod_metodo_pagto (string)

DERIVADAS (Engineered)
├─ delinquency_rate = (atraso / (atraso+pagto)) * 100
├─ risk_score_delinquency = faixa * rate / 100
└─ flag_cliente_em_risco = write_off OR aca OR atraso>0
```

---

## 🔐 Validações (Gates em Detalhes)

```
GATE 1: GRAIN 1:1
┌──────────────────────────────────────────┐
│ count(*) == count(distinct num_cpf, safra) │
├──────────────────────────────────────────┤
│ Pré-requisito: Nenhum duplicado           │
│ Failure: AssertionError se duplicatas     │
│ Impact: Data quality crítica              │
└──────────────────────────────────────────┘

GATE 2: NENHUM NULL NAS CHAVES
┌──────────────────────────────────────────┐
│ count(where num_cpf=NULL OR safra=NULL)=0 │
├──────────────────────────────────────────┤
│ Pré-requisito: Chaves sempre preenchidas  │
│ Failure: AssertionError se algum NULL     │
│ Impact: JOIN integrity                   │
└──────────────────────────────────────────┘

GATE 3: DISTRIBUIÇÃO SENSATA
┌──────────────────────────────────────────┐
│ count(flag_cliente_em_risco=0/1) % distrib │
├──────────────────────────────────────────┤
│ Esperado: 70% baixo risco, 30% em risco   │
│ Failure: Apenas report (não critical)     │
│ Impact: Sanity check                      │
└──────────────────────────────────────────┘

GATE 4: COMPLETUDE > 70%
┌──────────────────────────────────────────┐
│ count(feature != NULL) / total * 100 > 70% │
├──────────────────────────────────────────┤
│ Esperado: Atraso ~60%, Pagto ~90%         │
│ Failure: Report (missing data ok se flag) │
│ Impact: Feature quality                   │
└──────────────────────────────────────────┘
```

---

## 📊 Schema do Output (ABT v1 rev)

```sql
CREATE TABLE gold_abt_v1_rev (
  -- CHAVES (2)
  num_cpf STRING,                     -- ID cliente
  safra STRING,                       -- YYYYMM
  
  -- ATRASO (10)
  atraso_faixa_aging INT,
  flag_write_off INT,
  flag_pdd INT,
  flag_aca INT,
  atraso_faixa_tempo_base INT,
  atraso_valor_aberto DOUBLE,
  atraso_valor_multa_juros DOUBLE,
  flag_ind_wo_sentinela INT,
  flag_ind_pdd_sentinela INT,
  flag_status_fat_missing INT,
  
  -- PAGAMENTO (8)
  pagto_valor_atual DOUBLE,
  pagto_valor_original DOUBLE,
  pagto_valor_fatura DOUBLE,
  pagto_desconto_total DOUBLE,
  pagto_juros_total DOUBLE,
  flag_pagto_pendente INT,
  flag_juros_incidido INT,
  cod_metodo_pagto STRING,
  
  -- DERIVADAS (3)
  delinquency_rate DOUBLE,
  risk_score_delinquency DOUBLE,
  flag_cliente_em_risco INT,
  
  -- METADADOS (6)
  gold_version STRING,
  gold_build_date TIMESTAMP,
  gold_feature_blocks STRING,
  num_atraso_features INT,
  num_pagamento_features INT,
  num_derivadas INT
)
USING DELTA
LOCATION '/Volumes/hackathon_2025/default/gold/rev_abt/abt_v1_rev_delta/'
```

---

## 🔄 Integração com Próximas Versões

```
v1_rev (Este)
├─ Output: abt_v1_rev_delta/
├─ Features: Atraso (12) + Pagamento (8) + Derivadas (3)
└─ KS Esperado: 40-42%
     ↓
v2 (Recarga) - Template pronto
├─ Input: abt_v1_rev_delta/ + recarga_silver_delta/
├─ JOIN: CPF + SAFRA
├─ Features: +Recarga (18)
└─ KS Esperado: 45-47% (+ 5pp)
     ↓
v3 (Cadastro) - Template pronto
├─ Input: abt_v2_rev_delta/ + cadastro_silver_delta/
├─ JOIN: CPF + SAFRA
├─ Features: +Cadastro (33)
└─ KS Esperado: 46-49% (+ 1-2pp)
     ↓
v4 (Telco) → v5 (Score_01) → v6 (Score_02) → v6.1 (Enhanced)
```

---

## 💾 Armazenamento

```
Disco (Delta Lake)
/Volumes/hackathon_2025/default/gold/rev_abt/abt_v1_rev_delta/
├─ _delta_log/         (transaction log)
├─ part-00000...       (parquet files)
├─ part-00001...
└─ ... (múltiplos partitions)

Tamanho estimado: 100-500 MB (por ~100K-500K registros)
Compressão: Snappy (default Delta)

Metastore (Unity Catalog)
hackathon_2025.rev_gold.gold_abt_v1_rev
├─ Queryable: SELECT * FROM hackathon_2025.rev_gold.gold_abt_v1_rev
├─ Versionado: DESCRIBE HISTORY hackathon_2025.rev_gold.gold_abt_v1_rev
└─ Compartilhado: Acesso entre notebooks/jobs
```

---

## 🎯 Checklist de Validação

Antes de usar v1_rev em produção:

- [ ] Executar script
- [ ] Verificar Gate 1 (grain 1:1)
- [ ] Verificar Gate 2 (null chaves)
- [ ] Revisar Gate 3 (distribuição flag_risco)
- [ ] Revisar Gate 4 (completude)
- [ ] Validar count output
- [ ] Spot check: abrir alguns registros
- [ ] Treinar modelo prototipo
- [ ] Medir KS (esperado 40-42%)
- [ ] Comparar com v1_orig (33.1%)
- [ ] Validar Δ ≈ +7-9pp
- [ ] Prosseguir com v2 se OK

---

**Arquitetura finalizada em 27 de janeiro de 2026**

Próximo: Execução ✅
