# ABT v5 Specification — Bureau + Scores + Telco + Cadastro + Recarga

**Version:** v5  
**Status:** Ready for Execution  
**Roadmap Position:** Step 5 of 6 (v1 → v2 → v3 → v4 → **v5** → v6)  
**Expected Output:** 3.79M records × 195+ columns  
**Expected KS (OOT):** ~42.0-42.5 (incremental +1.8-2.3pp from Recarga)

---

## 1. Objective

Extend ABT v4 by adding **Recarga temporal features** via LEFT JOIN and event-level aggregation.

### Rationale

- **v1** (Score_01): Baseline predictive model (KS ~33.1)
- **v2** (Score_02): Incremental scoring (+1.0-2.0pp)
- **v3** (Telco): Behavioral enrichment from mobile signals (+1.5-2.0pp, 20.51% coverage)
- **v4** (Cadastro): Demographic enrichment (+0.5-1.5pp, ~35-40% coverage)
- **v5** (Recarga): Transaction-level credit recharge patterns ← **THIS STEP** (+1.8-2.3pp expected, ~40%+ coverage)

Recarga is a **high-value transactional source** capturing credit purchase behavior—direct signal of active mobile usage and prepay commitment.

---

## 2. Roadmap & Feature Evolution

| Version | Feature Block | Coverage | KS Baseline | Delta | Status |
|---------|---------------|----------|-------------|-------|--------|
| v1      | Score_01      | 98.18%   | 33.1        | —     | ✅ Complete |
| v2      | + Score_02    | 99.95%   | 34.2-35.0   | +1.0-2.0 | ✅ Complete |
| v3      | + Telco (68v) | 20.51%   | 35.5-36.5   | +1.5-2.0 | ✅ Complete |
| v4      | + Cadastro    | ~35-40%  | 36.0-38.0   | +0.5-1.5 | ✅ Complete |
| **v5**  | **+ Recarga (temporal)** | **~40%+** | **42.0-42.5** | **+1.8-2.3** | 🟡 Ready |
| v6      | + Pagamento + Atraso | TBD | TBD | TBD | ⏳ Planned |

---

## 3. Data Anchor & Temporal Rules

### Anchor Event (Unchanged from v1)

- **Unit of Analysis:** `NUM_CPF + SAFRA` (1:1 grain, no duplicates)
- **Reference Date:** `DT_SAFRA = first day of month` (derived from SAFRA = YYYYMM format)
- **Grain:** **1 row per CPF per monthly cohort**

### New: Temporal Windows for Recarga Aggregation

Features are aggregated using **lookback windows** from `DT_SAFRA`:

```
├── M1: 1 month lookback
│   └─ Includes events where DT_RECARGA >= ADD_MONTHS(DT_SAFRA, -1) AND DT_RECARGA < DT_SAFRA
│
├── M3: 3 months lookback
│   └─ Includes events where DT_RECARGA >= ADD_MONTHS(DT_SAFRA, -3) AND DT_RECARGA < DT_SAFRA
│
└── M6: 6 months lookback
    └─ Includes events where DT_RECARGA >= ADD_MONTHS(DT_SAFRA, -6) AND DT_RECARGA < DT_SAFRA
```

**Example:**
- If `DT_SAFRA = 2024-07-01`:
  - **M1 window:** 2024-06-01 to 2024-06-30
  - **M3 window:** 2024-04-01 to 2024-06-30
  - **M6 window:** 2024-01-01 to 2024-06-30

---

## 4. Feature Specifications

### v5 Recarga Features (New Block)

#### Core Temporal Aggregations (for each M1/M3/M6)

| Feature | Type | Definition | Range | Example |
|---------|------|-----------|-------|---------|
| `qtd_recargas_m*` | INT | Count of recharge events | [0, ∞) | 15 (M1), 45 (M3), 120 (M6) |
| `sum_val_real_clean_m*` | DOUBLE | Sum of VAL_REAL (negative filtering) | [0, ∞) | 500.00 (M1) |
| `sum_val_bonus_clean_m*` | DOUBLE | Sum of VAL_BONUS (negative filtering) | [0, ∞) | 50.00 (M1) |
| `sum_val_credito_inserido_clean_m*` | DOUBLE | Sum of VAL_CREDITO_INSERIDO | [0, ∞) | 550.00 (M1) |
| `avg_val_real_clean_m*` | DOUBLE | Average of VAL_REAL_CLEAN | [0, ∞) | 33.33 (M1) |
| `flag_teve_sos_m*` | INT | Indicator if SOS event occurred (binary) | {0, 1} | 1 (has SOS) |

**Total new features:** 6 × 3 periods = **18 temporal features**

#### Optional: Dimensionality by Good Codes

Only **4 dimensionally-good codes** (0% sentinelas per quality report) will be used for granular features:

```
✅ cod_tipo_credito        (0.00% sentinelas) — credit type dimension
✅ cod_status_plataforma    (0.03% sentinelas) — platform status dimension
✅ cod_tecnologia_dw        (0.00% sentinelas) — technology dimension
✅ cod_plataforma_atu       (0.00% sentinelas) — platform update dimension
```

**Optional features** (if providing significant KS lift):
- `qtd_recargas_por_tipo_credito_m1` (pivot-like: quantity by credit type)
- `sum_val_real_clean_por_status_m3` (pivot-like: amount by status)

**Excluded codes** (90%+ sentinelas—completely unusable):
- ❌ `dw_forma_pagamento` (99.04% sentinelas)
- ❌ `cod_promocao` (99.04% sentinelas)
- ❌ `dw_tipo_recarga` (94.29% sentinelas)
- ❌ `dw_tipo_insercao` (94.29% sentinelas)
- ❌ `dw_plano_tarifacao` (~95% sentinelas)

---

## 5. Join Strategy & Data Integration

### Input Tables

| Table | Grain | Rows | Role | Join Key |
|-------|-------|------|------|----------|
| `gold_abt_v4` | client-month | 3.79M | Spine (1:1) | NUM_CPF + SAFRA |
| `silver_recarga` | event-level | 95.2M | Transactional | NUM_CPF + SAFRA_RECARGA (N:1) |

### Join Logic

```sql
SELECT 
  v4.*,
  COALESCE(rec.qtd_recargas_m1, 0) AS qtd_recargas_m1,
  ...
FROM gold_abt_v4 v4
LEFT JOIN (
  SELECT 
    num_cpf, 
    safra,
    COUNT(*) AS qtd_recargas_m1,
    SUM(val_real_clean) AS sum_val_real_clean_m1,
    ...
  FROM silver_recarga
  WHERE DT_RECARGA >= ADD_MONTHS(DT_SAFRA, -1)
    AND DT_RECARGA < DT_SAFRA
  GROUP BY num_cpf, safra
) rec
ON v4.num_cpf = rec.num_cpf AND v4.safra = rec.safra
```

**Join Type:** `LEFT` (v4 is spine; clients without recharge events get 0/NULL)

**Aggregation:** Event-to-client-month (N:1 reduction)

**Temporal Alignment:** SAFRA_RECARGA (from Silver) maps to DT_SAFRA lookback windows

---

## 6. Data Quality Rules (New for v5)

### Recarga-Specific Validations

| Rule | Gate | Threshold | Action |
|------|------|-----------|--------|
| **Temporal coverage** | Gate 9 | ≥5% of clients have ≥1 recharge | WARN if <5% (highly unusual) |
| **Quantity sanity** | Gate 10 | QTD_RECARGAS_M1 ∈ [0, ∞), no NaNs/Infs | FAIL if found |
| **Value positivity** | N/A | SUM_VAL_*_CLEAN ≥ 0 (by construction) | Guaranteed by `*_clean` filtering |
| **SOS flag binary** | N/A | FLAG_TEVE_SOS_M1/M3/M6 ∈ {0, 1} | Guaranteed by aggregation logic |

### Negative Value Handling (Inherited from Silver)

- **VAL_REAL_CLEAN**: NULLs if original < 0; aggregate with `SUM(COALESCE(..., 0))`
- **VAL_BONUS_CLEAN**: Same as VAL_REAL_CLEAN
- **VAL_CREDITO_INSERIDO**: No negatives observed in Silver quality report

---

## 7. Expected Data Distribution

### Coverage Estimates

- **Recarga events linked to clients:** ~40-50% (not all clients have active recharge history)
- **Clients with M1 recharge (qtd_recargas_m1 > 0):** ~35-40%
- **Clients with M3 recharge (qtd_recargas_m3 > 0):** ~60-70%
- **Clients with M6 recharge (qtd_recargas_m6 > 0):** ~80-90%

### Quantity Distribution (Expected)

```
QTD_RECARGAS_M1:
  Mean: ~8-12 events per month
  Median: ~5-8 events per month
  P75: ~15-20 events per month
  Max: Could be 30+ for heavy users

SUM_VAL_REAL_CLEAN_M1:
  Mean: ~150-300 (BRL)
  Median: ~100-200 (BRL)
  P75: ~400-600 (BRL)
  Max: Could be 1000+ for high-value users
```

---

## 8. Validation Gates (10 Total)

### Previous Gates (Maintained from v4)

1. **Gate 1:** Uniqueness — 1:1 per NUM_CPF + SAFRA (no duplicates)
2. **Gate 2:** Anti-leakage — FPD observed only in FLAG_INSTALACAO=1
3. **Gate 3:** Key integrity — No NULLs in NUM_CPF or SAFRA
4. **Gate 4:** Label distribution — Both FLAG_INSTALACAO=0 and =1 present
5. **Gate 5:** Score_01 coverage ≥ 90%
6. **Gate 6:** Score_02 coverage ≥ 40%
7. **Gate 7:** Telco coverage ≥ 20%
8. **Gate 8:** Cadastro coverage ≥ 20%

### New Gates (v5)

9. **Gate 9 (NEW):** Recarga coverage ≥ 5% (clientes with qtd_recargas_m1 > 0)
   - Threshold lower than other features because not all clients use prepay recharge
   
10. **Gate 10 (NEW):** QTD_RECARGAS_M1 sanity
    - No NaN or Inf values
    - Min ≥ 0, sensible max (< 1M)

---

## 9. Schema & Column Order

### Column Naming Convention

```
Inherited from v4:
├── num_cpf (STRING)
├── safra (STRING, YYYYMM)
├── dt_safra (DATE)
├── flag_instalacao_int, fpd_int
├── score_01_adj, score_02_adj
├── var_26_adj...var_93_adj (Telco, 68 cols)
├── var_02_adj...var_25_adj (Cadastro, 24 cols)
├── prod, flag_mig2
├── metadata_*, gold_version, gold_build_date, gold_feature_blocks
│
New in v5:
├── qtd_recargas_m1, qtd_recargas_m3, qtd_recargas_m6
├── sum_val_real_clean_m1, sum_val_real_clean_m3, sum_val_real_clean_m6
├── sum_val_bonus_clean_m1, sum_val_bonus_clean_m3, sum_val_bonus_clean_m6
├── sum_val_credito_inserido_clean_m1, sum_val_credito_inserido_clean_m3, sum_val_credito_inserido_clean_m6
├── avg_val_real_clean_m1, avg_val_real_clean_m3, avg_val_real_clean_m6
└── flag_teve_sos_m1, flag_teve_sos_m3, flag_teve_sos_m6
```

**Total columns:** ~195 (3 metadata + 2 labels + 2 scores + 68 Telco + 24 Cadastro + 18 Recarga + 10 support)

---

## 10. Anti-Leakage Rules

### Critical (Must Enforce)

- ❌ **FPD_INT:** LABEL only—never as feature. Use only for:
  - Labeling training data
  - Computing model metrics
  - Audit/traceability
  
- ❌ **FLAG_INSTALACAO_INT:** LABEL/Decision variable—never as feature. Use only for:
  - Filtering train/eval sets (FPD observability rule)
  - Impact analysis (swap-in/swap-out scenarios)
  - Audit/traceability

### Recarga-Specific (Safe by Design)

- ✅ **Temporal separation:** Recarga aggregations use **lookback windows** (past relative to DT_SAFRA)
  - No forward leakage possible
  - Label observation window (FPD default 30-90 days) **after** DT_SAFRA is independent
  
- ✅ **Event-level dedupe in Silver:** All recharge events deduplicated by EVENT_KEY (SHA2)
  - No double-counting

---

## 11. Execution & Deployment

### Script: `src/jobs/02_gold/04_gold_abt_v5_builder.py`

**Inputs:**
- Gold ABT v4: `/Volumes/hackathon_2025/default/gold/abt_v4_delta/`
- Silver Recarga: `/Volumes/hackathon_2025/default/silver/recarga_silver_delta/`

**Outputs:**
- Delta table: `/Volumes/hackathon_2025/default/gold/abt_v5_delta/`
- UC table: `hackathon_2025.default.gold_abt_v5`

**Execution (Databricks):**
```bash
%run /Workspace/src/jobs/02_gold/04_gold_abt_v5_builder.py
```

**Execution (Command Line):**
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/04_gold_abt_v5_builder.py \
  --gold_v4_path /Volumes/.../gold/abt_v4_delta/ \
  --silver_recarga_path /Volumes/.../silver/recarga_silver_delta/ \
  --output_path /Volumes/.../gold/abt_v5_delta/
```

---

## 12. Success Criteria

| Criterion | Target | Status |
|-----------|--------|--------|
| Output table created | `gold_abt_v5` | ⏳ Pending |
| Rows preserved | ~3.79M (same as v4) | ⏳ Pending |
| Columns added | 18 Recarga features | ⏳ Pending |
| All 10 gates pass | 100% (9 from v4 + 2 new) | ⏳ Pending |
| KS improvement | +1.8-2.3pp (40.2% → 42.0%+) | 📊 To measure post-execution |
| No NaNs/Infs | True | ⏳ Gate 10 validates |
| Recarga coverage ≥5% | True | ⏳ Gate 9 validates |

---

## 13. Next Steps (v6)

After v5 validation and KS confirmation:

1. **Build Silver Pagamento** (payment events)
2. **Build Silver Atraso** (overdue/delinquency events)
3. **Build Gold v6:** Combine v5 + Pagamento + Atraso (temporal aggregations + delinquency flags)
4. **Target KS:** 45.0% (from current baseline 33.1%, +20pp cumulative)

---

## 14. References

- **Quality Report:** [docs/05_recarga_silver_quality_report.md](../05_recarga_silver_quality_report.md)
- **Silver Recarga Script:** [src/jobs/01_silver/03_bronze_silver_recarga.py](../../01_silver/03_bronze_silver_recarga.py)
- **Gold Builder Script:** [src/jobs/02_gold/04_gold_abt_v5_builder.py](../04_gold_abt_v5_builder.py)
- **Validators:** [src/jobs/02_gold/validators/validate_abt.py](../validators/validate_abt.py)
- **Target Definition:** [docs/target_definition.md](../target_definition.md)

---

**Document Version:** 1.0  
**Date:** January 22, 2026  
**Status:** 🟡 Ready for Execution
