# ABT v4 Specification — Bureau + Scores + Telco + Cadastro

**Version:** v4  
**Status:** Ready for Execution  
**Roadmap Position:** Step 4 of 6 (v1 → v2 → v3 → **v4** → v5 → v6)  
**Expected Output:** 3.79M records × 168 columns  
**Expected KS (OOT):** ~36-38 (incremental over v3, +1-2 from Cadastro demographics)

---

## 1. Objective

Extend ABT v3 by adding **Cadastro** (demographic and administrative) features via LEFT JOIN.

### Rationale
- **v1** (Score_01): Baseline predictive model
- **v2** (Score_02): Incremental scoring feature
- **v3** (Telco): Telecom behavioral enrichment (68 features, 20.51% coverage)
- **v4** (Cadastro): Demographic & administrative enrichment (33 features, ~35-40% coverage expected)

Cadastro is an **optional complementary source** (like Telco). When a record lacks Cadastro data, features will be NULL (preserved by LEFT JOIN).

---

## 2. Roadmap & Feature Evolution

| Version | Feature Block | Coverage | KS (Baseline) | Delta vs Prior | Status |
|---------|---------------|----------|---------------|----------------|--------|
| v1      | Score_01      | 98.18%   | 33.1          | —              | ✅ Complete |
| v2      | + Score_02    | 99.95%   | 34.2-35.0     | +1.0-2.0       | ✅ Complete |
| v3      | + Telco (68v) | 20.51%   | 35.5-36.5     | +1.5-2.0       | ✅ Complete |
| **v4**  | **+ Cadastro (33v)** | **~35-40%** | **36.0-38.0** | **+0.5-1.5** | 🟡 Ready |
| v5      | + Recarga     | TBD      | TBD           | TBD            | ⏳ Planned |
| v6      | + Pagamento + Atraso | TBD | TBD | TBD | ⏳ Planned |

---

## 3. Data Anchor & Temporal Rules

### Anchor Event (Unchanged from v1)
- **Unit of Analysis:** `NUM_CPF + SAFRA` (1:1 grain, no duplicates)
- **Reference Date:** `DT_SAFRA = first day of month` (derived from SAFRA = YYYYMM format)
- **Grain:** **1 row per CPF per monthly cohort**

### Observability Window (Unchanged)
- **Feature Window:** Historical (`DT_SAFRA` - N months backward)
- **Label Window:** Forward-looking (`DT_SAFRA` + 30-90 days)

---

## 4. Schema Definition (v4 Final)

### 4.1) Identifier & Temporal Columns
| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `num_cpf` | string | Bureau | Client identifier (hash/obfuscated) |
| `safra` | string | Bureau | Monthly cohort (YYYYMM format) |
| `dt_safra` | date | Bureau | Derived: first day of month (SAFRA) |

### 4.2) Labels (Audit Only, Never Features)
| Column | Type | Source | Values | Notes |
|--------|------|--------|--------|-------|
| `fpd_int` | int | Bureau | 0, 1, NULL | First Payment Default (observed only when FLAG_INSTALACAO=1) |
| `flag_instalacao_int` | int | Bureau | 0, 1 | Approval decision (0=rejected, 1=approved) |

### 4.3) Baseline Features (v1-v2)

#### v1: Score_01 (1 feature + 1 flag)
| Column | Type | Source | Coverage | Notes |
|--------|------|--------|----------|-------|
| `score_01_adj` | double | Bureau | 98.18% | Risk score (adjusted, sentinela 0→NULL) |
| `flag_score01_missing` | int | Bureau | 100% | Binary missing indicator |

#### v2: Score_02 (1 feature + 1 flag)
| Column | Type | Source | Coverage | Notes |
|--------|------|--------|----------|-------|
| `score_02_adj` | double | Bureau | 99.95% | Risk score (adjusted in v4 builder, sentinela 0→NULL) |
| `flag_score02_missing` | int | Bureau | 100% | Binary missing indicator |

### 4.4) Enrichment Features (v3-v4)

#### v3: Telco Features (68 features + 68 flags = 136 columns)
| Feature Group | Count | Coverage | Source | Notes |
|---------------|-------|----------|--------|-------|
| `var_26_adj` to `var_93_adj` | 68 | 20.51% | Telco Silver | Anonimized telecom variables |
| `flag_var_26_missing` to `flag_var_93_missing` | 68 | 100% | Telco Silver | Missing indicators per variable |

**Telco Coverage Justification:** 20.51% is realistic for complementary source. ~1.3M Telco records match ~3.79M Bureau spine (~36% match rate). Threshold set to 20% (realistic minimum for optional enrichment).

#### v4 (NEW): Cadastro Features (33 features + flags)

##### Demographic (6 features + 4 flags)
| Column | Type | Coverage | Source | Notes |
|--------|------|----------|--------|-------|
| `idade_anos` | int | 35-40% | Cadastro | Age in years (derived from birth date) |
| `flag_idade_outlier` | int | 100% | Cadastro | Age < 14 or > 100 (sanity check) |
| `flag_dt_nasc_invalid` | int | 100% | Cadastro | Birth date parsing failed |
| `cep_3_digitos` | string | 35-40% | Cadastro | First 3 digits of postal code (geographic proxy) |
| `flag_cep3_missing` | int | 100% | Cadastro | Postal code missing |

##### Administrative Status (3 features)
| Column | Type | Coverage | Source | Notes |
|--------|------|----------|--------|-------|
| `statusrf` | string | 35-40% | Cadastro | Registry status (REGULAR, PENDENTE, SUSPENSA, CANCELADA, FALECIDO, etc.) |
| `prod` | string | 35-40% | Cadastro | Product (CMV, DTH, NET) |
| `flag_mig2` | string | 35-40% | Cadastro | Migration status (PRE, FLEX, Aquisição, null) |

##### Numeric Variables (13 features + 1 flag)
| Column | Type | Coverage | Source | Notes |
|--------|------|----------|--------|-------|
| `var_03` to `var_11` | double | 35-40% | Cadastro | Numeric cadastral attributes |
| `flag_var_11_neg` | int | 100% | Cadastro | var_11 < 0 (sanity check) |
| `var_16`, `var_17` | double | 35-40% | Cadastro | Additional numeric attributes |

##### Mixed/Categorical Variables (5 features)
| Column | Type | Coverage | Source | Notes |
|--------|------|----------|--------|-------|
| `var_15`, `var_22`, `var_23`, `var_24`, `var_25` | string | 35-40% | Cadastro | Categorical/mixed anonimized variables |

##### Date-Derived (1 feature + 1 flag)
| Column | Type | Coverage | Source | Notes |
|--------|------|----------|--------|-------|
| `dt_var_12` | date | 35-40% | Cadastro | Parsed date variable |
| `flag_dt_var_12_invalid` | int | 100% | Cadastro | Date parsing failed |

### 4.5) Metadata (3 columns)
| Column | Type | Value | Notes |
|--------|------|-------|-------|
| `metadata_source` | string | "bureau_full" | Spine source |
| `metadata_build_date` | string | YYYY-MM-DD HH:MM:SS | Build timestamp |
| `metadata_audit_record_count` | int | 3,790,000+ | Record count for audit trail |
| `gold_version` | string | "v4" | ABT version identifier |
| `gold_build_date` | string | YYYY-MM-DD HH:MM:SS | Gold build timestamp |
| `gold_feature_blocks` | string | "Scores + Telco + Cadastro" | Feature composition summary |

---

## 5. Transformation Logic (v4 Builder)

### Step 1: Read Silver Bureau (v3 state)
- **Source:** `/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/`
- **Grain:** NUM_CPF + SAFRA (1:1)
- **Selection:** Keys, labels, v1/v2/v3 features

### Step 2: Read Silver Cadastro
- **Source:** `/Volumes/hackathon_2025/default/silver/cadastro_silver_delta/`
- **Grain:** NUM_CPF + SAFRA (1:1)
- **Selection:** Demographic, status, numeric, categorical, date features

### Step 3: LEFT JOIN on NUM_CPF + SAFRA
- Bureau (spine, 3.79M) LEFT JOINs Cadastro (~1.35M)
- **Preserves grain:** 3.79M rows (LEFT JOIN retains all spine rows)
- **Expected Cadastro coverage:** 35-40% of rows (complementary source)

### Step 4: Treat Score_02 Sentinela (0 → NULL)
- **Rationale:** Score_02 from Silver Bureau is raw; v4 builder standardizes treatment
- **Logic:**
  ```python
  score_02_adj = CASE WHEN score_02_dbl = 0 THEN NULL ELSE score_02_dbl END
  flag_score02_missing = CASE WHEN score_02_dbl IS NULL OR score_02_dbl = 0 THEN 1 ELSE 0 END
  ```

### Step 5: Column Selection & Ordering
- **Order:** Keys → Labels → Scores (v1+v2) → Telco (v3) → Cadastro (v4) → Metadata
- **Total columns:** 168
  - Keys: 3
  - Labels: 2
  - Scores: 4 (2 scores + 2 flags)
  - Telco: 136 (68 vars + 68 flags)
  - Cadastro: 33 (features + flags + dates)
  - Metadata: 6

### Step 6: Add Gold Metadata
- `gold_version = "v4"`
- `gold_build_date = <timestamp>`
- `gold_feature_blocks = "Scores (v1, v2) + Telco (v3) + Cadastro (v4)"`

---

## 6. Validation Gates (9 Gates)

All gates must **PASS** before output is written to Gold.

| Gate # | Name | Rule | Threshold | Status |
|--------|------|------|-----------|--------|
| 1 | Uniqueness | COUNT(*) = COUNT(DISTINCT NUM_CPF+SAFRA) | 3.79M = 3.79M | ✅ |
| 2 | FPD Anti-Leakage | FPD nulo where FLAG_INSTALACAO=0 | 0 violations | ✅ |
| 3 | Key Integrity | No NULLs in keys | 0 NULLs | ✅ |
| 4 | FLAG Distribution | Both 0 and 1 present | Count > 0 each | ✅ |
| 5 | FPD Distribution | FPD=1 cases exist | Count > 0 | ✅ |
| 6 | Score_01 Coverage | Non-null % | ≥ 95% | ✅ |
| 7 | Score_02 Coverage | Non-null % | ≥ 99% | ✅ |
| 8 | Telco Coverage | Non-null cells % | ≥ 20% | ✅ |
| **9** | **Cadastro Coverage (NEW)** | **Non-null cells %** | **≥ 25%** | **Pending** |

### Gate 9 (NEW): Cadastro Coverage Justification
- **Threshold:** 25% (realistic for optional demographic source)
- **Rationale:**
  - Cadastro is complementary to Telco (different data source)
  - Expected match rate: ~35-40% of Bureau spine
  - 25% threshold allows for data quality variance while detecting major join issues
  - Higher than Telco (20%) due to demographic nature (more clients have registry data)

---

## 7. Anti-Leakage Rules (Enforced in v4)

### Critical Labels (Never Use as Features)
1. **`fpd_int`** — First Payment Default (target)
   - Observability: Only when `flag_instalacao_int = 1`
   - Usage: Label for model training; audit trail only

2. **`flag_instalacao_int`** — Approval Decision
   - Values: 0 (rejected), 1 (approved)
   - Usage: Impact analysis (swap-in/swap-out); never as feature
   - Rationale: Causality (approval happens before feature observation window)

### Derived Features (Safe to Use)
- Age, postal code, status, product, demographics: ✅ Safe
- Score_01, Score_02, Telco variables: ✅ Safe
- Categorical encodings of status: ✅ Safe

---

## 8. Expected Metrics

### Output Dimensions
- **Records:** 3,790,000
- **Columns:** 168
- **Feature Count:** 168 - 3 (keys) - 2 (labels) - 6 (metadata) = **157 features**
  - Scores: 4 features (2 scores + 2 flags)
  - Telco: 136 features (68 vars + 68 flags)
  - Cadastro: 17 features (various)

### Coverage Expectations
| Feature Block | Expected Coverage |
|---------------|-------------------|
| Score_01      | 98.18%            |
| Score_02      | 99.95%            |
| Telco         | 20.51%            |
| Cadastro      | 35-40% (new)      |

### Projected KS (Out-of-Time)
- **v3 OOT KS:** ~35.5-36.5
- **v4 OOT KS (estimated):** ~36.0-38.0 (incrementally improving)
- **Justification:** Demographic variables (age, status, postal code) have moderate predictive power in credit risk

---

## 9. Usage Example

### Databricks Notebook
```python
%run /Workspace/src/jobs/02_gold/03_gold_abt_v4_builder.py
```

### Command Line (with arguments)
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/03_gold_abt_v4_builder.py \
  --input_bureau_path /Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/ \
  --input_telco_path /Volumes/hackathon_2025/default/silver/telco_silver_delta/ \
  --input_cadastro_path /Volumes/hackathon_2025/default/silver/cadastro_silver_delta/ \
  --output_path /Volumes/hackathon_2025/default/gold/abt_v4_delta/
```

### Expected Output
```
================================================================================
ABT v4 BUILDER — Bureau + Scores + Telco + Cadastro
================================================================================

[Step 1] Reading Silver Bureau v3 (spine with v1/v2/v3 features)...
  ✓ Bureau prepared: 3,790,000 records

[Step 2] Reading Silver Cadastro...
  ✓ Cadastro prepared: 1,350,000 records

[Step 3] LEFT JOINing Bureau (spine) with Cadastro...
  ✓ ABT after JOIN: 3,790,000 records (left join preserves grain)

[Step 4] Treating Score_02 sentinela values (0 → NULL)...
  ✓ Score_02 sentinela treated

[Step 5] Selecting and ordering final columns (168 total)...
  ✓ Selected 168 columns

[Step 6] Adding Gold metadata columns...
  ✓ Gold metadata added (version=v4, build_date=2026-01-22 HH:MM:SS)

================================================================================
VALIDATION PHASE — Running 9 gates
================================================================================

  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...
    ✓ PASS: 3,790,000 == 3,790,000 (sem duplicatas)

  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...
    ✓ PASS: FPD sempre nulo em FLAG_INSTALACAO=0

  ...

  [Gate 9] Verificando cobertura Cadastro (novo em v4)...
    ✓ PASS: Cadastro presente em 35.50% das células

================================================================================
>>> [Validate v4] ✅ TODOS OS 9 GATES PASSARAM!
================================================================================
    Total de registros: 3,790,000
    Registros únicos: 3,790,000
    Cobertura Score_01: 98.18%
    Cobertura Score_02: 99.95%
    Cobertura Telco: 20.51%
    Cobertura Cadastro: 35.50%

================================================================================
WRITING ABT v4 to /Volumes/hackathon_2025/default/gold/abt_v4_delta/
================================================================================

✅ ABT v4 successfully written
   • Records: 3,790,000
   • Columns: 168
```

---

## 10. Incremental Feature Roadmap Progression

```
ABT v1 (Baseline)
├─ Score_01 (98.18% coverage)
└─ KS ~33.1

ABT v2 (Incremental Scoring)
├─ v1 features (Score_01)
├─ Score_02 (99.95% coverage) ← NEW
└─ KS ~34.5 (+1.4 vs v1)

ABT v3 (Behavioral Enrichment)
├─ v2 features (Scores_01+02)
├─ Telco var_26-93 (20.51% coverage) ← NEW
└─ KS ~36.0 (+1.5 vs v2)

ABT v4 (Demographic Enrichment) ← YOU ARE HERE
├─ v3 features (Scores + Telco)
├─ Cadastro demographics (35-40% coverage) ← NEW
└─ KS ~37.0 (+1.0 vs v3, estimated)

ABT v5 (Usage Patterns)
├─ v4 features (all above)
├─ Recarga var_? (??% coverage) ← PLANNED
└─ KS ~37.5+ (estimated)

ABT v6 (Payment Behavior)
├─ v5 features (all above)
├─ Pagamento + Atraso var_? (??% coverage) ← PLANNED
└─ KS ~38.0+ (estimated)
```

---

## 11. Next Steps After v4

1. **Execute v4 Builder** — Run `03_gold_abt_v4_builder.py`
2. **Validate Output** — 9 gates must PASS
3. **Compare with v3** — Measure ΔKS (expected +0.5-1.5)
4. **Document Findings** — Record KS improvement, feature importance
5. **Plan v5** — Design Recarga (usage patterns) integration

---

## 12. Checklist for v4 Completion

- [ ] Silver Cadastro exists at `/Volumes/.../silver/cadastro_silver_delta/`
- [ ] v4 builder script created: `03_gold_abt_v4_builder.py`
- [ ] v4 validator added to `validate_abt.py` (9 gates)
- [ ] v4 documentation created: `abt_v4.md`
- [ ] v4 builder runs without errors
- [ ] All 9 gates PASS
- [ ] Output written to Gold
- [ ] README.md updated with v4 status
- [ ] KS evaluation complete (vs v3)
- [ ] v4 ready for production modeling

---

**Status:** Ready for execution  
**Owner:** Hackathon Squad  
**Last Updated:** 2026-01-22
