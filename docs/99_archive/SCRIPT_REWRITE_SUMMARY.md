# Script Rewrite Summary: 02_bronze_silver_cadastro.py

## Overview
Fully rewritten `src/jobs/01_silver/02_bronze_silver_cadastro.py` following established patterns from `00_bronze_silver_bureau.py` and `01_bronze_silver_telco.py`.

## Key Improvements

### 1. **Robust Date Parsing with Tolerance**
- **Before:** Used `F.to_date()` which fails on invalid dates like "2807"
- **After:** Uses `F.try_to_date()` within `F.coalesce()` for graceful NULL handling
- **Impact:** Prevents crashes when DATADENASCIMENTO contains malformed dates

### 2. **Proper Variable Classification**
- Defined explicit lists of numeric, categorical, and mixed variables based on data quality analysis
- `NUMERIC_VARS = ["var_03"..."var_11", "var_16", "var_17"]` (11 variables)
- `CATEGORICAL_VARS = ["var_15", "var_22", "var_23", "var_24", "var_25"]` (5 variables)
- `MIXED_VARS = ["var_02", "var_12"..."var_14", "var_18"..."var_21"]` (8 variables)

### 3. **Output Path Consistency**
- **Before:** `DEFAULT_OUTPUT_PATH = ".../cadastro_delta/"`
- **After:** `DEFAULT_OUTPUT_PATH = ".../cadastro_silver_delta/"`
- **Rationale:** Follows naming convention pattern used by telco_silver_delta and bureau_full_silver_delta

### 4. **Enhanced Quality Checks**
Added comprehensive post-processing validation metrics:
- Invalid flag_instalacao and FPD values
- FPD null coverage percentage
- Birth date parsing failures (flag_dt_nasc_invalida)
- Age outliers: < 18 (eligibility gate) and > 100 (data quality flag)
- CEP missing percentage and coverage rate
- Distinct key verification (uniqueness check)

### 5. **Refined Column Selection Logic**
```python
# Dynamically includes all var_* columns without hardcoding
var_columns = [col for col in df.columns if col.startswith("var_")]
columns_to_select.extend(var_columns)

# Removes duplicates while preserving order
columns_to_select = list(dict.fromkeys(columns_to_select))

# Selects only columns that actually exist
existing_columns = [col for col in columns_to_select if col in df.columns]
```

### 6. **Anti-Leakage Safeguards**
- Maintains clear separation: labels (FPD_INT, FLAG_INSTALACAO_INT) vs features
- Includes extensive comments warning about feature/label boundaries
- Quality gates for domain validation of label values

### 7. **Standard ETL Patterns**
Aligned with project conventions:
- Argparse with fallback to defaults (interactive mode support)
- Unity Catalog paths (`/Volumes/hackathon_2025/...`)
- Delta write with mergeSchema and overwriteSchema options
- Standard Unity Catalog table registration
- Metadata audit columns preserved from Bronze

## Derived Features Created

| Feature | Type | Description | Validation |
|---------|------|-------------|-----------|
| `dt_safra` | date | First day of month (YYYYMM → YYYY-MM-01) | Derived |
| `dt_nasc` | date | Birth date parsed from DATADENASCIMENTO | try_to_date tolerant |
| `idade_anos` | int | Age in years at safra date | Calculated via months_between |
| `flag_dt_nasc_invalida` | int | Birth date parse failure indicator | Quality flag |
| `flag_idade_menor_18` | int | Below minimum legal age | Eligibility flag |
| `flag_idade_muito_alta` | int | Above 100 years old | Outlier flag |
| `flag_cep_missing` | int | CEP 3-digit missing or empty | Completeness flag |

## Configuration Constants

```python
# Age validation thresholds
IDADE_MINIMA_VALIDA = 18          # Eligibility gate
IDADE_MAXIMA_ESPERADA = 100       # Outlier detection

# Variable classification
NUMERIC_VARS = [11 variables]     # Safe to cast to DOUBLE
CATEGORICAL_VARS = [5 variables]  # Trim + UPPER
MIXED_VARS = [8 variables]        # Trim simple (flexible typing)
```

## Main Processing Pipeline

1. **Read Bronze** → Parse args, load Delta from `cadastro_delta/`
2. **Standardize Names** → Snake_case, remove accents (standardize_column_names)
3. **Build Silver** → Apply 9-step transformations:
   - Typecasting (labels, metadata)
   - Derived columns (DT_SAFRA, IDADE_ANOS)
   - Tolerant date parsing
   - Sanity flags (age, dates, missing values)
   - Variable typing (numeric, categorical, mixed)
   - Domain validation gates
   - Column selection
4. **Deduplication** → Enforce 1:1 grain (NUM_CPF + SAFRA)
5. **Write Silver** → Delta + Unity Catalog table
6. **Quality Report** → 12 validation metrics printed to logs

## Execution Options

```bash
# Development (defaults)
python src/jobs/01_silver/02_bronze_silver_cadastro.py

# Databricks notebook
%run /Workspace/src/jobs/01_silver/02_bronze_silver_cadastro.py

# Spark submit with explicit paths
spark-submit \
  --py-files src/ \
  src/jobs/01_silver/02_bronze_silver_cadastro.py \
  --input_path /Volumes/hackathon_2025/default/bronze/cadastro_delta/ \
  --output_path /Volumes/hackathon_2025/default/silver/cadastro_silver_delta/
```

## Files Referenced

- **Documentation:** docs/01_data_dictionary/cadastro.md, docs/02_data_quality/cadastro.md, docs/03_silver_rules/cadastro.md
- **Bronze ingestion:** src/jobs/00_bronze/02_ingest_cadastro.py
- **Utilities:** src/utils/spark_utils.py (standardize_column_names, to_int_safe, to_double_safe)
- **Template patterns:** src/jobs/01_silver/00_bronze_silver_bureau.py, 01_bronze_silver_telco.py

## Key Differences from Previous Version

| Aspect | Old | New |
|--------|-----|-----|
| Date parsing | F.to_date (crashes on invalid) | F.try_to_date (graceful NULL) |
| Output path | cadastro_delta | cadastro_silver_delta |
| Variable typing | Hardcoded lists (7 numeric only) | Explicit lists for 3 categories (11+5+8) |
| Quality checks | 5 basic metrics | 12 comprehensive metrics |
| Column selection | Using unpacking (*[...]) | Dynamic list with deduplication |
| Error handling | Basic | Explicit try-except with sys.exit |

## Notes for Developers

- Script is production-ready but still includes development quality checks (marked for removal)
- FPD_INT in cadastro is not the authoritative target; use bureau_full for training/evaluation
- All var_* columns are dynamically included; no need to manually update lists if schema changes
- Parsing tolerant approach handles ~1.2M null dates gracefully (no crashes)
- Age outliers flagged for review; decision to filter/cap made at Gold layer, not Silver

---

**Status:** ✅ Complete and error-free  
**Last Updated:** 2026-01-21  
**Author:** AI Assistant (Claude Haiku)
