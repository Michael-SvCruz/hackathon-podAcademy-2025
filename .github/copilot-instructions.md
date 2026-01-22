# AI Copilot Instructions — Hackathon PodAcademy 2025

This project implements a **credit risk modeling pipeline** using CRISP-DM methodology with incremental feature evaluation. These instructions guide AI agents to be immediately productive.

## 🎯 Project Architecture

### Data Flow (Medallion Architecture)
```
LANDING (Raw Parquet) 
  ↓
BRONZE (Read + Metadata Audit Trail)
  └─ 00_ingest_bureau_full.py → bureau_full_delta/
  └─ 01_ingest_telco.py → telco_delta/
  
SILVER (Type Cast + Validation + Denormalization)
  └─ 00_bronze_silver_bureau.py → bureau_full_silver_delta/
  └─ 01_bronze_silver_telco.py → telco_silver_delta/
  
GOLD (Analytical Base Tables for Modeling)
  └─ 00_gold_abt_builder.py → abt_v*_delta/ (v1 currently implemented)
```

### Core Concept: Incremental Feature Roadmap
The project evaluates KS (Kolmogorov-Smirnov) incrementally:
- **v1** = Score_01 (baseline, KS≈33.1 OOT expected)
- **v2** = + Score_02
- **v3** = + Telco (var_26-93)
- **v4** = + Cadastro
- **v5** = + Recarga  
- **v6** = + Pagamento + Atraso

Each ABT version adds feature blocks; previous features are retained.

## 🔑 Critical Definitions (From [target_definition.md](docs/target_definition.md))

### Anchor Event: Client-Month
- **Unit of analysis:** `NUM_CPF + SAFRA` (1:1 grain, no duplicates)
- **Reference date:** `DT_SAFRA = first day of month`

### Labels (NEVER Features)
| Label | Type | Observability | Validation Gate |
|-------|------|----------------|-----------------|
| `FPD_INT` (0/1) | Target (risk) | Observed **only** when `FLAG_INSTALACAO_INT=1` | Gate 2: Enforce this rule |
| `FLAG_INSTALACAO_INT` (0/1) | Decision (approval) | Always present | Gate 4: Check 0 and 1 values exist |

**CRITICAL Anti-Leakage Rules:**
- `FPD_INT` = inadimplência (first payment default) — train/eval only on `FLAG_INSTALACAO_INT=1`
- `FLAG_INSTALACAO_INT` = approval decision — use only for impact analysis and swap-in/swap-out, never as feature

### Features Pattern (v1 example from [abt_v1.md](docs/04_gold_rules/abt_v1.md))
- `SCORE_01_ADJ` (double) — sentinel 0 → NULL with flag
- `FLAG_SCORE01_MISSING` (int) — capture missing/sentinel values
- Metadata columns included for audit trail only

## 🛠️ Critical Workflows

### ETL Pattern (Bronze/Silver/Gold)
1. **Read phase:** Use `/Volumes/hackathon_2025/default/` paths (Unity Catalog)
2. **Transform phase:** Call reusable functions from `src/utils/spark_utils.py`
3. **Validate phase:** Implement 6-gate validation (see [validate_abt.py](src/jobs/02_gold/validators/validate_abt.py))
4. **Write phase:** Delta Lake format with metadata columns

### Sentinel & Missing Value Handling
Use `treat_sentinel_value()` from [spark_utils.py](src/utils/spark_utils.py):
- Telco: sentinel 304 = "not reported" → NULL
- Score_01: sentinel 0 = missing → NULL
- Always create binary flag (`FLAG_*_MISSING`) to preserve signal

### Column Naming Convention
- Silver layer: standardize to `snake_case` via `standardize_column_names()`
- Remove accents, spaces, special chars automatically
- Gold layer: preserve Silver names + add `gold_*` prefixes for generated metadata

## 📊 Key Files Reference

| File | Purpose | Examples |
|------|---------|----------|
| `src/utils/spark_utils.py` | Reusable Spark utilities | `get_spark_session()`, `to_int_safe()`, `treat_sentinel_value()` |
| `src/jobs/02_gold/validators/validate_abt.py` | 6-gate validation framework | Gates 1-6: uniqueness, FPD rules, nulls, distributions |
| `docs/04_gold_rules/abt_v1.md` | ABT spec + roadmap | Schema, column definitions, anti-leakage rules |
| `docs/target_definition.md` | Temporal rules + label definitions | Anchor event, feature window, anti-leakage gates |
| `src/jobs/00_bronze/*.py` | Landing→Bronze | Metadata audit: `_metadata.file_path`, timestamps |

## 🔍 Common Patterns

### Pattern: Adding a New Feature Block (v2, v3, etc.)
1. Ensure Silver transformation exists in `src/jobs/01_silver/`
2. Create new ABT version script `src/jobs/02_gold/0X_gold_abt_vX.py` (copy v1 as template)
3. Extend `build_abt_vX()`: add feature columns after v1 columns
4. Extend `validate_abt_vX()`: add gates for new feature null checks
5. Document in `docs/04_gold_rules/abt_vX.md` with new roadmap row
6. Update [README.md](README.md) progress table

### Pattern: Handling New Data Source
1. Create Bronze ingestion in `src/jobs/00_bronze/0X_ingest_*.py`
   - Read from Landing path
   - Add metadata: `_metadata.file_path`, timestamp, system origin
   - Write to Delta
2. Create Silver transform in `src/jobs/01_silver/0X_bronze_silver_*.py`
   - Apply `standardize_column_names()`
   - Apply sentinel handling per data dictionary ([docs/01_data_dictionary/](docs/01_data_dictionary/))
   - Apply validation gates per data quality rules ([docs/02_data_quality/](docs/02_data_quality/))
   - Write to Delta
3. Document data dict, quality, and Silver rules in respective [docs/](docs/) folders

## 🚀 Execution Commands

```bash
# Development (Databricks Notebook)
%run /Workspace/src/jobs/02_gold/00_gold_abt_builder.py

# Databricks Jobs (with args)
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/00_gold_abt_builder.py \
  --input_path /Volumes/.../silver/bureau_full_silver_delta/ \
  --output_path /Volumes/.../gold/abt_v1_delta/

# Local/Development
python src/jobs/02_gold/00_gold_abt_builder.py
```

## ⚠️ Anti-Patterns to Avoid

1. **Using labels as features:** FPD_INT and FLAG_INSTALACAO_INT are for audit/impact only
2. **Skipping sentinel handling:** Always create missing flags; don't drop nulls
3. **Inconsistent grain:** Verify 1:1 on `NUM_CPF + SAFRA` before each layer
4. **Missing validation gates:** Gate failures indicate data quality issues, not code bugs
5. **Hardcoded paths:** Use defaults in `settings.py` or parameterize via argparse

## � Project Context & Meetings

The `informacoes_adicionais/` folder contains critical meeting transcripts that provide business context for the hackathon:
- **00_reuniao_apresentacao-hackathon-2025.pdf** — Initial project presentation & objectives
- **01_reuniao_tira-duvidas-claro-gustavoLenin-20260107.pdf** — Q&A session with Claro stakeholder (Gustavo Lenin) clarifying data availability and business rules
- **Agenda Hackathon 2025 - 15.12.2025.pptx.pdf** — Hackathon schedule and team structure
- **check_point_20260115.pdf** — Latest progress checkpoint meeting (Jan 15, 2026)

**When to reference these meetings:**
- Making architectural decisions or roadmap changes
- Evaluating feature priorities and data source additions (v4+)
- Clarifying business constraints or stakeholder requirements
- Assessing feasibility of proposed transformations

## 📚 When in Doubt

- Check `docs/04_gold_rules/00_QUICK_START.md` for hands-on examples
- Review [validate_abt.py](src/jobs/02_gold/validators/validate_abt.py) for gate patterns
- See [IMPLEMENTATION_SUMMARY.txt](IMPLEMENTATION_SUMMARY.txt) for current status
- Trace column lineage via metadata columns: `metadata_*` and `gold_*`
- Reference `informacoes_adicionais/` folder for business context and stakeholder requirements
