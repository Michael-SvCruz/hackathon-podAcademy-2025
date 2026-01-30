# 🛠️ Data Engineering Roadmap — Modifications & Enhancements
**Date:** January 28, 2026  
**Scope:** Bronze → Silver → Gold pipeline improvements  
**Status:** Operational with targeted enhancements  

---

## 📊 Current State Assessment

### What's Working ✅
| Layer | Component | Status | Confidence |
|-------|-----------|--------|-----------|
| **Bronze** | 4 sources ingested | ✅ Complete | High |
| **Silver** | Type casting, deduplication | ✅ Complete | High |
| **Gold** | ABT v1-v6.1 (7 versions) | ✅ Complete | High |
| **Validation** | 14+ gates across versions | ✅ Complete | High |
| **Metadata** | Audit trail in all layers | ✅ Complete | High |

### Data Engineering Gaps (from audit)
| Gap | Impact | Priority | Est. Effort |
|-----|--------|----------|---|
| **Control Group Missing** | Can't isolate ZZ/ZX segment (~2%) | 🟡 High | 1-2 days |
| **Data Quality Report** | No consolidated anomaly documentation | 🟡 High | 2-3 days |
| **Lineage Tracking** | Can't trace column transformations | 🟠 Medium | 1-2 days |
| **Partition Strategy** | Gold tables not optimized for queries | 🟠 Medium | 1 day |

---

## 🎯 Phase 1: Control Group Extraction (Days 1-2)

### Objective
Isolate and tag the ~2% control group (CPF digits 6-7 = ZZ or ZX) for alternative policy testing.

### Implementation

#### 1.1 Add Control Group Filter to Silver Layer

**File to create:** `src/jobs/01_silver/control_group_filter.py`

```python
"""
Control Group Extraction: CPF digits 6-7 in {ZZ, ZX}
Purpose: Enable holdout testing of alternative approval policies
Target: ~2% of population (1.5-2.5% range)
"""

from pyspark.sql import functions as F

def extract_control_group(df, cpf_col="num_cpf"):
    """
    Extract control group: CPF digits 6-7 = ZZ or ZX
    
    Args:
        df: DataFrame with num_cpf column
        cpf_col: name of CPF column
    
    Returns:
        df_control: Control group records
        df_main: Main population (excluding control)
    """
    
    # Extract digits 6-7 (positions 5-6 in 0-indexed string)
    df_with_cpf_segment = df.withColumn(
        "cpf_segment_6_7",
        F.substring(F.col(cpf_col), 6, 2)
    )
    
    # Flag control group
    df_with_flag = df_with_cpf_segment.withColumn(
        "flag_control_group",
        F.when(
            F.col("cpf_segment_6_7").isin(["ZZ", "ZX"]),
            1
        ).otherwise(0)
    )
    
    # Split populations
    df_control = df_with_flag.filter(F.col("flag_control_group") == 1)
    df_main = df_with_flag.filter(F.col("flag_control_group") == 0)
    
    # Validation
    control_count = df_control.count()
    main_count = df_main.count()
    total_count = control_count + main_count
    control_pct = (control_count / total_count) * 100
    
    assert 1.5 <= control_pct <= 2.5, \
        f"Control group {control_pct:.2f}% out of expected range [1.5%, 2.5%]"
    
    print(f"✓ Control Group: {control_count:,} records ({control_pct:.2f}%)")
    print(f"✓ Main Population: {main_count:,} records ({100-control_pct:.2f}%)")
    
    return df_control, df_main, df_with_flag


def main():
    """
    Applies control group extraction to Silver Bureau
    Writes separate tables for control vs main
    """
    from src.utils.spark_utils import get_spark_session
    
    spark = get_spark_session("ControlGroupExtraction")
    
    # Read Silver Bureau
    df_bureau = spark.read.format("delta").load(
        "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
    )
    
    print(f"[Input] Bureau records: {df_bureau.count():,}")
    
    # Extract control group
    df_control, df_main, df_flagged = extract_control_group(df_bureau)
    
    # Write control group (for alternative policy testing)
    df_control.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/Volumes/hackathon_2025/default/silver/bureau_control_group_delta/")
    
    spark.sql("""
        CREATE OR REPLACE TABLE silver_bureau_control_group
        USING DELTA
        LOCATION '/Volumes/hackathon_2025/default/silver/bureau_control_group_delta/'
    """)
    
    # Write main population (for standard modeling)
    df_main.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/Volumes/hackathon_2025/default/silver/bureau_main_population_delta/")
    
    spark.sql("""
        CREATE OR REPLACE TABLE silver_bureau_main_population
        USING DELTA
        LOCATION '/Volumes/hackathon_2025/default/silver/bureau_main_population_delta/'
    """)
    
    # Write combined with flag (for reference)
    df_flagged.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/Volumes/hackathon_2025/default/silver/bureau_with_control_flag_delta/")
    
    print("\n✓ Control group tables created successfully")
    print("  └─ silver_bureau_control_group")
    print("  └─ silver_bureau_main_population")
    print("  └─ silver_bureau_with_control_flag")


if __name__ == "__main__":
    main()
```

#### 1.2 Add Control Group Gate to Validation

**Modify:** `src/utils/validate_abt.py` — Add as Gate 7 (all versions):

```python
def validate_control_group_marker(df_abt):
    """
    Gate 7: Control group properly flagged
    Ensures FLAG_CONTROL_GROUP is present and values are {0, 1}
    Range check: 1.5% ≤ control ≤ 2.5%
    """
    print("\n  [Gate 7] Validating control group flagging...")
    
    # Check column exists
    assert "flag_control_group" in df_abt.columns, \
        "FAIL Gate 7: flag_control_group column missing"
    
    total = df_abt.count()
    control_count = df_abt.filter(F.col("flag_control_group") == 1).count()
    control_pct = (control_count / total) * 100
    
    # Range check
    assert 1.5 <= control_pct <= 2.5, \
        f"FAIL Gate 7: Control {control_pct:.2f}% out of range [1.5%, 2.5%]"
    
    # Values check
    distinct_vals = df_abt.select("flag_control_group").distinct().collect()
    vals = {row[0] for row in distinct_vals}
    assert vals <= {0, 1, None}, \
        f"FAIL Gate 7: flag_control_group has unexpected values: {vals}"
    
    print(f"    ✓ PASS: Control group {control_pct:.2f}% (expected 1.5-2.5%)")
```

#### 1.3 Apply to All Gold ABT Versions

**Modify:** Each `src/jobs/02_gold/0X_gold_abt_v*.py`

Add after `build_abt_vX()` function, before writing to Delta:

```python
# Add control group flag
df_abt = df_abt.withColumn(
    "flag_control_group",
    F.when(
        F.substring(F.col("num_cpf"), 6, 2).isin(["ZZ", "ZX"]),
        1
    ).otherwise(0)
)
```

### Deliverables
- ✅ `src/jobs/01_silver/control_group_filter.py` (standalone script)
- ✅ Modified: All Gold builders (add control group flag)
- ✅ Modified: `validate_abt.py` (add Gate 7)
- ✅ Updated: Documentation in ABT specs

---

## 🔍 Phase 2: Data Quality Report (Days 3-4)

### Objective
Consolidate identified anomalies and document systematic treatments applied during transformations.

### Implementation

#### 2.1 Create Data Quality Audit Trail

**File to create:** `src/jobs/02_gold/data_quality_report_generator.py`

```python
"""
Data Quality Report Generator
Analyzes Bronze → Silver → Gold transformations
Documents anomalies and treatments applied
"""

from pyspark.sql import functions as F
import json
from datetime import datetime

def analyze_data_quality(df_bronze, df_silver, df_gold, source_name):
    """
    Comprehensive data quality analysis across layers
    
    Returns:
        dict with metrics per layer and anomalies
    """
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "source": source_name,
        "layers": {}
    }
    
    # ===== BRONZE LAYER ANALYSIS =====
    report["layers"]["bronze"] = {
        "total_records": df_bronze.count(),
        "schema": str(df_bronze.schema),
        "null_counts": {},
        "anomalies": []
    }
    
    for col in df_bronze.columns:
        null_count = df_bronze.filter(F.col(col).isNull()).count()
        if null_count > 0:
            report["layers"]["bronze"]["null_counts"][col] = null_count
    
    # ===== SILVER LAYER ANALYSIS =====
    report["layers"]["silver"] = {
        "total_records": df_silver.count(),
        "schema": str(df_silver.schema),
        "transformations_applied": [],
        "anomalies": []
    }
    
    # Check for sentinel values (source-specific)
    if source_name == "telco":
        sentinel_304_count = df_silver.filter(F.col("var_26") == 304).count()
        if sentinel_304_count > 0:
            report["layers"]["silver"]["anomalies"].append({
                "type": "sentinel_value",
                "column": "var_26",
                "value": 304,
                "count": sentinel_304_count,
                "action": "Treated as missing, FLAG_VAR26_MISSING=1"
            })
    
    # Check for age anomalies
    if "idade" in [c.lower() for c in df_silver.columns]:
        age_col = next(c for c in df_silver.columns if c.lower() == "idade")
        invalid_ages = df_silver.filter(
            (F.col(age_col) < 10) | (F.col(age_col) > 100)
        ).count()
        
        if invalid_ages > 0:
            report["layers"]["silver"]["anomalies"].append({
                "type": "age_out_of_range",
                "column": age_col,
                "range": "[10, 100]",
                "count": invalid_ages,
                "percentage": (invalid_ages / df_silver.count()) * 100,
                "action": "Flagged but retained for downstream analysis"
            })
    
    # ===== GOLD LAYER ANALYSIS =====
    report["layers"]["gold"] = {
        "total_records": df_gold.count(),
        "grain_check": {
            "num_cpf": df_gold.select("num_cpf").distinct().count(),
            "num_cpf_safra": df_gold.select("num_cpf", "safra").distinct().count(),
            "uniqueness": df_gold.select("num_cpf", "safra").distinct().count() == df_gold.count()
        },
        "feature_coverage": {}
    }
    
    # Feature coverage
    for col in df_gold.columns:
        if col.startswith("score_") or col.startswith("var_"):
            non_null = df_gold.filter(F.col(col).isNotNull()).count()
            coverage = (non_null / df_gold.count()) * 100
            report["layers"]["gold"]["feature_coverage"][col] = coverage
    
    return report


def generate_html_report(report_dict, output_path):
    """Generate HTML report from analysis"""
    
    html = f"""
    <html>
    <head>
        <title>Data Quality Report - {report_dict['source']}</title>
        <style>
            body {{ font-family: Arial; margin: 20px; }}
            h2 {{ color: #333; border-bottom: 2px solid #007bff; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
            th {{ background-color: #007bff; color: white; }}
            .pass {{ color: green; }}
            .warning {{ color: orange; }}
            .fail {{ color: red; }}
        </style>
    </head>
    <body>
        <h1>Data Quality Report</h1>
        <p><strong>Source:</strong> {report_dict['source']}</p>
        <p><strong>Generated:</strong> {report_dict['timestamp']}</p>
        
        <h2>Bronze Layer</h2>
        <p>Total Records: {report_dict['layers']['bronze']['total_records']:,}</p>
        
        <h2>Silver Layer</h2>
        <p>Total Records: {report_dict['layers']['silver']['total_records']:,}</p>
        <h3>Anomalies Detected</h3>
        <table>
            <tr><th>Type</th><th>Column</th><th>Count</th><th>Action</th></tr>
    """
    
    for anomaly in report_dict['layers']['silver']['anomalies']:
        html += f"""
            <tr>
                <td>{anomaly['type']}</td>
                <td>{anomaly.get('column', 'N/A')}</td>
                <td>{anomaly['count']:,}</td>
                <td>{anomaly['action']}</td>
            </tr>
        """
    
    html += f"""
        </table>
        
        <h2>Gold Layer</h2>
        <p>Total Records: {report_dict['layers']['gold']['total_records']:,}</p>
        <p class="{'pass' if report_dict['layers']['gold']['grain_check']['uniqueness'] else 'fail'}">
            Grain Check (NUM_CPF + SAFRA): {'✓ PASS' if report_dict['layers']['gold']['grain_check']['uniqueness'] else '✗ FAIL'}
        </p>
        
        <h3>Feature Coverage</h3>
        <table>
            <tr><th>Feature</th><th>Coverage %</th></tr>
    """
    
    for feat, coverage in report_dict['layers']['gold']['feature_coverage'].items():
        status = "pass" if coverage > 90 else "warning"
        html += f"<tr><td>{feat}</td><td class='{status}'>{coverage:.1f}%</td></tr>"
    
    html += """
        </table>
    </body>
    </html>
    """
    
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"✓ Report saved to: {output_path}")


def main():
    """Generate quality reports for all sources"""
    
    from src.utils.spark_utils import get_spark_session
    import os
    
    spark = get_spark_session("DataQualityReporter")
    
    sources = ["bureau", "telco", "cadastro", "pagamento", "atraso"]
    
    for source in sources:
        try:
            # Read layers
            df_bronze = spark.read.format("delta").load(
                f"/Volumes/hackathon_2025/default/bronze/{source}_delta/"
            )
            df_silver = spark.read.format("delta").load(
                f"/Volumes/hackathon_2025/default/silver/{source}_silver_delta/"
            )
            
            # Generate report
            report = analyze_data_quality(df_bronze, df_silver, None, source)
            
            # Save as JSON
            report_json_path = f"/Volumes/hackathon_2025/default/reports/data_quality_{source}.json"
            with open(report_json_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            # Save as HTML
            report_html_path = f"/Volumes/hackathon_2025/default/reports/data_quality_{source}.html"
            generate_html_report(report, report_html_path)
            
            print(f"✓ {source}: Quality report generated")
        
        except Exception as e:
            print(f"⚠️  {source}: {str(e)}")


if __name__ == "__main__":
    main()
```

#### 2.2 Document Consolidated Anomalies

**File to create:** `docs/10_data_quality/01_anomalies_and_treatments.md`

```markdown
# Data Quality Findings & Systematic Treatments

## Overview
This document consolidates anomalies discovered during exploratory analysis and the systematic treatments applied in Bronze → Silver → Gold transformations.

## 1. Bureau (Score & Approval)

### Anomaly 1.1: Score_01 Sentinel (0 = Missing)
- **Count:** X records
- **Treatment:** Converted to NULL + FLAG_SCORE01_MISSING=1
- **Rationale:** Preserves signal while indicating missing data
- **Affected Features:** score_01_adj (v1+)

### Anomaly 1.2: Age Range Anomalies
- **Type:** age < 10 or age > 100
- **Count:** Y records
- **Treatment:** Retained but flagged in Gate 8
- **Rationale:** May indicate data entry errors but could be valid edge cases

## 2. Telco (Usage Features)

### Anomaly 2.1: Sentinel Value 304 (Not Reported)
- **Columns:** var_26, var_27, ..., var_93
- **Count:** Z records per variable
- **Treatment:** Converted to NULL + FLAG_VAR*_MISSING=1
- **Rationale:** Systematic non-reporting pattern
- **Affected Features:** All 68 Telco features (v3+)

## 3. Pagamento (Payment Data)

### Anomaly 3.1: Discount > Amount Paid
- **Count:** K records
- **Treatment:** DESCONTO_RATE = desconto / (desconto + pagamento)
- **Rationale:** Ratio-based handling prevents division by zero

### Anomaly 3.2: Payment Before Due Date (Prepayment)
- **Count:** ~70% of cases
- **Treatment:** Considered valid (dias_atraso = 0)
- **Rationale:** Prepayment is legitimate behavior

## 4. Atraso (Default Data)

### Anomaly 4.1: Conflicting FPD Observations
- **Issue:** FPD observed in FLAG_INSTALACAO=0 cases
- **Treatment:** Enforced by Gate 2 validation
- **Outcome:** These records fail quality check

---

## Summary Table: Anomalies by Layer

| Source | Type | Count | Treatment | Gate |
|--------|------|-------|-----------|------|
| Bureau | Age < 10 or > 100 | X | Flag + Retain | 8 |
| Bureau | Score_01 = 0 | Y | NULL + FLAG | 1-6 |
| Telco | var_* = 304 | Z | NULL + FLAG | 7-10 |
| Pagamento | discount anomaly | K | Ratio-based | 11 |
| Atraso | FPD in FLAG=0 | L | Reject (Gate 2) | 2 |

```

### Deliverables
- ✅ `src/jobs/02_gold/data_quality_report_generator.py`
- ✅ `docs/10_data_quality/01_anomalies_and_treatments.md`
- ✅ Auto-generated reports: `reports/data_quality_*.json` and `.html`

---

## 📈 Phase 3: Partition & Lineage Optimization (Days 5-6)

### Objective
Improve query performance and enable column-level lineage tracking for downstream analysis.

### Implementation

#### 3.1 Add Partitioning to Gold Tables

**Modify:** All `src/jobs/02_gold/0X_gold_abt_v*.py` — Write phase:

```python
# BEFORE (current)
df_abt.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

# AFTER (with partitioning)
df_abt.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("safra") \  # Partition by month
    .save(output_path)
```

**Rationale:**
- Faster queries filtered by SAFRA (common operation)
- Enables deletion/update of specific months
- Improves data skipping

#### 3.2 Add Column-Level Lineage Metadata

**Create:** `src/jobs/02_gold/lineage_tracker.py`

```python
"""
Column-Level Lineage Tracking
Maps each Gold column back to source → Bronze → Silver → Gold
"""

from pyspark.sql import DataFrame
import json

class LineageTracker:
    """Track column transformations across layers"""
    
    def __init__(self):
        self.lineage_map = {}
    
    def map_column_source(self, gold_col, silver_col, source_name, transformation):
        """Record how a Gold column derives from Silver"""
        
        self.lineage_map[gold_col] = {
            "source": source_name,
            "silver_column": silver_col,
            "transformation": transformation,
            "layer_path": f"bronze/{source_name} → silver → gold"
        }
    
    def generate_lineage_report(self, output_path):
        """Export lineage as JSON"""
        
        with open(output_path, 'w') as f:
            json.dump(self.lineage_map, f, indent=2)
    
    @staticmethod
    def example_lineage_v1():
        """Example lineage for ABT v1"""
        return {
            "num_cpf": {
                "source": "bureau",
                "silver_column": "num_cpf",
                "transformation": "passthrough",
                "type": "key"
            },
            "score_01_adj": {
                "source": "bureau",
                "silver_column": "score_01",
                "transformation": "to_double_safe() + sentinel_handling(0→NULL)",
                "type": "feature",
                "feature_block": "v1"
            },
            "flag_score01_missing": {
                "source": "bureau",
                "silver_column": "score_01",
                "transformation": "when(isNull, 1).otherwise(0)",
                "type": "flag",
                "feature_block": "v1"
            },
            "fpd_int": {
                "source": "bureau",
                "silver_column": "fpd",
                "transformation": "passthrough",
                "type": "label",
                "usage": "target (train on FLAG_INSTALACAO=1 only)"
            },
            "flag_instalacao_int": {
                "source": "bureau",
                "silver_column": "flag_instalacao",
                "transformation": "passthrough",
                "type": "label",
                "usage": "decision (audit/swap analysis only)"
            }
        }
```

#### 3.3 Lineage Documentation in ABT Specs

**Add to each:** `docs/04_gold_rules/abt_v*.md`

```markdown
## Column Lineage

| Gold Column | Source | Silver Column | Transformation | Type |
|---|---|---|---|---|
| num_cpf | bureau | num_cpf | passthrough | key |
| score_01_adj | bureau | score_01 | to_double_safe() + sentinel(0→NULL) | feature |
| flag_score01_missing | bureau | score_01 | isNull() → 1/0 | flag |
| var_26 | telco | var_26 | to_int_safe() + sentinel(304→NULL) | feature (v3+) |
| flag_var26_missing | telco | var_26 | isNull() → 1/0 | flag (v3+) |

[Full lineage map: docs/lineage/abt_v*_lineage.json]
```

### Deliverables
- ✅ Modified: All Gold builders (add PARTITIONBY safra)
- ✅ `src/jobs/02_gold/lineage_tracker.py`
- ✅ Generated: `docs/lineage/abt_v*_lineage.json`
- ✅ Updated: ABT specs with lineage tables

---

## 🔐 Phase 4: Enhanced Validation Gates (Days 7-8)

### Objective
Add new gates to catch edge cases and ensure data quality across all versions.

### Implementation

#### 4.1 New Validation Gates (Gate 8-10)

**Add to:** `src/utils/validate_abt.py`

```python
def validate_age_range(df_abt):
    """
    Gate 8: Age distribution within reasonable bounds
    Alert on outliers but don't fail
    """
    
    age_col = next((c for c in df_abt.columns if 'idade' in c.lower()), None)
    
    if age_col:
        stats = df_abt.select(
            F.min(age_col).alias("min_age"),
            F.max(age_col).alias("max_age"),
            F.percentile_approx(age_col, 0.01).alias("p1"),
            F.percentile_approx(age_col, 0.99).alias("p99")
        ).collect()[0]
        
        print(f"\n  [Gate 8] Age Range Check:")
        print(f"    Range: {stats['min_age']} - {stats['max_age']}")
        print(f"    P1-P99: {stats['p1']} - {stats['p99']}")
        
        # Alert if out of typical range
        if stats['min_age'] < 10 or stats['max_age'] > 100:
            print(f"    ⚠️  WARNING: Age outliers detected (< 10 or > 100)")
        else:
            print(f"    ✓ PASS: Age distribution normal")


def validate_feature_coverage(df_abt, min_coverage=0.85):
    """
    Gate 9: Feature coverage threshold
    At least 85% of records have non-null feature values
    """
    
    print(f"\n  [Gate 9] Feature Coverage Check (minimum {min_coverage*100:.0f}%):")
    
    coverage_failures = []
    
    for col in df_abt.columns:
        if col.startswith(('score_', 'var_', 'desconto_', 'dias_')):
            non_null = df_abt.filter(F.col(col).isNotNull()).count()
            coverage = non_null / df_abt.count()
            
            if coverage < min_coverage:
                coverage_failures.append((col, coverage))
                print(f"    ✗ {col}: {coverage*100:.1f}%")
            else:
                print(f"    ✓ {col}: {coverage*100:.1f}%")
    
    assert len(coverage_failures) == 0, \
        f"FAIL Gate 9: {len(coverage_failures)} features below {min_coverage*100:.0f}%"


def validate_temporal_consistency(df_abt):
    """
    Gate 10: Temporal distribution
    Check that SAFRA spans expected months without gaps
    """
    
    print(f"\n  [Gate 10] Temporal Consistency Check:")
    
    safra_dist = df_abt.groupBy("safra").count() \
        .orderBy("safra").collect()
    
    safra_values = [int(row["safra"]) for row in safra_dist]
    
    print(f"    SAFRA range: {min(safra_values)} - {max(safra_values)}")
    print(f"    Records per month: min={min(r['count'] for r in safra_dist):,}, "
          f"max={max(r['count'] for r in safra_dist):,}")
    
    # Check for major imbalances (> 3x difference)
    counts = [r['count'] for r in safra_dist]
    if max(counts) / min(counts) > 3:
        print(f"    ⚠️  WARNING: Significant volume variation across months")
    else:
        print(f"    ✓ PASS: Temporal distribution stable")
```

#### 4.2 Apply Gates to All Validators

Add to each `validate_abt_v<X>()` function:

```python
def validate_abt_v3(df_abt, count_in_bureau):
    """v3 validations: inherit v1-v2 gates + add v3-specific"""
    
    # ... existing gates 1-6 ...
    
    # Gate 7: Control group
    validate_control_group_marker(df_abt)
    
    # Gate 8: Age range
    validate_age_range(df_abt)
    
    # Gate 9: Telco feature coverage
    validate_feature_coverage(df_abt, min_coverage=0.80)
    
    # Gate 10: Temporal consistency
    validate_temporal_consistency(df_abt)
    
    print("\n✓ All Gates PASSED (v3: 10 gates)")
```

### Deliverables
- ✅ Modified: `src/utils/validate_abt.py` (add Gates 8-10)
- ✅ Modified: All `validate_abt_v*()` functions
- ✅ Updated: Documentation with gate descriptions

---

## 📋 Phase 5: Documentation & Handoff (Days 9-10)

### 5.1 Create Data Engineering Spec Document

**File:** `docs/DATA_ENGINEERING_SPECIFICATION.md`

Sections:
1. **Pipeline Architecture** — How Bronze/Silver/Gold connects
2. **Data Lineage** — Column-to-column transformations
3. **Validation Framework** — All 10 gates + what each validates
4. **Partition Strategy** — Why SAFRA partitioning + query patterns
5. **Control Group Usage** — When/how to filter for alternative policies
6. **Quality Reports** — How to generate + interpret
7. **Troubleshooting** — Common issues + solutions

### 5.2 Update README with Phase Status

**Modify:** Top-level `README.md`

```markdown
## ✅ Data Engineering Status (v1.0 Complete)

### Completed
- [x] Bronze layer: 4 sources ingested with metadata audit
- [x] Silver layer: Type casting, deduplication, validation
- [x] Gold layer: 7 ABT versions (v1-v6.1) with incremental features
- [x] Validation: 10 gates across all versions
- [x] Control group: ZZ/ZX extraction (~2% population)
- [x] Lineage: Column-level traceability
- [x] Quality reporting: Automated anomaly documentation

### Ready for Data Science Phase
- ABT v1-v6.1 available in Unity Catalog
- All data quality issues documented
- Validation gates ensure consistency
- Control group available for alternative policy testing

### Next: Train Incremental Models (v1 → v6.1)
See [Data Science Roadmap](docs/DATA_SCIENCE_ROADMAP.md)
```

---

## 🎯 Implementation Timeline

| Phase | Days | Key Deliverables | Status |
|-------|------|---|---|
| **Phase 1:** Control Group Extraction | 1-2 | Filter + flag + validate | New |
| **Phase 2:** Data Quality Report | 3-4 | Consolidated anomalies + HTML reports | New |
| **Phase 3:** Partition & Lineage | 5-6 | Optimized tables + traceability | New |
| **Phase 4:** Enhanced Validation | 7-8 | Gates 8-10 + temporal checks | New |
| **Phase 5:** Documentation | 9-10 | Spec + README + handoff notes | New |

**Total Effort:** 10 days (part-time) to 5 days (full-time)

---

## 📊 Success Criteria

### Bronze → Silver → Gold
- [ ] All 7 ABT versions build successfully
- [ ] All 10 validation gates pass
- [ ] No data leakage (FPD/FLAG as features)
- [ ] Metadata audit trail complete

### Control Group
- [ ] ~2% population correctly flagged (ZZ/ZX)
- [ ] Separate tables created (control + main)
- [ ] Gate 7 validates presence + percentage

### Quality Assurance
- [ ] All anomalies documented
- [ ] HTML reports generated for each source
- [ ] No silent data quality issues

### Lineage & Optimization
- [ ] All Gold tables partitioned by SAFRA
- [ ] Column lineage JSON generated
- [ ] Lineage documented in ABT specs

### Documentation
- [ ] Data Engineering Spec complete
- [ ] README updated with completion status
- [ ] Handoff notes for Data Science phase

---

## 🚀 How to Execute

### Quick Start (5 days, full-time)
```bash
# Day 1: Control Group
python src/jobs/01_silver/control_group_filter.py

# Day 2: Apply to Gold (modify all 7 builders)
# Modify each: src/jobs/02_gold/0X_gold_abt_v*.py

# Day 3-4: Quality Report
python src/jobs/02_gold/data_quality_report_generator.py

# Day 5: Validation + Documentation
# Update validate_abt.py + create spec
```

### Phased Approach (10 days, part-time)
- Dedicate 2 days to each phase
- Test incrementally
- Update documentation as you go

---

## 🔗 Related Documents
- [Copilot Instructions](../.github/copilot-instructions.md) — Data engineering patterns
- [Target Definition](docs/target_definition.md) — Anti-leakage rules
- [ABT Specifications](docs/04_gold_rules/) — Per-version details
- [Audit Findings](analise_20260127.md) — Context for these enhancements

---

**Document Status:** Ready for implementation  
**Last Updated:** January 28, 2026  
**Owner:** Data Engineering Team
