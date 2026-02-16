# 🚀 Quick Start: Data Engineering Roadmap Implementation

**Timeline:** 5-10 days | **Effort:** 40-80 hours  
**Status:** Ready to implement  
**Owner:** Data Engineering Team  

---

## 📋 What to Do (In Order)

### ✅ Phase 1: Control Group Extraction (Days 1-2)

**Goal:** Extract and flag the ~2% control group (CPF digits 6-7 in {ZZ, ZX})

#### Step 1.1: Create control group filter script
```bash
# Create file: src/jobs/01_silver/control_group_filter.py
# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 1 → Section 1.1
# Contains: extract_control_group() function
```

**Test it:**
```bash
cd /path/to/repo
python src/jobs/01_silver/control_group_filter.py
# Expected output: "✓ Control Group: X records (1.5-2.5%)"
```

#### Step 1.2: Modify all Gold builders to add control group flag
```bash
# Files to modify (6 total):
# - src/jobs/02_gold/00_gold_abt_builder.py
# - src/jobs/02_gold/01_gold_abt_v2_builder.py
# - src/jobs/02_gold/02_gold_abt_v3_builder.py
# - src/jobs/02_gold/03_gold_abt_v4_builder.py
# - src/jobs/02_gold/04_gold_abt_v5_builder.py
# - src/jobs/02_gold/05_gold_abt_v6_builder.py
# - src/jobs/02_gold/06_gold_abt_v6_1_builder.py

# In each file's build_abt_vX() function, add AFTER feature selection:
df_abt = df_abt.withColumn(
    "flag_control_group",
    F.when(
        F.substring(F.col("num_cpf"), 6, 2).isin(["ZZ", "ZX"]),
        1
    ).otherwise(0)
)
```

#### Step 1.3: Add validation gate
```bash
# File: src/utils/validate_abt.py
# Add function: validate_control_group_marker()
# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 1 → Section 1.2

# Then add to each validate_abt_vX():
# validate_control_group_marker(df_abt)
```

**Validation:**
```bash
# Run any Gold builder
python src/jobs/02_gold/00_gold_abt_builder.py
# Look for: "[Gate 7] Control Group: X.XX% (expected 1.5-2.5%)"
```

---

### ✅ Phase 2: Data Quality Report Generator (Days 3-4)

**Goal:** Create automated reports documenting all data anomalies

#### Step 2.1: Create quality report generator
```bash
# Create file: src/jobs/02_gold/data_quality_report_generator.py
# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 2 → Section 2.1
# Contains: analyze_data_quality(), generate_html_report()
```

**Test it:**
```bash
python src/jobs/02_gold/data_quality_report_generator.py
# Output: 
# ✓ bureau: Quality report generated
# ✓ telco: Quality report generated
# ✓ cadastro: Quality report generated
# Files created in /Volumes/hackathon_2025/default/reports/
```

#### Step 2.2: Create consolidated anomalies documentation
```bash
# Create file: docs/10_data_quality/01_anomalies_and_treatments.md
# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 2 → Section 2.2

# This documents:
# - Bureau age anomalies
# - Telco sentinel value (304)
# - Pagamento discount issues
# - Atraso FPD violations
# - How each was treated
```

**Verify:**
```bash
# Check that reports were created:
ls -la /Volumes/hackathon_2025/default/reports/data_quality_*.html
```

---

### ✅ Phase 3: Partition & Lineage (Days 5-6)

**Goal:** Add SAFRA partitioning for performance + column-level lineage tracking

#### Step 3.1: Update all Gold builders to partition by SAFRA
```bash
# Files to modify (same 7 as Phase 1.2)
# In the write phase, change:

# FROM:
df_abt.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

# TO:
df_abt.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("safra") \
    .save(output_path)
```

**Why:** Faster queries when filtering by month, enables incremental updates

#### Step 3.2: Create lineage tracker
```bash
# Create file: src/jobs/02_gold/lineage_tracker.py
# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 3 → Section 3.2
# Contains: LineageTracker class + example_lineage_v1()

# Run once to generate example mapping
python src/jobs/02_gold/lineage_tracker.py
# Output: docs/lineage/abt_v1_lineage.json
```

#### Step 3.3: Update ABT documentation with lineage tables
```bash
# For each: docs/04_gold_rules/abt_vX.md
# Add section: "## Column Lineage"
# Create table mapping each Gold column to source

# Example structure:
# | Gold Column | Source | Silver Column | Transformation | Type |
# |---|---|---|---|---|
# | num_cpf | bureau | num_cpf | passthrough | key |
# | score_01_adj | bureau | score_01 | to_double_safe() + sentinel(0→NULL) | feature |
```

---

### ✅ Phase 4: Enhanced Validation Gates (Days 7-8)

**Goal:** Add Gates 8-10 for temporal checks and feature coverage

#### Step 4.1: Add three new validation functions
```bash
# File: src/utils/validate_abt.py
# Add three new functions:
# 1. validate_age_range() - Gate 8
# 2. validate_feature_coverage() - Gate 9
# 3. validate_temporal_consistency() - Gate 10

# Use code from DATA_ENGINEERING_ROADMAP.md → Phase 4 → Section 4.1
```

#### Step 4.2: Update all validators to call new gates
```bash
# For each: validate_abt_v1(), validate_abt_v2(), ..., validate_abt_v6_1()
# Add at the end (before final print):

validate_age_range(df_abt)
validate_feature_coverage(df_abt, min_coverage=0.85)
validate_temporal_consistency(df_abt)

# Update final print to say "X gates" instead of hardcoded number
```

**Test:**
```bash
python src/jobs/02_gold/00_gold_abt_builder.py
# Look for:
# [Gate 8] Age Range Check...
# [Gate 9] Feature Coverage Check...
# [Gate 10] Temporal Consistency Check...
```

---

### ✅ Phase 5: Documentation & Handoff (Days 9-10)

**Goal:** Consolidate all changes + prepare for Data Science phase

#### Step 5.1: Create Data Engineering Specification
```bash
# Create file: docs/DATA_ENGINEERING_SPECIFICATION.md

# Sections:
# 1. Pipeline Architecture
# 2. Data Lineage
# 3. Validation Framework (all 10 gates)
# 4. Partition Strategy
# 5. Control Group Usage
# 6. Quality Reports
# 7. Troubleshooting

# Use content from this roadmap + test results
```

#### Step 5.2: Update README with completion status
```bash
# Edit: README.md

# Add section:
## ✅ Data Engineering Status (v1.0 Complete)

### Completed
- [x] Bronze layer: 4 sources ingested with metadata
- [x] Silver layer: Type casting, deduplication, validation
- [x] Gold layer: 7 ABT versions (v1-v6.1)
- [x] Validation: 10 gates across all versions
- [x] Control group: ZZ/ZX extraction (~2% population)
- [x] Lineage: Column-level traceability
- [x] Quality reporting: Automated anomaly docs

### Next: Data Science Phase
- See docs/DATA_SCIENCE_ROADMAP.md
```

#### Step 5.3: Create handoff checklist
```bash
# Create: docs/HANDOFF_CHECKLIST.md

# For each ABT version (v1-v6.1):
# - [ ] All columns present and correct types
# - [ ] Control group flag = 1 for ~2% records
# - [ ] All 10 validation gates pass
# - [ ] Lineage JSON generated
# - [ ] Quality report available
# - [ ] Partition by SAFRA verified

# Sign-off: Data Engineering → Data Science
```

---

## 🧪 Testing Protocol

### Before Each Phase
```bash
# 1. Run target Gold builder
python src/jobs/02_gold/00_gold_abt_builder.py  # or 01, 02, etc.

# 2. Verify output
spark.read.format("delta").load("/Volumes/hackathon_2025/default/gold/abt_v1_delta/").show()

# 3. Check counts
df.count()  # Should match input count (or be subset if control group filtered)

# 4. Verify no data loss
# Count should equal previous layer or increase with new features
```

### After All Phases
```bash
# Run full validation suite
for version in v1 v2 v3 v4 v5 v6 v6_1; do
    echo "Testing $version..."
    python src/jobs/02_gold/${version}_gold_abt_*builder.py
done

# Generate quality reports
python src/jobs/02_gold/data_quality_report_generator.py

# Verify all reports exist
ls -la /Volumes/hackathon_2025/default/reports/data_quality_*.html
```

---

## 🚨 Common Issues & Solutions

### Issue: "Control group percentage out of range"
**Cause:** CPF segment extraction wrong or data doesn't have ZZ/ZX codes
**Solution:** 
- Verify CPF format: should be numeric or alphanumeric
- Check if positions 6-7 actually contain ZZ/ZX values
- Use: `df.select(F.substring(F.col("num_cpf"), 6, 2)).distinct().show(20)`

### Issue: "Feature coverage below 85%"
**Cause:** New feature block has too many nulls
**Solution:**
- Check if sentinel value handling is working
- Look at data quality report HTML file
- May need to adjust min_coverage threshold

### Issue: "Temporal consistency warning: volume > 3x variation"
**Cause:** Some months have significantly more records
**Solution:**
- Check if SAFRA is correct
- Verify no duplicate ingestion
- This is a WARNING, not a FAIL (may be legitimate)

---

## 📊 Success Checklist

### Day 1-2 (Control Group)
- [ ] `control_group_filter.py` created and tested
- [ ] All 7 Gold builders modified with `flag_control_group`
- [ ] Gate 7 added to `validate_abt.py`
- [ ] Control group shows ~2% (1.5-2.5%)

### Day 3-4 (Quality Report)
- [ ] `data_quality_report_generator.py` created
- [ ] Reports generated for all 4 sources (bureau, telco, cadastro, pagamento, atraso)
- [ ] HTML reports readable in browser
- [ ] Anomalies documented in `01_anomalies_and_treatments.md`

### Day 5-6 (Partitioning & Lineage)
- [ ] All 7 Gold builders use `partitionBy("safra")`
- [ ] `lineage_tracker.py` created
- [ ] Lineage JSON files generated
- [ ] ABT specs updated with lineage tables

### Day 7-8 (Enhanced Gates)
- [ ] Gates 8, 9, 10 implemented
- [ ] All validators updated
- [ ] All gates pass when builders run
- [ ] Gate output in logs shows new validation checks

### Day 9-10 (Documentation)
- [ ] `DATA_ENGINEERING_SPECIFICATION.md` complete
- [ ] README updated with completion status
- [ ] Handoff checklist created
- [ ] All code commented and clean

---

## 🎓 Learning Resources

Reference [DATA_ENGINEERING_ROADMAP.md](DATA_ENGINEERING_ROADMAP.md):
- **Section 1:** Control Group logic + code
- **Section 2:** Quality report templates
- **Section 3:** Lineage tracking implementation
- **Section 4:** New validation gates
- **Section 5:** Documentation standards

Reference [.github/copilot-instructions.md](../.github/copilot-instructions.md):
- ABT Building Pattern (copy-paste template)
- Sentinel & Missing Value Pattern
- Validation Gates (how each works)
- Anti-Patterns to Avoid

---

## 📞 Support

**Questions about implementation?**
- Check DATA_ENGINEERING_ROADMAP.md for detailed code
- Review existing Gold builders (00_gold_abt_builder.py) for patterns
- Check validate_abt.py for gate examples

**Issues with data?**
- Check data_quality_report_generator.py output
- Review docs/10_data_quality/01_anomalies_and_treatments.md
- Use Gate 7-10 warnings to identify edge cases

---

**Ready to start? Pick Phase 1 and commit to the 5-10 day timeline!**

Last updated: January 28, 2026
