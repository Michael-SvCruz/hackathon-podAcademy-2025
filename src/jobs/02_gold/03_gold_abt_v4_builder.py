"""
ABT v4 Builder — Bureau + Scores + Telco + Cadastro
Incremental Feature Roadmap: v1 (Scores) + v2 (Score_02) + v3 (Telco var_26-93) + v4 (Cadastro features)

This module extends v3 by adding Cadastro features via LEFT JOIN on NUM_CPF + SAFRA.
Cadastro is an optional complementary source (similar to Telco).

Key Orchestration:
  1. Read Silver Bureau (v3 spine with v1/v2/v3 features)
  2. Read Silver Cadastro (demographic & proprietary features)
  3. LEFT JOIN Bureau.NUM_CPF+SAFRA = Cadastro.NUM_CPF+SAFRA
  4. Select and order all features (Score_01/02, Telco var_26-93, Cadastro vars)
  5. Add Gold metadata (build_date, version, feature_blocks)
  6. Validate with 9-gate framework
  7. Write to Delta Lake

Cadastro Features (v4 additions):
  - Demographic: IDADE_ANOS, CEP_3_digitos
  - Status: STATUSRF, PROD, flag_mig2
  - Numeric: var_03, var_04, var_05, var_06, var_07, var_08, var_09, var_10, var_11, var_16, var_17
  - Mixed/Categorical: var_15, var_22, var_23, var_24, var_25
  - Dates: DT_var_12 (derived)
  - Flags: FLAG_CEP3_MISSING, FLAG_IDADE_OUTLIER, FLAG_var_11_NEG, FLAG_DT_NASC_INVALID, FLAG_DT_VAR12_INVALID

Anti-Leakage: FPD_INT and FLAG_INSTALACAO_INT from Cadastro are audit-only; never used as features.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from datetime import datetime
import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.spark_utils import get_spark_session
from .validators.validate_abt import validate_abt_v4


def build_abt_v4(
    spark: SparkSession,
    bureau_silver_path: str,
    telco_silver_path: str,
    cadastro_silver_path: str,
    output_path: str
) -> DataFrame:
    """
    Build ABT v4: Bureau spine + v1/v2/v3 features + Cadastro features.
    
    Args:
        spark: SparkSession
        bureau_silver_path: path to Silver Bureau Delta table
        telco_silver_path: path to Silver Telco Delta table
        cadastro_silver_path: path to Silver Cadastro Delta table
        output_path: path to write v4 Gold ABT
    
    Returns:
        DataFrame: v4 ABT with 3.79M records and ~160+ features
    """
    
    print("\n" + "="*80)
    print("ABT v4 BUILDER — Bureau + Scores + Telco + Cadastro")
    print("="*80)
    
    # Step 1: Read and prepare Bureau (spine with v1/v2/v3 features)
    print("\n[Step 1] Reading Silver Bureau v3 (spine with v1/v2/v3 features)...")
    df_bureau = spark.read.format("delta").load(bureau_silver_path)
    
    # Prepare Bureau subset: keys, labels, scores, telco vars from v3
    df_bureau_prepared = df_bureau.select(
        # Keys
        F.col("num_cpf"),
        F.col("safra"),
        F.col("dt_safra"),
        # Labels (audit only)
        F.col("fpd_int"),
        F.col("flag_instalacao_int"),
        # v1 features: Score_01
        F.col("score_01_adj"),
        F.col("flag_score01_missing"),
        # v2 features: Score_02 (raw, will adjust in Step 4)
        F.col("score_02_dbl"),
        # v3 features: Telco var_26-93 and flags (65 features + 65 flags = 130 cols)
        *[F.col(f"var_{i:02d}_adj") for i in range(26, 94)],
        *[F.col(f"flag_var_{i:02d}_missing") for i in range(26, 94)],
        # Metadata
        F.col("metadata_source"),
        F.col("metadata_build_date"),
        F.col("metadata_audit_record_count")
    )
    
    # Count records
    bureau_count = df_bureau_prepared.count()
    print(f"  ✓ Bureau prepared: {bureau_count:,} records")
    
    # Step 2: Read and prepare Cadastro (optional enrichment source)
    print("\n[Step 2] Reading Silver Cadastro...")
    df_cadastro = spark.read.format("delta").load(cadastro_silver_path)
    
    # Prepare Cadastro subset: numeric, categorical, and flag features
    df_cadastro_prepared = df_cadastro.select(
        F.col("num_cpf"),
        F.col("safra"),
        # Demographic
        F.col("idade_anos"),
        F.col("cep_3_digitos"),
        F.col("flag_cep3_missing"),
        F.col("flag_idade_outlier"),
        F.col("flag_dt_nasc_invalid"),
        # Status
        F.col("statusrf"),
        F.col("prod"),
        F.col("flag_mig2"),
        # Numeric cadastro vars
        F.col("var_03"),
        F.col("var_04"),
        F.col("var_05"),
        F.col("var_06"),
        F.col("var_07"),
        F.col("var_08"),
        F.col("var_09"),
        F.col("var_10"),
        F.col("var_11"),
        F.col("flag_var_11_neg"),
        F.col("var_16"),
        F.col("var_17"),
        # Mixed/Categorical cadastro vars
        F.col("var_15"),
        F.col("var_22"),
        F.col("var_23"),
        F.col("var_24"),
        F.col("var_25"),
        # Date-derived
        F.col("dt_var_12"),
        F.col("flag_dt_var_12_invalid")
    )
    
    cadastro_count = df_cadastro_prepared.count()
    print(f"  ✓ Cadastro prepared: {cadastro_count:,} records")
    
    # Step 3: LEFT JOIN Bureau with Cadastro on NUM_CPF + SAFRA
    print("\n[Step 3] LEFT JOINing Bureau (spine) with Cadastro...")
    df_abt = df_bureau_prepared.join(
        df_cadastro_prepared,
        on=[F.col("df_bureau_prepared.num_cpf") == F.col("df_cadastro_prepared.num_cpf"),
            F.col("df_bureau_prepared.safra") == F.col("df_cadastro_prepared.safra")],
        how="left"
    ).select("df_bureau_prepared.*", "df_cadastro_prepared.*")
    
    # Alternative approach (cleaner): use string keys for join
    df_abt = df_bureau_prepared.alias("bureau").join(
        df_cadastro_prepared.alias("cadastro"),
        on=["num_cpf", "safra"],
        how="left"
    ).select("bureau.*", "cadastro.*")
    
    # Note: after LEFT JOIN, some columns may be duplicated; take first occurrence
    # Spark automatically handles by preferring left table columns
    
    abt_after_join_count = df_abt.count()
    print(f"  ✓ ABT after JOIN: {abt_after_join_count:,} records (left join preserves grain)")
    
    # Step 4: Treat Score_02 sentinela (0 → NULL) if not already done
    print("\n[Step 4] Treating Score_02 sentinela values (0 → NULL)...")
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(
            F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    ).drop("score_02_dbl")
    print("  ✓ Score_02 sentinela treated")
    
    # Step 5: Select, order, and finalize columns
    print("\n[Step 5] Selecting and ordering final columns (165 total)...")
    
    # Column groups
    key_cols = ["num_cpf", "safra", "dt_safra"]
    label_cols = ["fpd_int", "flag_instalacao_int"]
    score_cols = ["score_01_adj", "flag_score01_missing", "score_02_adj", "flag_score02_missing"]
    telco_cols = (
        [f"var_{i:02d}_adj" for i in range(26, 94)] +
        [f"flag_var_{i:02d}_missing" for i in range(26, 94)]
    )
    demographic_cols = ["idade_anos", "cep_3_digitos"]
    demographic_flag_cols = ["flag_cep3_missing", "flag_idade_outlier", "flag_dt_nasc_invalid"]
    status_cols = ["statusrf", "prod", "flag_mig2"]
    numeric_cadastro_cols = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09", "var_10", "var_11", "var_16", "var_17"]
    numeric_cadastro_flag_cols = ["flag_var_11_neg"]
    categorical_cadastro_cols = ["var_15", "var_22", "var_23", "var_24", "var_25"]
    date_cadastro_cols = ["dt_var_12"]
    date_cadastro_flag_cols = ["flag_dt_var_12_invalid"]
    metadata_cols = ["metadata_source", "metadata_build_date", "metadata_audit_record_count"]
    
    # Build ordered select list
    all_cols = (
        key_cols +
        label_cols +
        score_cols +
        telco_cols +
        demographic_cols +
        demographic_flag_cols +
        status_cols +
        numeric_cadastro_cols +
        numeric_cadastro_flag_cols +
        categorical_cadastro_cols +
        date_cadastro_cols +
        date_cadastro_flag_cols +
        metadata_cols
    )
    
    df_abt = df_abt.select(*all_cols)
    print(f"  ✓ Selected {len(all_cols)} columns")
    
    # Step 6: Add Gold metadata
    print("\n[Step 6] Adding Gold metadata columns...")
    build_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_abt = df_abt.withColumn(
        "gold_version", F.lit("v4")
    ).withColumn(
        "gold_build_date", F.lit(build_date)
    ).withColumn(
        "gold_feature_blocks", F.lit("Scores (v1, v2) + Telco (v3) + Cadastro (v4)")
    )
    print(f"  ✓ Gold metadata added (version=v4, build_date={build_date})")
    
    print(f"\n[Step 7] Final ABT v4 structure:")
    print(f"  • Records: {abt_after_join_count:,}")
    print(f"  • Columns: {len(df_abt.columns)}")
    print(f"  • Feature blocks: Scores (2) + Telco (130) + Cadastro (33) + Metadata (3)")
    
    return df_abt


def main():
    parser = argparse.ArgumentParser(description="Build ABT v4 (Bureau + Scores + Telco + Cadastro)")
    parser.add_argument(
        "--input_bureau_path",
        type=str,
        default="/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/",
        help="Path to Silver Bureau Delta table"
    )
    parser.add_argument(
        "--input_telco_path",
        type=str,
        default="/Volumes/hackathon_2025/default/silver/telco_silver_delta/",
        help="Path to Silver Telco Delta table"
    )
    parser.add_argument(
        "--input_cadastro_path",
        type=str,
        default="/Volumes/hackathon_2025/default/silver/cadastro_silver_delta/",
        help="Path to Silver Cadastro Delta table"
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="/Volumes/hackathon_2025/default/gold/abt_v4_delta/",
        help="Path to write ABT v4 Gold Delta table"
    )
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = get_spark_session()
    
    try:
        # Build ABT v4
        df_abt_v4 = build_abt_v4(
            spark=spark,
            bureau_silver_path=args.input_bureau_path,
            telco_silver_path=args.input_telco_path,
            cadastro_silver_path=args.input_cadastro_path,
            output_path=args.output_path
        )
        
        # Validate v4
        print("\n" + "="*80)
        print("VALIDATION PHASE — Running 9 gates")
        print("="*80)
        validation_result = validate_abt_v4(df_abt_v4)
        
        if not validation_result["passed"]:
            print("\n❌ VALIDATION FAILED")
            for gate_name, gate_info in validation_result["gates"].items():
                if not gate_info["passed"]:
                    print(f"\n  {gate_name}:")
                    print(f"    {gate_info['message']}")
            sys.exit(1)
        
        print("\n✅ ALL GATES PASSED")
        
        # Write to Delta
        print("\n" + "="*80)
        print(f"WRITING ABT v4 to {args.output_path}")
        print("="*80)
        
        df_abt_v4.write.format("delta").mode("overwrite").save(args.output_path)
        
        print(f"\n✅ ABT v4 successfully written to {args.output_path}")
        print(f"   • Records: {df_abt_v4.count():,}")
        print(f"   • Columns: {len(df_abt_v4.columns)}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
