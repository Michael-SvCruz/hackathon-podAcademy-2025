# Arquivo: scripts/opc_standby/silver_bureau_opt_optimize.py
# OPCAO OPTIMIZE: opt_z + Delta OPTIMIZE apos escrita
# Elimina necessidade de calibrar BYTES_PER_ROW_ESTIMATE manualmente.
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_bureau_opt_optimize.py — OPCAO OPTIMIZE
OBJETIVO: Bronze -> Silver Bureau com Delta OPTIMIZE pos-escrita.
--------------------------------------------------------------------------------
PARTICULARIDADES BUREAU:
  - Spine oficial: grain 1:1 → dedup via dropDuplicates (sem Window shuffle).
  - Schema enxuto — bureau provavelmente gera arquivo pequeno (poucos arquivos).
  - SCORE_01 = 0 tratado como sentinela → score_01_adj + flag_score01_missing.

ARQUITETURA (2 actions + OPTIMIZE):
  action 1: coalesce(N).write() → grava Silver
  OPTIMIZE → Delta reorganiza para ~128MB por arquivo
  action 2: agg(Silver pos-OPTIMIZE) → quality check no arquivo real
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# =============================================================================
# UTILITY FUNCTIONS (inlined from spark_utils.py para OCI Data Flow)
# =============================================================================

def standardize_column_names(df):
    new_cols = []
    for col in df.columns:
        clean_col = col.lower().strip() \
            .replace(" ", "_") \
            .replace("/", "_") \
            .replace(".", "") \
            .replace("ç", "c") \
            .replace("ã", "a")
        new_cols.append(clean_col)
    return df.toDF(*new_cols)

def to_int_safe(colname):
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

def to_double_safe(colname):
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("double"))

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/bureau/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/bureau/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_bureau_full_v1"

# Tamanho alvo por arquivo apos OPTIMIZE
TARGET_FILE_SIZE_MB = 128

# Coalesce inicial antes do OPTIMIZE (generoso — OPTIMIZE vai reorganizar)
INITIAL_COALESCE = 20

# VACUUM: remover arquivos pre-OPTIMIZE
VACUUM_RETENTION_HOURS = 0
# =============================================================================


def build_silver(df_bronze):
    """Transform Bronze -> Silver Bureau (tipagem + sentinela score + derivadas)."""
    print(">>> [Transform] Construindo Silver Bureau...")

    print("    -> Step 1: Tipagem basica...")
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
        .withColumn("score_01_dbl", to_double_safe("score_01"))
        .withColumn("score_02_dbl", to_double_safe("score_02"))
    )

    print("    -> Step 2: Derivando DT_SAFRA...")
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )

    print("    -> Step 3: Tratamento sentinela SCORE_01 = 0...")
    df = df.withColumn(
        "flag_score01_missing",
        F.when(F.col("score_01_dbl").isNull() | (F.col("score_01_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "score_01_adj",
        F.when(F.col("score_01_dbl") == 0, F.lit(None)).otherwise(F.col("score_01_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    print("    -> Step 4: Quality flags de dominio...")
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when(~F.col("flag_instalacao_int").isin(0, 1) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when(~F.col("fpd_int").isin(0, 1) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    print("    -> Step 5: Adicionando metadados...")
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit(SILVER_VERSION))

    print("    -> Step 6: Selecao de colunas finais...")
    df_silver = df.select(
        "num_cpf", "safra", "dt_safra",
        "flag_instalacao_int", "fpd_int",
        "score_01_adj", "flag_score01_missing",
        "score_02_dbl", "flag_score02_missing",
        "prod", "flag_mig2",
        "flag_instalacao_invalida", "fpd_invalido",
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    return df_silver


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Bureau - Opt-Optimize")
    parser.add_argument("--input_path", help="Caminho Bronze Bureau")
    parser.add_argument("--output_path", help="Caminho destino Silver Bureau")
    parser.add_argument("--format", default=DEFAULT_FORMAT)

    args_parsed, _ = parser.parse_known_args()

    if args_parsed.input_path and args_parsed.output_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("Silver_Bureau_Optimize").getOrCreate()

    # Necessario para VACUUM com retencao 0h
    if VACUUM_RETENTION_HOURS == 0:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Alvo de bin-packing do OPTIMIZE: 128MB por arquivo
    # Default Delta open-source = 1GB — sem isso, gera arquivos de ~1GB
    spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE_MB * 1024 * 1024))

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Bureau: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)

    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # =========================================================================
    # 2) BUILD SILVER
    # =========================================================================
    df_silver = build_silver(df_bronze)

    # =========================================================================
    # 3) DEDUP — dropDuplicates (sem Window shuffle)
    # Bureau e o Spine oficial: grain 1:1 — dedup defensivo contra re-runs.
    # =========================================================================
    print(">>> [Dedup] Aplicando dropDuplicates por num_cpf + safra...")
    df_silver = df_silver.dropDuplicates(["num_cpf", "safra"])

    # =========================================================================
    # 4) ESCRITA SILVER — ACTION 1
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Bureau (Delta): {args.output_path}")
    print(f">>> [Info] Coalesce inicial: {INITIAL_COALESCE} particoes (sera otimizado pelo OPTIMIZE)")

    df_silver.coalesce(INITIAL_COALESCE) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(">>> [Escrita] Silver gravada com sucesso.")

    # =========================================================================
    # 5) DELTA OPTIMIZE (bin-packing para ~128MB por arquivo)
    # =========================================================================
    print(f"\n>>> [Optimize] Executando Delta OPTIMIZE em: {args.output_path}")
    dt = DeltaTable.forPath(spark, args.output_path)
    dt.optimize().executeCompaction()
    print(">>> [Optimize] Compactacao concluida.")

    print(f">>> [Vacuum] Removendo arquivos antigos (retencao: {VACUUM_RETENTION_HOURS}h)...")
    dt.vacuum(VACUUM_RETENTION_HOURS)
    print(">>> [Vacuum] Limpeza concluida.")

    # =========================================================================
    # 6) QUALITY CHECKS na Silver pos-OPTIMIZE — ACTION 2
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver pos-OPTIMIZE para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.countDistinct("num_cpf", "safra").alias("distinct_keys"),
        F.sum(F.when(F.col("flag_instalacao_invalida") == 1, 1).otherwise(0)).alias("flag_inst_invalida"),
        F.sum(F.when(F.col("fpd_invalido") == 1, 1).otherwise(0)).alias("fpd_invalido"),
        F.sum(F.when(F.col("flag_score01_missing") == 1, 1).otherwise(0)).alias("score01_missing"),
        F.sum(F.when(F.col("flag_score02_missing") == 1, 1).otherwise(0)).alias("score02_missing"),
        F.sum(F.when(F.col("fpd_int").isNull(), 1).otherwise(0)).alias("fpd_null"),
    ).collect()[0]

    count_out       = quality["total"]
    distinct_keys   = quality["distinct_keys"]
    flag_inst_inv   = quality["flag_inst_invalida"]
    fpd_inv         = quality["fpd_invalido"]
    score01_miss    = quality["score01_missing"]
    score02_miss    = quality["score02_missing"]
    fpd_null        = quality["fpd_null"]
    score01_cov     = 100 * (count_out - score01_miss) / count_out if count_out > 0 else 0
    score02_cov     = 100 * (count_out - score02_miss) / count_out if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER BUREAU (OPT-OPTIMIZE)")
    print("="*80)
    print(f"\n    Registros Silver (pos-dedup):         {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):     {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                         {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - FLAG_INSTALACAO invalida:    {flag_inst_inv:>12,}")
    print(f"    Gate 2 - FPD invalido:                {fpd_inv:>12,}")
    print(f"    Gate 3 - FPD nulo:                    {fpd_null:>12,}")
    print(f"\n    Gate 4 - SCORE_01 missing (=0/null):  {score01_miss:>12,}  (cobertura: {score01_cov:.2f}%) [esperado ~98.18%]")
    print(f"    Gate 5 - SCORE_02 missing:            {score02_miss:>12,}  (cobertura: {score02_cov:.2f}%) [esperado ~99.95%]")

    print("\n" + "="*80)
    print("Silver BUREAU concluido — opt_optimize (dropDuplicates + OPTIMIZE ~128MB)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  ~{TARGET_FILE_SIZE_MB}MB cada (garantido pelo OPTIMIZE)")
    print(f"  - Actions:            2 (write + agg pos-OPTIMIZE)")
    print(f"  - Dedup:              dropDuplicates (Spine 1:1 por NUM_CPF+SAFRA)")
    print(f"  - OPTIMIZE:           sim (targetFileSize={TARGET_FILE_SIZE_MB}MB)")
    print(f"  - VACUUM:             sim ({VACUUM_RETENTION_HOURS}h retencao)")
    print(f"  - Proximo passo:      gold_recarga.py / gold_pagamento.py / gold_atraso.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
