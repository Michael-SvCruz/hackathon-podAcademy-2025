# Arquivo: scripts/opc_standby/silver_pagamento_opt_z.py
# OPCAO Z: sem cache, quality na Silver JA ESCRITA + Coalesce DINAMICO
# Padrão vencedor de recarga aplicado ao pagamento.
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_pagamento_opt_z.py — OPCAO Z (sem cache + coalesce dinamico)
OBJETIVO: Bronze -> Silver Pagamento sem cache, quality sobre arquivo escrito,
          com numero de arquivos de saida calculado dinamicamente.
--------------------------------------------------------------------------------
ARQUITETURA (3 actions):
  action 1: count() pre-write   → calcula num_output_files (inclui shuffle dedup)
  action 2: coalesce(N).write() → grava Silver (re-executa shuffle dedup)
  action 3: agg(Silver escrita) → quality check no arquivo real

CALIBRACAO DO BYTES_PER_ROW_ESTIMATE:
  Valor atual: 4 bytes/registro (calibrado com base no 1o run: ~33,5M reg / ~120MB)
  Formula: BYTES_PER_ROW_ESTIMATE = tamanho_total_MB * 1024 * 1024 / count_registros
  Ajustar se o schema mudar ou a taxa de compressao variar.

DIFERENCA VS ORIGINAL (silver_pagamento.py):
  - 3 actions em vez de 7 (count_bronze + count_silver + 4 gates + write)
  - Coalesce dinamico: arquivos de ~128MB em vez de tamanho imprevisivel
  - Quality consolidado em unico agg() na Silver escrita
  - to_double_safe sem regex (cast direto)
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

def to_double_safe(colname):
    return F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == ""),
        F.lit(None)
    ).otherwise(
        F.col(colname).cast("double")
    )

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/pagamento/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/pagamento/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_pagamento_v1"

# Bytes estimados por registro na Silver (para coalesce dinamico)
# Calibrado com base no 2o run: ~30M registros → ~2.343MB Delta = ~85 bytes/reg
# (run anterior com BYTES=4 gerou coalesce(1) → arquivo de 2.29 GiB — muito grande)
# Formula: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros
BYTES_PER_ROW_ESTIMATE = 65
TARGET_FILE_SIZE_MB = 128
# =============================================================================

def build_silver_pagamento(df_bronze):
    """Transform Bronze -> Silver Pagamento (tipagem + dedup por versionamento)."""
    print(">>> [Transform] Construindo Silver Pagamento...")

    print("    -> Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_status_fatura",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FATURA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_pagamento",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_PAGAMENTO")), "ddMMMyyyy:HH:mm:ss")
    )

    print("    -> Step 2: Derivando SAFRA_PAGAMENTO...")
    df = df.withColumn(
        "safra_pagamento",
        F.date_format(F.to_date(F.col("ts_status_fatura")), "yyyyMM")
    )

    print("    -> Step 3: Casting valores monetarios...")
    monetary_cols = [
        "VAL_PAGAMENTO_FATURA", "VAL_PAGAMENTO_ITEM", "VAL_ATUAL_PAGAMENTO",
        "VAL_ORIGINAL_PAGAMENTO", "VAL_PAGAMENTO_CREDITO", "VAL_DESCONTO_ITEM",
        "VAL_JUROS_MULTAS_ITEM", "VAL_MULTA_EQUIP_ITEM", "VAL_MULTA_EQUIP_TOTAL",
        "VAL_MULTA_FID_ITEM", "VAL_BAIXA_ATIVIDADE"
    ]
    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(col.lower(), to_double_safe(col))

    print("    -> Step 4: Criando flags...")
    df = df.withColumn(
        "flag_ts_status_pagamento_missing",
        F.when(F.col("ts_status_pagamento").isNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "flag_juros_neg",
        F.when(
            F.col("val_juros_multas_item").isNotNull() & (F.col("val_juros_multas_item") < 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    ).withColumn(
        "val_juros_pos",
        F.greatest(F.col("val_juros_multas_item"), F.lit(0))
    ).withColumn(
        "val_juros_neg_abs",
        F.abs(F.least(F.col("val_juros_multas_item"), F.lit(0)))
    )

    print("    -> Step 5: Deduplicando por versionamento (Window)...")
    df = df.withColumn(
        "dedup_key",
        F.concat_ws("#", F.col("NUM_CPF"), F.col("CONTRATO"), F.col("SEQ_FATURA"),
                    F.col("NUM_SUB_SEQ_FATURA"), F.col("NUM_CREDITO_SEQ"))
    )
    window_spec = Window.partitionBy("dedup_key").orderBy(F.col("ts_status_fatura").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec))
    df_dedup = df.filter(F.col("rn") == 1).drop("rn", "dedup_key")

    print("    -> Step 6: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df_dedup)

    print("    -> Step 7: Adicionando metadados...")
    df_silver = df_silver \
        .withColumn("metadata_data_transformacao", F.current_timestamp()) \
        .withColumn("metadata_versao_regra", F.lit(SILVER_VERSION))

    return df_silver


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Pagamento - Opt-Z")
    parser.add_argument("--input_path", help="Caminho do Bronze Pagamento")
    parser.add_argument("--output_path", help="Caminho de destino Silver Pagamento")
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

    spark = SparkSession.builder.appName("Silver_Pagamento_OptZ").getOrCreate()

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Pagamento: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) BUILD SILVER
    # =========================================================================
    df_silver = build_silver_pagamento(df_bronze)

    # =========================================================================
    # 3) COALESCE DINAMICO — ACTION 1
    # Inclui o shuffle do Window (dedup por versionamento).
    # =========================================================================
    print(">>> [Coalesce] Contando registros pos-dedup para calcular coalesce dinamico...")
    count_pre = df_silver.count()
    print(f">>> [Info] Registros Silver (pos-dedup): {count_pre:,}")

    estimated_size_mb = count_pre * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Info] Coalesce dinamico: {num_output_files} arquivos (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f">>> [Info] Tamanho estimado total: {estimated_size_mb:.0f} MB")

    # =========================================================================
    # 4) ESCRITA SILVER — ACTION 2
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Pagamento (Delta): {args.output_path}")

    df_silver.coalesce(num_output_files) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(">>> [Escrita] Silver gravada com sucesso.")

    # =========================================================================
    # 5) QUALITY CHECKS na Silver JA ESCRITA — ACTION 3
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver gravada para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("ts_status_fatura").isNull(), 1).otherwise(0)).alias("invalidos_ts_fatura"),
        F.sum(F.when(F.col("num_cpf").isNull(), 1).otherwise(0)).alias("nulos_cpf"),
        F.sum(F.when(F.col("flag_juros_neg") == 1, 1).otherwise(0)).alias("juros_neg"),
        F.sum(F.when(F.col("flag_ts_status_pagamento_missing") == 1, 1).otherwise(0)).alias("missing_pag")
    ).collect()[0]

    count_out    = quality["total"]
    invalidos_ts = quality["invalidos_ts_fatura"]
    nulos_cpf    = quality["nulos_cpf"]
    juros_neg    = quality["juros_neg"]
    missing_pag  = quality["missing_pag"]

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER PAGAMENTO (OPT-Z)")
    print("="*80)
    print(f"\n    Registros Silver (pos-dedup):        {count_out:>12,}")
    print(f"\n    Gate 1 - TS_STATUS_FATURA invalidos: {invalidos_ts:>12,}")
    print(f"    Gate 2 - NUM_CPF nulos:              {nulos_cpf:>12,}")
    print(f"    Gate 3 - VAL_JUROS_NEG:              {juros_neg:>12,} ({100*juros_neg/count_out:.2f}%)")
    print(f"    Gate 4 - DAT_STATUS_PAG missing:     {missing_pag:>12,} ({100*missing_pag/count_out:.2f}%) [esperado ~28%]")

    print("\n" + "="*80)
    print("Silver PAGAMENTO concluido — opt_z (sem cache, coalesce dinamico)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  {num_output_files} (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f"  - Actions:            3 (count + write + agg Silver escrita)")
    print(f"  - BYTES_PER_ROW:      {BYTES_PER_ROW_ESTIMATE} (calibrado: ~30M reg / ~2.343MB)")
    print(f"  - Proximo passo:      gold_pagamento.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
