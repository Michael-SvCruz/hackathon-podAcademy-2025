# Arquivo: scripts/opc_standby/silver_recarga_opt_x.py
# OPCAO X: cache mantido, count() removido e embutido no agg()
# Comparar com silver_recarga.py (baseline 18m25s)
# Ganho esperado: ~30-60s (elimina 1 cache scan de 100M registros)
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_recarga_opt_x.py — OPCAO X (benchmark)
OBJETIVO: Bronze -> Silver sem count() separado.
--------------------------------------------------------------------------------
DIFERENCA VS PRINCIPAL (silver_recarga.py):
- count() separado REMOVIDO
- F.count("*") embutido no agg() existente (mesmo numero de actions, mas
  a contagem é feita junto com os quality checks — 1 cache scan a menos)
- Coalesce FIXO em 28 (valor conhecido: ~3.6GB / 128MB)
- 2 actions: write + agg_com_count (vs 3 no principal)

FLUXO:
  Bronze → cache() → write (action 1) → agg+count (action 2)
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/recarga/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/recarga/"
DEFAULT_FORMAT = "delta"

VALOR_COLUMNS = ["val_credito_inserido", "val_bonus", "val_real", "valor_sos"]

CODIGO_COLUMNS = [
    "cod_tecnologia_dw", "cod_tipo_credito", "dw_tipo_insercao",
    "dw_tipo_recarga", "dw_forma_pagamento", "cod_plataforma_atu",
    "cod_status_plataforma", "cod_canal_aquisicao", "dw_instituicao",
    "dw_plano_tarifacao", "cod_promocao"
]

SENTINELAS = [-1, -2, -3]

# Coalesce fixo: ~3.6GB / 128MB = 28 particoes
# Evita count() separado para calcular dinamicamente
NUM_OUTPUT_FILES = 28
# =============================================================================

def build_silver(df_bronze):
    """Identica ao principal (silver_recarga.py)."""
    print(">>> [Transform] Tipagem + regras Silver (recarga evento-level)...")

    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("dw_num_cliente", F.col("dw_num_cliente").cast("string"))
        .withColumn("dw_num_ntc", F.col("dw_num_ntc").cast("string"))
        .withColumn("flag_sos", to_int_safe("flag_sos"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao") if "flag_instalacao" in df_bronze.columns else F.lit(None).cast("int"))
        .withColumn("fpd_int", to_int_safe("fpd") if "fpd" in df_bronze.columns else F.lit(None).cast("int"))
    )

    print(">>> [Transform] Parseando DAT_INSERCAO_CREDITO com tolerancia a invalidos...")

    df = df.withColumn(
        "ts_recarga",
        F.when(
            F.col("dat_insercao_credito").isNotNull() & (F.trim(F.col("dat_insercao_credito")) != F.lit("")),
            F.to_timestamp(F.col("dat_insercao_credito"), "ddMMMyyyy:HH:mm:ss")
        ).otherwise(None)
    )

    df = df.withColumn(
        "flag_ts_recarga_invalida",
        F.when(
            (F.col("dat_insercao_credito").isNotNull()) &
            (F.trim(F.col("dat_insercao_credito")) != F.lit("")) &
            (F.col("ts_recarga").isNull()),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "dt_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.to_date(F.col("ts_recarga"))).otherwise(None)
    ).withColumn(
        "safra_recarga",
        F.when(F.col("dt_recarga").isNotNull(), F.date_format(F.col("dt_recarga"), "yyyyMM")).otherwise(None)
    )

    print(">>> [Transform] Tipando valores monetarios e criando flags de negativos...")

    for val_col in VALOR_COLUMNS:
        if val_col in df.columns:
            df = (
                df
                .withColumn(f"{val_col}", to_double_safe(val_col))
                .withColumn(f"flag_{val_col}_negativo", F.when(F.col(val_col) < 0, 1).otherwise(0))
                .withColumn(f"{val_col}_clean", F.when(F.col(val_col) < 0, None).otherwise(F.col(val_col)))
            )

    print(">>> [Transform] Tratando sentinelas em codigos dimensionais...")

    for cod_col in CODIGO_COLUMNS:
        if cod_col in df.columns:
            df = (
                df
                .withColumn(cod_col, to_int_safe(cod_col))
                .withColumn(f"flag_{cod_col}_sentinela", F.when(F.col(cod_col).isin(*SENTINELAS), 1).otherwise(0))
            )

    df = df.withColumn(
        "flag_sos_invalida",
        F.when((~F.col("flag_sos").isin(0, 1)) & F.col("flag_sos").isNotNull(), 1).otherwise(0)
    )

    if "flag_instalacao_int" in df.columns:
        df = df.withColumn(
            "flag_instalacao_invalida",
            F.when((~F.col("flag_instalacao_int").isin(0, 1)) & F.col("flag_instalacao_int").isNotNull(), 1).otherwise(0)
        )

    if "fpd_int" in df.columns:
        df = df.withColumn(
            "fpd_invalido",
            F.when((~F.col("fpd_int").isin(0, 1)) & F.col("fpd_int").isNotNull(), 1).otherwise(0)
        )

    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_recarga_v1"))

    columns_to_select = [
        "num_cpf", "dw_num_cliente", "dw_num_ntc",
        "ts_recarga", "dt_recarga", "safra_recarga", "flag_ts_recarga_invalida",
        "flag_instalacao_int", "flag_instalacao_invalida", "fpd_int", "fpd_invalido",
        "val_credito_inserido", "flag_val_credito_inserido_negativo", "val_credito_inserido_clean",
        "val_bonus", "flag_val_bonus_negativo", "val_bonus_clean",
        "val_real", "flag_val_real_negativo", "val_real_clean",
        "flag_sos", "flag_sos_invalida", "valor_sos",
        *[f"{cod}" for cod in CODIGO_COLUMNS if cod in df.columns],
        *[f"flag_{cod}_sentinela" for cod in CODIGO_COLUMNS if cod in df.columns],
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao", "metadata_versao_regra"
    ]

    columns_to_select = list(dict.fromkeys(columns_to_select))
    existing_columns = [col for col in columns_to_select if col in df.columns]
    return df.select(existing_columns)


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - OPCAO X (sem count separado)")
    parser.add_argument("--input_path", help="Caminho da Bronze (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino na Silver (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT)

    args_parsed, _ = parser.parse_known_args()

    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("Silver_Recarga_OptX").getOrCreate()

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) PADRONIZACAO + TRANSFORM
    # =========================================================================
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)
    df_silver = build_silver(df_bronze)

    # =========================================================================
    # 3) CACHE
    # Sem count() separado — Bronze lida 1x ao materializar o cache na escrita.
    # count_out vem do agg() de qualidade (action 2), eliminando 1 cache scan.
    # =========================================================================
    df_silver.cache()

    # =========================================================================
    # 4) ESCRITA (action 1 — materializa o cache)
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")
    print(f">>> [Info] Coalesce fixo: {NUM_OUTPUT_FILES} arquivos (~128MB cada)")

    df_silver.coalesce(NUM_OUTPUT_FILES) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # 5) QUALITY CHECKS + COUNT (action 2 — le do cache)
    # F.count("*") embutido no agg: 1 passada para tudo.
    # =========================================================================
    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER RECARGA (OPT-X)")
    print("="*80)

    quality_metrics = df_silver.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("flag_ts_recarga_invalida") == 1, 1).otherwise(0)).alias("ts_invalida"),
        *[F.sum(F.when(F.col(f"flag_{val_col}_negativo") == 1, 1).otherwise(0)).alias(f"neg_{val_col}")
          for val_col in VALOR_COLUMNS if f"flag_{val_col}_negativo" in df_silver.columns],
        *[F.sum(F.when(F.col(f"flag_{cod_col}_sentinela") == 1, 1).otherwise(0)).alias(f"sent_{cod_col}")
          for cod_col in CODIGO_COLUMNS if f"flag_{cod_col}_sentinela" in df_silver.columns],
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("sos_presente"),
        F.sum(F.when(F.col("flag_instalacao_int").isNull(), 1).otherwise(0)).alias("flag_null"),
        F.sum(F.when(F.col("flag_instalacao_int") == 0, 1).otherwise(0)).alias("flag_0"),
        F.sum(F.when(F.col("flag_instalacao_int") == 1, 1).otherwise(0)).alias("flag_1")
    ).collect()[0]

    count_out = quality_metrics["total"]

    ts_invalida = quality_metrics["ts_invalida"]
    ts_valida = count_out - ts_invalida
    ts_coverage = 100 * ts_valida / count_out if count_out > 0 else 0
    print(f"\n>>> [Quality] Parsing de data:")
    print(f"    TS_RECARGA valida:   {ts_valida:>12} ({ts_coverage:.2f}%)")
    print(f"    TS_RECARGA invalida: {ts_invalida:>12}")

    print(f"\n>>> [Quality] Valores negativos:")
    for val_col in VALOR_COLUMNS:
        if f"flag_{val_col}_negativo" in df_silver.columns:
            neg_count = quality_metrics[f"neg_{val_col}"]
            print(f"    {val_col}_negativo: {neg_count:>12} ({100*neg_count/count_out:.2f}%)")

    print(f"\n>>> [Quality] Sentinelas em codigos dimensionais (-1/-2/-3):")
    for cod_col in CODIGO_COLUMNS:
        if f"flag_{cod_col}_sentinela" in df_silver.columns:
            sent_count = quality_metrics[f"sent_{cod_col}"]
            print(f"    {cod_col}_sentinela: {sent_count:>12} ({100*sent_count/count_out:.2f}%)")

    sos_presente = quality_metrics["sos_presente"]
    print(f"\n>>> [Quality] SOS:")
    print(f"    Eventos com SOS: {sos_presente:>12} ({100*sos_presente/count_out:.2f}%)")

    flag_null = quality_metrics["flag_null"]
    flag_0    = quality_metrics["flag_0"]
    flag_1    = quality_metrics["flag_1"]
    print(f"\n>>> [Quality] FLAG_INSTALACAO:")
    print(f"    Nulo:   {flag_null:>12}")
    print(f"    FLAG=0: {flag_0:>12} ({100*flag_0/count_out:.2f}%)")
    print(f"    FLAG=1: {flag_1:>12} ({100*flag_1/count_out:.2f}%)")

    df_silver.unpersist()

    print("\n" + "="*80)
    print(f"Silver RECARGA concluido — OPCAO X (cache + agg-com-count)")
    print(f"  - Registros:          {count_out}")
    print(f"  - Arquivos de saida:  {NUM_OUTPUT_FILES} (~128MB cada)")
    print(f"  - Actions:            2 (write + agg)")
    print(f"  - Dedupe:             removida (Gold groupBy absorve 0.3%)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
