# Arquivo: scripts/opc_standby/silver_telco_opt_optimize.py
# OPCAO OPTIMIZE: opt_z + Delta OPTIMIZE apos escrita
# Elimina necessidade de calibrar BYTES_PER_ROW_ESTIMATE manualmente.
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_telco_opt_optimize.py — OPCAO OPTIMIZE
OBJETIVO: Bronze -> Silver Telco com Delta OPTIMIZE pos-escrita.
--------------------------------------------------------------------------------
PARTICULARIDADE TELCO:
  - EDA confirmou grain 1:1 → dedup via dropDuplicates (sem Window shuffle).
  - Schema largo (~200+ colunas) → BYTES_PER_ROW dificil de estimar.
    O OPTIMIZE e especialmente util aqui pois calibra automaticamente.

ARQUITETURA (2 actions + OPTIMIZE):
  action 1: coalesce(N).write() → grava Silver
  OPTIMIZE → Delta reorganiza fisicamente os arquivos para ~128MB cada
  action 2: agg(Silver pos-OPTIMIZE) → quality check no arquivo real

VANTAGEM VS OPT-Z EM TELCO:
  + Schema largo → BYTES_PER_ROW dificil de estimar manualmente
  + OPTIMIZE garante ~128MB independente do volume e schema
  + Especialmente util se o schema telco mudar entre runs

DESVANTAGEM:
  - 1 operacao extra (OPTIMIZE varre e reescreve os arquivos)
  - VACUUM necessario para limpar arquivos pre-OPTIMIZE

QUANDO USAR:
  Quando BYTES_PER_ROW_ESTIMATE for impreciso (schema largo ou volume variavel).
  Para volumes estaveis e calibrados, opt_z e mais eficiente.
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
    return F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == ""),
        F.lit(None)
    ).otherwise(
        F.col(colname).cast("double")
    )

def treat_sentinel_value(colname, sentinel_values=[304]):
    sentinel_str_values = [str(s) for s in sentinel_values]
    sentinel_condition = F.col(colname).isin(sentinel_str_values)
    expr_treated = F.when(
        F.col(colname).isNull() |
        (F.trim(F.col(colname)) == "") |
        sentinel_condition,
        F.lit(None)
    ).otherwise(F.col(colname).cast("double"))
    expr_flag = F.when(
        F.col(colname).isNull() |
        (F.trim(F.col(colname)) == "") |
        sentinel_condition,
        F.lit(1)
    ).otherwise(F.lit(0))
    return {
        "colname_treated": f"{colname}_adj",
        "flag_name": f"flag_{colname}_missing",
        "expr_treated": expr_treated,
        "expr_flag": expr_flag
    }

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/telco/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/telco/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_telco_v1"

# Tamanho alvo por arquivo apos OPTIMIZE
TARGET_FILE_SIZE_MB = 128

# Coalesce inicial antes do OPTIMIZE (generoso — OPTIMIZE vai reorganizar)
INITIAL_COALESCE = 20

# VACUUM: remover arquivos pre-OPTIMIZE
VACUUM_RETENTION_HOURS = 0

# Lista de colunas var_* esperadas (var_26 a var_93 = 68 colunas)
VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# =============================================================================


def build_silver(df_bronze):
    """Transform Bronze -> Silver Telco (tipagem + sentinela 304 + derivadas)."""
    print(">>> [Transform] Construindo Silver Telco...")

    print("    -> Step 1: Tipagem basica...")
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
    )

    print("    -> Step 2: Derivando DT_SAFRA...")
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )

    print("    -> Step 3: Tratando sentinela 304 em var_* (68 colunas)...")
    for var_col in VAR_COLUMNS:
        if var_col in df.columns:
            treatment = treat_sentinel_value(var_col, sentinel_values=[304])
            df = df \
                .withColumn(treatment["colname_treated"], treatment["expr_treated"]) \
                .withColumn(treatment["flag_name"], treatment["expr_flag"])
        else:
            print(f"!!! AVISO: Coluna {var_col} nao encontrada no DataFrame.")

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
        "prod", "flag_mig2",
        *[f"var_{i}_adj" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],
        *[f"flag_var_{i}_missing" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],
        "flag_instalacao_invalida", "fpd_invalido",
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    return df_silver


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Telco - Opt-Optimize")
    parser.add_argument("--input_path", help="Caminho Bronze Telco")
    parser.add_argument("--output_path", help="Caminho destino Silver Telco")
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

    spark = SparkSession.builder.appName("Silver_Telco_Optimize").getOrCreate()

    # Necessario para VACUUM com retencao 0h
    if VACUUM_RETENTION_HOURS == 0:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Alvo de bin-packing do OPTIMIZE: 128MB por arquivo
    # Default Delta open-source = 1GB — sem isso, gera arquivos de ~1GB
    spark.conf.set("spark.databricks.delta.targetFileSize", str(TARGET_FILE_SIZE_MB * 1024 * 1024))

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Telco: {args.input_path}")
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
    # EDA confirmou grain 1:1 — dedup e defensivo contra re-runs de ingestao.
    # =========================================================================
    print(">>> [Dedup] Aplicando dropDuplicates por num_cpf + safra...")
    df_silver = df_silver.dropDuplicates(["num_cpf", "safra"])

    # =========================================================================
    # 4) ESCRITA SILVER — ACTION 1
    # Coalesce inicial generoso: o OPTIMIZE vai reorganizar para ~128MB.
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Telco (Delta): {args.output_path}")
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
    # targetFileSize configurado acima para 128MB (corrige default 1GB)
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
        F.sum(F.when(F.col("fpd_int").isNull(), 1).otherwise(0)).alias("fpd_null"),
    ).collect()[0]

    count_out       = quality["total"]
    distinct_keys   = quality["distinct_keys"]
    flag_inst_inv   = quality["flag_inst_invalida"]
    fpd_inv         = quality["fpd_invalido"]
    fpd_null        = quality["fpd_null"]

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER TELCO (OPT-OPTIMIZE)")
    print("="*80)
    print(f"\n    Registros Silver (pos-dedup):         {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):     {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                         {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - FLAG_INSTALACAO invalida:    {flag_inst_inv:>12,}")
    print(f"    Gate 2 - FPD invalido:                {fpd_inv:>12,}")
    print(f"    Gate 3 - FPD nulo:                    {fpd_null:>12,}  ({100*fpd_null/count_out:.2f}%) [esperado ~3.36%]")

    print("\n" + "="*80)
    print("Silver TELCO concluido — opt_optimize (dropDuplicates + OPTIMIZE ~128MB)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  ~{TARGET_FILE_SIZE_MB}MB cada (garantido pelo OPTIMIZE)")
    print(f"  - Actions:            2 (write + agg pos-OPTIMIZE)")
    print(f"  - Dedup:              dropDuplicates (EDA confirmou grain 1:1)")
    print(f"  - OPTIMIZE:           sim (targetFileSize={TARGET_FILE_SIZE_MB}MB)")
    print(f"  - VACUUM:             sim ({VACUUM_RETENTION_HOURS}h retencao)")
    print(f"  - Proximo passo:      gold_recarga.py / gold_pagamento.py / gold_atraso.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
