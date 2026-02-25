# Arquivo: scripts/opc_standby/silver_telco_opt_z.py
# OPCAO Z: sem cache, quality na Silver JA ESCRITA + Coalesce DINAMICO
# Padrao opt_z aplicado ao telco. Dedup via dropDuplicates (sem Window shuffle).
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_telco_opt_z.py — OPCAO Z
OBJETIVO: Bronze -> Silver Telco sem cache, quality sobre arquivo escrito,
          com numero de arquivos de saida calculado dinamicamente.
--------------------------------------------------------------------------------
PARTICULARIDADE TELCO vs PAGAMENTO:
  - EDA confirmou grain 1:1 por NUM_CPF + SAFRA (sem duplicatas de negocio).
  - O dedup original usa Window+row_number apenas como defesa contra re-runs
    de ingestao (nao e regra de negocio como em pagamento).
  - Portanto: substituimos por dropDuplicates(["num_cpf", "safra"]), que e
    mais barato (sem shuffle ordenado) e suficiente para o caso de uso.
  - Se duplicatas forem raras (EDA = 0), o ganho e modesto mas sem custo.

SCHEMA LARGO:
  - 68 var_* → 136 colunas derivadas (var_X_adj + flag_var_X_missing)
  - BYTES_PER_ROW_ESTIMATE precisa ser calibrado apos 1o run
    (schema mais largo que recarga/pagamento → bytes/registro maior)
  - Formula: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros

ARQUITETURA (3 actions):
  action 1: count() pre-write   → calcula num_output_files
  action 2: coalesce(N).write() → grava Silver
  action 3: agg(Silver escrita) → quality check no arquivo real

DIFERENCA VS ORIGINAL (silver_telco.py):
  - dropDuplicates em vez de Window+row_number (sem shuffle ordenado)
  - 3 actions em vez de 5+ (count_in + count_out + write + 4 quality counts)
  - Coalesce dinamico (~128MB por arquivo)
  - Quality consolidado em unico agg() na Silver escrita
  - to_double_safe sem regex (cast direto)
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

# Bytes estimados por registro na Silver (para coalesce dinamico)
# NOTA: Telco tem schema largo (~200+ colunas apos derivadas) → bytes/reg alto
# Calibrar apos 1o run: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros
# Valor inicial conservador — ajustar conforme resultado real
BYTES_PER_ROW_ESTIMATE = 200
TARGET_FILE_SIZE_MB = 128

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
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Telco - Opt-Z")
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

    spark = SparkSession.builder.appName("Silver_Telco_OptZ").getOrCreate()

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
    # EDA confirmou grain 1:1 por NUM_CPF + SAFRA (nao e regra de negocio).
    # dropDuplicates e suficiente: mais barato que Window+row_number porque
    # nao requer shuffle com ordenacao — apenas hash partition.
    # =========================================================================
    print(">>> [Dedup] Aplicando dropDuplicates por num_cpf + safra...")
    df_silver = df_silver.dropDuplicates(["num_cpf", "safra"])

    # =========================================================================
    # 4) COALESCE DINAMICO — ACTION 1
    # =========================================================================
    print(">>> [Coalesce] Contando registros para calcular coalesce dinamico...")
    count_pre = df_silver.count()
    print(f">>> [Info] Registros Silver (pos-dedup): {count_pre:,}")

    estimated_size_mb = count_pre * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Info] Coalesce dinamico: {num_output_files} arquivos (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f">>> [Info] Tamanho estimado total: {estimated_size_mb:.0f} MB")
    print(f">>> [Info] ATENCAO: BYTES_PER_ROW_ESTIMATE={BYTES_PER_ROW_ESTIMATE} e estimativa inicial.")
    print(f"           Calibrar apos 1o run: BYTES = tamanho_real_MB * 1024 * 1024 / {count_pre}")

    # =========================================================================
    # 5) ESCRITA SILVER — ACTION 2
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Telco (Delta): {args.output_path}")

    df_silver.coalesce(num_output_files) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(">>> [Escrita] Silver gravada com sucesso.")

    # =========================================================================
    # 6) QUALITY CHECKS na Silver JA ESCRITA — ACTION 3
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver gravada para validacao: {args.output_path}")
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
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER TELCO (OPT-Z)")
    print("="*80)
    print(f"\n    Registros Silver (pos-dedup):         {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):     {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                         {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - FLAG_INSTALACAO invalida:    {flag_inst_inv:>12,}")
    print(f"    Gate 2 - FPD invalido:                {fpd_inv:>12,}")
    print(f"    Gate 3 - FPD nulo:                    {fpd_null:>12,}  ({100*fpd_null/count_out:.2f}%) [esperado ~3.36%]")

    print("\n" + "="*80)
    print("Silver TELCO concluido — opt_z (sem cache, dropDuplicates, coalesce dinamico)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  {num_output_files} (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f"  - Actions:            3 (count + write + agg Silver escrita)")
    print(f"  - Dedup:              dropDuplicates (EDA confirmou grain 1:1)")
    print(f"  - BYTES_PER_ROW:      {BYTES_PER_ROW_ESTIMATE} (estimativa inicial — calibrar apos run)")
    print(f"  - Proximo passo:      gold_recarga.py / gold_pagamento.py / gold_atraso.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
