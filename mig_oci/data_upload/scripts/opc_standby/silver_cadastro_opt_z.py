# Arquivo: scripts/opc_standby/silver_cadastro_opt_z.py
# OPCAO Z: sem cache, quality na Silver JA ESCRITA + Coalesce DINAMICO
# Padrao opt_z aplicado ao cadastro. Dedup via dropDuplicates (sem Window shuffle).
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_cadastro_opt_z.py — OPCAO Z
OBJETIVO: Bronze -> Silver Cadastro sem cache, quality sobre arquivo escrito,
          com numero de arquivos de saida calculado dinamicamente.
--------------------------------------------------------------------------------
PARTICULARIDADES CADASTRO:

  DEDUP:
  - EDA confirmou grain 1:1 por NUM_CPF + SAFRA (sem duplicatas de negocio).
  - Dedup original usa Window+row_number apenas como defesa contra re-runs.
  - Substituido por dropDuplicates(["num_cpf", "safra"]) — sem shuffle ordenado.

  DATADENASCIMENTO (CRITICO):
  - Parse via F.coalesce(F.to_date(...)) com 4 formatos tolerantes.
  - NUNCA usar Python UDF aqui — falha silenciosa em execucao distribuida.
  - Lição aprendida em Jan/2026 (UDF serializacao → idade_anos vazio).

  QUALITY:
  - Original tinha 5 count() separados (5 actions desnecessarias).
  - Substituido por 1 agg() na Silver ja escrita (1 action = leitura real do bucket).

  SCHEMA:
  - ~35 colunas (menor que telco/pagamento) → BYTES_PER_ROW menor.
  - Calibrar apos 1o run: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros.

ARQUITETURA (3 actions):
  action 1: count() pre-write   → calcula num_output_files
  action 2: coalesce(N).write() → grava Silver
  action 3: agg(Silver escrita) → quality check no arquivo real

DIFERENCA VS ORIGINAL (silver_cadastro.py):
  - dropDuplicates em vez de Window+row_number (sem shuffle ordenado)
  - 3 actions em vez de 8+ (count_in + count_out + 5 quality counts + write)
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

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/cadastro/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/cadastro/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_cadastro_v1"

# Constants para sanity checks de idade
IDADE_MINIMA_VALIDA = 18
IDADE_MAXIMA_ESPERADA = 100

# Variaveis numericas confirmadas (0 nao numericos no data quality check)
NUMERIC_VARS = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09"]

# Variaveis categoricas (alto volume de nao numericos)
CATEGORICAL_VARS = ["var_15", "var_22", "var_23", "var_24", "var_25"]

# Variaveis mistas/outras (trim simples)
MIXED_VARS = ["var_02", "var_10", "var_11", "var_12", "var_13", "var_14",
              "var_16", "var_17", "var_18", "var_19", "var_20", "var_21"]

# Bytes estimados por registro na Silver (para coalesce dinamico)
# Cadastro tem schema menor que telco/pagamento (~35 colunas)
# Calibrar apos 1o run: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros
BYTES_PER_ROW_ESTIMATE = 150
TARGET_FILE_SIZE_MB = 128
# =============================================================================


def build_silver(df_bronze):
    """Transform Bronze -> Silver Cadastro (tipagem + derivadas + sanity checks)."""
    print(">>> [Transform] Construindo Silver Cadastro...")

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

    print("    -> Step 3: Parseando DATADENASCIMENTO (F.coalesce nativo — sem UDF)...")
    # CRITICO: usar F.to_date() nativo do Spark. NUNCA Python UDF (falha silenciosa).
    # Licao aprendida Jan/2026: UDF serializacao → idade_anos vazio em prod.
    df = df.withColumn(
        "dt_nasc",
        F.coalesce(
            F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"),
            F.to_date(F.col("datadenascimento"), "dd-MM-yyyy"),
            F.to_date(F.col("datadenascimento"), "yyyy-MM-dd"),
            F.to_date(F.col("datadenascimento"), "ddMMyyyy"),
            F.lit(None).cast("date")
        )
    ).withColumn(
        "flag_dt_nasc_invalida",
        F.when(
            F.col("datadenascimento").isNotNull() &
            (F.trim(F.col("datadenascimento")) != F.lit("")) &
            F.col("dt_nasc").isNull(),
            1
        ).otherwise(0)
    )

    print("    -> Step 4: Derivando IDADE_ANOS e flags de sanity...")
    df = df.withColumn(
        "idade_anos",
        F.when(
            F.col("dt_nasc").isNotNull(),
            F.floor(F.months_between(F.col("dt_safra"), F.col("dt_nasc")) / 12)
        ).otherwise(None)
    ).withColumn(
        "flag_idade_menor_18",
        F.when(F.col("idade_anos") < IDADE_MINIMA_VALIDA, 1).otherwise(0)
    ).withColumn(
        "flag_idade_muito_alta",
        F.when(F.col("idade_anos") > IDADE_MAXIMA_ESPERADA, 1).otherwise(0)
    )

    print("    -> Step 5: CEP e STATUSRF...")
    df = df.withColumn("cep_3_digitos", F.trim(F.col("cep_3_digitos"))) \
           .withColumn(
               "flag_cep_missing",
               F.when(
                   F.col("cep_3_digitos").isNull() | (F.col("cep_3_digitos") == F.lit("")),
                   1
               ).otherwise(0)
           ) \
           .withColumn("statusrf", F.trim(F.upper(F.col("statusrf"))))

    print("    -> Step 6: Casting variaveis numericas, categoricas e mistas...")
    for var in NUMERIC_VARS:
        if var in df.columns:
            df = df.withColumn(var, to_double_safe(var))
    for var in CATEGORICAL_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.upper(F.col(var))))
    for var in MIXED_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.col(var)))

    print("    -> Step 7: Quality flags de dominio...")
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when((~F.col("flag_instalacao_int").isin(0, 1)) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when((~F.col("fpd_int").isin(0, 1)) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    print("    -> Step 8: Adicionando metadados...")
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit(SILVER_VERSION))

    print("    -> Step 9: Selecao de colunas finais...")
    base_cols = [
        "num_cpf", "safra", "dt_safra",
        "flag_instalacao_int", "fpd_int",
        "dt_nasc", "idade_anos",
        "flag_dt_nasc_invalida", "flag_idade_menor_18", "flag_idade_muito_alta",
        "cep_3_digitos", "flag_cep_missing",
        "statusrf",
        "prod", "flag_mig2",
        "flag_instalacao_invalida", "fpd_invalido",
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao",
        "metadata_versao_regra"
    ]
    var_cols = [c for c in df.columns if c.startswith("var_")]
    all_cols = list(dict.fromkeys(base_cols + var_cols))
    existing_cols = [c for c in all_cols if c in df.columns]

    return df.select(existing_cols)


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Cadastro - Opt-Z")
    parser.add_argument("--input_path", help="Caminho Bronze Cadastro")
    parser.add_argument("--output_path", help="Caminho destino Silver Cadastro")
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

    spark = SparkSession.builder.appName("Silver_Cadastro_OptZ").getOrCreate()

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Cadastro: {args.input_path}")
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
    # Nao ha criterio de negocio para desempate → qualquer registro serve.
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
    print(f">>> [Escrita] Salvando Silver Cadastro (Delta): {args.output_path}")

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
    # Quality consolidado em unico agg() — substitui 5 count() separados do original.
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver gravada para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.countDistinct("num_cpf", "safra").alias("distinct_keys"),
        F.sum(F.when(F.col("flag_instalacao_invalida") == 1, 1).otherwise(0)).alias("flag_inst_invalida"),
        F.sum(F.when(F.col("fpd_invalido") == 1, 1).otherwise(0)).alias("fpd_invalido"),
        F.sum(F.when(F.col("fpd_int").isNull(), 1).otherwise(0)).alias("fpd_null"),
        F.sum(F.when(F.col("flag_dt_nasc_invalida") == 1, 1).otherwise(0)).alias("dt_nasc_invalida"),
        F.sum(F.when(F.col("flag_idade_menor_18") == 1, 1).otherwise(0)).alias("idade_menor_18"),
        F.sum(F.when(F.col("flag_idade_muito_alta") == 1, 1).otherwise(0)).alias("idade_muito_alta"),
        F.sum(F.when(F.col("flag_cep_missing") == 1, 1).otherwise(0)).alias("cep_missing"),
        F.sum(F.when(F.col("idade_anos").isNotNull(), 1).otherwise(0)).alias("idade_preenchida"),
    ).collect()[0]

    count_out       = quality["total"]
    distinct_keys   = quality["distinct_keys"]
    flag_inst_inv   = quality["flag_inst_invalida"]
    fpd_inv         = quality["fpd_invalido"]
    fpd_null        = quality["fpd_null"]
    dt_nasc_inv     = quality["dt_nasc_invalida"]
    idade_18        = quality["idade_menor_18"]
    idade_100       = quality["idade_muito_alta"]
    cep_miss        = quality["cep_missing"]
    idade_preen     = quality["idade_preenchida"]
    idade_cobertura = 100 * idade_preen / count_out if count_out > 0 else 0
    cep_cobertura   = 100 * (count_out - cep_miss) / count_out if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER CADASTRO (OPT-Z)")
    print("="*80)
    print(f"\n    Registros Silver (pos-dedup):         {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):     {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                         {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - FLAG_INSTALACAO invalida:    {flag_inst_inv:>12,}")
    print(f"    Gate 2 - FPD invalido:                {fpd_inv:>12,}")
    print(f"    Gate 3 - FPD nulo:                    {fpd_null:>12,}  ({100*fpd_null/count_out:.2f}%)")
    print(f"\n    Gate 4 - DT_NASC invalida:            {dt_nasc_inv:>12,}")
    print(f"    Gate 5 - Idade < 18 (inelegivel):     {idade_18:>12,}  ({100*idade_18/count_out:.2f}%)")
    print(f"    Gate 6 - Idade > 100 (outlier):       {idade_100:>12,}  ({100*idade_100/count_out:.2f}%)")
    print(f"    Gate 7 - Cobertura IDADE_ANOS:        {idade_cobertura:>11.2f}%  [esperado ~99.57%]")
    print(f"    Gate 8 - CEP missing:                 {cep_miss:>12,}  (cobertura: {cep_cobertura:.1f}%)")

    print("\n" + "="*80)
    print("Silver CADASTRO concluido — opt_z (sem cache, dropDuplicates, coalesce dinamico)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  {num_output_files} (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f"  - Actions:            3 (count + write + agg Silver escrita)")
    print(f"  - Dedup:              dropDuplicates (EDA confirmou grain 1:1)")
    print(f"  - BYTES_PER_ROW:      {BYTES_PER_ROW_ESTIMATE} (estimativa inicial — calibrar apos run)")
    print(f"  - Proximo passo:      gold_recarga.py / gold_pagamento.py / gold_atraso.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
