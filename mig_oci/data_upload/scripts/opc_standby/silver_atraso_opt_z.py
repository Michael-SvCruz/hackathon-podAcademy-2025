# Arquivo: scripts/opc_standby/silver_atraso_opt_z.py
# OPCAO Z: sem cache, quality na Silver JA ESCRITA + Coalesce DINAMICO
# Padrao opt_z aplicado ao atraso. SEM dedup (grain multiplo por design).
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_atraso_opt_z.py — OPCAO Z
OBJETIVO: Bronze -> Silver Atraso sem cache, quality sobre arquivo escrito,
          com numero de arquivos de saida calculado dinamicamente.
--------------------------------------------------------------------------------
PARTICULARIDADE CRITICA — GRAIN MULTIPLO:
  Atraso e snapshot mensal de faturamento: multiplas linhas por CPF
  (faturas, itens, ajustes). NAO ha dedup — cada linha e um registro
  distinto e contem sinal de risco independente.
  Remover linhas = perder sinal → NUNCA aplicar dedup agressiva aqui.

  Diferenca vs telco/cadastro: ambos tinham grain 1:1 (dedup defensivo ok).
  Diferenca vs pagamento: pagamento tinha regra de negocio de versionamento.
  Atraso: nem defensivo nem negocio — simplesmente nao ha dedup.

ARQUITETURA (3 actions):
  action 1: count() pre-write   → calcula num_output_files
  action 2: coalesce(N).write() → grava Silver
  action 3: agg(Silver escrita) → quality check no arquivo real

DIFERENCA VS ORIGINAL (silver_atraso.py):
  - SEM dedup (mantido — ja era assim no original)
  - 3 actions em vez de 6 (count_bronze + count_silver + 4 quality counts)
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

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/atraso/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/atraso/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_atraso_v1"

# Sentinelas observadas
SENTINELAS = ['-1', '-2', '-3']

# Colunas com potencial sentinela
COLS_WITH_SENTINELA = {
    "IND_WO": "flag_ind_wo_sentinela",
    "IND_PDD": "flag_ind_pdd_sentinela",
    "IND_PCCR": "flag_ind_pccr_sentinela",
    "DW_TIPO_CLIENTE_CONTA": "flag_dw_tipo_cliente_sentinela",
    "COD_PLATAFORMA": "flag_cod_plataforma_sentinela",
    "DW_FAIXA_TEMPO_BASE": "flag_faixa_tempo_base_sentinela",
    "DW_FAIXA_AGING_PROX_FECH": "flag_faixa_aging_prox_fech_sentinela"
}

# Bytes estimados por registro na Silver (para coalesce dinamico)
# Atraso: grain multiplo → volume potencialmente alto
# Calibrar apos 1o run: BYTES = tamanho_total_MB * 1024 * 1024 / count_registros
BYTES_PER_ROW_ESTIMATE = 120
TARGET_FILE_SIZE_MB = 128
# =============================================================================


def build_silver_atraso(df_bronze):
    """Transform Bronze -> Silver Atraso (datas + monetarios + flags sentinela)."""
    print(">>> [Transform] Construindo Silver Atraso...")

    print("    -> Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_referencia",
        F.to_timestamp(F.upper(F.col("DAT_REFERENCIA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_vencimento",
        F.to_timestamp(F.upper(F.col("DAT_VENCIMENTO_FAT")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_fat",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FAT")), "ddMMMyyyy:HH:mm:ss")
    )

    print("    -> Step 2: Derivando SAFRA_ATRASO...")
    df = df.withColumn(
        "safra_atraso",
        F.date_format(F.to_date(F.col("ts_referencia")), "yyyyMM")
    )

    print("    -> Step 3: Casting valores monetarios...")
    monetary_cols = [
        "VAL_FAT_LIQUIDO", "VAL_FAT_BRUTO", "VAL_FAT_CREDITO",
        "VAL_FAT_AJUSTE", "VAL_FAT_BRUTO_BC", "VAL_FAT_PAGAMENTO_BRUTO",
        "VAL_FAT_ABERTO", "VAL_FAT_ABERTO_LIQ", "VAL_MULTA_JUROS",
        "VAL_MULTA_CANCELAMENTO", "VAL_PARC_APARELHO_LIQ", "VAL_FAT_LIQ_JM_MC"
    ]
    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(col.lower(), to_double_safe(col))

    print("    -> Step 4: Criando flags de sentinelas e missing...")
    df = df.withColumn(
        "flag_status_fat_missing",
        F.when(F.col("ts_status_fat").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    for col_orig, flag_name in COLS_WITH_SENTINELA.items():
        if col_orig in df.columns:
            df = df.withColumn(
                flag_name,
                F.when(F.col(col_orig.lower()).isin(SENTINELAS), F.lit(1)).otherwise(F.lit(0))
            )

    print("    -> Step 5: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df)

    print("    -> Step 6: Adicionando metadados...")
    df_silver = df_silver \
        .withColumn("metadata_data_transformacao", F.current_timestamp()) \
        .withColumn("metadata_versao_regra", F.lit(SILVER_VERSION))

    return df_silver


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Atraso - Opt-Z")
    parser.add_argument("--input_path", help="Caminho Bronze Atraso")
    parser.add_argument("--output_path", help="Caminho destino Silver Atraso")
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

    spark = SparkSession.builder.appName("Silver_Atraso_OptZ").getOrCreate()

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Atraso: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) BUILD SILVER
    # SEM dedup: grain multiplo por design (faturas/itens por CPF).
    # Remover linhas = perder sinal de risco.
    # =========================================================================
    df_silver = build_silver_atraso(df_bronze)

    # =========================================================================
    # 3) COALESCE DINAMICO — ACTION 1
    # =========================================================================
    print(">>> [Coalesce] Contando registros para calcular coalesce dinamico...")
    count_pre = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_pre:,}")

    estimated_size_mb = count_pre * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Info] Coalesce dinamico: {num_output_files} arquivos (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f">>> [Info] Tamanho estimado total: {estimated_size_mb:.0f} MB")
    print(f">>> [Info] ATENCAO: BYTES_PER_ROW_ESTIMATE={BYTES_PER_ROW_ESTIMATE} e estimativa inicial.")
    print(f"           Calibrar apos 1o run: BYTES = tamanho_real_MB * 1024 * 1024 / {count_pre}")

    # =========================================================================
    # 4) ESCRITA SILVER — ACTION 2
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Atraso (Delta): {args.output_path}")

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
    # Quality consolidado em unico agg() — substitui 4 count() do original.
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver gravada para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("ts_referencia").isNull(), 1).otherwise(0)).alias("invalidos_ref"),
        F.sum(F.when(F.col("num_cpf").isNull(), 1).otherwise(0)).alias("nulos_cpf"),
        F.sum(F.when(F.col("flag_status_fat_missing") == 1, 1).otherwise(0)).alias("missing_status_fat"),
        F.sum(F.when(F.col("flag_cod_plataforma_sentinela") == 1, 1).otherwise(0)).alias("sentinela_plataforma"),
        F.countDistinct("num_cpf").alias("cpfs_distintos"),
    ).collect()[0]

    count_out           = quality["total"]
    invalidos_ref       = quality["invalidos_ref"]
    nulos_cpf           = quality["nulos_cpf"]
    missing_status      = quality["missing_status_fat"]
    sentinela_plat      = quality["sentinela_plataforma"]
    cpfs_distintos      = quality["cpfs_distintos"]
    linhas_por_cpf      = count_out / cpfs_distintos if cpfs_distintos > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER ATRASO (OPT-Z)")
    print("="*80)
    print(f"\n    Registros Silver (sem dedup — grain multiplo): {count_out:>12,}")
    print(f"    CPFs distintos:                               {cpfs_distintos:>12,}")
    print(f"    Media linhas por CPF:                        {linhas_por_cpf:>12.1f}")
    print(f"\n    Gate 1 - TS_REFERENCIA invalidos:            {invalidos_ref:>12,}")
    print(f"    Gate 2 - NUM_CPF nulos:                      {nulos_cpf:>12,}")
    print(f"    Gate 3 - DAT_STATUS_FAT missing:             {missing_status:>12,}  ({100*missing_status/count_out:.2f}%) [esperado ~3.4%]")
    print(f"    Gate 4 - COD_PLATAFORMA sentinela:           {sentinela_plat:>12,}  ({100*sentinela_plat/count_out:.2f}%)")

    print("\n" + "="*80)
    print("Silver ATRASO concluido — opt_z (sem cache, sem dedup, coalesce dinamico)")
    print(f"  - Registros:          {count_out:,}")
    print(f"  - Arquivos de saida:  {num_output_files} (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f"  - Actions:            3 (count + write + agg Silver escrita)")
    print(f"  - Dedup:              NENHUM (grain multiplo — cada linha e sinal de risco)")
    print(f"  - BYTES_PER_ROW:      {BYTES_PER_ROW_ESTIMATE} (estimativa inicial — calibrar apos run)")
    print(f"  - Proximo passo:      gold_atraso.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
