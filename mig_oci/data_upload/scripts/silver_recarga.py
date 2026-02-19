# Arquivo: mig_oci/data_upload/scripts/silver_recarga.py
# Adaptado de src/jobs/01_silver/03_bronze_silver_recarga.py para OCI Data Flow
# Versao final: sem cache, sem dedup, quality na Silver ja escrita (10m32s)
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_recarga.py (OCI Data Flow)
OBJETIVO: Transformacao da camada Bronze para Silver - Base RECARGA.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script le a tabela Delta da camada Bronze (recarga), aplica tipagem
explicita, cria colunas derivadas (DT_SAFRA, TS_RECARGA, SAFRA_RECARGA) e
trata particularidades de base de eventos: sentinelas em codigos (-1/-2/-3),
valores negativos em montantes e SOS.

REGRAS DE NEGOCIO (BASEADO EM recarga.md):
- Grao esperado: EVENT-LEVEL (multiplos registros por NUM_CPF + SAFRA)
- Chave temporal do evento: DAT_INSERCAO_CREDITO (formato: ddMMMyyyy:HH:mm:ss)
- SAFRA derivada: YYYYMM a partir de DAT_INSERCAO_CREDITO (primeiro dia do mes)
- Variaveis principais:
  - Monetarias: VAL_CREDITO_INSERIDO, VAL_BONUS, VAL_REAL, VALOR_SOS
  - Dimensionais (codigos): COD_TECNOLOGIA_DW, COD_TIPO_CREDITO, etc.
  - Flags: FLAG_SOS, FLAG_INSTALACAO, FPD (quando houver)

PARTICULARIDADES DA RECARGA:
- BASE TRANSACIONAL (evento-level, 100M+ registros)
- Sentinelas observadas: -1 (nao aplica), -2 (nao determinado), -3 (nao informado)
  -> Trata mapeando para "sentinela" e criando flags
- Valores negativos em VAL_BONUS e VAL_REAL (~6% dos registros)
  -> Trata criando colunas "clean" (NULL se negativo) + flags
- SOS (~6,5M eventos, valor de 3-20)
  -> Mantem como feature separada + flag de presenca
- Duplicidades observadas: ~320k duplicatas potenciais em 100M registros (~0.3%)
  -> Dedup removida: Gold agrega por NUM_CPF+SAFRA com groupBy, absorvendo
     duplicatas naturalmente. Versao com dedup: opc_standby/silver_recarga_with_dedup.py

ANTI-LEAKAGE:
- FPD e FLAG_INSTALACAO nao devem ser usados como features (apenas labels/auditoria).

ARQUITETURA (2 actions, sem cache):
  Bronze(~4GB OCI) → write → Silver(~3GB OCI)   [action 1]
  Silver(~3GB OCI) → agg + count → metricas      [action 2]

  Sem cache(): sem pressao de memoria nos executors.
  Quality check le a Silver JA GRAVADA — valida o arquivo real no bucket.
  Silver Delta (tipado, comprimido) e mais rapido de reler que cache em
  memoria (Java objects descomprimidos de ~15-20GB).

BENCHMARK (18/02/2026 — OCI Data Flow):
  - Duracao: 10m 32s  (vs 55min original, -81%)
  - Data read: 3 GB   (vs 36GB com cache)
  - Data written: 3 GB

VERSOES ALTERNATIVAS (opc_standby/):
  - silver_recarga_opt_x.py     : cache + count embutido no agg (17m04s)
  - silver_recarga_with_dedup.py: com dedup EVENT_KEY, sem Kryo (~32m)
  - COMPARATIVO_VERSOES_SILVER_RECARGA.md: analise completa de cada versao

NOTA OCI:
- try_cast (Databricks-specific) substituido por to_int_safe inline
- KryoSerializer INCOMPATIVEL com OCI Resource Principal (X509FederationClient)
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# UTILITY FUNCTIONS (inlined from spark_utils.py para OCI Data Flow)
# No Data Flow, a SparkSession ja vem pre-configurada pelo servico.
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

# Colunas numericas de valor (montantes)
VALOR_COLUMNS = ["val_credito_inserido", "val_bonus", "val_real", "valor_sos"]

# Colunas dimensionais (codigos com sentinelas -1/-2/-3)
CODIGO_COLUMNS = [
    "cod_tecnologia_dw", "cod_tipo_credito", "dw_tipo_insercao",
    "dw_tipo_recarga", "dw_forma_pagamento", "cod_plataforma_atu",
    "cod_status_plataforma", "cod_canal_aquisicao", "dw_instituicao",
    "dw_plano_tarifacao", "cod_promocao"
]

# Sentinelas padronizadas
SENTINELAS = [-1, -2, -3]

# Coalesce fixo: ~3.6GB / 128MB = 28 particoes
# Fixo evita count() separado (action extra desnecessario)
NUM_OUTPUT_FILES = 28
# =============================================================================

def build_silver(df_bronze):
    """
    Aplica tipagem explicita, cria derivadas e trata particularidades da Recarga.

    Pipeline:
    1. Tipagem basica (NUM_CPF, FLAG_SOS, FLAG_INSTALACAO como int)
    2. Parsing de DAT_INSERCAO_CREDITO -> TS_RECARGA, DT_RECARGA, SAFRA_RECARGA
    3. Casting de valores monetarios para double com flags de negativos
    4. Tratamento de sentinelas em codigos dimensionais (-1/-2/-3 -> flags)
    5. Quality gates de dominio
    6. Selecao final de colunas
    """
    print(">>> [Transform] Tipagem + regras Silver (recarga evento-level)...")

    # 1) Tipagem basica (chaves + flags)
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("dw_num_cliente", F.col("dw_num_cliente").cast("string"))
        .withColumn("dw_num_ntc", F.col("dw_num_ntc").cast("string"))
        .withColumn("flag_sos", to_int_safe("flag_sos"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao") if "flag_instalacao" in df_bronze.columns else F.lit(None).cast("int"))
        .withColumn("fpd_int", to_int_safe("fpd") if "fpd" in df_bronze.columns else F.lit(None).cast("int"))
    )

    # 2) Parsing tolerante de data/hora do evento
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

    # 3) Casting de valores monetarios (com flags de negativos)
    print(">>> [Transform] Tipando valores monetarios e criando flags de negativos...")

    for val_col in VALOR_COLUMNS:
        if val_col in df.columns:
            df = (
                df
                .withColumn(f"{val_col}", to_double_safe(val_col))
                .withColumn(f"flag_{val_col}_negativo", F.when(F.col(val_col) < 0, 1).otherwise(0))
                .withColumn(f"{val_col}_clean", F.when(F.col(val_col) < 0, None).otherwise(F.col(val_col)))
            )

    # 4) Tratamento de sentinelas em codigos dimensionais (-1/-2/-3)
    print(">>> [Transform] Tratando sentinelas em codigos dimensionais...")

    for cod_col in CODIGO_COLUMNS:
        if cod_col in df.columns:
            df = (
                df
                .withColumn(cod_col, to_int_safe(cod_col))
                .withColumn(f"flag_{cod_col}_sentinela", F.when(F.col(cod_col).isin(*SENTINELAS), 1).otherwise(0))
            )

    # 5) Quality gates de dominio
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

    # 6) Metadados de transformacao
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_recarga_v1"))

    # 7) Selecao final de colunas (Silver "clean" - evento level)
    columns_to_select = [
        # Chaves (identificacao do evento)
        "num_cpf",
        "dw_num_cliente",
        "dw_num_ntc",

        # Timestamp do evento
        "ts_recarga",
        "dt_recarga",
        "safra_recarga",
        "flag_ts_recarga_invalida",

        # Labels (nao usar como features)
        "flag_instalacao_int",
        "flag_instalacao_invalida",
        "fpd_int",
        "fpd_invalido",

        # Valores monetarios (clean + flags de negativos)
        "val_credito_inserido",
        "flag_val_credito_inserido_negativo",
        "val_credito_inserido_clean",

        "val_bonus",
        "flag_val_bonus_negativo",
        "val_bonus_clean",

        "val_real",
        "flag_val_real_negativo",
        "val_real_clean",

        # SOS (separado)
        "flag_sos",
        "flag_sos_invalida",
        "valor_sos",

        # Codigos dimensionais + flags sentinela
        *[f"{cod}" for cod in CODIGO_COLUMNS if cod in df.columns],
        *[f"flag_{cod}_sentinela" for cod in CODIGO_COLUMNS if cod in df.columns],

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    ]

    columns_to_select = list(dict.fromkeys(columns_to_select))
    existing_columns = [col for col in columns_to_select if col in df.columns]
    return df.select(existing_columns)


def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base RECARGA (evento-level)")
    parser.add_argument("--input_path", help="Caminho da Bronze (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino na Silver (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem (delta)")

    args_parsed, _ = parser.parse_known_args()

    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("Silver_Recarga").getOrCreate()

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
    # 3) ESCRITA SILVER (action 1 — le Bronze 1x do OCI, sem cache)
    # Pipeline e map puro (sem shuffle, sem dedup), entao Bronze e lida
    # exatamente 1x. Sem cache() = sem pressao de memoria nos executors.
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

    print(f">>> [Escrita] Silver gravada com sucesso.")

    # =========================================================================
    # 4) QUALITY CHECKS na Silver JA ESCRITA (action 2 — le Silver do OCI)
    # Le ~3GB da Silver (tipada, comprimida, Delta) — mais eficiente que
    # varrer cache de ~15-20GB em memoria. Valida o arquivo real no bucket.
    # =========================================================================
    print(f"\n>>> [Quality] Lendo Silver gravada para validacao: {args.output_path}")
    df_silver_written = spark.read.format("delta").load(args.output_path)

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER RECARGA")
    print("="*80)

    # Uma unica passada: count + todos os quality metrics juntos
    quality_metrics = df_silver_written.agg(
        F.count("*").alias("total"),
        # Parsing
        F.sum(F.when(F.col("flag_ts_recarga_invalida") == 1, 1).otherwise(0)).alias("ts_invalida"),
        # Valores negativos
        *[F.sum(F.when(F.col(f"flag_{val_col}_negativo") == 1, 1).otherwise(0)).alias(f"neg_{val_col}")
          for val_col in VALOR_COLUMNS if f"flag_{val_col}_negativo" in df_silver_written.columns],
        # Sentinelas
        *[F.sum(F.when(F.col(f"flag_{cod_col}_sentinela") == 1, 1).otherwise(0)).alias(f"sent_{cod_col}")
          for cod_col in CODIGO_COLUMNS if f"flag_{cod_col}_sentinela" in df_silver_written.columns],
        # SOS
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("sos_presente"),
        # Labels
        F.sum(F.when(F.col("flag_instalacao_int").isNull(), 1).otherwise(0)).alias("flag_null"),
        F.sum(F.when(F.col("flag_instalacao_int") == 0, 1).otherwise(0)).alias("flag_0"),
        F.sum(F.when(F.col("flag_instalacao_int") == 1, 1).otherwise(0)).alias("flag_1")
    ).collect()[0]

    count_out = quality_metrics["total"]

    # Parsing
    ts_invalida = quality_metrics["ts_invalida"]
    ts_valida = count_out - ts_invalida
    ts_coverage = 100 * ts_valida / count_out if count_out > 0 else 0
    print(f"\n>>> [Quality] Parsing de data:")
    print(f"    TS_RECARGA valida:   {ts_valida:>12} ({ts_coverage:.2f}%)")
    print(f"    TS_RECARGA invalida: {ts_invalida:>12}")

    # Valores negativos
    print(f"\n>>> [Quality] Valores negativos (indicadores de ajuste/sentinela):")
    for val_col in VALOR_COLUMNS:
        if f"flag_{val_col}_negativo" in df_silver_written.columns:
            neg_count = quality_metrics[f"neg_{val_col}"]
            print(f"    {val_col}_negativo: {neg_count:>12} ({100*neg_count/count_out:.2f}%)")

    # Sentinelas
    print(f"\n>>> [Quality] Sentinelas em codigos dimensionais (-1/-2/-3):")
    for cod_col in CODIGO_COLUMNS:
        if f"flag_{cod_col}_sentinela" in df_silver_written.columns:
            sent_count = quality_metrics[f"sent_{cod_col}"]
            print(f"    {cod_col}_sentinela: {sent_count:>12} ({100*sent_count/count_out:.2f}%)")

    # SOS
    sos_presente = quality_metrics["sos_presente"]
    print(f"\n>>> [Quality] SOS (servico especial):")
    print(f"    Eventos com SOS: {sos_presente:>12} ({100*sos_presente/count_out:.2f}%)")

    # Labels
    flag_null = quality_metrics["flag_null"]
    flag_0    = quality_metrics["flag_0"]
    flag_1    = quality_metrics["flag_1"]
    print(f"\n>>> [Quality] FLAG_INSTALACAO (label de decisao):")
    print(f"    Nulo:   {flag_null:>12}")
    print(f"    FLAG=0: {flag_0:>12} ({100*flag_0/count_out:.2f}%)")
    print(f"    FLAG=1: {flag_1:>12} ({100*flag_1/count_out:.2f}%)")

    print("\n" + "="*80)
    print(f"Silver RECARGA concluido (evento-level, pronto para agregacao Gold)")
    print(f"  - Registros:          {count_out}")
    print(f"  - Arquivos de saida:  {NUM_OUTPUT_FILES} (~128MB cada)")
    print(f"  - Actions:            2 (write + agg na Silver escrita)")
    print(f"  - Dedupe:             removida (Gold groupBy absorve 0.3%)")
    print(f"  - Cache:              removido (Silver 3GB < cache JVM 15-20GB)")
    print(f"  - Benchmark:          10m32s / 3GB lidos (18/02/2026)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
