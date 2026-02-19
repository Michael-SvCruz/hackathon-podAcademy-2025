# Arquivo: scripts/opc_standby/silver_recarga_with_dedup.py
# VERSAO STANDBY: com deduplicacao por EVENT_KEY
# Usar esta versao se a dedup na Silver for requisito (ex: consultas diretas na Silver)
# Versao principal (sem dedup): scripts/silver_recarga.py
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_recarga_with_dedup.py (OCI Data Flow) - VERSAO STANDBY
OBJETIVO: Transformacao Bronze -> Silver com deduplicacao por EVENT_KEY.
--------------------------------------------------------------------------------
DIFERENCA DA VERSAO PRINCIPAL:
- Inclui deduplicacao por EVENT_KEY (xxhash64 + dropDuplicates)
- Custo: ~15-20GB de shuffle I/O (vs 0 na versao principal)
- Beneficio: garante unicidade event-level na Silver

QUANDO USAR:
- Se a Silver for consultada diretamente (sem passar pelo Gold groupBy)
- Se duplicatas causarem problemas em algum pipeline downstream
- Se for requisito de governanca que Silver seja deduplicada
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
# =============================================================================

def build_silver(df_bronze):
    """
    Aplica tipagem explicita, cria derivadas e trata particularidades da Recarga.
    (Identica a versao principal)
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
                .withColumn(
                    f"flag_{val_col}_negativo",
                    F.when(F.col(val_col) < 0, 1).otherwise(0)
                )
                .withColumn(
                    f"{val_col}_clean",
                    F.when(F.col(val_col) < 0, None).otherwise(F.col(val_col))
                )
            )

    # 4) Tratamento de sentinelas em codigos dimensionais (-1/-2/-3)
    print(">>> [Transform] Tratando sentinelas em codigos dimensionais...")

    for cod_col in CODIGO_COLUMNS:
        if cod_col in df.columns:
            df = (
                df
                .withColumn(cod_col, to_int_safe(cod_col))
                .withColumn(
                    f"flag_{cod_col}_sentinela",
                    F.when(F.col(cod_col).isin(*SENTINELAS), 1).otherwise(0)
                )
            )

    # 5) Quality gates simples (dominio)
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

    # 6) Novos metadados
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
    df_silver = df.select(existing_columns)

    return df_silver

def dedupe_by_event_key(df_silver):
    """
    Garante 1 registro por evento (deduplicacao robusta).

    Estrategia:
    - Cria EVENT_KEY via xxhash64 (hash nao-criptografico, nativo do Spark)
    - dropDuplicates por EVENT_KEY (sem sort, O(n) em vez de O(n log n))
    - Sort e desnecessario porque TS_RECARGA faz parte do hash — duplicatas
      genuinas ja tem o mesmo timestamp.

    Colunas usadas para EVENT_KEY:
    NUM_CPF, DW_NUM_NTC, TS_RECARGA, VAL_REAL, VAL_CREDITO_INSERIDO,
    COD_TIPO_CREDITO, COD_STATUS_PLATAFORMA
    """
    print(">>> [Transform] Deduplicacao por EVENT_KEY (evento)...")

    event_key_cols = [
        "num_cpf",
        "dw_num_ntc",
        "ts_recarga",
        "val_real",
        "val_credito_inserido",
        "cod_tipo_credito",
        "cod_status_plataforma"
    ]

    event_key_cols = [col for col in event_key_cols if col in df_silver.columns]

    df_silver = df_silver.withColumn(
        "event_key",
        F.xxhash64(*[F.col(col).cast("string") for col in event_key_cols])
    )

    df_out = df_silver.dropDuplicates(["event_key"]).drop("event_key")

    return df_out

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base RECARGA (com dedup)")
    parser.add_argument("--input_path", help="Caminho da Bronze (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino na Silver (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("Silver_Recarga_WithDedup").getOrCreate()

    # NOTA: KryoSerializer incompativel com OCI Resource Principal.
    # O Kryo altera o class loader da JVM, quebrando a desserializacao Jackson
    # usada pelo X509FederationClient para renovar tokens IAM (BmcException -1).
    # Usando Java serializer padrao (funciona normalmente com Data Flow).

    # Shuffle partitions otimizado para ~3.6GB de dados
    spark.conf.set("spark.sql.shuffle.partitions", "30")

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
    # 1.5) PADRONIZACAO DE NOMES DE COLUNA
    # =========================================================================
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # =========================================================================
    # 2) TRANSFORM SILVER
    # =========================================================================
    df_silver = build_silver(df_bronze)

    # =========================================================================
    # 3) DEDUP POR EVENT_KEY
    # =========================================================================
    df_silver_dedup = dedupe_by_event_key(df_silver)

    # =========================================================================
    # 4) CACHE + CONTAGEM (evita recomputacao nos quality checks)
    # =========================================================================
    df_silver_dedup.cache()

    count_out = df_silver_dedup.count()  # Materializa o cache
    print(f">>> [Info] Registros na Silver (apos dedupe): {count_out}")

    # =========================================================================
    # 5) ESCRITA SILVER (com coalesce para controlar tamanho dos arquivos)
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")

    TARGET_FILE_SIZE_MB = 128
    estimated_size_mb = count_out * 40 / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Info] Coalesce para {num_output_files} arquivos (~{TARGET_FILE_SIZE_MB}MB cada)")

    df_silver_dedup.coalesce(num_output_files) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # 6) QUALITY CHECKS (rapidos porque df_silver_dedup esta em cache)
    # =========================================================================
    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - SILVER RECARGA (COM DEDUP)")
    print("="*80)

    quality_metrics = df_silver_dedup.agg(
        F.sum(F.when(F.col("flag_ts_recarga_invalida") == 1, 1).otherwise(0)).alias("ts_invalida"),
        *[F.sum(F.when(F.col(f"flag_{val_col}_negativo") == 1, 1).otherwise(0)).alias(f"neg_{val_col}")
          for val_col in VALOR_COLUMNS if f"flag_{val_col}_negativo" in df_silver_dedup.columns],
        *[F.sum(F.when(F.col(f"flag_{cod_col}_sentinela") == 1, 1).otherwise(0)).alias(f"sent_{cod_col}")
          for cod_col in CODIGO_COLUMNS if f"flag_{cod_col}_sentinela" in df_silver_dedup.columns],
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("sos_presente"),
        F.sum(F.when(F.col("flag_instalacao_int").isNull(), 1).otherwise(0)).alias("flag_null"),
        F.sum(F.when(F.col("flag_instalacao_int") == 0, 1).otherwise(0)).alias("flag_0"),
        F.sum(F.when(F.col("flag_instalacao_int") == 1, 1).otherwise(0)).alias("flag_1")
    ).collect()[0]

    ts_invalida = quality_metrics["ts_invalida"]
    ts_valida = count_out - ts_invalida
    ts_coverage = 100 * ts_valida / count_out if count_out > 0 else 0
    print(f"\n>>> [Quality] Parsing de data:")
    print(f"    TS_RECARGA valida: {ts_valida:>12} ({ts_coverage:.2f}%)")
    print(f"    TS_RECARGA invalida: {ts_invalida:>12}")

    print(f"\n>>> [Quality] Valores negativos (indicadores de ajuste/sentinela):")
    for val_col in VALOR_COLUMNS:
        if f"flag_{val_col}_negativo" in df_silver_dedup.columns:
            neg_count = quality_metrics[f"neg_{val_col}"]
            neg_pct = 100 * neg_count / count_out if count_out > 0 else 0
            print(f"    {val_col}_negativo: {neg_count:>12} ({neg_pct:.2f}%)")

    print(f"\n>>> [Quality] Sentinelas em codigos dimensionais (-1/-2/-3):")
    for cod_col in CODIGO_COLUMNS:
        if f"flag_{cod_col}_sentinela" in df_silver_dedup.columns:
            sent_count = quality_metrics[f"sent_{cod_col}"]
            sent_pct = 100 * sent_count / count_out if count_out > 0 else 0
            print(f"    {cod_col}_sentinela: {sent_count:>12} ({sent_pct:.2f}%)")

    sos_presente = quality_metrics["sos_presente"]
    sos_pct = 100 * sos_presente / count_out if count_out > 0 else 0
    print(f"\n>>> [Quality] SOS (servico especial):")
    print(f"    Eventos com SOS: {sos_presente:>12} ({sos_pct:.2f}%)")

    flag_null = quality_metrics["flag_null"]
    flag_0 = quality_metrics["flag_0"]
    flag_1 = quality_metrics["flag_1"]
    print(f"\n>>> [Quality] FLAG_INSTALACAO (label de decisao):")
    print(f"    Nulo: {flag_null:>12}")
    print(f"    FLAG=0: {flag_0:>12} ({100*flag_0/count_out:.2f}%)")
    print(f"    FLAG=1: {flag_1:>12} ({100*flag_1/count_out:.2f}%)")

    df_silver_dedup.unpersist()

    print("\n" + "="*80)
    print(f"Silver RECARGA concluido (COM DEDUP - versao standby)")
    print(f"  - Registros: {count_out}")
    print(f"  - Arquivos de saida: ~{num_output_files} (~{TARGET_FILE_SIZE_MB}MB cada)")
    print(f"  - Dedupe: aplicado por EVENT_KEY (xxhash64 + dropDuplicates)")
    print(f"  - Periodo SAFRA_RECARGA: (a confirmar no Gold com derivacao mensal)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
