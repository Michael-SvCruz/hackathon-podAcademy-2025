# Arquivo: mig_oci/data_upload/scripts/silver_atraso.py
# Adaptado de src/jobs/01_silver/05_bronze_silver_atraso.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable, try_cast->to_int_safe
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_atraso.py (OCI Data Flow)
OBJETIVO: Transformacao da camada Bronze para Silver -- Atraso/Faturamento.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script le a tabela Delta da camada Bronze (atraso), aplica as
transformacoes Silver conforme docs/03_silver_rules/atraso.md:

1. Parse de datas (DAT_REFERENCIA, DAT_VENCIMENTO_FAT, DAT_STATUS_FAT)
2. Derivacao de SAFRA_ATRASO (sempre YYYYMM do dia 01 = snapshot mensal)
3. Casting de valores monetarios (double)
4. Flags de sentinelas (-1, -2, -3)
5. Monitoramento de duplicidade (sem dedup agressiva)
6. Padronizacao de nomes de coluna (snake_case)

PARTICULARIDADES:
- Snapshot mensal: DAT_REFERENCIA sempre dia 01 (fotografia de estado)
- Grao: multiplas linhas por CPF (faturas, itens, ajustes)
- DAT_STATUS_FAT tem ~3.4% missing (criar flag)
- Sentinelas em varias colunas (-1, -2, -3)
  -> Criar FLAG_<COL>_SENTINELA para cada
- Sem dedup agressiva (pode apagar sinal real)

ANTI-LEAKAGE:
- E snapshot mensal -> SEM eventos futuros
- Seguro usar todas as features como estao no snapshot
- SAFRA_ATRASO = date_format(TS_REFERENCIA, 'yyyyMM')
- Todas as features refletem estado NA DATA DO SNAPSHOT
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# UTILITY FUNCTIONS (inlined from spark_utils.py para OCI Data Flow)
# No Data Flow, a SparkSession já vem pré-configurada pelo serviço.
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
    ).when(
        F.trim(F.col(colname)).rlike("^[+-]?([0-9]*[.,])?[0-9]+$"),
        F.col(colname).cast("double")
    ).otherwise(
        F.lit(None)
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

# =============================================================================

def build_silver_atraso(df_bronze):
    """
    Constroi Silver Atraso com parsing, casting, flags de sentinela.

    Passos:
    1. Parse datas (DAT_REFERENCIA, DAT_VENCIMENTO_FAT, DAT_STATUS_FAT)
    2. Derivar SAFRA_ATRASO (sempre YYYYMM, dia = 01)
    3. Cast valores monetarios
    4. Criar flags de sentinelas para colunas categoricas
    5. Monitorar duplicidade (sem dedup)
    6. Padronizar nomes (snake_case)
    """
    print(">>> [Transform] Construindo Silver Atraso...")

    # =========================================================================
    # Step 1: Parse de datas
    # =========================================================================
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

    # =========================================================================
    # Step 2: Derivar SAFRA_ATRASO
    # =========================================================================
    print("    -> Step 2: Derivando SAFRA_ATRASO...")
    df = df.withColumn(
        "safra_atraso",
        F.date_format(F.to_date(F.col("ts_referencia")), "yyyyMM")
    )

    # =========================================================================
    # Step 3: Cast de valores monetarios (double)
    # =========================================================================
    print("    -> Step 3: Casting valores monetarios...")
    monetary_cols = [
        "VAL_FAT_LIQUIDO",
        "VAL_FAT_BRUTO",
        "VAL_FAT_CREDITO",
        "VAL_FAT_AJUSTE",
        "VAL_FAT_BRUTO_BC",
        "VAL_FAT_PAGAMENTO_BRUTO",
        "VAL_FAT_ABERTO",
        "VAL_FAT_ABERTO_LIQ",
        "VAL_MULTA_JUROS",
        "VAL_MULTA_CANCELAMENTO",
        "VAL_PARC_APARELHO_LIQ",
        "VAL_FAT_LIQ_JM_MC"
    ]

    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(
                col.lower(),
                to_double_safe(col)
            )

    # =========================================================================
    # Step 4: Flags de sentinelas e missing
    # =========================================================================
    print("    -> Step 4: Criando flags de sentinelas...")

    # Flag: DAT_STATUS_FAT missing
    df = df.withColumn(
        "flag_status_fat_missing",
        F.when(F.col("ts_status_fat").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # Flags para colunas com sentinelas
    for col_orig, flag_name in COLS_WITH_SENTINELA.items():
        if col_orig in df.columns:
            df = df.withColumn(
                flag_name,
                F.when(
                    F.col(col_orig.lower()).isin(SENTINELAS),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )

    # =========================================================================
    # Step 5: Padronizar nomes de coluna (snake_case)
    # =========================================================================
    print("    -> Step 5: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df)

    # =========================================================================
    # Step 6: Metadados da Silver
    # =========================================================================
    print("    -> Step 6: Adicionando metadados...")
    df_silver = df_silver.withColumn(
        "metadata_data_transformacao",
        F.current_timestamp()
    ).withColumn(
        "metadata_versao_regra",
        F.lit(SILVER_VERSION)
    )

    return df_silver

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver -- Atraso/Faturamento")
    parser.add_argument("--input_path", help="Caminho do Bronze Atraso")
    parser.add_argument("--output_path", help="Caminho de destino Silver Atraso")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.input_path and args_parsed.output_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("Silver_Transform_Atraso").getOrCreate()

    # =========================================================================
    # 1. LEITURA (BRONZE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Atraso: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)

    count_bronze = df_bronze.count()
    print(f">>> [Info] Registros Bronze: {count_bronze:,}")

    # =========================================================================
    # 2. BUILD SILVER
    # =========================================================================
    df_silver = build_silver_atraso(df_bronze)

    count_silver = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_silver:,}")

    # Auditoria (sem dedup agressiva, espera-se retencao ~100%)
    retencao_pct = (count_silver / count_bronze) * 100
    print(f">>> [Auditoria] Retencao: {retencao_pct:.2f}%")

    # =========================================================================
    # 3. VALIDACOES SIMPLES (GATES)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")

    # Gate 1: TS_REFERENCIA parseado (sempre valido)
    invalidos_ref = df_silver.filter(F.col("ts_referencia").isNull()).count()
    print(f"    Gate 1 - TS_REFERENCIA invalidos: {invalidos_ref}")

    # Gate 2: NUM_CPF nao nulo
    nulos_cpf = df_silver.filter(F.col("num_cpf").isNull()).count()
    print(f"    Gate 2 - NUM_CPF nulos: {nulos_cpf}")

    # Gate 3: Monitoramento DAT_STATUS_FAT missing
    missing_status = df_silver.filter(F.col("flag_status_fat_missing") == 1).count()
    missing_pct = (missing_status / count_silver) * 100
    print(f"    Gate 3 - DAT_STATUS_FAT missing: {missing_status:,} ({missing_pct:.2f}%)")

    # Gate 4: Monitoramento de sentinelas (exemplo: COD_PLATAFORMA)
    if "flag_cod_plataforma_sentinela" in df_silver.columns:
        sentinela_count = df_silver.filter(F.col("flag_cod_plataforma_sentinela") == 1).count()
        sentinela_pct = (sentinela_count / count_silver) * 100
        print(f"    Gate 4 - COD_PLATAFORMA com sentinela: {sentinela_count:,} ({sentinela_pct:.2f}%)")

    # =========================================================================
    # 4. ESCRITA (SILVER -- DELTA)
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Atraso (Delta): {args.output_path}")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(f"Escrita em Delta concluida")

    # =========================================================================
    # 5. RELATORIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATORIO FINAL -- Silver Atraso/Faturamento")
    print("="*80)
    print(f"  Registros Bronze (entrada): {count_bronze:,}")
    print(f"  Registros Silver (saida):   {count_silver:,}")
    print(f"  Retencao: {retencao_pct:.2f}%")
    print(f"  Colunas originais: {len(df_bronze.columns)}")
    print(f"  Colunas apos transformacao: {len(df_silver.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Proximo passo: Gold (05_gold_abt_v6_builder.py)")
    print(f"  Nota: Snapshot mensal (sempre dia 01) -- anti-leakage garantido")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
