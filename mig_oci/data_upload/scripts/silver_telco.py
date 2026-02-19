# Arquivo: mig_oci/data_upload/scripts/silver_telco.py
# Adaptado de src/jobs/01_silver/01_bronze_silver_telco.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable, try_cast->to_int_safe
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_telco.py (OCI Data Flow)
OBJETIVO: Transformacao da camada Bronze para Silver - Base TELCO.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script le a tabela Delta da camada Bronze (telco), aplica tipagem
explicita, cria colunas derivadas (DT_SAFRA), trata sentinelas de valor 304
em variaveis anonimizadas (var_26 a var_93) e garante o grao 1:1 por
NUM_CPF + SAFRA atraves de deduplicacao controlada.

REGRAS DE NEGOCIO (BASEADO EM telco.md):
- Grain esperado: 1 linha por NUM_CPF + SAFRA (confirmado em EDA).
- FLAG_INSTALACAO e label de decisao/politica (0/1).
- FPD e label de risco (0/1) com ~3,36% de missing (usar com cautela).
- Variaveis var_26 a var_93: features anonimizadas (cast para double).
- SENTINELA 304: muito frequente em var_* (nao informado/nao aplicavel).
  -> Trata convertendo 304 para NULL e criando flag de missing.
- PROD e flag_mig2: metadados de origem (manter como string).

ANTI-LEAKAGE:
- FPD e FLAG_INSTALACAO nao devem ser usados como features (apenas labels/auditoria).
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

def to_int_safe(colname):
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

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

# Lista de colunas var_* esperadas (var_26 a var_93 = 68 colunas)
VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# =============================================================================

def build_silver(df_bronze):
    """
    Aplica tipagem explicita, cria derivadas e trata sentinelas da Telco.

    Pipeline:
    1. Tipagem basica (NUM_CPF, SAFRA, FLAG_INSTALACAO, FPD como int/string)
    2. Criacao de DT_SAFRA derivada
    3. Casting de var_* para double
    4. Tratamento de sentinela 304 em var_* (criar flags de missing)
    5. Quality gates (validacao de dominio)
    6. Selecao final de colunas
    """
    print(">>> [Transform] Tipagem + regras Silver (telco)...")

    # 1) Tipagem basica
    # Nota: Colnames ja estao padronizadas (snake_case) apos standardize_column_names()
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
    )

    # 2) DT_SAFRA (primeiro dia do mes)
    # SAFRA vem como YYYYMM -> YYYY-MM-01
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )

    # 3) Casting de var_* para double com tratamento de sentinela 304
    print(">>> [Transform] Tratando sentinela 304 em var_*...")

    for var_col in VAR_COLUMNS:
        if var_col in df.columns:
            treatment = treat_sentinel_value(var_col, sentinel_values=[304])
            df = df \
                .withColumn(treatment["colname_treated"], treatment["expr_treated"]) \
                .withColumn(treatment["flag_name"], treatment["expr_flag"])
        else:
            print(f"!!! AVISO: Coluna {var_col} nao encontrada no DataFrame.")

    # 4) Quality gates simples (dominio)
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when(~F.col("flag_instalacao_int").isin(0, 1) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when(~F.col("fpd_int").isin(0, 1) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # 5) Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_telco_v1"))

    # 6) Selecao de colunas finais (Silver "clean")
    # Chaves
    df_silver = df.select(
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels/Politica (nao usar como features)
        "flag_instalacao_int",
        "fpd_int",

        # Metadados de origem
        "prod",
        "flag_mig2",

        # Features var_* (versao ajustada com tratamento de 304)
        *[f"var_{i}_adj" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],

        # Flags de missing para var_*
        *[f"flag_var_{i}_missing" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],

        # Quality flags
        "flag_instalacao_invalida",
        "fpd_invalido",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    return df_silver

def dedupe_by_key(df_silver):
    """
    Garante grao 1:1 por NUM_CPF + SAFRA.
    Criterio de desempate: metadata_data_ingestao DESC (mais recente).
    """
    print(">>> [Transform] Deduplicacao por num_cpf + safra (se necessario)...")

    w = Window.partitionBy("num_cpf", "safra").orderBy(F.col("metadata_data_ingestao").desc())

    df_ranked = df_silver.withColumn("rn", F.row_number().over(w))
    df_out = df_ranked.filter(F.col("rn") == 1).drop("rn")

    return df_out

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base TELCO")
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

    spark = SparkSession.builder.appName("Silver_Telco").getOrCreate()

    # 1) Leitura Bronze
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # 1.5) Padronizacao de nomes de coluna (Snake_case + sem acentos)
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # 2) Transform Silver
    df_silver = build_silver(df_bronze)

    # 3) Dedup por chave
    df_silver_dedup = dedupe_by_key(df_silver)

    count_out = df_silver_dedup.count()
    print(f">>> [Info] Registros na Silver (apos dedupe): {count_out}")
    print(f">>> [Info] Linhas removidas no dedupe: {count_in - count_out}")

    # 4) Escrita Silver
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")

    df_silver_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # 5) Quality checks (rapidos)
    print(">>> [Quality] Checando dominios e unicidade...")

    invalid_flag = df_silver_dedup.filter(F.col("flag_instalacao_invalida") == 1).count()
    invalid_fpd = df_silver_dedup.filter(F.col("fpd_invalido") == 1).count()

    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()

    fpd_null = df_silver_dedup.filter(F.col("fpd_int").isNull()).count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] fpd null: {fpd_null} ({fpd_null*100/count_out:.2f}%)")
    print(f">>> [Quality] distinct num_cpf+safra: {distinct_key} | total_out: {count_out}")

    print(f">>> [Sucesso] Silver telco concluido.")

if __name__ == "__main__":
    main()
