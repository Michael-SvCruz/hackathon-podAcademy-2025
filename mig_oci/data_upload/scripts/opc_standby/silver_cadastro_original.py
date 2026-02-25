# Arquivo: mig_oci/data_upload/scripts/silver_cadastro.py
# Adaptado de src/jobs/01_silver/02_bronze_silver_cadastro.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable, try_cast->to_int_safe
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: silver_cadastro.py (OCI Data Flow)
OBJETIVO: Transformacao da camada Bronze para Silver - Base CADASTRO.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script le a tabela Delta da camada Bronze (cadastro), aplica tipagem
explicita, cria colunas derivadas (DT_SAFRA, IDADE_ANOS) e garante o grao 1:1
por NUM_CPF + SAFRA atraves de deduplicacao controlada.

REGRAS DE NEGOCIO (BASEADO EM cadastro.md):
- Grao esperado: 1 linha por NUM_CPF + SAFRA (confirmado em EDA).
- FLAG_INSTALACAO e label de decisao/politica (0/1).
- FPD e label de risco (pode ser nulo; usar Bureau como fonte de verdade).
- DATADENASCIMENTO: parsing tolerante (dd/MM/yyyy) -> DT_NASC.
- IDADE_ANOS: derivada com flags de sanity (menor de 18, > 100).
- CEP_3_digitos: feature categorica (regional) -> manter como string.
- STATUSRF: status cadastral (categorico).
- var_02 a var_25: mix de numericas e categoricas (tipagem individual).

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

def to_date_safe(colname, date_format="dd/MM/yyyy"):
    return F.when(
        (F.col(colname).isNull()) | (F.trim(F.col(colname)) == ""),
        F.lit(None)
    ).otherwise(
        F.from_unixtime(
            F.unix_timestamp(F.col(colname), date_format),
            date_format
        ).cast("date")
    )

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_INPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/cadastro/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/cadastro/"
DEFAULT_FORMAT = "delta"

# Constants para sanity checks de idade
IDADE_MINIMA_VALIDA = 18
IDADE_MAXIMA_ESPERADA = 100

# Variaveis numericas confirmadas (0 nao numericos no data quality check)
# var_03-var_09: todas confirmadas como numericas (ver docs/02_data_quality/cadastro.md secao 5.1)
NUMERIC_VARS = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09"]

# Variaveis categoricas (alto volume de nao numericos)
CATEGORICAL_VARS = ["var_15", "var_22", "var_23", "var_24", "var_25"]

# Variaveis mistas (possivel data, texto, ou parse customizado)
# var_10-var_14, var_16-var_21: podem ter valores nao numericos
MIXED_VARS = ["var_02", "var_10", "var_11", "var_12", "var_13", "var_14", "var_16", "var_17", "var_18", "var_19", "var_20", "var_21"]
# =============================================================================


def build_silver(df_bronze):
    """
    Aplica tipagem explicita, cria derivadas e trata particularidades do Cadastro.

    Transformacoes:
    1. Tipagem basica (labels, metadados)
    2. DT_SAFRA derivada (primeiro dia do mes)
    3. Parse tolerante de DATADENASCIMENTO -> DT_NASC
    4. Derivacao de IDADE_ANOS e flags de sanity
    5. Normalizacao de CEP e STATUSRF (categoricas)
    6. Casting de var_* (numericas vs categoricas)
    7. Quality gates simples (dominio)
    8. Selecao final de colunas
    """
    print(">>> [Transform] Tipagem + regras Silver (cadastro)...")

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

    # 3) Parse tolerante de DATADENASCIMENTO -> DT_NASC
    # Usa to_date() do Spark (nativo, sem UDF) para garantir execucao distribuida
    # Evita problemas de serializacao de UDFs em ambientes Databricks/Unity Catalog
    print(">>> [Transform] Parseando DATADENASCIMENTO com to_date() nativo do Spark...")

    # Tentar multiplos formatos de data (tolerante a variacoes no formato)
    # F.coalesce retorna o primeiro valor nao-NULL encontrado
    df = df.withColumn(
        "dt_nasc",
        F.coalesce(
            # Formato padrao: dd/MM/yyyy (ex: 15/03/1985)
            F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"),
            # Formato alternativo: dd-MM-yyyy (ex: 15-03-1985)
            F.to_date(F.col("datadenascimento"), "dd-MM-yyyy"),
            # Formato alternativo: yyyy-MM-dd (ex: 1985-03-15)
            F.to_date(F.col("datadenascimento"), "yyyy-MM-dd"),
            # Formato alternativo: ddMMyyyy (ex: 15031985)
            F.to_date(F.col("datadenascimento"), "ddMMyyyy"),
            # Se nenhum formato funcionar, retorna NULL
            F.lit(None).cast("date")
        )
    )

    # Flag de data invalida (preenchida mas nao conseguiu fazer parse)
    df = df.withColumn(
        "flag_dt_nasc_invalida",
        F.when(
            (F.col("datadenascimento").isNotNull()) &
            (F.trim(F.col("datadenascimento")) != F.lit("")) &
            (F.col("dt_nasc").isNull()),
            1
        ).otherwise(0)
    )

    # 4) Derivacao de IDADE_ANOS
    df = df.withColumn(
        "idade_anos",
        F.when(
            F.col("dt_nasc").isNotNull(),
            F.floor(F.months_between(F.col("dt_safra"), F.col("dt_nasc")) / 12)
        ).otherwise(None)
    )

    # Flags de sanity check (idade)
    df = df.withColumn(
        "flag_idade_menor_18",
        F.when(F.col("idade_anos") < IDADE_MINIMA_VALIDA, 1).otherwise(0)
    ).withColumn(
        "flag_idade_muito_alta",
        F.when(F.col("idade_anos") > IDADE_MAXIMA_ESPERADA, 1).otherwise(0)
    )

    # 5) CEP como feature regional (string)
    df = df.withColumn("cep_3_digitos", F.trim(F.col("cep_3_digitos")))
    df = df.withColumn(
        "flag_cep_missing",
        F.when(
            (F.col("cep_3_digitos").isNull()) | (F.col("cep_3_digitos") == F.lit("")),
            1
        ).otherwise(0)
    )

    # STATUSRF (categorico)
    df = df.withColumn("statusrf", F.trim(F.upper(F.col("statusrf"))))

    # 6) Casting de var_* (numericas)
    print(">>> [Transform] Tipando variaveis numericas...")
    for var in NUMERIC_VARS:
        if var in df.columns:
            df = df.withColumn(var, to_double_safe(var))

    # Categoricas var_* (trim + upper)
    print(">>> [Transform] Normalizando variaveis categoricas...")
    for var in CATEGORICAL_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.upper(F.col(var))))

    # Variaveis mistas/outras (trim simples)
    print(">>> [Transform] Normalizando variaveis mistas...")
    for var in MIXED_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.col(var)))

    # 7) Quality gates simples (dominio)
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when((~F.col("flag_instalacao_int").isin(0, 1)) & (F.col("flag_instalacao_int").isNotNull()), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when((~F.col("fpd_int").isin(0, 1)) & (F.col("fpd_int").isNotNull()), F.lit(1)).otherwise(F.lit(0))
    )

    # 8) Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_cadastro_v1"))

    # 9) Selecao final de colunas (Silver "clean")
    # Apenas as colunas uteis + auditoria
    columns_to_select = [
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels (nao usar como features)
        "flag_instalacao_int",
        "fpd_int",

        # Features cadastrais explicitas
        "dt_nasc",
        "idade_anos",
        "flag_dt_nasc_invalida",
        "flag_idade_menor_18",
        "flag_idade_muito_alta",

        # CEP (regional)
        "cep_3_digitos",
        "flag_cep_missing",

        # Status cadastral
        "statusrf",

        # Metadados de origem
        "prod",
        "flag_mig2",

        # Quality flags
        "flag_instalacao_invalida",
        "fpd_invalido",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    ]

    # Adicionar todas as var_* que existem no dataframe
    var_columns = [col for col in df.columns if col.startswith("var_")]
    columns_to_select.extend(var_columns)

    # Remover duplicatas mantendo ordem
    columns_to_select = list(dict.fromkeys(columns_to_select))

    # Selecionar apenas colunas que existem no DF
    existing_columns = [col for col in columns_to_select if col in df.columns]
    df_silver = df.select(existing_columns)

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
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base CADASTRO")
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

    spark = SparkSession.builder.appName("Silver_Cadastro").getOrCreate()

    # 1) Leitura Bronze
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # 1.5) Padronizacao de nomes de coluna (snake_case + sem acentos)
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

    idade_menor_18 = df_silver_dedup.filter(F.col("flag_idade_menor_18") == 1).count()
    idade_muito_alta = df_silver_dedup.filter(F.col("flag_idade_muito_alta") == 1).count()
    dt_nasc_invalida = df_silver_dedup.filter(F.col("flag_dt_nasc_invalida") == 1).count()

    cep_missing = df_silver_dedup.filter(F.col("flag_cep_missing") == 1).count()
    cep_coverage = 100 * (count_out - cep_missing) / count_out if count_out > 0 else 0

    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()

    fpd_null = df_silver_dedup.filter(F.col("fpd_int").isNull()).count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] fpd null: {fpd_null} ({fpd_null*100/count_out:.2f}%)")
    print(f">>> [Quality] data nascimento invalida: {dt_nasc_invalida}")
    print(f">>> [Quality] idade_menor_18 (ineligivel): {idade_menor_18} ({idade_menor_18*100/count_out:.2f}%)")
    print(f">>> [Quality] idade_muito_alta (outlier >100): {idade_muito_alta} ({idade_muito_alta*100/count_out:.2f}%)")
    print(f">>> [Quality] cep missing: {cep_missing} ({cep_missing*100/count_out:.2f}%) | cobertura: {cep_coverage:.1f}%")

    print(f">>> [Sucesso] Silver cadastro concluido.")

if __name__ == "__main__":
    main()
