"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 02_bronze_silver_cadastro.py
OBJETIVO: Transformação da camada Bronze para Silver - Base CADASTRO.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (cadastro), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA, IDADE_ANOS) e garante o grão 1:1
por NUM_CPF + SAFRA através de deduplicação controlada.

REGRAS DE NEGÓCIO (BASEADO EM cadastro.md):
- Grão esperado: 1 linha por NUM_CPF + SAFRA (confirmado em EDA).
- FLAG_INSTALACAO é label de decisão/política (0/1).
- FPD é label de risco (pode ser nulo; usar Bureau como fonte de verdade).
- DATADENASCIMENTO: parsing tolerante (dd/MM/yyyy) → DT_NASC.
- IDADE_ANOS: derivada com flags de sanity (menor de 18, > 100).
- CEP_3_digitos: feature categórica (regional) → manter como string.
- STATUSRF: status cadastral (categórico).
- var_02 a var_25: mix de numéricas e categóricas (tipagem individual).

ANTI-LEAKAGE:
- FPD e FLAG_INSTALACAO não devem ser usados como features (apenas labels/auditoria).

AJUSTES UNITY CATALOG:
- Leitura/escrita em Delta nos caminhos /Volumes/...
- Mantém colunas de auditoria da Bronze para rastreabilidade.
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe,
    to_date_safe
)

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/cadastro_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/cadastro_silver_delta/"
DEFAULT_FORMAT = "delta"

# Constants para sanity checks de idade
IDADE_MINIMA_VALIDA = 18
IDADE_MAXIMA_ESPERADA = 100

# Variáveis numéricas confirmadas (0 não numéricos no data quality check)
# var_03-var_09: todas confirmadas como numéricas (ver docs/02_data_quality/cadastro.md seção 5.1)
NUMERIC_VARS = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09"]

# Variáveis categóricas (alto volume de não numéricos)
CATEGORICAL_VARS = ["var_15", "var_22", "var_23", "var_24", "var_25"]

# Variáveis mistas (possível data, texto, ou parse customizado)
# var_10-var_14, var_16-var_21: podem ter valores não numéricos
MIXED_VARS = ["var_02", "var_10", "var_11", "var_12", "var_13", "var_14", "var_16", "var_17", "var_18", "var_19", "var_20", "var_21"]
# =============================================================================


def build_silver(df_bronze):
    """
    Aplica tipagem explícita, cria derivadas e trata particularidades do Cadastro.
    
    Transformações:
    1. Tipagem básica (labels, metadados)
    2. DT_SAFRA derivada (primeiro dia do mês)
    3. Parse tolerante de DATADENASCIMENTO → DT_NASC
    4. Derivação de IDADE_ANOS e flags de sanity
    5. Normalização de CEP e STATUSRF (categóricas)
    6. Casting de var_* (numéricas vs categóricas)
    7. Quality gates simples (domínio)
    8. Seleção final de colunas
    """
    print(">>> [Transform] Tipagem + regras Silver (cadastro)...")

    # 1) Tipagem básica
    # Nota: Colnames já estão padronizadas (snake_case) após standardize_column_names()
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
    )

    # 2) DT_SAFRA (primeiro dia do mês)
    # SAFRA vem como YYYYMM -> YYYY-MM-01
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )

    # 3) Parse tolerante de DATADENASCIMENTO → DT_NASC
    # Trata valores inválidos (ex: "2807") graciosamente (retorna NULL)
    # Usando F.to_date() com tratamento de erro via estrutura robusta
    print(">>> [Transform] Parseando DATADENASCIMENTO com tolerância a inválidos...")
    
    # Helper: função de parse seguro de data
    def safe_parse_date(date_str, date_format="dd/MM/yyyy"):
        """
        UDF para parser seguro de datas. Retorna NULL em caso de erro.
        """
        if date_str is None or date_str.strip() == "":
            return None
        try:
            from datetime import datetime
            return datetime.strptime(date_str.strip(), date_format).date()
        except:
            return None
    
    # Registrar UDF
    from pyspark.sql.types import DateType
    safe_parse_date_udf = F.udf(safe_parse_date, DateType())
    
    # Aplicar parse seguro
    df = df.withColumn(
        "dt_nasc",
        F.when(
            F.col("datadenascimento").isNotNull() & (F.trim(F.col("datadenascimento")) != F.lit("")),
            safe_parse_date_udf(F.col("datadenascimento"))
        ).otherwise(None)
    )
    
    # Flag de data inválida (preenchida mas não faz parse)
    df = df.withColumn(
        "flag_dt_nasc_invalida",
        F.when(
            (F.col("datadenascimento").isNotNull()) &
            (F.trim(F.col("datadenascimento")) != F.lit("")) &
            (F.col("dt_nasc").isNull()),
            1
        ).otherwise(0)
    )

    # 4) Derivação de IDADE_ANOS
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
    
    # STATUSRF (categórico)
    df = df.withColumn("statusrf", F.trim(F.upper(F.col("statusrf"))))

    # 6) Casting de var_* (numéricas)
    print(">>> [Transform] Tipando variáveis numéricas...")
    for var in NUMERIC_VARS:
        if var in df.columns:
            df = df.withColumn(var, to_double_safe(var))

    # Categóricas var_* (trim + upper)
    print(">>> [Transform] Normalizando variáveis categóricas...")
    for var in CATEGORICAL_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.upper(F.col(var))))

    # Variáveis mistas/outras (trim simples)
    print(">>> [Transform] Normalizando variáveis mistas...")
    for var in MIXED_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.col(var)))

    # 7) Quality gates simples (domínio)
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

    # 9) Seleção final de colunas (Silver "clean")
    # Apenas as colunas uteis + auditoria
    columns_to_select = [
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels (não usar como features)
        "flag_instalacao_int",
        "fpd_int",

        # Features cadastrais explícitas
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
    Garante grão 1:1 por NUM_CPF + SAFRA.
    Critério de desempate: metadata_data_ingestao DESC (mais recente).
    """
    print(">>> [Transform] Deduplicação por num_cpf + safra (se necessário)...")

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
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Silver_Cadastro")

    # 1) Leitura Bronze
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # 1.5) Padronização de nomes de coluna (snake_case + sem acentos)
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # 2) Transform Silver
    df_silver = build_silver(df_bronze)

    # 3) Dedup por chave
    df_silver_dedup = dedupe_by_key(df_silver)

    count_out = df_silver_dedup.count()
    print(f">>> [Info] Registros na Silver (após dedupe): {count_out}")
    print(f">>> [Info] Linhas removidas no dedupe: {count_in - count_out}")

    # 4) Escrita Silver
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")

    df_silver_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # -------------------------------------------------------------------------
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # -------------------------------------------------------------------------
    target_table = "hackathon_2025.default.silver_cadastro"
    df_silver_dedup.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog, destino: {target_table}.")
    # -------------------------------------------------------------------------

    # 5) Quality checks (rápidos) -- retirar quando em produção
    print(">>> [Quality] Checando domínios e unicidade...")

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
    print(f">>> [Quality] data nascimento inválida: {dt_nasc_invalida}")
    print(f">>> [Quality] idade_menor_18 (ineligível): {idade_menor_18} ({idade_menor_18*100/count_out:.2f}%)")
    print(f">>> [Quality] idade_muito_alta (outlier >100): {idade_muito_alta} ({idade_muito_alta*100/count_out:.2f}%)")
    print(f">>> [Quality] cep missing: {cep_missing} ({cep_missing*100/count_out:.2f}%) | cobertura: {cep_coverage:.1f}%")

    print(f">>> [Sucesso] Silver cadastro concluído.")

if __name__ == "__main__":
    main()
