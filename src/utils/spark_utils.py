# Arquivo: src/utils/spark_utils.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_spark_session(app_name="Hackathon_App"):
    """
    Centraliza a criação da SparkSession.
    Se precisarmos mudar a configuração do Delta ou S3/OCI, mudamos SÓ AQUI.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def standardize_column_names(df):
    """
    Padroniza colunas para snake_case e remove acentos.
    Útil para toda camada Silver.
    """
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
    """
    Converte string -> int de forma segura (vazio/null vira null).
    
    Uso: df.withColumn("col_int", to_int_safe("col_str"))
    """
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

def to_double_safe(colname):
    """
    Converte string -> double de forma segura (vazio/null vira null).
    Para valores não numéricos, retorna NULL (não falha).
    
    Uso: df.withColumn("col_dbl", to_double_safe("col_str"))
    """
    # Primeiro trata NULL e vazio
    col_trimmed = F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)).otherwise(F.trim(F.col(colname)))
    
    # Depois tenta cast, retornando NULL se falhar (não numérico)
    # Usa regex para checar se é numérico antes de fazer cast
    return F.when(
        F.col(colname).isNull() | (F.trim(F.col(colname)) == ""),
        F.lit(None)
    ).when(
        # Verifica se é numérico (inteiro ou decimal, positivo ou negativo)
        F.trim(F.col(colname)).rlike("^[+-]?([0-9]*[.,])?[0-9]+$"),
        F.col(colname).cast("double")
    ).otherwise(
        F.lit(None)
    )

def to_date_safe(colname, date_format="dd/MM/yyyy"):
    """
    Converte string -> date de forma tolerante (valores inválidos vira null).
    Não falha em parsing inválido.
    
    Uso: df.withColumn("col_date", to_date_safe("col_str", "dd/MM/yyyy"))
    """
    return F.when(
        (F.col(colname).isNull()) | (F.trim(F.col(colname)) == ""),
        F.lit(None)
    ).otherwise(
        F.from_unixtime(
            F.unix_timestamp(F.col(colname), date_format),
            date_format
        ).cast("date")
    )

def treat_sentinel_value(colname, sentinel_values=[304]):
    """
    Trata sentinelas (ex: 304 = "não informado") criando flag de missing.
    Retorna um dicionário com:
    - colname_treated: coluna com sentinela convertida para NULL
    - flag_colname_missing: flag indicando se é missing (NULL, vazio ou sentinela)
    
    Uso:
    result = treat_sentinel_value("var_29", sentinel_values=[304])
    df = df.withColumn(result["colname_treated"], result["expr_treated"]) \
           .withColumn(result["flag_name"], result["expr_flag"])
    
    IMPORTANTE: A comparação com sentinelas é feita em STRING para evitar
    erros de cast implícito com valores decimais.
    """
    # Converter sentinelas para string para comparação segura
    sentinel_str_values = [str(s) for s in sentinel_values]
    
    # Checar se é NULL, vazio ou sentinela (como string)
    sentinel_condition = F.col(colname).isin(sentinel_str_values)
    
    # Fazer o cast para double APÓS a comparação da sentinela
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