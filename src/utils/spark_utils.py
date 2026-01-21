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
    
    Uso: df.withColumn("col_dbl", to_double_safe("col_str"))
    """
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("double"))

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
    """
    col_expr = F.col(colname).cast("double")
    
    sentinel_condition = F.col(colname).isin(sentinel_values)
    
    expr_treated = F.when(
        col_expr.isNull() | (F.trim(F.col(colname)) == "") | sentinel_condition,
        F.lit(None)
    ).otherwise(col_expr)
    
    expr_flag = F.when(
        col_expr.isNull() | (F.trim(F.col(colname)) == "") | sentinel_condition,
        F.lit(1)
    ).otherwise(F.lit(0))
    
    return {
        "colname_treated": f"{colname}_adj",
        "flag_name": f"flag_{colname}_missing",
        "expr_treated": expr_treated,
        "expr_flag": expr_flag
    }