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