# Arquivo: mig_oci/data_upload/scripts/bronze_bureau.py
# Adaptado de src/jobs/00_bronze/00_ingest_bureau_full.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, F.input_file_name(), sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: bronze_bureau.py (OCI Data Flow)
OBJETIVO: Ingestao da camada Landing (Raw) para Bronze - Bureau.
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# NAMESPACE OCI (via argumento CLI)
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço:
# - Delta Lake (via configuration{} do Terraform)
# - Autenticação OCI (Resource Principal automático)
# Não é necessário addPyFile nem imports externos para scripts Bronze.

# =============================================================================
# CONFIGURACAO PADRAO (OCI Object Storage)
# =============================================================================
DEFAULT_INPUT_PATH = f"oci://hackathon-2025-landing-zone@{namespace}/source/bureau/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/bureau/"
DEFAULT_FORMAT = "parquet"
# =============================================================================

def add_metadata(df):
    """
    Adiciona colunas de controle exigidas na camada Bronze.

    NOTA SOBRE OCI DATA FLOW:
    Usa F.input_file_name() ao inves de _metadata.file_path (Databricks-specific).
    """
    print(">>> [Transform] Adicionando metadados de ingestao...")

    return df \
        .withColumn("metadata_data_ingestao", F.current_timestamp()) \
        .withColumn("metadata_nome_arquivo_origem", F.input_file_name()) \
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING"))

def main():
    # Definicao dos argumentos esperados
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze")
    parser.add_argument("--input_path", help="Caminho do arquivo na Landing Zone")
    parser.add_argument("--output_path", help="Caminho de destino na Bronze Zone")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem")

    # -------------------------------------------------------------------------
    # TRATAMENTO DE ARGUMENTOS
    # -------------------------------------------------------------------------
    args_parsed, unknown_args = parser.parse_known_args()

    # Logica de selecao de variaveis (Prioridade: Argumento > Padrao)
    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    # Inicia Sessao Spark
    spark = SparkSession.builder.appName("Bronze_Ingestion_Bureau").getOrCreate()

    # -------------------------------------------------------------------------
    # 1. LEITURA (LANDING)
    # -------------------------------------------------------------------------
    print(f">>> [Leitura] Lendo dados da Landing: {args.input_path}")

    try:
        if args.format == "csv":
            df_landing = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(args.input_path)
        else:
            df_landing = spark.read.format(args.format).load(args.input_path)

    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 2. ENRIQUECIMENTO (METADADOS)
    # -------------------------------------------------------------------------
    df_bronze = add_metadata(df_landing)

    # -------------------------------------------------------------------------
    # 3. ESCRITA (BRONZE - DELTA LAKE)
    # -------------------------------------------------------------------------
    print(f">>> [Escrita] Salvando na camada Bronze (Delta): {args.output_path}")

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(f">>> [Sucesso] Processo finalizado. Total de registros: {df_bronze.count()}")


if __name__ == "__main__":
    main()
