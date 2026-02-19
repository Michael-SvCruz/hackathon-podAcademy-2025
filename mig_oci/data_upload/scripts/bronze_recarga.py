# Arquivo: mig_oci/data_upload/scripts/bronze_recarga.py
# Adaptado de src/jobs/00_bronze/03_ingest_recarga.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, F.input_file_name(), sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: bronze_recarga.py (OCI Data Flow)
OBJETIVO: Ingestao da camada Landing (Raw) para Bronze - Base RECARGA.
--------------------------------------------------------------------------------
CARACTERISTICAS ESPECIFICAS - BASE RECARGA:
- Fonte: base_recargas com eventos de recarga de cliente (transacional)
- Grao esperado: Multiplos registros por NUM_CPF + DT_RECARGA
- Variaveis principais: NUM_CPF, DT_RECARGA, VALOR_RECARGA, TIPO_RECARGA
- Periodo: 12-36 meses de historico (necessario para features v5)
- Tipagem em Bronze: mantem como string (tipagem sera feita na Silver)

DIFERENCAS COM CADASTRO/BUREAU:
- BASE RECARGA e EVENT-LEVEL (multiplos registros por cliente)
- Requer aggregacao para chegar a cliente-mes (NUM_CPF + SAFRA)
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
DEFAULT_INPUT_PATH = f"oci://hackathon-2025-landing-zone@{namespace}/source/recarga/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/recarga/"
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
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING_RECARGA"))

def main():
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze - Base RECARGA")
    parser.add_argument("--input_path", help="Caminho do arquivo na Landing Zone")
    parser.add_argument("--output_path", help="Caminho de destino na Bronze Zone")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem")

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

    spark = SparkSession.builder.appName("Bronze_Ingestion_Recarga").getOrCreate()

    # =========================================================================
    # 1. LEITURA (LANDING)
    # =========================================================================
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

    count_in = df_landing.count()
    print(f">>> [Info] Registros lidos da Landing: {count_in}")

    # =========================================================================
    # 2. ENRIQUECIMENTO (METADADOS)
    # =========================================================================
    df_bronze = add_metadata(df_landing)

    count_out = df_bronze.count()
    print(f">>> [Info] Registros apos enriquecimento: {count_out}")

    # =========================================================================
    # 3. ESCRITA (BRONZE - DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando na camada Bronze (Delta): {args.output_path}")

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(f">>> [Sucesso] Processo finalizado. Total de registros: {count_out}")

    # =========================================================================
    # 4) RELATORIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATORIO FINAL - BRONZE RECARGA")
    print("="*80)
    print(f">>> [Stats] Ingestao RECARGA:")
    print(f"    Registros de origem (Landing): {count_in:>12}")
    print(f"    Registros em Bronze: {count_out:>12}")
    print(f"    Metadados adicionados: data_ingestao, arquivo_origem, sistema_origem")
    print(f"    Status: Pronto para Silver (aggregacao para cliente-mes)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
