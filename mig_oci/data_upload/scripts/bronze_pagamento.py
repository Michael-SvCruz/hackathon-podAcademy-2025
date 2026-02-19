# Arquivo: mig_oci/data_upload/scripts/bronze_pagamento.py
# Adaptado de src/jobs/00_bronze/04_ingest_pagamento.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, F.input_file_name(), sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: bronze_pagamento.py (OCI Data Flow)
OBJETIVO: Ingestao da camada Landing (Raw) para Bronze - Pagamento.
--------------------------------------------------------------------------------
ESTRUTURA ESPERADA (Landing):
- Tabela transacional: multiplas linhas por cliente
- Chaves naturais: NUM_CPF + CONTRATO + SEQ_FATURA + NUM_SUB_SEQ_FATURA + NUM_CREDITO_SEQ
- Datas: DAT_STATUS_FATURA (completa), DAT_STATUS_PAGAMENTO (missing ~28%)
- Valores: monetarios em string (casting sera feito na Silver)
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
DEFAULT_INPUT_PATH = f"oci://hackathon-2025-landing-zone@{namespace}/source/pagamento/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-bronze-layer@{namespace}/pagamento/"
DEFAULT_FORMAT = "parquet"
BRONZE_VERSION = "bronze_pagamento_v1"
# =============================================================================

def add_metadata(df):
    """
    Adiciona colunas de controle exigidas na camada Bronze.

    NOTA SOBRE OCI DATA FLOW:
    Usa F.input_file_name() ao inves de _metadata.file_path (Databricks-specific).
    """
    print(">>> [Transform] Adicionando metadados de ingestao (Pagamento)...")

    return df \
        .withColumn("metadata_data_ingestao", F.current_timestamp()) \
        .withColumn("metadata_nome_arquivo_origem", F.input_file_name()) \
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING")) \
        .withColumn("bronze_version", F.lit(BRONZE_VERSION))

def main():
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze - Pagamento")
    parser.add_argument("--input_path", help="Caminho do arquivo na Landing Zone")
    parser.add_argument("--output_path", help="Caminho de destino na Bronze Zone")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem")

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

    spark = SparkSession.builder.appName("Bronze_Ingestion_Pagamento").getOrCreate()

    # =========================================================================
    # 1. LEITURA (LANDING)
    # =========================================================================
    print(f">>> [Leitura] Carregando Pagamento (Landing): {args.input_path}")
    try:
        df_landing = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)

    count_landing = df_landing.count()
    print(f">>> [Info] Registros carregados: {count_landing:,}")

    # =========================================================================
    # 2. TRANSFORMACOES (METADADOS + AUDITORIA)
    # =========================================================================
    print(">>> [Transform] Processando metadados...")
    df_bronze = add_metadata(df_landing)

    count_bronze = df_bronze.count()
    print(f">>> [Info] Registros apos transformacao: {count_bronze:,}")

    # Auditoria simples
    if count_bronze != count_landing:
        print(f"!!! AVISO: Retencao diferente! In: {count_landing}, Out: {count_bronze}")
    else:
        print(f"Retencao OK: {count_bronze:,} registros preservados")

    # =========================================================================
    # 3. ESCRITA (BRONZE - DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando em Bronze (Delta): {args.output_path}")

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    print(f"Escrita em Delta concluida")

    # =========================================================================
    # 4. RELATORIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATORIO FINAL - Bronze Pagamento")
    print("="*80)
    print(f"  Registros de entrada (Landing): {count_landing:,}")
    print(f"  Registros de saida (Bronze):    {count_bronze:,}")
    print(f"  Retencao: {(count_bronze/count_landing)*100:.2f}%")
    print(f"  Colunas originais: {len(df_landing.columns)}")
    print(f"  Colunas apos metadados: {len(df_bronze.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Proximo passo: Silver (bronze_silver_pagamento)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
