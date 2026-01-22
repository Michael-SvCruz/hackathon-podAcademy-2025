"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 03_ingest_recarga.py
OBJETIVO: Ingestão da camada Landing (Raw) para Bronze - Base RECARGA.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê os dados brutos (Parquet) do volume de origem (Landing),
adiciona metadados de auditoria (data de ingestão e origem) e salva
em formato Delta Lake na camada Bronze.

CARACTERÍSTICAS ESPECÍFICAS - BASE RECARGA:
- Fonte: base_recargas com eventos de recarga de cliente (transacional)
- Grão esperado: Múltiplos registros por NUM_CPF + DT_RECARGA
- Variáveis principais: NUM_CPF, DT_RECARGA, VALOR_RECARGA, TIPO_RECARGA
- Observações: Data de recarga é a chave temporal (não SAFRA)
- Período: 12-36 meses de histórico (necessário para features v5)
- Tipagem em Bronze: mantém como string (tipagem será feita na Silver)

OBSERVAÇÕES:
- Aggregação por SAFRA (mês) será feita na Silver (GROUP BY NUM_CPF + SAFRA)
- Features temporais (M1, M3, M6) serão calculadas na Gold v5
- Tratamento de valores NULL/sentinelas será feito na Silver
- Validações de qualidade serão feitas na Silver

DIFERENÇAS COM CADASTRO/BUREAU:
- BASE RECARGA é EVENT-LEVEL (múltiplos registros por cliente)
- Requer aggregação para chegar a cliente-mês (NUM_CPF + SAFRA)
- Necessário parsear data de recarga para derivar SAFRA (primeiro dia do mês)

AJUSTES UNITY CATALOG:
- Utiliza caminhos no formato /Volumes/...
- Utiliza colunas ocultas (_metadata) para rastreio de origem.
- Trata argumentos de sistema do Databricks Notebook.
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from src.utils.spark_utils import get_spark_session

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
# Caminhos apontando para os Volumes do Unity Catalog
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/source/base_recargas/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/bronze/recarga_delta/"
DEFAULT_FORMAT = "parquet"
# =============================================================================

def add_metadata(df):
    """
    Adiciona colunas de controle exigidas na camada Bronze.
    
    Colunas adicionadas:
    - metadata_data_ingestao: timestamp do momento da ingestão
    - metadata_nome_arquivo_origem: caminho do arquivo de origem (via _metadata)
    - metadata_sistema_origem: identificação do sistema de origem
    
    NOTA SOBRE UNITY CATALOG:
    A função F.input_file_name() é bloqueada em alguns modos do UC.
    A forma correta é acessar a coluna oculta "_metadata.file_path".
    """
    print(">>> [Transform] Adicionando metadados de ingestão...")
    
    return df \
        .withColumn("metadata_data_ingestao", F.current_timestamp()) \
        .withColumn("metadata_nome_arquivo_origem", F.col("_metadata.file_path")) \
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING_RECARGA"))

def main():
    # Definição dos argumentos esperados
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze - Base RECARGA")
    parser.add_argument("--input_path", help="Caminho do arquivo na Landing Zone")
    parser.add_argument("--output_path", help="Caminho de destino na Bronze Zone")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem")
    
    # -------------------------------------------------------------------------
    # TRATAMENTO DE ARGUMENTOS (FIX PARA NOTEBOOK DATABRICKS)
    # -------------------------------------------------------------------------
    # O Databricks injeta argumentos como '-f /kernel/...' que quebram o parser.
    # Usamos parse_known_args() para capturar apenas o que definimos e ignorar o resto.
    args_parsed, unknown_args = parser.parse_known_args()
    
    # Lógica de seleção de variáveis (Prioridade: Argumento > Padrão)
    if args_parsed.input_path:
        # Se veio via Job (argumento explícito), usa ele
        args = args_parsed
    else:
        # Se não veio argumento (Modo Interativo/Notebook), usa os defaults do topo
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    # Inicia Sessão Spark
    spark = get_spark_session("Bronze_Ingestion_Recarga")
    
    # =========================================================================
    # 1. LEITURA (LANDING)
    # =========================================================================
    print(f">>> [Leitura] Lendo dados da Landing: {args.input_path}")
    
    try:
        if args.format == "csv":
            # Para CSV, forçamos leitura como string para evitar quebra de schema
            df_landing = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .load(args.input_path)
        else:
            # Para Parquet, a leitura é direta
            df_landing = spark.read.format(args.format).load(args.input_path)
            
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_landing.count()
    print(f">>> [Info] Registros lidos da Landing: {count_in}")

    # =========================================================================
    # 2. ENRIQUECIMENTO (METADADOS)
    # =========================================================================
    # Apenas adicionamos colunas de controle. Sem limpeza de negócio na Bronze.
    df_bronze = add_metadata(df_landing)
    
    count_out = df_bronze.count()
    print(f">>> [Info] Registros após enriquecimento: {count_out}")

    # =========================================================================
    # 3. ESCRITA (BRONZE - DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando na camada Bronze (Delta): {args.output_path}")
    
    # Opções Delta:
    # mergeSchema: true -> Aceita novas colunas se a origem mudar
    # overwriteSchema: true -> Força a atualização do schema se usarmos mode overwrite
    
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
        
    print(f">>> [Sucesso] Processo finalizado. Total de registros: {count_out}")

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.bronze_recarga"
    df_bronze.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    
    # =========================================================================
    # 4) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - BRONZE RECARGA")
    print("="*80)
    print(f">>> [Stats] Ingestão RECARGA:")
    print(f"    Registros de origem (Landing): {count_in:>12}")
    print(f"    Registros em Bronze: {count_out:>12}")
    print(f"    Metadados adicionados: data_ingestao, arquivo_origem, sistema_origem")
    print(f"    Status: ✓ Pronto para Silver (aggregação para cliente-mês)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
