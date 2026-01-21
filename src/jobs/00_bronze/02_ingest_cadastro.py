"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 02_ingest_cadastro.py
OBJETIVO: Ingestão da camada Landing (Raw) para Bronze - Base CADASTRO.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê os dados brutos (Parquet) do volume de origem (Landing),
adiciona metadados de auditoria (data de ingestão e origem) e salva
em formato Delta Lake na camada Bronze.

CARACTERÍSTICAS ESPECÍFICAS - BASE CADASTRO:
- Fonte: base_dados_cadastrais com variáveis de perfil do cliente
- Grão esperado: 1:1 por NUM_CPF + SAFRA (confirmado em EDA)
- Variáveis principais: STATUSRF, DATADENASCIMENTO, CEP_3_digitos, var_02-var_25
- Sentinelas observadas: NULL em datas e CEP (parsing necessário na Silver)
- FPD: presente mas menor frequência que Bureau (usar Bureau como fonte de verdade)
- FLAG_INSTALACAO: {0,1}
- Tipagem em Bronze: mantém como string (tipagem explícita será feita na Silver)

OBSERVAÇÕES:
- Tratamento de datas (parsing tolerante) será feito na Silver
- Tratamento de idade (sanity check: menor de idade + idade muito alta) será feito na Silver
- CEP_3_digitos será tratado como feature categórica (regional) na Silver
- Validações de qualidade serão feitas na Silver

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
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/source/base_dados_cadastrais/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/bronze/cadastro_delta/"
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
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING_CADASTRO"))

def main():
    # Definição dos argumentos esperados
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze - Base CADASTRO")
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
    spark = get_spark_session("Bronze_Ingestion_Cadastro")
    
    # -------------------------------------------------------------------------
    # 1. LEITURA (LANDING)
    # -------------------------------------------------------------------------
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

    count_landing = df_landing.count()
    print(f">>> [Info] Registros na Landing: {count_landing}")

    # -------------------------------------------------------------------------
    # 2. ENRIQUECIMENTO (METADADOS)
    # -------------------------------------------------------------------------
    # Apenas adicionamos colunas de controle. Sem limpeza de negócio na Bronze.
    df_bronze = add_metadata(df_landing)

    # -------------------------------------------------------------------------
    # 3. ESCRITA (BRONZE - DELTA LAKE)
    # -------------------------------------------------------------------------
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
        
    print(f">>> [Sucesso] Processo finalizado. Total de registros: {df_bronze.count()}")

    # -------------------------------------------------------------------------
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # -------------------------------------------------------------------------
    target_table = "hackathon_2025.default.bronze_cadastro"
    df_bronze.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog, destino: {target_table}.")

if __name__ == "__main__":
    main()
