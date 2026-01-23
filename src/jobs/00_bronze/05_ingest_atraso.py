"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 05_ingest_atraso.py
OBJETIVO: Ingestão da camada Landing (Raw) para Bronze — Atraso/Faturamento.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê os dados brutos (Parquet) da base de Atraso/Faturamento do 
volume de origem (Landing), adiciona metadados de auditoria (data de ingestão 
e origem) e salva em formato Delta Lake na camada Bronze.

ESTRUTURA ESPERADA (Landing):
- Tabela snapshot: fotografia mensal (DAT_REFERENCIA sempre dia 01)
- Grão: múltiplas linhas por CPF (fatura/item/ajuste)
- Chaves naturais: NUM_CPF + DAT_REFERENCIA + NUM_FATURA_HASH (com duplicatas)
- Datas: DAT_REFERENCIA (completa, sempre 01), DAT_STATUS_FAT (missing ~3.4%)
- Valores: monetários em string (casting será feito na Silver)
- Particularidades: alto volume (31.6M linhas), histórico legado até 2006

AJUSTES UNITY CATALOG:
- Utiliza caminhos no formato /Volumes/...
- Utiliza colunas ocultas (_metadata) para rastreio de origem.
- Trata argumentos de sistema do Databricks Notebook.

ANTI-LEAKAGE:
- Este é um snapshot mensal (não há eventos futuros aqui).
- Todas as linhas representam o estado NA DATA DA FOTO (DAT_REFERENCIA).
- Seguro para usar na Gold para aggregação por safra.

PRÓXIMAS CAMADAS:
→ Silver: 05_bronze_silver_atraso.py
→ Gold: aggregação para ABT v6
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
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/source/book_atraso/dados_faturamento/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/bronze/atraso_delta/"
DEFAULT_FORMAT = "parquet"
BRONZE_VERSION = "bronze_atraso_v1"

# =============================================================================

def add_metadata(df):
    """
    Adiciona colunas de controle exigidas na camada Bronze.
    
    Metadados incluídos:
    - metadata_data_ingestao: timestamp de processamento
    - metadata_nome_arquivo_origem: arquivo original (Unity Catalog)
    - metadata_sistema_origem: sistema de origem (HACKATHON_LANDING)
    - bronze_version: versão do schema Bronze
    
    NOTA SOBRE UNITY CATALOG:
    A função F.input_file_name() é bloqueada em alguns modos do UC.
    A forma correta é acessar a coluna oculta "_metadata.file_path".
    """
    print(">>> [Transform] Adicionando metadados de ingestão (Atraso)...")
    
    return df \
        .withColumn("metadata_data_ingestao", F.current_timestamp()) \
        .withColumn("metadata_nome_arquivo_origem", F.col("_metadata.file_path")) \
        .withColumn("metadata_sistema_origem", F.lit("HACKATHON_LANDING")) \
        .withColumn("bronze_version", F.lit(BRONZE_VERSION))

def main():
    # Definição dos argumentos esperados
    parser = argparse.ArgumentParser(description="ETL Landing to Bronze — Atraso/Faturamento")
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
    if args_parsed.input_path and args_parsed.output_path:
        # Se veio via Job (argumentos explícitos), usa eles
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
    spark = get_spark_session("Bronze_Ingestion_Atraso")
    
    # =========================================================================
    # 1. LEITURA (LANDING)
    # =========================================================================
    print(f">>> [Leitura] Carregando Atraso/Faturamento (Landing): {args.input_path}")
    try:
        df_landing = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)
    
    count_landing = df_landing.count()
    print(f">>> [Info] Registros carregados: {count_landing:,}")
    
    # =========================================================================
    # 2. TRANSFORMAÇÕES (METADADOS + AUDITORIA)
    # =========================================================================
    print(">>> [Transform] Processando metadados...")
    df_bronze = add_metadata(df_landing)
    
    count_bronze = df_bronze.count()
    print(f">>> [Info] Registros após transformação: {count_bronze:,}")
    
    # Auditoria simples
    if count_bronze != count_landing:
        print(f"!!! AVISO: Retenção diferente! In: {count_landing}, Out: {count_bronze}")
    else:
        print(f"✓ Retenção OK: {count_bronze:,} registros preservados")
    
    # =========================================================================
    # 3. ESCRITA (BRONZE — DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando em Bronze (Delta): {args.output_path}")
    
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
    
    print(f"✓ Escrita em Delta concluída")
    
    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.bronze_atraso"
    df_bronze.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    
    # =========================================================================
    # 4. RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL — Bronze Atraso/Faturamento")
    print("="*80)
    print(f"  Registros de entrada (Landing): {count_landing:,}")
    print(f"  Registros de saída (Bronze):    {count_bronze:,}")
    print(f"  Retenção: {(count_bronze/count_landing)*100:.2f}%")
    print(f"  Colunas originais: {len(df_landing.columns)}")
    print(f"  Colunas após metadados: {len(df_bronze.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Próximo passo: Silver (05_bronze_silver_atraso.py)")
    print(f"  Nota: Snapshot mensal (DAT_REFERENCIA sempre dia 01) — anti-leakage garantido")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
