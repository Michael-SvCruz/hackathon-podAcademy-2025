"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 01_bronze_silver_telco.py
OBJETIVO: Transformação da camada Bronze para Silver - Base TELCO.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (telco), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA), trata sentinelas de valor 304
em variáveis anonimizadas (var_26 a var_93) e garante o grão 1:1 por 
NUM_CPF + SAFRA através de deduplicação controlada.

REGRAS DE NEGÓCIO (BASEADO EM telco.md):
- Grain esperado: 1 linha por NUM_CPF + SAFRA (confirmado em EDA).
- FLAG_INSTALACAO é label de decisão/política (0/1).
- FPD é label de risco (0/1) com ~3,36% de missing (usar com cautela).
- Variáveis var_26 a var_93: features anonimizadas (cast para double).
- SENTINELA 304: muito frequente em var_* (não informado/não aplicável).
  → Trata convertendo 304 para NULL e criando flag de missing.
- PROD e flag_mig2: metadados de origem (manter como string).

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
    treat_sentinel_value
)

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/telco_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/telco_silver_delta/"
DEFAULT_FORMAT = "delta"

# Lista de colunas var_* esperadas (var_26 a var_93 = 68 colunas)
VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# =============================================================================

def build_silver(df_bronze):
    """
    Aplica tipagem explícita, cria derivadas e trata sentinelas da Telco.
    
    Pipeline:
    1. Tipagem básica (NUM_CPF, SAFRA, FLAG_INSTALACAO, FPD como int/string)
    2. Criação de DT_SAFRA derivada
    3. Casting de var_* para double
    4. Tratamento de sentinela 304 em var_* (criar flags de missing)
    5. Quality gates (validação de domínio)
    6. Seleção final de colunas
    """
    print(">>> [Transform] Tipagem + regras Silver (telco)...")

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

    # 3) Casting de var_* para double com tratamento de sentinela 304
    print(">>> [Transform] Tratando sentinela 304 em var_*...")
    
    for var_col in VAR_COLUMNS:
        if var_col in df.columns:
            treatment = treat_sentinel_value(var_col, sentinel_values=[304])
            df = df \
                .withColumn(treatment["colname_treated"], treatment["expr_treated"]) \
                .withColumn(treatment["flag_name"], treatment["expr_flag"])
        else:
            print(f"!!! AVISO: Coluna {var_col} não encontrada no DataFrame.")

    # 4) Quality gates simples (domínio)
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when(~F.col("flag_instalacao_int").isin(0, 1) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when(~F.col("fpd_int").isin(0, 1) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # 5) Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_telco_v1"))

    # 6) Seleção de colunas finais (Silver "clean")
    # Chaves
    df_silver = df.select(
        "num_cpf",
        "safra",
        "dt_safra",
        
        # Labels/Política (não usar como features)
        "flag_instalacao_int",
        "fpd_int",
        
        # Metadados de origem
        "prod",
        "flag_mig2",
        
        # Features var_* (versão ajustada com tratamento de 304)
        *[f"var_{i}_adj" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],
        
        # Flags de missing para var_*
        *[f"flag_var_{i}_missing" for i in range(26, 94) if f"var_{i}" in df_bronze.columns],
        
        # Quality flags
        "flag_instalacao_invalida",
        "fpd_invalido",
        
        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

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
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base TELCO")
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

    spark = get_spark_session("Silver_Telco")

    # 1) Leitura Bronze
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # 1.5) Padronização de nomes de coluna (Snake_case + sem acentos)
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
    target_table = "hackathon_2025.default.silver_telco"
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

    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()

    fpd_null = df_silver_dedup.filter(F.col("fpd_int").isNull()).count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] fpd null: {fpd_null} ({fpd_null*100/count_out:.2f}%)")
    print(f">>> [Quality] distinct num_cpf+safra: {distinct_key} | total_out: {count_out}")

    print(f">>> [Sucesso] Silver telco concluído.")

if __name__ == "__main__":
    main()
