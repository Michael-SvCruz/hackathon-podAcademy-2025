"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 01_silver_bureau.py
OBJETIVO: Transformação da camada Bronze para Silver (Bureau Full / Spine).
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (bureau_full), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA), trata sentinelas de score e
garante o grão 1:1 por NUM_CPF + SAFRA através de deduplicação controlada.

REGRAS DE NEGÓCIO (BASEADO EM bureau.pdf):
- Grain esperado: 1 linha por NUM_CPF + SAFRA (Spine oficial).
- FLAG_INSTALACAO é label de decisão/política (0/1).
- FPD é label de risco (0/1) e pode ser nulo quando FLAG_INSTALACAO=0.
- SCORE_01 e SCORE_02 são features potenciais; inicialmente usaremos apenas SCORE_01.
- SCORE_01 = 0 deve ser tratado como sentinela/missing (criar flag e opcionalmente converter para NULL).

AJUSTES UNITY CATALOG:
- Leitura/escrita em Delta nos caminhos /Volumes/...
- Mantém colunas de auditoria da Bronze (opcional).
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session, standardize_column_names

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/bureau_full_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
DEFAULT_FORMAT = "delta"
# =============================================================================

def to_int_safe(colname):
    """Converte string -> int de forma segura (vazio/null vira null)."""
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("int"))

def to_double_safe(colname):
    """Converte string -> double de forma segura (vazio/null vira null)."""
    return F.when(F.col(colname).isNull() | (F.trim(F.col(colname)) == ""), F.lit(None)) \
            .otherwise(F.col(colname).cast("double"))

def build_silver(df_bronze):
    print(">>> [Transform] Tipagem + regras Silver (bureau_full)...")

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
        .withColumn("score_01_dbl", to_double_safe("score_01"))
        .withColumn("score_02_dbl", to_double_safe("score_02"))
    )

    # 2) DT_SAFRA (primeiro dia do mês)
    # SAFRA vem como YYYYMM -> YYYY-MM-01
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )

    # 3) Tratamento de sentinela (SCORE_01 = 0)
    df = df.withColumn(
        "flag_score01_missing",
        F.when(F.col("score_01_dbl").isNull() | (F.col("score_01_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "score_01_adj",
        F.when(F.col("score_01_dbl") == 0, F.lit(None)).otherwise(F.col("score_01_dbl"))
    )

    # (Opcional) SCORE_02 missing flag (não necessariamente usar agora no modelo)
    df = df.withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # 4) Quality gates simples (domínio)
    df = df.withColumn(
        "flag_instalacao_invalida",
        F.when(~F.col("flag_instalacao_int").isin(0, 1) & F.col("flag_instalacao_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "fpd_invalido",
        F.when(~F.col("fpd_int").isin(0, 1) & F.col("fpd_int").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
                     .withColumn("metadata_versao_regra", F.lit("silver_bureau_full_v1"))

    # 5) Seleção de colunas finais (Silver "clean")
    # Mantemos metadados da Bronze por rastreabilidade (pode remover depois)
    df_silver = df.select(
        "num_cpf",
        "safra",
        "dt_safra",

        "flag_instalacao_int",
        "fpd_int",

        # Features principais (primeira versão usa SCORE_01_ADJ)
        "score_01_adj",
        "flag_score01_missing",

        # Mantém score_02 (opcional) para evolução do modelo
        "score_02_dbl",
        "flag_score02_missing",

        "prod",
        "flag_mig2",

        "flag_instalacao_invalida",
        "fpd_invalido",

        # auditoria
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
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Bureau Full")
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

    spark = get_spark_session("Silver_Bureau_Full")

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

    # 3) Dedup por chave do spine
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
    target_table = "hackathon_2025.default.silver_bureau"
    df_silver_dedup.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog, destino: {target_table}. ")
    # -------------------------------------------------------------------------
    
    # 5) Quality checks (rápidos) -- retirar quando em produção
    print(">>> [Quality] Checando domínios e unicidade...")

    invalid_flag = df_silver_dedup.filter(F.col("flag_instalacao_invalida") == 1).count()
    invalid_fpd = df_silver_dedup.filter(F.col("fpd_invalido") == 1).count()

    distinct_key = df_silver_dedup.select("num_cpf", "safra").distinct().count()

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] distinct num_cpf+safra: {distinct_key} | total_out: {count_out}")

    print(f">>> [Sucesso] Silver bureau_full concluído.")

if __name__ == "__main__":
    main()