# Arquivo: mig_oci/data_upload/scripts/abt_v2_builder.py
# Adaptado de src/jobs/02_gold/01_gold_abt_v2_builder.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: abt_v2_builder.py (OCI Data Flow)
OBJETIVO: Construir Analytical Base Table (ABT) v2 para modelagem.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script estende ABT v1 com Score_02 como feature adicional.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ABT v2: ABT_v1 + SCORE_02                                 <- v2 (este script)
3. ABT v3: ABT_v2 + Telco features (var_26-93)
4. ABT v4: ABT_v3 + Cadastro
5. ABT v5: ABT_v4 + Recarga
6. ABT v6: ABT_v5 + Pagamento + Atraso

DEFINICOES CRITICAS (conforme target_definition.md):
- Evento ancora: cliente-mes (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SO em FLAG_INSTALACAO=1)
- Decisao observada: FLAG_INSTALACAO (para analise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ (novidade em v2)

ANTI-LEAKAGE CRITICO:
- FPD_INT e LABEL, nao feature
- FLAG_INSTALACAO e LABEL, nao feature
- Ambas incluidas apenas para auditoria e analise de impacto (swaps)

NOVO EM V2:
- SCORE_02_ADJ: Score historico secundario (sentinela 0 -> NULL)
- FLAG_SCORE02_MISSING: Flag de missing/sentinela para score_02
- gold_feature_blocks: Agora "score_01,score_02"
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço.
# from validate_abt import validate_abt_v2  # TODO: inlinar validação quando necessário

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_SILVER_BUREAU_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/bureau/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v2/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v2"

# --- Otimizacao small files ---
BYTES_PER_ROW_ESTIMATE = 50    # ~50 bytes/row (poucos features)
TARGET_FILE_SIZE_MB = 64       # Target ~64 MB por arquivo
# =============================================================================

def build_abt_v2(df_bureau):
    """
    Constroi ABT v2: estende v1 com SCORE_02 como feature adicional.

    Inclucoes v1 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria/impacto): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Metadados: prod, flag_mig2

    Novas inclusoes em v2:
    - Features v2: score_02_adj, flag_score02_missing
    - Incrementa blocos de feature (score_01,score_02)

    Exclusoes (anti-leakage):
    - FPD_INT nao pode ser feature
    - FLAG_INSTALACAO_INT nao pode ser feature
    - Telco features ficam para v3
    """
    print(">>> [Transform] Selecionando colunas para ABT v2...")

    # Selecao de colunas ordenadas logicamente
    df_abt = df_bureau.select(
        # CHAVES (obrigatorias para identificacao)
        "num_cpf",
        "safra",
        "dt_safra",

        # LABELS (para auditoria e analise de impacto - NAO usar como features)
        "flag_instalacao_int",     # Decisao observada (0/1)
        "fpd_int",                 # Target de risco (0/1, observado SO em flag_instalacao_int=1)

        # FEATURES v1 (SCORE_01) - MANTIDAS DE V1
        "score_01_adj",            # Score 1 ajustado (sentinela 0 -> NULL)
        "flag_score01_missing",    # Flag de missing/sentinela para score_01

        # FEATURES v2 (SCORE_02) - NOVAS
        "score_02_dbl",            # Score 2 tipado (ja vem como double da Silver)
        "flag_score02_missing",    # Flag de missing para score_02

        # METADADOS DE ORIGEM
        "prod",
        "flag_mig2",

        # AUDITORIA (rastreabilidade)
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    # Tratar sentinela em SCORE_02: valor 0 e sentinela (nao informado)
    # Converter para NULL e manter flag de missing
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    )

    # Remover coluna score_02_dbl (usar apenas ajustada)
    df_abt = df_abt.drop("score_02_dbl")

    # Reordenar colunas para clareza (features agrupadas)
    df_abt = df_abt.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels
        "flag_instalacao_int",
        "fpd_int",

        # Features Score_01 (v1)
        "score_01_adj",
        "flag_score01_missing",

        # Features Score_02 (v2 novo)
        "score_02_adj",
        "flag_score02_missing",

        # Metadados
        "prod",
        "flag_mig2",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    # Adicionar metadados de gold
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02"))  # Quais blocos estao em v2

    return df_abt

def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v2 - Score_01 + Score_02")
    parser.add_argument("--silver_path", help="Caminho da Silver Bureau (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.silver_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            silver_path = DEFAULT_SILVER_BUREAU_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("abt_v2_optz_sf").getOrCreate()

    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) BUILD ABT v2 + ESCRITA — ACTION 1
    # =========================================================================
    print(">>> [Transform] Construindo ABT v2 (Score_01 + Score_02)...")
    df_abt = build_abt_v2(df_bureau)

    print(f">>> [Escrita] Salvando Gold ABT v2 (Delta): {args.output_path}")

    # --- Coalesce dinamico (otimizacao small files) ---
    count_abt = df_abt.count()
    estimated_size_mb = count_abt * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Otimizacao] coalesce({num_output_files}) — ~{TARGET_FILE_SIZE_MB}MB/arquivo")
    print(f">>> [Otimizacao] {count_abt:,} registros, ~{estimated_size_mb:.0f} MB estimados")

    df_abt.coalesce(num_output_files) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
    print(">>> [Escrita] ABT v2 gravada com sucesso.")

    # =========================================================================
    # 3) QUALITY CHECK na ABT JA ESCRITA — ACTION 2
    # =========================================================================
    print(f"\n>>> [Quality] Lendo ABT v2 gravada para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.countDistinct("num_cpf", "safra").alias("distinct_keys"),
        F.sum(F.when(F.col("num_cpf").isNull(), 1).otherwise(0)).alias("nulos_cpf"),
        F.sum(F.when(F.col("safra").isNull(), 1).otherwise(0)).alias("nulos_safra"),
        # Anti-leakage gates
        F.sum(F.when(F.col("flag_instalacao_int") == 1, 1).otherwise(0)).alias("instalados"),
        F.sum(F.when(F.col("flag_instalacao_int") == 0, 1).otherwise(0)).alias("nao_instalados"),
        F.sum(F.when(F.col("fpd_int") == 1, 1).otherwise(0)).alias("fpd_1"),
        F.sum(F.when(F.col("fpd_int") == 0, 1).otherwise(0)).alias("fpd_0"),
        F.sum(F.when(
            (F.col("fpd_int").isNotNull()) & (F.col("flag_instalacao_int") == 0), 1
        ).otherwise(0)).alias("fpd_sem_instalacao"),
        # Cobertura de features
        F.sum(F.when(F.col("score_01_adj").isNull(), 1).otherwise(0)).alias("score01_null"),
        F.sum(F.when(F.col("score_02_adj").isNull(), 1).otherwise(0)).alias("score02_null"),
        # Cobertura dupla (ambos scores disponiveis)
        F.sum(F.when(
            F.col("score_01_adj").isNotNull() & F.col("score_02_adj").isNotNull(), 1
        ).otherwise(0)).alias("ambos_scores"),
    ).collect()[0]

    count_out       = quality["total"]
    distinct_keys   = quality["distinct_keys"]
    nulos_cpf       = quality["nulos_cpf"]
    nulos_safra     = quality["nulos_safra"]
    instalados      = quality["instalados"]
    nao_inst        = quality["nao_instalados"]
    fpd_1           = quality["fpd_1"]
    fpd_0           = quality["fpd_0"]
    fpd_sem_inst    = quality["fpd_sem_instalacao"]
    score01_null    = quality["score01_null"]
    score02_null    = quality["score02_null"]
    ambos_scores    = quality["ambos_scores"]
    score01_cov     = 100 * (count_out - score01_null) / count_out if count_out > 0 else 0
    score02_cov     = 100 * (count_out - score02_null) / count_out if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - ABT v2 (Score_01 + Score_02)")
    print("="*80)
    print(f"\n    Registros ABT (grain 1:1 CPF+SAFRA):    {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):       {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                           {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - NUM_CPF nulos:                 {nulos_cpf:>12,}  [esperado = 0]")
    print(f"    Gate 2 - SAFRA nulos:                   {nulos_safra:>12,}  [esperado = 0]")
    print(f"\n    FLAG_INSTALACAO = 1 (instalados):       {instalados:>12,}  ({100*instalados/count_out:.2f}%)")
    print(f"    FLAG_INSTALACAO = 0 (nao instalados):  {nao_inst:>12,}  ({100*nao_inst/count_out:.2f}%)")
    print(f"\n    FPD = 1 (default):                      {fpd_1:>12,}  ({100*fpd_1/count_out:.2f}%)")
    print(f"    FPD = 0 (nao default):                  {fpd_0:>12,}  ({100*fpd_0/count_out:.2f}%)")
    print(f"\n    Gate 3 - FPD sem FLAG_INSTALACAO=1:     {fpd_sem_inst:>12,}  [esperado = 0 — anti-leakage]")
    print(f"    Anti-leakage OK:                        {'SIM' if fpd_sem_inst == 0 else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 4 - SCORE_01_ADJ cobertura:        {score01_cov:>11.2f}%  [esperado ~98.18%]")
    print(f"    Gate 5 - SCORE_02_ADJ cobertura:        {score02_cov:>11.2f}%  [esperado ~99.95%]")
    print(f"    Registros com ambos scores:             {ambos_scores:>12,}  ({100*ambos_scores/count_out:.2f}%)")

    print("\n" + "="*80)
    print(f"ABT v2 PRONTA PARA MODELAGEM")
    print(f"  - Versao:          {GOLD_VERSION}")
    print(f"  - Feature blocks:  Score_01, Score_02")
    print(f"  - Registros:       {count_out:,}")
    print(f"  - Actions:         2 (select+write + agg ABT escrita)")
    print(f"  - Grao:            1:1 NUM_CPF + SAFRA")
    print(f"  - Target:          FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Proximo passo:   abt_v3_builder.py")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
