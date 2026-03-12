# Arquivo: mig_oci/data_upload/scripts/abt_v3_builder.py
# Adaptado de src/jobs/02_gold/02_gold_abt_v3_builder.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: abt_v3_builder.py (OCI Data Flow)
OBJETIVO: Construir Analytical Base Table (ABT) v3 para modelagem.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script estende ABT v2 com features Telco (var_26-93) via join direto.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ABT v2: ABT_v1 + SCORE_02                                 [COMPLETED]
3. ABT v3: ABT_v2 + Telco features (var_26-93)              <- v3 (este script)
4. ABT v4: ABT_v3 + Cadastro
5. ABT v5: ABT_v4 + Recarga
6. ABT v6: ABT_v5 + Pagamento + Atraso

DEFINICOES CRITICAS (conforme target_definition.md):
- Evento ancora: cliente-mes (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SO em FLAG_INSTALACAO=1)
- Decisao observada: FLAG_INSTALACAO (para analise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ + TELCO (68 variaveis)

ANTI-LEAKAGE CRITICO:
- FPD_INT e LABEL, nao feature
- FLAG_INSTALACAO e LABEL, nao feature
- Ambas incluidas apenas para auditoria e analise de impacto (swaps)

NOVO EM V3:
- VAR_26_ADJ a VAR_93_ADJ: 68 variaveis Telco anonimizadas (sentinela 304 -> NULL)
- FLAG_VAR_*_MISSING: 68 flags para capturas de missing (304 ou NULL original)
- PROD e flag_mig2: Metadados Telco retidos para analise
- gold_feature_blocks: Agora "score_01,score_02,telco"
- Trato de sentinela 304: Conversao para NULL com flags de missing

ESTRUTURA DO JOIN:
- Spine (Bureau Silver) : 1:1 por NUM_CPF + SAFRA
- Telco Silver          : 1:1 por NUM_CPF + SAFRA
- Join type            : LEFT (Bureau e spine, Telco e enriquecimento)
- Resultado            : 1:1 mantida, Telco features como NULLs quando nao encontrado
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço.
# from validate_abt import validate_abt_v3  # TODO: inlinar validação quando necessário

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_SILVER_BUREAU_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/bureau/"
DEFAULT_SILVER_TELCO_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/telco/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v3/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v3"

# --- Otimizacao small files ---
BYTES_PER_ROW_ESTIMATE = 100   # ~100 bytes/row (89 features)
TARGET_FILE_SIZE_MB = 64       # Target ~64 MB por arquivo

# Variaveis Telco esperadas (var_26 a var_93 = 68 colunas)
TELCO_VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
# =============================================================================

def build_abt_v3(df_bureau, df_telco):
    """
    Constroi ABT v3: estende v2 com features Telco (var_26-93).

    Inclucoes v1 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria/impacto): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Metadados: prod, flag_mig2

    Inclucoes v2 (mantidas):
    - Features v2: score_02_adj, flag_score02_missing

    Novas inclusoes em v3:
    - Features v3: var_26_adj a var_93_adj (68 variaveis Telco)
    - Flags v3: flag_var_26_missing a flag_var_93_missing
    - Metadados Telco: prod (Telco), flag_mig2 (Telco)
    - Incrementa blocos de feature (score_01,score_02,telco)

    Exclusoes (anti-leakage):
    - FPD_INT nao pode ser feature
    - FLAG_INSTALACAO_INT nao pode ser feature
    """
    print(">>> [Transform] JOIN Bureau + Telco para ABT v3...")

    # Step 1: Preparar subset do Bureau para join (apenas chaves + v1/v2 features)
    df_bureau_prepared = df_bureau.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels
        "flag_instalacao_int",
        "fpd_int",

        # Features v1
        "score_01_adj",
        "flag_score01_missing",

        # Features v2 (raw, sera ajustado aqui em v3)
        "score_02_dbl",

        # Metadados Bureau
        "prod",
        "flag_mig2",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )

    # Step 2: Preparar subset do Telco para join (apenas var_*_adj + flags)
    telco_cols_to_select = ["num_cpf", "safra"]

    # Adicionar var_*_adj (ajustadas com sentinela tratada)
    for var_idx in range(26, 94):
        var_col_adj = f"var_{var_idx}_adj"
        if var_col_adj in df_telco.columns:
            telco_cols_to_select.append(var_col_adj)

    # Adicionar flags de missing
    for var_idx in range(26, 94):
        flag_col = f"flag_var_{var_idx}_missing"
        if flag_col in df_telco.columns:
            telco_cols_to_select.append(flag_col)

    df_telco_prepared = df_telco.select(*telco_cols_to_select)

    # Step 3: JOIN LEFT Bureau + Telco (Bureau e spine)
    print(">>> [Transform] Executando LEFT JOIN Bureau.NUM_CPF+SAFRA = Telco.NUM_CPF+SAFRA...")

    df_abt = df_bureau_prepared.join(
        df_telco_prepared,
        on=["num_cpf", "safra"],
        how="left"
    )

    # Step 4: Tratar sentinela em SCORE_02 (0 -> NULL) - CRITICO
    # Score_02 vem como score_02_dbl da Silver Bureau
    print(">>> [Transform] Tratando sentinela Score_02 (0 -> NULL)...")
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    ).drop("score_02_dbl")

    # Step 5: Selecionar e ordenar colunas logicamente
    df_abt = df_abt.select(
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

        # FEATURES v2 (SCORE_02) - MANTIDAS DE V2
        "score_02_adj",            # Score 2 ajustado (sentinela 0 -> NULL)
        "flag_score02_missing",    # Flag de missing/sentinela para score_02

        # FEATURES v3 (TELCO) - NOVAS - Variaveis anonimizadas var_26 a var_93
        *[f"var_{i}_adj" for i in range(26, 94)],
        *[f"flag_var_{i}_missing" for i in range(26, 94)],

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

    # Step 6: Adicionar metadados de gold
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco"))  # Quais blocos estao em v3

    return df_abt

def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v3 - Score_01 + Score_02 + Telco")
    parser.add_argument("--silver_bureau_path", help="Caminho da Silver Bureau (Delta)")
    parser.add_argument("--silver_telco_path", help="Caminho da Silver Telco (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.silver_bureau_path and args_parsed.silver_telco_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            silver_bureau_path = DEFAULT_SILVER_BUREAU_PATH
            silver_telco_path = DEFAULT_SILVER_TELCO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("abt_v3_optz_sf").getOrCreate()

    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_bureau_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_bureau_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA BUREAU: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) LEITURA SILVER TELCO (ENRIQUECIMENTO)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Telco (Enriquecimento): {args.silver_telco_path}")
    try:
        df_telco = spark.read.format(args.format).load(args.silver_telco_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA TELCO: {e}")
        sys.exit(1)

    # =========================================================================
    # 3) BUILD ABT v3 + ESCRITA — ACTION 1
    # =========================================================================
    print(">>> [Transform] Construindo ABT v3 (Score_01 + Score_02 + Telco)...")
    df_abt = build_abt_v3(df_bureau, df_telco)

    print(f">>> [Escrita] Salvando Gold ABT v3 (Delta): {args.output_path}")

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
    print(">>> [Escrita] ABT v3 gravada com sucesso.")

    # =========================================================================
    # 4) QUALITY CHECK na ABT JA ESCRITA — ACTION 2
    # =========================================================================
    print(f"\n>>> [Quality] Lendo ABT v3 gravada para validacao: {args.output_path}")
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
        # Cobertura de features v1 e v2
        F.sum(F.when(F.col("score_01_adj").isNull(), 1).otherwise(0)).alias("score01_null"),
        F.sum(F.when(F.col("score_02_adj").isNull(), 1).otherwise(0)).alias("score02_null"),
        # Cobertura Telco (match no JOIN): var_26_adj presente = teve match
        F.sum(F.when(F.col("var_26_adj").isNotNull(), 1).otherwise(0)).alias("telco_match"),
        # Telco coverage agregada (celulas nao nulas sobre total de celulas das 68 vars)
        *[F.sum(F.when(F.col(f"var_{i}_adj").isNotNull(), 1).otherwise(0)).alias(f"var_{i}_nn") for i in range(26, 94)],
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
    telco_match     = quality["telco_match"]
    telco_nn_total  = sum(quality[f"var_{i}_nn"] for i in range(26, 94))

    score01_cov     = 100 * (count_out - score01_null) / count_out if count_out > 0 else 0
    score02_cov     = 100 * (count_out - score02_null) / count_out if count_out > 0 else 0
    telco_match_pct = 100 * telco_match / count_out if count_out > 0 else 0
    telco_cov       = 100 * telco_nn_total / (68 * count_out) if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - ABT v3 (Score_01 + Score_02 + Telco)")
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
    print(f"\n    Gate 6 - Telco match (LEFT JOIN):       {telco_match:>12,}  ({telco_match_pct:.2f}%)  [esperado ~35.46%]")
    print(f"    Gate 7 - Telco cobertura agregada:      {telco_cov:>11.2f}%  [68 vars x {count_out:,} registros]")

    print("\n" + "="*80)
    print(f"ABT v3 PRONTA PARA MODELAGEM")
    print(f"  - Versao:          {GOLD_VERSION}")
    print(f"  - Feature blocks:  Score_01, Score_02, Telco (68 variaveis)")
    print(f"  - Registros:       {count_out:,}")
    print(f"  - Actions:         2 (join+write + agg ABT escrita)")
    print(f"  - Grao:            1:1 NUM_CPF + SAFRA")
    print(f"  - Target:          FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Proximo passo:   abt_v4_builder.py")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
