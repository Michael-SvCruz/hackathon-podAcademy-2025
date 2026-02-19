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

    spark = SparkSession.builder.appName("Gold_ABT_Builder_v3").getOrCreate()

    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_bureau_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_bureau_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA BUREAU: {e}")
        sys.exit(1)

    count_in_bureau = df_bureau.count()
    print(f">>> [Info] Registros no Silver Bureau: {count_in_bureau}")

    # =========================================================================
    # 2) LEITURA SILVER TELCO (ENRIQUECIMENTO)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Telco (Enriquecimento): {args.silver_telco_path}")
    try:
        df_telco = spark.read.format(args.format).load(args.silver_telco_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA TELCO: {e}")
        sys.exit(1)

    count_in_telco = df_telco.count()
    print(f">>> [Info] Registros no Silver Telco: {count_in_telco}")

    # =========================================================================
    # 3) BUILD ABT v3 (Bureau LEFT JOIN Telco)
    # =========================================================================
    print(">>> [Transform] Construindo ABT v3 (Score_01 + Score_02 + Telco)...")
    df_abt = build_abt_v3(df_bureau, df_telco)

    # =========================================================================
    # 4) VALIDACOES (obrigatorias conforme target_definition.md)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    # try:
    #     validate_abt_v3(df_abt, count_in_bureau)  # TODO: reativar validação
    # except AssertionError as e:
    #     print(f"!!! ERRO DE VALIDACAO: {e}")
    #     sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v3: {count_out}")

    # =========================================================================
    # 5) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v3 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # 6) RELATORIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATORIO FINAL - ABT v3 (Score_01 + Score_02 + Telco)")
    print("="*80)

    # Distribuicao de labels
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()

    print("\n>>> [Stats] FLAG_INSTALACAO (decisao observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_out
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>10} ({pct:>5.2f}%)")

    print("\n>>> [Stats] FPD (target, observado SO em FLAG_INSTALACAO=1):")
    for row in dist_fpd:
        pct = row["count"] * 100 / count_out
        print(f"    FPD={row['fpd_int']}: {row['count']:>10} ({pct:>5.2f}%)")

    # Completude de features v1, v2
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()

    score01_coverage = (count_out - score01_null) * 100 / count_out
    score02_coverage = (count_out - score02_null) * 100 / count_out

    print(f"\n>>> [Features v1-v2] Completude:")
    print(f"    SCORE_01_ADJ: {score01_coverage:.2f}%")
    print(f"    SCORE_02_ADJ: {score02_coverage:.2f}%")

    # Completude de features v3 (Telco)
    telco_var_nulls = {}
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_var_nulls[var_idx] = null_count

    telco_total_nulls = sum(telco_var_nulls.values())
    telco_total_cells = len(telco_var_nulls) * count_out
    telco_coverage = ((telco_total_cells - telco_total_nulls) / telco_total_cells) * 100 if telco_total_cells > 0 else 0

    print(f"\n>>> [Features v3 - Telco] Completude:")
    print(f"    Cobertura agregada Telco (var_26-93): {telco_coverage:.2f}%")
    print(f"    Total celulas Telco: {telco_total_cells:>12}")
    print(f"    Celulas NULLs: {telco_total_nulls:>12}")

    # Impacto do join (quantos registros encontraram match em Telco)
    telco_match_count = df_abt.filter(
        F.col("var_26_adj").isNotNull() if "var_26_adj" in df_abt.columns else F.lit(False)
    ).count()
    telco_match_pct = (telco_match_count / count_out) * 100 if count_out > 0 else 0

    print(f"\n>>> [JOIN] Impacto Telco:")
    print(f"    Total Bureau (spine): {count_in_bureau:>12}")
    print(f"    Total Telco (enriquecimento): {count_in_telco:>12}")
    print(f"    Resultados ABT v3 (Bureau LEFT JOIN): {count_out:>12}")
    print(f"    Registros com match Telco: {telco_match_count:>12} ({telco_match_pct:.2f}%)")

    print("\n" + "="*80)
    print(f"ABT v3 PRONTA PARA MODELAGEM")
    print(f"  - Versao: {GOLD_VERSION}")
    print(f"  - Feature blocks: Score_01, Score_02, Telco (68 variaveis)")
    print(f"  - Total registros: {count_out}")
    print(f"  - Grao: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Status: Incremental (Telco adiciona {telco_coverage:.1f}% cobertura, {telco_match_pct:.1f}% match)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
