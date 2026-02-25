# Arquivo: scripts/opc_standby/abt_v6_builder_original.py
# VERSAO ORIGINAL: primeira adaptacao Databricks -> OCI Data Flow
# Muitas actions: count_v5 + 2x df.count() leituras + count_v6 + validate_abt_v6 (5 actions) + gerar_relatorio_final (4+ counts)
# Referencia para comparacao com abt_v6_builder.py (optz)
"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: abt_v6_builder.py (OCI Data Flow)
OBJETIVO: Construir ABT v6 integrando ABT v5 v2 + Pagamento + Atraso Features v2
================================================================================

DESCRICAO TECNICA:
Este script e uma versao aprimorada do builder ABT v6 que utiliza as features
comportamentais geradas pelos scripts gold_*_features_v2.py.

Diferencas da versao original (05_gold_abt_v6_builder.py):
- Le de ABT v5 v2 (com 60+ features de recarga)
- Le de Gold Pagamento Features v2 (50+ features)
- Le de Gold Atraso Features v2 (60+ features)
- Total: ~250+ features para modelagem

================================================================================
ROADMAP INCREMENTAL:
================================================================================

1. ABT v1: bureau_full (spine) + SCORE_01                      [BASELINE]
2. ABT v2: v1 + SCORE_02                                       [COMPLETED]
3. ABT v3: v2 + Telco features (var_26-93)                     [COMPLETED]
4. ABT v4: v3 + Cadastro (age, CEP, var_02-25)                 [COMPLETED]
5. ABT v5 v2: v4 + Recarga (60+ features, M1/M3/M6)           [COMPLETED]
6. ABT v6 v2: v5 v2 + Pagamento (50+) + Atraso (60+)          <- ESTE SCRIPT

================================================================================
ARQUITETURA DO JOIN:
================================================================================

    ABT v5 v2 (gold/abt_v5_v2_delta)
    +-- Grao: NUM_CPF + SAFRA (cliente-mes do spine)
    +-- ~1.2M registros
    +-- Features: Score_01/02 + Telco + Cadastro + Recarga (M1/M3/M6)
                    |
    Gold Pagamento Features v2 -----+
    +-- Grao: NUM_CPF + SAFRA_PAGAMENTO
    +-- 50+ features comportamentais
                    |
    Gold Atraso Features v2 --------+--> ABT v6 v2
    +-- Grao: NUM_CPF + SAFRA_ATRASO
    +-- 60+ features comportamentais

    JOIN: LEFT por (NUM_CPF) + Filtro temporal
    SAFRA_PAGAMENTO/ATRASO < SAFRA (anti-leakage)

================================================================================
"""

import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Namespace OCI (passado como argumento)
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço:
# - Delta Lake (via configuration{} do Terraform)
# - Autenticação OCI (Resource Principal automático)

# =============================================================================
# CONFIGURACAO
# =============================================================================
DEFAULT_GOLD_ABT_V5_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v5_v2/"
DEFAULT_GOLD_PAGAMENTO_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/pagamento_features_v2/"
DEFAULT_GOLD_ATRASO_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/atraso_features_v2/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v6_v2/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v6_v2"

TEMPORAL_WINDOWS = {"m1": 1, "m3": 3, "m6": 6}


def agregar_features_por_janela(
    df_spine: DataFrame,
    df_features: DataFrame,
    safra_col: str,
    prefix: str
) -> dict:
    """
    Agrega features de uma fonte (Pagamento ou Atraso) por janelas temporais.

    Args:
        df_spine: ABT v5 com NUM_CPF, SAFRA, DT_SAFRA
        df_features: Gold features com NUM_CPF, SAFRA_*, DT_SAFRA_*
        safra_col: Nome da coluna SAFRA na fonte (safra_pagamento ou safra_atraso)
        prefix: Prefixo para as colunas (pag ou atr)

    Returns:
        Dict com DataFrames agregados por janela {m1: df, m3: df, m6: df}
    """
    print(f">>> [Agg] Agregando {prefix} por janelas temporais...")

    # Preparar spine keys
    df_keys = df_spine.select("num_cpf", "safra", "dt_safra").distinct()

    # Preparar features
    dt_safra_col = f"dt_{safra_col}"
    if dt_safra_col not in df_features.columns:
        df_features = df_features.withColumn(
            dt_safra_col,
            F.to_date(F.concat(F.col(safra_col), F.lit("01")), "yyyyMMdd")
        )

    # Identificar colunas de features (excluindo chaves e metadados)
    meta_cols = ["num_cpf", safra_col, dt_safra_col, "gold_version", "gold_build_date"]
    feature_cols = [c for c in df_features.columns if c not in meta_cols]

    agg_por_janela = {}

    for janela, num_meses in TEMPORAL_WINDOWS.items():
        print(f"    -> Janela {janela.upper()} ({num_meses} mes(es))...")

        sfx = f"_{prefix}_{janela}"

        # JOIN por NUM_CPF
        df_joined = df_keys.join(df_features, on="num_cpf", how="left")

        # Filtrar por janela temporal (anti-leakage)
        df_filtered = df_joined.filter(
            (F.col(dt_safra_col) >= F.add_months(F.col("dt_safra"), -num_meses)) &
            (F.col(dt_safra_col) < F.col("dt_safra"))
        )

        # Agregacoes
        agg_exprs = [F.count("*").alias(f"qtd_meses_dados{sfx}")]

        for col in feature_cols:
            # Colunas de contagem/soma: SUM
            if col.startswith(("qtd_", "sum_")):
                agg_exprs.append(F.sum(F.coalesce(F.col(col), F.lit(0))).alias(f"{col}{sfx}"))
            # Colunas de flag: MAX (se teve em algum mes)
            elif col.startswith("flag_"):
                agg_exprs.append(F.max(F.coalesce(F.col(col), F.lit(0))).alias(f"{col}{sfx}"))
            # Colunas de percentual/ratio/media: AVG
            elif col.startswith(("pct_", "ratio_", "avg_", "coef_", "ticket_")):
                agg_exprs.append(F.avg(F.col(col)).alias(f"{col}{sfx}"))
            # Colunas de max: MAX
            elif col.startswith("max_"):
                agg_exprs.append(F.max(F.col(col)).alias(f"{col}{sfx}"))
            # Colunas de min: MIN
            elif col.startswith("min_"):
                agg_exprs.append(F.min(F.col(col)).alias(f"{col}{sfx}"))
            # Outros: AVG como default
            else:
                agg_exprs.append(F.avg(F.col(col)).alias(f"{col}{sfx}"))

        df_agg = df_filtered.groupBy("num_cpf", "safra").agg(*agg_exprs)

        agg_por_janela[janela] = df_agg

    return agg_por_janela


def build_abt_v6(
    df_abt_v5: DataFrame,
    df_pagamento_features: DataFrame,
    df_atraso_features: DataFrame
) -> DataFrame:
    """
    Constroi ABT v6: ABT v5 v2 + Pagamento Features + Atraso Features (M1/M3/M6).
    """
    print("\n" + "="*80)
    print("CONSTRUINDO ABT v6 v2 (v5 + Pagamento + Atraso)")
    print("="*80 + "\n")

    # -------------------------------------------------------------------------
    # AGREGAR PAGAMENTO POR JANELA
    # -------------------------------------------------------------------------
    pag_por_janela = agregar_features_por_janela(
        df_abt_v5, df_pagamento_features,
        safra_col="safra_pagamento", prefix="pag"
    )

    # Combinar janelas de Pagamento
    df_pag_all = pag_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_pag_all = df_pag_all.join(pag_por_janela[janela], on=["num_cpf", "safra"], how="outer")

    # -------------------------------------------------------------------------
    # AGREGAR ATRASO POR JANELA
    # -------------------------------------------------------------------------
    atr_por_janela = agregar_features_por_janela(
        df_abt_v5, df_atraso_features,
        safra_col="safra_atraso", prefix="atr"
    )

    # Combinar janelas de Atraso
    df_atr_all = atr_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_atr_all = df_atr_all.join(atr_por_janela[janela], on=["num_cpf", "safra"], how="outer")

    # -------------------------------------------------------------------------
    # JOIN COM ABT V5
    # -------------------------------------------------------------------------
    print(">>> [Join] Combinando ABT v5 + Pagamento + Atraso...")

    df_abt_v6 = df_abt_v5.join(df_pag_all, on=["num_cpf", "safra"], how="left")
    df_abt_v6 = df_abt_v6.join(df_atr_all, on=["num_cpf", "safra"], how="left")

    # -------------------------------------------------------------------------
    # PREENCHER NULLS
    # -------------------------------------------------------------------------
    print(">>> [Clean] Preenchendo NULLs com valores default...")

    for col in df_abt_v6.columns:
        if any(x in col for x in ["_pag_", "_atr_"]):
            if col.startswith(("qtd_", "sum_", "flag_")):
                df_abt_v6 = df_abt_v6.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
            elif col.startswith(("pct_", "ratio_")):
                df_abt_v6 = df_abt_v6.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))

    # -------------------------------------------------------------------------
    # FLAGS DE COBERTURA
    # -------------------------------------------------------------------------
    print(">>> [Flags] Criando flags de cobertura...")

    for janela in TEMPORAL_WINDOWS.keys():
        # Flag sem pagamento na janela
        df_abt_v6 = df_abt_v6.withColumn(
            f"flag_sem_pagamento_{janela}",
            F.when(
                F.col(f"qtd_meses_dados_pag_{janela}").isNull() |
                (F.col(f"qtd_meses_dados_pag_{janela}") == 0),
                1
            ).otherwise(0)
        )

        # Flag sem atraso na janela
        df_abt_v6 = df_abt_v6.withColumn(
            f"flag_sem_atraso_{janela}",
            F.when(
                F.col(f"qtd_meses_dados_atr_{janela}").isNull() |
                (F.col(f"qtd_meses_dados_atr_{janela}") == 0),
                1
            ).otherwise(0)
        )

    # -------------------------------------------------------------------------
    # METADADOS
    # -------------------------------------------------------------------------
    print(">>> [Meta] Atualizando metadados...")

    df_abt_v6 = df_abt_v6 \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco,cadastro,recarga_v2,pagamento_v2,atraso_v2"))

    return df_abt_v6


def validate_abt_v6(df: DataFrame, count_expected: int) -> bool:
    """
    Validacao para ABT v6 v2.
    """
    print("\n" + "="*80)
    print("VALIDACAO ABT v6 v2")
    print("="*80 + "\n")

    count_atual = df.count()
    errors = []

    # Gate 1: Unicidade
    print(">>> [Gate 1] Unicidade...")
    count_distinct = df.select("num_cpf", "safra").distinct().count()
    if count_distinct != count_atual:
        errors.append(f"Gate 1 FALHOU: duplicatas encontradas")
    else:
        print(f"    PASS: {count_atual:,} registros unicos")

    # Gate 2: Anti-leakage
    print(">>> [Gate 2] Anti-leakage FPD...")
    fpd_em_flag_0 = df.filter(
        (F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())
    ).count()
    if fpd_em_flag_0 > 0:
        errors.append(f"Gate 2 FALHOU: {fpd_em_flag_0} FPD em FLAG=0")
    else:
        print(f"    PASS: FPD correto")

    # Gate 3: Cobertura Score_01
    print(">>> [Gate 3] Cobertura Score_01...")
    score_cov = df.filter(F.col("score_01_adj").isNotNull()).count() * 100 / count_atual
    if score_cov < 90:
        errors.append(f"Gate 3 FALHOU: Score_01 {score_cov:.1f}% < 90%")
    else:
        print(f"    PASS: Score_01 {score_cov:.1f}%")

    # Gate 4: Cobertura Pagamento M1
    print(">>> [Gate 4] Cobertura Pagamento M1...")
    pag_col = "qtd_meses_dados_pag_m1"
    if pag_col in df.columns:
        pag_cov = df.filter(F.col(pag_col) > 0).count() * 100 / count_atual
        print(f"    INFO: Pagamento M1 {pag_cov:.1f}%")
    else:
        print(f"    SKIP: Coluna {pag_col} nao encontrada")

    # Gate 5: Cobertura Atraso M1
    print(">>> [Gate 5] Cobertura Atraso M1...")
    atr_col = "qtd_meses_dados_atr_m1"
    if atr_col in df.columns:
        atr_cov = df.filter(F.col(atr_col) > 0).count() * 100 / count_atual
        print(f"    INFO: Atraso M1 {atr_cov:.1f}%")
    else:
        print(f"    SKIP: Coluna {atr_col} nao encontrada")

    # Resultado
    print("\n" + "="*80)
    if errors:
        print("VALIDACAO COM ERROS")
        for e in errors:
            print(f"   {e}")
        return False
    else:
        print("VALIDACAO OK")
        return True


def gerar_relatorio_final(df: DataFrame, count_v5: int) -> None:
    """Gera relatorio final."""
    print("\n" + "="*80)
    print("RELATORIO FINAL - ABT v6 v2")
    print("="*80 + "\n")

    count_v6 = df.count()

    print(f">>> [Volumetria]")
    print(f"    ABT v5 (input): {count_v5:>12,}")
    print(f"    ABT v6 (output): {count_v6:>12,}")

    print(f"\n>>> [Labels]")
    dist_flag = df.groupBy("flag_instalacao_int").count().collect()
    for row in dist_flag:
        pct = 100 * row["count"] / count_v6
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>10,} ({pct:.2f}%)")

    print(f"\n>>> [Cobertura por Fonte]")

    # Recarga
    if "qtd_recargas_m1" in df.columns:
        cov = df.filter(F.col("qtd_recargas_m1") > 0).count() * 100 / count_v6
        print(f"    Recarga M1: {cov:.2f}%")

    # Pagamento
    if "qtd_meses_dados_pag_m1" in df.columns:
        cov = df.filter(F.col("qtd_meses_dados_pag_m1") > 0).count() * 100 / count_v6
        print(f"    Pagamento M1: {cov:.2f}%")

    # Atraso
    if "qtd_meses_dados_atr_m1" in df.columns:
        cov = df.filter(F.col("qtd_meses_dados_atr_m1") > 0).count() * 100 / count_v6
        print(f"    Atraso M1: {cov:.2f}%")

    print(f"\n>>> [Schema]")
    print(f"    Total colunas: {len(df.columns)}")

    # Contar por tipo
    recarga_cols = len([c for c in df.columns if "_m1" in c and "recargas" in c])
    pag_cols = len([c for c in df.columns if "_pag_" in c])
    atr_cols = len([c for c in df.columns if "_atr_" in c])
    print(f"    Colunas Recarga: ~{recarga_cols * 3}")
    print(f"    Colunas Pagamento: {pag_cols}")
    print(f"    Colunas Atraso: {atr_cols}")


def main():
    parser = argparse.ArgumentParser(description="Construir ABT v6 v2")
    parser.add_argument("--abt_v5_path", default=DEFAULT_GOLD_ABT_V5_PATH)
    parser.add_argument("--pagamento_path", default=DEFAULT_GOLD_PAGAMENTO_PATH)
    parser.add_argument("--atraso_path", default=DEFAULT_GOLD_ATRASO_PATH)
    parser.add_argument("--output_path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--format", default=DEFAULT_FORMAT)
    parser.add_argument("--skip_validate", action="store_true")
    parser.add_argument("--skip_save", action="store_true")

    args, unknown = parser.parse_known_args()
    if unknown:
        print(f">>> [Config] Ignorando argumentos nao reconhecidos: {unknown[:2]}...")

    print("\n")
    print("=" * 78)
    print("ABT V6 BUILDER V2 - OCI Data Flow".center(78))
    print("ABT v5 v2 + Pagamento v2 + Atraso v2".center(78))
    print("=" * 78)
    print("\n")

    spark = SparkSession.builder.appName("abt_v6_original").getOrCreate()

    # =========================================================================
    # LEITURA
    # =========================================================================
    print(f">>> [Leitura] ABT v5: {args.abt_v5_path}")
    try:
        df_abt_v5 = spark.read.format(args.format).load(args.abt_v5_path)
    except Exception as e:
        print(f"!!! ERRO ABT v5: {e}")
        sys.exit(1)
    count_v5 = df_abt_v5.count()
    print(f"    Registros: {count_v5:,}")

    print(f"\n>>> [Leitura] Pagamento Features: {args.pagamento_path}")
    try:
        df_pag = spark.read.format(args.format).load(args.pagamento_path)
    except Exception as e:
        print(f"!!! ERRO Pagamento: {e}")
        sys.exit(1)
    print(f"    Registros: {df_pag.count():,}")

    print(f"\n>>> [Leitura] Atraso Features: {args.atraso_path}")
    try:
        df_atr = spark.read.format(args.format).load(args.atraso_path)
    except Exception as e:
        print(f"!!! ERRO Atraso: {e}")
        sys.exit(1)
    print(f"    Registros: {df_atr.count():,}")

    # =========================================================================
    # BUILD
    # =========================================================================
    df_abt_v6 = build_abt_v6(df_abt_v5, df_pag, df_atr)

    count_v6 = df_abt_v6.count()
    print(f"\n>>> [Info] ABT v6: {count_v6:,} registros, {len(df_abt_v6.columns)} colunas")

    # =========================================================================
    # VALIDACAO
    # =========================================================================
    if not args.skip_validate:
        validate_abt_v6(df_abt_v6, count_v5)

    # =========================================================================
    # RELATORIO
    # =========================================================================
    gerar_relatorio_final(df_abt_v6, count_v5)

    # =========================================================================
    # ESCRITA
    # =========================================================================
    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando: {args.output_path}")
        df_abt_v6.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(args.output_path)

    # =========================================================================
    # RESUMO
    # =========================================================================
    print("\n" + "="*80)
    print("ABT V6 V2 CONSTRUIDO COM SUCESSO!")
    print("="*80)
    print(f"""
    Resumo:
    +-- ABT v5 (input):      {count_v5:>12,} registros
    +-- ABT v6 (output):     {count_v6:>12,} registros
    +-- Colunas totais:      {len(df_abt_v6.columns):>12}
    +-- Output:              {args.output_path}

    Feature Blocks:
    +-- Score_01, Score_02
    +-- Telco (68 vars)
    +-- Cadastro (33 vars)
    +-- Recarga v2 (60+ features, M1/M3/M6)
    +-- Pagamento v2 (50+ features, M1/M3/M6)
    +-- Atraso v2 (60+ features, M1/M3/M6)

    Total: ~250+ features para modelagem
    """)
    print("="*80 + "\n")

    return df_abt_v6


if __name__ == "__main__":
    main()
