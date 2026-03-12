# Arquivo: mig_oci/data_upload/scripts/abt_v6_builder.py
# Adaptado de src/jobs/02_gold/05_gold_abt_v6_builder_v2.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable
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
DEFAULT_GOLD_ABT_V5_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v5/"
DEFAULT_GOLD_PAGAMENTO_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/gold_pagamento_features/"
DEFAULT_GOLD_ATRASO_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/gold_atraso_features/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v6_v2/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v6_v2"

TEMPORAL_WINDOWS = {"m1": 1, "m3": 3, "m6": 6}

# --- Otimizacao small files ---
BYTES_PER_ROW_ESTIMATE = 400   # ~400 bytes/row (261 features, ~614 colunas)
TARGET_FILE_SIZE_MB = 64       # Target ~64 MB por arquivo


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

    spark = SparkSession.builder.appName("abt_v6_optz_sf").getOrCreate()

    # =========================================================================
    # LEITURA
    # =========================================================================
    print(f">>> [Leitura] ABT v5: {args.abt_v5_path}")
    try:
        df_abt_v5 = spark.read.format(args.format).load(args.abt_v5_path)
    except Exception as e:
        print(f"!!! ERRO ABT v5: {e}")
        sys.exit(1)

    print(f">>> [Leitura] Pagamento Features: {args.pagamento_path}")
    try:
        df_pag = spark.read.format(args.format).load(args.pagamento_path)
    except Exception as e:
        print(f"!!! ERRO Pagamento: {e}")
        sys.exit(1)

    print(f">>> [Leitura] Atraso Features: {args.atraso_path}")
    try:
        df_atr = spark.read.format(args.format).load(args.atraso_path)
    except Exception as e:
        print(f"!!! ERRO Atraso: {e}")
        sys.exit(1)

    # =========================================================================
    # BUILD ABT V6 + ESCRITA — ACTION 1
    # =========================================================================
    df_abt_v6 = build_abt_v6(df_abt_v5, df_pag, df_atr)

    if not args.skip_save:
        print(f">>> [Escrita] Salvando ABT v6 (Delta): {args.output_path}")

        # --- Coalesce dinamico (otimizacao small files) ---
        count_abt = df_abt_v6.count()
        estimated_size_mb = count_abt * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
        num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
        print(f">>> [Otimizacao] coalesce({num_output_files}) — ~{TARGET_FILE_SIZE_MB}MB/arquivo")
        print(f">>> [Otimizacao] {count_abt:,} registros, ~{estimated_size_mb:.0f} MB estimados")

        df_abt_v6.coalesce(num_output_files) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(args.output_path)
        print(">>> [Escrita] ABT v6 gravada com sucesso.")

        # -----------------------------------------------------------------
        # VACUUM — Remove parquets órfãos de execuções anteriores
        # -----------------------------------------------------------------
        # O Delta .mode("overwrite") substitui logicamente os dados no _delta_log,
        # mas NÃO deleta fisicamente os arquivos .parquet antigos do Object Storage.
        # O script do modelo (pandas) lê via list_objects (todos os .parquet),
        # ignorando o _delta_log → carrega dados duplicados → OOM.
        # O VACUUM remove fisicamente os arquivos que não pertencem à versão atual.
        print(">>> [Vacuum] Removendo parquets orfaos de execucoes anteriores...")
        try:
            from delta.tables import DeltaTable
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            delta_table = DeltaTable.forPath(spark, args.output_path)
            delta_table.vacuum(retentionHours=0)
            print(">>> [Vacuum] Concluido — apenas arquivos da versao atual permanecem.")
        except Exception as e:
            # Vacuum é best-effort: se falhar, o dedup incremental no modelo cobre
            print(f">>> [Vacuum] AVISO: falhou ({e}). Dedup incremental no modelo sera usado.")
    else:
        print(">>> [Skip] Salvamento pulado (--skip_save)")
        return df_abt_v6

    # =========================================================================
    # QUALITY CHECK na ABT JA ESCRITA — ACTION 2
    # =========================================================================
    print(f"\n>>> [Quality] Lendo ABT v6 gravada para validacao: {args.output_path}")
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
        # Cobertura scores
        F.sum(F.when(F.col("score_01_adj").isNull(), 1).otherwise(0)).alias("score01_null"),
        F.sum(F.when(F.col("score_02_adj").isNull(), 1).otherwise(0)).alias("score02_null"),
        # Cobertura blocos de features (match = tem dado na janela)
        F.sum(F.when(F.col("var_26_adj").isNotNull(), 1).otherwise(0)).alias("telco_match"),
        F.sum(F.when(F.col("qtd_recargas_m1") > 0, 1).otherwise(0)).alias("recarga_m1"),
        F.sum(F.when(F.col("qtd_meses_dados_pag_m1") > 0, 1).otherwise(0)).alias("pagamento_m1"),
        F.sum(F.when(F.col("qtd_meses_dados_atr_m1") > 0, 1).otherwise(0)).alias("atraso_m1"),
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
    recarga_m1      = quality["recarga_m1"]
    pagamento_m1    = quality["pagamento_m1"]
    atraso_m1       = quality["atraso_m1"]

    score01_cov     = 100 * (count_out - score01_null) / count_out if count_out > 0 else 0
    score02_cov     = 100 * (count_out - score02_null) / count_out if count_out > 0 else 0
    telco_pct       = 100 * telco_match / count_out if count_out > 0 else 0
    rec_m1_pct      = 100 * recarga_m1 / count_out if count_out > 0 else 0
    pag_m1_pct      = 100 * pagamento_m1 / count_out if count_out > 0 else 0
    atr_m1_pct      = 100 * atraso_m1 / count_out if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - ABT v6 (FINAL)")
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
    print(f"    Gate 6 - Telco match:                   {telco_match:>12,}  ({telco_pct:.2f}%)  [esperado ~35.46%]")
    print(f"    Gate 7 - Recarga M1 cobertura:          {recarga_m1:>12,}  ({rec_m1_pct:.2f}%)  [esperado ~56.12%]")
    print(f"    Gate 8 - Pagamento M1 cobertura:        {pagamento_m1:>12,}  ({pag_m1_pct:.2f}%)  [esperado ~16.13%]")
    print(f"    Gate 9 - Atraso M1 cobertura:           {atraso_m1:>12,}  ({atr_m1_pct:.2f}%)  [esperado ~21.79%]")

    print("\n" + "="*80)
    print(f"ABT v6 PRONTA PARA MODELAGEM (VERSAO FINAL)")
    print(f"  - Versao:          {GOLD_VERSION}")
    print(f"  - Feature blocks:  Score_01, Score_02, Telco, Cadastro, Recarga, Pagamento, Atraso")
    print(f"  - Registros:       {count_out:,}")
    print(f"  - Actions:         2 (join+write + agg ABT escrita)")
    print(f"  - Grao:            1:1 NUM_CPF + SAFRA")
    print(f"  - Target:          FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Total colunas:   {len(df_written.columns)}")
    print("="*80 + "\n")

    return df_abt_v6


if __name__ == "__main__":
    main()
