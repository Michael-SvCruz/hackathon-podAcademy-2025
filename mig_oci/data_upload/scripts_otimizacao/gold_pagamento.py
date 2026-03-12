# Arquivo: mig_oci/data_upload/scripts/gold_pagamento.py
# Adaptado de src/jobs/02_gold/gold_pagamento_features_v2.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable
"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: gold_pagamento.py (OCI Data Flow)
OBJETIVO: Gerar features comportamentais de Pagamento para ABT v6
================================================================================

DESCRICAO TECNICA:
Este script processa dados de Pagamento da Silver e gera features comportamentais
agregadas por cliente-mes para modelagem de risco de credito.

================================================================================
ARQUITETURA:
================================================================================

    SILVER (pagamento_silver_delta)
         | Transacional: multiplas linhas por fatura/item
         | Grao: fatura + item + pagamento + credito
         |
         v
    +------------------------------------------------------------+
    |           GOLD PAGAMENTO FEATURES V2 (este script)         |
    |                                                            |
    |  1. Classificacao de tipos de pagamento                    |
    |  2. Metricas de valor (pagamentos, descontos, juros)       |
    |  3. Padroes de comportamento (formas de pagamento)         |
    |  4. Indicadores de atraso (juros pagos = atraso passado)   |
    |  5. Agregacao mensal                                       |
    |                                                            |
    +------------------------------------------------------------+
         |
         | Grao: 1 linha por NUM_CPF + SAFRA_PAGAMENTO
         v
    GOLD (pagamento_features_v2_delta)

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
DEFAULT_SILVER_PAGAMENTO_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/pagamento/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/gold_pagamento_features/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_pagamento_features_v2"

# --- Otimizacao small files ---
BYTES_PER_ROW_ESTIMATE = 150   # ~150 bytes/row
TARGET_FILE_SIZE_MB = 128      # Target ~128 MB por arquivo


def criar_features_pagamento(df_silver: DataFrame) -> DataFrame:
    """
    Gera features comportamentais de Pagamento agregadas por cliente-mes.
    """
    print("\n" + "="*80)
    print("GOLD PAGAMENTO FEATURES - PIPELINE PRINCIPAL")
    print("="*80 + "\n")

    df = df_silver

    # -------------------------------------------------------------------------
    # PREPARACAO
    # -------------------------------------------------------------------------
    print(">>> [Prep] Preparando dados de Pagamento...")

    # Garantir safra_pagamento existe
    if "safra_pagamento" not in df.columns:
        df = df.withColumn(
            "safra_pagamento",
            F.date_format(F.col("ts_status_fatura"), "yyyyMM")
        )

    # Criar dt_safra_pagamento para joins temporais
    df = df.withColumn(
        "dt_safra_pagamento",
        F.to_date(F.concat(F.col("safra_pagamento"), F.lit("01")), "yyyyMMdd")
    )

    # Garantir valores numericos
    val_pago = F.coalesce(F.col("val_atual_pagamento"), F.lit(0.0))
    val_desconto = F.coalesce(F.col("val_desconto_item"), F.lit(0.0))
    val_juros_pos = F.coalesce(F.col("val_juros_pos"), F.lit(0.0))
    val_juros_neg = F.coalesce(F.col("val_juros_neg_abs"), F.lit(0.0))

    # Flag de pagamento valido (valor > 0)
    df = df.withColumn(
        "flag_pagamento_valido",
        F.when(val_pago > 0, 1).otherwise(0)
    )

    # Flag com desconto
    df = df.withColumn(
        "flag_com_desconto",
        F.when(val_desconto > 0, 1).otherwise(0)
    )

    # Flag com juros (indicador de atraso passado)
    df = df.withColumn(
        "flag_com_juros",
        F.when(val_juros_pos > 0, 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # AGREGACAO MENSAL
    # -------------------------------------------------------------------------
    print(">>> [Agg] Agregando por NUM_CPF + SAFRA_PAGAMENTO...")

    df_gold = df.groupBy("num_cpf", "safra_pagamento", "dt_safra_pagamento").agg(

        # === VOLUME ===
        F.count("*").alias("qtd_transacoes_mes"),
        F.sum("flag_pagamento_valido").alias("qtd_pagamentos_validos_mes"),
        F.countDistinct("seq_fatura").alias("qtd_faturas_distintas_mes"),
        F.countDistinct("contrato").alias("qtd_contratos_distintos_mes"),

        # === VALORES PAGOS ===
        F.sum(val_pago).alias("sum_val_pago_mes"),
        F.avg(F.when(F.col("flag_pagamento_valido") == 1, val_pago)).alias("avg_val_pago_mes"),
        F.max(val_pago).alias("max_val_pago_mes"),
        F.min(F.when(val_pago > 0, val_pago)).alias("min_val_pago_mes"),
        F.stddev(F.when(F.col("flag_pagamento_valido") == 1, val_pago)).alias("std_val_pago_mes"),

        # === DESCONTOS ===
        F.sum(val_desconto).alias("sum_val_desconto_mes"),
        F.sum("flag_com_desconto").alias("qtd_com_desconto_mes"),
        F.avg(F.when(F.col("flag_com_desconto") == 1, val_desconto)).alias("avg_val_desconto_mes"),
        F.max(val_desconto).alias("max_val_desconto_mes"),

        # === JUROS E MULTAS (indicador de atraso passado) ===
        F.sum(val_juros_pos).alias("sum_val_juros_pos_mes"),
        F.sum(val_juros_neg).alias("sum_val_juros_neg_mes"),
        F.sum("flag_com_juros").alias("qtd_com_juros_mes"),
        F.avg(F.when(F.col("flag_com_juros") == 1, val_juros_pos)).alias("avg_val_juros_mes"),
        F.max(val_juros_pos).alias("max_val_juros_mes"),

        # === MULTAS DE EQUIPAMENTO/FIDELIDADE ===
        F.sum(F.coalesce(F.col("val_multa_equip_item"), F.lit(0.0))).alias("sum_val_multa_equip_mes"),
        F.sum(F.coalesce(F.col("val_multa_fid_item"), F.lit(0.0))).alias("sum_val_multa_fid_mes"),

        # === FORMAS DE PAGAMENTO ===
        F.countDistinct(F.when(F.col("cod_forma_pagamento").isNotNull(), F.col("cod_forma_pagamento")))
            .alias("qtd_formas_pagamento_distintas_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "01", val_pago).otherwise(0)).alias("sum_pago_forma_01_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "02", val_pago).otherwise(0)).alias("sum_pago_forma_02_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento") == "03", val_pago).otherwise(0)).alias("sum_pago_forma_03_mes"),
        F.sum(F.when(F.col("cod_forma_pagamento").isNull(), val_pago).otherwise(0)).alias("sum_pago_forma_missing_mes"),

        # === METODOS DE PAGAMENTO ===
        F.countDistinct(F.when(F.col("cod_metodo_pagamento").isNotNull(), F.col("cod_metodo_pagamento")))
            .alias("qtd_metodos_pagamento_distintos_mes"),

        # === STATUS DE PAGAMENTO ===
        F.sum(F.when(F.col("ind_status_pagamento") == "P", 1).otherwise(0)).alias("qtd_status_p_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "R", 1).otherwise(0)).alias("qtd_status_r_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "C", 1).otherwise(0)).alias("qtd_status_c_mes"),
        F.sum(F.when(F.col("ind_status_pagamento") == "B", 1).otherwise(0)).alias("qtd_status_b_mes"),

        # === MISSING ===
        F.sum(F.when(F.col("flag_ts_status_pagamento_missing") == 1, 1).otherwise(0)).alias("qtd_status_pag_missing_mes"),
    )

    # -------------------------------------------------------------------------
    # FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(">>> [Deriv] Criando features derivadas...")

    # Ticket medio
    df_gold = df_gold.withColumn(
        "ticket_medio_pagamento_mes",
        F.when(
            F.col("qtd_pagamentos_validos_mes") > 0,
            F.round(F.col("sum_val_pago_mes") / F.col("qtd_pagamentos_validos_mes"), 2)
        ).otherwise(0.0)
    )

    # Percentual com desconto
    df_gold = df_gold.withColumn(
        "pct_pagamentos_com_desconto_mes",
        F.when(
            F.col("qtd_transacoes_mes") > 0,
            F.round((F.col("qtd_com_desconto_mes") / F.col("qtd_transacoes_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Ratio desconto/pago
    df_gold = df_gold.withColumn(
        "ratio_desconto_pago_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round(F.col("sum_val_desconto_mes") / F.col("sum_val_pago_mes"), 4)
        ).otherwise(0.0)
    )

    # Percentual com juros (indicador de atraso)
    df_gold = df_gold.withColumn(
        "pct_pagamentos_com_juros_mes",
        F.when(
            F.col("qtd_transacoes_mes") > 0,
            F.round((F.col("qtd_com_juros_mes") / F.col("qtd_transacoes_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Ratio juros/pago
    df_gold = df_gold.withColumn(
        "ratio_juros_pago_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round(F.col("sum_val_juros_pos_mes") / F.col("sum_val_pago_mes"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variacao
    df_gold = df_gold.withColumn(
        "coef_variacao_pagamento_mes",
        F.when(
            (F.col("avg_val_pago_mes").isNotNull()) & (F.col("avg_val_pago_mes") > 0),
            F.round(F.col("std_val_pago_mes") / F.col("avg_val_pago_mes"), 4)
        ).otherwise(None)
    )

    # Valor liquido (pago - desconto)
    df_gold = df_gold.withColumn(
        "val_liquido_pago_mes",
        F.col("sum_val_pago_mes") - F.col("sum_val_desconto_mes")
    )

    # Concentracao na forma de pagamento dominante
    df_gold = df_gold.withColumn(
        "max_forma_pagamento_mes",
        F.greatest(
            F.col("sum_pago_forma_01_mes"),
            F.col("sum_pago_forma_02_mes"),
            F.col("sum_pago_forma_03_mes"),
            F.col("sum_pago_forma_missing_mes")
        )
    ).withColumn(
        "pct_forma_dominante_mes",
        F.when(
            F.col("sum_val_pago_mes") > 0,
            F.round((F.col("max_forma_pagamento_mes") / F.col("sum_val_pago_mes")) * 100, 2)
        ).otherwise(0.0)
    ).drop("max_forma_pagamento_mes")

    # -------------------------------------------------------------------------
    # FLAGS DE COMPORTAMENTO
    # -------------------------------------------------------------------------
    print(">>> [Flags] Criando flags de comportamento...")

    # Sem pagamento no mes
    df_gold = df_gold.withColumn(
        "flag_sem_pagamento_mes",
        F.when(F.col("qtd_pagamentos_validos_mes") == 0, 1).otherwise(0)
    )

    # Sempre com juros (>80% dos pagamentos)
    df_gold = df_gold.withColumn(
        "flag_sempre_com_juros_mes",
        F.when(F.col("pct_pagamentos_com_juros_mes") > 80, 1).otherwise(0)
    )

    # Alto desconto (>10%)
    df_gold = df_gold.withColumn(
        "flag_alto_desconto_mes",
        F.when(F.col("ratio_desconto_pago_mes") > 0.10, 1).otherwise(0)
    )

    # Baixo volume de pagamentos (<2)
    df_gold = df_gold.withColumn(
        "flag_baixo_volume_pagamento_mes",
        F.when(F.col("qtd_pagamentos_validos_mes") < 2, 1).otherwise(0)
    )

    # Alta multa (multa > 5% do valor pago)
    df_gold = df_gold.withColumn(
        "flag_alta_multa_mes",
        F.when(
            (F.col("sum_val_pago_mes") > 0) &
            ((F.col("sum_val_multa_equip_mes") + F.col("sum_val_multa_fid_mes")) / F.col("sum_val_pago_mes") > 0.05),
            1
        ).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # METADADOS
    # -------------------------------------------------------------------------
    df_gold = df_gold.withColumn("gold_version", F.lit(GOLD_VERSION))
    df_gold = df_gold.withColumn("gold_build_date", F.current_timestamp())

    print(f">>> [Info] Features geradas: {len(df_gold.columns)} colunas")

    return df_gold


def main():
    parser = argparse.ArgumentParser(description="Gerar Gold Pagamento Features v2")
    parser.add_argument("--input_path", default=DEFAULT_SILVER_PAGAMENTO_PATH)
    parser.add_argument("--output_path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--format", default=DEFAULT_FORMAT)
    parser.add_argument("--skip_save", action="store_true")

    args, unknown = parser.parse_known_args()
    if unknown:
        print(f">>> [Config] Ignorando argumentos nao reconhecidos: {unknown[:2]}...")

    print("\n")
    print("=" * 78)
    print("GOLD PAGAMENTO FEATURES V2 - OCI Data Flow".center(78))
    print("=" * 78)
    print("\n")

    spark = SparkSession.builder.appName("gold_pagamento_optz_sf").getOrCreate()

    # =========================================================================
    # 1) LEITURA SILVER
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Pagamento: {args.input_path}")
    try:
        df_silver = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) PROCESSAMENTO — ACTION 1 (groupBy + write)
    # =========================================================================
    df_gold = criar_features_pagamento(df_silver)

    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando: {args.output_path}")

        # --- Coalesce dinamico (otimizacao small files) ---
        count_gold = df_gold.count()
        estimated_size_mb = count_gold * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
        num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
        print(f">>> [Otimizacao] repartition({num_output_files}, safra_pagamento) — ~{TARGET_FILE_SIZE_MB}MB/arquivo")
        print(f">>> [Otimizacao] {count_gold:,} registros, ~{estimated_size_mb:.0f} MB estimados")

        df_gold.repartition(num_output_files, "safra_pagamento") \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("safra_pagamento") \
            .option("mergeSchema", "true") \
            .save(args.output_path)
        print(">>> [Escrita] Gold gravada com sucesso.")

        # =====================================================================
        # 3) QUALITY CHECK na Gold JA ESCRITA — ACTION 2
        # =====================================================================
        print(f"\n>>> [Quality] Lendo Gold gravada para validacao: {args.output_path}")
        df_written = spark.read.format("delta").load(args.output_path)

        quality = df_written.agg(
            F.count("*").alias("total"),
            F.countDistinct("num_cpf", "safra_pagamento").alias("distinct_keys"),
            F.countDistinct("num_cpf").alias("cpfs_distintos"),
            F.countDistinct("safra_pagamento").alias("safras_distintas"),
            F.sum(F.when(F.col("num_cpf").isNull(), 1).otherwise(0)).alias("nulos_cpf"),
            F.sum(F.when(F.col("flag_sem_pagamento_mes") == 1, 1).otherwise(0)).alias("sem_pagamento"),
            F.sum(F.when(F.col("flag_sempre_com_juros_mes") == 1, 1).otherwise(0)).alias("sempre_com_juros"),
            F.sum(F.when(F.col("flag_alto_desconto_mes") == 1, 1).otherwise(0)).alias("alto_desconto"),
        ).collect()[0]

        count_gold    = quality["total"]
        distinct_keys = quality["distinct_keys"]
        cpfs          = quality["cpfs_distintos"]
        safras        = quality["safras_distintas"]
        nulos_cpf     = quality["nulos_cpf"]
        sem_pag       = quality["sem_pagamento"]
        com_juros     = quality["sempre_com_juros"]
        alto_desc     = quality["alto_desconto"]

        print("\n" + "="*80)
        print(">>> [Quality] RELATORIO DE QUALIDADE - GOLD PAGAMENTO")
        print("="*80)
        print(f"\n    Registros Gold (1 linha/CPF+SAFRA):      {count_gold:>12,}")
        print(f"    Chaves distintas (num_cpf+safra):        {distinct_keys:>12,}  [esperado = total]")
        print(f"    Unicidade OK:                            {'SIM' if count_gold == distinct_keys else 'NAO — VERIFICAR'}")
        print(f"\n    CPFs distintos:                          {cpfs:>12,}")
        print(f"    Safras distintas:                        {safras:>12,}")
        print(f"\n    Gate 1 - NUM_CPF nulos:                  {nulos_cpf:>12,}  [esperado = 0]")
        print(f"\n    Flag sem pagamento no mes:               {sem_pag:>12,}  ({100*sem_pag/count_gold:.2f}%)")
        print(f"    Flag sempre com juros (>80%):            {com_juros:>12,}  ({100*com_juros/count_gold:.2f}%)")
        print(f"    Flag alto desconto (>10%):               {alto_desc:>12,}  ({100*alto_desc/count_gold:.2f}%)")

        print("\n" + "="*80)
        print("Gold PAGAMENTO concluido")
        print(f"  - Registros:          {count_gold:,}")
        print(f"  - Compressao:         Silver (N linhas/CPF) → Gold (1 linha/CPF+SAFRA)")
        print(f"  - Actions:            2 (groupBy+write + agg Gold escrita)")
        print(f"  - Particionamento:    safra_pagamento")
        print(f"  - Proximo passo:      abt_v6_builder.py")
        print("="*80 + "\n")

    return df_gold


if __name__ == "__main__":
    main()
