"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: gold_atraso_features_v2.py
OBJETIVO: Gerar features comportamentais de Atraso para ABT v6
================================================================================

DESCRIÇÃO TÉCNICA:
Este script processa dados de Atraso da Silver e gera features comportamentais
agregadas por cliente-mês para modelagem de risco de crédito.

A base de Atraso é um SNAPSHOT MENSAL (sempre dia 01) que mostra o estado das
faturas em aberto, aging, write-offs, provisões, etc.

================================================================================
ARQUITETURA:
================================================================================

    SILVER (atraso_silver_delta)
         │ Snapshot mensal: estado das faturas/conta
         │ Grão: múltiplas linhas por CPF (faturas em diferentes estados)
         │
         ▼
    ┌────────────────────────────────────────────────────────────────┐
    │               GOLD ATRASO FEATURES V2 (este script)            │
    │                                                                │
    │  1. Métricas de faturas abertas                               │
    │  2. Distribuição de aging (0-30, 31-60, 61-90, 90+)          │
    │  3. Indicadores de risco (WO, PDD, Fraude)                    │
    │  4. Indicadores de recuperação (ACA, PCCR)                    │
    │  5. Valores em aberto e pagamentos                            │
    │                                                                │
    └────────────────────────────────────────────────────────────────┘
         │
         │ Grão: 1 linha por NUM_CPF + SAFRA_ATRASO
         ▼
    GOLD (atraso_features_v2_delta)

================================================================================
FEATURES GERADAS (60+ variáveis):
================================================================================

1. FATURAS ABERTAS
   - qtd_faturas_abertas_mes: Total de faturas em aberto
   - sum_val_aberto_mes: Valor total em aberto
   - avg_val_aberto_mes: Média por fatura
   - max_val_aberto_mes: Maior fatura em aberto

2. AGING (DISTRIBUIÇÃO DE ATRASO)
   - qtd_aging_0_30_mes: Faturas com 0-30 dias
   - qtd_aging_31_60_mes: Faturas com 31-60 dias
   - qtd_aging_61_90_mes: Faturas com 61-90 dias
   - qtd_aging_90_plus_mes: Faturas com >90 dias
   - pct_aging_90_plus_mes: % em atraso grave (>90 dias)
   - aging_max_dias_mes: Maior aging (dias)

3. INDICADORES DE RISCO
   - flag_teve_wo_mes: Write-off no mês
   - flag_teve_pdd_mes: Provisão para devedores duvidosos
   - flag_teve_fraude_mes: Indicador de fraude
   - qtd_com_wo_mes: Quantidade com WO
   - qtd_com_pdd_mes: Quantidade com PDD

4. INDICADORES DE RECUPERAÇÃO
   - flag_teve_aca_mes: Acordo de pagamento
   - flag_teve_pccr_mes: Programa de recuperação
   - qtd_com_aca_mes: Quantidade com ACA
   - qtd_com_pccr_mes: Quantidade com PCCR

5. VALORES FINANCEIROS
   - sum_val_fat_bruto_mes: Faturamento bruto
   - sum_val_fat_liquido_mes: Faturamento líquido
   - sum_val_pagamento_mes: Pagamentos realizados
   - sum_val_multa_juros_mes: Multas e juros
   - ratio_aberto_faturado_mes: Aberto/Faturado

6. PADRÕES TEMPORAIS
   - qtd_meses_consecutivos_atraso: Meses seguidos em atraso
   - tendencia_valor_aberto: Crescendo ou diminuindo?

7. FLAGS DE COMPORTAMENTO
   - flag_sem_atraso_mes: Sem faturas em aberto
   - flag_atraso_grave_mes: >50% em aging 90+
   - flag_risco_alto_mes: WO ou PDD ou Fraude
   - flag_em_recuperacao_mes: ACA ou PCCR ativo

================================================================================
"""

import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from src.utils.spark_utils import get_spark_session
except ImportError:
    def get_spark_session(app_name="Hackathon_App"):
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
DEFAULT_SILVER_ATRASO_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/atraso_features_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_atraso_features_v2"


def criar_features_atraso(df_silver: DataFrame) -> DataFrame:
    """
    Gera features comportamentais de Atraso agregadas por cliente-mês.
    """
    print("\n" + "="*80)
    print("GOLD ATRASO FEATURES - PIPELINE PRINCIPAL")
    print("="*80 + "\n")

    df = df_silver

    # -------------------------------------------------------------------------
    # PREPARAÇÃO
    # -------------------------------------------------------------------------
    print(">>> [Prep] Preparando dados de Atraso...")

    # Garantir safra_atraso existe
    if "safra_atraso" not in df.columns:
        df = df.withColumn(
            "safra_atraso",
            F.date_format(F.col("ts_referencia"), "yyyyMM")
        )

    # Criar dt_safra_atraso para joins temporais
    df = df.withColumn(
        "dt_safra_atraso",
        F.to_date(F.concat(F.col("safra_atraso"), F.lit("01")), "yyyyMMdd")
    )

    # Garantir valores numéricos
    val_aberto = F.coalesce(F.col("val_fat_aberto"), F.lit(0.0))
    val_bruto = F.coalesce(F.col("val_fat_bruto"), F.lit(0.0))
    val_liquido = F.coalesce(F.col("val_fat_liquido"), F.lit(0.0))
    val_pagamento = F.coalesce(F.col("val_fat_pagamento_bruto"), F.lit(0.0))
    val_multa_juros = F.coalesce(F.col("val_multa_juros"), F.lit(0.0))

    # Indicadores binários
    ind_wo = F.coalesce(F.col("ind_wo"), F.lit(0))
    ind_pdd = F.coalesce(F.col("ind_pdd"), F.lit(0))
    ind_fraude = F.coalesce(F.col("ind_fraude"), F.lit(0))
    ind_aca = F.coalesce(F.col("ind_aca"), F.lit(0))
    ind_pccr = F.coalesce(F.col("ind_pccr"), F.lit(0))

    # Flag de fatura em aberto
    df = df.withColumn(
        "flag_fatura_aberta",
        F.when(val_aberto > 0, 1).otherwise(0)
    )

    # Classificar aging
    df = df.withColumn(
        "aging_bucket",
        F.when(F.col("dw_faixa_aging_fatura") == "0-30 dias", "0_30")
         .when(F.col("dw_faixa_aging_fatura") == "31-60 dias", "31_60")
         .when(F.col("dw_faixa_aging_fatura") == "61-90 dias", "61_90")
         .when(F.col("dw_faixa_aging_fatura") == ">90 dias", "90_plus")
         .otherwise("missing")
    )

    # -------------------------------------------------------------------------
    # AGREGAÇÃO MENSAL
    # -------------------------------------------------------------------------
    print(">>> [Agg] Agregando por NUM_CPF + SAFRA_ATRASO...")

    df_gold = df.groupBy("num_cpf", "safra_atraso", "dt_safra_atraso").agg(

        # === FATURAS ABERTAS ===
        F.count("*").alias("qtd_registros_mes"),
        F.sum("flag_fatura_aberta").alias("qtd_faturas_abertas_mes"),
        F.countDistinct(F.when(val_aberto > 0, F.col("seq_fatura"))).alias("qtd_faturas_distintas_abertas_mes"),

        # === VALORES EM ABERTO ===
        F.sum(val_aberto).alias("sum_val_aberto_mes"),
        F.avg(F.when(val_aberto > 0, val_aberto)).alias("avg_val_aberto_mes"),
        F.max(val_aberto).alias("max_val_aberto_mes"),
        F.min(F.when(val_aberto > 0, val_aberto)).alias("min_val_aberto_mes"),
        F.stddev(F.when(val_aberto > 0, val_aberto)).alias("std_val_aberto_mes"),

        # === FATURAMENTO ===
        F.sum(val_bruto).alias("sum_val_fat_bruto_mes"),
        F.sum(val_liquido).alias("sum_val_fat_liquido_mes"),
        F.sum(val_pagamento).alias("sum_val_pagamento_mes"),
        F.sum(val_multa_juros).alias("sum_val_multa_juros_mes"),

        # === AGING DISTRIBUTION ===
        F.sum(F.when(F.col("aging_bucket") == "0_30", 1).otherwise(0)).alias("qtd_aging_0_30_mes"),
        F.sum(F.when(F.col("aging_bucket") == "31_60", 1).otherwise(0)).alias("qtd_aging_31_60_mes"),
        F.sum(F.when(F.col("aging_bucket") == "61_90", 1).otherwise(0)).alias("qtd_aging_61_90_mes"),
        F.sum(F.when(F.col("aging_bucket") == "90_plus", 1).otherwise(0)).alias("qtd_aging_90_plus_mes"),
        F.sum(F.when(F.col("aging_bucket") == "missing", 1).otherwise(0)).alias("qtd_aging_missing_mes"),

        # Valores por aging
        F.sum(F.when(F.col("aging_bucket") == "0_30", val_aberto).otherwise(0)).alias("sum_val_aging_0_30_mes"),
        F.sum(F.when(F.col("aging_bucket") == "31_60", val_aberto).otherwise(0)).alias("sum_val_aging_31_60_mes"),
        F.sum(F.when(F.col("aging_bucket") == "61_90", val_aberto).otherwise(0)).alias("sum_val_aging_61_90_mes"),
        F.sum(F.when(F.col("aging_bucket") == "90_plus", val_aberto).otherwise(0)).alias("sum_val_aging_90_plus_mes"),

        # === INDICADORES DE RISCO ===
        F.max(ind_wo).alias("flag_teve_wo_mes"),
        F.max(ind_pdd).alias("flag_teve_pdd_mes"),
        F.max(ind_fraude).alias("flag_teve_fraude_mes"),
        F.sum(F.when(ind_wo == 1, 1).otherwise(0)).alias("qtd_com_wo_mes"),
        F.sum(F.when(ind_pdd == 1, 1).otherwise(0)).alias("qtd_com_pdd_mes"),
        F.sum(F.when(ind_fraude == 1, 1).otherwise(0)).alias("qtd_com_fraude_mes"),

        # Valores em WO e PDD
        F.sum(F.when(ind_wo == 1, val_aberto).otherwise(0)).alias("sum_val_wo_mes"),
        F.sum(F.when(ind_pdd == 1, val_aberto).otherwise(0)).alias("sum_val_pdd_mes"),

        # === INDICADORES DE RECUPERAÇÃO ===
        F.max(ind_aca).alias("flag_teve_aca_mes"),
        F.max(ind_pccr).alias("flag_teve_pccr_mes"),
        F.sum(F.when(ind_aca == 1, 1).otherwise(0)).alias("qtd_com_aca_mes"),
        F.sum(F.when(ind_pccr == 1, 1).otherwise(0)).alias("qtd_com_pccr_mes"),

        # === TIPO CLIENTE E PLATAFORMA ===
        F.countDistinct(F.when(F.col("dw_tipo_cliente_conta").isNotNull(), F.col("dw_tipo_cliente_conta")))
            .alias("qtd_tipos_cliente_distintos_mes"),
        F.countDistinct(F.when(F.col("cod_plataforma").isNotNull(), F.col("cod_plataforma")))
            .alias("qtd_plataformas_distintas_mes"),

        # === TEMPO DE BASE ===
        F.countDistinct(F.when(F.col("dw_faixa_tempo_base").isNotNull(), F.col("dw_faixa_tempo_base")))
            .alias("qtd_faixas_tempo_base_mes"),
    )

    # -------------------------------------------------------------------------
    # FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(">>> [Deriv] Criando features derivadas...")

    # Percentual por aging bucket
    df_gold = df_gold.withColumn(
        "pct_aging_0_30_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round((F.col("qtd_aging_0_30_mes") / F.col("qtd_faturas_abertas_mes")) * 100, 2)
        ).otherwise(0.0)
    ).withColumn(
        "pct_aging_31_60_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round((F.col("qtd_aging_31_60_mes") / F.col("qtd_faturas_abertas_mes")) * 100, 2)
        ).otherwise(0.0)
    ).withColumn(
        "pct_aging_61_90_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round((F.col("qtd_aging_61_90_mes") / F.col("qtd_faturas_abertas_mes")) * 100, 2)
        ).otherwise(0.0)
    ).withColumn(
        "pct_aging_90_plus_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round((F.col("qtd_aging_90_plus_mes") / F.col("qtd_faturas_abertas_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual de valor por aging
    df_gold = df_gold.withColumn(
        "pct_val_aging_90_plus_mes",
        F.when(
            F.col("sum_val_aberto_mes") > 0,
            F.round((F.col("sum_val_aging_90_plus_mes") / F.col("sum_val_aberto_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Ticket médio de fatura em aberto
    df_gold = df_gold.withColumn(
        "ticket_medio_aberto_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round(F.col("sum_val_aberto_mes") / F.col("qtd_faturas_abertas_mes"), 2)
        ).otherwise(0.0)
    )

    # Ratio aberto/faturado (quanto do faturado ainda está em aberto)
    df_gold = df_gold.withColumn(
        "ratio_aberto_faturado_mes",
        F.when(
            F.col("sum_val_fat_bruto_mes") > 0,
            F.round(F.col("sum_val_aberto_mes") / F.col("sum_val_fat_bruto_mes"), 4)
        ).otherwise(0.0)
    )

    # Ratio pagamento/faturado (quanto foi pago)
    df_gold = df_gold.withColumn(
        "ratio_pagamento_faturado_mes",
        F.when(
            F.col("sum_val_fat_bruto_mes") > 0,
            F.round(F.col("sum_val_pagamento_mes") / F.col("sum_val_fat_bruto_mes"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variação
    df_gold = df_gold.withColumn(
        "coef_variacao_aberto_mes",
        F.when(
            (F.col("avg_val_aberto_mes").isNotNull()) & (F.col("avg_val_aberto_mes") > 0),
            F.round(F.col("std_val_aberto_mes") / F.col("avg_val_aberto_mes"), 4)
        ).otherwise(None)
    )

    # Percentual WO sobre aberto
    df_gold = df_gold.withColumn(
        "pct_val_wo_sobre_aberto_mes",
        F.when(
            F.col("sum_val_aberto_mes") > 0,
            F.round((F.col("sum_val_wo_mes") / F.col("sum_val_aberto_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # -------------------------------------------------------------------------
    # FLAGS DE COMPORTAMENTO
    # -------------------------------------------------------------------------
    print(">>> [Flags] Criando flags de comportamento...")

    # Sem atraso no mês
    df_gold = df_gold.withColumn(
        "flag_sem_atraso_mes",
        F.when(F.col("qtd_faturas_abertas_mes") == 0, 1).otherwise(0)
    )

    # Atraso grave (>50% em aging 90+)
    df_gold = df_gold.withColumn(
        "flag_atraso_grave_mes",
        F.when(F.col("pct_aging_90_plus_mes") > 50, 1).otherwise(0)
    )

    # Risco alto (WO ou PDD ou Fraude)
    df_gold = df_gold.withColumn(
        "flag_risco_alto_mes",
        F.when(
            (F.col("flag_teve_wo_mes") == 1) |
            (F.col("flag_teve_pdd_mes") == 1) |
            (F.col("flag_teve_fraude_mes") == 1),
            1
        ).otherwise(0)
    )

    # Em recuperação (ACA ou PCCR)
    df_gold = df_gold.withColumn(
        "flag_em_recuperacao_mes",
        F.when(
            (F.col("flag_teve_aca_mes") == 1) |
            (F.col("flag_teve_pccr_mes") == 1),
            1
        ).otherwise(0)
    )

    # Alto valor em aberto (>R$500)
    df_gold = df_gold.withColumn(
        "flag_alto_valor_aberto_mes",
        F.when(F.col("sum_val_aberto_mes") > 500, 1).otherwise(0)
    )

    # Muitas faturas abertas (>3)
    df_gold = df_gold.withColumn(
        "flag_muitas_faturas_abertas_mes",
        F.when(F.col("qtd_faturas_abertas_mes") > 3, 1).otherwise(0)
    )

    # Concentração em aging grave
    df_gold = df_gold.withColumn(
        "flag_concentrado_aging_grave_mes",
        F.when(F.col("pct_val_aging_90_plus_mes") > 70, 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # METADADOS
    # -------------------------------------------------------------------------
    df_gold = df_gold.withColumn("gold_version", F.lit(GOLD_VERSION))
    df_gold = df_gold.withColumn("gold_build_date", F.current_timestamp())

    print(f">>> [Info] Features geradas: {len(df_gold.columns)} colunas")

    return df_gold


def main():
    parser = argparse.ArgumentParser(description="Gerar Gold Atraso Features v2")
    parser.add_argument("--input_path", default=DEFAULT_SILVER_ATRASO_PATH)
    parser.add_argument("--output_path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--format", default=DEFAULT_FORMAT)
    parser.add_argument("--skip_save", action="store_true")

    args, unknown = parser.parse_known_args()
    if unknown:
        print(f">>> [Config] Ignorando argumentos não reconhecidos: {unknown[:2]}...")

    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + "GOLD ATRASO FEATURES V2".center(78) + "║")
    print("╚" + "="*78 + "╝")
    print("\n")

    spark = get_spark_session("Gold_Atraso_Features_V2")

    # Leitura
    print(f">>> [Leitura] Carregando Silver Atraso: {args.input_path}")
    try:
        df_silver = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO: {e}")
        sys.exit(1)

    count_silver = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_silver:,}")

    # Processamento
    df_gold = criar_features_atraso(df_silver)
    count_gold = df_gold.count()
    print(f">>> [Info] Registros Gold: {count_gold:,}")

    # Escrita
    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando: {args.output_path}")
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("safra_atraso") \
            .option("mergeSchema", "true") \
            .save(args.output_path)

        target_table = "hackathon_2025.default.gold_atraso_features_v2"
        try:
            df_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            print(f">>> [Sucesso] Tabela: {target_table}")
        except Exception as e:
            print(f">>> [Aviso] Não foi possível registrar tabela: {e}")

    print("\n" + "="*80)
    print("ATRASO FEATURES V2 CONCLUÍDO!")
    print(f"  Silver: {count_silver:,} → Gold: {count_gold:,}")
    print(f"  Compressão: {count_silver/count_gold:.1f}x")
    print("="*80 + "\n")

    return df_gold


if __name__ == "__main__":
    main()
