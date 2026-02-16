"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: gold_pagamento_features_v2.py
OBJETIVO: Gerar features comportamentais de Pagamento para ABT v6
================================================================================

DESCRIÇÃO TÉCNICA:
Este script processa dados de Pagamento da Silver e gera features comportamentais
agregadas por cliente-mês para modelagem de risco de crédito.

================================================================================
ARQUITETURA:
================================================================================

    SILVER (pagamento_silver_delta)
         │ Transacional: múltiplas linhas por fatura/item
         │ Grão: fatura + item + pagamento + crédito
         │
         ▼
    ┌────────────────────────────────────────────────────────────────┐
    │             GOLD PAGAMENTO FEATURES V2 (este script)           │
    │                                                                │
    │  1. Classificação de tipos de pagamento                       │
    │  2. Métricas de valor (pagamentos, descontos, juros)          │
    │  3. Padrões de comportamento (formas de pagamento)            │
    │  4. Indicadores de atraso (juros pagos = atraso passado)      │
    │  5. Agregação mensal                                          │
    │                                                                │
    └────────────────────────────────────────────────────────────────┘
         │
         │ Grão: 1 linha por NUM_CPF + SAFRA_PAGAMENTO
         ▼
    GOLD (pagamento_features_v2_delta)

================================================================================
FEATURES GERADAS (50+ variáveis):
================================================================================

1. VOLUME DE PAGAMENTOS
   - qtd_pagamentos_mes: Total de transações
   - qtd_faturas_distintas_mes: Faturas distintas pagas
   - qtd_contratos_distintos_mes: Contratos distintos

2. VALORES PAGOS
   - sum_val_pago_mes: Soma total pago
   - avg_val_pago_mes: Ticket médio de pagamento
   - max_val_pago_mes: Maior pagamento
   - min_val_pago_mes: Menor pagamento
   - std_val_pago_mes: Desvio padrão

3. DESCONTOS (comportamento de negociação)
   - sum_val_desconto_mes: Total de descontos obtidos
   - qtd_com_desconto_mes: Quantidade com desconto
   - pct_pagamentos_com_desconto_mes: % com desconto
   - ratio_desconto_pago_mes: Desconto/Valor pago

4. JUROS E MULTAS (indicador de atrasos passados)
   - sum_val_juros_pos_mes: Juros/multas pagos (atraso anterior)
   - qtd_com_juros_mes: Quantidade com juros
   - pct_pagamentos_com_juros_mes: % com juros
   - ratio_juros_pago_mes: Juros/Valor pago

5. FORMAS DE PAGAMENTO (diversidade)
   - qtd_formas_pagamento_distintas_mes: Diversidade
   - sum_pago_forma_01_mes, ..._02, ..._03: Por forma
   - pct_forma_dominante_mes: Concentração

6. MÉTODOS DE PAGAMENTO
   - qtd_metodos_pagamento_distintos_mes
   - pct_metodo_dominante_mes

7. PADRÕES TEMPORAIS
   - dias_medio_entre_pagamentos_mes
   - regularidade_pagamento_mes

8. FLAGS
   - flag_sem_pagamento_mes: Cliente sem pagamento
   - flag_sempre_com_juros_mes: Sempre paga com atraso
   - flag_alto_desconto_mes: Desconto > 10%

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
DEFAULT_SILVER_PAGAMENTO_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/pagamento_features_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_pagamento_features_v2"


def criar_features_pagamento(df_silver: DataFrame) -> DataFrame:
    """
    Gera features comportamentais de Pagamento agregadas por cliente-mês.
    """
    print("\n" + "="*80)
    print("GOLD PAGAMENTO FEATURES - PIPELINE PRINCIPAL")
    print("="*80 + "\n")

    df = df_silver

    # -------------------------------------------------------------------------
    # PREPARAÇÃO
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

    # Garantir valores numéricos
    val_pago = F.coalesce(F.col("val_atual_pagamento"), F.lit(0.0))
    val_desconto = F.coalesce(F.col("val_desconto_item"), F.lit(0.0))
    val_juros_pos = F.coalesce(F.col("val_juros_pos"), F.lit(0.0))
    val_juros_neg = F.coalesce(F.col("val_juros_neg_abs"), F.lit(0.0))

    # Flag de pagamento válido (valor > 0)
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
    # AGREGAÇÃO MENSAL
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

        # === MÉTODOS DE PAGAMENTO ===
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

    # Ticket médio
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

    # Coeficiente de variação
    df_gold = df_gold.withColumn(
        "coef_variacao_pagamento_mes",
        F.when(
            (F.col("avg_val_pago_mes").isNotNull()) & (F.col("avg_val_pago_mes") > 0),
            F.round(F.col("std_val_pago_mes") / F.col("avg_val_pago_mes"), 4)
        ).otherwise(None)
    )

    # Valor líquido (pago - desconto)
    df_gold = df_gold.withColumn(
        "val_liquido_pago_mes",
        F.col("sum_val_pago_mes") - F.col("sum_val_desconto_mes")
    )

    # Concentração na forma de pagamento dominante
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

    # Sem pagamento no mês
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
        print(f">>> [Config] Ignorando argumentos não reconhecidos: {unknown[:2]}...")

    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + "GOLD PAGAMENTO FEATURES V2".center(78) + "║")
    print("╚" + "="*78 + "╝")
    print("\n")

    spark = get_spark_session("Gold_Pagamento_Features_V2")

    # Leitura
    print(f">>> [Leitura] Carregando Silver Pagamento: {args.input_path}")
    try:
        df_silver = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO: {e}")
        sys.exit(1)

    count_silver = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_silver:,}")

    # Processamento
    df_gold = criar_features_pagamento(df_silver)
    count_gold = df_gold.count()
    print(f">>> [Info] Registros Gold: {count_gold:,}")

    # Escrita
    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando: {args.output_path}")
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("safra_pagamento") \
            .option("mergeSchema", "true") \
            .save(args.output_path)

        target_table = "hackathon_2025.default.gold_pagamento_features_v2"
        try:
            df_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            print(f">>> [Sucesso] Tabela: {target_table}")
        except Exception as e:
            print(f">>> [Aviso] Não foi possível registrar tabela: {e}")

    print("\n" + "="*80)
    print("PAGAMENTO FEATURES V2 CONCLUÍDO!")
    print(f"  Silver: {count_silver:,} → Gold: {count_gold:,}")
    print(f"  Compressão: {count_silver/count_gold:.1f}x")
    print("="*80 + "\n")

    return df_gold


if __name__ == "__main__":
    main()
