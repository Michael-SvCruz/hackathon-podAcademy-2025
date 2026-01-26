"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 06_gold_abt_v6_1_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v6.1 com features enhancement
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script estende ABT v6 com 3 features críticas para melhoria de performance:
  1. Desconto Rate (Pagamento): proporção de descontos no total pago
  2. Delinquency Rate (Atraso): % de faturas abertas vs total
  3. Max Days in Arrears (Atraso): idade máxima da dívida

ROADMAP INCREMENTAL:
... (v1-v6 como antes) ...
7. ⏳ ABT v6.1: ABT_v6 + 3 Features Enhancement (este script)   ← v6.1
   - DESCONTO_RATE_M1/M3/M6 (Pagamento)
   - DELINQUENCY_RATE_M1/M3/M6 (Atraso)
   - MAX_DIAS_ATRASO_M1/M3/M6 (Atraso)
   - Total: 72 + 9 = 81 features

IMPACTO ESPERADO:
- KS esperado: 45-46% (vs v6: 44-45%, delta: +1-1.5pp)
- Grão mantido: 1:1 NUM_CPF + SAFRA
- Anti-leakage: respeitado (todas são histórico)
- Validation gates: 14 + novos checks para ratios

NOVO EM V6.1:
- Desconto Rate (3 features): relação desconto / (desconto + pago)
  → Intuição: cliente negociador agressivo = mais risco
  
- Delinquency Rate (3 features): faturas abertas / total de faturas
  → Intuição: cliente com alta % de inadimplência = muito risco
  
- Max Days Arrears (3 features): dias entre referência e vencimento mais antigo
  → Intuição: dívida muito antiga = indicador de insolvência

ESTRUTURA DE DADOS (v6.1):
- Entrada: ABT v6 Gold (3.795.310 registros, 250+ colunas)
- Entrada adicional: Silver Pagamento + Silver Atraso (para recálculos)
- Saída: ABT v6.1 (3.795.310 registros, 259+ colunas = v6 + 9 novas)

VALIDAÇÕES OBRIGATÓRIAS (14 + 3):
- Gates 1-14: herdadas de v6 (unicidade, FPD, coberturas, etc)
- Gate 15: DESCONTO_RATE_M1 between 0-1 (ou NULL)
- Gate 16: DELINQUENCY_RATE_M1 between 0-1 (ou NULL)
- Gate 17: MAX_DIAS_ATRASO_M1 >= 0 (ou NULL/0)
--------------------------------------------------------------------------------
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v6_1

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_GOLD_ABT_V6_PATH = "/Volumes/hackathon_2025/default/gold/abt_v6_delta/"
DEFAULT_SILVER_PAGAMENTO_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_SILVER_ATRASO_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v6_1_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v6_1"
# =============================================================================

def aggregate_pagamento_with_ratios(df_pagamento, df_spine):
    """
    Agrega Pagamento por (NUM_CPF, SAFRA) com M1/M3/M6.
    Adiciona: DESCONTO_RATE = sum_desconto / (sum_desconto + sum_pago)
    
    Retorna: DataFrame com grão (NUM_CPF, SAFRA) e features incluindo ratios
    """
    print(">>> [Pagamento Enhanced] Agregando com ratios...")
    
    spine_keys = df_spine.select("num_cpf", "safra").distinct()
    
    df_pagamento = df_pagamento.withColumn(
        "safra_pag_calc",
        F.date_format(F.col("ts_status_fatura"), "yyyyMM").cast(IntegerType())
    )
    
    df_pagamento_with_safra = df_pagamento.join(spine_keys, on="num_cpf", how="inner")
    
    agg_list = []
    
    for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
        
        df_period = df_pagamento_with_safra.withColumn(
            "safra_lower_bound",
            F.when(F.col("safra") > (100 * months_back),
                   F.col("safra") - (100 * months_back)
            ).otherwise(F.lit(0))
        ).filter(
            (F.col("safra_pag_calc") >= F.col("safra_lower_bound")) &
            (F.col("safra_pag_calc") <= F.col("safra"))
        )
        
        # Agregações base (do v6)
        agg = df_period.groupBy("num_cpf", "safra").agg(
            F.count(F.lit(1)).alias(f"qtd_itens_pagamento_{period_name}"),
            F.sum(F.col("val_atual_pagamento")).alias(f"sum_val_pago_{period_name}"),
            F.sum(F.col("val_desconto_item")).alias(f"sum_val_desconto_{period_name}"),
            F.sum(F.col("val_juros_pos")).alias(f"sum_val_juros_pos_{period_name}"),
            F.sum(F.col("val_juros_neg_abs")).alias(f"sum_val_juros_neg_abs_{period_name}"),
            F.avg(F.col("val_atual_pagamento")).alias(f"avg_val_pago_{period_name}"),
            F.max(F.col("val_atual_pagamento")).alias(f"max_val_pago_{period_name}"),
            F.max(F.when(F.col("val_desconto_item") > 0, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"flag_teve_desconto_{period_name}"),
            F.sum(F.when(F.col("cod_forma_pagamento") == "01", F.col("val_atual_pagamento")))
             .alias(f"sum_pago_forma_01_{period_name}"),
            F.sum(F.when(F.col("cod_forma_pagamento") == "02", F.col("val_atual_pagamento")))
             .alias(f"sum_pago_forma_02_{period_name}"),
            F.sum(F.when(F.col("cod_forma_pagamento") == "03", F.col("val_atual_pagamento")))
             .alias(f"sum_pago_forma_03_{period_name}"),
            F.sum(F.when(F.col("cod_forma_pagamento").isNull(), F.col("val_atual_pagamento")))
             .alias(f"sum_pago_forma_missing_{period_name}"),
            F.sum(F.when(F.col("flag_ts_status_pagamento_missing") == 1, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"_missing_count_{period_name}"),
            F.count(F.lit(1)).alias(f"_total_count_{period_name}")
        )
        
        agg = agg.withColumn(
            f"flag_missing_ts_status_pagamento_{period_name}",
            F.when(
                (F.col(f"_missing_count_{period_name}") / F.col(f"_total_count_{period_name}")) > 0.5,
                F.lit(1)
            ).otherwise(F.lit(0))
        ).drop(f"_missing_count_{period_name}", f"_total_count_{period_name}")
        
        # ====== NOVO EM v6.1: DESCONTO RATE ======
        # Desconto Rate = SUM(desconto) / (SUM(desconto) + SUM(pago))
        # Lógica: quando denominador = 0, NULL (não pode calcular)
        agg = agg.withColumn(
            f"desconto_rate_{period_name}",
            F.when(
                (F.col(f"sum_val_desconto_{period_name}") + F.col(f"sum_val_pago_{period_name}")) > 0,
                F.col(f"sum_val_desconto_{period_name}") / 
                (F.col(f"sum_val_desconto_{period_name}") + F.col(f"sum_val_pago_{period_name}"))
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        
        agg_list.append(agg)
    
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    result = spine_keys.join(result, on=["num_cpf", "safra"], how="left")
    
    print(f"    ✓ Agregação Pagamento Enhanced: {result.count():,} registros")
    return result

def aggregate_atraso_with_enhancements(df_atraso, df_spine):
    """
    Agrega Atraso por (NUM_CPF, SAFRA_ATRASO) com M1/M3/M6.
    Adiciona:
      1. DELINQUENCY_RATE = qtd_faturas_abertas / qtd_total_faturas
      2. MAX_DIAS_ATRASO = MAX(data_referencia - data_vencimento) para abertas
    
    Retorna: DataFrame com grão (NUM_CPF, SAFRA) e features com enhancements
    """
    print(">>> [Atraso Enhanced] Agregando com ratios e aging...")
    
    spine_keys = df_spine.select("num_cpf", "safra").distinct()
    
    if "safra_atraso" not in df_atraso.columns:
        df_atraso = df_atraso.withColumn(
            "safra_atraso",
            F.date_format(F.col("ts_referencia"), "yyyyMM").cast(IntegerType())
        )
    
    df_atraso_with_safra = df_atraso.join(spine_keys, on="num_cpf", how="inner")
    
    agg_list = []
    
    for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
        
        df_period = df_atraso_with_safra.withColumn(
            "safra_lower_bound",
            F.when(F.col("safra") > (100 * months_back),
                   F.col("safra") - (100 * months_back)
            ).otherwise(F.lit(0))
        ).filter(
            (F.col("safra_atraso") >= F.col("safra_lower_bound")) &
            (F.col("safra_atraso") <= F.col("safra"))
        )
        
        # Agregações base (do v6)
        agg = df_period.groupBy("num_cpf", "safra").agg(
            F.countDistinct(F.when(F.col("val_fat_aberto") > 0, F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_abertas_{period_name}"),
            F.sum(F.col("val_fat_aberto")).alias(f"sum_val_aberto_{period_name}"),
            F.avg(F.col("val_fat_aberto")).alias(f"avg_val_aberto_{period_name}"),
            F.max(F.col("val_fat_aberto")).alias(f"max_val_aberto_{period_name}"),
            F.sum(F.col("val_fat_pagamento_bruto")).alias(f"sum_val_pagamento_{period_name}"),
            F.max(F.col("ind_wo")).alias(f"flag_teve_wo_{period_name}"),
            F.max(F.col("ind_pdd")).alias(f"flag_teve_pdd_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "0-30 dias", F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_aging_0_30_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "31-60 dias", F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_aging_31_60_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "61-90 dias", F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_aging_61_90_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == ">90 dias", F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_aging_90_plus_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura").isNull(), F.col("num_fatura_hash")))
             .alias(f"qtd_faturas_aging_missing_{period_name}"),
            # ====== NOVO EM v6.1: contagem total de faturas (DISTINCT) ======
            F.countDistinct(F.col("num_fatura_hash"))
             .alias(f"_total_faturas_count_{period_name}")
        )
        
        agg = agg.join(
            df_period.groupBy("num_cpf", "safra").agg(
                F.max(F.col("ind_fraude")).alias(f"flag_teve_fraude_{period_name}"),
                F.max(F.col("ind_aca")).alias(f"flag_teve_aca_{period_name}"),
                F.max(F.col("ind_pccr")).alias(f"flag_teve_pccr_{period_name}")
            ),
            on=["num_cpf", "safra"],
            how="left"
        )
        
        # ====== NOVO EM v6.1: DELINQUENCY RATE ======
        # Delinquency Rate = QTD_FATURAS_ABERTAS / QTD_TOTAL_FATURAS
        agg = agg.withColumn(
            f"delinquency_rate_{period_name}",
            F.when(
                F.col(f"_total_faturas_count_{period_name}") > 0,
                F.col(f"qtd_faturas_abertas_{period_name}") / 
                F.col(f"_total_faturas_count_{period_name}")
            ).otherwise(F.lit(None).cast(DoubleType()))
        ).drop(f"_total_faturas_count_{period_name}")
        
        # ====== NOVO EM v6.1: MAX DIAS EM ATRASO ======
        # Max Days Arrears = MAX(DATA_REFERENCIA - DATA_VENCIMENTO) para faturas abertas
        # Lógica: contar dias entre snapshot (DAT_REFERENCIA, sempre dia 01) e vencimento original
        # IMPORTANTE: apenas contar para faturas VENCIDAS (referencia >= vencimento)
        # Nota: datas em Atraso estão no formato string ddMMMyyyy:HH:mm:ss
        max_dias_agg = df_period.filter(
            F.col("val_fat_aberto") > 0  # Apenas faturas abertas
        ).groupBy("num_cpf", "safra").agg(
            F.max(
                F.when(
                    F.to_date(F.col("ts_referencia"), "ddMMMyyyy:HH:mm:ss") >= 
                    F.to_date(F.col("dat_vencimento_fat"), "ddMMMyyyy:HH:mm:ss"),
                    # Fatura vencida: calcula dias
                    F.datediff(
                        F.to_date(F.col("ts_referencia"), "ddMMMyyyy:HH:mm:ss"),
                        F.to_date(F.col("dat_vencimento_fat"), "ddMMMyyyy:HH:mm:ss")
                    )
                ).otherwise(
                    # Fatura não vencida (futuro): 0 dias em atraso
                    F.lit(0)
                )
            ).alias(f"max_dias_atraso_{period_name}")
        )
        
        agg = agg.join(max_dias_agg, on=["num_cpf", "safra"], how="left")
        
        agg_list.append(agg)
    
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    result = spine_keys.join(result, on=["num_cpf", "safra"], how="left")
    
    print(f"    ✓ Agregação Atraso Enhanced: {result.count():,} registros")
    return result

def build_abt_v6_1(df_abt_v6, df_pag_enh, df_atr_enh):
    """
    Constrói ABT v6.1: estende v6 com features enhancement.
    Joins adicionais apenas das novas colunas (desconto_rate, delinquency_rate, max_dias_atraso)
    """
    print(">>> [Transform] Construindo ABT v6.1...")
    
    print("    → LEFT JOIN Pagamento (Desconto Rate)...")
    df_abt = df_abt_v6.join(
        df_pag_enh.select(
            "num_cpf", "safra",
            "desconto_rate_m1", "desconto_rate_m3", "desconto_rate_m6"
        ),
        on=["num_cpf", "safra"],
        how="left"
    )
    
    print("    → LEFT JOIN Atraso (Delinquency Rate + Max Days)...")
    df_abt = df_abt.join(
        df_atr_enh.select(
            "num_cpf", "safra",
            "delinquency_rate_m1", "delinquency_rate_m3", "delinquency_rate_m6",
            "max_dias_atraso_m1", "max_dias_atraso_m3", "max_dias_atraso_m6"
        ),
        on=["num_cpf", "safra"],
        how="left"
    )
    
    print("    → Aplicando coalesce para NULLs (novos)...")
    # Desconto rates: NULL é válido (cliente sem pagamento), mas quando existe preenche com 0
    for period in ["m1", "m3", "m6"]:
        df_abt = df_abt.withColumn(
            f"desconto_rate_{period}",
            F.coalesce(F.col(f"desconto_rate_{period}"), F.lit(0.0))
        )
        df_abt = df_abt.withColumn(
            f"delinquency_rate_{period}",
            F.coalesce(F.col(f"delinquency_rate_{period}"), F.lit(0.0))
        )
        df_abt = df_abt.withColumn(
            f"max_dias_atraso_{period}",
            F.coalesce(F.col(f"max_dias_atraso_{period}"), F.lit(0))
        )
    
    print("    → Atualizando metadados de Gold...")
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.lit(datetime.now().isoformat())) \
        .withColumn("gold_feature_blocks", 
                   F.lit("score_01,score_02,telco,cadastro,recarga,pagamento,atraso,enhancement"))
    
    return df_abt

def main():
    parser = argparse.ArgumentParser(
        description="Build Gold ABT v6.1 - Extensão com 3 Features Enhancement"
    )
    parser.add_argument("--gold_abt_v6_path", help="Caminho do Gold ABT v6 (Delta)")
    parser.add_argument("--silver_pagamento_path", help="Caminho da Silver Pagamento (Delta)")
    parser.add_argument("--silver_atraso_path", help="Caminho da Silver Atraso (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT v6.1 (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.gold_abt_v6_path and args_parsed.silver_pagamento_path and args_parsed.silver_atraso_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            gold_abt_v6_path = DEFAULT_GOLD_ABT_V6_PATH
            silver_pagamento_path = DEFAULT_SILVER_PAGAMENTO_PATH
            silver_atraso_path = DEFAULT_SILVER_ATRASO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_Builder_v6_1")

    # =========================================================================
    # 1) LEITURA GOLD ABT v6 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Gold ABT v6 (Spine): {args.gold_abt_v6_path}")
    try:
        df_abt_v6 = spark.read.format(args.format).load(args.gold_abt_v6_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ABT v6: {e}")
        sys.exit(1)

    count_in_v6 = df_abt_v6.count()
    print(f">>> [Info] Registros no Gold ABT v6: {count_in_v6:,}")

    # =========================================================================
    # 2) LEITURA SILVER PAGAMENTO (PARA CÁLCULO DE RATIOS)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Pagamento: {args.silver_pagamento_path}")
    try:
        df_pagamento = spark.read.format(args.format).load(args.silver_pagamento_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA PAGAMENTO: {e}")
        sys.exit(1)

    count_in_pagamento = df_pagamento.count()
    print(f">>> [Info] Registros no Silver Pagamento: {count_in_pagamento:,}")

    # =========================================================================
    # 3) LEITURA SILVER ATRASO (PARA CÁLCULO DE RATIOS + AGING)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Atraso: {args.silver_atraso_path}")
    try:
        df_atraso = spark.read.format(args.format).load(args.silver_atraso_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ATRASO: {e}")
        sys.exit(1)

    count_in_atraso = df_atraso.count()
    print(f">>> [Info] Registros no Silver Atraso: {count_in_atraso:,}")

    # =========================================================================
    # 4) AGREGAÇÕES TEMPORAIS COM ENHANCEMENTS (M1/M3/M6)
    # =========================================================================
    print("\n>>> [Agregação] Processando janelas temporais com enhancements...")
    df_pag_enh = aggregate_pagamento_with_ratios(df_pagamento, df_abt_v6)
    df_atr_enh = aggregate_atraso_with_enhancements(df_atraso, df_abt_v6)

    # =========================================================================
    # 5) BUILD ABT v6.1 (ABT v6 LEFT JOIN Pag Enhancement + Atr Enhancement)
    # =========================================================================
    print("\n>>> [Transform] Construindo ABT v6.1...")
    df_abt = build_abt_v6_1(df_abt_v6, df_pag_enh, df_atr_enh)

    # =========================================================================
    # 6) VALIDAÇÕES (obrigatórias para v6.1: 17 gates totais)
    # =========================================================================
    print("\n>>> [Validate] Executando validações de qualidade...")
    try:
        validate_abt_v6_1(df_abt, count_in_v6)  # Gates 1-17 (14 herdadas + 3 novas)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v6.1: {count_out:,}")

    # =========================================================================
    # 7) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f"\n>>> [Escrita] Salvando Gold ABT v6.1 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v6_1"
    df_abt.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    # =========================================================================

    # =========================================================================
    # 8) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v6.1 (v6 + Enhancement: Desconto, Delinquency, MaxDays)")
    print("="*80)
    
    # Distribuição de labels (herdada)
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    
    print("\n>>> [Stats] FLAG_INSTALACAO (decisão observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_out
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>12,} ({pct:>6.2f}%)")
    
    # Completude de features (herdadas)
    print("\n>>> [Features v1-v6] Cobertura (herdada de ABT v6):")
    score01_cov = (count_out - df_abt.filter(F.col("score_01_adj").isNull()).count()) * 100 / count_out
    recarga_cov = (df_abt.filter(F.col("qtd_recargas_m1") > 0).count()) * 100 / count_out
    pag_cov = (df_abt.filter(F.col("qtd_itens_pagamento_m1") > 0).count()) * 100 / count_out
    atr_cov = (df_abt.filter(F.col("qtd_faturas_abertas_m1") > 0).count()) * 100 / count_out
    
    print(f"    SCORE_01:      {score01_cov:>6.2f}%")
    print(f"    RECARGA:       {recarga_cov:>6.2f}%")
    print(f"    PAGAMENTO:     {pag_cov:>6.2f}%")
    print(f"    ATRASO:        {atr_cov:>6.2f}%")
    
    # Completude de features NOVAS v6.1
    print("\n>>> [Features v6.1 - ENHANCEMENT] Cobertura:")
    
    desconto_notnull = df_abt.filter(F.col("desconto_rate_m1").isNotNull()).count()
    desconto_cov = desconto_notnull * 100 / count_out
    desconto_mean = df_abt.agg(F.mean("desconto_rate_m1")).collect()[0][0]
    desconto_max = df_abt.agg(F.max("desconto_rate_m1")).collect()[0][0]
    print(f"    DESCONTO_RATE_M1:")
    print(f"      - Cobertura: {desconto_cov:>6.2f}%")
    print(f"      - Média:     {desconto_mean:>6.4f}")
    print(f"      - Máximo:    {desconto_max:>6.4f}")
    
    del_notnull = df_abt.filter(F.col("delinquency_rate_m1").isNotNull()).count()
    del_cov = del_notnull * 100 / count_out
    del_mean = df_abt.agg(F.mean("delinquency_rate_m1")).collect()[0][0]
    del_max = df_abt.agg(F.max("delinquency_rate_m1")).collect()[0][0]
    print(f"\n    DELINQUENCY_RATE_M1:")
    print(f"      - Cobertura: {del_cov:>6.2f}%")
    print(f"      - Média:     {del_mean:>6.4f}")
    print(f"      - Máximo:    {del_max:>6.4f}")
    
    dias_notnull = df_abt.filter(F.col("max_dias_atraso_m1") > 0).count()
    dias_cov = dias_notnull * 100 / count_out
    dias_mean = df_abt.filter(F.col("max_dias_atraso_m1") > 0).agg(F.mean("max_dias_atraso_m1")).collect()[0][0]
    dias_max = df_abt.agg(F.max("max_dias_atraso_m1")).collect()[0][0]
    print(f"\n    MAX_DIAS_ATRASO_M1:")
    print(f"      - Clientes com atraso: {dias_cov:>6.2f}%")
    print(f"      - Média dias (c/ atraso): {dias_mean:>6.1f}")
    print(f"      - Máximo dias:         {dias_max:>6.0f}")
    
    # Impacto do join
    print(f"\n>>> [JOIN] Impacto das agregações:")
    print(f"    Total ABT v6 (spine):        {count_in_v6:>12,}")
    print(f"    Total Pagamento (eventos):   {count_in_pagamento:>12,}")
    print(f"    Total Atraso (eventos):      {count_in_atraso:>12,}")
    print(f"    Resultado ABT v6.1 (LEFT):   {count_out:>12,}")
    print(f"    Retenção vs v6:              {count_out*100/count_in_v6:>12.2f}%")
    
    print("\n" + "="*80)
    print(f"✓ ABT v6.1 PRONTA PARA MODELAGEM")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: v6 + Enhancement (Desconto, Delinquency, MaxDays)")
    print(f"  - Total registros: {count_out:,}")
    print(f"  - Total colunas: {len(df_abt.columns)}")
    print(f"  - Grão: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Status: ENHANCEMENT com 9 features novas (Desconto, Delinquency, Aging)")
    print(f"  - KS esperado: 45-46% (vs v6: 44-45%, delta: +1-1.5pp)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
