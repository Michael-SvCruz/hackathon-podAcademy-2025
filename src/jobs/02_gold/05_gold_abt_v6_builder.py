"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 05_gold_abt_v6_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v6 para modelagem.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script estende ABT v5 com features Pagamento e Atraso via join temporal.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ✅ ABT v1: bureau_full (spine) + SCORE_01                         [BASELINE]
2. ✅ ABT v2: ABT_v1 + SCORE_02                                      [COMPLETED]
3. ✅ ABT v3: ABT_v2 + Telco features (var_26-93)                   [COMPLETED]
4. ✅ ABT v4: ABT_v3 + Cadastro (age, CEP, var_02-25)               [COMPLETED]
5. ✅ ABT v5: ABT_v4 + Recarga (qtd_m1/m3/m6)                        [COMPLETED]
6. ⏳ ABT v6: ABT_v5 + Pagamento + Atraso (M1/M3/M6)                 ← v6 (este script)

DEFINIÇÕES CRÍTICAS (conforme target_definition.md):
- Evento âncora: cliente-mês (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SÓ em FLAG_INSTALACAO=1)
- Decisão observada: FLAG_INSTALACAO (para análise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ + TELCO + CADASTRO + RECARGA + PAGAMENTO + ATRASO

ANTI-LEAKAGE CRÍTICO:
- FPD_INT é LABEL, não feature
- FLAG_INSTALACAO_INT é LABEL, não feature
- Ambas incluídas apenas para auditoria e análise de impacto (swaps)
- Pagamento: histórico transacional (M1/M3/M6 = 1-6 meses antes)
- Atraso: snapshots mensais (não há dados futuros)

NOVO EM V6:
- PAGAMENTO (36 features): QTD/SUM/AVG/MAX M1/M3/M6 + quality flags
- ATRASO (36 features): QTD/SUM/AVG/MAX M1/M3/M6 + indicator flags

ESTRUTURA DO JOIN:
- Spine (ABT v5 Gold)       : 1:1 por NUM_CPF + SAFRA
- Pagamento Silver (agg)    : 1:1 por NUM_CPF + SAFRA (agregado M1/M3/M6)
- Atraso Silver (agg)       : 1:1 por NUM_CPF + SAFRA (agregado M1/M3/M6)
- Join type                 : LEFT (ABT v5 é spine, Pag+Atr são enriquecimento)
- Resultado                 : 1:1 mantida, features como 0/0.0 quando não encontrado

VALIDAÇÕES OBRIGATÓRIAS (14 gates):
1. Unicidade: 1:1 por NUM_CPF + SAFRA
2. FPD observado SÓ em FLAG_INSTALACAO=1
3. Sem NULLs nas chaves
4. Distribuições balanceadas dos labels
5. Score_01 présente (cobertura > 90%)
6. Score_02 présente (cobertura > 40%)
7. Cobertura Telco > 20%
8. Cobertura Cadastro > 20%
9. Cobertura Recarga > 5%
10. Sanidade Recarga (sem NaN/Inf)
11. Cobertura Pagamento > 2%
12. Cobertura Atraso > 10%
13. Sanidade Pagamento (ranges válidos)
14. Sanidade Atraso (ranges válidos)

AJUSTES UNITY CATALOG:
- Leitura de Gold ABT v5 + Silver Pagamento + Silver Atraso (tabelas Databricks)
- Agregação temporal M1/M3/M6 com coalesce para NULLs
- Escrita em Delta + tabela UC
- Metadados de rastreabilidade
--------------------------------------------------------------------------------
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v6

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_GOLD_ABT_V5_PATH = "/Volumes/hackathon_2025/default/gold/abt_v5_delta/"
DEFAULT_SILVER_PAGAMENTO_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_SILVER_ATRASO_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v6_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v6"
# ============================================================================= 
# =============================================================================

def aggregate_pagamento_temporal(df_pagamento, df_spine):
    """
    Agrega Pagamento por (NUM_CPF, SAFRA) com janelas M1/M3/M6.
    Usa spine para obter SAFRA anchor de cada cliente.
    Retorna: DataFrame com grão (NUM_CPF, SAFRA) e 36 features
    """
    print(">>> [Pagamento] Agregando por (NUM_CPF, SAFRA) com M1/M3/M6...")
    
    # Obter (NUM_CPF, SAFRA) únicos da spine
    spine_keys = df_spine.select("num_cpf", "safra").distinct()
    
    # Derivar SAFRA_PAGAMENTO
    df_pagamento = df_pagamento.withColumn(
        "safra_pag_calc",
        F.date_format(F.col("ts_status_fatura"), "yyyyMM").cast(IntegerType())
    )
    
    # JOIN com spine para obter SAFRA anchor
    df_pagamento_with_safra = df_pagamento.join(spine_keys, on="num_cpf", how="inner")
    
    agg_list = []
    
    for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
        
        # Filtrar pelo período
        df_period = df_pagamento_with_safra.withColumn(
            "safra_lower_bound",
            F.when(F.col("safra") > (100 * months_back),
                   F.col("safra") - (100 * months_back)
            ).otherwise(F.lit(0))
        ).filter(
            (F.col("safra_pag_calc") >= F.col("safra_lower_bound")) &
            (F.col("safra_pag_calc") <= F.col("safra"))
        )
        
        # Agregações
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
        
        agg_list.append(agg)
    
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    # RIGHT OUTER para incluir todos os clientes (mesmo sem Pagamento)
    result = spine_keys.join(result, on=["num_cpf", "safra"], how="left")
    
    print(f"    ✓ Agregação Pagamento: {result.count():,} registros")
    return result

def aggregate_atraso_temporal(df_atraso, df_spine):
    """
    Agrega Atraso por (NUM_CPF, SAFRA_ATRASO) com janelas M1/M3/M6.
    Usa spine para obter SAFRA anchor de cada cliente.
    Retorna: DataFrame com grão (NUM_CPF, SAFRA) e 36 features
    """
    print(">>> [Atraso] Agregando por (NUM_CPF, SAFRA) com M1/M3/M6...")
    
    # Obter (NUM_CPF, SAFRA) únicos da spine
    spine_keys = df_spine.select("num_cpf", "safra").distinct()
    
    if "safra_atraso" not in df_atraso.columns:
        df_atraso = df_atraso.withColumn(
            "safra_atraso",
            F.date_format(F.col("ts_referencia"), "yyyyMM").cast(IntegerType())
        )
    
    # JOIN com spine para obter SAFRA anchor
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
        
        agg = df_period.groupBy("num_cpf", "safra").agg(
            F.sum(F.when(F.col("val_fat_aberto") > 0, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"qtd_faturas_abertas_{period_name}"),
            F.sum(F.col("val_fat_aberto")).alias(f"sum_val_aberto_{period_name}"),
            F.avg(F.col("val_fat_aberto")).alias(f"avg_val_aberto_{period_name}"),
            F.max(F.col("val_fat_aberto")).alias(f"max_val_aberto_{period_name}"),
            F.sum(F.col("val_fat_pagamento_bruto")).alias(f"sum_val_pagamento_{period_name}"),
            F.max(F.col("ind_wo")).alias(f"flag_teve_wo_{period_name}"),
            F.max(F.col("ind_pdd")).alias(f"flag_teve_pdd_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "0-30 dias", 1))
             .alias(f"qtd_faturas_aging_0_30_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "31-60 dias", 1))
             .alias(f"qtd_faturas_aging_31_60_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == "61-90 dias", 1))
             .alias(f"qtd_faturas_aging_61_90_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura") == ">90 dias", 1))
             .alias(f"qtd_faturas_aging_90_plus_{period_name}"),
            F.countDistinct(F.when(F.col("dw_faixa_aging_fatura").isNull(), 1))
             .alias(f"qtd_faturas_aging_missing_{period_name}")
        )
        
        # FRAUDE, ACA, PCCR para todos os períodos (M1, M3, M6)
        agg = agg.join(
            df_period.groupBy("num_cpf", "safra").agg(
                F.max(F.col("ind_fraude")).alias(f"flag_teve_fraude_{period_name}"),
                F.max(F.col("ind_aca")).alias(f"flag_teve_aca_{period_name}"),
                F.max(F.col("ind_pccr")).alias(f"flag_teve_pccr_{period_name}")
            ),
            on=["num_cpf", "safra"],
            how="left"
        )
        
        agg_list.append(agg)
    
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    # RIGHT OUTER para incluir todos os clientes (mesmo sem Atraso)
    result = spine_keys.join(result, on=["num_cpf", "safra"], how="left")
    
    print(f"    ✓ Agregação Atraso: {result.count():,} registros")
    return result

def build_abt_v6(df_abt_v5, df_pag_agg, df_atr_agg):
    """
    Constrói ABT v6: estende v5 com features Pagamento e Atraso.
    """
    print(">>> [Transform] Construindo ABT v6...")
    
    print("    → LEFT JOIN Pagamento...")
    df_abt = df_abt_v5.join(df_pag_agg, on=["num_cpf", "safra"], how="left")
    
    print("    → LEFT JOIN Atraso...")
    df_abt = df_abt.join(df_atr_agg, on=["num_cpf", "safra"], how="left")
    
    print("    → Aplicando coalesce para NULLs...")
    for field in df_abt.schema:
        col_name = field.name
        col_type = field.dataType.simpleString()
        
        if col_name.startswith("qtd_") and "int" in col_type.lower():
            df_abt = df_abt.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))
        elif col_name.startswith(("sum_", "avg_", "max_")) and ("double" in col_type.lower() or "float" in col_type.lower()):
            df_abt = df_abt.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))
        elif col_name.startswith("flag_") and "int" in col_type.lower():
            df_abt = df_abt.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))
    
    print("    → Atualizando metadados de Gold...")
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.lit(datetime.now().isoformat())) \
        .withColumn("gold_feature_blocks", 
                   F.lit("score_01,score_02,telco,cadastro,recarga,pagamento,atraso"))
    
    return df_abt

def main():
    parser = argparse.ArgumentParser(
        description="Build Gold ABT v6 - Score + Telco + Cadastro + Recarga + Pagamento + Atraso"
    )
    parser.add_argument("--gold_abt_v5_path", help="Caminho do Gold ABT v5 (Delta)")
    parser.add_argument("--silver_pagamento_path", help="Caminho da Silver Pagamento (Delta)")
    parser.add_argument("--silver_atraso_path", help="Caminho da Silver Atraso (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.gold_abt_v5_path and args_parsed.silver_pagamento_path and args_parsed.silver_atraso_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            gold_abt_v5_path = DEFAULT_GOLD_ABT_V5_PATH
            silver_pagamento_path = DEFAULT_SILVER_PAGAMENTO_PATH
            silver_atraso_path = DEFAULT_SILVER_ATRASO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_Builder_v6")

    # =========================================================================
    # 1) LEITURA GOLD ABT v5 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Gold ABT v5 (Spine): {args.gold_abt_v5_path}")
    try:
        df_abt_v5 = spark.read.format(args.format).load(args.gold_abt_v5_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ABT v5: {e}")
        sys.exit(1)

    count_in_v5 = df_abt_v5.count()
    print(f">>> [Info] Registros no Gold ABT v5: {count_in_v5:,}")

    # =========================================================================
    # 2) LEITURA SILVER PAGAMENTO (ENRIQUECIMENTO)
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
    # 3) LEITURA SILVER ATRASO (ENRIQUECIMENTO)
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
    # 4) AGREGAÇÕES TEMPORAIS (M1/M3/M6)
    # =========================================================================
    print("\n>>> [Agregação] Processando janelas temporais...")
    df_pag_agg = aggregate_pagamento_temporal(df_pagamento, df_abt_v5)
    df_atr_agg = aggregate_atraso_temporal(df_atraso, df_abt_v5)

    # =========================================================================
    # 5) BUILD ABT v6 (ABT v5 LEFT JOIN Pag + Atr)
    # =========================================================================
    print("\n>>> [Transform] Construindo ABT v6...")
    df_abt = build_abt_v6(df_abt_v5, df_pag_agg, df_atr_agg)

    # =========================================================================
    # 6) VALIDAÇÕES (obrigatórias conforme target_definition.md)
    # =========================================================================
    print("\n>>> [Validate] Executando 14 gates de qualidade...")
    try:
        validate_abt_v6(df_abt, count_in_v5)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v6: {count_out:,}")

    # =========================================================================
    # 7) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f"\n>>> [Escrita] Salvando Gold ABT v6 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v6"
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
    print("RELATÓRIO FINAL - ABT v6 (Score + Telco + Cadastro + Recarga + Pagamento + Atraso)")
    print("="*80)
    
    # Distribuição de labels
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()
    
    print("\n>>> [Stats] FLAG_INSTALACAO (decisão observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_out
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>12,} ({pct:>6.2f}%)")
    
    print("\n>>> [Stats] FPD (target, observado SÓ em FLAG_INSTALACAO=1):")
    for row in dist_fpd:
        pct = row["count"] * 100 / count_out
        print(f"    FPD={row['fpd_int']}: {row['count']:>12,} ({pct:>6.2f}%)")
    
    # Completude de features v1-v5 (herdadas)
    print("\n>>> [Features v1-v5] Cobertura (herdada de ABT v5):")
    score01_cov = (count_out - df_abt.filter(F.col("score_01_adj").isNull()).count()) * 100 / count_out
    score02_cov = (count_out - df_abt.filter(F.col("score_02_adj").isNull()).count()) * 100 / count_out
    recarga_cov = (df_abt.filter(F.col("qtd_recargas_m1") > 0).count()) * 100 / count_out
    
    print(f"    SCORE_01: {score01_cov:>6.2f}%")
    print(f"    SCORE_02: {score02_cov:>6.2f}%")
    print(f"    RECARGA:  {recarga_cov:>6.2f}%")
    
    # Completude de features v6 (novas)
    print("\n>>> [Features v6 - PAGAMENTO] Cobertura:")
    pag_cov = (df_abt.filter(F.col("qtd_itens_pagamento_m1") > 0).count()) * 100 / count_out
    print(f"    Clientes com Pagamento: {pag_cov:>6.2f}%")
    total_pag_m1 = df_abt.agg(F.sum('qtd_itens_pagamento_m1')).collect()[0][0]
    total_valor_pago_m1 = df_abt.agg(F.sum('sum_val_pago_m1')).collect()[0][0]
    print(f"    Total transações M1: {total_pag_m1:,.0f}")
    print(f"    Total pago M1: {total_valor_pago_m1:,.2f}")
    
    print("\n>>> [Features v6 - ATRASO] Cobertura:")
    atr_cov = (df_abt.filter(F.col("qtd_faturas_abertas_m1") > 0).count()) * 100 / count_out
    print(f"    Clientes com Atraso: {atr_cov:>6.2f}%")
    total_faturas_m1 = df_abt.agg(F.sum('qtd_faturas_abertas_m1')).collect()[0][0]
    total_saldo_m1 = df_abt.agg(F.sum('sum_val_aberto_m1')).collect()[0][0]
    print(f"    Total faturas abertas M1: {total_faturas_m1:,.0f}")
    print(f"    Total saldo devedor M1: {total_saldo_m1:,.2f}")
    
    # Impacto do join
    print(f"\n>>> [JOIN] Impacto das agregações:")
    print(f"    Total ABT v5 (spine):        {count_in_v5:>12,}")
    print(f"    Total Pagamento (eventos):   {count_in_pagamento:>12,}")
    print(f"    Total Atraso (eventos):      {count_in_atraso:>12,}")
    print(f"    Resultado ABT v6 (LEFT):     {count_out:>12,}")
    print(f"    Retenção vs v5:              {count_out*100/count_in_v5:>12.2f}%")
    
    print("\n" + "="*80)
    print(f"✓ ABT v6 PRONTA PARA MODELAGEM")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: Score_01, Score_02, Telco, Cadastro, Recarga, Pagamento, Atraso")
    print(f"  - Total registros: {count_out:,}")
    print(f"  - Total colunas: {len(df_abt.columns)}")
    print(f"  - Grão: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Status: FINAL com 72 features novas (Pagamento + Atraso)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
