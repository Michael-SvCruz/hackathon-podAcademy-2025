"""
ABT v6 Builder — Gold Layer
Combina todas as features v1-v6: Score + Telco + Cadastro + Recarga + Pagamento + Atraso

Entrada: ABT v5 (spine) + Silver Pagamento + Silver Atraso
Saída: ABT v6 (~250 colunas, 3.795.310 registros)

Anti-Leakage: ✅ Todas as features são históricas ou snapshots mensais
Validações: 14 gates (8 herdados + 6 novos)
KS Esperado: 44-45% (vs v5 ~42%)

Execução:
  python src/jobs/02_gold/05_gold_abt_v6_builder.py
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Adiciona src ao path
sys.path.insert(0, '/Workspace/src')

from utils.spark_utils import (
    get_spark_session, 
    to_int_safe, 
    treat_sentinel_value,
    standardize_column_names
)
from utils.validate_abt import validate_abt_v6

def load_abt_v5(spark):
    """Carrega ABT v5 (spine) com 3.795.310 registros"""
    print("🔹 Carregando ABT v5...")
    abt_v5 = spark.read.format("delta").table("gold.abt_v5_delta")
    print(f"   ✓ ABT v5: {abt_v5.count():,} registros")
    return abt_v5

def load_silver_pagamento(spark):
    """Carrega Silver Pagamento com 21.821.465 registros"""
    print("🔹 Carregando Silver Pagamento...")
    pagamento = spark.read.format("delta").table("silver.telco_silver_delta")
    
    # Se houver múltiplas cópias, selecionar a Pagamento corretamente
    # Por enquanto, assume que é a tabela padrão carregada
    try:
        pagamento = spark.read.format("delta").load(
            "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
        )
    except:
        pagamento = spark.read.format("delta").table("silver.pagamento_silver_delta")
    
    print(f"   ✓ Silver Pagamento: {pagamento.count():,} registros")
    return pagamento

def load_silver_atraso(spark):
    """Carrega Silver Atraso com 31.611.316 registros"""
    print("🔹 Carregando Silver Atraso...")
    try:
        atraso = spark.read.format("delta").load(
            "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
        )
    except:
        atraso = spark.read.format("delta").table("silver.atraso_silver_delta")
    
    print(f"   ✓ Silver Atraso: {atraso.count():,} registros")
    return atraso

def aggregate_pagamento_temporal(pagamento, abt_v5):
    """
    Agrega Pagamento por (NUM_CPF, SAFRA) com M1/M3/M6 windows.
    
    Lógica:
    - SAFRA_PAGAMENTO = YYYYMM(TS_STATUS_FATURA)
    - M1: SAFRA_PAGAMENTO = SAFRA
    - M3: SAFRA_PAGAMENTO em [SAFRA-2, SAFRA]
    - M6: SAFRA_PAGAMENTO em [SAFRA-5, SAFRA]
    
    Features agregadas (8 por período × 3 períodos = 24):
      QTD_ITENS, SUM_VAL_PAGO, SUM_DESCONTO, SUM_JUROS_POS, SUM_JUROS_NEG_ABS, 
      AVG_VAL_PAGO, MAX_VAL_PAGO, FLAG_TEVE_DESCONTO
    
    Plus 4 flags de qualidade × 3 períodos = 12
    Total: 36 features
    """
    print("📊 Agregando Pagamento temporal...")
    
    # Derivar SAFRA_PAGAMENTO e outras dimensões temporais
    pagamento = pagamento.withColumn(
        "safra_pagamento_calc",
        F.date_format(F.col("ts_status_fatura"), "yyyyMM")
    ).cache()
    
    # Obter range de SAFRA de v5 para contexto
    safra_min = abt_v5.agg(F.min("safra")).collect()[0][0]
    safra_max = abt_v5.agg(F.max("safra")).collect()[0][0]
    print(f"   Safras em ABT v5: {safra_min} a {safra_max}")
    
    # M1, M3, M6 windows
    agg_list = []
    
    for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
        print(f"     → Agregando {period_name.upper()}...")
        
        # Window: últimos N meses
        safra_min_period = f"(safra - {months_back:02d} * 100)" if months_back > 0 else "safra"
        
        # Filtrar Pagamento para o período
        pag_period = pagamento.filter(
            (F.col("safra_pagamento_calc") >= F.expr(safra_min_period)) &
            (F.col("safra_pagamento_calc") <= F.col("safra"))
        )
        
        # Agregações
        pag_agg = pag_period.groupBy("num_cpf", "safra").agg(
            # Contagem e somas
            F.count(F.lit(1)).alias(f"qtd_itens_pagamento_{period_name}"),
            F.sum(F.col("val_atual_pagamento")).alias(f"sum_val_pago_{period_name}"),
            F.sum(F.col("val_desconto_item")).alias(f"sum_val_desconto_{period_name}"),
            F.sum(F.col("val_juros_pos")).alias(f"sum_val_juros_pos_{period_name}"),
            F.sum(F.col("val_juros_neg_abs")).alias(f"sum_val_juros_neg_abs_{period_name}"),
            
            # Médias e máximos
            F.avg(F.col("val_atual_pagamento")).alias(f"avg_val_pago_{period_name}"),
            F.max(F.col("val_atual_pagamento")).alias(f"max_val_pago_{period_name}"),
            
            # Indicador desconto
            F.max(F.when(F.col("val_desconto_item") > 0, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"flag_teve_desconto_{period_name}"),
            
            # Qualidade: % missing TS_STATUS_PAGAMENTO
            F.sum(F.when(F.col("flag_ts_status_pagamento_missing") == 1, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"_count_missing_ts_status_pag_{period_name}"),
            F.count(F.lit(1)).alias(f"_count_total_items_{period_name}")
        )
        
        # Criar flag se missing% > 50%
        pag_agg = pag_agg.withColumn(
            f"flag_missing_ts_status_pagamento_{period_name}",
            F.when(
                (F.col(f"_count_missing_ts_status_pag_{period_name}") / 
                 F.col(f"_count_total_items_{period_name}")) > 0.5,
                F.lit(1)
            ).otherwise(F.lit(0))
        ).drop(f"_count_missing_ts_status_pag_{period_name}", f"_count_total_items_{period_name}")
        
        agg_list.append(pag_agg)
    
    # Combinar todas as agregações
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    print(f"   ✓ Agregação Pagamento: {result.count():,} registros únicos (num_cpf, safra)")
    return result

def aggregate_atraso_temporal(atraso, abt_v5):
    """
    Agrega Atraso por (NUM_CPF, SAFRA_ATRASO) com M1/M3/M6 windows.
    
    Lógica:
    - SAFRA_ATRASO = YYYYMM(TS_REFERENCIA) — já derivado em Silver
    - M1: SAFRA_ATRASO = SAFRA
    - M3: SAFRA_ATRASO em [SAFRA-2, SAFRA]
    - M6: SAFRA_ATRASO em [SAFRA-5, SAFRA]
    
    Features: QTD_FATURAS_ABERTAS, SUM_VAL_ABERTO, AVG/MAX, SUM_PAGAMENTO, SUM_MULTA_JUROS,
              FLAG_WO, FLAG_PDD, FLAG_FRAUDE, FLAG_ACA, FLAG_PCCR
    """
    print("📊 Agregando Atraso temporal...")
    
    # Atraso já tem safra_atraso derivado em Silver
    # Renomear para safra para unificar com ABT v5
    atraso = atraso.withColumnRenamed("safra_atraso", "safra_atraso_orig").cache()
    
    # M1, M3, M6
    agg_list = []
    
    for period_name, months_back in [("m1", 0), ("m3", 2), ("m6", 5)]:
        print(f"     → Agregando {period_name.upper()}...")
        
        # Filtrar por período temporal
        atr_period = atraso.filter(
            (F.col("safra_atraso_orig") >= F.expr(f"safra - {months_back:02d} * 100")) &
            (F.col("safra_atraso_orig") <= F.col("safra"))
        )
        
        # Agregações
        atr_agg = atr_period.groupBy("num_cpf", "safra").agg(
            # Quantidade e valores
            F.sum(F.when(F.col("val_fat_aberto") > 0, F.lit(1)).otherwise(F.lit(0)))
             .alias(f"qtd_faturas_abertas_{period_name}"),
            F.sum(F.col("val_fat_aberto")).alias(f"sum_val_aberto_{period_name}"),
            F.avg(F.col("val_fat_aberto")).alias(f"avg_val_aberto_{period_name}"),
            F.max(F.col("val_fat_aberto")).alias(f"max_val_aberto_{period_name}"),
            F.sum(F.col("val_fat_pagamento_bruto")).alias(f"sum_val_pagamento_{period_name}"),
            F.sum(F.col("val_multa_juros")).alias(f"sum_val_multa_juros_{period_name}"),
            
            # Indicadores (max = any true em período)
            F.max(F.col("ind_wo")).alias(f"flag_teve_wo_{period_name}"),
            F.max(F.col("ind_pdd")).alias(f"flag_teve_pdd_{period_name}"),
            F.max(F.col("ind_fraude")).alias(f"flag_teve_fraude_{period_name}"),
            F.max(F.col("ind_aca")).alias(f"flag_teve_aca_{period_name}"),
            F.max(F.col("ind_pccr")).alias(f"flag_teve_pccr_{period_name}")
        )
        
        agg_list.append(atr_agg)
    
    # Combinar
    result = agg_list[0]
    for agg in agg_list[1:]:
        result = result.join(agg, on=["num_cpf", "safra"], how="outer")
    
    print(f"   ✓ Agregação Atraso: {result.count():,} registros únicos (num_cpf, safra)")
    return result

def build_abt_v6(abt_v5, pag_agg, atr_agg):
    """
    Constrói ABT v6 via LEFT JOIN:
    
    ABT v5 (spine: 3.795.310)
      ↓ LEFT JOIN Pagamento (enriquecimento)
      ↓ LEFT JOIN Atraso (enriquecimento)
      ↓
    ABT v6: 3.795.310 linhas com ~250 colunas
    """
    print("🔨 Construindo ABT v6...")
    
    # JOIN 1: Pagamento
    abt = abt_v5.join(
        pag_agg,
        on=["num_cpf", "safra"],
        how="left"
    )
    print(f"   ✓ Após JOIN Pagamento: {abt.count():,} registros")
    
    # JOIN 2: Atraso
    abt = abt.join(
        atr_agg,
        on=["num_cpf", "safra"],
        how="left"
    )
    print(f"   ✓ Após JOIN Atraso: {abt.count():,} registros")
    
    # Coalesce NULLs para 0 (missing values)
    print("   → Aplicando coalesce para NULLs...")
    
    # Features de contagem
    for col in abt.columns:
        if col.startswith("qtd_"):
            abt = abt.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
    
    # Features de soma/média/max (valores monetários)
    for col in abt.columns:
        if col.startswith(("sum_", "avg_", "max_")):
            abt = abt.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
    
    # Flags
    for col in abt.columns:
        if col.startswith("flag_"):
            abt = abt.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
    
    print(f"   ✓ Coalesce aplicado, NULLs → 0/0.0")
    
    # Adicionar metadados
    abt = abt.withColumn("gold_version", F.lit("v6"))
    abt = abt.withColumn("gold_created_at", F.lit(datetime.now().isoformat()))
    abt = abt.withColumn("gold_row_count", F.lit(abt.count()))
    
    print(f"\n✅ ABT v6 construído: {abt.count():,} registros, {len(abt.columns)} colunas")
    return abt

def write_abt_v6(abt, spark):
    """Escreve ABT v6 em Delta Lake"""
    print("💾 Escrevendo ABT v6 em Delta Lake...")
    
    output_path = "/Volumes/hackathon_2025/default/gold/abt_v6_delta/"
    
    abt.write.format("delta").mode("overwrite").save(output_path)
    print(f"   ✓ Salvo em: {output_path}")
    
    # Registrar como tabela
    spark.sql(f"""
        CREATE OR REPLACE TABLE gold.abt_v6_delta
        USING DELTA
        LOCATION '{output_path}'
    """)
    print(f"   ✓ Tabela registrada: gold.abt_v6_delta")
    
    # Estatísticas
    print(f"\n📊 ABT v6 Estatísticas Finais:")
    print(f"   Registros: {abt.count():,}")
    print(f"   Colunas: {len(abt.columns)}")
    print(f"   Tamanho (approx): {(abt.count() * len(abt.columns)) / 1e6:.1f}M células")

def main():
    """Pipeline principal ABT v6"""
    print("\n" + "="*70)
    print("  🚀 ABT v6 BUILDER — Score + Telco + Cadastro + Recarga + Pag + Atraso")
    print("="*70 + "\n")
    
    spark = get_spark_session()
    
    try:
        # 1. Carregar
        abt_v5 = load_abt_v5(spark)
        pagamento = load_silver_pagamento(spark)
        atraso = load_silver_atraso(spark)
        
        # 2. Agregar
        pag_agg = aggregate_pagamento_temporal(pagamento, abt_v5)
        atr_agg = aggregate_atraso_temporal(atraso, abt_v5)
        
        # 3. Construir
        abt_v6 = build_abt_v6(abt_v5, pag_agg, atr_agg)
        
        # 4. Validar
        validate_abt_v6(abt_v6, abt_v5.count())
        
        # 5. Escrever
        write_abt_v6(abt_v6, spark)
        
        print("\n✅ ABT v6 COMPLETO E VALIDADO!")
        print("   Próximo: Criar Variable Book para cientistas de dados\n")
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
