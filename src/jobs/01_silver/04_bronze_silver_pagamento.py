"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 04_bronze_silver_pagamento.py
OBJETIVO: Transformação da camada Bronze para Silver — Pagamento.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (pagamento), aplica as 
transformações Silver conforme docs/03_silver_rules/pagamento.md:

1. Parse de datas (DAT_STATUS_FATURA, DAT_STATUS_PAGAMENTO)
2. Derivação de SAFRA_PAGAMENTO
3. Casting de valores monetários (double)
4. Deduplicação por versionamento (manter versão mais recente)
5. Flags de sentinelas e condições especiais
6. Padronização de nomes de coluna (snake_case)

PARTICULARIDADES:
- Versionamento: 8.163 chaves duplicadas (2 versões cada)
  → Manter versão mais recente (TS_STATUS_FATURA DESC)
- VAL_JUROS_MULTAS_ITEM pode ser negativo (é contábil, não erro)
  → Criar FLAG_JUROS_NEG
- DAT_STATUS_PAGAMENTO tem ~28% missing
  → Criar FLAG_TS_STATUS_PAGAMENTO_MISSING

ANTI-LEAKAGE:
- Não há issues de leakage nesta camada (é transacional pura)
- SAFRA_PAGAMENTO derivada de DAT_STATUS_FATURA (sempre preenchida)

PRÓXIMAS CAMADAS:
→ Gold: 05_gold_abt_v6_builder.py (agregação M1/M3/M6)
→ Cientistas: features agregadas por cliente-mês
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session, standardize_column_names, to_double_safe

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/pagamento_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_pagamento_v1"

# Sentinelas observadas em Atraso
SENTINELAS = ['-1', '-2', '-3']

# =============================================================================

def build_silver_pagamento(df_bronze):
    """
    Constrói Silver Pagamento com parsing, casting e deduplicação.
    
    Passos:
    1. Parse datas (DAT_STATUS_FATURA, DAT_STATUS_PAGAMENTO)
    2. Derivar SAFRA_PAGAMENTO
    3. Cast valores monetários
    4. Criar flags de sentinelas e especiais
    5. Deduplicação por versionamento (row_number by DEDUP_KEY, order by TS_STATUS_FATURA DESC)
    6. Padronizar nomes (snake_case)
    """
    print(">>> [Transform] Construindo Silver Pagamento...")
    
    # =========================================================================
    # Step 1: Parse de datas
    # =========================================================================
    print("    → Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_status_fatura",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FATURA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_pagamento",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_PAGAMENTO")), "ddMMMyyyy:HH:mm:ss")
    )
    
    # =========================================================================
    # Step 2: Derivar SAFRA_PAGAMENTO
    # =========================================================================
    print("    → Step 2: Derivando SAFRA_PAGAMENTO...")
    df = df.withColumn(
        "safra_pagamento",
        F.date_format(F.to_date(F.col("ts_status_fatura")), "yyyyMM")
    )
    
    # =========================================================================
    # Step 3: Cast de valores monetários (double)
    # =========================================================================
    print("    → Step 3: Casting valores monetários...")
    monetary_cols = [
        "VAL_PAGAMENTO_FATURA",
        "VAL_PAGAMENTO_ITEM",
        "VAL_ATUAL_PAGAMENTO",
        "VAL_ORIGINAL_PAGAMENTO",
        "VAL_PAGAMENTO_CREDITO",
        "VAL_DESCONTO_ITEM",
        "VAL_JUROS_MULTAS_ITEM",
        "VAL_MULTA_EQUIP_ITEM",
        "VAL_MULTA_EQUIP_TOTAL",
        "VAL_MULTA_FID_ITEM",
        "VAL_BAIXA_ATIVIDADE"
    ]
    
    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(
                col.lower(),
                to_double_safe(col)
            )
    
    # =========================================================================
    # Step 4: Flags de sentinelas e condições especiais
    # =========================================================================
    print("    → Step 4: Criando flags...")
    
    # Flag: DAT_STATUS_PAGAMENTO missing
    df = df.withColumn(
        "flag_ts_status_pagamento_missing",
        F.when(F.col("ts_status_pagamento").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    
    # Flag: VAL_JUROS_MULTAS_ITEM negativo
    df = df.withColumn(
        "flag_juros_neg",
        F.when(
            F.col("val_juros_multas_item").isNotNull() & (F.col("val_juros_multas_item") < 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    
    # Criar VAL_JUROS_POS e VAL_JUROS_NEG_ABS para Gold
    df = df.withColumn(
        "val_juros_pos",
        F.greatest(F.col("val_juros_multas_item"), F.lit(0))
    ).withColumn(
        "val_juros_neg_abs",
        F.abs(F.least(F.col("val_juros_multas_item"), F.lit(0)))
    )
    
    # =========================================================================
    # Step 5: Deduplicação por versionamento
    # =========================================================================
    print("    → Step 5: Deduplicando por versionamento...")
    
    # Criar DEDUP_KEY
    df = df.withColumn(
        "dedup_key",
        F.concat_ws("#", F.col("NUM_CPF"), F.col("CONTRATO"), F.col("SEQ_FATURA"),
                    F.col("NUM_SUB_SEQ_FATURA"), F.col("NUM_CREDITO_SEQ"))
    )
    
    # Aplicar row_number para deduplicação
    window_spec = Window.partitionBy("dedup_key").orderBy(F.col("ts_status_fatura").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec))
    
    # Manter apenas rn = 1
    df_dedup = df.filter(F.col("rn") == 1).drop("rn", "dedup_key")
    
    # =========================================================================
    # Step 6: Padronizar nomes de coluna (snake_case)
    # =========================================================================
    print("    → Step 6: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df_dedup)
    
    # =========================================================================
    # Step 7: Metadados da Silver
    # =========================================================================
    print("    → Step 7: Adicionando metadados...")
    df_silver = df_silver.withColumn(
        "metadata_data_transformacao",
        F.current_timestamp()
    ).withColumn(
        "metadata_versao_regra",
        F.lit(SILVER_VERSION)
    )
    
    return df_silver

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver — Pagamento")
    parser.add_argument("--input_path", help="Caminho do Bronze Pagamento")
    parser.add_argument("--output_path", help="Caminho de destino Silver Pagamento")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")
    
    args_parsed, unknown_args = parser.parse_known_args()
    
    if args_parsed.input_path and args_parsed.output_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()
    
    spark = get_spark_session("Silver_Transform_Pagamento")
    
    # =========================================================================
    # 1. LEITURA (BRONZE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Pagamento: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)
    
    count_bronze = df_bronze.count()
    print(f">>> [Info] Registros Bronze: {count_bronze:,}")
    
    # =========================================================================
    # 2. BUILD SILVER
    # =========================================================================
    df_silver = build_silver_pagamento(df_bronze)
    
    count_silver = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_silver:,}")
    
    # Auditoria de deduplicação
    linhas_removidas = count_bronze - count_silver
    print(f">>> [Auditoria] Linhas removidas (versionamento): {linhas_removidas:,}")
    print(f"    Esperado: ~8.163")
    
    # =========================================================================
    # 3. VALIDAÇÕES SIMPLES (GATES)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    
    # Gate 1: TS_STATUS_FATURA parseado
    invalidos_ts = df_silver.filter(F.col("ts_status_fatura").isNull()).count()
    print(f"    Gate 1 - TS_STATUS_FATURA inválidos: {invalidos_ts}")
    
    # Gate 2: NUM_CPF não nulo
    nulos_cpf = df_silver.filter(F.col("num_cpf").isNull()).count()
    print(f"    Gate 2 - NUM_CPF nulos: {nulos_cpf}")
    
    # Gate 3: Monitoramento VAL_JUROS_NEG
    juros_neg_count = df_silver.filter(F.col("flag_juros_neg") == 1).count()
    juros_neg_pct = (juros_neg_count / count_silver) * 100
    print(f"    Gate 3 - VAL_JUROS_MULTAS_ITEM < 0: {juros_neg_count:,} ({juros_neg_pct:.2f}%)")
    
    # Gate 4: Monitoramento DAT_STATUS_PAGAMENTO missing
    missing_status_pag = df_silver.filter(F.col("flag_ts_status_pagamento_missing") == 1).count()
    missing_pct = (missing_status_pag / count_silver) * 100
    print(f"    Gate 4 - DAT_STATUS_PAGAMENTO missing: {missing_status_pag:,} ({missing_pct:.2f}%)")
    
    # =========================================================================
    # 4. ESCRITA (SILVER — DELTA)
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Pagamento (Delta): {args.output_path}")
    
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
    
    print(f"✓ Escrita em Delta concluída")
    
    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS
    # =========================================================================
    target_table = "hackathon_2025.default.silver_pagamento"
    df_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    
    # =========================================================================
    # 5. RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL — Silver Pagamento")
    print("="*80)
    print(f"  Registros Bronze (entrada): {count_bronze:,}")
    print(f"  Registros Silver (saída):   {count_silver:,}")
    print(f"  Retenção: {(count_silver/count_bronze)*100:.2f}%")
    print(f"  Linhas removidas (versionamento): {linhas_removidas:,}")
    print(f"  Colunas originais: {len(df_bronze.columns)}")
    print(f"  Colunas após transformação: {len(df_silver.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Próximo passo: Gold (05_gold_abt_v6_builder.py)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
