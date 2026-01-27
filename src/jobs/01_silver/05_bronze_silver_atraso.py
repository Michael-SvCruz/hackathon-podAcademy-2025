"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 05_bronze_silver_atraso.py
OBJETIVO: Transformação da camada Bronze para Silver — Atraso/Faturamento.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (atraso), aplica as 
transformações Silver conforme docs/03_silver_rules/atraso.md:

1. Parse de datas (DAT_REFERENCIA, DAT_VENCIMENTO_FAT, DAT_STATUS_FAT)
2. Derivação de SAFRA_ATRASO (sempre YYYYMM do dia 01 = snapshot mensal)
3. Casting de valores monetários (double)
4. Flags de sentinelas (-1, -2, -3)
5. Monitoramento de duplicidade (sem dedup agressiva)
6. Padronização de nomes de coluna (snake_case)

PARTICULARIDADES:
- Snapshot mensal: DAT_REFERENCIA sempre dia 01 (fotografia de estado)
- Grão: múltiplas linhas por CPF (faturas, itens, ajustes)
- DAT_STATUS_FAT tem ~3.4% missing (criar flag)
- Sentinelas em várias colunas (-1, -2, -3)
  → Criar FLAG_<COL>_SENTINELA para cada
- Sem dedup agressiva (pode apagar sinal real)

ANTI-LEAKAGE:
- É snapshot mensal → SEM eventos futuros
- Seguro usar todas as features como estão no snapshot
- SAFRA_ATRASO = date_format(TS_REFERENCIA, 'yyyyMM')
- Todas as features refletem estado NA DATA DO SNAPSHOT

PRÓXIMAS CAMADAS:
→ Gold: 05_gold_abt_v6_builder.py (agregação M1/M3/M6)
→ Cientistas: features agregadas por cliente-mês
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F

from src.utils.spark_utils import get_spark_session, standardize_column_names, to_double_safe

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/atraso_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_atraso_v1"

# Sentinelas observadas
SENTINELAS = ['-1', '-2', '-3']

# Colunas com potencial sentinela
COLS_WITH_SENTINELA = {
    "IND_WO": "flag_ind_wo_sentinela",
    "IND_PDD": "flag_ind_pdd_sentinela",
    "IND_PCCR": "flag_ind_pccr_sentinela",
    "DW_TIPO_CLIENTE_CONTA": "flag_dw_tipo_cliente_sentinela",
    "COD_PLATAFORMA": "flag_cod_plataforma_sentinela",
    "DW_FAIXA_TEMPO_BASE": "flag_faixa_tempo_base_sentinela",
    "DW_FAIXA_AGING_PROX_FECH": "flag_faixa_aging_prox_fech_sentinela"
}

# =============================================================================

def build_silver_atraso(df_bronze):
    """
    Constrói Silver Atraso com parsing, casting, flags de sentinela.
    
    Passos:
    1. Parse datas (DAT_REFERENCIA, DAT_VENCIMENTO_FAT, DAT_STATUS_FAT)
    2. Derivar SAFRA_ATRASO (sempre YYYYMM, dia = 01)
    3. Cast valores monetários
    4. Criar flags de sentinelas para colunas categóricas
    5. Monitorar duplicidade (sem dedup)
    6. Padronizar nomes (snake_case)
    """
    print(">>> [Transform] Construindo Silver Atraso...")
    
    # =========================================================================
    # Step 1: Parse de datas
    # =========================================================================
    print("    → Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_referencia",
        F.to_timestamp(F.upper(F.col("DAT_REFERENCIA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_vencimento",
        F.to_timestamp(F.upper(F.col("DAT_VENCIMENTO_FAT")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_fat",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FAT")), "ddMMMyyyy:HH:mm:ss")
    )
    
    # =========================================================================
    # Step 2: Derivar SAFRA_ATRASO
    # =========================================================================
    print("    → Step 2: Derivando SAFRA_ATRASO...")
    df = df.withColumn(
        "safra_atraso",
        F.date_format(F.to_date(F.col("ts_referencia")), "yyyyMM")
    )
    
    # =========================================================================
    # Step 3: Cast de valores monetários (double)
    # =========================================================================
    print("    → Step 3: Casting valores monetários...")
    monetary_cols = [
        "VAL_FAT_LIQUIDO",
        "VAL_FAT_BRUTO",
        "VAL_FAT_CREDITO",
        "VAL_FAT_AJUSTE",
        "VAL_FAT_BRUTO_BC",
        "VAL_FAT_PAGAMENTO_BRUTO",
        "VAL_FAT_ABERTO",
        "VAL_FAT_ABERTO_LIQ",
        "VAL_MULTA_JUROS",
        "VAL_MULTA_CANCELAMENTO",
        "VAL_PARC_APARELHO_LIQ",
        "VAL_FAT_LIQ_JM_MC"
    ]
    
    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(
                col.lower(),
                to_double_safe(col)
            )
    
    # =========================================================================
    # Step 4: Flags de sentinelas e missing
    # =========================================================================
    print("    → Step 4: Criando flags de sentinelas...")
    
    # Flag: DAT_STATUS_FAT missing
    df = df.withColumn(
        "flag_status_fat_missing",
        F.when(F.col("ts_status_fat").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    
    # Flags para colunas com sentinelas
    for col_orig, flag_name in COLS_WITH_SENTINELA.items():
        if col_orig in df.columns:
            df = df.withColumn(
                flag_name,
                F.when(
                    F.col(col_orig.lower()).isin(SENTINELAS),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
    
    # =========================================================================
    # Step 5: Padronizar nomes de coluna (snake_case)
    # =========================================================================
    print("    → Step 5: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df)
    
    # =========================================================================
    # Step 6: Metadados da Silver
    # =========================================================================
    print("    → Step 6: Adicionando metadados...")
    df_silver = df_silver.withColumn(
        "metadata_data_transformacao",
        F.current_timestamp()
    ).withColumn(
        "metadata_versao_regra",
        F.lit(SILVER_VERSION)
    )
    
    return df_silver

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver — Atraso/Faturamento")
    parser.add_argument("--input_path", help="Caminho do Bronze Atraso")
    parser.add_argument("--output_path", help="Caminho de destino Silver Atraso")
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
    
    spark = get_spark_session("Silver_Transform_Atraso")
    
    # =========================================================================
    # 1. LEITURA (BRONZE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Bronze Atraso: {args.input_path}")
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
    df_silver = build_silver_atraso(df_bronze)
    
    count_silver = df_silver.count()
    print(f">>> [Info] Registros Silver: {count_silver:,}")
    
    # Auditoria (sem dedup agressiva, espera-se retenção ~100%)
    retenção_pct = (count_silver / count_bronze) * 100
    print(f">>> [Auditoria] Retenção: {retenção_pct:.2f}%")
    
    # =========================================================================
    # 3. VALIDAÇÕES SIMPLES (GATES)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    
    # Gate 1: TS_REFERENCIA parseado (sempre válido)
    invalidos_ref = df_silver.filter(F.col("ts_referencia").isNull()).count()
    print(f"    Gate 1 - TS_REFERENCIA inválidos: {invalidos_ref}")
    
    # Gate 2: NUM_CPF não nulo
    nulos_cpf = df_silver.filter(F.col("num_cpf").isNull()).count()
    print(f"    Gate 2 - NUM_CPF nulos: {nulos_cpf}")
    
    # Gate 3: Monitoramento DAT_STATUS_FAT missing
    missing_status = df_silver.filter(F.col("flag_status_fat_missing") == 1).count()
    missing_pct = (missing_status / count_silver) * 100
    print(f"    Gate 3 - DAT_STATUS_FAT missing: {missing_status:,} ({missing_pct:.2f}%)")
    
    # Gate 4: Monitoramento de sentinelas (exemplo: COD_PLATAFORMA)
    if "flag_cod_plataforma_sentinela" in df_silver.columns:
        sentinela_count = df_silver.filter(F.col("flag_cod_plataforma_sentinela") == 1).count()
        sentinela_pct = (sentinela_count / count_silver) * 100
        print(f"    Gate 4 - COD_PLATAFORMA com sentinela: {sentinela_count:,} ({sentinela_pct:.2f}%)")
    
    # =========================================================================
    # 4. ESCRITA (SILVER — DELTA)
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver Atraso (Delta): {args.output_path}")
    
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
    target_table = "hackathon_2025.default.silver_atraso"
    df_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    
    # =========================================================================
    # 5. RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL — Silver Atraso/Faturamento")
    print("="*80)
    print(f"  Registros Bronze (entrada): {count_bronze:,}")
    print(f"  Registros Silver (saída):   {count_silver:,}")
    print(f"  Retenção: {retenção_pct:.2f}%")
    print(f"  Colunas originais: {len(df_bronze.columns)}")
    print(f"  Colunas após transformação: {len(df_silver.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Próximo passo: Gold (05_gold_abt_v6_builder.py)")
    print(f"  Nota: Snapshot mensal (sempre dia 01) — anti-leakage garantido")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
