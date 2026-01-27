"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 03_bronze_silver_recarga.py
OBJETIVO: Transformação da camada Bronze para Silver - Base RECARGA.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script lê a tabela Delta da camada Bronze (recarga), aplica tipagem
explícita, cria colunas derivadas (DT_SAFRA, TS_RECARGA, SAFRA_RECARGA) e 
trata particularidades de base de eventos: sentinelas em códigos (-1/-2/-3),
valores negativos em montantes, SOS e deduplicação robusta.

REGRAS DE NEGÓCIO (BASEADO EM recarga.md):
- Grão esperado: EVENT-LEVEL (múltiplos registros por NUM_CPF + SAFRA)
- Chave temporal do evento: DAT_INSERCAO_CREDITO (formato: ddMMMyyyy:HH:mm:ss)
- SAFRA derivada: YYYYMM a partir de DAT_INSERCAO_CREDITO (primeiro dia do mês)
- Variáveis principais:
  - Monetárias: VAL_CREDITO_INSERIDO, VAL_BONUS, VAL_REAL, VALOR_SOS
  - Dimensionais (códigos): COD_TECNOLOGIA_DW, COD_TIPO_CREDITO, etc.
  - Flags: FLAG_SOS, FLAG_INSTALACAO, FPD (quando houver)
  
PARTICULARIDADES DA RECARGA:
- BASE TRANSACIONAL (evento-level, 100M+ registros)
- Sentinelas observadas: -1 (não aplica), -2 (não determinado), -3 (não informado)
  → Trata mapeando para "sentinela" e criando flags
- Valores negativos em VAL_BONUS e VAL_REAL (~6% dos registros)
  → Trata criando colunas "clean" (NULL se negativo) + flags
- SOS (~6,5M eventos, valor de 3-20)
  → Mantém como feature separada + flag de presença
- Duplicidades observadas: ~320k duplicatas potenciais em 100M registros
  → Deduplicação robusta por chave de evento (EVENT_KEY)

AGREGAÇÃO PARA CLIENTE-MÊS:
- Na Silver: mantém nível de EVENTO
- Na Gold v5: agregação para NUM_CPF + SAFRA (features temporais: M1, M3, M6)
- Features v5: QTD_RECARGAS, SUM_VAL_REAL, SUM_VAL_BONUS, FLAG_SOS, etc.

ANTI-LEAKAGE:
- FPD e FLAG_INSTALACAO não devem ser usados como features (apenas labels/auditoria).

AJUSTES UNITY CATALOG:
- Leitura/escrita em Delta nos caminhos /Volumes/...
- Mantém colunas de auditoria da Bronze para rastreabilidade.
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import hashlib

from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe,
)

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/recarga_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/"
DEFAULT_FORMAT = "delta"

# Colunas numéricas de valor (montantes)
VALOR_COLUMNS = ["val_credito_inserido", "val_bonus", "val_real", "valor_sos"]

# Colunas dimensionais (códigos com sentinelas -1/-2/-3)
CODIGO_COLUMNS = [
    "cod_tecnologia_dw", "cod_tipo_credito", "dw_tipo_insercao", 
    "dw_tipo_recarga", "dw_forma_pagamento", "cod_plataforma_atu",
    "cod_status_plataforma", "cod_canal_aquisicao", "dw_instituicao",
    "dw_plano_tarifacao", "cod_promocao"
]

# Sentinelas padronizadas
SENTINELAS = [-1, -2, -3]
# =============================================================================

def build_silver(df_bronze):
    """
    Aplica tipagem explícita, cria derivadas e trata particularidades da Recarga.
    
    Pipeline:
    1. Tipagem básica (NUM_CPF, FLAG_SOS, FLAG_INSTALACAO como int)
    2. Parsing de DAT_INSERCAO_CREDITO → TS_RECARGA, DT_RECARGA, SAFRA_RECARGA
    3. Casting de valores monetários para double com flags de negativos
    4. Tratamento de sentinelas em códigos dimensionais (-1/-2/-3 → flags)
    5. Quality gates (domínio, valores)
    6. Seleção final de colunas
    
    Nota: Deduplicação é feita em função separada (dedupe_by_event_key).
    """
    print(">>> [Transform] Tipagem + regras Silver (recarga evento-level)...")

    # 1) Tipagem básica (chaves + flags)
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("dw_num_cliente", F.col("dw_num_cliente").cast("string"))
        .withColumn("dw_num_ntc", F.col("dw_num_ntc").cast("string"))
        .withColumn("flag_sos", to_int_safe("flag_sos"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao") if "flag_instalacao" in df_bronze.columns else F.lit(None).cast("int"))
        .withColumn("fpd_int", to_int_safe("fpd") if "fpd" in df_bronze.columns else F.lit(None).cast("int"))
    )

    # 2) Parsing tolerante de data/hora do evento
    print(">>> [Transform] Parseando DAT_INSERCAO_CREDITO com tolerância a inválidos...")
    
    # Parse TS_RECARGA (timestamp do evento)
    df = df.withColumn(
        "ts_recarga",
        F.when(
            F.col("dat_insercao_credito").isNotNull() & (F.trim(F.col("dat_insercao_credito")) != F.lit("")),
            F.to_timestamp(F.col("dat_insercao_credito"), "ddMMMyyyy:HH:mm:ss")
        ).otherwise(None)
    )
    
    # Flag de parsing inválido
    df = df.withColumn(
        "flag_ts_recarga_invalida",
        F.when(
            (F.col("dat_insercao_credito").isNotNull()) &
            (F.trim(F.col("dat_insercao_credito")) != F.lit("")) &
            (F.col("ts_recarga").isNull()),
            1
        ).otherwise(0)
    )
    
    # Derivar DT_RECARGA (data) e SAFRA_RECARGA (YYYYMM)
    df = df.withColumn(
        "dt_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.to_date(F.col("ts_recarga"))).otherwise(None)
    ).withColumn(
        "safra_recarga",
        F.when(F.col("dt_recarga").isNotNull(), F.date_format(F.col("dt_recarga"), "yyyyMM")).otherwise(None)
    )

    # 3) Casting de valores monetários (com flags de negativos)
    print(">>> [Transform] Tipando valores monetários e criando flags de negativos...")
    
    for val_col in VALOR_COLUMNS:
        if val_col in df.columns:
            df = (
                df
                .withColumn(f"{val_col}", to_double_safe(val_col))
                .withColumn(
                    f"flag_{val_col}_negativo",
                    F.when(F.col(val_col) < 0, 1).otherwise(0)
                )
                .withColumn(
                    f"{val_col}_clean",
                    F.when(F.col(val_col) < 0, None).otherwise(F.col(val_col))
                )
            )

    # 4) Tratamento de sentinelas em códigos dimensionais (-1/-2/-3)
    # Nota: colunas dimensionais podem ter valores não-numéricos (ex: "PE", "MG")
    # Usar expr com try_cast (SQL function) para tolerar valores malformados (retorna NULL)
    print(">>> [Transform] Tratando sentinelas em códigos dimensionais...")
    
    for cod_col in CODIGO_COLUMNS:
        if cod_col in df.columns:
            # Usar SQL try_cast via F.expr para ser tolerante com valores não-numéricos
            df = (
                df
                .withColumn(cod_col, F.expr(f"try_cast({cod_col} as int)"))
                .withColumn(
                    f"flag_{cod_col}_sentinela",
                    F.when(F.col(cod_col).isin(*SENTINELAS), 1).otherwise(0)
                )
            )

    # 5) Quality gates simples (domínio)
    df = df.withColumn(
        "flag_sos_invalida",
        F.when((~F.col("flag_sos").isin(0, 1)) & F.col("flag_sos").isNotNull(), 1).otherwise(0)
    )
    
    if "flag_instalacao_int" in df.columns and df.select("flag_instalacao_int").schema[0].dataType.simpleString() != "null":
        df = df.withColumn(
            "flag_instalacao_invalida",
            F.when((~F.col("flag_instalacao_int").isin(0, 1)) & F.col("flag_instalacao_int").isNotNull(), 1).otherwise(0)
        )
    
    if "fpd_int" in df.columns and df.select("fpd_int").schema[0].dataType.simpleString() != "null":
        df = df.withColumn(
            "fpd_invalido",
            F.when((~F.col("fpd_int").isin(0, 1)) & F.col("fpd_int").isNotNull(), 1).otherwise(0)
        )

    # 6) Novos metadados
    df = df.withColumn("metadata_data_transformacao", F.current_timestamp()) \
           .withColumn("metadata_versao_regra", F.lit("silver_recarga_v1"))

    # 7) Seleção final de colunas (Silver "clean" - evento level)
    columns_to_select = [
        # Chaves (identificação do evento)
        "num_cpf",
        "dw_num_cliente",
        "dw_num_ntc",
        
        # Timestamp do evento
        "ts_recarga",
        "dt_recarga",
        "safra_recarga",
        "flag_ts_recarga_invalida",
        
        # Labels (não usar como features)
        "flag_instalacao_int",
        "fpd_int",
        
        # Valores monetários (clean + flags de negativos)
        "val_credito_inserido",
        "flag_val_credito_inserido_negativo",
        "val_credito_inserido_clean",
        
        "val_bonus",
        "flag_val_bonus_negativo",
        "val_bonus_clean",
        
        "val_real",
        "flag_val_real_negativo",
        "val_real_clean",
        
        # SOS (separado)
        "flag_sos",
        "flag_sos_invalida",
        "valor_sos",
        
        # Códigos dimensionais + flags sentinela
        *[f"{cod}" for cod in CODIGO_COLUMNS if cod in df.columns],
        *[f"flag_{cod}_sentinela" for cod in CODIGO_COLUMNS if cod in df.columns],
        
        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    ]
    
    # Remover duplicatas mantendo ordem
    columns_to_select = list(dict.fromkeys(columns_to_select))
    
    # Selecionar apenas colunas que existem no DF
    existing_columns = [col for col in columns_to_select if col in df.columns]
    df_silver = df.select(existing_columns)

    return df_silver

def dedupe_by_event_key(df_silver):
    """
    Garante 1 registro por evento (deduplicação robusta).
    
    Estratégia:
    - Cria EVENT_KEY via hash SHA2 de colunas-chave do evento
    - Desempata por TS_RECARGA DESC (mais recente)
    
    Colunas usadas para EVENT_KEY:
    NUM_CPF, DW_NUM_NTC, TS_RECARGA, VAL_REAL, VAL_CREDITO_INSERIDO, 
    COD_TIPO_CREDITO, COD_STATUS_PLATAFORMA
    """
    print(">>> [Transform] Deduplicação por EVENT_KEY (evento)...")

    # Construir EVENT_KEY (hash de colunas-chave)
    event_key_cols = [
        "num_cpf",
        "dw_num_ntc",
        "ts_recarga",
        "val_real",
        "val_credito_inserido",
        "cod_tipo_credito",
        "cod_status_plataforma"
    ]
    
    # Filtrar apenas colunas que existem
    event_key_cols = [col for col in event_key_cols if col in df_silver.columns]
    
    # Criar EVENT_KEY via hash
    df_silver = df_silver.withColumn(
        "event_key",
        F.sha2(
            F.concat_ws("||", *[F.col(col).cast("string") for col in event_key_cols]),
            256
        )
    )

    # Dedupe: row_number por EVENT_KEY, ordenado por TS_RECARGA DESC
    w = Window.partitionBy("event_key").orderBy(F.col("ts_recarga").desc())
    df_ranked = df_silver.withColumn("rn", F.row_number().over(w))
    df_out = df_ranked.filter(F.col("rn") == 1).drop("rn", "event_key")

    return df_out

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver - Base RECARGA (evento-level)")
    parser.add_argument("--input_path", help="Caminho da Bronze (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino na Silver (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato do arquivo de origem (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.input_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            input_path = DEFAULT_INPUT_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Silver_Recarga")

    # =========================================================================
    # 1) LEITURA BRONZE
    # =========================================================================
    print(f">>> [Leitura] Lendo Bronze: {args.input_path}")
    try:
        df_bronze = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bronze.count()
    print(f">>> [Info] Registros na Bronze: {count_in}")

    # =========================================================================
    # 1.5) PADRONIZAÇÃO DE NOMES DE COLUNA
    # =========================================================================
    print(">>> [Transform] Padronizando nomes de colunas...")
    df_bronze = standardize_column_names(df_bronze)

    # =========================================================================
    # 2) TRANSFORM SILVER
    # =========================================================================
    df_silver = build_silver(df_bronze)

    # =========================================================================
    # 3) DEDUP POR EVENT_KEY
    # =========================================================================
    df_silver_dedup = dedupe_by_event_key(df_silver)

    count_out = df_silver_dedup.count()
    count_removed = count_in - count_out
    pct_removed = 100 * count_removed / count_in if count_in > 0 else 0
    
    print(f">>> [Info] Registros na Silver (após dedupe): {count_out}")
    print(f">>> [Info] Eventos removidos no dedupe: {count_removed} ({pct_removed:.2f}%)")

    # =========================================================================
    # 4) ESCRITA SILVER
    # =========================================================================
    print(f">>> [Escrita] Salvando Silver (Delta): {args.output_path}")

    df_silver_dedup.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.silver_recarga"
    df_silver_dedup.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    # =========================================================================

    # =========================================================================
    # 5) QUALITY CHECKS
    # =========================================================================
    print("\n" + "="*80)
    print(">>> [Quality] RELATÓRIO DE QUALIDADE - SILVER RECARGA")
    print("="*80)

    # Parsing issues
    ts_invalida = df_silver_dedup.filter(F.col("flag_ts_recarga_invalida") == 1).count()
    ts_valida = count_out - ts_invalida
    ts_coverage = 100 * ts_valida / count_out if count_out > 0 else 0
    
    print(f"\n>>> [Quality] Parsing de data:")
    print(f"    TS_RECARGA válida: {ts_valida:>12} ({ts_coverage:.2f}%)")
    print(f"    TS_RECARGA inválida: {ts_invalida:>12}")

    # Valores negativos
    print(f"\n>>> [Quality] Valores negativos (indicadores de ajuste/sentinela):")
    for val_col in VALOR_COLUMNS:
        if val_col in df_silver_dedup.columns:
            neg_count = df_silver_dedup.filter(F.col(f"flag_{val_col}_negativo") == 1).count()
            neg_pct = 100 * neg_count / count_out if count_out > 0 else 0
            print(f"    {val_col}_negativo: {neg_count:>12} ({neg_pct:.2f}%)")

    # Sentinelas em códigos
    print(f"\n>>> [Quality] Sentinelas em códigos dimensionais (-1/-2/-3):")
    for cod_col in CODIGO_COLUMNS:
        if f"flag_{cod_col}_sentinela" in df_silver_dedup.columns:
            sent_count = df_silver_dedup.filter(F.col(f"flag_{cod_col}_sentinela") == 1).count()
            sent_pct = 100 * sent_count / count_out if count_out > 0 else 0
            print(f"    {cod_col}_sentinela: {sent_count:>12} ({sent_pct:.2f}%)")

    # SOS
    sos_presente = df_silver_dedup.filter(F.col("flag_sos") == 1).count()
    sos_pct = 100 * sos_presente / count_out if count_out > 0 else 0
    
    print(f"\n>>> [Quality] SOS (serviço especial):")
    print(f"    Eventos com SOS: {sos_presente:>12} ({sos_pct:.2f}%)")
    
    # Labels (auditoria)
    if "flag_instalacao_int" in df_silver_dedup.columns:
        flag_null = df_silver_dedup.filter(F.col("flag_instalacao_int").isNull()).count()
        flag_0 = df_silver_dedup.filter(F.col("flag_instalacao_int") == 0).count()
        flag_1 = df_silver_dedup.filter(F.col("flag_instalacao_int") == 1).count()
        
        print(f"\n>>> [Quality] FLAG_INSTALACAO (label de decisão):")
        print(f"    Nulo: {flag_null:>12}")
        print(f"    FLAG=0: {flag_0:>12} ({100*flag_0/count_out:.2f}%)")
        print(f"    FLAG=1: {flag_1:>12} ({100*flag_1/count_out:.2f}%)")

    print("\n" + "="*80)
    print(f"✓ Silver RECARGA concluído (evento-level, pronto para agregação Gold)")
    print(f"  - Registros: {count_out}")
    print(f"  - Dedupe: {count_removed} duplicatas removidas")
    print(f"  - Período SAFRA_RECARGA: (a confirmar no Gold com derivação mensal)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
