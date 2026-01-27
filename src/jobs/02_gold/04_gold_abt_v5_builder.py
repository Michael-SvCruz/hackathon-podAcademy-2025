"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 04_gold_abt_v5_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v5 para modelagem.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script estende ABT v4 com features Recarga (agregação event-level para M1/M3/M6).

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ✅ ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ✅ ABT v2: ABT_v1 + SCORE_02                                 [COMPLETED]
3. ✅ ABT v3: ABT_v2 + Telco features (var_26-93)              [COMPLETED]
4. ✅ ABT v4: ABT_v3 + Cadastro (age, CEP, var_02-25)          [COMPLETED]
5. ⏳ ABT v5: ABT_v4 + Recarga (temporal features M1/M3/M6)   ← v5 (este script)
6.   ABT v6: ABT_v5 + Pagamento + Atraso

DEFINIÇÕES CRÍTICAS (conforme target_definition.md):
- Evento âncora: cliente-mês (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SÓ em FLAG_INSTALACAO=1)
- Decisão observada: FLAG_INSTALACAO (para análise de swaps)
- Features (X): SCORE_01/02 + TELCO (68) + CADASTRO (33) + RECARGA (temporal)

ANTI-LEAKAGE CRÍTICO:
- FPD_INT é LABEL, não feature
- FLAG_INSTALACAO_INT é LABEL, não feature
- Ambas incluídas apenas para auditoria e análise de impacto (swaps)

NOVO EM V5 (Recarga):
Agregação de 95.2M eventos Recarga para cliente-mês com temporal windows:

Features por SAFRA_RECARGA (data do evento, agrupa em mês):
├── M1 (último mês): 1 mês anterior a DT_SAFRA
├── M3 (últimos 3 meses): 3 meses anteriores a DT_SAFRA
└── M6 (últimos 6 meses): 6 meses anteriores a DT_SAFRA

Para cada período (M1/M3/M6):
  ├── QTD_RECARGAS_M*: Quantidade de eventos
  ├── SUM_VAL_REAL_CLEAN_M*: Soma de VAL_REAL (sem negativos)
  ├── SUM_VAL_BONUS_CLEAN_M*: Soma de VAL_BONUS (sem negativos)
  ├── SUM_VAL_CREDITO_INSERIDO_CLEAN_M*: Soma de VAL_CREDITO_INSERIDO
  ├── AVG_VAL_REAL_CLEAN_M*: Média de VAL_REAL (sem negativos)
  ├── FLAG_TEVE_SOS_M*: Indicador de presença de SOS
  └── [Extras por dimensão BOA]:
      └── QTD_RECARGAS_POR_TIPO_CREDITO_M* (dimensão perfeita, 0% sentinelas)

Dimensões UTILIZADAS (somente as BOAS — 0% sentinelas):
├── cod_tipo_credito (0.00% sentinelas) ✅
├── cod_status_plataforma (0.03% sentinelas) ✅
├── cod_tecnologia_dw (0.00% sentinelas) ✅ [NÃO USAR SE CORRELACIONADO COM TIPO_CREDITO]
└── cod_plataforma_atu (0.00% sentinelas) ✅ [NÃO USAR SE CONSTANTE]

Dimensões EXCLUÍDAS (90%+ sentinelas):
├── dw_forma_pagamento (99.04% sentinelas) ❌
├── cod_promocao (99.04% sentinelas) ❌
├── dw_tipo_recarga (94.29% sentinelas) ❌
├── dw_tipo_insercao (94.29% sentinelas) ❌
└── dw_plano_tarifacao (~95% sentinelas) ❌

ESTRUTURA DO JOIN:
- Spine (ABT v4 Gold)     : 1:1 por NUM_CPF + SAFRA
- Recarga Silver (evento) : N:1 por NUM_CPF + SAFRA_RECARGA (múltiplos eventos)
- Join type              : LEFT (v4 é spine, Recarga é enriquecimento)
- Agregação             : Temporal (M1/M3/M6 baseado em DT_SAFRA vs DT_RECARGA)
- Resultado             : 1:1 mantida, features como 0/NULL quando sem recarga

VALIDAÇÕES OBRIGATÓRIAS (10 gates):
1. Unicidade: 1:1 por NUM_CPF + SAFRA (mantida de v4)
2. FPD observado SÓ em FLAG_INSTALACAO=1
3. Sem NULLs nas chaves
4. Distribuições balanceadas dos labels
5. Score_01 présente (cobertura > 90%)
6. Score_02 présente (cobertura > 40%)
7. Cobertura Telco > 20%
8. Cobertura Cadastro > 20%
9. Cobertura Recarga > 5% (novo em v5) — alguns clientes sem recarga é normal
10. QTD_RECARGAS_M1 distribution sensata (não infinitos, não NaNs)

AJUSTES UNITY CATALOG:
- Leitura de Gold ABT v4 + Silver Recarga
- Escrita em Delta + tabela UC
- Metadados de rastreabilidade

NOTAS IMPORTANTES:
- SAFRA_RECARGA na Silver = YYYYMM (primeiro dia do mês do evento)
- DT_SAFRA no ABT = primeiro dia do mês da safra (cliente-mês)
- Temporal window: se DT_RECARGA (ou SAFRA_RECARGA) está dentro dos últimos M1/M3/M6 
  antes de DT_SAFRA, inclui no agregado
- Valores negativos: usar colunas *_CLEAN (já filtradas na Silver)
- Deduplicação já realizada na Silver por EVENT_KEY SHA2
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v5

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_GOLD_ABT_V4_PATH = "/Volumes/hackathon_2025/default/gold/abt_v4_delta/"
DEFAULT_SILVER_RECARGA_PATH = "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v5_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v5"

# Dimensões BOAS em Recarga (0% sentinelas conforme quality report)
RECARGA_BOAS_DIMENSOES = [
    "cod_tipo_credito",           # 0.00% sentinelas ✅
    "cod_status_plataforma",       # 0.03% sentinelas ✅ (praticamente perfeito)
    "cod_tecnologia_dw",           # 0.00% sentinelas ✅
    "cod_plataforma_atu"           # 0.00% sentinelas ✅
]

# Períodos temporais para agregação
TEMPORAL_WINDOWS = {
    "M1": 1,   # 1 mês
    "M3": 3,   # 3 meses
    "M6": 6    # 6 meses
}

# =============================================================================

def aggregate_recarga_temporal(df_abt_v4, df_recarga):
    """
    Agrega eventos de Recarga para cliente-mês com windows temporais M1/M3/M6.
    
    Inputs:
    - df_abt_v4: Gold ABT v4 com NUM_CPF, SAFRA, DT_SAFRA (referência temporal)
    - df_recarga: Silver Recarga com NUM_CPF, SAFRA, SAFRA_RECARGA, DT_RECARGA, 
                  valores monetários (*_clean), dimensões
    
    Outputs:
    - Agregações por (NUM_CPF, SAFRA) para M1/M3/M6:
      * QTD_RECARGAS_M*
      * SUM_VAL_REAL_CLEAN_M*
      * SUM_VAL_BONUS_CLEAN_M*
      * SUM_VAL_CREDITO_INSERIDO_CLEAN_M*
      * AVG_VAL_REAL_CLEAN_M*
      * FLAG_TEVE_SOS_M*
    
    Nota: Usa DT_SAFRA do ABT v4 como referência temporal (cliente-mês)
          Filtra eventos Recarga por SAFRA_RECARGA em M1/M3/M6 lookback windows
    """
    print(">>> [Transform] Agregando Recarga para temporal windows M1/M3/M6...")

    # Preparar: garantir SAFRA_RECARGA em formato DATE (primeiro dia do mês)
    # Exemplo: SAFRA_RECARGA = "202406" → DT_RECARGA_SAFRA = 2024-06-01
    df_rec = df_recarga.withColumn(
        "dt_recarga_safra",
        F.to_date(F.concat_ws("-", F.col("safra_recarga"), F.lit("01")), "yyyyMM-dd")
    )

    # Preparar ABT v4: selecionar apenas chaves + DT_SAFRA
    df_abt_dates = df_abt_v4.select("num_cpf", "safra", "dt_safra").distinct()

    # Inicializar agregação: criar chaves de cliente-mês
    agg_dict = {}
    
    # Para cada janela temporal (M1, M3, M6)
    for window_name, num_months in TEMPORAL_WINDOWS.items():
        print(f"    → Agregando {window_name} ({num_months} mês(es))...")
        
        # JOIN Recarga com ABT v4 (apenas por num_cpf para pegar todos os eventos do cliente)
        # Depois filtramos por janela temporal usando dt_safra como referência
        df_with_dates = df_rec.join(
            df_abt_dates,
            on=["num_cpf"],
            how="inner"
        )
        
        # Filtrar eventos dentro da janela temporal
        # Nota: safra_recarga em Recarga corresponde a dt_recarga_safra
        #       dt_safra em ABT é a referência (primeiro dia do mês do cliente-mês)
        df_window = df_with_dates.withColumn(
            f"is_in_{window_name}",
            F.when(
                (F.col("dt_recarga_safra") >= F.add_months(F.col("dt_safra"), -num_months)) &
                (F.col("dt_recarga_safra") < F.col("dt_safra")),
                1
            ).otherwise(0)
        )

        # Agregações básicas (quantidade, somas, médias)
        # Preserva safra para posterior join com ABT v4
        df_agg_basic = df_window \
            .filter(F.col(f"is_in_{window_name}") == 1) \
            .groupBy("num_cpf", "safra") \
            .agg(
                F.count("*").alias(f"qtd_recargas_{window_name.lower()}"),
                F.sum(F.coalesce(F.col("val_real_clean"), F.lit(0))).alias(f"sum_val_real_clean_{window_name.lower()}"),
                F.sum(F.coalesce(F.col("val_bonus_clean"), F.lit(0))).alias(f"sum_val_bonus_clean_{window_name.lower()}"),
                F.sum(F.coalesce(F.col("val_credito_inserido_clean"), F.lit(0))).alias(f"sum_val_credito_inserido_clean_{window_name.lower()}"),
                F.avg(F.coalesce(F.col("val_real_clean"), F.lit(0))).alias(f"avg_val_real_clean_{window_name.lower()}"),
                F.max(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias(f"flag_teve_sos_{window_name.lower()}"),
            )
        
        agg_dict[window_name] = df_agg_basic

    # Combinar todas as janelas temporais em um único DF (LEFT JOINs para manter todos os cliente-mês)
    df_agg_recarga = agg_dict["M1"]
    for window_name in ["M3", "M6"]:
        df_agg_recarga = df_agg_recarga.join(
            agg_dict[window_name],
            on=["num_cpf", "safra"],
            how="left"  # LEFT para manter M1 como base, mesmo se M3/M6 estiverem vazios
        )

    # Preencher NULLs com 0 (para clientes sem recarga em cada período)
    for window_name in TEMPORAL_WINDOWS.keys():
        w_lower = window_name.lower()
        df_agg_recarga = df_agg_recarga \
            .withColumn(f"qtd_recargas_{w_lower}", F.coalesce(F.col(f"qtd_recargas_{w_lower}"), F.lit(0))) \
            .withColumn(f"sum_val_real_clean_{w_lower}", F.coalesce(F.col(f"sum_val_real_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"sum_val_bonus_clean_{w_lower}", F.coalesce(F.col(f"sum_val_bonus_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"sum_val_credito_inserido_clean_{w_lower}", F.coalesce(F.col(f"sum_val_credito_inserido_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"avg_val_real_clean_{w_lower}", F.coalesce(F.col(f"avg_val_real_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"flag_teve_sos_{w_lower}", F.coalesce(F.col(f"flag_teve_sos_{w_lower}"), F.lit(0)))

    return df_agg_recarga

def build_abt_v5(df_abt_v4, df_recarga):
    """
    Constrói ABT v5: estende v4 com features Recarga (temporal M1/M3/M6).
    
    Inclusões v4 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria): flag_instalacao_int, fpd_int
    - Features v1-v2: score_01/02_adj + flags
    - Features v3: var_26_adj a var_93_adj + flags (Telco, 68 vars)
    - Features v4: idade_anos, cep_3_digitos, statusrf, var_02-25 + flags (Cadastro, 33 vars)
    - Metadados: prod, flag_mig2
    - Gold metadata: gold_version, gold_build_date, gold_feature_blocks
    
    Novas inclusões em v5:
    - QTD_RECARGAS_M1/M3/M6: Quantidade de eventos por período
    - SUM_VAL_REAL_CLEAN_M1/M3/M6: Soma de crédito real
    - SUM_VAL_BONUS_CLEAN_M1/M3/M6: Soma de bônus
    - SUM_VAL_CREDITO_INSERIDO_CLEAN_M1/M3/M6: Soma de crédito inserido
    - AVG_VAL_REAL_CLEAN_M1/M3/M6: Média de crédito real
    - FLAG_TEVE_SOS_M1/M3/M6: Indicador de SOS
    - [Opcional] Granularidade por dimensão boa (ex: QTD_RECARGAS_POR_TIPO_CREDITO_M1)
    - Atualiza gold_feature_blocks: "score_01,score_02,telco,cadastro,recarga"
    
    Exclusões (anti-leakage):
    - FPD_INT não pode ser feature
    - FLAG_INSTALACAO não pode ser feature
    """
    print(">>> [Transform] JOIN ABT v4 + Recarga (agregada) para ABT v5...")

    # Step 1: Agregar Recarga (passa v4 para ter contexto de dt_safra)
    df_recarga_agg = aggregate_recarga_temporal(df_abt_v4, df_recarga)

    # Step 2: LEFT JOIN ABT v4 com Recarga agregada
    df_abt_v5 = df_abt_v4.join(
        df_recarga_agg,
        on=["num_cpf", "safra"],
        how="left"
    )

    # Step 3: Preencher NULLs nas features Recarga (clientes sem qualquer evento)
    for window_name in TEMPORAL_WINDOWS.keys():
        w_lower = window_name.lower()
        df_abt_v5 = df_abt_v5 \
            .withColumn(f"qtd_recargas_{w_lower}", F.coalesce(F.col(f"qtd_recargas_{w_lower}"), F.lit(0))) \
            .withColumn(f"sum_val_real_clean_{w_lower}", F.coalesce(F.col(f"sum_val_real_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"sum_val_bonus_clean_{w_lower}", F.coalesce(F.col(f"sum_val_bonus_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"sum_val_credito_inserido_clean_{w_lower}", F.coalesce(F.col(f"sum_val_credito_inserido_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"avg_val_real_clean_{w_lower}", F.coalesce(F.col(f"avg_val_real_clean_{w_lower}"), F.lit(0.0))) \
            .withColumn(f"flag_teve_sos_{w_lower}", F.coalesce(F.col(f"flag_teve_sos_{w_lower}"), F.lit(0)))

    # Step 4: Atualizar metadados de Gold
    df_abt_v5 = df_abt_v5 \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco,cadastro,recarga"))

    return df_abt_v5

def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v5 - Recarga temporal features")
    parser.add_argument("--gold_v4_path", help="Caminho do Gold ABT v4 (Delta)")
    parser.add_argument("--silver_recarga_path", help="Caminho da Silver Recarga (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT v5 (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.gold_v4_path and args_parsed.silver_recarga_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            gold_v4_path = DEFAULT_GOLD_ABT_V4_PATH
            silver_recarga_path = DEFAULT_SILVER_RECARGA_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_V5_Builder")

    # =========================================================================
    # 1) LEITURA GOLD ABT V4 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Gold ABT v4 (Spine): {args.gold_v4_path}")
    try:
        df_abt_v4 = spark.read.format(args.format).load(args.gold_v4_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ABT V4: {e}")
        sys.exit(1)

    count_v4 = df_abt_v4.count()
    print(f">>> [Info] Registros no ABT v4: {count_v4}")

    # =========================================================================
    # 2) LEITURA SILVER RECARGA (EVENTOS)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Recarga (eventos): {args.silver_recarga_path}")
    try:
        df_recarga = spark.read.format(args.format).load(args.silver_recarga_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA RECARGA: {e}")
        sys.exit(1)

    count_recarga = df_recarga.count()
    print(f">>> [Info] Registros na Silver Recarga: {count_recarga}")

    # =========================================================================
    # 3) BUILD ABT v5
    # =========================================================================
    print(">>> [Transform] Construindo ABT v5 (Score_01 + Score_02 + Telco + Cadastro + Recarga)...")
    df_abt_v5 = build_abt_v5(df_abt_v4, df_recarga)

    # =========================================================================
    # 4) VALIDAÇÕES (obrigatórias conforme target_definition.md)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    try:
        validate_abt_v5(df_abt_v5, count_v4)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_v5 = df_abt_v5.count()
    print(f">>> [Info] Registros no ABT v5: {count_v5}")

    # =========================================================================
    # 5) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v5 (Delta): {args.output_path}")

    df_abt_v5.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v5"
    df_abt_v5.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    # =========================================================================

    # =========================================================================
    # 6) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v5 (Score_01 + Score_02 + Telco + Cadastro + Recarga)")
    print("="*80)
    
    # Distribuição de labels
    dist_flag = df_abt_v5.groupBy("flag_instalacao_int").count().collect()
    dist_fpd = df_abt_v5.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()
    
    print("\n>>> [Stats] FLAG_INSTALACAO (decisão observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_v5
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>10} ({pct:>5.2f}%)")
    
    print("\n>>> [Stats] FPD_INT (target de risco, observado só em FLAG_INSTALACAO=1):")
    fpd_count = sum([row["count"] for row in dist_fpd if row["fpd_int"] == 1])
    flag_1_count = df_abt_v5.filter(F.col("flag_instalacao_int") == 1).count()
    fpd_rate = 100 * fpd_count / flag_1_count if flag_1_count > 0 else 0
    print(f"    FPD=1 (em FLAG=1): {fpd_count:>10} ({fpd_rate:>5.2f}%)")

    # Cobertura de features por versão
    score_01_coverage = df_abt_v5.filter(F.col("score_01_adj").isNotNull()).count() * 100 / count_v5
    score_02_coverage = df_abt_v5.filter(F.col("score_02_adj").isNotNull()).count() * 100 / count_v5
    telco_cols = [col for col in df_abt_v5.columns if col.startswith("var_") and col.endswith("_adj") and int(col.split("_")[1]) >= 26]
    telco_coverage = df_abt_v5.filter(F.col(telco_cols[0]).isNotNull()).count() * 100 / count_v5 if telco_cols else 0
    cadastro_cols = [col for col in df_abt_v5.columns if col.startswith("var_") and col.endswith("_adj") and int(col.split("_")[1]) < 26]
    cadastro_coverage = df_abt_v5.filter(F.col(cadastro_cols[0]).isNotNull()).count() * 100 / count_v5 if cadastro_cols else 0
    recarga_coverage = df_abt_v5.filter(F.col("qtd_recargas_m1") > 0).count() * 100 / count_v5

    print(f"\n>>> [Coverage] Cobertura de features por bloco:")
    print(f"    Score_01:   {score_01_coverage:>6.2f}%")
    print(f"    Score_02:   {score_02_coverage:>6.2f}%")
    print(f"    Telco:      {telco_coverage:>6.2f}%")
    print(f"    Cadastro:   {cadastro_coverage:>6.2f}%")
    print(f"    Recarga:    {recarga_coverage:>6.2f}% (clientes com qtd_recargas_m1 > 0)")

    # Estatísticas de Recarga
    print(f"\n>>> [Stats] Agregados Recarga (M1/M3/M6):")
    recarga_stats = df_abt_v5.select(
        F.mean("qtd_recargas_m1").alias("mean_qtd_m1"),
        F.mean("qtd_recargas_m3").alias("mean_qtd_m3"),
        F.mean("qtd_recargas_m6").alias("mean_qtd_m6"),
        F.mean("sum_val_real_clean_m1").alias("mean_val_m1"),
        F.mean("sum_val_real_clean_m3").alias("mean_val_m3"),
        F.mean("sum_val_real_clean_m6").alias("mean_val_m6"),
    ).collect()[0]
    
    print(f"    QTD_RECARGAS (média):")
    print(f"      M1: {recarga_stats['mean_qtd_m1']:.2f}")
    print(f"      M3: {recarga_stats['mean_qtd_m3']:.2f}")
    print(f"      M6: {recarga_stats['mean_qtd_m6']:.2f}")
    
    print(f"    SUM_VAL_REAL_CLEAN (média):")
    print(f"      M1: {recarga_stats['mean_val_m1']:.2f}")
    print(f"      M3: {recarga_stats['mean_val_m3']:.2f}")
    print(f"      M6: {recarga_stats['mean_val_m6']:.2f}")

    print("\n" + "="*80)
    print(f"✓ Gold ABT v5 concluído com sucesso!")
    print(f"  - Registros: {count_v5}")
    print(f"  - Features: Score_01 + Score_02 + Telco (68) + Cadastro (33) + Recarga (temporal)")
    print(f"  - Próximo passo: Gold v6 (Pagamento + Atraso)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
