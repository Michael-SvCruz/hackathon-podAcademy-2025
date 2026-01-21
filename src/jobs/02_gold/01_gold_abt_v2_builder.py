"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 01_gold_abt_v2_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v2 para modelagem.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script estende ABT v1 com Score_02 como feature adicional.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ✅ ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ⏳ ABT v2: ABT_v1 + SCORE_02                                 ← v2 (este script)
3.   ABT v3: ABT_v2 + Telco features (var_26-93)
4.   ABT v4: ABT_v3 + Cadastro
5.   ABT v5: ABT_v4 + Recarga
6.   ABT v6: ABT_v5 + Pagamento + Atraso

DEFINIÇÕES CRÍTICAS (conforme target_definition.md):
- Evento âncora: cliente-mês (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SÓ em FLAG_INSTALACAO=1)
- Decisão observada: FLAG_INSTALACAO (para análise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ (novidade em v2)

ANTI-LEAKAGE CRÍTICO:
- FPD_INT é LABEL, não feature
- FLAG_INSTALACAO é LABEL, não feature
- Ambas incluídas apenas para auditoria e análise de impacto (swaps)

VALIDAÇÕES OBRIGATÓRIAS:
1. Unicidade: 1:1 por NUM_CPF + SAFRA
2. FPD observado SÓ em FLAG_INSTALACAO=1
3. Sem NULLs nas chaves
4. Distribuições balanceadas
5. Score_02 présente com cobertura razoável

NOVO EM V2:
- SCORE_02_ADJ: Score histórico secundário (sentinela 0 → NULL)
- FLAG_SCORE02_MISSING: Flag de missing/sentinela para score_02
- gold_feature_blocks: Agora "score_01,score_02"

AJUSTES UNITY CATALOG:
- Leitura de Silver Bureau (tabelas Databricks)
- Escrita em Delta + tabela UC
- Metadados de rastreabilidade
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

from src.utils.spark_utils import get_spark_session
from .validators.validate_abt import validate_abt_v2

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_SILVER_BUREAU_PATH = "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v2"
# =============================================================================

def build_abt_v2(df_bureau):
    """
    Constrói ABT v2: estende v1 com SCORE_02 como feature adicional.
    
    Incluções v1 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria/impacto): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Metadados: prod, flag_mig2
    
    Novas inclusões em v2:
    - Features v2: score_02_adj, flag_score02_missing
    - Incrementa blocos de feature (score_01,score_02)
    
    Exclusões (anti-leakage):
    - FPD_INT não pode ser feature
    - FLAG_INSTALACAO_INT não pode ser feature
    - Telco features ficam para v3
    """
    print(">>> [Transform] Selecionando colunas para ABT v2...")

    # Seleção de colunas ordenadas logicamente
    df_abt = df_bureau.select(
        # CHAVES (obrigatórias para identificação)
        "num_cpf",
        "safra",
        "dt_safra",
        
        # LABELS (para auditoria e análise de impacto - NÃO usar como features)
        "flag_instalacao_int",     # Decisão observada (0/1)
        "fpd_int",                 # Target de risco (0/1, observado SÓ em flag_instalacao_int=1)
        
        # FEATURES v1 (SCORE_01) - MANTIDAS DE V1
        "score_01_adj",            # Score 1 ajustado (sentinela 0 → NULL)
        "flag_score01_missing",    # Flag de missing/sentinela para score_01
        
        # FEATURES v2 (SCORE_02) - NOVAS
        "score_02_dbl",            # Score 2 tipado (já vem como double da Silver)
        "flag_score02_missing",    # Flag de missing para score_02
        
        # METADADOS DE ORIGEM
        "prod",
        "flag_mig2",
        
        # AUDITORIA (rastreabilidade)
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
    
    # Tratar sentinela em SCORE_02: valor 0 é sentinela (não informado)
    # Converter para NULL e manter flag de missing
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    )
    
    # Remover coluna score_02_dbl (usar apenas ajustada)
    df_abt = df_abt.drop("score_02_dbl")
    
    # Reordenar colunas para clareza (features agrupadas)
    df_abt = df_abt.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",
        
        # Labels
        "flag_instalacao_int",
        "fpd_int",
        
        # Features Score_01 (v1)
        "score_01_adj",
        "flag_score01_missing",
        
        # Features Score_02 (v2 novo)
        "score_02_adj",
        "flag_score02_missing",
        
        # Metadados
        "prod",
        "flag_mig2",
        
        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
    
    # Adicionar metadados de gold
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02"))  # Quais blocos estão em v2
    
    return df_abt

def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v2 - Score_01 + Score_02")
    parser.add_argument("--silver_path", help="Caminho da Silver Bureau (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.silver_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            silver_path = DEFAULT_SILVER_BUREAU_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_Builder_v2")

    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bureau.count()
    print(f">>> [Info] Registros no Silver Bureau: {count_in}")

    # =========================================================================
    # 2) BUILD ABT v2
    # =========================================================================
    print(">>> [Transform] Construindo ABT v2 (Score_01 + Score_02)...")
    df_abt = build_abt_v2(df_bureau)

    # =========================================================================
    # 3) VALIDAÇÕES (obrigatórias conforme target_definition.md)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    try:
        validate_abt_v2(df_abt, count_in)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v2: {count_out}")

    # =========================================================================
    # 4) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v2 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v2"
    df_abt.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    # =========================================================================

    # =========================================================================
    # 5) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v2 (Score_01 + Score_02)")
    print("="*80)
    
    # Distribuição de labels
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()
    
    print("\n>>> [Stats] FLAG_INSTALACAO (decisão observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_out
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>10} ({pct:>5.2f}%)")
    
    print("\n>>> [Stats] FPD (target, observado SÓ em FLAG_INSTALACAO=1):")
    for row in dist_fpd:
        pct = row["count"] * 100 / count_out
        print(f"    FPD={row['fpd_int']}: {row['count']:>10} ({pct:>5.2f}%)")
    
    # Completude de features
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    
    score01_coverage = (count_out - score01_null) * 100 / count_out
    score02_coverage = (count_out - score02_null) * 100 / count_out
    
    print(f"\n>>> [Features] Completude:")
    print(f"    SCORE_01_ADJ: {score01_coverage:.2f}%")
    print(f"    SCORE_02_ADJ: {score02_coverage:.2f}%")
    
    # Incremento vs v1
    print(f"\n>>> [ΔKS] Impacto potencial do Score_02:")
    print(f"    Registros apenas com Score_01: {df_abt.filter((F.col('score_01_adj').isNotNull()) & (F.col('score_02_adj').isNull())).count()}")
    print(f"    Registros com ambos Scores: {df_abt.filter((F.col('score_01_adj').isNotNull()) & (F.col('score_02_adj').isNotNull())).count()}")
    print(f"    Registros com Score_02 mas não Score_01: {df_abt.filter((F.col('score_01_adj').isNull()) & (F.col('score_02_adj').isNotNull())).count()}")
    
    print("\n" + "="*80)
    print(f"✓ ABT v2 PRONTA PARA MODELAGEM")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: Score_01, Score_02")
    print(f"  - Total registros: {count_out}")
    print(f"  - Grão: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Status: Incremental (Score_02 adiciona {score02_coverage:.1f}% de cobertura)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
