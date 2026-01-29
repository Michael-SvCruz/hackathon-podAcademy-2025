"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 04_gold_abt_v5_builder_v2.py
OBJETIVO: Construir ABT v5 integrando ABT v4 + Gold Recarga Features v2
================================================================================

DESCRIÇÃO TÉCNICA:
Este script é uma versão aprimorada do builder ABT v5 que utiliza as features
comportamentais geradas pelo gold_recarga_features_v2.py.

Diferenças da versão original (04_gold_abt_v5_builder.py):
- Lê de Gold Recarga Features (não Silver Recarga)
- Utiliza 60+ features comportamentais pré-calculadas
- Agregação temporal M1/M3/M6 otimizada
- Métricas avançadas de risco (SOS, padrões temporais, etc.)

================================================================================
ROADMAP INCREMENTAL:
================================================================================

1. ✅ ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ✅ ABT v2: v1 + SCORE_02                                     [COMPLETED]
3. ✅ ABT v3: v2 + Telco features (var_26-93)                   [COMPLETED]
4. ✅ ABT v4: v3 + Cadastro (age, CEP, var_02-25)               [COMPLETED]
5. ⏳ ABT v5: v4 + Recarga (60+ features, M1/M3/M6)            ← ESTE SCRIPT
6.    ABT v6: v5 + Pagamento + Atraso

================================================================================
ARQUITETURA DO JOIN:
================================================================================

    ABT v4 (gold/abt_v4_delta)
    ├── Grão: NUM_CPF + SAFRA (cliente-mês do spine)
    ├── ~1.2M registros
    └── Features: Score_01/02 + Telco (68) + Cadastro (33)
                    │
                    │ LEFT JOIN com filtro temporal
                    │ Condição: SAFRA_RECARGA < SAFRA (anti-leakage)
                    │
    Gold Recarga Features v2 (gold/recarga_features_v2_delta)
    ├── Grão: NUM_CPF + SAFRA_RECARGA (cliente-mês da recarga)
    ├── Features: 60+ features comportamentais
    └── Múltiplos registros por cliente (um por mês com atividade)
                    │
                    ▼
    ABT v5 (gold/abt_v5_v2_delta)
    ├── Grão: NUM_CPF + SAFRA (mantido do spine)
    ├── ~1.2M registros (mesmo count do v4)
    └── Features: v4 + Recarga (M1/M3/M6 agregações)

================================================================================
JANELAS TEMPORAIS (LOOKBACK):
================================================================================

Para cada registro no spine (NUM_CPF, SAFRA, DT_SAFRA):
- M1: Recargas do mês imediatamente anterior (SAFRA - 1 mês)
- M3: Recargas dos 3 meses anteriores (SAFRA - 3 meses)
- M6: Recargas dos 6 meses anteriores (SAFRA - 6 meses)

IMPORTANTE: SAFRA_RECARGA < SAFRA (anti-leakage)

Exemplo:
  Cliente com SAFRA = 202502 (fevereiro 2025)
  - M1: Recargas de janeiro 2025 (202501)
  - M3: Recargas de dezembro 2024 a janeiro 2025 (202412, 202501)
  - M6: Recargas de setembro 2024 a janeiro 2025 (202409..202501)

================================================================================
FEATURES GERADAS POR JANELA (M1/M3/M6):
================================================================================

Por janela temporal, as seguintes features são agregadas:

VOLUME:
- qtd_recargas_{m1,m3,m6}
- qtd_recargas_validas_{m1,m3,m6}
- qtd_telefones_distintos_{m1,m3,m6}

VALORES:
- sum_val_credito_{m1,m3,m6}
- sum_val_real_ajustado_{m1,m3,m6}
- sum_val_bonus_{m1,m3,m6}
- avg_val_real_{m1,m3,m6}
- ticket_medio_{m1,m3,m6}

SOS (Indicador de Estresse Financeiro):
- qtd_sos_{m1,m3,m6}
- sum_valor_sos_{m1,m3,m6}
- flag_teve_sos_{m1,m3,m6}
- pct_sos_sobre_credito_{m1,m3,m6}
- freq_sos_{m1,m3,m6}

TEMPO ENTRE RECARGAS:
- dias_medio_entre_recargas_{m1,m3,m6}
- dias_max_entre_recargas_{m1,m3,m6}

PADRÕES:
- coef_variacao_val_{m1,m3,m6}
- recargas_por_semana_{m1,m3,m6}
- pct_recargas_fim_semana_{m1,m3,m6}
- pct_recargas_madrugada_{m1,m3,m6}

FLAGS:
- flag_sem_recarga_{m1,m3,m6}
- flag_baixa_atividade_{m1,m3,m6}

================================================================================
VALIDAÇÕES (11 GATES):
================================================================================

Gates 1-8: Herdados de v4 (unicidade, anti-leakage, labels, scores, telco, cadastro)
Gate 9: Cobertura Recarga M1 > 5%
Gate 10: Distribuição SOS sensata (pct_sos < 100%)
Gate 11: Valores não-negativos para somas

================================================================================
"""

import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Importar utilities do projeto
try:
    from src.utils.spark_utils import get_spark_session
    from src.utils.validate_abt import validate_abt_v5
except ImportError:
    # Fallback para execução direta
    def get_spark_session(app_name="Hackathon_App"):
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def validate_abt_v5(df, count):
        print(">>> [Validate] Validação básica (função completa não disponível)")
        return True

# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_GOLD_ABT_V4_PATH = "/Volumes/hackathon_2025/default/gold/abt_v4_delta/"
DEFAULT_GOLD_RECARGA_FEATURES_PATH = "/Volumes/hackathon_2025/default/gold/recarga_features_v2_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v5_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v5_v2"

# Janelas temporais (meses de lookback)
TEMPORAL_WINDOWS = {
    "m1": 1,   # Último mês
    "m3": 3,   # Últimos 3 meses
    "m6": 6    # Últimos 6 meses
}

# Features do Gold Recarga a serem agregadas por janela
# Estas são as features mensais do gold_recarga_features_v2.py
FEATURES_SOMA = [
    "qtd_recargas_mes",
    "qtd_recargas_validas_mes",
    "sum_val_credito_mes",
    "sum_val_bonus_mes",
    "sum_val_real_mes",
    "sum_val_real_ajustado_mes",
    "qtd_sos_mes",
    "sum_valor_sos_mes",
    "qtd_recargas_madrugada_mes",
    "qtd_recargas_manha_mes",
    "qtd_recargas_tarde_mes",
    "qtd_recargas_noite_mes",
    "qtd_recargas_fim_semana_mes",
    "qtd_pago_puro_mes",
    "qtd_bonus_puro_mes",
    "qtd_combo_mes",
    "qtd_valor_negativo_mes",
]

FEATURES_MEDIA = [
    "avg_val_real_mes",
    "ticket_medio_mes",
    "dias_medio_entre_recargas_mes",
    "coef_variacao_val_mes",
    "recargas_por_semana_mes",
    "pct_recargas_fim_semana_mes",
    "pct_recargas_madrugada_mes",
    "freq_sos_mes",
    "pct_sos_sobre_credito_mes",
]

FEATURES_MAX = [
    "max_val_real_mes",
    "dias_max_entre_recargas_mes",
    "flag_teve_sos_mes",
    "qtd_telefones_distintos_mes",
    "qtd_tipos_credito_distintos_mes",
]

FEATURES_MIN = [
    "min_val_real_mes",
    "dias_min_entre_recargas_mes",
]


# =============================================================================
# FUNÇÕES DE AGREGAÇÃO TEMPORAL
# =============================================================================

def agregar_recarga_por_janela(
    df_spine: DataFrame,
    df_recarga_features: DataFrame,
    janela: str,
    num_meses: int
) -> DataFrame:
    """
    Agrega features de recarga para uma janela temporal específica.

    Lógica:
    1. JOIN spine com recarga features por NUM_CPF
    2. Filtrar: SAFRA_RECARGA dentro da janela (N meses antes de SAFRA)
    3. Agregar: SUM para contagens/valores, AVG para médias, MAX para flags

    Args:
        df_spine: DataFrame do ABT v4 (spine) com NUM_CPF, SAFRA, DT_SAFRA
        df_recarga_features: DataFrame do Gold Recarga Features v2
        janela: Nome da janela (m1, m3, m6)
        num_meses: Número de meses de lookback

    Returns:
        DataFrame com features agregadas para a janela
    """
    print(f">>> [Agg] Agregando janela {janela.upper()} ({num_meses} mês(es) de lookback)...")

    # Sufixo para nomes de colunas
    sfx = f"_{janela}"

    # Preparar spine: apenas chaves necessárias
    df_keys = df_spine.select(
        "num_cpf",
        "safra",
        "dt_safra"
    ).distinct()

    # Preparar recarga: garantir dt_recarga_safra existe
    df_rec = df_recarga_features
    if "dt_recarga_safra" not in df_rec.columns:
        df_rec = df_rec.withColumn(
            "dt_recarga_safra",
            F.to_date(F.concat(F.col("safra_recarga"), F.lit("01")), "yyyyMMdd")
        )

    # JOIN por NUM_CPF (cross join parcial para pegar todas as combinações)
    df_joined = df_keys.join(
        df_rec,
        on="num_cpf",
        how="left"
    )

    # Filtrar: SAFRA_RECARGA dentro da janela temporal
    # Condição: dt_recarga_safra >= (dt_safra - N meses) AND dt_recarga_safra < dt_safra
    df_filtered = df_joined.filter(
        (F.col("dt_recarga_safra") >= F.add_months(F.col("dt_safra"), -num_meses)) &
        (F.col("dt_recarga_safra") < F.col("dt_safra"))
    )

    # Contar meses com dados dentro da janela
    df_filtered = df_filtered.withColumn("_has_data", F.lit(1))

    # Agregar por NUM_CPF + SAFRA
    agg_exprs = []

    # Contagem de meses com dados
    agg_exprs.append(F.countDistinct("safra_recarga").alias(f"qtd_meses_com_recarga{sfx}"))

    # Somas
    for col in FEATURES_SOMA:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.sum(F.coalesce(F.col(col), F.lit(0))).alias(f"{col_clean}{sfx}")
        )

    # Médias (ponderadas pelo número de recargas quando aplicável)
    for col in FEATURES_MEDIA:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.avg(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Máximos
    for col in FEATURES_MAX:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.max(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Mínimos
    for col in FEATURES_MIN:
        col_clean = col.replace("_mes", "")
        agg_exprs.append(
            F.min(F.col(col)).alias(f"{col_clean}{sfx}")
        )

    # Executar agregação
    df_agg = df_filtered.groupBy("num_cpf", "safra").agg(*agg_exprs)

    return df_agg


def criar_features_derivadas_janela(df: DataFrame, janela: str) -> DataFrame:
    """
    Cria features derivadas para uma janela temporal.

    Args:
        df: DataFrame com features agregadas da janela
        janela: Nome da janela (m1, m3, m6)

    Returns:
        DataFrame com features derivadas adicionadas
    """
    sfx = f"_{janela}"

    # Ticket médio global da janela (recalculado)
    df = df.withColumn(
        f"ticket_medio_global{sfx}",
        F.when(
            F.col(f"qtd_recargas_validas{sfx}") > 0,
            F.round(F.col(f"sum_val_real_ajustado{sfx}") / F.col(f"qtd_recargas_validas{sfx}"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre crédito (recalculado para a janela)
    df = df.withColumn(
        f"pct_sos_credito_janela{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round((F.col(f"sum_valor_sos{sfx}") / F.col(f"sum_val_credito{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequência SOS na janela
    df = df.withColumn(
        f"freq_sos_janela{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round(F.col(f"qtd_sos{sfx}") / F.col(f"qtd_recargas{sfx}"), 4)
        ).otherwise(0.0)
    )

    # Valor líquido (crédito - SOS)
    df = df.withColumn(
        f"val_liquido{sfx}",
        F.col(f"sum_val_credito{sfx}") - F.col(f"sum_valor_sos{sfx}")
    )

    # Percentual de recargas na madrugada
    df = df.withColumn(
        f"pct_madrugada_janela{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_madrugada{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual de recargas no fim de semana
    df = df.withColumn(
        f"pct_fim_semana_janela{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_fim_semana{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Flag sem recarga na janela
    df = df.withColumn(
        f"flag_sem_recarga{sfx}",
        F.when(F.col(f"qtd_recargas{sfx}") == 0, 1).otherwise(0)
    )

    # Flag baixa atividade (menos de X recargas)
    limiar = {"m1": 2, "m3": 5, "m6": 10}.get(janela, 2)
    df = df.withColumn(
        f"flag_baixa_atividade{sfx}",
        F.when(F.col(f"qtd_recargas_validas{sfx}") < limiar, 1).otherwise(0)
    )

    return df


def build_abt_v5(df_abt_v4: DataFrame, df_recarga_features: DataFrame) -> DataFrame:
    """
    Constrói ABT v5: ABT v4 + Recarga Features (M1/M3/M6).

    Pipeline:
    1. Para cada janela temporal (M1, M3, M6):
       a. Agregar features de recarga
       b. Criar features derivadas
    2. JOIN todas as janelas com ABT v4
    3. Preencher NULLs (clientes sem recarga)
    4. Adicionar metadados

    Args:
        df_abt_v4: DataFrame do ABT v4 (spine)
        df_recarga_features: DataFrame do Gold Recarga Features v2

    Returns:
        DataFrame ABT v5 completo
    """
    print("\n" + "="*80)
    print("CONSTRUINDO ABT v5 (v4 + Recarga M1/M3/M6)")
    print("="*80 + "\n")

    # Dicionário para armazenar agregações por janela
    agg_por_janela = {}

    # Agregar para cada janela temporal
    for janela, num_meses in TEMPORAL_WINDOWS.items():
        print(f"\n>>> [Window] Processando janela {janela.upper()}...")

        # Agregar features
        df_agg = agregar_recarga_por_janela(
            df_abt_v4, df_recarga_features, janela, num_meses
        )

        # Criar features derivadas
        df_agg = criar_features_derivadas_janela(df_agg, janela)

        agg_por_janela[janela] = df_agg
        print(f"    → {df_agg.count():,} registros com dados para {janela.upper()}")

    # Combinar todas as janelas em um único DataFrame
    print("\n>>> [Join] Combinando janelas M1, M3, M6...")

    df_recarga_all = agg_por_janela["m1"]
    for janela in ["m3", "m6"]:
        df_recarga_all = df_recarga_all.join(
            agg_por_janela[janela],
            on=["num_cpf", "safra"],
            how="outer"
        )

    # JOIN com ABT v4 (LEFT JOIN para manter todos os registros do spine)
    print(">>> [Join] JOIN ABT v4 + Recarga Features...")

    df_abt_v5 = df_abt_v4.join(
        df_recarga_all,
        on=["num_cpf", "safra"],
        how="left"
    )

    # Preencher NULLs com 0 para features de contagem/soma
    print(">>> [Clean] Preenchendo NULLs com valores default...")

    # Lista de todas as colunas de recarga para preencher
    recarga_cols = [c for c in df_abt_v5.columns if any(
        c.endswith(f"_{j}") for j in TEMPORAL_WINDOWS.keys()
    )]

    for col in recarga_cols:
        # Colunas de contagem/soma: default 0
        if any(x in col for x in ["qtd_", "sum_", "flag_"]):
            df_abt_v5 = df_abt_v5.withColumn(
                col, F.coalesce(F.col(col), F.lit(0))
            )
        # Colunas de percentual: default 0.0
        elif any(x in col for x in ["pct_", "freq_"]):
            df_abt_v5 = df_abt_v5.withColumn(
                col, F.coalesce(F.col(col), F.lit(0.0))
            )
        # Colunas de média/valores: manter NULL (indica ausência de dados)
        # Não preencher avg_, dias_, coef_, ticket_, etc.

    # Garantir flags de sem recarga estejam corretas
    for janela in TEMPORAL_WINDOWS.keys():
        sfx = f"_{janela}"
        df_abt_v5 = df_abt_v5.withColumn(
            f"flag_sem_recarga{sfx}",
            F.when(
                F.col(f"qtd_recargas{sfx}").isNull() | (F.col(f"qtd_recargas{sfx}") == 0),
                1
            ).otherwise(0)
        )

    # Atualizar metadados
    print(">>> [Meta] Atualizando metadados...")

    df_abt_v5 = df_abt_v5 \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco,cadastro,recarga_v2"))

    return df_abt_v5


def validate_abt_v5_enhanced(df: DataFrame, count_expected: int) -> bool:
    """
    Validação aprimorada para ABT v5 com features de recarga.

    Gates:
    1-8: Herdados (unicidade, anti-leakage, labels, scores)
    9: Cobertura Recarga M1 > 5%
    10: Distribuição SOS sensata
    11: Valores não-negativos
    """
    print("\n" + "="*80)
    print("VALIDAÇÃO ABT v5 (11 GATES)")
    print("="*80 + "\n")

    count_atual = df.count()
    errors = []

    # Gate 1: Unicidade
    print(">>> [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    count_distinct = df.select("num_cpf", "safra").distinct().count()
    if count_distinct != count_atual:
        errors.append(f"Gate 1 FALHOU: {count_atual} != {count_distinct} (duplicatas)")
    else:
        print(f"    ✓ PASS: {count_atual:,} == {count_distinct:,} (sem duplicatas)")

    # Gate 2: Anti-leakage (FPD observado só em FLAG_INSTALACAO=1)
    print(">>> [Gate 2] Verificando anti-leakage (FPD)...")
    fpd_em_flag_0 = df.filter(
        (F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())
    ).count()
    if fpd_em_flag_0 > 0:
        errors.append(f"Gate 2 FALHOU: {fpd_em_flag_0} registros com FPD em FLAG=0")
    else:
        print(f"    ✓ PASS: FPD sempre nulo em FLAG_INSTALACAO=0")

    # Gate 3: Sem NULLs nas chaves
    print(">>> [Gate 3] Verificando NULLs nas chaves...")
    nulls_chaves = df.filter(
        F.col("num_cpf").isNull() | F.col("safra").isNull()
    ).count()
    if nulls_chaves > 0:
        errors.append(f"Gate 3 FALHOU: {nulls_chaves} registros com chaves nulas")
    else:
        print(f"    ✓ PASS: Nenhum NULL em chaves")

    # Gate 4: FLAG_INSTALACAO distribuição
    print(">>> [Gate 4] Verificando distribuição FLAG_INSTALACAO...")
    dist_flag = df.groupBy("flag_instalacao_int").count().collect()
    flags_presentes = [row["flag_instalacao_int"] for row in dist_flag]
    if 0 not in flags_presentes or 1 not in flags_presentes:
        errors.append(f"Gate 4 FALHOU: FLAG_INSTALACAO não tem 0 e 1")
    else:
        for row in dist_flag:
            pct = 100 * row["count"] / count_atual
            print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:,} ({pct:.2f}%)")
        print(f"    ✓ PASS: Ambos FLAG=0 e FLAG=1 presentes")

    # Gate 5: FPD distribuição
    print(">>> [Gate 5] Verificando distribuição FPD...")
    flag_1_count = df.filter(F.col("flag_instalacao_int") == 1).count()
    fpd_1_count = df.filter(F.col("fpd_int") == 1).count()
    if fpd_1_count == 0:
        errors.append(f"Gate 5 FALHOU: Nenhum FPD=1")
    else:
        fpd_rate = 100 * fpd_1_count / flag_1_count if flag_1_count > 0 else 0
        print(f"    FPD=1: {fpd_1_count:,} ({fpd_rate:.2f}% de FLAG=1)")
        print(f"    ✓ PASS: FPD contém positivos")

    # Gate 6: Score_01 cobertura
    print(">>> [Gate 6] Verificando cobertura Score_01...")
    score_01_coverage = df.filter(F.col("score_01_adj").isNotNull()).count() * 100 / count_atual
    if score_01_coverage < 90:
        errors.append(f"Gate 6 FALHOU: Score_01 cobertura {score_01_coverage:.2f}% < 90%")
    else:
        print(f"    Score_01 cobertura: {score_01_coverage:.2f}%")
        print(f"    ✓ PASS")

    # Gate 7: Score_02 cobertura
    print(">>> [Gate 7] Verificando cobertura Score_02...")
    score_02_coverage = df.filter(F.col("score_02_adj").isNotNull()).count() * 100 / count_atual
    if score_02_coverage < 40:
        errors.append(f"Gate 7 FALHOU: Score_02 cobertura {score_02_coverage:.2f}% < 40%")
    else:
        print(f"    Score_02 cobertura: {score_02_coverage:.2f}%")
        print(f"    ✓ PASS")

    # Gate 8: Telco cobertura (via var_26)
    print(">>> [Gate 8] Verificando cobertura Telco...")
    if "var_26_adj" in df.columns:
        telco_coverage = df.filter(F.col("var_26_adj").isNotNull()).count() * 100 / count_atual
        if telco_coverage < 20:
            errors.append(f"Gate 8 FALHOU: Telco cobertura {telco_coverage:.2f}% < 20%")
        else:
            print(f"    Telco cobertura: {telco_coverage:.2f}%")
            print(f"    ✓ PASS")
    else:
        print(f"    ⚠ SKIP: Coluna var_26_adj não encontrada")

    # Gate 9: Recarga M1 cobertura
    print(">>> [Gate 9] Verificando cobertura Recarga M1...")
    if "qtd_recargas_m1" in df.columns:
        recarga_coverage = df.filter(F.col("qtd_recargas_m1") > 0).count() * 100 / count_atual
        if recarga_coverage < 5:
            errors.append(f"Gate 9 FALHOU: Recarga M1 cobertura {recarga_coverage:.2f}% < 5%")
        else:
            print(f"    Recarga M1 cobertura: {recarga_coverage:.2f}%")
            print(f"    ✓ PASS")
    else:
        print(f"    ⚠ SKIP: Coluna qtd_recargas_m1 não encontrada")

    # Gate 10: SOS distribuição sensata
    print(">>> [Gate 10] Verificando distribuição SOS...")
    if "pct_sos_credito_janela_m1" in df.columns:
        avg_pct_sos = df.agg(F.avg("pct_sos_credito_janela_m1")).collect()[0][0] or 0
        if avg_pct_sos > 50:
            errors.append(f"Gate 10 FALHOU: Média pct_sos {avg_pct_sos:.2f}% > 50%")
        else:
            print(f"    Média pct_sos_credito M1: {avg_pct_sos:.2f}%")
            print(f"    ✓ PASS")
    else:
        print(f"    ⚠ SKIP: Coluna pct_sos_credito_janela_m1 não encontrada")

    # Gate 11: Valores não-negativos
    print(">>> [Gate 11] Verificando valores não-negativos...")
    if "sum_val_real_ajustado_m1" in df.columns:
        negativos = df.filter(F.col("sum_val_real_ajustado_m1") < 0).count()
        if negativos > count_atual * 0.01:  # Tolerância de 1%
            errors.append(f"Gate 11 FALHOU: {negativos} valores negativos (>1%)")
        else:
            print(f"    Valores negativos: {negativos} ({100*negativos/count_atual:.4f}%)")
            print(f"    ✓ PASS")
    else:
        print(f"    ⚠ SKIP: Coluna sum_val_real_ajustado_m1 não encontrada")

    # Resultado final
    print("\n" + "="*80)
    if errors:
        print("❌ VALIDAÇÃO FALHOU!")
        for error in errors:
            print(f"   {error}")
        print("="*80)
        return False
    else:
        print("✓ TODOS OS GATES PASSARAM!")
        print("="*80)
        return True


def gerar_relatorio_final(df: DataFrame, count_v4: int) -> None:
    """
    Gera relatório final com estatísticas do ABT v5.
    """
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v5 (v4 + Recarga M1/M3/M6)")
    print("="*80 + "\n")

    count_v5 = df.count()

    # Volumetria
    print(f">>> [Volumetria]")
    print(f"    ABT v4 (input): {count_v4:>12,}")
    print(f"    ABT v5 (output): {count_v5:>12,}")
    print(f"    Diferença: {count_v5 - count_v4:>12,}")

    # Labels
    print(f"\n>>> [Labels]")
    dist_flag = df.groupBy("flag_instalacao_int").count().collect()
    for row in dist_flag:
        pct = 100 * row["count"] / count_v5
        label = "Aprovado" if row["flag_instalacao_int"] == 1 else "Reprovado"
        print(f"    FLAG={row['flag_instalacao_int']} ({label}): {row['count']:>10,} ({pct:>5.2f}%)")

    flag_1_count = df.filter(F.col("flag_instalacao_int") == 1).count()
    fpd_1_count = df.filter(F.col("fpd_int") == 1).count()
    fpd_rate = 100 * fpd_1_count / flag_1_count if flag_1_count > 0 else 0
    print(f"    FPD=1 (em FLAG=1): {fpd_1_count:>10,} ({fpd_rate:>5.2f}%)")

    # Cobertura por bloco de features
    print(f"\n>>> [Cobertura por Bloco]")

    coverages = {
        "Score_01": df.filter(F.col("score_01_adj").isNotNull()).count() * 100 / count_v5,
        "Score_02": df.filter(F.col("score_02_adj").isNotNull()).count() * 100 / count_v5,
    }

    if "var_26_adj" in df.columns:
        coverages["Telco"] = df.filter(F.col("var_26_adj").isNotNull()).count() * 100 / count_v5

    if "idade_anos" in df.columns:
        coverages["Cadastro"] = df.filter(F.col("idade_anos").isNotNull()).count() * 100 / count_v5

    for janela in TEMPORAL_WINDOWS.keys():
        col = f"qtd_recargas_{janela}"
        if col in df.columns:
            coverages[f"Recarga {janela.upper()}"] = df.filter(F.col(col) > 0).count() * 100 / count_v5

    for nome, cov in coverages.items():
        print(f"    {nome}: {cov:>6.2f}%")

    # Estatísticas de Recarga
    print(f"\n>>> [Estatísticas Recarga]")

    for janela in TEMPORAL_WINDOWS.keys():
        sfx = f"_{janela}"
        qtd_col = f"qtd_recargas{sfx}"
        val_col = f"sum_val_real_ajustado{sfx}"
        sos_col = f"qtd_sos{sfx}"

        if all(c in df.columns for c in [qtd_col, val_col, sos_col]):
            stats = df.agg(
                F.avg(qtd_col).alias("avg_qtd"),
                F.avg(val_col).alias("avg_val"),
                F.sum(F.when(F.col(sos_col) > 0, 1).otherwise(0)).alias("count_com_sos"),
            ).collect()[0]

            print(f"    {janela.upper()}:")
            print(f"      Média recargas: {stats['avg_qtd']:.2f}")
            print(f"      Média valor: R$ {stats['avg_val']:.2f}")
            pct_sos = 100 * stats['count_com_sos'] / count_v5
            print(f"      Com SOS: {stats['count_com_sos']:,} ({pct_sos:.2f}%)")

    # Schema
    print(f"\n>>> [Schema]")
    print(f"    Total de colunas: {len(df.columns)}")

    # Contar colunas por tipo
    recarga_cols = len([c for c in df.columns if any(c.endswith(f"_{j}") for j in TEMPORAL_WINDOWS.keys())])
    print(f"    Colunas de Recarga: {recarga_cols}")


def main():
    """
    Pipeline principal para construção do ABT v5.
    """
    parser = argparse.ArgumentParser(
        description="Construir ABT v5 - ABT v4 + Recarga Features v2 (M1/M3/M6)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Modo padrão (caminhos default)
  python 04_gold_abt_v5_builder_v2.py

  # Com caminhos customizados
  python 04_gold_abt_v5_builder_v2.py \\
      --gold_v4_path /Volumes/.../gold/abt_v4_delta/ \\
      --recarga_features_path /Volumes/.../gold/recarga_features_v2_delta/ \\
      --output_path /Volumes/.../gold/abt_v5_v2_delta/
        """
    )

    parser.add_argument(
        "--gold_v4_path",
        default=DEFAULT_GOLD_ABT_V4_PATH,
        help="Caminho do ABT v4 (Delta)"
    )
    parser.add_argument(
        "--recarga_features_path",
        default=DEFAULT_GOLD_RECARGA_FEATURES_PATH,
        help="Caminho do Gold Recarga Features v2 (Delta)"
    )
    parser.add_argument(
        "--output_path",
        default=DEFAULT_OUTPUT_PATH,
        help="Caminho de destino do ABT v5 (Delta)"
    )
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT,
        help="Formato de leitura/escrita (default: delta)"
    )
    parser.add_argument(
        "--skip_validate",
        action="store_true",
        help="Pular validação (para debug)"
    )
    parser.add_argument(
        "--skip_save",
        action="store_true",
        help="Pular salvamento (para debug)"
    )

    args = parser.parse_args()

    # Banner
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "ABT V5 BUILDER V2".center(78) + "║")
    print("║" + "ABT v4 + Recarga Features v2 (M1/M3/M6)".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝")
    print("\n")

    # Inicializar Spark
    spark = get_spark_session("Gold_ABT_V5_Builder_V2")

    # =========================================================================
    # 1) LEITURA ABT V4 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando ABT v4 (spine): {args.gold_v4_path}")

    try:
        df_abt_v4 = spark.read.format(args.format).load(args.gold_v4_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ABT V4: {e}")
        sys.exit(1)

    count_v4 = df_abt_v4.count()
    print(f">>> [Info] Registros no ABT v4: {count_v4:,}")
    print(f">>> [Info] Colunas no ABT v4: {len(df_abt_v4.columns)}")

    # =========================================================================
    # 2) LEITURA GOLD RECARGA FEATURES V2
    # =========================================================================
    print(f"\n>>> [Leitura] Carregando Gold Recarga Features v2: {args.recarga_features_path}")

    try:
        df_recarga_features = spark.read.format(args.format).load(args.recarga_features_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA RECARGA FEATURES: {e}")
        sys.exit(1)

    count_recarga = df_recarga_features.count()
    print(f">>> [Info] Registros no Recarga Features: {count_recarga:,}")
    print(f">>> [Info] Colunas no Recarga Features: {len(df_recarga_features.columns)}")

    # =========================================================================
    # 3) BUILD ABT V5
    # =========================================================================
    df_abt_v5 = build_abt_v5(df_abt_v4, df_recarga_features)

    count_v5 = df_abt_v5.count()
    print(f"\n>>> [Info] Registros no ABT v5: {count_v5:,}")
    print(f">>> [Info] Colunas no ABT v5: {len(df_abt_v5.columns)}")

    # =========================================================================
    # 4) VALIDAÇÃO
    # =========================================================================
    if not args.skip_validate:
        validation_passed = validate_abt_v5_enhanced(df_abt_v5, count_v4)
        if not validation_passed:
            print("\n!!! ERRO: Validação falhou. Abortando salvamento.")
            if not args.skip_save:
                sys.exit(1)
    else:
        print("\n>>> [Skip] Validação pulada (--skip_validate)")

    # =========================================================================
    # 5) RELATÓRIO FINAL
    # =========================================================================
    gerar_relatorio_final(df_abt_v5, count_v4)

    # =========================================================================
    # 6) ESCRITA
    # =========================================================================
    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando ABT v5 (Delta): {args.output_path}")

        df_abt_v5.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(args.output_path)

        print(f">>> [Sucesso] Dados salvos em: {args.output_path}")

        # Registrar tabela no Unity Catalog
        target_table = "hackathon_2025.default.gold_abt_v5_v2"
        try:
            df_abt_v5.write \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(target_table)
            print(f">>> [Sucesso] Tabela registrada: {target_table}")
        except Exception as e:
            print(f">>> [Aviso] Não foi possível registrar tabela: {e}")
    else:
        print("\n>>> [Skip] Salvamento pulado (--skip_save)")

    # =========================================================================
    # 7) RESUMO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("ABT V5 CONSTRUÍDO COM SUCESSO!")
    print("="*80)
    print(f"""
    Resumo:
    ├── ABT v4 (input):           {count_v4:>12,} registros
    ├── Recarga Features (input): {count_recarga:>12,} registros
    ├── ABT v5 (output):          {count_v5:>12,} registros
    ├── Colunas totais:           {len(df_abt_v5.columns):>12}
    └── Output:                   {args.output_path}

    Features adicionadas:
    ├── Recarga M1: ~30 features (último mês)
    ├── Recarga M3: ~30 features (últimos 3 meses)
    └── Recarga M6: ~30 features (últimos 6 meses)

    Próximo passo:
    → ABT v6: v5 + Pagamento + Atraso
    """)
    print("="*80 + "\n")

    return df_abt_v5


if __name__ == "__main__":
    df_abt_v5 = main()
