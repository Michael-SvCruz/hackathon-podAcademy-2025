# Arquivo: scripts/opc_standby/gold_recarga_original.py
# VERSAO ORIGINAL: primeira adaptacao Databricks -> OCI Data Flow
# 5 actions pre-escrita: count_silver + count_gold + gerar_relatorio_qualidade (3 actions) + write
# Referencia para comparacao com gold_recarga.py (optz)
"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: gold_recarga.py (OCI Data Flow)
OBJETIVO: Processar base de Recarga e gerar features comportamentais para ABT v5+
================================================================================

DESCRICAO TECNICA:
Este script e uma versao unificada e aprimorada do processamento de Recarga,
combinando as melhores praticas de:
- 03_bronze_silver_recarga.py (tipagem, sentinelas, deduplicacao)
- tratamento_recarga_v1.py (SOS adjustment, enrichment, temporal metrics)

Adiciona features avancadas relevantes para modelagem de risco de credito.

================================================================================
ARQUITETURA DO PIPELINE:
================================================================================

    SILVER (recarga_silver_delta)
         | Event-level: ~95M registros
         | Grao: 1 linha por evento de recarga
         |
         v
    +------------------------------------------------------------+
    |             GOLD RECARGA FEATURES (este script)            |
    |                                                            |
    |  1. Enriquecimento com dimensoes (opcional)                |
    |  2. Ajuste de valores (SOS, Bonus)                         |
    |  3. Metricas temporais (tempo entre recargas)              |
    |  4. Agregacao por janelas (M1, M3, M6)                     |
    |  5. Features comportamentais avancadas                     |
    |                                                            |
    +------------------------------------------------------------+
         |
         | Grao: 1 linha por NUM_CPF + SAFRA_RECARGA
         v
    GOLD (recarga_features_delta)
         |
         | LEFT JOIN por (NUM_CPF, SAFRA)
         v
    ABT v5+ (abt_v5_delta, abt_v6_delta, etc.)

================================================================================
REGRAS DE NEGOCIO (conforme reuniao com Fernando/Claro - 07/01/2026):
================================================================================

1. SOS (Servico de Emprestimo de Credito):
   - SOS e um adiantamento/emprestimo de R$3-20 (geralmente R$5)
   - E descontado da proxima recarga tradicional do cliente
   - SOS e bonus NAO contam como "dinheiro real"
   - Exemplo: Recarga R$20 + SOS R$5 pendente = R$20 dinheiro real, nao R$25
   - IMPORTANTE: Frequencia de SOS e indicador de estresse financeiro

2. Valores Negativos:
   - Podem indicar ajustes, estornos ou sentinelas
   - Tratados com colunas *_CLEAN (NULL se negativo) + flags

3. Sentinelas em Codigos Dimensionais:
   - -1 = "Nao se aplica"
   - -2 = "Nao determinado"
   - -3 = "Nao informado"
   - Tratados com flags FLAG_*_SENTINELA

4. Anti-Leakage:
   - SAFRA_RECARGA deve ser ANTERIOR a SAFRA do cliente no spine
   - Janelas temporais (M1, M3, M6) garantem lookback correto
   - FPD e FLAG_INSTALACAO NAO sao features (labels apenas)

================================================================================
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Namespace OCI (passado como argumento)
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço:
# - Delta Lake (via configuration{} do Terraform)
# - Autenticação OCI (Resource Principal automático)

# =============================================================================
# CONFIGURACAO PADRAO
# =============================================================================
DEFAULT_SILVER_RECARGA_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/recarga/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/gold_recarga_features/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_recarga_features_v2"

# Configuracao das janelas temporais (meses de lookback)
TEMPORAL_WINDOWS = {
    "m1": 1,   # Ultimo mes
    "m3": 3,   # Ultimos 3 meses
    "m6": 6    # Ultimos 6 meses
}

# Limiar para "baixa atividade" (recargas por periodo)
LIMIAR_BAIXA_ATIVIDADE = {
    "m1": 2,   # Menos de 2 recargas/mes = baixa atividade
    "m3": 5,   # Menos de 5 recargas/trimestre
    "m6": 10   # Menos de 10 recargas/semestre
}

# Colunas monetarias esperadas na Silver
COLUNAS_VALOR = [
    "val_credito_inserido",
    "val_bonus",
    "val_real",
    "valor_sos"
]

# Colunas monetarias limpas (sem negativos)
COLUNAS_VALOR_CLEAN = [
    "val_credito_inserido_clean",
    "val_bonus_clean",
    "val_real_clean"
]


# =============================================================================
# FUNCOES DE PROCESSAMENTO
# =============================================================================

def preparar_recarga_para_agregacao(df_silver: DataFrame) -> DataFrame:
    """
    Prepara dados da Silver Recarga para agregacao Gold.

    Etapas:
    1. Garantir tipos corretos
    2. Criar dt_recarga_safra (primeiro dia do mes) para joins temporais
    3. Criar colunas de classificacao (tipo de transacao)
    4. Ajustar valores considerando SOS e bonus
    5. Criar colunas auxiliares para metricas temporais

    Args:
        df_silver: DataFrame da Silver Recarga (event-level)

    Returns:
        DataFrame preparado para agregacao
    """
    print(">>> [Prep] Preparando Recarga para agregacao Gold...")

    df = df_silver

    # -------------------------------------------------------------------------
    # 1. GARANTIR SAFRA_RECARGA E CRIAR DT_RECARGA_SAFRA
    # -------------------------------------------------------------------------
    # SAFRA_RECARGA (YYYYMM) -> DT_RECARGA_SAFRA (primeiro dia do mes)
    df = df.withColumn(
        "dt_recarga_safra",
        F.to_date(F.concat(F.col("safra_recarga"), F.lit("01")), "yyyyMMdd")
    )

    # Garantir ts_recarga existe (para calculos temporais)
    if "ts_recarga" not in df.columns:
        # Se nao existir, derivar de dt_recarga
        df = df.withColumn(
            "ts_recarga",
            F.when(F.col("dt_recarga").isNotNull(),
                   F.to_timestamp(F.col("dt_recarga")))
            .otherwise(None)
        )

    # -------------------------------------------------------------------------
    # 2. CLASSIFICACAO DE TIPO DE TRANSACAO
    # -------------------------------------------------------------------------
    # Classificar tipo de valor para analise
    print(">>> [Prep] Criando classificacao de tipo de transacao...")

    # Usar colunas clean se existirem, senao usar originais
    val_cred = F.coalesce(F.col("val_credito_inserido_clean"), F.col("val_credito_inserido"), F.lit(0.0))
    val_bonus = F.coalesce(F.col("val_bonus_clean"), F.col("val_bonus"), F.lit(0.0))
    val_real = F.coalesce(F.col("val_real_clean"), F.col("val_real"), F.lit(0.0))
    valor_sos = F.coalesce(F.col("valor_sos"), F.lit(0.0))
    flag_sos = F.coalesce(F.col("flag_sos"), F.lit(0))

    df = df.withColumn(
        "tipo_transacao",
        F.when((val_cred > 0) & (val_bonus == 0), "PAGO_PURO")
         .when((val_cred == 0) & (val_bonus > 0), "BONUS_PURO")
         .when((val_cred > 0) & (val_bonus > 0), "COMBO_PAGO_BONUS")
         .when((val_cred == 0) & (val_bonus == 0) & (val_real == 0), "ZERO_TOTAL")
         .when(val_real < 0, "VALOR_NEGATIVO")
         .otherwise("OUTROS")
    )

    # Flag de recarga valida (exclui ZERO_TOTAL)
    df = df.withColumn(
        "flag_recarga_valida",
        F.when(F.col("tipo_transacao") != "ZERO_TOTAL", 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # 3. AJUSTE DE VALORES (SOS E BONUS)
    # -------------------------------------------------------------------------
    # Conforme explicacao de Fernando (Claro):
    # - SOS e emprestimo que sera descontado da proxima recarga
    # - Bonus nao e dinheiro real
    # - VAL_REAL_AJUSTADO = credito real desconsiderando SOS pendente e bonus
    print(">>> [Prep] Aplicando ajuste de SOS e bonus...")

    # Etapa 1: Ajustar por SOS
    # Se FLAG_SOS=1 e VALOR_SOS == VAL_CREDITO_INSERIDO, significa que toda a recarga foi SOS
    # Se FLAG_SOS=1 e VALOR_SOS != VAL_CREDITO_INSERIDO, desconta o SOS
    df = df.withColumn(
        "val_real_ajustado_sos",
        F.when(
            (flag_sos == 1) & (valor_sos == val_cred),
            -valor_sos  # Toda a recarga foi SOS, valor real negativo
        )
        .when(
            (flag_sos == 1) & (valor_sos != val_cred),
            val_cred - valor_sos  # Desconta o SOS do credito
        )
        .otherwise(val_real)  # Sem SOS, mantem valor original
    )

    # Etapa 2: Ajustar por bonus (bonus nao e dinheiro real)
    # Para transacoes COMBO ou BONUS_PURO, desconta o bonus
    df = df.withColumn(
        "val_real_ajustado_final",
        F.when(
            F.col("tipo_transacao").isin(["COMBO_PAGO_BONUS", "BONUS_PURO"]),
            F.col("val_real_ajustado_sos") - val_bonus
        ).otherwise(
            F.col("val_real_ajustado_sos")
        )
    )

    # Garantir nao negativo para calculos (manter original para analise)
    df = df.withColumn(
        "val_real_ajustado_clean",
        F.when(F.col("val_real_ajustado_final") > 0, F.col("val_real_ajustado_final"))
         .otherwise(0.0)
    )

    # -------------------------------------------------------------------------
    # 4. COLUNAS AUXILIARES PARA METRICAS TEMPORAIS
    # -------------------------------------------------------------------------
    print(">>> [Prep] Calculando metricas de tempo entre recargas...")

    # Window para calcular tempo entre recargas (por CPF, ordenado por timestamp)
    window_tempo = Window.partitionBy("num_cpf").orderBy("ts_recarga")

    # Timestamp da recarga anterior
    df = df.withColumn(
        "ts_recarga_anterior",
        F.lag("ts_recarga", 1).over(window_tempo)
    )

    # Dias desde a recarga anterior (apenas entre recargas validas)
    df = df.withColumn(
        "dias_desde_recarga_anterior",
        F.when(
            F.col("ts_recarga_anterior").isNotNull() & F.col("ts_recarga").isNotNull(),
            F.datediff(F.col("ts_recarga"), F.col("ts_recarga_anterior"))
        ).otherwise(None)
    )

    # Horas desde a recarga anterior (mais granular)
    df = df.withColumn(
        "horas_desde_recarga_anterior",
        F.when(
            F.col("ts_recarga_anterior").isNotNull() & F.col("ts_recarga").isNotNull(),
            (F.unix_timestamp("ts_recarga") - F.unix_timestamp("ts_recarga_anterior")) / 3600
        ).otherwise(None)
    )

    # -------------------------------------------------------------------------
    # 5. FEATURES DE HORARIO/PERIODO (para analise comportamental)
    # -------------------------------------------------------------------------
    print(">>> [Prep] Extraindo features de horario e periodo...")

    # Extrair hora do dia (se disponivel)
    df = df.withColumn(
        "hora_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.hour("ts_recarga"))
         .otherwise(None)
    )

    # Classificar periodo do dia
    df = df.withColumn(
        "periodo_dia",
        F.when(F.col("hora_recarga").between(6, 11), "MANHA")
         .when(F.col("hora_recarga").between(12, 17), "TARDE")
         .when(F.col("hora_recarga").between(18, 23), "NOITE")
         .otherwise("MADRUGADA")
    )

    # Dia da semana (1=Domingo, 7=Sabado)
    df = df.withColumn(
        "dia_semana",
        F.when(F.col("dt_recarga").isNotNull(), F.dayofweek("dt_recarga"))
         .otherwise(None)
    )

    # Flag fim de semana
    df = df.withColumn(
        "flag_fim_semana",
        F.when(F.col("dia_semana").isin(1, 7), 1).otherwise(0)
    )

    # Semana do ano (para agregacao semanal)
    df = df.withColumn(
        "semana_ano",
        F.when(F.col("dt_recarga").isNotNull(), F.weekofyear("dt_recarga"))
         .otherwise(None)
    )

    print(f">>> [Prep] Preparacao concluida. Colunas: {len(df.columns)}")

    return df


def agregar_por_janela_temporal(
    df_prep: DataFrame,
    safra_referencia: str,
    janela: str,
    num_meses: int
) -> DataFrame:
    """
    Agrega eventos de recarga para uma janela temporal especifica.

    Args:
        df_prep: DataFrame preparado com eventos de recarga
        safra_referencia: SAFRA de referencia (YYYYMM) do cliente no spine
        janela: Nome da janela (m1, m3, m6)
        num_meses: Numero de meses de lookback

    Returns:
        DataFrame agregado com features para a janela especificada
    """
    print(f">>> [Agg] Agregando janela {janela.upper()} ({num_meses} mes(es) de lookback)...")

    # Sufixo para nomes de colunas
    sfx = f"_{janela}"

    # Agregar por NUM_CPF + SAFRA_RECARGA
    # Nota: Na Gold final, isso sera filtrado pela janela temporal relativa ao spine

    # -------------------------------------------------------------------------
    # AGREGACOES BASICAS
    # -------------------------------------------------------------------------
    df_agg = df_prep.groupBy("num_cpf", "safra_recarga").agg(

        # === VOLUME ===
        F.count("*").alias(f"qtd_recargas{sfx}"),
        F.sum("flag_recarga_valida").alias(f"qtd_recargas_validas{sfx}"),
        F.countDistinct("dw_num_ntc").alias(f"qtd_telefones_distintos{sfx}"),

        # === VALORES BRUTOS ===
        F.sum(F.coalesce(F.col("val_credito_inserido_clean"), F.col("val_credito_inserido"), F.lit(0.0)))
            .alias(f"sum_val_credito{sfx}"),
        F.sum(F.coalesce(F.col("val_bonus_clean"), F.col("val_bonus"), F.lit(0.0)))
            .alias(f"sum_val_bonus{sfx}"),
        F.sum(F.coalesce(F.col("val_real_clean"), F.col("val_real"), F.lit(0.0)))
            .alias(f"sum_val_real{sfx}"),

        # === VALORES AJUSTADOS (apos SOS e bonus) ===
        F.sum("val_real_ajustado_clean").alias(f"sum_val_real_ajustado{sfx}"),

        # === ESTATISTICAS DE VALOR ===
        F.avg(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias(f"avg_val_real{sfx}"),
        F.min(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias(f"min_val_real{sfx}"),
        F.max(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias(f"max_val_real{sfx}"),
        F.stddev(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias(f"std_val_real{sfx}"),

        # === SOS (indicador de estresse financeiro) ===
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias(f"qtd_sos{sfx}"),
        F.sum(F.when(F.col("flag_sos") == 1, F.col("valor_sos")).otherwise(0)).alias(f"sum_valor_sos{sfx}"),
        F.max(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias(f"flag_teve_sos{sfx}"),

        # === TEMPO ENTRE RECARGAS ===
        F.avg("dias_desde_recarga_anterior").alias(f"dias_medio_entre_recargas{sfx}"),
        F.min(F.when(F.col("dias_desde_recarga_anterior") > 0, F.col("dias_desde_recarga_anterior")))
            .alias(f"dias_min_entre_recargas{sfx}"),
        F.max("dias_desde_recarga_anterior").alias(f"dias_max_entre_recargas{sfx}"),
        F.stddev("dias_desde_recarga_anterior").alias(f"std_dias_entre_recargas{sfx}"),

        # === RECENCIA ===
        F.max("dt_recarga").alias(f"dt_ultima_recarga{sfx}"),
        F.min("dt_recarga").alias(f"dt_primeira_recarga{sfx}"),

        # === PADROES DE HORARIO ===
        F.sum(F.when(F.col("periodo_dia") == "MADRUGADA", 1).otherwise(0)).alias(f"qtd_recargas_madrugada{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "MANHA", 1).otherwise(0)).alias(f"qtd_recargas_manha{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "TARDE", 1).otherwise(0)).alias(f"qtd_recargas_tarde{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "NOITE", 1).otherwise(0)).alias(f"qtd_recargas_noite{sfx}"),
        F.sum("flag_fim_semana").alias(f"qtd_recargas_fim_semana{sfx}"),

        # === SEMANAS ATIVAS ===
        F.countDistinct("semana_ano").alias(f"qtd_semanas_com_recarga{sfx}"),

        # === TIPOS DE TRANSACAO ===
        F.sum(F.when(F.col("tipo_transacao") == "PAGO_PURO", 1).otherwise(0)).alias(f"qtd_pago_puro{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "BONUS_PURO", 1).otherwise(0)).alias(f"qtd_bonus_puro{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "COMBO_PAGO_BONUS", 1).otherwise(0)).alias(f"qtd_combo{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "VALOR_NEGATIVO", 1).otherwise(0)).alias(f"qtd_valor_negativo{sfx}"),

        # === DIMENSOES (se disponiveis) ===
        F.countDistinct(F.when(F.col("cod_tipo_credito") >= 0, F.col("cod_tipo_credito")))
            .alias(f"qtd_tipos_credito_distintos{sfx}"),
        F.countDistinct(F.when(F.col("cod_status_plataforma") >= 0, F.col("cod_status_plataforma")))
            .alias(f"qtd_status_plataforma_distintos{sfx}"),
    )

    # -------------------------------------------------------------------------
    # FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(f">>> [Agg] Criando features derivadas para {janela.upper()}...")

    # Ticket medio (ja temos avg, mas reforcando)
    df_agg = df_agg.withColumn(
        f"ticket_medio{sfx}",
        F.when(
            F.col(f"qtd_recargas_validas{sfx}") > 0,
            F.round(F.col(f"sum_val_real_ajustado{sfx}") / F.col(f"qtd_recargas_validas{sfx}"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre credito (indicador de estresse)
    df_agg = df_agg.withColumn(
        f"pct_sos_sobre_credito{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round((F.col(f"sum_valor_sos{sfx}") / F.col(f"sum_val_credito{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequencia de SOS (qtd_sos / qtd_recargas)
    df_agg = df_agg.withColumn(
        f"freq_sos{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round(F.col(f"qtd_sos{sfx}") / F.col(f"qtd_recargas{sfx}"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variacao do valor (estabilidade financeira)
    df_agg = df_agg.withColumn(
        f"coef_variacao_val{sfx}",
        F.when(
            (F.col(f"avg_val_real{sfx}").isNotNull()) & (F.col(f"avg_val_real{sfx}") > 0),
            F.round(F.col(f"std_val_real{sfx}") / F.col(f"avg_val_real{sfx}"), 4)
        ).otherwise(None)
    )

    # Razao max/min (amplitude de valores)
    df_agg = df_agg.withColumn(
        f"ratio_max_min_val{sfx}",
        F.when(
            (F.col(f"min_val_real{sfx}").isNotNull()) & (F.col(f"min_val_real{sfx}") > 0),
            F.round(F.col(f"max_val_real{sfx}") / F.col(f"min_val_real{sfx}"), 2)
        ).otherwise(None)
    )

    # Razao bonus/credito
    df_agg = df_agg.withColumn(
        f"ratio_bonus_credito{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round(F.col(f"sum_val_bonus{sfx}") / F.col(f"sum_val_credito{sfx}"), 4)
        ).otherwise(0.0)
    )

    # Recargas por semana (media)
    # Nota: num_meses * 4.33 semanas aproximadas
    semanas_periodo = num_meses * 4.33
    df_agg = df_agg.withColumn(
        f"recargas_por_semana{sfx}",
        F.round(F.col(f"qtd_recargas_validas{sfx}") / semanas_periodo, 2)
    )

    # Percentual de semanas com recarga
    semanas_max = int(num_meses * 4.33)
    df_agg = df_agg.withColumn(
        f"pct_semanas_com_recarga{sfx}",
        F.when(
            F.col(f"qtd_semanas_com_recarga{sfx}") > 0,
            F.round((F.col(f"qtd_semanas_com_recarga{sfx}") / semanas_max) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual de recargas no fim de semana
    df_agg = df_agg.withColumn(
        f"pct_recargas_fim_semana{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_fim_semana{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual de recargas na madrugada (comportamento atipico)
    df_agg = df_agg.withColumn(
        f"pct_recargas_madrugada{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_madrugada{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Valor liquido (credito - SOS)
    df_agg = df_agg.withColumn(
        f"val_liquido{sfx}",
        F.col(f"sum_val_credito{sfx}") - F.col(f"sum_valor_sos{sfx}")
    )

    # Percentual de transacoes validas
    df_agg = df_agg.withColumn(
        f"pct_transacoes_validas{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_validas{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # -------------------------------------------------------------------------
    # FLAGS DE COBERTURA
    # -------------------------------------------------------------------------

    # Flag sem recarga no periodo
    df_agg = df_agg.withColumn(
        f"flag_sem_recarga{sfx}",
        F.when(F.col(f"qtd_recargas{sfx}") == 0, 1).otherwise(0)
    )

    # Flag baixa atividade
    limiar = LIMIAR_BAIXA_ATIVIDADE.get(janela, 2)
    df_agg = df_agg.withColumn(
        f"flag_baixa_atividade{sfx}",
        F.when(F.col(f"qtd_recargas_validas{sfx}") < limiar, 1).otherwise(0)
    )

    return df_agg


def criar_features_recarga_completas(df_silver: DataFrame) -> DataFrame:
    """
    Pipeline principal: gera todas as features de Recarga agregadas por SAFRA_RECARGA.

    Este script gera features agregadas por NUM_CPF + SAFRA_RECARGA.
    O join com o spine (ABT) sera feito posteriormente, filtrando por janela temporal.

    Para uso direto com ABT, as features aqui geradas podem ser filtradas por:
    - SAFRA_RECARGA < SAFRA (do spine) para anti-leakage
    - Janelas M1/M3/M6 baseadas na diferenca de meses

    Args:
        df_silver: DataFrame Silver Recarga (event-level)

    Returns:
        DataFrame Gold com features por NUM_CPF + SAFRA_RECARGA
    """
    print("\n" + "="*80)
    print("GOLD RECARGA FEATURES - PIPELINE PRINCIPAL")
    print("="*80 + "\n")

    # -------------------------------------------------------------------------
    # ETAPA 1: PREPARACAO
    # -------------------------------------------------------------------------
    df_prep = preparar_recarga_para_agregacao(df_silver)

    # -------------------------------------------------------------------------
    # ETAPA 2: AGREGACAO POR SAFRA_RECARGA (mensal)
    # -------------------------------------------------------------------------
    # Gera uma versao agregada por mes (SAFRA_RECARGA)
    # As janelas temporais (M1, M3, M6) serao aplicadas no join com o spine

    print(">>> [Agg] Agregando por NUM_CPF + SAFRA_RECARGA (mensal)...")

    df_gold = df_prep.groupBy("num_cpf", "safra_recarga", "dt_recarga_safra").agg(

        # === VOLUME ===
        F.count("*").alias("qtd_recargas_mes"),
        F.sum("flag_recarga_valida").alias("qtd_recargas_validas_mes"),
        F.countDistinct("dw_num_ntc").alias("qtd_telefones_distintos_mes"),

        # === VALORES BRUTOS ===
        F.sum(F.coalesce(F.col("val_credito_inserido_clean"), F.col("val_credito_inserido"), F.lit(0.0)))
            .alias("sum_val_credito_mes"),
        F.sum(F.coalesce(F.col("val_bonus_clean"), F.col("val_bonus"), F.lit(0.0)))
            .alias("sum_val_bonus_mes"),
        F.sum(F.coalesce(F.col("val_real_clean"), F.col("val_real"), F.lit(0.0)))
            .alias("sum_val_real_mes"),

        # === VALORES AJUSTADOS ===
        F.sum("val_real_ajustado_clean").alias("sum_val_real_ajustado_mes"),

        # === ESTATISTICAS DE VALOR ===
        F.avg(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias("avg_val_real_mes"),
        F.min(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias("min_val_real_mes"),
        F.max(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias("max_val_real_mes"),
        F.stddev(F.when(F.col("flag_recarga_valida") == 1, F.col("val_real_ajustado_clean")))
            .alias("std_val_real_mes"),

        # === SOS ===
        F.sum(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("qtd_sos_mes"),
        F.sum(F.when(F.col("flag_sos") == 1, F.col("valor_sos")).otherwise(0)).alias("sum_valor_sos_mes"),
        F.max(F.when(F.col("flag_sos") == 1, 1).otherwise(0)).alias("flag_teve_sos_mes"),

        # === TEMPO ENTRE RECARGAS ===
        F.avg("dias_desde_recarga_anterior").alias("dias_medio_entre_recargas_mes"),
        F.min(F.when(F.col("dias_desde_recarga_anterior") > 0, F.col("dias_desde_recarga_anterior")))
            .alias("dias_min_entre_recargas_mes"),
        F.max("dias_desde_recarga_anterior").alias("dias_max_entre_recargas_mes"),
        F.stddev("dias_desde_recarga_anterior").alias("std_dias_entre_recargas_mes"),

        # === RECENCIA ===
        F.max("dt_recarga").alias("dt_ultima_recarga_mes"),
        F.min("dt_recarga").alias("dt_primeira_recarga_mes"),

        # === PADROES DE HORARIO ===
        F.sum(F.when(F.col("periodo_dia") == "MADRUGADA", 1).otherwise(0)).alias("qtd_recargas_madrugada_mes"),
        F.sum(F.when(F.col("periodo_dia") == "MANHA", 1).otherwise(0)).alias("qtd_recargas_manha_mes"),
        F.sum(F.when(F.col("periodo_dia") == "TARDE", 1).otherwise(0)).alias("qtd_recargas_tarde_mes"),
        F.sum(F.when(F.col("periodo_dia") == "NOITE", 1).otherwise(0)).alias("qtd_recargas_noite_mes"),
        F.sum("flag_fim_semana").alias("qtd_recargas_fim_semana_mes"),

        # === SEMANAS ATIVAS ===
        F.countDistinct("semana_ano").alias("qtd_semanas_com_recarga_mes"),

        # === TIPOS DE TRANSACAO ===
        F.sum(F.when(F.col("tipo_transacao") == "PAGO_PURO", 1).otherwise(0)).alias("qtd_pago_puro_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "BONUS_PURO", 1).otherwise(0)).alias("qtd_bonus_puro_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "COMBO_PAGO_BONUS", 1).otherwise(0)).alias("qtd_combo_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "VALOR_NEGATIVO", 1).otherwise(0)).alias("qtd_valor_negativo_mes"),

        # === DIMENSOES ===
        F.countDistinct(F.when(F.col("cod_tipo_credito") >= 0, F.col("cod_tipo_credito")))
            .alias("qtd_tipos_credito_distintos_mes"),
        F.countDistinct(F.when(F.col("cod_status_plataforma") >= 0, F.col("cod_status_plataforma")))
            .alias("qtd_status_plataforma_distintos_mes"),
    )

    # -------------------------------------------------------------------------
    # ETAPA 3: FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(">>> [Agg] Criando features derivadas mensais...")

    # Ticket medio
    df_gold = df_gold.withColumn(
        "ticket_medio_mes",
        F.when(
            F.col("qtd_recargas_validas_mes") > 0,
            F.round(F.col("sum_val_real_ajustado_mes") / F.col("qtd_recargas_validas_mes"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre credito
    df_gold = df_gold.withColumn(
        "pct_sos_sobre_credito_mes",
        F.when(
            F.col("sum_val_credito_mes") > 0,
            F.round((F.col("sum_valor_sos_mes") / F.col("sum_val_credito_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequencia de SOS
    df_gold = df_gold.withColumn(
        "freq_sos_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round(F.col("qtd_sos_mes") / F.col("qtd_recargas_mes"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variacao
    df_gold = df_gold.withColumn(
        "coef_variacao_val_mes",
        F.when(
            (F.col("avg_val_real_mes").isNotNull()) & (F.col("avg_val_real_mes") > 0),
            F.round(F.col("std_val_real_mes") / F.col("avg_val_real_mes"), 4)
        ).otherwise(None)
    )

    # Razao max/min
    df_gold = df_gold.withColumn(
        "ratio_max_min_val_mes",
        F.when(
            (F.col("min_val_real_mes").isNotNull()) & (F.col("min_val_real_mes") > 0),
            F.round(F.col("max_val_real_mes") / F.col("min_val_real_mes"), 2)
        ).otherwise(None)
    )

    # Razao bonus/credito
    df_gold = df_gold.withColumn(
        "ratio_bonus_credito_mes",
        F.when(
            F.col("sum_val_credito_mes") > 0,
            F.round(F.col("sum_val_bonus_mes") / F.col("sum_val_credito_mes"), 4)
        ).otherwise(0.0)
    )

    # Recargas por semana
    df_gold = df_gold.withColumn(
        "recargas_por_semana_mes",
        F.round(F.col("qtd_recargas_validas_mes") / 4.33, 2)
    )

    # Percentual de semanas com recarga
    df_gold = df_gold.withColumn(
        "pct_semanas_com_recarga_mes",
        F.when(
            F.col("qtd_semanas_com_recarga_mes") > 0,
            F.round((F.col("qtd_semanas_com_recarga_mes") / 4.33) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual recargas fim de semana
    df_gold = df_gold.withColumn(
        "pct_recargas_fim_semana_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round((F.col("qtd_recargas_fim_semana_mes") / F.col("qtd_recargas_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Percentual recargas madrugada
    df_gold = df_gold.withColumn(
        "pct_recargas_madrugada_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round((F.col("qtd_recargas_madrugada_mes") / F.col("qtd_recargas_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Valor liquido
    df_gold = df_gold.withColumn(
        "val_liquido_mes",
        F.col("sum_val_credito_mes") - F.col("sum_valor_sos_mes")
    )

    # Percentual transacoes validas
    df_gold = df_gold.withColumn(
        "pct_transacoes_validas_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round((F.col("qtd_recargas_validas_mes") / F.col("qtd_recargas_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # -------------------------------------------------------------------------
    # ETAPA 4: FLAGS DE COBERTURA
    # -------------------------------------------------------------------------

    # Flag sem recarga
    df_gold = df_gold.withColumn(
        "flag_sem_recarga_mes",
        F.when(F.col("qtd_recargas_mes") == 0, 1).otherwise(0)
    )

    # Flag baixa atividade (menos de 2 recargas/mes)
    df_gold = df_gold.withColumn(
        "flag_baixa_atividade_mes",
        F.when(F.col("qtd_recargas_validas_mes") < 2, 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # ETAPA 5: METADADOS
    # -------------------------------------------------------------------------
    df_gold = df_gold.withColumn("gold_version", F.lit(GOLD_VERSION))
    df_gold = df_gold.withColumn("gold_build_date", F.current_timestamp())

    return df_gold


def gerar_relatorio_qualidade(df_gold: DataFrame, count_silver: int) -> None:
    """
    Gera relatorio de qualidade das features Gold de Recarga.
    """
    print("\n" + "="*80)
    print("RELATORIO DE QUALIDADE - GOLD RECARGA FEATURES")
    print("="*80 + "\n")

    count_gold = df_gold.count()

    print(f">>> [Stats] Volumetria:")
    print(f"    Silver (eventos): {count_silver:>15,}")
    print(f"    Gold (cliente-mes): {count_gold:>15,}")
    print(f"    Compressao: {count_silver/count_gold:.1f}x")

    # Distribuicao de SAFRAs
    print(f"\n>>> [Stats] Distribuicao de SAFRAs:")
    safras = df_gold.groupBy("safra_recarga").count().orderBy("safra_recarga").collect()
    for row in safras[:6]:  # Mostrar apenas 6 primeiras
        print(f"    {row['safra_recarga']}: {row['count']:>10,}")
    if len(safras) > 6:
        print(f"    ... ({len(safras) - 6} safras adicionais)")

    # Estatisticas de features
    stats = df_gold.select(
        F.mean("qtd_recargas_mes").alias("mean_qtd"),
        F.mean("sum_val_real_ajustado_mes").alias("mean_val"),
        F.mean("ticket_medio_mes").alias("mean_ticket"),
        F.sum("flag_teve_sos_mes").alias("total_com_sos"),
        F.mean("dias_medio_entre_recargas_mes").alias("mean_dias_entre"),
    ).collect()[0]

    print(f"\n>>> [Stats] Features principais:")
    print(f"    QTD_RECARGAS_MES (media): {stats['mean_qtd']:.2f}")
    print(f"    SUM_VAL_REAL_AJUSTADO_MES (media): R$ {stats['mean_val']:.2f}")
    print(f"    TICKET_MEDIO_MES (media): R$ {stats['mean_ticket']:.2f}")
    print(f"    Registros com SOS: {stats['total_com_sos']:,} ({100*stats['total_com_sos']/count_gold:.2f}%)")
    print(f"    DIAS_MEDIO_ENTRE_RECARGAS (media): {stats['mean_dias_entre']:.1f} dias")

    # Distribuicao de cobertura
    print(f"\n>>> [Stats] Cobertura de features:")
    cobertura = df_gold.select(
        F.sum(F.when(F.col("flag_sem_recarga_mes") == 1, 1).otherwise(0)).alias("sem_recarga"),
        F.sum(F.when(F.col("flag_baixa_atividade_mes") == 1, 1).otherwise(0)).alias("baixa_ativ"),
        F.sum(F.when(F.col("flag_teve_sos_mes") == 1, 1).otherwise(0)).alias("com_sos"),
    ).collect()[0]

    print(f"    Sem recarga: {cobertura['sem_recarga']:,} ({100*cobertura['sem_recarga']/count_gold:.2f}%)")
    print(f"    Baixa atividade: {cobertura['baixa_ativ']:,} ({100*cobertura['baixa_ativ']/count_gold:.2f}%)")
    print(f"    Com SOS: {cobertura['com_sos']:,} ({100*cobertura['com_sos']/count_gold:.2f}%)")

    # Schema
    print(f"\n>>> [Schema] Total de colunas: {len(df_gold.columns)}")
    print("    Colunas de features:")
    feature_cols = [c for c in df_gold.columns if c not in ["num_cpf", "safra_recarga", "dt_recarga_safra", "gold_version", "gold_build_date"]]
    for i, col in enumerate(feature_cols[:20]):
        print(f"      {i+1:2d}. {col}")
    if len(feature_cols) > 20:
        print(f"      ... ({len(feature_cols) - 20} features adicionais)")


def main():
    """
    Pipeline principal para geracao de Gold Recarga Features.
    """
    parser = argparse.ArgumentParser(
        description="Gerar Gold Recarga Features v2 - Features comportamentais para modelagem de risco",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Modo padrao (caminhos default)
  python gold_recarga.py

  # Com caminhos customizados
  python gold_recarga.py \\
      --input_path oci://hackathon-2025-silver-layer@namespace/recarga/ \\
      --output_path oci://hackathon-2025-gold-layer@namespace/recarga_features_v2/
        """
    )

    parser.add_argument(
        "--input_path",
        default=DEFAULT_SILVER_RECARGA_PATH,
        help="Caminho da Silver Recarga (Delta)"
    )
    parser.add_argument(
        "--output_path",
        default=DEFAULT_OUTPUT_PATH,
        help="Caminho de destino das features Gold (Delta)"
    )
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT,
        help="Formato de leitura/escrita (default: delta)"
    )
    parser.add_argument(
        "--skip_save",
        action="store_true",
        help="Pular salvamento (para debug)"
    )

    # Use parse_known_args() to ignore Databricks/Jupyter kernel arguments
    args, unknown = parser.parse_known_args()

    if unknown:
        print(f">>> [Config] Ignorando argumentos nao reconhecidos (Databricks/Jupyter): {unknown[:2]}...")

    # Banner
    print("\n")
    print("=" * 78)
    print("GOLD RECARGA FEATURES V2 - OCI Data Flow".center(78))
    print("Features Comportamentais para Modelagem de Risco de Credito".center(78))
    print("=" * 78)
    print("\n")

    # Inicializar Spark
    spark = SparkSession.builder.appName("gold_recarga_original").getOrCreate()

    # =========================================================================
    # 1) LEITURA SILVER RECARGA
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Recarga: {args.input_path}")

    try:
        df_silver = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA: {e}")
        sys.exit(1)

    count_silver = df_silver.count()
    print(f">>> [Info] Registros na Silver: {count_silver:,}")
    print(f">>> [Info] Colunas disponiveis: {len(df_silver.columns)}")

    # =========================================================================
    # 2) PROCESSAMENTO
    # =========================================================================
    df_gold = criar_features_recarga_completas(df_silver)

    count_gold = df_gold.count()
    print(f">>> [Info] Registros no Gold: {count_gold:,}")
    print(f">>> [Info] Colunas geradas: {len(df_gold.columns)}")

    # =========================================================================
    # 3) RELATORIO DE QUALIDADE
    # =========================================================================
    gerar_relatorio_qualidade(df_gold, count_silver)

    # =========================================================================
    # 4) ESCRITA
    # =========================================================================
    if not args.skip_save:
        print(f"\n>>> [Escrita] Salvando Gold Recarga Features (Delta): {args.output_path}")

        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("safra_recarga") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(args.output_path)

        print(f">>> [Sucesso] Dados salvos em: {args.output_path}")
    else:
        print("\n>>> [Skip] Salvamento pulado (--skip_save)")

    # =========================================================================
    # 5) RESUMO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("PROCESSAMENTO CONCLUIDO COM SUCESSO!")
    print("="*80)
    print(f"""
    Resumo:
    +-- Silver (eventos):     {count_silver:>15,} registros
    +-- Gold (cliente-mes):   {count_gold:>15,} registros
    +-- Compressao:           {count_silver/count_gold:>15.1f}x
    +-- Features geradas:     {len(df_gold.columns) - 5:>15} (excluindo metadados)
    +-- Output:               {args.output_path}

    Proximos passos:
    1. JOIN com spine (ABT v4) por (NUM_CPF, SAFRA)
    2. Filtrar por janela temporal (SAFRA_RECARGA < SAFRA)
    3. Agregar M1/M3/M6 usando dt_recarga_safra vs dt_safra
    4. Validar cobertura e gates de qualidade
    """)
    print("="*80 + "\n")

    return df_gold


if __name__ == "__main__":
    df_gold = main()
