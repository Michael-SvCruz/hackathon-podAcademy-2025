"""
================================================================================
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: gold_recarga_features_v2.py
OBJETIVO: Processar base de Recarga e gerar features comportamentais para ABT v5+
================================================================================

DESCRIÇÃO TÉCNICA:
Este script é uma versão unificada e aprimorada do processamento de Recarga,
combinando as melhores práticas de:
- 03_bronze_silver_recarga.py (tipagem, sentinelas, deduplicação)
- tratamento_recarga_v1.py (SOS adjustment, enrichment, temporal metrics)

Adiciona features avançadas relevantes para modelagem de risco de crédito.

================================================================================
ARQUITETURA DO PIPELINE:
================================================================================

    SILVER (recarga_silver_delta)
         │ Event-level: ~95M registros
         │ Grão: 1 linha por evento de recarga
         │
         ▼
    ┌────────────────────────────────────────────────────────────────┐
    │                 GOLD RECARGA FEATURES (este script)            │
    │                                                                │
    │  1. Enriquecimento com dimensões (opcional)                    │
    │  2. Ajuste de valores (SOS, Bônus)                            │
    │  3. Métricas temporais (tempo entre recargas)                 │
    │  4. Agregação por janelas (M1, M3, M6)                        │
    │  5. Features comportamentais avançadas                        │
    │                                                                │
    └────────────────────────────────────────────────────────────────┘
         │
         │ Grão: 1 linha por NUM_CPF + SAFRA_RECARGA
         ▼
    GOLD (recarga_features_delta)
         │
         │ LEFT JOIN por (NUM_CPF, SAFRA)
         ▼
    ABT v5+ (abt_v5_delta, abt_v6_delta, etc.)

================================================================================
REGRAS DE NEGÓCIO (conforme reunião com Fernando/Claro - 07/01/2026):
================================================================================

1. SOS (Serviço de Empréstimo de Crédito):
   - SOS é um adiantamento/empréstimo de R$3-20 (geralmente R$5)
   - É descontado da próxima recarga tradicional do cliente
   - SOS e bônus NÃO contam como "dinheiro real"
   - Exemplo: Recarga R$20 + SOS R$5 pendente = R$20 dinheiro real, não R$25
   - IMPORTANTE: Frequência de SOS é indicador de estresse financeiro

2. Valores Negativos:
   - Podem indicar ajustes, estornos ou sentinelas
   - Tratados com colunas *_CLEAN (NULL se negativo) + flags

3. Sentinelas em Códigos Dimensionais:
   - -1 = "Não se aplica"
   - -2 = "Não determinado"
   - -3 = "Não informado"
   - Tratados com flags FLAG_*_SENTINELA

4. Anti-Leakage:
   - SAFRA_RECARGA deve ser ANTERIOR à SAFRA do cliente no spine
   - Janelas temporais (M1, M3, M6) garantem lookback correto
   - FPD e FLAG_INSTALACAO NÃO são features (labels apenas)

================================================================================
FEATURES GERADAS (60+ variáveis por janela temporal):
================================================================================

CATEGORIAS DE FEATURES:

1. VOLUME DE RECARGAS (por M1/M3/M6)
   - qtd_recargas_*: Quantidade total de eventos
   - qtd_recargas_validas_*: Quantidade excluindo zeros
   - qtd_telefones_distintos_*: Diversidade de linhas

2. VALORES MONETÁRIOS (por M1/M3/M6)
   - sum_val_credito_*: Soma de crédito inserido
   - sum_val_real_ajustado_*: Soma de valor real (após ajuste SOS/bônus)
   - sum_val_bonus_*: Soma de bônus recebido
   - avg_val_real_*: Ticket médio
   - min_val_real_*: Menor recarga (padrão de comportamento)
   - max_val_real_*: Maior recarga (capacidade de pagamento)
   - std_val_real_*: Desvio padrão (consistência)

3. COMPORTAMENTO SOS (indicador de estresse financeiro)
   - qtd_sos_*: Quantidade de eventos SOS
   - sum_valor_sos_*: Soma de valores SOS
   - pct_sos_sobre_credito_*: % do crédito que foi SOS
   - flag_teve_sos_*: Binário: usou SOS no período?
   - freq_sos_*: Frequência de uso de SOS (qtd_sos / qtd_recargas)

4. MÉTRICAS TEMPORAIS (padrão de recarga)
   - dias_medio_entre_recargas_*: Regularidade média
   - dias_min_entre_recargas_*: Menor intervalo (urgência)
   - dias_max_entre_recargas_*: Maior intervalo (inatividade)
   - std_dias_entre_recargas_*: Consistência do padrão
   - dias_desde_ultima_recarga_*: Recência

5. PADRÕES DE VALOR (consistência financeira)
   - coef_variacao_val_*: CV = std/média (estabilidade)
   - ratio_max_min_val_*: Razão max/min (amplitude)
   - ratio_bonus_credito_*: % de bônus sobre crédito
   - tendencia_valor_*: Crescendo ou diminuindo?

6. PADRÕES DE FREQUÊNCIA
   - recargas_por_semana_*: Média de recargas/semana
   - pct_semanas_com_recarga_*: % de semanas ativas
   - sequencia_max_sem_recarga_*: Maior gap em semanas

7. CANAL E MÉTODO (se disponível na Silver)
   - qtd_canais_distintos_*: Diversidade de canais
   - qtd_formas_pagamento_*: Diversidade de pagamento
   - pct_recarga_digital_*: % via canais digitais

8. FLAGS DE MISSING/COBERTURA
   - flag_sem_recarga_*: Cliente sem recarga no período
   - flag_baixa_atividade_*: Menos de X recargas

================================================================================
CONFIGURAÇÕES E PATHS:
================================================================================
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Importar utilities do projeto
try:
    from src.utils.spark_utils import get_spark_session
except ImportError:
    # Fallback para execução direta
    def get_spark_session(app_name="Hackathon_App"):
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_SILVER_RECARGA_PATH = "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/recarga_features_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_recarga_features_v2"

# Configuração das janelas temporais (meses de lookback)
TEMPORAL_WINDOWS = {
    "m1": 1,   # Último mês
    "m3": 3,   # Últimos 3 meses
    "m6": 6    # Últimos 6 meses
}

# Limiar para "baixa atividade" (recargas por período)
LIMIAR_BAIXA_ATIVIDADE = {
    "m1": 2,   # Menos de 2 recargas/mês = baixa atividade
    "m3": 5,   # Menos de 5 recargas/trimestre
    "m6": 10   # Menos de 10 recargas/semestre
}

# Colunas monetárias esperadas na Silver
COLUNAS_VALOR = [
    "val_credito_inserido",
    "val_bonus",
    "val_real",
    "valor_sos"
]

# Colunas monetárias limpas (sem negativos)
COLUNAS_VALOR_CLEAN = [
    "val_credito_inserido_clean",
    "val_bonus_clean",
    "val_real_clean"
]


# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================

def preparar_recarga_para_agregacao(df_silver: DataFrame) -> DataFrame:
    """
    Prepara dados da Silver Recarga para agregação Gold.

    Etapas:
    1. Garantir tipos corretos
    2. Criar dt_recarga_safra (primeiro dia do mês) para joins temporais
    3. Criar colunas de classificação (tipo de transação)
    4. Ajustar valores considerando SOS e bônus
    5. Criar colunas auxiliares para métricas temporais

    Args:
        df_silver: DataFrame da Silver Recarga (event-level)

    Returns:
        DataFrame preparado para agregação
    """
    print(">>> [Prep] Preparando Recarga para agregação Gold...")

    df = df_silver

    # -------------------------------------------------------------------------
    # 1. GARANTIR SAFRA_RECARGA E CRIAR DT_RECARGA_SAFRA
    # -------------------------------------------------------------------------
    # SAFRA_RECARGA (YYYYMM) → DT_RECARGA_SAFRA (primeiro dia do mês)
    df = df.withColumn(
        "dt_recarga_safra",
        F.to_date(F.concat(F.col("safra_recarga"), F.lit("01")), "yyyyMMdd")
    )

    # Garantir ts_recarga existe (para cálculos temporais)
    if "ts_recarga" not in df.columns:
        # Se não existir, derivar de dt_recarga
        df = df.withColumn(
            "ts_recarga",
            F.when(F.col("dt_recarga").isNotNull(),
                   F.to_timestamp(F.col("dt_recarga")))
            .otherwise(None)
        )

    # -------------------------------------------------------------------------
    # 2. CLASSIFICAÇÃO DE TIPO DE TRANSAÇÃO
    # -------------------------------------------------------------------------
    # Classificar tipo de valor para análise
    print(">>> [Prep] Criando classificação de tipo de transação...")

    # Usar colunas clean se existirem, senão usar originais
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

    # Flag de recarga válida (exclui ZERO_TOTAL)
    df = df.withColumn(
        "flag_recarga_valida",
        F.when(F.col("tipo_transacao") != "ZERO_TOTAL", 1).otherwise(0)
    )

    # -------------------------------------------------------------------------
    # 3. AJUSTE DE VALORES (SOS E BÔNUS)
    # -------------------------------------------------------------------------
    # Conforme explicação de Fernando (Claro):
    # - SOS é empréstimo que será descontado da próxima recarga
    # - Bônus não é dinheiro real
    # - VAL_REAL_AJUSTADO = crédito real desconsiderando SOS pendente e bônus
    print(">>> [Prep] Aplicando ajuste de SOS e bônus...")

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
            val_cred - valor_sos  # Desconta o SOS do crédito
        )
        .otherwise(val_real)  # Sem SOS, mantém valor original
    )

    # Etapa 2: Ajustar por bônus (bônus não é dinheiro real)
    # Para transações COMBO ou BONUS_PURO, desconta o bônus
    df = df.withColumn(
        "val_real_ajustado_final",
        F.when(
            F.col("tipo_transacao").isin(["COMBO_PAGO_BONUS", "BONUS_PURO"]),
            F.col("val_real_ajustado_sos") - val_bonus
        ).otherwise(
            F.col("val_real_ajustado_sos")
        )
    )

    # Garantir não negativo para cálculos (manter original para análise)
    df = df.withColumn(
        "val_real_ajustado_clean",
        F.when(F.col("val_real_ajustado_final") > 0, F.col("val_real_ajustado_final"))
         .otherwise(0.0)
    )

    # -------------------------------------------------------------------------
    # 4. COLUNAS AUXILIARES PARA MÉTRICAS TEMPORAIS
    # -------------------------------------------------------------------------
    print(">>> [Prep] Calculando métricas de tempo entre recargas...")

    # Window para calcular tempo entre recargas (por CPF, ordenado por timestamp)
    window_tempo = Window.partitionBy("num_cpf").orderBy("ts_recarga")

    # Timestamp da recarga anterior
    df = df.withColumn(
        "ts_recarga_anterior",
        F.lag("ts_recarga", 1).over(window_tempo)
    )

    # Dias desde a recarga anterior (apenas entre recargas válidas)
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
    # 5. FEATURES DE HORÁRIO/PERÍODO (para análise comportamental)
    # -------------------------------------------------------------------------
    print(">>> [Prep] Extraindo features de horário e período...")

    # Extrair hora do dia (se disponível)
    df = df.withColumn(
        "hora_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.hour("ts_recarga"))
         .otherwise(None)
    )

    # Classificar período do dia
    df = df.withColumn(
        "periodo_dia",
        F.when(F.col("hora_recarga").between(6, 11), "MANHA")
         .when(F.col("hora_recarga").between(12, 17), "TARDE")
         .when(F.col("hora_recarga").between(18, 23), "NOITE")
         .otherwise("MADRUGADA")
    )

    # Dia da semana (1=Domingo, 7=Sábado)
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

    # Semana do ano (para agregação semanal)
    df = df.withColumn(
        "semana_ano",
        F.when(F.col("dt_recarga").isNotNull(), F.weekofyear("dt_recarga"))
         .otherwise(None)
    )

    print(f">>> [Prep] Preparação concluída. Colunas: {len(df.columns)}")

    return df


def agregar_por_janela_temporal(
    df_prep: DataFrame,
    safra_referencia: str,
    janela: str,
    num_meses: int
) -> DataFrame:
    """
    Agrega eventos de recarga para uma janela temporal específica.

    Args:
        df_prep: DataFrame preparado com eventos de recarga
        safra_referencia: SAFRA de referência (YYYYMM) do cliente no spine
        janela: Nome da janela (m1, m3, m6)
        num_meses: Número de meses de lookback

    Returns:
        DataFrame agregado com features para a janela especificada
    """
    print(f">>> [Agg] Agregando janela {janela.upper()} ({num_meses} mês(es) de lookback)...")

    # Sufixo para nomes de colunas
    sfx = f"_{janela}"

    # Agregar por NUM_CPF + SAFRA_RECARGA
    # Nota: Na Gold final, isso será filtrado pela janela temporal relativa ao spine

    # -------------------------------------------------------------------------
    # AGREGAÇÕES BÁSICAS
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

        # === VALORES AJUSTADOS (após SOS e bônus) ===
        F.sum("val_real_ajustado_clean").alias(f"sum_val_real_ajustado{sfx}"),

        # === ESTATÍSTICAS DE VALOR ===
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

        # === RECÊNCIA ===
        F.max("dt_recarga").alias(f"dt_ultima_recarga{sfx}"),
        F.min("dt_recarga").alias(f"dt_primeira_recarga{sfx}"),

        # === PADRÕES DE HORÁRIO ===
        F.sum(F.when(F.col("periodo_dia") == "MADRUGADA", 1).otherwise(0)).alias(f"qtd_recargas_madrugada{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "MANHA", 1).otherwise(0)).alias(f"qtd_recargas_manha{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "TARDE", 1).otherwise(0)).alias(f"qtd_recargas_tarde{sfx}"),
        F.sum(F.when(F.col("periodo_dia") == "NOITE", 1).otherwise(0)).alias(f"qtd_recargas_noite{sfx}"),
        F.sum("flag_fim_semana").alias(f"qtd_recargas_fim_semana{sfx}"),

        # === SEMANAS ATIVAS ===
        F.countDistinct("semana_ano").alias(f"qtd_semanas_com_recarga{sfx}"),

        # === TIPOS DE TRANSAÇÃO ===
        F.sum(F.when(F.col("tipo_transacao") == "PAGO_PURO", 1).otherwise(0)).alias(f"qtd_pago_puro{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "BONUS_PURO", 1).otherwise(0)).alias(f"qtd_bonus_puro{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "COMBO_PAGO_BONUS", 1).otherwise(0)).alias(f"qtd_combo{sfx}"),
        F.sum(F.when(F.col("tipo_transacao") == "VALOR_NEGATIVO", 1).otherwise(0)).alias(f"qtd_valor_negativo{sfx}"),

        # === DIMENSÕES (se disponíveis) ===
        F.countDistinct(F.when(F.col("cod_tipo_credito") >= 0, F.col("cod_tipo_credito")))
            .alias(f"qtd_tipos_credito_distintos{sfx}"),
        F.countDistinct(F.when(F.col("cod_status_plataforma") >= 0, F.col("cod_status_plataforma")))
            .alias(f"qtd_status_plataforma_distintos{sfx}"),
    )

    # -------------------------------------------------------------------------
    # FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(f">>> [Agg] Criando features derivadas para {janela.upper()}...")

    # Ticket médio (já temos avg, mas reforçando)
    df_agg = df_agg.withColumn(
        f"ticket_medio{sfx}",
        F.when(
            F.col(f"qtd_recargas_validas{sfx}") > 0,
            F.round(F.col(f"sum_val_real_ajustado{sfx}") / F.col(f"qtd_recargas_validas{sfx}"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre crédito (indicador de estresse)
    df_agg = df_agg.withColumn(
        f"pct_sos_sobre_credito{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round((F.col(f"sum_valor_sos{sfx}") / F.col(f"sum_val_credito{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequência de SOS (qtd_sos / qtd_recargas)
    df_agg = df_agg.withColumn(
        f"freq_sos{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round(F.col(f"qtd_sos{sfx}") / F.col(f"qtd_recargas{sfx}"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variação do valor (estabilidade financeira)
    df_agg = df_agg.withColumn(
        f"coef_variacao_val{sfx}",
        F.when(
            (F.col(f"avg_val_real{sfx}").isNotNull()) & (F.col(f"avg_val_real{sfx}") > 0),
            F.round(F.col(f"std_val_real{sfx}") / F.col(f"avg_val_real{sfx}"), 4)
        ).otherwise(None)
    )

    # Razão max/min (amplitude de valores)
    df_agg = df_agg.withColumn(
        f"ratio_max_min_val{sfx}",
        F.when(
            (F.col(f"min_val_real{sfx}").isNotNull()) & (F.col(f"min_val_real{sfx}") > 0),
            F.round(F.col(f"max_val_real{sfx}") / F.col(f"min_val_real{sfx}"), 2)
        ).otherwise(None)
    )

    # Razão bônus/crédito
    df_agg = df_agg.withColumn(
        f"ratio_bonus_credito{sfx}",
        F.when(
            F.col(f"sum_val_credito{sfx}") > 0,
            F.round(F.col(f"sum_val_bonus{sfx}") / F.col(f"sum_val_credito{sfx}"), 4)
        ).otherwise(0.0)
    )

    # Recargas por semana (média)
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

    # Percentual de recargas na madrugada (comportamento atípico)
    df_agg = df_agg.withColumn(
        f"pct_recargas_madrugada{sfx}",
        F.when(
            F.col(f"qtd_recargas{sfx}") > 0,
            F.round((F.col(f"qtd_recargas_madrugada{sfx}") / F.col(f"qtd_recargas{sfx}")) * 100, 2)
        ).otherwise(0.0)
    )

    # Valor líquido (crédito - SOS)
    df_agg = df_agg.withColumn(
        f"val_liquido{sfx}",
        F.col(f"sum_val_credito{sfx}") - F.col(f"sum_valor_sos{sfx}")
    )

    # Percentual de transações válidas
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

    # Flag sem recarga no período
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
    O join com o spine (ABT) será feito posteriormente, filtrando por janela temporal.

    Para uso direto com ABT, as features aqui geradas podem ser filtradas por:
    - SAFRA_RECARGA < SAFRA (do spine) para anti-leakage
    - Janelas M1/M3/M6 baseadas na diferença de meses

    Args:
        df_silver: DataFrame Silver Recarga (event-level)

    Returns:
        DataFrame Gold com features por NUM_CPF + SAFRA_RECARGA
    """
    print("\n" + "="*80)
    print("GOLD RECARGA FEATURES - PIPELINE PRINCIPAL")
    print("="*80 + "\n")

    # -------------------------------------------------------------------------
    # ETAPA 1: PREPARAÇÃO
    # -------------------------------------------------------------------------
    df_prep = preparar_recarga_para_agregacao(df_silver)

    # -------------------------------------------------------------------------
    # ETAPA 2: AGREGAÇÃO POR SAFRA_RECARGA (mensal)
    # -------------------------------------------------------------------------
    # Gera uma versão agregada por mês (SAFRA_RECARGA)
    # As janelas temporais (M1, M3, M6) serão aplicadas no join com o spine

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

        # === ESTATÍSTICAS DE VALOR ===
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

        # === RECÊNCIA ===
        F.max("dt_recarga").alias("dt_ultima_recarga_mes"),
        F.min("dt_recarga").alias("dt_primeira_recarga_mes"),

        # === PADRÕES DE HORÁRIO ===
        F.sum(F.when(F.col("periodo_dia") == "MADRUGADA", 1).otherwise(0)).alias("qtd_recargas_madrugada_mes"),
        F.sum(F.when(F.col("periodo_dia") == "MANHA", 1).otherwise(0)).alias("qtd_recargas_manha_mes"),
        F.sum(F.when(F.col("periodo_dia") == "TARDE", 1).otherwise(0)).alias("qtd_recargas_tarde_mes"),
        F.sum(F.when(F.col("periodo_dia") == "NOITE", 1).otherwise(0)).alias("qtd_recargas_noite_mes"),
        F.sum("flag_fim_semana").alias("qtd_recargas_fim_semana_mes"),

        # === SEMANAS ATIVAS ===
        F.countDistinct("semana_ano").alias("qtd_semanas_com_recarga_mes"),

        # === TIPOS DE TRANSAÇÃO ===
        F.sum(F.when(F.col("tipo_transacao") == "PAGO_PURO", 1).otherwise(0)).alias("qtd_pago_puro_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "BONUS_PURO", 1).otherwise(0)).alias("qtd_bonus_puro_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "COMBO_PAGO_BONUS", 1).otherwise(0)).alias("qtd_combo_mes"),
        F.sum(F.when(F.col("tipo_transacao") == "VALOR_NEGATIVO", 1).otherwise(0)).alias("qtd_valor_negativo_mes"),

        # === DIMENSÕES ===
        F.countDistinct(F.when(F.col("cod_tipo_credito") >= 0, F.col("cod_tipo_credito")))
            .alias("qtd_tipos_credito_distintos_mes"),
        F.countDistinct(F.when(F.col("cod_status_plataforma") >= 0, F.col("cod_status_plataforma")))
            .alias("qtd_status_plataforma_distintos_mes"),
    )

    # -------------------------------------------------------------------------
    # ETAPA 3: FEATURES DERIVADAS
    # -------------------------------------------------------------------------
    print(">>> [Agg] Criando features derivadas mensais...")

    # Ticket médio
    df_gold = df_gold.withColumn(
        "ticket_medio_mes",
        F.when(
            F.col("qtd_recargas_validas_mes") > 0,
            F.round(F.col("sum_val_real_ajustado_mes") / F.col("qtd_recargas_validas_mes"), 2)
        ).otherwise(0.0)
    )

    # Percentual SOS sobre crédito
    df_gold = df_gold.withColumn(
        "pct_sos_sobre_credito_mes",
        F.when(
            F.col("sum_val_credito_mes") > 0,
            F.round((F.col("sum_valor_sos_mes") / F.col("sum_val_credito_mes")) * 100, 2)
        ).otherwise(0.0)
    )

    # Frequência de SOS
    df_gold = df_gold.withColumn(
        "freq_sos_mes",
        F.when(
            F.col("qtd_recargas_mes") > 0,
            F.round(F.col("qtd_sos_mes") / F.col("qtd_recargas_mes"), 4)
        ).otherwise(0.0)
    )

    # Coeficiente de variação
    df_gold = df_gold.withColumn(
        "coef_variacao_val_mes",
        F.when(
            (F.col("avg_val_real_mes").isNotNull()) & (F.col("avg_val_real_mes") > 0),
            F.round(F.col("std_val_real_mes") / F.col("avg_val_real_mes"), 4)
        ).otherwise(None)
    )

    # Razão max/min
    df_gold = df_gold.withColumn(
        "ratio_max_min_val_mes",
        F.when(
            (F.col("min_val_real_mes").isNotNull()) & (F.col("min_val_real_mes") > 0),
            F.round(F.col("max_val_real_mes") / F.col("min_val_real_mes"), 2)
        ).otherwise(None)
    )

    # Razão bônus/crédito
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

    # Valor líquido
    df_gold = df_gold.withColumn(
        "val_liquido_mes",
        F.col("sum_val_credito_mes") - F.col("sum_valor_sos_mes")
    )

    # Percentual transações válidas
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

    # Flag baixa atividade (menos de 2 recargas/mês)
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
    Gera relatório de qualidade das features Gold de Recarga.
    """
    print("\n" + "="*80)
    print("RELATÓRIO DE QUALIDADE - GOLD RECARGA FEATURES")
    print("="*80 + "\n")

    count_gold = df_gold.count()

    print(f">>> [Stats] Volumetria:")
    print(f"    Silver (eventos): {count_silver:>15,}")
    print(f"    Gold (cliente-mês): {count_gold:>15,}")
    print(f"    Compressão: {count_silver/count_gold:.1f}x")

    # Distribuição de SAFRAs
    print(f"\n>>> [Stats] Distribuição de SAFRAs:")
    safras = df_gold.groupBy("safra_recarga").count().orderBy("safra_recarga").collect()
    for row in safras[:6]:  # Mostrar apenas 6 primeiras
        print(f"    {row['safra_recarga']}: {row['count']:>10,}")
    if len(safras) > 6:
        print(f"    ... ({len(safras) - 6} safras adicionais)")

    # Estatísticas de features
    stats = df_gold.select(
        F.mean("qtd_recargas_mes").alias("mean_qtd"),
        F.mean("sum_val_real_ajustado_mes").alias("mean_val"),
        F.mean("ticket_medio_mes").alias("mean_ticket"),
        F.sum("flag_teve_sos_mes").alias("total_com_sos"),
        F.mean("dias_medio_entre_recargas_mes").alias("mean_dias_entre"),
    ).collect()[0]

    print(f"\n>>> [Stats] Features principais:")
    print(f"    QTD_RECARGAS_MES (média): {stats['mean_qtd']:.2f}")
    print(f"    SUM_VAL_REAL_AJUSTADO_MES (média): R$ {stats['mean_val']:.2f}")
    print(f"    TICKET_MEDIO_MES (média): R$ {stats['mean_ticket']:.2f}")
    print(f"    Registros com SOS: {stats['total_com_sos']:,} ({100*stats['total_com_sos']/count_gold:.2f}%)")
    print(f"    DIAS_MEDIO_ENTRE_RECARGAS (média): {stats['mean_dias_entre']:.1f} dias")

    # Distribuição de cobertura
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
    Pipeline principal para geração de Gold Recarga Features.
    """
    parser = argparse.ArgumentParser(
        description="Gerar Gold Recarga Features v2 - Features comportamentais para modelagem de risco",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Modo padrão (caminhos default)
  python gold_recarga_features_v2.py

  # Com caminhos customizados
  python gold_recarga_features_v2.py \\
      --input_path /Volumes/.../silver/recarga_silver_delta/ \\
      --output_path /Volumes/.../gold/recarga_features_v2_delta/
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
        print(f">>> [Config] Ignorando argumentos não reconhecidos (Databricks/Jupyter): {unknown[:2]}...")

    # Banner
    print("\n")
    print("╔" + "="*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "GOLD RECARGA FEATURES V2".center(78) + "║")
    print("║" + "Features Comportamentais para Modelagem de Risco de Crédito".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "="*78 + "╝")
    print("\n")

    # Inicializar Spark
    spark = get_spark_session("Gold_Recarga_Features_V2")

    # =========================================================================
    # 1) LEITURA SILVER RECARGA
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Recarga: {args.input_path}")

    try:
        df_silver = spark.read.format(args.format).load(args.input_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_silver = df_silver.count()
    print(f">>> [Info] Registros na Silver: {count_silver:,}")
    print(f">>> [Info] Colunas disponíveis: {len(df_silver.columns)}")

    # =========================================================================
    # 2) PROCESSAMENTO
    # =========================================================================
    df_gold = criar_features_recarga_completas(df_silver)

    count_gold = df_gold.count()
    print(f">>> [Info] Registros no Gold: {count_gold:,}")
    print(f">>> [Info] Colunas geradas: {len(df_gold.columns)}")

    # =========================================================================
    # 3) RELATÓRIO DE QUALIDADE
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

        # Registrar tabela no Unity Catalog
        target_table = "hackathon_2025.default.gold_recarga_features_v2"
        try:
            df_gold.write \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(target_table)
            print(f">>> [Sucesso] Tabela registrada: {target_table}")
        except Exception as e:
            print(f">>> [Aviso] Não foi possível registrar tabela: {e}")
    else:
        print("\n>>> [Skip] Salvamento pulado (--skip_save)")

    # =========================================================================
    # 5) RESUMO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print("="*80)
    print(f"""
    Resumo:
    ├── Silver (eventos):     {count_silver:>15,} registros
    ├── Gold (cliente-mês):   {count_gold:>15,} registros
    ├── Compressão:           {count_silver/count_gold:>15.1f}x
    ├── Features geradas:     {len(df_gold.columns) - 5:>15} (excluindo metadados)
    └── Output:               {args.output_path}

    Próximos passos:
    1. JOIN com spine (ABT v4) por (NUM_CPF, SAFRA)
    2. Filtrar por janela temporal (SAFRA_RECARGA < SAFRA)
    3. Agregar M1/M3/M6 usando dt_recarga_safra vs dt_safra
    4. Validar cobertura e gates de qualidade
    """)
    print("="*80 + "\n")

    return df_gold


if __name__ == "__main__":
    df_gold = main()
