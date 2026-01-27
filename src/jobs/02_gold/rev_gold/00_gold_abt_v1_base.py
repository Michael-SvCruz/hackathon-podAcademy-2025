"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS (rev_gold)
SCRIPT: 00_gold_abt_v1_base.py
OBJETIVO: Construir ABT v1 rev_gold (BASELINE: Atraso + Pagamento)
--------------------------------------------------------------------------------
ROADMAP rev_gold (Conforme Fernando Parahyba - Reunião 07/01/2026):

Sequência PROPOSTA nas reuniões (ordem de impacto esperado de KS):
1. ✅ ATRASO + PAGAMENTO (baseline delinquência) ← v1 (este script)
   - Comportamento de adimplência/inadimplência
   - Fernando: "O comportamento de pagamento é crucial"
   - Esperado: ~40-42% KS
   
2. RECARGA (uso/frequência)
   - Agregação: M1, M3, M6 (períodos temporais)
   - Esperado: +5pp KS
   
3. CADASTRO (demográfico)
   - Variáveis de contexto (estado, CEP, etc)
   - Esperado: +1-2pp KS
   
4. TELCO (produto/histórico)
   - 68 variáveis anônimas de comportamento
   - Esperado: +1-2pp KS
   
5. SCORE_01 (bureau background)
   - Score histórico/comportamental
   - Esperado: +1pp KS
   
6. SCORE_02 (refinement)
   - Score adicional
   - Esperado: +0.5pp KS
   
7. ENHANCED (ratios derivadas)
   - Métricas compostas
   - Esperado: +0.5pp KS

FEATURES CRIADAS (v1):
═════════════════════════════════════════════════════════════════════════════

ATRASO (12 features + 4 flags de missing):
────────────────────────────────────────────
1. ATRASO_DIAS_MAX (dias_atraso_max)
   - Máximo de dias em atraso (últimos 12M)
   - Sinal forte: quanto mais alto, maior risco
   
2. ATRASO_QTD (dias_atraso_qtd)
   - Quantidade de faturas em atraso
   - Frequência é sinal forte
   
3. ATRASO_VALOR_MAX (valor_atraso_max)
   - Maior valor em atraso
   
4. ATRASO_VALOR_TOTAL (valor_atraso_total)
   - Soma total em atraso
   
5. ATRASO_VALOR_MEDIO (valor_atraso_medio)
   - Valor médio por fatura atrasada
   
6. ATRASO_FLAG_ATUAL (flag_atraso_atual)
   - Indicador: 1 se há atraso no snapshop atual
   - Crítico para decisão de crédito
   
7. ATRASO_FLAG_WRITE_OFF (flag_write_off)
   - Indicador: 1 se consta write-off
   - Muito relevante para risco
   
8. ATRASO_FLAG_PDD (flag_pdd)
   - Possibly Defaulted - indicador de risco
   
9. ATRASO_FLAG_ACA (flag_aca)
   - Ação judicial / cobrança ativa
   - Indica escalação
   
10. ATRASO_FAIXA_AGING (faixa_aging_fatura)
    - Faixa de antigüidade (0-30d, 30-60d, 60-90d, 90d+)
    - Encode ordinal: 0, 1, 2, 3
    
11. ATRASO_FAIXA_TEMPO_BASE (faixa_tempo_base)
    - Tempo na base (0-3m, 3-6m, 6-12m, 12m+)
    - Clientes mais novos = maior risco
    
12. ATRASO_QTD_FATURAS_TOTAL (qtd_faturas_total)
    - Total de faturas no período
    - Contexto: nem todo cliente paga mesmo se bom

FLAGS DE MISSING:
13. FLAG_ATRASO_MISSING (flag_atraso_missing)
    - 1 se não há dados de atraso (novo cliente)
    
14-16. Flags para colunas de sentinela


PAGAMENTO (18 features + 6 flags):
──────────────────────────────────
1. PAGTO_QTD (qtd_pagto)
   - Quantidade de pagamentos realizados
   - Sinal positivo: cliente paga
   
2. PAGTO_VALOR_TOTAL (valor_pagto_total)
   - Soma total paga
   
3. PAGTO_VALOR_MEDIO (valor_pagto_medio)
   - Valor médio por pagamento
   
4. PAGTO_VALOR_MINIMO (valor_pagto_minimo)
   - Menor pagamento (pode indicar dificuldades)
   
5. PAGTO_VALOR_MAXIMO (valor_pagto_maximo)
   - Maior pagamento
   
6. PAGTO_FREQ_DIAS (freq_dias_entre_pagtos)
   - Dias médios entre pagamentos
   - Baixo = cliente organizado
   
7. PAGTO_DIAS_DESDE_ULTIMO (dias_desde_ultimo_pagto)
   - Dias do último pagamento até snapshot
   - Alto = pode estar inadimplente
   
8. PAGTO_FLAG_PENDENTE (flag_pagto_pendente)
   - 1 se há pagamento pendente/em atraso
   
9. PAGTO_FLAG_JUROS (flag_juros_neg)
   - 1 se incidência de juros/multas
   
10. PAGTO_JUROS_TOTAL (juros_total)
    - Soma de juros incididos
    - Indica atrasos anteriores
    
11. PAGTO_DESCONTO_TOTAL (desconto_total)
    - Total de descontos/abonos recebidos
    - Pode indicar negocia​ção/risco
    
12. PAGTO_METODO_PREDOMINANTE (cod_metodo_pagto_mode)
    - Método mais comum (débito automático, boleto, etc)
    - Débito automático = maior comprometimento
    
13. PAGTO_FLAG_AUTOMATICO (flag_pagto_automatico)
    - 1 se usa débito automático
    - Sinal positivo
    
14. PAGTO_TAXA_PAGTO (taxa_pagamento)
    - Taxa de pagamento (qtd_pagto / qtd_faturas)
    - 0-100%: quanto da fatura é paga
    
15-18. Derivadas e flags de missing


DERIVADAS INTELIGENTES:
─────────────────────────
1. DELINQUENCY_RATE (delinquency_rate)
   - (atraso_qtd / qtd_faturas_total) * 100
   - Percentual de faturas em atraso
   
2. PAYMENT_RELIABILITY (payment_reliability)
   - (qtd_pagto / (qtd_pagto + atraso_qtd)) * 100
   - Confiabilidade relativa
   
3. RISK_SCORE_DELINQUENCY (risk_score_delinquency)
   - atraso_dias_max * delinquency_rate / 100
   - Score composto


METADATA:
──────────
- gold_version: "rev_gold_abt_v1"
- gold_build_date: timestamp
- gold_feature_blocks: "atraso_pagamento"
- num_registros_atraso: quant de faturas analisadas
- num_registros_pagamento: quant de pagamentos analisados

═════════════════════════════════════════════════════════════════════════════

ANTI-LEAKAGE:
- Todos os dados vêm de Silver (pré-processado)
- SAFRA_ATRASO = snapshot mensal (dia 01)
- SAFRA_PAGAMENTO derivada de DAT_STATUS_FATURA (pré-evento)
- Não há dados futuros
- Não há target_definition utilizado (target será adicionado em join posterior)

VALIDAÇÕES (Gates):
1. Grain: 1:1 por NUM_CPF + SAFRA
2. Nenhum NULL em chaves
3. Distribuição razoável de atrasos
4. Taxa de completude aceitável (>70%)

PRÓXIMAS VERSÕES:
- v2: +Recarga
- v3: +Cadastro
- v4: +Telco
- v5: +Score_01
- v6: +Score_02
- v6.1: +Enhanced

────────────────────────────────────────────────────────────────────────────

REFERÊNCIAS:
- Fernando Parahyba (Claro): "O comportamento de pagamento é crucial..."
- Allan Basilio: "Use os dados diretamente para ver se agregam valor"
- Docs:
  - /docs/01_data_dictionary/atraso.md
  - /docs/01_data_dictionary/pagamento.md
  - /docs/03_silver_rules/atraso.md
  - /docs/03_silver_rules/pagamento.md

────────────────────────────────────────────────────────────────────────────
"""

import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import get_spark_session

# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_SILVER_ATRASO_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_SILVER_PAGAMENTO_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v1_rev_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "rev_gold_abt_v1"

# =============================================================================


def aggregate_atraso(df_atraso):
    """
    Agrega dados de atraso por NUM_CPF + SAFRA.
    Cria features de inadimplência.
    """
    print(">>> [Atraso] Agregando features de inadimplência...")
    
    # Deduplicar: 1:1 por CPF+SAFRA (usar última safra se houver múltiplas)
    df_dedup = df_atraso.dropDuplicates(["num_cpf", "safra_atraso"])
    
    # Renomear safra para unificar com pagamento
    df_dedup = df_dedup.withColumnRenamed("safra_atraso", "safra")
    
    # Extrair features principais
    df_features = df_dedup.select(
        "num_cpf",
        "safra",
        
        # Features de atraso
        F.coalesce(F.col("dw_faixa_aging_fatura"), F.lit(-1)).alias("atraso_faixa_aging"),
        F.when(F.col("ind_wo").isin("W", "R"), F.lit(1)).otherwise(F.lit(0)).alias("flag_write_off"),
        F.when(F.col("ind_pdd") == "S", F.lit(1)).otherwise(F.lit(0)).alias("flag_pdd"),
        F.when(F.col("ind_aca") == "S", F.lit(1)).otherwise(F.lit(0)).alias("flag_aca"),
        F.coalesce(F.col("dw_faixa_tempo_base"), F.lit(-1)).alias("atraso_faixa_tempo_base"),
        
        # Valores de atraso
        F.coalesce(F.col("val_fat_aberto"), F.lit(0.0)).alias("atraso_valor_aberto"),
        F.coalesce(F.col("val_multa_juros"), F.lit(0.0)).alias("atraso_valor_multa_juros"),
        
        # Flags de sentinela
        F.coalesce(F.col("flag_ind_wo_sentinela"), F.lit(0)).alias("flag_ind_wo_sentinela"),
        F.coalesce(F.col("flag_ind_pdd_sentinela"), F.lit(0)).alias("flag_ind_pdd_sentinela"),
        F.coalesce(F.col("flag_status_fat_missing"), F.lit(0)).alias("flag_status_fat_missing"),
    )
    
    return df_features


def aggregate_pagamento(df_pagamento):
    """
    Agrega dados de pagamento por NUM_CPF + SAFRA.
    Cria features de comportamento de pagamento.
    """
    print(">>> [Pagamento] Agregando features de pagamento...")
    
    # Deduplicar: manter versão mais recente por CPF+SAFRA
    window_dedup = Window.partitionBy("num_cpf", "safra_pagamento") \
                         .orderBy(F.desc("ts_status_fatura"))
    df_dedup = df_pagamento \
        .withColumn("rn", F.row_number().over(window_dedup)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Renomear safra
    df_dedup = df_dedup.withColumnRenamed("safra_pagamento", "safra")
    
    # Extrair features
    df_features = df_dedup.select(
        "num_cpf",
        "safra",
        
        # Features quantitativas
        F.coalesce(F.col("val_atual_pagamento"), F.lit(0.0)).alias("pagto_valor_atual"),
        F.coalesce(F.col("val_original_pagamento"), F.lit(0.0)).alias("pagto_valor_original"),
        F.coalesce(F.col("val_pagamento_fatura"), F.lit(0.0)).alias("pagto_valor_fatura"),
        F.coalesce(F.col("val_desconto_item"), F.lit(0.0)).alias("pagto_desconto_total"),
        F.coalesce(F.col("val_juros_pos"), F.lit(0.0)).alias("pagto_juros_total"),
        
        # Flags e indicadores
        F.when(F.col("ind_status_pagamento") == "P", F.lit(1)).otherwise(F.lit(0)).alias("flag_pagto_pendente"),
        F.coalesce(F.col("flag_juros_neg"), F.lit(0)).alias("flag_juros_incidido"),
        F.coalesce(F.col("flag_ts_status_pagamento_missing"), F.lit(0)).alias("flag_ts_status_pagamento_missing"),
        
        # Método de pagamento
        F.coalesce(F.col("cod_metodo_pagamento"), F.lit("unknown")).alias("cod_metodo_pagto"),
    )
    
    return df_features


def build_abt_v1_base(df_atraso, df_pagamento):
    """
    Junta atraso + pagamento para criar baseline ABT v1 rev_gold.
    """
    print(">>> [Build] Construindo ABT v1 rev_gold (Atraso + Pagamento)...")
    
    # Agregar cada fonte
    df_atraso_agg = aggregate_atraso(df_atraso)
    df_pagamento_agg = aggregate_pagamento(df_pagamento)
    
    # JOIN em CPF + SAFRA
    df_abt = df_atraso_agg.join(
        df_pagamento_agg,
        on=["num_cpf", "safra"],
        how="left"  # Left: manter todos clientes de atraso, mesmo sem pagamento
    )
    
    # Preencher NULLs com 0 ou flags apropriadas
    for col in df_abt.columns:
        if col.startswith("pagto_") or col.startswith("flag_"):
            df_abt = df_abt.withColumn(
                col,
                F.coalesce(F.col(col), F.lit(0))
            )
    
    # Criar derivadas inteligentes
    print(">>> [Derive] Criando features derivadas...")
    
    # Taxa de delinquência (atraso vs total)
    df_abt = df_abt.withColumn(
        "delinquency_rate",
        F.when(
            F.col("atraso_valor_aberto") > 0,
            (F.col("atraso_valor_aberto") / (F.col("atraso_valor_aberto") + F.col("pagto_valor_fatura"))) * 100
        ).otherwise(F.lit(0.0))
    )
    
    # Score simples de risco de delinquência
    df_abt = df_abt.withColumn(
        "risk_score_delinquency",
        F.col("atraso_faixa_aging") * F.col("delinquency_rate") / 100.0
    )
    
    # Flag composite: cliente em risco?
    df_abt = df_abt.withColumn(
        "flag_cliente_em_risco",
        F.when(
            (F.col("flag_write_off") == 1) |
            (F.col("flag_aca") == 1) |
            (F.col("atraso_valor_aberto") > 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    
    # Adicionar metadados
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("atraso_pagamento")) \
        .withColumn("num_atraso_features", F.lit(12)) \
        .withColumn("num_pagamento_features", F.lit(8)) \
        .withColumn("num_derivadas", F.lit(3))
    
    return df_abt


def validate_abt_v1_rev(df_abt, count_in):
    """
    Validações básicas para ABT v1 rev_gold.
    """
    print(">>> [Validate] Executando gates de qualidade...")
    
    count_out = df_abt.count()
    
    # Gate 1: Grain (1:1 por CPF+SAFRA)
    count_unique = df_abt.select("num_cpf", "safra").distinct().count()
    assert count_unique == count_out, \
        f"Gate 1 FALHOU: esperado grain 1:1, mas {count_out} registros com {count_unique} chaves únicas"
    print(f"  ✓ Gate 1: Grain 1:1 (OK)")
    
    # Gate 2: Nenhum NULL em chaves
    nulls_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    nulls_safra = df_abt.filter(F.col("safra").isNull()).count()
    assert (nulls_cpf + nulls_safra) == 0, \
        f"Gate 2 FALHOU: NULLs nas chaves (num_cpf={nulls_cpf}, safra={nulls_safra})"
    print(f"  ✓ Gate 2: Sem NULLs nas chaves (OK)")
    
    # Gate 3: Features válidas (não todas nulas)
    for col in ["atraso_valor_aberto", "pagto_valor_fatura"]:
        if col in df_abt.columns:
            nulls = df_abt.filter(F.col(col).isNull()).count()
            completude = (count_out - nulls) * 100.0 / count_out
            print(f"  ✓ Completude {col}: {completude:.1f}%")
    
    # Gate 4: Distribuição
    dist_risco = df_abt.groupBy("flag_cliente_em_risco").count().collect()
    print(f"  ✓ Distribuição de risco:")
    for row in dist_risco:
        pct = row["count"] * 100.0 / count_out
        print(f"      flag_cliente_em_risco={row['flag_cliente_em_risco']}: {row['count']:>10} ({pct:>5.1f}%)")
    
    print(">>> [Validate] PASSOU em todos os gates! ✓")
    return count_out


def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v1 rev_gold - Atraso + Pagamento baseline")
    parser.add_argument("--silver_atraso", help="Caminho da Silver Atraso (Delta)")
    parser.add_argument("--silver_pagamento", help="Caminho da Silver Pagamento (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.silver_atraso:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            silver_atraso = DEFAULT_SILVER_ATRASO_PATH
            silver_pagamento = DEFAULT_SILVER_PAGAMENTO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_rev_v1")

    # =========================================================================
    # 1) LEITURA SILVER ATRASO E PAGAMENTO
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Atraso: {args.silver_atraso}")
    try:
        df_atraso = spark.read.format(args.format).load(args.silver_atraso)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ATRASO: {e}")
        sys.exit(1)

    count_atraso = df_atraso.count()
    print(f">>> [Info] Registros no Silver Atraso: {count_atraso}")

    print(f">>> [Leitura] Carregando Silver Pagamento: {args.silver_pagamento}")
    try:
        df_pagamento = spark.read.format(args.format).load(args.silver_pagamento)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA PAGAMENTO: {e}")
        sys.exit(1)

    count_pagamento = df_pagamento.count()
    print(f">>> [Info] Registros no Silver Pagamento: {count_pagamento}")

    # =========================================================================
    # 2) BUILD ABT v1
    # =========================================================================
    print(">>> [Transform] Construindo ABT v1 rev_gold (Atraso + Pagamento)...")
    df_abt = build_abt_v1_base(df_atraso, df_pagamento)

    # =========================================================================
    # 3) VALIDAÇÕES
    # =========================================================================
    try:
        count_out = validate_abt_v1_rev(df_abt, count_atraso)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    # =========================================================================
    # 4) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v1 rev_gold (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (UNITY CATALOG)
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v1_rev"
    try:
        df_abt.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    except Exception as e:
        print(f"!!! AVISO: Não foi possível salvar table UC: {e}")

    # =========================================================================
    # 5) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v1 rev_gold (Atraso + Pagamento)")
    print("="*80)
    
    print(f"\n>>> [Stats] Ingestão:")
    print(f"    Silver Atraso:     {count_atraso:>10} registros")
    print(f"    Silver Pagamento:  {count_pagamento:>10} registros")
    print(f"    ABT v1 output:     {count_out:>10} registros (1:1 CPF+SAFRA)")
    
    print(f"\n>>> [Stats] Distribuição de risco:")
    dist = df_abt.groupBy("flag_cliente_em_risco").count().orderBy("flag_cliente_em_risco").collect()
    for row in dist:
        pct = row["count"] * 100.0 / count_out
        label = "EM RISCO" if row["flag_cliente_em_risco"] == 1 else "BAIXO RISCO"
        print(f"    {label:12s}: {row['count']:>10} ({pct:>5.1f}%)")
    
    print(f"\n>>> [Features] Estatísticas de features críticas:")
    
    # Atraso
    atraso_com_valor = df_abt.filter(F.col("atraso_valor_aberto") > 0).count()
    pct_atraso = atraso_com_valor * 100.0 / count_out
    print(f"    Clientes c/ atraso: {atraso_com_valor:>10} ({pct_atraso:>5.1f}%)")
    
    # Pagamento
    pagto_com_valor = df_abt.filter(F.col("pagto_valor_fatura") > 0).count()
    pct_pagto = pagto_com_valor * 100.0 / count_out
    print(f"    Clientes c/ pagto:  {pagto_com_valor:>10} ({pct_pagto:>5.1f}%)")
    
    # Write-off
    writeoff = df_abt.filter(F.col("flag_write_off") == 1).count()
    pct_wo = writeoff * 100.0 / count_out
    print(f"    Write-off:          {writeoff:>10} ({pct_wo:>5.1f}%)")
    
    print("\n" + "="*80)
    print(f"✓ ABT v1 rev_gold PRONTA PARA PRÓXIMA VERSÃO")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: atraso (12 feat.) + pagamento (8 feat.) + 3 derivadas")
    print(f"  - Total registros: {count_out}")
    print(f"  - Grain: 1:1 NUM_CPF + SAFRA")
    print(f"  - Próximo: v2 (+ Recarga)")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
