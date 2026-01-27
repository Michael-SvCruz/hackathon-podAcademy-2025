"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 03_gold_abt_v4_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v4 para modelagem.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script estende ABT v3 com features Cadastro (idade, CEP, var_02-25) via join.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ✅ ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ✅ ABT v2: ABT_v1 + SCORE_02                                 [COMPLETED]
3. ✅ ABT v3: ABT_v2 + Telco features (var_26-93)              [COMPLETED]
4. ⏳ ABT v4: ABT_v3 + Cadastro (age, CEP, var_02-25)          ← v4 (este script)
5.   ABT v5: ABT_v4 + Recarga
6.   ABT v6: ABT_v5 + Pagamento + Atraso

DEFINIÇÕES CRÍTICAS (conforme target_definition.md):
- Evento âncora: cliente-mês (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SÓ em FLAG_INSTALACAO=1)
- Decisão observada: FLAG_INSTALACAO (para análise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ + TELCO (68) + CADASTRO (33)

ANTI-LEAKAGE CRÍTICO:
- FPD_INT é LABEL, não feature
- FLAG_INSTALACAO_INT é LABEL, não feature
- Ambas incluídas apenas para auditoria e análise de impacto (swaps)

NOVO EM V4:
- IDADE_ANOS: Idade derivada do cliente
- FLAG_IDADE_MENOR_18 / FLAG_IDADE_MUITO_ALTA: Sanity checks
- CEP_3_DIGITOS: Feature regional (3 primeiros dígitos)
- FLAG_CEP_MISSING: Indicador de CEP ausente
- STATUSRF: Status cadastral (categórico)
- VAR_02 a VAR_25: 24 variáveis cadastrais (mix numéricas e categóricas)
- FLAGS para cada var_*

ESTRUTURA DO JOIN:
- Spine (ABT v3 Gold)    : 1:1 por NUM_CPF + SAFRA
- Cadastro Silver        : 1:1 por NUM_CPF + SAFRA
- Join type              : LEFT (v3 é spine, Cadastro é enriquecimento)
- Resultado              : 1:1 mantida, features como NULLs quando não encontrado

VALIDAÇÕES OBRIGATÓRIAS (9 gates):
1. Unicidade: 1:1 por NUM_CPF + SAFRA
2. FPD observado SÓ em FLAG_INSTALACAO=1
3. Sem NULLs nas chaves
4. Distribuições balanceadas dos labels
5. Score_01 présente (cobertura > 90%)
6. Score_02 présente (cobertura > 40%)
7. Cobertura Telco > 20%
8. Cobertura Cadastro > 20% (novo em v4)
9. Sanity check idade (não menores de 18 como features)

AJUSTES UNITY CATALOG:
- Leitura de Gold ABT v3 + Silver Cadastro
- Escrita em Delta + tabela UC
- Metadados de rastreabilidade
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import functions as F

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v4

# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_GOLD_ABT_V3_PATH = "/Volumes/hackathon_2025/default/gold/abt_v3_delta/"
DEFAULT_SILVER_CADASTRO_PATH = "/Volumes/hackathon_2025/default/silver/cadastro_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v4_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v4"

# Variáveis Cadastro esperadas (var_02 a var_25 = 24 variáveis)
CADASTRO_VAR_COLUMNS = [f"var_{i}" for i in range(2, 26)]
# =============================================================================

def build_abt_v4(df_abt_v3, df_cadastro):
    """
    Constrói ABT v4: estende v3 com features Cadastro (idade, CEP, var_02-25).
    
    Inclusões v3 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Features v2: score_02_adj, flag_score02_missing
    - Features v3: var_26_adj a var_93_adj + flags (68 vars Telco)
    - Metadados: prod, flag_mig2
    - Gold metadata: gold_version, gold_build_date, gold_feature_blocks
    
    Novas inclusões em v4:
    - IDADE_ANOS: Idade derivada (numérica)
    - FLAG_IDADE_MENOR_18: Sanity check (< 18)
    - FLAG_IDADE_MUITO_ALTA: Outlier check (> 100)
    - CEP_3_DIGITOS: Regional proxy (string)
    - FLAG_CEP_MISSING: Missing indicator
    - STATUSRF: Status cadastral (categórico)
    - VAR_02 a VAR_25: 24 vars cadastrais
    - Flag para cada var_* de Cadastro
    - Atualiza gold_feature_blocks: "score_01,score_02,telco,cadastro"
    
    Exclusões (anti-leakage):
    - FPD_INT não pode ser feature
    - FLAG_INSTALACAO não pode ser feature
    """
    print(">>> [Transform] JOIN ABT v3 + Cadastro para ABT v4...")

    # Step 1: Preparar subset do ABT v3 (manter como spine)
    v3_cols = ["num_cpf", "safra", "dt_safra", "flag_instalacao_int", "fpd_int"]
    v3_cols.extend(["score_01_adj", "flag_score01_missing"])
    v3_cols.extend(["score_02_adj", "flag_score02_missing"])
    v3_cols.extend([f"var_{i}_adj" for i in range(26, 94)])
    v3_cols.extend([f"flag_var_{i}_missing" for i in range(26, 94)])
    v3_cols.extend(["prod", "flag_mig2"])
    v3_cols.extend(["metadata_data_ingestao", "metadata_nome_arquivo_origem", 
                    "metadata_sistema_origem", "metadata_data_transformacao", 
                    "metadata_versao_regra"])
    v3_cols.extend(["gold_version", "gold_build_date", "gold_feature_blocks"])
    
    df_abt_v3_prepared = df_abt_v3.select(*[col for col in v3_cols if col in df_abt_v3.columns])

    # Step 2: Preparar subset do Cadastro para join
    cadastro_cols_to_select = ["num_cpf", "safra"]
    
    # Adicionar variáveis demográficas
    if "idade_anos" in df_cadastro.columns:
        cadastro_cols_to_select.append("idade_anos")
    if "flag_idade_menor_18" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_idade_menor_18")
    if "flag_idade_muito_alta" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_idade_muito_alta")
    if "cep_3_digitos" in df_cadastro.columns:
        cadastro_cols_to_select.append("cep_3_digitos")
    if "flag_cep_missing" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_cep_missing")
    if "statusrf" in df_cadastro.columns:
        cadastro_cols_to_select.append("statusrf")
    
    # Adicionar var_02 a var_25 (com zero-padding: var_02, var_03, ..., var_25)
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_cadastro.columns:
            cadastro_cols_to_select.append(var_col)
    
    # Adicionar flags de missing para cada var
    for var_idx in range(2, 26):
        flag_col = f"flag_var_{var_idx:02d}_missing"
        if flag_col in df_cadastro.columns:
            cadastro_cols_to_select.append(flag_col)
    
    df_cadastro_prepared = df_cadastro.select(*cadastro_cols_to_select)

    # Step 3: JOIN LEFT ABT v3 + Cadastro (v3 é spine)
    print(">>> [Transform] Executando LEFT JOIN ABT_v3.NUM_CPF+SAFRA = Cadastro.NUM_CPF+SAFRA...")
    
    df_abt = df_abt_v3_prepared.join(
        df_cadastro_prepared,
        on=["num_cpf", "safra"],
        how="left"
    )

    # Step 4: Atualizar metadados de gold (version, feature blocks, build date)
    print(">>> [Transform] Atualizando metadados de Gold (version, feature blocks, build date)...")
    from datetime import datetime
    build_date = datetime.now().isoformat()
    
    df_abt = df_abt \
        .withColumn("gold_version", F.lit("gold_abt_v4")) \
        .withColumn("gold_build_date", F.lit(build_date)) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco,cadastro"))

    # Step 5: Selecionar e ordenar colunas logicamente
    final_cols = [
        # CHAVES
        "num_cpf", "safra", "dt_safra",
        
        # LABELS (auditoria - não são features!)
        "flag_instalacao_int", "fpd_int",
        
        # FEATURES v1 (Score_01)
        "score_01_adj", "flag_score01_missing",
        
        # FEATURES v2 (Score_02)
        "score_02_adj", "flag_score02_missing",
        
        # FEATURES v3 (Telco 68 vars)
        *[f"var_{i}_adj" for i in range(26, 94)],
        *[f"flag_var_{i}_missing" for i in range(26, 94)],
        
        # FEATURES v4 (Cadastro - Demográficas)
        "idade_anos", "flag_idade_menor_18", "flag_idade_muito_alta",
        "cep_3_digitos", "flag_cep_missing", "statusrf",
        
        # FEATURES v4 (Cadastro - Variáveis anonimizadas)
        *[f"var_{i:02d}" for i in range(2, 26) if f"var_{i:02d}" in df_abt.columns],
        *[f"flag_var_{i:02d}_missing" for i in range(2, 26) if f"flag_var_{i:02d}_missing" in df_abt.columns],
        
        # METADADOS
        "prod", "flag_mig2",
        
        # AUDITORIA
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao",
        "metadata_versao_regra",
        
        # GOLD METADATA
        "gold_version", "gold_build_date", "gold_feature_blocks"
    ]
    
    # Filtrar apenas colunas que existem no dataframe
    final_cols = [col for col in final_cols if col in df_abt.columns]
    
    df_abt = df_abt.select(*final_cols)

    return df_abt

def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v4 - Score_01 + Score_02 + Telco + Cadastro")
    parser.add_argument("--gold_abt_v3_path", help="Caminho do Gold ABT v3 (Delta)")
    parser.add_argument("--silver_cadastro_path", help="Caminho da Silver Cadastro (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()

    if args_parsed.gold_abt_v3_path and args_parsed.silver_cadastro_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            gold_abt_v3_path = DEFAULT_GOLD_ABT_V3_PATH
            silver_cadastro_path = DEFAULT_SILVER_CADASTRO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = get_spark_session("Gold_ABT_Builder_v4")

    # =========================================================================
    # 1) LEITURA GOLD ABT v3 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Gold ABT v3 (Spine): {args.gold_abt_v3_path}")
    try:
        df_abt_v3 = spark.read.format(args.format).load(args.gold_abt_v3_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA ABT v3: {e}")
        sys.exit(1)

    count_in_v3 = df_abt_v3.count()
    print(f">>> [Info] Registros no Gold ABT v3: {count_in_v3}")

    # =========================================================================
    # 2) LEITURA SILVER CADASTRO (ENRIQUECIMENTO)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Cadastro (Enriquecimento): {args.silver_cadastro_path}")
    try:
        df_cadastro = spark.read.format(args.format).load(args.silver_cadastro_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA CADASTRO: {e}")
        sys.exit(1)

    count_in_cadastro = df_cadastro.count()
    print(f">>> [Info] Registros no Silver Cadastro: {count_in_cadastro}")

    # =========================================================================
    # 3) BUILD ABT v4 (ABT v3 LEFT JOIN Cadastro)
    # =========================================================================
    print(">>> [Transform] Construindo ABT v4 (Score_01 + Score_02 + Telco + Cadastro)...")
    df_abt = build_abt_v4(df_abt_v3, df_cadastro)

    # =========================================================================
    # 4) VALIDAÇÕES (obrigatórias conforme target_definition.md)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    try:
        validate_abt_v4(df_abt)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v4: {count_out}")

    # =========================================================================
    # 5) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v4 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)

    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v4"
    df_abt.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
    # =========================================================================

    # =========================================================================
    # 6) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v4 (Score_01 + Score_02 + Telco + Cadastro)")
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
    
    # Completude de features v1, v2
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    
    score01_coverage = (count_out - score01_null) * 100 / count_out
    score02_coverage = (count_out - score02_null) * 100 / count_out
    
    print(f"\n>>> [Features v1-v2] Completude:")
    print(f"    SCORE_01_ADJ: {score01_coverage:.2f}%")
    print(f"    SCORE_02_ADJ: {score02_coverage:.2f}%")
    
    # Completude de features v3 (Telco)
    telco_var_nulls = {}
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_var_nulls[var_idx] = null_count
    
    telco_total_nulls = sum(telco_var_nulls.values())
    telco_total_cells = len(telco_var_nulls) * count_out
    telco_coverage = ((telco_total_cells - telco_total_nulls) / telco_total_cells) * 100 if telco_total_cells > 0 else 0
    
    print(f"\n>>> [Features v3 - Telco] Completude:")
    print(f"    Cobertura agregada Telco (var_26-93): {telco_coverage:.2f}%")
    print(f"    Total células Telco: {telco_total_cells:>12}")
    print(f"    Células NULLs: {telco_total_nulls:>12}")
    
    # Completude de features v4 (Cadastro)
    cadastro_var_nulls = {}
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx}"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            cadastro_var_nulls[var_idx] = null_count
    
    cadastro_total_nulls = sum(cadastro_var_nulls.values())
    cadastro_total_cells = len(cadastro_var_nulls) * count_out
    cadastro_coverage = ((cadastro_total_cells - cadastro_total_nulls) / cadastro_total_cells) * 100 if cadastro_total_cells > 0 else 0
    
    print(f"\n>>> [Features v4 - Cadastro] Completude:")
    print(f"    Cobertura agregada Cadastro (var_02-25): {cadastro_coverage:.2f}%")
    print(f"    Total células Cadastro: {cadastro_total_cells:>12}")
    print(f"    Células NULLs: {cadastro_total_nulls:>12}")
    
    # Breakdown por variável de Cadastro (para debug)
    print(f"\n>>> [Features v4 - Cadastro Detail] Distribuição por variável:")
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            coverage_pct = ((count_out - null_count) / count_out) * 100 if count_out > 0 else 0
            print(f"      {var_col}: {coverage_pct:>6.2f}% ({count_out - null_count:>10} não-NULL)")
        else:
            print(f"      {var_col}: NÃO ENCONTRADO NA TABELA")
    
    # Impacto do join (quantos registros encontraram match em Cadastro)
    # Verificar se há QUALQUER variável de Cadastro (não só var_02)
    cadastro_match_conditions = []
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_abt.columns:
            cadastro_match_conditions.append(F.col(var_col).isNotNull())
    
    if cadastro_match_conditions:
        # Se houver alguma condição, usar OR
        match_condition = cadastro_match_conditions[0]
        for cond in cadastro_match_conditions[1:]:
            match_condition = match_condition | cond
        cadastro_match_count = df_abt.filter(match_condition).count()
    else:
        cadastro_match_count = 0
    
    cadastro_match_pct = (cadastro_match_count / count_out) * 100 if count_out > 0 else 0
    
    print(f"\n>>> [JOIN] Impacto Cadastro:")
    print(f"    Total ABT v3 (spine): {count_in_v3:>12}")
    print(f"    Total Cadastro (enriquecimento): {count_in_cadastro:>12}")
    print(f"    Resultados ABT v4 (ABT v3 LEFT JOIN): {count_out:>12}")
    print(f"    Registros com match Cadastro: {cadastro_match_count:>12} ({cadastro_match_pct:.2f}%)")
    
    print("\n" + "="*80)
    print(f"✓ ABT v4 PRONTA PARA MODELAGEM")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: Score_01, Score_02, Telco (68 variáveis), Cadastro (24 variáveis)")
    print(f"  - Total registros: {count_out}")
    print(f"  - Grão: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Status: Incremental (Cadastro adiciona {cadastro_coverage:.1f}% cobertura, {cadastro_match_pct:.1f}% match)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
