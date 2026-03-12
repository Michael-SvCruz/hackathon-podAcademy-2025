# Arquivo: mig_oci/data_upload/scripts/abt_v4_builder.py
# Adaptado de src/jobs/02_gold/03_gold_abt_v4_builder.py para OCI Data Flow
# Mudancas: paths OCI, imports flat, sem saveAsTable
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: abt_v4_builder.py (OCI Data Flow)
OBJETIVO: Construir Analytical Base Table (ABT) v4 para modelagem.
--------------------------------------------------------------------------------
DESCRICAO TECNICA:
Este script estende ABT v3 com features Cadastro (idade, CEP, var_02-25) via join.

ROADMAP INCREMENTAL (conforme target_definition.md):
1. ABT v1: bureau_full (spine) + SCORE_01                    [BASELINE]
2. ABT v2: ABT_v1 + SCORE_02                                 [COMPLETED]
3. ABT v3: ABT_v2 + Telco features (var_26-93)              [COMPLETED]
4. ABT v4: ABT_v3 + Cadastro (age, CEP, var_02-25)          <- v4 (este script)
5. ABT v5: ABT_v4 + Recarga
6. ABT v6: ABT_v5 + Pagamento + Atraso

DEFINICOES CRITICAS (conforme target_definition.md):
- Evento ancora: cliente-mes (NUM_CPF + SAFRA)
- Target (Y): FPD_INT (observado SO em FLAG_INSTALACAO=1)
- Decisao observada: FLAG_INSTALACAO (para analise de swaps)
- Features (X): SCORE_01_ADJ + SCORE_02_ADJ + TELCO (68) + CADASTRO (33)

ANTI-LEAKAGE CRITICO:
- FPD_INT e LABEL, nao feature
- FLAG_INSTALACAO_INT e LABEL, nao feature
- Ambas incluidas apenas para auditoria e analise de impacto (swaps)

NOVO EM V4:
- IDADE_ANOS: Idade derivada do cliente
- FLAG_IDADE_MENOR_18 / FLAG_IDADE_MUITO_ALTA: Sanity checks
- CEP_3_DIGITOS: Feature regional (3 primeiros digitos)
- FLAG_CEP_MISSING: Indicador de CEP ausente
- STATUSRF: Status cadastral (categorico)
- VAR_02 a VAR_25: 24 variaveis cadastrais (mix numericas e categoricas)
- FLAGS para cada var_*

ESTRUTURA DO JOIN:
- Spine (ABT v3 Gold)    : 1:1 por NUM_CPF + SAFRA
- Cadastro Silver        : 1:1 por NUM_CPF + SAFRA
- Join type              : LEFT (v3 e spine, Cadastro e enriquecimento)
- Resultado              : 1:1 mantida, features como NULLs quando nao encontrado
--------------------------------------------------------------------------------
"""

import sys
import argparse
from pyspark.sql import SparkSession, functions as F

# No OCI Data Flow, a SparkSession já vem pré-configurada pelo serviço.
# from validate_abt import validate_abt_v4  # TODO: inlinar validação quando necessário

# =============================================================================
# CONFIGURACAO OCI DATA FLOW
# =============================================================================
namespace = sys.argv[1] if len(sys.argv) > 1 else "default_namespace"

DEFAULT_GOLD_ABT_V3_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v3/"
DEFAULT_SILVER_CADASTRO_PATH = f"oci://hackathon-2025-silver-layer@{namespace}/cadastro/"
DEFAULT_OUTPUT_PATH = f"oci://hackathon-2025-gold-layer@{namespace}/abt_v4/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v4"

# --- Otimizacao small files ---
BYTES_PER_ROW_ESTIMATE = 120   # ~120 bytes/row (95 features)
TARGET_FILE_SIZE_MB = 64       # Target ~64 MB por arquivo

# Variaveis Cadastro esperadas (var_02 a var_25 = 24 variaveis)
CADASTRO_VAR_COLUMNS = [f"var_{i}" for i in range(2, 26)]
# =============================================================================

def build_abt_v4(df_abt_v3, df_cadastro):
    """
    Constroi ABT v4: estende v3 com features Cadastro (idade, CEP, var_02-25).

    Inclusoes v3 (mantidas):
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Features v2: score_02_adj, flag_score02_missing
    - Features v3: var_26_adj a var_93_adj + flags (68 vars Telco)
    - Metadados: prod, flag_mig2
    - Gold metadata: gold_version, gold_build_date, gold_feature_blocks

    Novas inclusoes em v4:
    - IDADE_ANOS: Idade derivada (numerica)
    - FLAG_IDADE_MENOR_18: Sanity check (< 18)
    - FLAG_IDADE_MUITO_ALTA: Outlier check (> 100)
    - CEP_3_DIGITOS: Regional proxy (string)
    - FLAG_CEP_MISSING: Missing indicator
    - STATUSRF: Status cadastral (categorico)
    - VAR_02 a VAR_25: 24 vars cadastrais
    - Flag para cada var_* de Cadastro
    - Atualiza gold_feature_blocks: "score_01,score_02,telco,cadastro"

    Exclusoes (anti-leakage):
    - FPD_INT nao pode ser feature
    - FLAG_INSTALACAO nao pode ser feature
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

    # Adicionar variaveis demograficas
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

    # Step 3: JOIN LEFT ABT v3 + Cadastro (v3 e spine)
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

        # LABELS (auditoria - nao sao features!)
        "flag_instalacao_int", "fpd_int",

        # FEATURES v1 (Score_01)
        "score_01_adj", "flag_score01_missing",

        # FEATURES v2 (Score_02)
        "score_02_adj", "flag_score02_missing",

        # FEATURES v3 (Telco 68 vars)
        *[f"var_{i}_adj" for i in range(26, 94)],
        *[f"flag_var_{i}_missing" for i in range(26, 94)],

        # FEATURES v4 (Cadastro - Demograficas)
        "idade_anos", "flag_idade_menor_18", "flag_idade_muito_alta",
        "cep_3_digitos", "flag_cep_missing", "statusrf",

        # FEATURES v4 (Cadastro - Variaveis anonimizadas)
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
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrao.")
        class Args:
            gold_abt_v3_path = DEFAULT_GOLD_ABT_V3_PATH
            silver_cadastro_path = DEFAULT_SILVER_CADASTRO_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()

    spark = SparkSession.builder.appName("abt_v4_optz_sf").getOrCreate()

    # =========================================================================
    # 1) LEITURA GOLD ABT v3 (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Gold ABT v3 (Spine): {args.gold_abt_v3_path}")
    try:
        df_abt_v3 = spark.read.format(args.format).load(args.gold_abt_v3_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA ABT v3: {e}")
        sys.exit(1)

    # =========================================================================
    # 2) LEITURA SILVER CADASTRO (ENRIQUECIMENTO)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Cadastro (Enriquecimento): {args.silver_cadastro_path}")
    try:
        df_cadastro = spark.read.format(args.format).load(args.silver_cadastro_path)
    except Exception as e:
        print(f"!!! ERRO CRITICO NA LEITURA CADASTRO: {e}")
        sys.exit(1)

    # =========================================================================
    # 3) BUILD ABT v4 + ESCRITA — ACTION 1
    # =========================================================================
    print(">>> [Transform] Construindo ABT v4 (Score_01 + Score_02 + Telco + Cadastro)...")
    df_abt = build_abt_v4(df_abt_v3, df_cadastro)

    print(f">>> [Escrita] Salvando Gold ABT v4 (Delta): {args.output_path}")

    # --- Coalesce dinamico (otimizacao small files) ---
    count_abt = df_abt.count()
    estimated_size_mb = count_abt * BYTES_PER_ROW_ESTIMATE / (1024 * 1024)
    num_output_files = max(1, int(estimated_size_mb / TARGET_FILE_SIZE_MB))
    print(f">>> [Otimizacao] coalesce({num_output_files}) — ~{TARGET_FILE_SIZE_MB}MB/arquivo")
    print(f">>> [Otimizacao] {count_abt:,} registros, ~{estimated_size_mb:.0f} MB estimados")

    df_abt.coalesce(num_output_files) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
    print(">>> [Escrita] ABT v4 gravada com sucesso.")

    # =========================================================================
    # 4) QUALITY CHECK na ABT JA ESCRITA — ACTION 2
    # =========================================================================
    print(f"\n>>> [Quality] Lendo ABT v4 gravada para validacao: {args.output_path}")
    df_written = spark.read.format("delta").load(args.output_path)

    quality = df_written.agg(
        F.count("*").alias("total"),
        F.countDistinct("num_cpf", "safra").alias("distinct_keys"),
        F.sum(F.when(F.col("num_cpf").isNull(), 1).otherwise(0)).alias("nulos_cpf"),
        F.sum(F.when(F.col("safra").isNull(), 1).otherwise(0)).alias("nulos_safra"),
        # Anti-leakage gates
        F.sum(F.when(F.col("flag_instalacao_int") == 1, 1).otherwise(0)).alias("instalados"),
        F.sum(F.when(F.col("flag_instalacao_int") == 0, 1).otherwise(0)).alias("nao_instalados"),
        F.sum(F.when(F.col("fpd_int") == 1, 1).otherwise(0)).alias("fpd_1"),
        F.sum(F.when(F.col("fpd_int") == 0, 1).otherwise(0)).alias("fpd_0"),
        F.sum(F.when(
            (F.col("fpd_int").isNotNull()) & (F.col("flag_instalacao_int") == 0), 1
        ).otherwise(0)).alias("fpd_sem_instalacao"),
        # Cobertura de features v1 e v2
        F.sum(F.when(F.col("score_01_adj").isNull(), 1).otherwise(0)).alias("score01_null"),
        F.sum(F.when(F.col("score_02_adj").isNull(), 1).otherwise(0)).alias("score02_null"),
        # Cobertura Telco: match via var_26_adj + cobertura agregada 68 vars
        F.sum(F.when(F.col("var_26_adj").isNotNull(), 1).otherwise(0)).alias("telco_match"),
        *[F.sum(F.when(F.col(f"var_{i}_adj").isNotNull(), 1).otherwise(0)).alias(f"telco_{i}_nn") for i in range(26, 94)],
        # Cobertura Cadastro: match via idade_anos + cobertura agregada 24 vars
        F.sum(F.when(F.col("idade_anos").isNotNull(), 1).otherwise(0)).alias("cadastro_match"),
        *[F.sum(F.when(F.col(f"var_{i:02d}").isNotNull(), 1).otherwise(0)).alias(f"cad_{i:02d}_nn") for i in range(2, 26)],
    ).collect()[0]

    count_out       = quality["total"]
    distinct_keys   = quality["distinct_keys"]
    nulos_cpf       = quality["nulos_cpf"]
    nulos_safra     = quality["nulos_safra"]
    instalados      = quality["instalados"]
    nao_inst        = quality["nao_instalados"]
    fpd_1           = quality["fpd_1"]
    fpd_0           = quality["fpd_0"]
    fpd_sem_inst    = quality["fpd_sem_instalacao"]
    score01_null    = quality["score01_null"]
    score02_null    = quality["score02_null"]
    telco_match     = quality["telco_match"]
    telco_nn_total  = sum(quality[f"telco_{i}_nn"] for i in range(26, 94))
    cadastro_match  = quality["cadastro_match"]
    cadastro_nn_total = sum(quality[f"cad_{i:02d}_nn"] for i in range(2, 26))

    score01_cov        = 100 * (count_out - score01_null) / count_out if count_out > 0 else 0
    score02_cov        = 100 * (count_out - score02_null) / count_out if count_out > 0 else 0
    telco_match_pct    = 100 * telco_match / count_out if count_out > 0 else 0
    telco_cov          = 100 * telco_nn_total / (68 * count_out) if count_out > 0 else 0
    cadastro_match_pct = 100 * cadastro_match / count_out if count_out > 0 else 0
    cadastro_cov       = 100 * cadastro_nn_total / (24 * count_out) if count_out > 0 else 0

    print("\n" + "="*80)
    print(">>> [Quality] RELATORIO DE QUALIDADE - ABT v4 (Score_01 + Score_02 + Telco + Cadastro)")
    print("="*80)
    print(f"\n    Registros ABT (grain 1:1 CPF+SAFRA):    {count_out:>12,}")
    print(f"    Chaves distintas (num_cpf+safra):       {distinct_keys:>12,}  [esperado = total]")
    print(f"    Unicidade OK:                           {'SIM' if count_out == distinct_keys else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 1 - NUM_CPF nulos:                 {nulos_cpf:>12,}  [esperado = 0]")
    print(f"    Gate 2 - SAFRA nulos:                   {nulos_safra:>12,}  [esperado = 0]")
    print(f"\n    FLAG_INSTALACAO = 1 (instalados):       {instalados:>12,}  ({100*instalados/count_out:.2f}%)")
    print(f"    FLAG_INSTALACAO = 0 (nao instalados):  {nao_inst:>12,}  ({100*nao_inst/count_out:.2f}%)")
    print(f"\n    FPD = 1 (default):                      {fpd_1:>12,}  ({100*fpd_1/count_out:.2f}%)")
    print(f"    FPD = 0 (nao default):                  {fpd_0:>12,}  ({100*fpd_0/count_out:.2f}%)")
    print(f"\n    Gate 3 - FPD sem FLAG_INSTALACAO=1:     {fpd_sem_inst:>12,}  [esperado = 0 — anti-leakage]")
    print(f"    Anti-leakage OK:                        {'SIM' if fpd_sem_inst == 0 else 'NAO — VERIFICAR'}")
    print(f"\n    Gate 4 - SCORE_01_ADJ cobertura:        {score01_cov:>11.2f}%  [esperado ~98.18%]")
    print(f"    Gate 5 - SCORE_02_ADJ cobertura:        {score02_cov:>11.2f}%  [esperado ~99.95%]")
    print(f"\n    Gate 6 - Telco match (LEFT JOIN):       {telco_match:>12,}  ({telco_match_pct:.2f}%)  [esperado ~35.46%]")
    print(f"    Gate 7 - Telco cobertura agregada:      {telco_cov:>11.2f}%  [68 vars]")
    print(f"\n    Gate 8 - Cadastro match (LEFT JOIN):    {cadastro_match:>12,}  ({cadastro_match_pct:.2f}%)  [esperado ~35-40%]")
    print(f"    Gate 9 - Cadastro cobertura agregada:   {cadastro_cov:>11.2f}%  [24 vars]")

    print("\n" + "="*80)
    print(f"ABT v4 PRONTA PARA MODELAGEM")
    print(f"  - Versao:          {GOLD_VERSION}")
    print(f"  - Feature blocks:  Score_01, Score_02, Telco (68 vars), Cadastro (24 vars)")
    print(f"  - Registros:       {count_out:,}")
    print(f"  - Actions:         2 (join+write + agg ABT escrita)")
    print(f"  - Grao:            1:1 NUM_CPF + SAFRA")
    print(f"  - Target:          FPD_INT (observado em FLAG_INSTALACAO=1)")
    print(f"  - Proximo passo:   abt_v5_builder.py")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
