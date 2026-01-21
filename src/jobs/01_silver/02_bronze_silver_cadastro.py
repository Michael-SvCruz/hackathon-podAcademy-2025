"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 02_bronze_silver_cadastro.py
OBJETIVO: Transformação Bronze → Silver - Base CADASTRO com CEP e Idade.
--------------------------------------------------------------------------------
DESCRIÇÃO TÉCNICA:
Este script transforma os dados brutos da camada Bronze do Cadastro,
aplicando tipagem explícita, tratamento de datas, sanity checks de idade,
normalização de CEP (regional), deduplicação e quality gates.

CARACTERÍSTICAS ESPECÍFICAS - SILVER CADASTRO:
- Tipagem: strings → int/double com funções safe (null-preserving)
- Datas: parsing tolerante de DATADENASCIMENTO (dd/MM/yyyy)
- Idade: derivação com sanity check (menor de idade < 18 anos, outliers > 100 anos)
- CEP_3_digitos: manter como string (categórico/regional) com flag de missing
- Grão saída: 1:1 NUM_CPF + SAFRA
- Deduplicação: por NUM_CPF + SAFRA (deve ser única)
- Quality Gates: 6 validações automáticas para rastreio de qualidade

PIPELINE DE TRANSFORMAÇÃO:
1. Leitura (Bronze → DataFrame)
2. Tipagem (casting seguro com funções reutilizáveis)
3. Normalização (trim, upper, parsing de datas)
4. Derivações (idade, flags de missing/invalid)
5. Deduplicação (por NUM_CPF + SAFRA com row_number)
6. Quality Gates (6 validações com logging)
7. Escrita (Silver → Delta)

SANITY CHECKS - IDADE:
- FLAG_IDADE_INVALIDA: 1 quando DT_NASC não faz parse (DATADENASCIMENTO inválida)
- FLAG_IDADE_MENOR_18: 1 quando IDADE_ANOS < 18 (cliente menor de idade)
- FLAG_IDADE_MUITO_ALTA: 1 quando IDADE_ANOS > 100 (outlier de idade)
- Recomendação: Na Gold, considerar remover FLAG_IDADE_MENOR_18=1 (ineligível)

CEP COMO FEATURE REGIONAL:
- CEP_3_digitos: mantém como string (01xxx=SP, 20xxx=RJ, etc)
- FLAG_CEP_MISSING: 1 quando CEP_3_digitos nulo/vazio (missing moderado em dados)
- Uso futuro: discretização em macrorregião ou estado (v4 ou posterior)

AJUSTES UNITY CATALOG:
- Entrada: tabela bronze_cadastro (ou caminho Delta)
- Saída: tabela silver_cadastro + Delta em volume
- Modo escrita: overwrite com mergeSchema/overwriteSchema
- Registra logs de validação automaticamente

DEPENDÊNCIAS:
- src.utils.spark_utils: get_spark_session(), standardize_column_names(),
  to_int_safe(), to_double_safe()

EXEMPLO DE USO (Databricks):
  %run /Workspace/src/jobs/01_silver/02_bronze_silver_cadastro.py
  ou
  spark-submit src/jobs/01_silver/02_bronze_silver_cadastro.py
--------------------------------------------------------------------------------
"""

import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe
)

# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_BRONZE_TABLE = "hackathon_2025.default.bronze_cadastro"
DEFAULT_BRONZE_PATH = "/Volumes/hackathon_2025/default/bronze/cadastro_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/cadastro_delta/"
DEFAULT_OUTPUT_TABLE = "hackathon_2025.default.silver_cadastro"

# =============================================================================
# CONSTANTS
# =============================================================================
IDADE_MINIMA_VALIDA = 18
IDADE_MAXIMA_ESPERADA = 100

# =============================================================================
# FUNÇÕES DE TRANSFORMAÇÃO
# =============================================================================

def tipagem_base(df):
    """
    Aplica tipagem explícita nas colunas core do Cadastro.
    
    Conversões:
    - NUM_CPF: string (já é)
    - SAFRA: string YYYYMM (já é)
    - DT_SAFRA: date (derivada)
    - FLAG_INSTALACAO, FPD: int (0/1)
    - STATUSRF, PROD, flag_mig2: string (normalizado)
    - DATADENASCIMENTO: string raw → tentativa parse em Silver
    - CEP_3_digitos: string (categórico/regional)
    - var_*: mixed (casting individual conforme tipo)
    """
    print(">>> [Tipagem] Aplicando tipagem explícita...")
    
    # Derive DT_SAFRA
    df = df.withColumn(
        "DT_SAFRA",
        F.to_date(F.concat(F.col("SAFRA"), F.lit("01")), "yyyyMMdd")
    )
    
    # Tipagem de labels e metadados
    df = df.withColumn("FLAG_INSTALACAO_INT", to_int_safe(F.col("FLAG_INSTALACAO")))
    df = df.withColumn("FPD_INT", to_int_safe(F.col("FPD")))
    
    # Normalização de strings categóricas
    df = df.withColumn("PROD", F.trim(F.upper(F.col("PROD"))))
    df = df.withColumn("flag_mig2", F.trim(F.upper(F.col("flag_mig2"))))
    df = df.withColumn("STATUSRF", F.trim(F.upper(F.col("STATUSRF"))))
    
    # CEP como categórico (regional)
    df = df.withColumn("CEP_3_digitos", F.trim(F.col("CEP_3_digitos")))
    df = df.withColumn(
        "FLAG_CEP_MISSING",
        F.when(
            (F.col("CEP_3_digitos").isNull()) | (F.col("CEP_3_digitos") == ""),
            1
        ).otherwise(0)
    )
    
    return df

def parse_e_derivacoes_idade(df):
    """
    Faz parsing tolerante de DATADENASCIMENTO e deriva idade.
    
    Regras:
    - DT_NASC = try_to_date(DATADENASCIMENTO, 'dd/MM/yyyy')
    - IDADE_ANOS = floor(months_between(DT_SAFRA, DT_NASC) / 12)
    - FLAG_IDADE_INVALIDA = 1 quando DATADENASCIMENTO preenchida mas DT_NASC é nulo
    - FLAG_IDADE_MENOR_18 = 1 quando IDADE_ANOS < 18
    - FLAG_IDADE_MUITO_ALTA = 1 quando IDADE_ANOS > 100
    
    Nota: Menor de idade é ineligível para conta telecom. Idades > 100 são outliers.
    """
    print(">>> [Derivações] Calculando idade e flags...")
    
    # Parse tolerante
    df = df.withColumn(
        "DT_NASC",
        F.try_to_date(F.col("DATADENASCIMENTO"), "dd/MM/yyyy")
    )
    
    # Flag de data inválida (preenchida mas não faz parse)
    df = df.withColumn(
        "FLAG_DT_NASC_INVALIDA",
        F.when(
            (F.col("DATADENASCIMENTO").isNotNull()) & 
            (F.trim(F.col("DATADENASCIMENTO")) != "") &
            (F.col("DT_NASC").isNull()),
            1
        ).otherwise(0)
    )
    
    # Derivar idade
    df = df.withColumn(
        "IDADE_ANOS",
        F.when(
            F.col("DT_NASC").isNotNull(),
            F.floor(F.months_between(F.col("DT_SAFRA"), F.col("DT_NASC")) / 12)
        ).otherwise(None)
    )
    
    # Flags de sanity check
    df = df.withColumn(
        "FLAG_IDADE_MENOR_18",
        F.when(F.col("IDADE_ANOS") < IDADE_MINIMA_VALIDA, 1).otherwise(0)
    )
    
    df = df.withColumn(
        "FLAG_IDADE_MUITO_ALTA",
        F.when(F.col("IDADE_ANOS") > IDADE_MAXIMA_ESPERADA, 1).otherwise(0)
    )
    
    return df

def tipagem_variaveis_anonimizadas(df):
    """
    Tipagem das variáveis anonimizadas var_02 a var_25.
    
    Baseado em Data Quality Cadastro:
    - var_03 a var_09: numéricas (casting seguro)
    - var_15, var_22, var_23, var_24: categóricas (manter como string)
    - var_12: possível data (manter raw + tentar parse)
    
    Para esta versão, aplicamos casting seguro em candidatas numéricas.
    Categóricas mantêm como string (normalização com trim).
    """
    print(">>> [Variáveis Anonimizadas] Tipando var_*...")
    
    # Candidatas numéricas
    numeric_vars = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09"]
    for var in numeric_vars:
        if var in df.columns:
            df = df.withColumn(var, to_double_safe(F.col(var)))
    
    # Candidatas categóricas (trim + upper)
    categorical_vars = ["var_15", "var_22", "var_23", "var_24"]
    for var in categorical_vars:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.upper(F.col(var))))
    
    # var_12: possível data (manter raw + derivar parse)
    if "var_12" in df.columns:
        df = df.withColumn("var_12_raw", F.trim(F.col("var_12")))
        df = df.withColumn("DT_var_12", F.try_to_date(F.col("var_12"), "dd/MM/yyyy"))
        df = df.withColumn(
            "FLAG_DT_var_12_INVALIDA",
            F.when(
                (F.col("var_12_raw").isNotNull()) & 
                (F.col("var_12_raw") != "") &
                (F.col("DT_var_12").isNull()),
                1
            ).otherwise(0)
        )
    
    # Outras var_* (var_10, var_11, var_13, var_14, var_16+): manter como string/normalizar
    other_vars = [col for col in df.columns if col.startswith("var_")]
    for var in other_vars:
        if var not in numeric_vars and var not in categorical_vars and var not in ["var_12_raw", "DT_var_12"]:
            df = df.withColumn(var, F.trim(F.col(var)))
    
    return df

def deduplicacao(df):
    """
    Garante grão 1:1 por NUM_CPF + SAFRA.
    
    Estratégia: usa row_number() com partição por NUM_CPF + SAFRA
    e order by metadata_data_ingestao (descending, mais recente first).
    Mantém apenas a primeira (rn=1, mais recente).
    
    Nota: Idealmente Bronze já é 1:1, mas garantimos aqui por segurança.
    """
    print(">>> [Deduplicação] Garantindo grão 1:1 (NUM_CPF + SAFRA)...")
    
    window_spec = Window.partitionBy("NUM_CPF", "SAFRA") \
        .orderBy(F.desc("metadata_data_ingestao"))
    
    df = df.withColumn("rn", F.row_number().over(window_spec))
    df = df.filter(F.col("rn") == 1).drop("rn")
    
    return df

def qualidade_gates(df):
    """
    Implementa 6 quality gates de validação automática.
    
    Gate 1: Unicidade 1:1 NUM_CPF + SAFRA
    Gate 2: Sem NULLs em chaves (NUM_CPF, SAFRA, DT_SAFRA)
    Gate 3: FLAG_INSTALACAO com valores 0 e 1 (balanceado)
    Gate 4: FPD nulo (herdar de Bureau, apenas auditoria)
    Gate 5: IDADE_ANOS não negativa (sanity)
    Gate 6: CEP_3_digitos cobertura (% não missing)
    """
    print(">>> [Validação] Executando 6 quality gates...")
    
    logs = {}
    
    # Gate 1: Unicidade
    total_registros = df.count()
    unique_keys = df.select("NUM_CPF", "SAFRA").distinct().count()
    logs["Gate_1_Unicidade"] = f"Total: {total_registros}, Únicos: {unique_keys}" + \
        (" ✓ PASS" if total_registros == unique_keys else " ✗ FAIL")
    
    # Gate 2: Sem NULLs em chaves
    null_check = df.filter(
        (F.col("NUM_CPF").isNull()) | 
        (F.col("SAFRA").isNull()) | 
        (F.col("DT_SAFRA").isNull())
    ).count()
    logs["Gate_2_NULLs_Chaves"] = f"Nulos: {null_check}" + \
        (" ✓ PASS" if null_check == 0 else " ✗ FAIL")
    
    # Gate 3: FLAG_INSTALACAO balanceado
    flag_dist = df.groupBy("FLAG_INSTALACAO_INT").count().collect()
    flag_summary = {row["FLAG_INSTALACAO_INT"]: row["count"] for row in flag_dist}
    logs["Gate_3_FLAG_INSTALACAO"] = f"Distribuição: {flag_summary}" + \
        (" ✓ PASS" if len(flag_summary) == 2 else " ⚠ WARN (valores ausentes)")
    
    # Gate 4: FPD (em cadastro, espera-se principalmente nulo)
    fpd_not_null = df.filter(F.col("FPD_INT").isNotNull()).count()
    logs["Gate_4_FPD"] = f"Não-nulo: {fpd_not_null}" + \
        (" ⚠ INFO (usar Bureau como fonte de verdade)" if fpd_not_null > 0 else " ⚠ INFO (esperado nulo)")
    
    # Gate 5: IDADE não negativa
    negative_idade = df.filter(F.col("IDADE_ANOS") < 0).count()
    logs["Gate_5_IDADE_Negativa"] = f"Registros negativos: {negative_idade}" + \
        (" ✓ PASS" if negative_idade == 0 else " ✗ FAIL")
    
    # Gate 6: CEP cobertura (% não missing)
    cep_non_missing = df.filter(F.col("FLAG_CEP_MISSING") == 0).count()
    cep_coverage = 100 * cep_non_missing / total_registros if total_registros > 0 else 0
    logs["Gate_6_CEP_Coverage"] = f"Cobertura: {cep_coverage:.1f}%" + \
        (" ✓ PASS" if cep_coverage >= 90 else " ⚠ WARN (< 90%)")
    
    # Print logs
    print("\n" + "="*80)
    print("RELATÓRIO DE QUALIDADE - SILVER CADASTRO")
    print("="*80)
    for gate, resultado in logs.items():
        print(f"  [{gate}] {resultado}")
    print("="*80 + "\n")
    
    return df

# =============================================================================
# MAIN
# =============================================================================

def main():
    # Inicia Spark
    spark = get_spark_session("Silver_Transform_Cadastro")
    
    # -------------------------------------------------------------------------
    # 1. LEITURA (BRONZE)
    # -------------------------------------------------------------------------
    print(f">>> [Leitura] Carregando Bronze Cadastro...")
    
    try:
        # Tenta ler como tabela UC primeiro, depois como path
        try:
            df_bronze = spark.read.table(DEFAULT_BRONZE_TABLE)
            print(f">>> [Info] Lido da tabela Unity Catalog: {DEFAULT_BRONZE_TABLE}")
        except:
            df_bronze = spark.read.format("delta").load(DEFAULT_BRONZE_PATH)
            print(f">>> [Info] Lido do path Delta: {DEFAULT_BRONZE_PATH}")
            
    except Exception as e:
        print(f"!!! ERRO NA LEITURA: {e}")
        sys.exit(1)
    
    count_bronze = df_bronze.count()
    print(f">>> [Info] Registros no Bronze: {count_bronze}")
    
    # -------------------------------------------------------------------------
    # 2. TRANSFORMAÇÃO
    # -------------------------------------------------------------------------
    
    # Padroniza nomes de colunas (snake_case)
    df_silver = standardize_column_names(df_bronze)
    
    # Aplicar transformações em sequência
    df_silver = tipagem_base(df_silver)
    df_silver = parse_e_derivacoes_idade(df_silver)
    df_silver = tipagem_variaveis_anonimizadas(df_silver)
    df_silver = deduplicacao(df_silver)
    
    # -------------------------------------------------------------------------
    # 3. QUALITY GATES
    # -------------------------------------------------------------------------
    df_silver = qualidade_gates(df_silver)
    
    count_silver = df_silver.count()
    print(f">>> [Info] Registros após transformações: {count_silver}")
    
    # -------------------------------------------------------------------------
    # 4. ESCRITA (SILVER - DELTA + TABELA UC)
    # -------------------------------------------------------------------------
    print(f">>> [Escrita] Salvando Silver Cadastro...")
    
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(DEFAULT_OUTPUT_PATH)
    
    print(f">>> [Sucesso] Dados salvos em: {DEFAULT_OUTPUT_PATH}")
    
    # Registra tabela no Unity Catalog
    df_silver.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(DEFAULT_OUTPUT_TABLE)
    
    print(f">>> [Sucesso] Tabela registrada: {DEFAULT_OUTPUT_TABLE}")
    
    # -------------------------------------------------------------------------
    # RESUMO FINAL
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print("RESUMO FINAL - SILVER CADASTRO")
    print("="*80)
    print(f"  Bronze input: {count_bronze} registros")
    print(f"  Silver output: {count_silver} registros")
    print(f"  Delta: {count_bronze - count_silver} registros removidos (duplicatas)")
    print(f"  Output path: {DEFAULT_OUTPUT_PATH}")
    print(f"  Output table: {DEFAULT_OUTPUT_TABLE}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
