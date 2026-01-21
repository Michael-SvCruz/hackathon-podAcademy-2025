# Arquivo: src/jobs/02_gold/validators/validate_abt.py

from pyspark.sql import functions as F

def validate_abt_v1(df_abt, count_silver):
    """
    Validações obrigatórias para ABT v1, conforme target_definition.md.
    
    Gates:
    1. Unicidade: 1:1 por NUM_CPF + SAFRA (sem duplicatas)
    2. FPD observado SÓ em FLAG_INSTALACAO=1
    3. Sem NULLs nas chaves
    4. Distribuição razoável de labels
    5. Score_01 presente
    
    Levanta AssertionError se algum gate falhar.
    """
    print("\n>>> [Validate ABT v1] Iniciando gates de qualidade...\n")
    
    total = df_abt.count()
    
    # =========================================================================
    # GATE 1: Unicidade (1:1 NUM_CPF + SAFRA)
    # =========================================================================
    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    
    if total != unique_key:
        raise AssertionError(
            f"FALHA Gate 1 - Duplicatas detectadas! "
            f"Total: {total}, Chaves únicas: {unique_key}"
        )
    print(f"    ✓ PASS: {total} == {unique_key} (sem duplicatas)")
    
    # =========================================================================
    # GATE 2: FPD observado SÓ em FLAG_INSTALACAO=1
    # =========================================================================
    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter(
        (F.col("flag_instalacao_int") == 0) & 
        (F.col("fpd_int").isNotNull())
    ).count()
    
    if fpd_where_flag0 > 0:
        raise AssertionError(
            f"FALHA Gate 2 - {fpd_where_flag0} registros têm FPD não nulo "
            f"onde FLAG_INSTALACAO=0 (NUNCA deve ocorrer!)"
        )
    print(f"    ✓ PASS: FPD sempre nulo em FLAG_INSTALACAO=0 ({fpd_where_flag0}=0)")
    
    # =========================================================================
    # GATE 3: Sem NULLs nas chaves
    # =========================================================================
    print("  [Gate 3] Verificando NULLs nas chaves...")
    null_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    null_safra = df_abt.filter(F.col("safra").isNull()).count()
    
    if null_cpf > 0 or null_safra > 0:
        raise AssertionError(
            f"FALHA Gate 3 - NULLs em chaves! "
            f"num_cpf NULL: {null_cpf}, safra NULL: {null_safra}"
        )
    print(f"    ✓ PASS: Nenhum NULL em chaves")
    
    # =========================================================================
    # GATE 4: Distribuição de FLAG_INSTALACAO
    # =========================================================================
    print("  [Gate 4] Verificando distribuição de FLAG_INSTALACAO...")
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    
    for row in dist_flag:
        flag_val = row["flag_instalacao_int"]
        count_val = row["count"]
        pct = count_val * 100 / total
        print(f"    FLAG_INSTALACAO={flag_val}: {count_val:>10} ({pct:>6.2f}%)")
    
    # Gate: ambos os valores devem estar presentes
    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    if flag_values != {0, 1}:
        raise AssertionError(
            f"FALHA Gate 4 - FLAG_INSTALACAO não contém ambos 0 e 1! "
            f"Valores presentes: {flag_values}"
        )
    print(f"    ✓ PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")
    
    # =========================================================================
    # GATE 5: Distribuição de FPD (SÓ em FLAG_INSTALACAO=1)
    # =========================================================================
    print("  [Gate 5] Verificando distribuição de FPD (target)...")
    fpd_count_total = df_abt.filter(F.col("fpd_int").isNotNull()).count()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()
    
    for row in dist_fpd:
        fpd_val = row["fpd_int"]
        count_val = row["count"]
        pct = count_val * 100 / fpd_count_total
        print(f"    FPD={fpd_val}: {count_val:>10} ({pct:>6.2f}%)")
    
    # Gate: FPD deve ter ambos 0 e 1 (para treinar modelo)
    fpd_values = {row["fpd_int"] for row in dist_fpd}
    if fpd_values != {0, 1}:
        raise AssertionError(
            f"FALHA Gate 5 - FPD não contém ambos 0 e 1! "
            f"Valores presentes: {fpd_values}"
        )
    print(f"    ✓ PASS: FPD contém positivos (1) e negativos (0)")
    
    # =========================================================================
    # GATE 6: Score_01 presente e com cobertura
    # =========================================================================
    print("  [Gate 6] Verificando completude de SCORE_01_ADJ...")
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score01_valid = total - score01_null
    score01_pct = score01_valid * 100 / total
    
    print(f"    SCORE_01_ADJ: {score01_valid:>10} / {total} ({score01_pct:>6.2f}%)")
    
    if score01_pct < 90:
        raise AssertionError(
            f"FALHA Gate 6 - SCORE_01_ADJ com cobertura baixa! "
            f"{score01_pct:.2f}% < 90%"
        )
    print(f"    ✓ PASS: SCORE_01_ADJ presente em {score01_pct:.2f}% dos registros")
    
    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print("\n>>> [Validate] TODOS OS GATES PASSARAM ✓")
    print(f"    Total de registros: {total}")
    print(f"    Registros únicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retenção vs Silver: {total*100/count_silver:.2f}%\n")
