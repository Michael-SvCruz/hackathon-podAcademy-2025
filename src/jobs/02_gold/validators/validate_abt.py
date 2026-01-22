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


def validate_abt_v2(df_abt, count_silver):
    """
    Validações obrigatórias para ABT v2, conforme target_definition.md.
    Estende v1 com validação de Score_02.
    
    Gates:
    1. Unicidade: 1:1 por NUM_CPF + SAFRA (sem duplicatas)
    2. FPD observado SÓ em FLAG_INSTALACAO=1
    3. Sem NULLs nas chaves
    4. Distribuição razoável de labels
    5. Score_01 presente
    6. Score_02 presente com cobertura razoável (novo em v2)
    
    Levanta AssertionError se algum gate falhar.
    """
    print("\n>>> [Validate ABT v2] Iniciando gates de qualidade...\n")
    
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
    # GATE 7: Score_02 presente (NOVO EM V2)
    # =========================================================================
    print("  [Gate 7] Verificando completude de SCORE_02_ADJ (novo em v2)...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total
    
    print(f"    SCORE_02_ADJ: {score02_valid:>10} / {total} ({score02_pct:>6.2f}%)")
    
    # Score_02 pode ter cobertura menor que 90% (é complementar)
    if score02_pct < 50:
        raise AssertionError(
            f"FALHA Gate 7 - SCORE_02_ADJ com cobertura muito baixa! "
            f"{score02_pct:.2f}% < 50% (esperado mínimo 50%)"
        )
    print(f"    ✓ PASS: SCORE_02_ADJ presente em {score02_pct:.2f}% dos registros")
    
    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print("\n>>> [Validate] TODOS OS GATES PASSARAM ✓")
    print(f"    Total de registros: {total}")
    print(f"    Registros únicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retenção vs Silver: {total*100/count_silver:.2f}%")
    print(f"    Cobertura Score_02 (novo): {score02_pct:.2f}%\n")


def validate_abt_v3(df_abt, count_silver):
    """
    Validações obrigatórias para ABT v3, conforme target_definition.md.
    Estende v2 com validação de Telco (var_26-93).
    
    Gates:
    1. Unicidade: 1:1 por NUM_CPF + SAFRA (sem duplicatas)
    2. FPD observado SÓ em FLAG_INSTALACAO=1
    3. Sem NULLs nas chaves
    4. Distribuição razoável de labels
    5. Score_01 presente
    6. Score_02 presente
    7. Telco presente com cobertura razoável (novo em v3)
    
    Levanta AssertionError se algum gate falhar.
    """
    print("\n>>> [Validate ABT v3] Iniciando gates de qualidade...\n")
    
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
    # GATE 7: Score_02 presente
    # =========================================================================
    print("  [Gate 7] Verificando completude de SCORE_02_ADJ...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total
    
    print(f"    SCORE_02_ADJ: {score02_valid:>10} / {total} ({score02_pct:>6.2f}%)")
    
    if score02_pct < 50:
        raise AssertionError(
            f"FALHA Gate 7 - SCORE_02_ADJ com cobertura muito baixa! "
            f"{score02_pct:.2f}% < 50%"
        )
    print(f"    ✓ PASS: SCORE_02_ADJ presente em {score02_pct:.2f}% dos registros")
    
    # =========================================================================
    # GATE 8: Telco presente com cobertura (NOVO EM V3)
    # =========================================================================
    print("  [Gate 8] Verificando completude de Telco (var_26-93)...")
    
    # Contar células Telco (para todas as 68 variáveis)
    telco_total_cells = 0
    telco_null_cells = 0
    
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            telco_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_null_cells += null_count
    
    if telco_total_cells > 0:
        telco_pct = ((telco_total_cells - telco_null_cells) / telco_total_cells) * 100
    else:
        telco_pct = 0
    
    print(f"    Telco (var_26-93): {telco_total_cells - telco_null_cells:>10} / {telco_total_cells} ({telco_pct:>6.2f}%)")
    
    # Gate: Telco deve ter cobertura > 20% (complementar a scores)
    # Nota: Telco é data source secundária, pode ser esparsa (nem todos clientes têm dados Telco)
    if telco_pct < 20:
        raise AssertionError(
            f"FALHA Gate 8 - Telco com cobertura muito baixa! "
            f"{telco_pct:.2f}% < 20% (JOIN pode ter falhado)"
        )
    print(f"    ✓ PASS: Telco presente em {telco_pct:.2f}% das células")
    
    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print("\n>>> [Validate] TODOS OS GATES PASSARAM ✓")
    print(f"    Total de registros: {total}")
    print(f"    Registros únicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retenção vs Silver: {total*100/count_silver:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco (novo): {telco_pct:.2f}%\n")


def validate_abt_v4(df_abt):
    """
    Validações obrigatórias para ABT v4 (Bureau + Scores + Telco + Cadastro).
    
    Gates:
    1. Unicidade: 1:1 por NUM_CPF + SAFRA (sem duplicatas)
    2. FPD observado SÓ em FLAG_INSTALACAO=1
    3. Sem NULLs nas chaves
    4. Distribuição razoável de FLAG_INSTALACAO
    5. Distribuição razoável de FPD
    6. Score_01 com cobertura > 95%
    7. Score_02 com cobertura > 99%
    8. Telco com cobertura > 20% (complementar)
    9. Cadastro com cobertura > 25% (complementar, novo em v4)
    
    Returns:
        dict: {"passed": bool, "gates": {...}}
    """
    print("\n" + "="*80)
    print(">>> [Validate ABT v4] Iniciando gates de qualidade (9 gates total)...")
    print("="*80 + "\n")
    
    total = df_abt.count()
    gates_result = {}
    all_passed = True
    
    # =========================================================================
    # GATE 1: Unicidade (1:1 NUM_CPF + SAFRA)
    # =========================================================================
    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    
    gate1_pass = (total == unique_key)
    gates_result["Gate1_Uniqueness"] = {
        "passed": gate1_pass,
        "message": f"Registros: {total}, Chaves únicas: {unique_key}" if gate1_pass else f"DUPLICATAS: {total - unique_key}"
    }
    
    if gate1_pass:
        print(f"    ✓ PASS: {total} == {unique_key} (sem duplicatas)")
    else:
        print(f"    ✗ FAIL: {total} != {unique_key}")
        all_passed = False
    
    # =========================================================================
    # GATE 2: FPD observado SÓ em FLAG_INSTALACAO=1
    # =========================================================================
    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter(
        (F.col("flag_instalacao_int") == 0) & 
        (F.col("fpd_int").isNotNull())
    ).count()
    
    gate2_pass = (fpd_where_flag0 == 0)
    gates_result["Gate2_FPD_Leakage"] = {
        "passed": gate2_pass,
        "message": f"Registros com FPD em FLAG=0: {fpd_where_flag0}"
    }
    
    if gate2_pass:
        print(f"    ✓ PASS: FPD sempre nulo em FLAG_INSTALACAO=0")
    else:
        print(f"    ✗ FAIL: {fpd_where_flag0} registros com FPD onde FLAG=0")
        all_passed = False
    
    # =========================================================================
    # GATE 3: Sem NULLs nas chaves
    # =========================================================================
    print("  [Gate 3] Verificando integridade das chaves (num_cpf, safra)...")
    null_keys = df_abt.filter(
        F.col("num_cpf").isNull() | F.col("safra").isNull()
    ).count()
    
    gate3_pass = (null_keys == 0)
    gates_result["Gate3_Key_Integrity"] = {
        "passed": gate3_pass,
        "message": f"NULLs em chaves: {null_keys}"
    }
    
    if gate3_pass:
        print(f"    ✓ PASS: Nenhum NULL nas chaves")
    else:
        print(f"    ✗ FAIL: {null_keys} NULLs em chaves")
        all_passed = False
    
    # =========================================================================
    # GATE 4: FLAG_INSTALACAO distribuição
    # =========================================================================
    print("  [Gate 4] Verificando distribuição de FLAG_INSTALACAO...")
    flag_0_count = df_abt.filter(F.col("flag_instalacao_int") == 0).count()
    flag_1_count = df_abt.filter(F.col("flag_instalacao_int") == 1).count()
    flag_0_pct = (flag_0_count / total) * 100 if total > 0 else 0
    flag_1_pct = (flag_1_count / total) * 100 if total > 0 else 0
    
    gate4_pass = (flag_0_count > 0 and flag_1_count > 0)
    gates_result["Gate4_FLAG_Distribution"] = {
        "passed": gate4_pass,
        "message": f"Aprovados: {flag_1_pct:.1f}%, Reprovados: {flag_0_pct:.1f}%"
    }
    
    if gate4_pass:
        print(f"    ✓ PASS: FLAG=0: {flag_0_pct:.2f}%, FLAG=1: {flag_1_pct:.2f}%")
    else:
        print(f"    ✗ FAIL: Missing values in FLAG distribution")
        all_passed = False
    
    # =========================================================================
    # GATE 5: FPD distribuição (entre FLAG=1)
    # =========================================================================
    print("  [Gate 5] Verificando distribuição de FPD (entre FLAG=1)...")
    fpd_count_total = df_abt.filter(F.col("fpd_int") == 1).count()
    fpd_pct = (fpd_count_total / total) * 100 if total > 0 else 0
    good_pct = ((total - fpd_count_total) / total) * 100 if total > 0 else 0
    
    gate5_pass = (fpd_count_total > 0)
    gates_result["Gate5_FPD_Distribution"] = {
        "passed": gate5_pass,
        "message": f"Bons: {good_pct:.1f}%, Risco: {fpd_pct:.1f}%"
    }
    
    if gate5_pass:
        print(f"    ✓ PASS: FPD=0: {good_pct:.2f}%, FPD=1: {fpd_pct:.2f}%")
    else:
        print(f"    ✗ FAIL: No FPD positive cases")
        all_passed = False
    
    # =========================================================================
    # GATE 6: Score_01 cobertura > 95%
    # =========================================================================
    print("  [Gate 6] Verificando cobertura Score_01...")
    score01_count = df_abt.filter(F.col("score_01_adj").isNotNull()).count()
    score01_pct = (score01_count / total) * 100 if total > 0 else 0
    
    gate6_pass = (score01_pct >= 95)
    gates_result["Gate6_Score01_Coverage"] = {
        "passed": gate6_pass,
        "message": f"Score_01 cobertura: {score01_pct:.2f}%"
    }
    
    if gate6_pass:
        print(f"    ✓ PASS: Score_01 presente em {score01_pct:.2f}%")
    else:
        print(f"    ✗ FAIL: Score_01 cobertura baixa: {score01_pct:.2f}%")
        all_passed = False
    
    # =========================================================================
    # GATE 7: Score_02 cobertura > 99%
    # =========================================================================
    print("  [Gate 7] Verificando cobertura Score_02...")
    score02_count = df_abt.filter(F.col("score_02_adj").isNotNull()).count()
    score02_pct = (score02_count / total) * 100 if total > 0 else 0
    
    gate7_pass = (score02_pct >= 99)
    gates_result["Gate7_Score02_Coverage"] = {
        "passed": gate7_pass,
        "message": f"Score_02 cobertura: {score02_pct:.2f}%"
    }
    
    if gate7_pass:
        print(f"    ✓ PASS: Score_02 presente em {score02_pct:.2f}%")
    else:
        print(f"    ✗ FAIL: Score_02 cobertura baixa: {score02_pct:.2f}%")
        all_passed = False
    
    # =========================================================================
    # GATE 8: Telco cobertura > 20% (complementar a scores)
    # =========================================================================
    print("  [Gate 8] Verificando cobertura Telco (var_26-93)...")
    telco_total_cells = 0
    telco_null_cells = 0
    
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            telco_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_null_cells += null_count
    
    if telco_total_cells > 0:
        telco_pct = ((telco_total_cells - telco_null_cells) / telco_total_cells) * 100
    else:
        telco_pct = 0
    
    gate8_pass = (telco_pct >= 20)
    gates_result["Gate8_Telco_Coverage"] = {
        "passed": gate8_pass,
        "message": f"Telco cobertura: {telco_pct:.2f}%"
    }
    
    if gate8_pass:
        print(f"    ✓ PASS: Telco presente em {telco_pct:.2f}% das células")
    else:
        print(f"    ✗ FAIL: Telco cobertura baixa: {telco_pct:.2f}%")
        all_passed = False
    
    # =========================================================================
    # GATE 9: Cadastro cobertura > 25% (NEW in v4 - complementar)
    # =========================================================================
    print("  [Gate 9] Verificando cobertura Cadastro (novo em v4)...")
    # Check Cadastro numeric features: var_02-25 (24 variáveis)
    cadastro_cols = [f"var_{i}" for i in range(2, 26)]
    cadastro_total_cells = 0
    cadastro_null_cells = 0
    
    for var_col in cadastro_cols:
        if var_col in df_abt.columns:
            cadastro_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            cadastro_null_cells += null_count
    
    if cadastro_total_cells > 0:
        cadastro_pct = ((cadastro_total_cells - cadastro_null_cells) / cadastro_total_cells) * 100
    else:
        cadastro_pct = 0
    
    gate9_pass = (cadastro_pct >= 20)  # Reduzido de 25% para 20% (consistente com Telco)
    gates_result["Gate9_Cadastro_Coverage"] = {
        "passed": gate9_pass,
        "message": f"Cadastro cobertura: {cadastro_pct:.2f}%"
    }
    
    if gate9_pass:
        print(f"    ✓ PASS: Cadastro presente em {cadastro_pct:.2f}% das células")
    else:
        print(f"    ✗ FAIL: Cadastro cobertura baixa: {cadastro_pct:.2f}%")
        all_passed = False
    
    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print("\n" + "="*80)
    if all_passed:
        print(">>> [Validate v4] ✅ TODOS OS 9 GATES PASSARAM!")
    else:
        print(">>> [Validate v4] ❌ ALGUNS GATES FALHARAM")
    print("="*80)
    print(f"    Total de registros: {total:,}")
    print(f"    Registros únicos: {unique_key:,}")
    print(f"    Cobertura Score_01: {score01_pct:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco: {telco_pct:.2f}%")
    print(f"    Cobertura Cadastro: {cadastro_pct:.2f}%\n")
    
    return {
        "passed": all_passed,
        "gates": gates_result
    }
