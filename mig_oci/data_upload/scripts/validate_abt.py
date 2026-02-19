# Arquivo: mig_oci/data_upload/scripts/validate_abt.py
# Copia direta de src/utils/validate_abt.py (100% portavel para OCI Data Flow)

from pyspark.sql import functions as F

def validate_abt_v1(df_abt, count_silver):
    """
    Validacoes obrigatorias para ABT v1, conforme target_definition.md.

    Gates:
    1. Unicidade: 1:1 por NUM_CPF + SAFRA (sem duplicatas)
    2. FPD observado SO em FLAG_INSTALACAO=1
    3. Sem NULLs nas chaves
    4. Distribuicao razoavel de labels
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
            f"Total: {total}, Chaves unicas: {unique_key}"
        )
    print(f"    PASS: {total} == {unique_key} (sem duplicatas)")

    # =========================================================================
    # GATE 2: FPD observado SO em FLAG_INSTALACAO=1
    # =========================================================================
    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter(
        (F.col("flag_instalacao_int") == 0) &
        (F.col("fpd_int").isNotNull())
    ).count()

    if fpd_where_flag0 > 0:
        raise AssertionError(
            f"FALHA Gate 2 - {fpd_where_flag0} registros tem FPD nao nulo "
            f"onde FLAG_INSTALACAO=0 (NUNCA deve ocorrer!)"
        )
    print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0 ({fpd_where_flag0}=0)")

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
    print(f"    PASS: Nenhum NULL em chaves")

    # =========================================================================
    # GATE 4: Distribuicao de FLAG_INSTALACAO
    # =========================================================================
    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
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
            f"FALHA Gate 4 - FLAG_INSTALACAO nao contem ambos 0 e 1! "
            f"Valores presentes: {flag_values}"
        )
    print(f"    PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")

    # =========================================================================
    # GATE 5: Distribuicao de FPD (SO em FLAG_INSTALACAO=1)
    # =========================================================================
    print("  [Gate 5] Verificando distribuicao de FPD (target)...")
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
            f"FALHA Gate 5 - FPD nao contem ambos 0 e 1! "
            f"Valores presentes: {fpd_values}"
        )
    print(f"    PASS: FPD contem positivos (1) e negativos (0)")

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
    print(f"    PASS: SCORE_01_ADJ presente em {score01_pct:.2f}% dos registros")

    # =========================================================================
    # SUMARIO
    # =========================================================================
    print("\n>>> [Validate] TODOS OS GATES PASSARAM")
    print(f"    Total de registros: {total}")
    print(f"    Registros unicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retencao vs Silver: {total*100/count_silver:.2f}%\n")


def validate_abt_v2(df_abt, count_silver):
    """
    Validacoes obrigatorias para ABT v2, conforme target_definition.md.
    Estende v1 com validacao de Score_02.
    """
    print("\n>>> [Validate ABT v2] Iniciando gates de qualidade...\n")

    total = df_abt.count()

    # GATE 1: Unicidade (1:1 NUM_CPF + SAFRA)
    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()

    if total != unique_key:
        raise AssertionError(
            f"FALHA Gate 1 - Duplicatas detectadas! "
            f"Total: {total}, Chaves unicas: {unique_key}"
        )
    print(f"    PASS: {total} == {unique_key} (sem duplicatas)")

    # GATE 2: FPD observado SO em FLAG_INSTALACAO=1
    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter(
        (F.col("flag_instalacao_int") == 0) &
        (F.col("fpd_int").isNotNull())
    ).count()

    if fpd_where_flag0 > 0:
        raise AssertionError(
            f"FALHA Gate 2 - {fpd_where_flag0} registros tem FPD nao nulo "
            f"onde FLAG_INSTALACAO=0 (NUNCA deve ocorrer!)"
        )
    print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0 ({fpd_where_flag0}=0)")

    # GATE 3: Sem NULLs nas chaves
    print("  [Gate 3] Verificando NULLs nas chaves...")
    null_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    null_safra = df_abt.filter(F.col("safra").isNull()).count()

    if null_cpf > 0 or null_safra > 0:
        raise AssertionError(
            f"FALHA Gate 3 - NULLs em chaves! "
            f"num_cpf NULL: {null_cpf}, safra NULL: {null_safra}"
        )
    print(f"    PASS: Nenhum NULL em chaves")

    # GATE 4: Distribuicao de FLAG_INSTALACAO
    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()

    for row in dist_flag:
        flag_val = row["flag_instalacao_int"]
        count_val = row["count"]
        pct = count_val * 100 / total
        print(f"    FLAG_INSTALACAO={flag_val}: {count_val:>10} ({pct:>6.2f}%)")

    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    if flag_values != {0, 1}:
        raise AssertionError(
            f"FALHA Gate 4 - FLAG_INSTALACAO nao contem ambos 0 e 1! "
            f"Valores presentes: {flag_values}"
        )
    print(f"    PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")

    # GATE 5: Distribuicao de FPD (SO em FLAG_INSTALACAO=1)
    print("  [Gate 5] Verificando distribuicao de FPD (target)...")
    fpd_count_total = df_abt.filter(F.col("fpd_int").isNotNull()).count()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()

    for row in dist_fpd:
        fpd_val = row["fpd_int"]
        count_val = row["count"]
        pct = count_val * 100 / fpd_count_total
        print(f"    FPD={fpd_val}: {count_val:>10} ({pct:>6.2f}%)")

    fpd_values = {row["fpd_int"] for row in dist_fpd}
    if fpd_values != {0, 1}:
        raise AssertionError(
            f"FALHA Gate 5 - FPD nao contem ambos 0 e 1! "
            f"Valores presentes: {fpd_values}"
        )
    print(f"    PASS: FPD contem positivos (1) e negativos (0)")

    # GATE 6: Score_01 presente e com cobertura
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
    print(f"    PASS: SCORE_01_ADJ presente em {score01_pct:.2f}% dos registros")

    # GATE 7: Score_02 presente (NOVO EM V2)
    print("  [Gate 7] Verificando completude de SCORE_02_ADJ (novo em v2)...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total

    print(f"    SCORE_02_ADJ: {score02_valid:>10} / {total} ({score02_pct:>6.2f}%)")

    if score02_pct < 50:
        raise AssertionError(
            f"FALHA Gate 7 - SCORE_02_ADJ com cobertura muito baixa! "
            f"{score02_pct:.2f}% < 50% (esperado minimo 50%)"
        )
    print(f"    PASS: SCORE_02_ADJ presente em {score02_pct:.2f}% dos registros")

    # SUMARIO
    print("\n>>> [Validate] TODOS OS GATES PASSARAM")
    print(f"    Total de registros: {total}")
    print(f"    Registros unicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retencao vs Silver: {total*100/count_silver:.2f}%")
    print(f"    Cobertura Score_02 (novo): {score02_pct:.2f}%\n")


def validate_abt_v3(df_abt, count_silver):
    """
    Validacoes obrigatorias para ABT v3.
    Estende v2 com validacao de Telco (var_26-93).
    """
    print("\n>>> [Validate ABT v3] Iniciando gates de qualidade...\n")

    total = df_abt.count()

    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    if total != unique_key:
        raise AssertionError(f"FALHA Gate 1 - Duplicatas! Total: {total}, Unicas: {unique_key}")
    print(f"    PASS: {total} == {unique_key} (sem duplicatas)")

    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter((F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())).count()
    if fpd_where_flag0 > 0:
        raise AssertionError(f"FALHA Gate 2 - {fpd_where_flag0} registros com FPD em FLAG=0")
    print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0")

    print("  [Gate 3] Verificando NULLs nas chaves...")
    null_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    null_safra = df_abt.filter(F.col("safra").isNull()).count()
    if null_cpf > 0 or null_safra > 0:
        raise AssertionError(f"FALHA Gate 3 - NULLs em chaves! cpf: {null_cpf}, safra: {null_safra}")
    print(f"    PASS: Nenhum NULL em chaves")

    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    for row in dist_flag:
        print(f"    FLAG_INSTALACAO={row['flag_instalacao_int']}: {row['count']:>10} ({row['count']*100/total:>6.2f}%)")
    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    if flag_values != {0, 1}:
        raise AssertionError(f"FALHA Gate 4 - FLAG_INSTALACAO nao contem ambos 0 e 1!")
    print(f"    PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")

    print("  [Gate 5] Verificando distribuicao de FPD (target)...")
    fpd_count_total = df_abt.filter(F.col("fpd_int").isNotNull()).count()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()
    for row in dist_fpd:
        print(f"    FPD={row['fpd_int']}: {row['count']:>10} ({row['count']*100/fpd_count_total:>6.2f}%)")
    fpd_values = {row["fpd_int"] for row in dist_fpd}
    if fpd_values != {0, 1}:
        raise AssertionError(f"FALHA Gate 5 - FPD nao contem ambos 0 e 1!")
    print(f"    PASS: FPD contem positivos (1) e negativos (0)")

    print("  [Gate 6] Verificando completude de SCORE_01_ADJ...")
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score01_valid = total - score01_null
    score01_pct = score01_valid * 100 / total
    print(f"    SCORE_01_ADJ: {score01_valid:>10} / {total} ({score01_pct:>6.2f}%)")
    if score01_pct < 90:
        raise AssertionError(f"FALHA Gate 6 - SCORE_01_ADJ cobertura baixa: {score01_pct:.2f}%")
    print(f"    PASS: SCORE_01_ADJ presente em {score01_pct:.2f}% dos registros")

    print("  [Gate 7] Verificando completude de SCORE_02_ADJ...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total
    print(f"    SCORE_02_ADJ: {score02_valid:>10} / {total} ({score02_pct:>6.2f}%)")
    if score02_pct < 50:
        raise AssertionError(f"FALHA Gate 7 - SCORE_02_ADJ cobertura baixa: {score02_pct:.2f}%")
    print(f"    PASS: SCORE_02_ADJ presente em {score02_pct:.2f}% dos registros")

    # GATE 8: Telco presente com cobertura (NOVO EM V3)
    print("  [Gate 8] Verificando completude de Telco (var_26-93)...")
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
    if telco_pct < 20:
        raise AssertionError(f"FALHA Gate 8 - Telco cobertura baixa: {telco_pct:.2f}%")
    print(f"    PASS: Telco presente em {telco_pct:.2f}% das celulas")

    print("\n>>> [Validate] TODOS OS GATES PASSARAM")
    print(f"    Total de registros: {total}")
    print(f"    Registros unicos: {unique_key}")
    print(f"    Registros com FPD observado: {fpd_count_total}")
    print(f"    Retencao vs Silver: {total*100/count_silver:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco (novo): {telco_pct:.2f}%\n")


def validate_abt_v4(df_abt):
    """
    Validacoes obrigatorias para ABT v4 (Bureau + Scores + Telco + Cadastro).

    Returns:
        dict: {"passed": bool, "gates": {...}}
    """
    print("\n" + "="*80)
    print(">>> [Validate ABT v4] Iniciando gates de qualidade (9 gates total)...")
    print("="*80 + "\n")

    total = df_abt.count()
    gates_result = {}
    all_passed = True

    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    gate1_pass = (total == unique_key)
    gates_result["Gate1_Uniqueness"] = {"passed": gate1_pass, "message": f"Registros: {total}, Chaves unicas: {unique_key}" if gate1_pass else f"DUPLICATAS: {total - unique_key}"}
    if gate1_pass:
        print(f"    PASS: {total} == {unique_key} (sem duplicatas)")
    else:
        print(f"    FAIL: {total} != {unique_key}")
        all_passed = False

    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter((F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())).count()
    gate2_pass = (fpd_where_flag0 == 0)
    gates_result["Gate2_FPD_Leakage"] = {"passed": gate2_pass, "message": f"Registros com FPD em FLAG=0: {fpd_where_flag0}"}
    if gate2_pass:
        print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0")
    else:
        print(f"    FAIL: {fpd_where_flag0} registros com FPD onde FLAG=0")
        all_passed = False

    print("  [Gate 3] Verificando integridade das chaves (num_cpf, safra)...")
    null_keys = df_abt.filter(F.col("num_cpf").isNull() | F.col("safra").isNull()).count()
    gate3_pass = (null_keys == 0)
    gates_result["Gate3_Key_Integrity"] = {"passed": gate3_pass, "message": f"NULLs em chaves: {null_keys}"}
    if gate3_pass:
        print(f"    PASS: Nenhum NULL nas chaves")
    else:
        print(f"    FAIL: {null_keys} NULLs em chaves")
        all_passed = False

    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
    flag_0_count = df_abt.filter(F.col("flag_instalacao_int") == 0).count()
    flag_1_count = df_abt.filter(F.col("flag_instalacao_int") == 1).count()
    flag_0_pct = (flag_0_count / total) * 100 if total > 0 else 0
    flag_1_pct = (flag_1_count / total) * 100 if total > 0 else 0
    gate4_pass = (flag_0_count > 0 and flag_1_count > 0)
    gates_result["Gate4_FLAG_Distribution"] = {"passed": gate4_pass, "message": f"Aprovados: {flag_1_pct:.1f}%, Reprovados: {flag_0_pct:.1f}%"}
    if gate4_pass:
        print(f"    PASS: FLAG=0: {flag_0_pct:.2f}%, FLAG=1: {flag_1_pct:.2f}%")
    else:
        print(f"    FAIL: Missing values in FLAG distribution")
        all_passed = False

    print("  [Gate 5] Verificando distribuicao de FPD (entre FLAG=1)...")
    fpd_count_total = df_abt.filter(F.col("fpd_int") == 1).count()
    fpd_pct = (fpd_count_total / total) * 100 if total > 0 else 0
    good_pct = ((total - fpd_count_total) / total) * 100 if total > 0 else 0
    gate5_pass = (fpd_count_total > 0)
    gates_result["Gate5_FPD_Distribution"] = {"passed": gate5_pass, "message": f"Bons: {good_pct:.1f}%, Risco: {fpd_pct:.1f}%"}
    if gate5_pass:
        print(f"    PASS: FPD=0: {good_pct:.2f}%, FPD=1: {fpd_pct:.2f}%")
    else:
        print(f"    FAIL: No FPD positive cases")
        all_passed = False

    print("  [Gate 6] Verificando cobertura Score_01...")
    score01_count = df_abt.filter(F.col("score_01_adj").isNotNull()).count()
    score01_pct = (score01_count / total) * 100 if total > 0 else 0
    gate6_pass = (score01_pct >= 95)
    gates_result["Gate6_Score01_Coverage"] = {"passed": gate6_pass, "message": f"Score_01 cobertura: {score01_pct:.2f}%"}
    if gate6_pass:
        print(f"    PASS: Score_01 presente em {score01_pct:.2f}%")
    else:
        print(f"    FAIL: Score_01 cobertura baixa: {score01_pct:.2f}%")
        all_passed = False

    print("  [Gate 7] Verificando cobertura Score_02...")
    score02_count = df_abt.filter(F.col("score_02_adj").isNotNull()).count()
    score02_pct = (score02_count / total) * 100 if total > 0 else 0
    gate7_pass = (score02_pct >= 99)
    gates_result["Gate7_Score02_Coverage"] = {"passed": gate7_pass, "message": f"Score_02 cobertura: {score02_pct:.2f}%"}
    if gate7_pass:
        print(f"    PASS: Score_02 presente em {score02_pct:.2f}%")
    else:
        print(f"    FAIL: Score_02 cobertura baixa: {score02_pct:.2f}%")
        all_passed = False

    print("  [Gate 8] Verificando cobertura Telco (var_26-93)...")
    telco_total_cells = 0
    telco_null_cells = 0
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            telco_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_null_cells += null_count
    telco_pct = ((telco_total_cells - telco_null_cells) / telco_total_cells) * 100 if telco_total_cells > 0 else 0
    gate8_pass = (telco_pct >= 20)
    gates_result["Gate8_Telco_Coverage"] = {"passed": gate8_pass, "message": f"Telco cobertura: {telco_pct:.2f}%"}
    if gate8_pass:
        print(f"    PASS: Telco presente em {telco_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Telco cobertura baixa: {telco_pct:.2f}%")
        all_passed = False

    print("  [Gate 9] Verificando cobertura Cadastro (novo em v4)...")
    cadastro_cols = [f"var_{i}" for i in range(2, 26)]
    cadastro_total_cells = 0
    cadastro_null_cells = 0
    for var_col in cadastro_cols:
        if var_col in df_abt.columns:
            cadastro_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            cadastro_null_cells += null_count
    cadastro_pct = ((cadastro_total_cells - cadastro_null_cells) / cadastro_total_cells) * 100 if cadastro_total_cells > 0 else 0
    gate9_pass = (cadastro_pct >= 20)
    gates_result["Gate9_Cadastro_Coverage"] = {"passed": gate9_pass, "message": f"Cadastro cobertura: {cadastro_pct:.2f}%"}
    if gate9_pass:
        print(f"    PASS: Cadastro presente em {cadastro_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Cadastro cobertura baixa: {cadastro_pct:.2f}%")
        all_passed = False

    print("\n" + "="*80)
    if all_passed:
        print(">>> [Validate v4] TODOS OS 9 GATES PASSARAM!")
    else:
        print(">>> [Validate v4] ALGUNS GATES FALHARAM")
    print("="*80)
    print(f"    Total de registros: {total:,}")
    print(f"    Registros unicos: {unique_key:,}")
    print(f"    Cobertura Score_01: {score01_pct:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco: {telco_pct:.2f}%")
    print(f"    Cobertura Cadastro: {cadastro_pct:.2f}%\n")

    return {"passed": all_passed, "gates": gates_result}


def validate_abt_v5(df_abt, count_v4):
    """
    Validacoes obrigatorias para ABT v5, conforme target_definition.md.
    Gates (10 no total): v4 gates + Recarga cobertura + QTD_RECARGAS_M1 sanidade.
    """
    print("\n>>> [Validate ABT v5] Iniciando gates de qualidade...\n")

    total = df_abt.count()
    all_passed = True
    gates_result = {}

    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    gate1_pass = (total == unique_key)
    gates_result["Gate1_Uniqueness"] = {"passed": gate1_pass, "message": f"Total: {total}, Chaves unicas: {unique_key}"}
    if gate1_pass:
        print(f"    PASS: {total} == {unique_key} (sem duplicatas)")
    else:
        print(f"    FAIL: Duplicatas detectadas! Total: {total}, Chaves: {unique_key}")
        all_passed = False

    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter((F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())).count()
    gate2_pass = (fpd_where_flag0 == 0)
    gates_result["Gate2_FPD_AntiLeakage"] = {"passed": gate2_pass, "message": f"FPD nao-nulo com FLAG=0: {fpd_where_flag0}"}
    if gate2_pass:
        print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0")
    else:
        print(f"    FAIL: {fpd_where_flag0} registros tem FPD nao nulo com FLAG=0!")
        all_passed = False

    print("  [Gate 3] Verificando NULLs nas chaves...")
    null_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    null_safra = df_abt.filter(F.col("safra").isNull()).count()
    gate3_pass = (null_cpf == 0 and null_safra == 0)
    gates_result["Gate3_Keys_NotNull"] = {"passed": gate3_pass, "message": f"num_cpf NULL: {null_cpf}, safra NULL: {null_safra}"}
    if gate3_pass:
        print(f"    PASS: Nenhum NULL em chaves")
    else:
        print(f"    FAIL: NULLs detectados em chaves!")
        all_passed = False

    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    for row in dist_flag:
        print(f"    FLAG_INSTALACAO={row['flag_instalacao_int']}: {row['count']:>10} ({row['count']*100/total:>6.2f}%)")
    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    gate4_pass = (flag_values == {0, 1})
    gates_result["Gate4_Flag_Distribution"] = {"passed": gate4_pass, "message": f"Valores presentes: {flag_values}"}
    if gate4_pass:
        print(f"    PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")
    else:
        print(f"    FAIL: FLAG_INSTALACAO nao contem ambos 0 e 1!")
        all_passed = False

    print("  [Gate 5] Verificando cobertura Score_01...")
    score01_not_null = df_abt.filter(F.col("score_01_adj").isNotNull()).count()
    score01_pct = (score01_not_null / total) * 100
    gate5_pass = (score01_pct >= 90)
    gates_result["Gate5_Score01_Coverage"] = {"passed": gate5_pass, "message": f"Score_01 cobertura: {score01_pct:.2f}%"}
    if gate5_pass:
        print(f"    PASS: Score_01 presente em {score01_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Score_01 cobertura baixa: {score01_pct:.2f}%")
        all_passed = False

    print("  [Gate 6] Verificando cobertura Score_02...")
    score02_not_null = df_abt.filter(F.col("score_02_adj").isNotNull()).count()
    score02_pct = (score02_not_null / total) * 100
    gate6_pass = (score02_pct >= 40)
    gates_result["Gate6_Score02_Coverage"] = {"passed": gate6_pass, "message": f"Score_02 cobertura: {score02_pct:.2f}%"}
    if gate6_pass:
        print(f"    PASS: Score_02 presente em {score02_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Score_02 cobertura baixa: {score02_pct:.2f}%")
        all_passed = False

    print("  [Gate 7] Verificando cobertura Telco...")
    telco_cols = [f"var_{i}_adj" for i in range(26, 94)]
    telco_total_cells = 0
    telco_null_cells = 0
    for var_col in telco_cols:
        if var_col in df_abt.columns:
            telco_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_null_cells += null_count
    telco_pct = ((telco_total_cells - telco_null_cells) / telco_total_cells) * 100 if telco_total_cells > 0 else 0
    gate7_pass = (telco_pct >= 20)
    gates_result["Gate7_Telco_Coverage"] = {"passed": gate7_pass, "message": f"Telco cobertura: {telco_pct:.2f}%"}
    if gate7_pass:
        print(f"    PASS: Telco presente em {telco_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Telco cobertura baixa: {telco_pct:.2f}%")
        all_passed = False

    print("  [Gate 8] Verificando cobertura Cadastro...")
    cadastro_cols = [f"var_{i}" for i in range(2, 26)]
    cadastro_total_cells = 0
    cadastro_null_cells = 0
    for var_col in cadastro_cols:
        if var_col in df_abt.columns:
            cadastro_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            cadastro_null_cells += null_count
    cadastro_pct = ((cadastro_total_cells - cadastro_null_cells) / cadastro_total_cells) * 100 if cadastro_total_cells > 0 else 0
    gate8_pass = (cadastro_pct >= 20)
    gates_result["Gate8_Cadastro_Coverage"] = {"passed": gate8_pass, "message": f"Cadastro cobertura: {cadastro_pct:.2f}%"}
    if gate8_pass:
        print(f"    PASS: Cadastro presente em {cadastro_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Cadastro cobertura baixa: {cadastro_pct:.2f}%")
        all_passed = False

    print("  [Gate 9] Verificando cobertura Recarga (novo em v5)...")
    recarga_with_events = df_abt.filter(F.col("qtd_recargas_m1") > 0).count()
    recarga_pct = (recarga_with_events / total) * 100
    gate9_pass = (recarga_pct >= 5)
    gates_result["Gate9_Recarga_Coverage"] = {"passed": gate9_pass, "message": f"Recarga cobertura: {recarga_pct:.2f}% (clientes com qtd_recargas_m1 > 0)"}
    if gate9_pass:
        print(f"    PASS: Recarga presente em {recarga_pct:.2f}% das celulas")
    else:
        print(f"    FAIL: Recarga cobertura baixa: {recarga_pct:.2f}%")
        all_passed = False

    print("  [Gate 10] Verificando sanidade de QTD_RECARGAS_M1...")
    qtd_stats = df_abt.select(
        F.min(F.col("qtd_recargas_m1")).alias("min_qty"),
        F.max(F.col("qtd_recargas_m1")).alias("max_qty"),
        F.avg(F.col("qtd_recargas_m1")).alias("avg_qty"),
        F.count(F.when(F.isnan(F.col("qtd_recargas_m1")), 1)).alias("nan_count"),
        F.count(F.when((F.col("qtd_recargas_m1") == F.lit(float('inf'))) |
                       (F.col("qtd_recargas_m1") == F.lit(float('-inf'))), 1)).alias("inf_count")
    ).collect()[0]
    nan_count = qtd_stats["nan_count"] if qtd_stats["nan_count"] else 0
    inf_count = qtd_stats["inf_count"] if qtd_stats["inf_count"] else 0
    min_qty = qtd_stats["min_qty"]
    max_qty = qtd_stats["max_qty"]
    avg_qty = qtd_stats["avg_qty"]
    gate10_pass = (nan_count == 0 and inf_count == 0 and min_qty >= 0)
    gates_result["Gate10_Recarga_Sanity"] = {"passed": gate10_pass, "message": f"Min: {min_qty}, Max: {max_qty}, Avg: {avg_qty:.2f}, NaNs: {nan_count}, Infs: {inf_count}"}
    print(f"    QTD_RECARGAS_M1 stats:")
    print(f"      Min: {min_qty}, Max: {max_qty}, Avg: {avg_qty:.2f}")
    print(f"      NaNs: {nan_count}, Infs: {inf_count}")
    if gate10_pass:
        print(f"    PASS: QTD_RECARGAS_M1 distribuicao sensata")
    else:
        print(f"    FAIL: QTD_RECARGAS_M1 contem valores invalidos!")
        all_passed = False

    print("\n" + "="*80)
    if all_passed:
        print(">>> [Validate v5] TODOS OS 10 GATES PASSARAM!")
    else:
        print(">>> [Validate v5] ALGUNS GATES FALHARAM")
    print("="*80)
    print(f"    Total de registros: {total:,}")
    print(f"    Registros unicos: {unique_key:,}")
    print(f"    Cobertura Score_01: {score01_pct:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco: {telco_pct:.2f}%")
    print(f"    Cobertura Cadastro: {cadastro_pct:.2f}%")
    print(f"    Cobertura Recarga: {recarga_pct:.2f}%\n")

    return {"passed": all_passed, "gates": gates_result}


def validate_abt_v6(df_abt, count_abt_v5):
    """
    Validacoes obrigatorias para ABT v6 (Score + Telco + Cadastro + Recarga + Pagamento + Atraso).
    14 gates de qualidade.
    """
    print("\n>>> [Validate ABT v6] Iniciando 14 gates de qualidade...\n")

    total = df_abt.count()

    print("  [Gate 1] Verificando unicidade (NUM_CPF + SAFRA)...")
    unique_key = df_abt.select("num_cpf", "safra").distinct().count()
    if total != unique_key:
        raise AssertionError(f"FALHA Gate 1 - Duplicatas! Total: {total}, Unicas: {unique_key}")
    print(f"    PASS: {total:,} == {unique_key:,} (sem duplicatas)")

    print("  [Gate 2] Verificando FPD observado apenas em FLAG_INSTALACAO=1...")
    fpd_where_flag0 = df_abt.filter((F.col("flag_instalacao_int") == 0) & (F.col("fpd_int").isNotNull())).count()
    if fpd_where_flag0 > 0:
        raise AssertionError(f"FALHA Gate 2 - {fpd_where_flag0} registros com FPD em FLAG=0")
    print(f"    PASS: FPD sempre nulo em FLAG_INSTALACAO=0 ({fpd_where_flag0}=0)")

    print("  [Gate 3] Verificando NULLs nas chaves...")
    null_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
    null_safra = df_abt.filter(F.col("safra").isNull()).count()
    if null_cpf > 0 or null_safra > 0:
        raise AssertionError(f"FALHA Gate 3 - NULLs em chaves! cpf: {null_cpf}, safra: {null_safra}")
    print(f"    PASS: Nenhum NULL em chaves")

    print("  [Gate 4] Verificando distribuicao de FLAG_INSTALACAO...")
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    for row in dist_flag:
        print(f"    FLAG_INSTALACAO={row['flag_instalacao_int']}: {row['count']:>12,} ({row['count']*100/total:>6.2f}%)")
    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    if flag_values != {0, 1}:
        raise AssertionError(f"FALHA Gate 4 - FLAG_INSTALACAO nao contem ambos 0 e 1!")
    print(f"    PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")

    print("  [Gate 5] Verificando completude de SCORE_01_ADJ...")
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score01_valid = total - score01_null
    score01_pct = score01_valid * 100 / total
    print(f"    SCORE_01_ADJ: {score01_valid:>12,} / {total:,} ({score01_pct:>6.2f}%)")
    if score01_pct < 90:
        raise AssertionError(f"FALHA Gate 5 - SCORE_01_ADJ cobertura baixa: {score01_pct:.2f}%")
    print(f"    PASS: SCORE_01_ADJ em {score01_pct:.2f}% dos registros")

    print("  [Gate 6] Verificando completude de SCORE_02_ADJ...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total
    print(f"    SCORE_02_ADJ: {score02_valid:>12,} / {total:,} ({score02_pct:>6.2f}%)")
    if score02_pct < 40:
        raise AssertionError(f"FALHA Gate 6 - SCORE_02_ADJ cobertura baixa: {score02_pct:.2f}%")
    print(f"    PASS: SCORE_02_ADJ em {score02_pct:.2f}% dos registros")

    print("  [Gate 7] Verificando completude de Telco (var_26-93)...")
    telco_total_cells = 0
    telco_null_cells = 0
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            telco_total_cells += total
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_null_cells += null_count
    telco_pct = ((telco_total_cells - telco_null_cells) / telco_total_cells) * 100 if telco_total_cells > 0 else 0
    print(f"    Telco (var_26-93): {telco_total_cells - telco_null_cells:>12,} / {telco_total_cells:,} ({telco_pct:>6.2f}%)")
    if telco_pct < 20:
        raise AssertionError(f"FALHA Gate 7 - Telco cobertura baixa: {telco_pct:.2f}%")
    print(f"    PASS: Telco em {telco_pct:.2f}% das celulas")

    print("  [Gate 8] Verificando completude de Cadastro (age, var_02-25)...")
    cadastro_cols = ["idade_anos", "cep_3_digitos", "statusrf"] + [f"var_{i:02d}" for i in range(2, 26)]
    cadastro_total_cells = 0
    cadastro_non_null_cells = 0
    cols_found = 0
    for col in cadastro_cols:
        if col in df_abt.columns:
            cols_found += 1
            cadastro_total_cells += total
            non_null_count = df_abt.filter(F.col(col).isNotNull()).count()
            cadastro_non_null_cells += non_null_count
    cadastro_pct = (cadastro_non_null_cells / cadastro_total_cells) * 100 if cadastro_total_cells > 0 else 0
    print(f"    Cadastro (age+var_02-25): {cadastro_non_null_cells:>12,} / {cadastro_total_cells:,} ({cadastro_pct:>6.2f}%) [{cols_found} colunas encontradas]")
    if cadastro_total_cells > 0 and cadastro_pct < 5:
        raise AssertionError(f"FALHA Gate 8 - Cadastro cobertura baixa: {cadastro_pct:.2f}%")
    print(f"    PASS: Cadastro em {cadastro_pct:.2f}% das celulas")

    print("  [Gate 9] Verificando completude de Recarga...")
    recarga_non_null = df_abt.filter(F.col("qtd_recargas_m1") > 0).count()
    recarga_pct = (recarga_non_null / total) * 100
    print(f"    Clientes com Recarga: {recarga_non_null:>12,} / {total:,} ({recarga_pct:>6.2f}%)")
    if recarga_pct < 5:
        raise AssertionError(f"FALHA Gate 9 - Recarga cobertura baixa: {recarga_pct:.2f}%")
    print(f"    PASS: Recarga em {recarga_pct:.2f}% dos registros")

    print("  [Gate 10] Verificando sanidade de QTD_RECARGAS_M1...")
    recarga_bad = df_abt.filter(
        (F.col("qtd_recargas_m1") == F.lit(float('inf'))) |
        (F.col("qtd_recargas_m1") == F.lit(float('-inf'))) |
        (F.isnan(F.col("qtd_recargas_m1")))
    ).count()
    if recarga_bad > 0:
        raise AssertionError(f"FALHA Gate 10 - {recarga_bad} registros com NaN/Inf em QTD_RECARGAS")
    print(f"    PASS: Sem NaN/Inf em QTD_RECARGAS_M1")

    print("  [Gate 11] Verificando cobertura de Pagamento...")
    pag_non_null = df_abt.filter(F.col("qtd_itens_pagamento_m1") > 0).count()
    pag_pct = (pag_non_null / total) * 100
    print(f"    Clientes com Pagamento: {pag_non_null:>12,} / {total:,} ({pag_pct:>6.2f}%)")
    if pag_pct < 2:
        raise AssertionError(f"FALHA Gate 11 - Pagamento cobertura baixa: {pag_pct:.2f}%")
    print(f"    PASS: Pagamento em {pag_pct:.2f}% dos registros (esperado 5-10%)")

    print("  [Gate 12] Verificando cobertura de Atraso...")
    atr_non_null = df_abt.filter(F.col("qtd_faturas_abertas_m1") > 0).count()
    atr_pct = (atr_non_null / total) * 100
    print(f"    Clientes com Atraso: {atr_non_null:>12,} / {total:,} ({atr_pct:>6.2f}%)")
    if atr_pct < 10:
        raise AssertionError(f"FALHA Gate 12 - Atraso cobertura baixa: {atr_pct:.2f}%")
    print(f"    PASS: Atraso em {atr_pct:.2f}% dos registros (esperado 20-30%)")

    print("  [Gate 13] Verificando sanidade de QTD_ITENS_PAGAMENTO_M1...")
    pag_bad = df_abt.filter((F.col("qtd_itens_pagamento_m1") < 0) | (F.col("qtd_itens_pagamento_m1") > 1000)).count()
    if pag_bad > 0:
        raise AssertionError(f"FALHA Gate 13 - {pag_bad} registros com QTD_ITENS_PAGAMENTO fora do range [0-1000]")
    min_pag = df_abt.agg(F.min("qtd_itens_pagamento_m1")).collect()[0][0]
    max_pag = df_abt.agg(F.max("qtd_itens_pagamento_m1")).collect()[0][0]
    print(f"    Range: [{min_pag}, {max_pag}]")
    print(f"    PASS: QTD_ITENS_PAGAMENTO_M1 sanidade OK")

    print("  [Gate 14] Verificando sanidade de QTD_FATURAS_ABERTAS_M1...")
    atr_bad = df_abt.filter((F.col("qtd_faturas_abertas_m1") < 0) | (F.col("qtd_faturas_abertas_m1") > 500)).count()
    if atr_bad > 0:
        raise AssertionError(f"FALHA Gate 14 - {atr_bad} registros com QTD_FATURAS_ABERTAS fora do range [0-500]")
    min_atr = df_abt.agg(F.min("qtd_faturas_abertas_m1")).collect()[0][0]
    max_atr = df_abt.agg(F.max("qtd_faturas_abertas_m1")).collect()[0][0]
    print(f"    Range: [{min_atr}, {max_atr}]")
    print(f"    PASS: QTD_FATURAS_ABERTAS_M1 sanidade OK")

    print("\n>>> [Validate ABT v6] TODOS OS 14 GATES PASSARAM")
    print(f"    Total de registros: {total:,}")
    print(f"    Retencao vs ABT v5: {total*100/count_abt_v5:.2f}%")
    print(f"    Cobertura Score_01: {score01_pct:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco: {telco_pct:.2f}%")
    print(f"    Cobertura Cadastro: {cadastro_pct:.2f}%")
    print(f"    Cobertura Recarga: {recarga_pct:.2f}%")
    print(f"    Cobertura Pagamento: {pag_pct:.2f}%")
    print(f"    Cobertura Atraso: {atr_pct:.2f}%\n")


def validate_abt_v6_1(df_abt, count_abt_v6):
    """
    Validacoes obrigatorias para ABT v6.1 (v6 + Feature Enhancement).
    Gates 1-14 herdados de v6, + 15-17 novos.
    """
    print("\n>>> [Validate ABT v6.1] Iniciando 17 gates de qualidade...\n")

    total = df_abt.count()

    # GATES 1-14: Herdadas de v6
    validate_abt_v6(df_abt, count_abt_v6)

    # GATE 15: DESCONTO_RATE entre 0-1
    print("  [Gate 15] Verificando sanidade de DESCONTO_RATE...")
    invalid_desconto = df_abt.filter(
        (F.col("desconto_rate_m1") < 0) | (F.col("desconto_rate_m1") > 1) |
        (F.col("desconto_rate_m3") < 0) | (F.col("desconto_rate_m3") > 1) |
        (F.col("desconto_rate_m6") < 0) | (F.col("desconto_rate_m6") > 1)
    ).count()
    if invalid_desconto > 0:
        raise AssertionError(f"FALHA Gate 15 - {invalid_desconto} registros com DESCONTO_RATE fora do range [0,1]")
    desconto_m1_notnull = df_abt.filter(F.col("desconto_rate_m1").isNotNull()).count()
    desconto_m1_cov = (desconto_m1_notnull / total) * 100
    desconto_m1_mean = df_abt.filter(F.col("desconto_rate_m1") > 0).agg(F.mean("desconto_rate_m1")).collect()[0][0]
    desconto_m1_max = df_abt.agg(F.max("desconto_rate_m1")).collect()[0][0]
    print(f"    DESCONTO_RATE_M1:")
    print(f"      Cobertura: {desconto_m1_cov:>6.2f}%")
    print(f"      Media (c/ valor): {desconto_m1_mean:.4f}" if desconto_m1_mean else "      Media: N/A")
    print(f"      Maximo: {desconto_m1_max:.4f}")
    print(f"    PASS: DESCONTO_RATE valido (range 0-1)")

    # GATE 16: DELINQUENCY_RATE entre 0-1
    print("  [Gate 16] Verificando sanidade de DELINQUENCY_RATE...")
    invalid_del = df_abt.filter(
        (F.col("delinquency_rate_m1") < 0) | (F.col("delinquency_rate_m1") > 1) |
        (F.col("delinquency_rate_m3") < 0) | (F.col("delinquency_rate_m3") > 1) |
        (F.col("delinquency_rate_m6") < 0) | (F.col("delinquency_rate_m6") > 1)
    ).count()
    if invalid_del > 0:
        raise AssertionError(f"FALHA Gate 16 - {invalid_del} registros com DELINQUENCY_RATE fora do range [0,1]")
    del_m1_notnull = df_abt.filter(F.col("delinquency_rate_m1").isNotNull()).count()
    del_m1_cov = (del_m1_notnull / total) * 100
    del_m1_mean = df_abt.filter(F.col("delinquency_rate_m1") > 0).agg(F.mean("delinquency_rate_m1")).collect()[0][0]
    del_m1_max = df_abt.agg(F.max("delinquency_rate_m1")).collect()[0][0]
    print(f"    DELINQUENCY_RATE_M1:")
    print(f"      Cobertura: {del_m1_cov:>6.2f}%")
    print(f"      Media (c/ valor): {del_m1_mean:.4f}" if del_m1_mean else "      Media: N/A")
    print(f"      Maximo: {del_m1_max:.4f}")
    print(f"    PASS: DELINQUENCY_RATE valido (range 0-1)")

    # GATE 17: MAX_DIAS_ATRASO >= 0
    print("  [Gate 17] Verificando sanidade de MAX_DIAS_ATRASO...")
    invalid_dias = df_abt.filter(
        (F.col("max_dias_atraso_m1") < 0) |
        (F.col("max_dias_atraso_m3") < 0) |
        (F.col("max_dias_atraso_m6") < 0)
    ).count()
    if invalid_dias > 0:
        raise AssertionError(f"FALHA Gate 17 - {invalid_dias} registros com MAX_DIAS_ATRASO < 0")
    dias_m1_positive = df_abt.filter(F.col("max_dias_atraso_m1") > 0).count()
    dias_m1_cov = (dias_m1_positive / total) * 100
    dias_m1_mean = df_abt.filter(F.col("max_dias_atraso_m1") > 0).agg(F.mean("max_dias_atraso_m1")).collect()[0][0]
    dias_m1_max = df_abt.agg(F.max("max_dias_atraso_m1")).collect()[0][0]
    dias_m1_p90 = df_abt.approxQuantile("max_dias_atraso_m1", [0.9], 0.01)[0]
    print(f"    MAX_DIAS_ATRASO_M1:")
    print(f"      Clientes c/ atraso: {dias_m1_cov:>6.2f}%")
    print(f"      Media dias (c/ atraso): {dias_m1_mean:.1f}" if dias_m1_mean else "      Media: N/A")
    print(f"      P90: {dias_m1_p90:.0f}")
    print(f"      Maximo: {dias_m1_max:.0f}")
    print(f"    PASS: MAX_DIAS_ATRASO valido (>= 0)")

    print("\n>>> [Validate ABT v6.1] TODOS OS 17 GATES PASSARAM")
    print(f"    Total de registros: {total:,}")
    print(f"    Retencao vs ABT v6: {total*100/count_abt_v6:.2f}%")
    print(f"\n    Enhancement Features (NOVO em v6.1):")
    print(f"    - DESCONTO_RATE_M1: {desconto_m1_cov:.2f}% cobertura, media {desconto_m1_mean:.4f}" if desconto_m1_mean else f"    - DESCONTO_RATE_M1: {desconto_m1_cov:.2f}% cobertura")
    print(f"    - DELINQUENCY_RATE_M1: {del_m1_cov:.2f}% cobertura, media {del_m1_mean:.4f}" if del_m1_mean else f"    - DELINQUENCY_RATE_M1: {del_m1_cov:.2f}% cobertura")
    print(f"    - MAX_DIAS_ATRASO_M1: {dias_m1_cov:.2f}% clientes c/ atraso, P90 {dias_m1_p90:.0f} dias\n")
