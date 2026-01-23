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


def validate_abt_v6(df_abt, count_abt_v5):
    """
    Validações obrigatórias para ABT v6 (Score + Telco + Cadastro + Recarga + Pagamento + Atraso).
    
    Gates:
    1-8: Herdados de v5 (unicidade, anti-leakage, cobertura v1-v5)
    9-10: De v5 (Recarga sanidade)
    11-12: Novos v6 (Pagamento e Atraso cobertura)
    13-14: Novos v6 (Sanidade de valores)
    
    Levanta AssertionError se algum gate falhar.
    """
    print("\n>>> [Validate ABT v6] Iniciando 14 gates de qualidade...\n")
    
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
    print(f"    ✓ PASS: {total:,} == {unique_key:,} (sem duplicatas)")
    
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
        print(f"    FLAG_INSTALACAO={flag_val}: {count_val:>12,} ({pct:>6.2f}%)")
    
    flag_values = {row["flag_instalacao_int"] for row in dist_flag}
    if flag_values != {0, 1}:
        raise AssertionError(
            f"FALHA Gate 4 - FLAG_INSTALACAO não contém ambos 0 e 1! "
            f"Valores presentes: {flag_values}"
        )
    print(f"    ✓ PASS: Ambos FLAG_INSTALACAO=0 e =1 presentes")
    
    # =========================================================================
    # GATE 5: Score_01 cobertura ≥90%
    # =========================================================================
    print("  [Gate 5] Verificando completude de SCORE_01_ADJ...")
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    score01_valid = total - score01_null
    score01_pct = score01_valid * 100 / total
    
    print(f"    SCORE_01_ADJ: {score01_valid:>12,} / {total:,} ({score01_pct:>6.2f}%)")
    
    if score01_pct < 90:
        raise AssertionError(
            f"FALHA Gate 5 - SCORE_01_ADJ com cobertura baixa! "
            f"{score01_pct:.2f}% < 90%"
        )
    print(f"    ✓ PASS: SCORE_01_ADJ em {score01_pct:.2f}% dos registros")
    
    # =========================================================================
    # GATE 6: Score_02 cobertura ≥40%
    # =========================================================================
    print("  [Gate 6] Verificando completude de SCORE_02_ADJ...")
    score02_null = df_abt.filter(F.col("score_02_adj").isNull()).count()
    score02_valid = total - score02_null
    score02_pct = score02_valid * 100 / total
    
    print(f"    SCORE_02_ADJ: {score02_valid:>12,} / {total:,} ({score02_pct:>6.2f}%)")
    
    if score02_pct < 40:
        raise AssertionError(
            f"FALHA Gate 6 - SCORE_02_ADJ com cobertura muito baixa! "
            f"{score02_pct:.2f}% < 40%"
        )
    print(f"    ✓ PASS: SCORE_02_ADJ em {score02_pct:.2f}% dos registros")
    
    # =========================================================================
    # GATE 7: Telco cobertura ≥20%
    # =========================================================================
    print("  [Gate 7] Verificando completude de Telco (var_26-93)...")
    
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
    
    print(f"    Telco (var_26-93): {telco_total_cells - telco_null_cells:>12,} / {telco_total_cells:,} ({telco_pct:>6.2f}%)")
    
    if telco_pct < 20:
        raise AssertionError(
            f"FALHA Gate 7 - Telco com cobertura baixa! "
            f"{telco_pct:.2f}% < 20%"
        )
    print(f"    ✓ PASS: Telco em {telco_pct:.2f}% das células")
    
    # =========================================================================
    # GATE 8: Cadastro cobertura ≥20%
    # =========================================================================
    print("  [Gate 8] Verificando completude de Cadastro (age, var_02-25)...")
    
    cadastro_cols = ["age"] + [f"var_{i}_adj" for i in range(2, 26)]
    cadastro_total_cells = 0
    cadastro_null_cells = 0
    
    for col in cadastro_cols:
        if col in df_abt.columns:
            cadastro_total_cells += total
            null_count = df_abt.filter(F.col(col).isNull()).count()
            cadastro_null_cells += null_count
    
    if cadastro_total_cells > 0:
        cadastro_pct = ((cadastro_total_cells - cadastro_null_cells) / cadastro_total_cells) * 100
    else:
        cadastro_pct = 0
    
    print(f"    Cadastro (age+var_02-25): {cadastro_total_cells - cadastro_null_cells:>12,} / {cadastro_total_cells:,} ({cadastro_pct:>6.2f}%)")
    
    if cadastro_pct < 20:
        raise AssertionError(
            f"FALHA Gate 8 - Cadastro com cobertura baixa! "
            f"{cadastro_pct:.2f}% < 20%"
        )
    print(f"    ✓ PASS: Cadastro em {cadastro_pct:.2f}% das células")
    
    # =========================================================================
    # GATE 9: Recarga cobertura ≥5%
    # =========================================================================
    print("  [Gate 9] Verificando completude de Recarga...")
    recarga_non_null = df_abt.filter(F.col("qtd_recargas_m1") > 0).count()
    recarga_pct = (recarga_non_null / total) * 100
    
    print(f"    Clientes com Recarga: {recarga_non_null:>12,} / {total:,} ({recarga_pct:>6.2f}%)")
    
    if recarga_pct < 5:
        raise AssertionError(
            f"FALHA Gate 9 - Recarga com cobertura muito baixa! "
            f"{recarga_pct:.2f}% < 5%"
        )
    print(f"    ✓ PASS: Recarga em {recarga_pct:.2f}% dos registros")
    
    # =========================================================================
    # GATE 10: QTD_RECARGAS_M1 sanidade (sem NaN/Inf)
    # =========================================================================
    print("  [Gate 10] Verificando sanidade de QTD_RECARGAS_M1...")
    recarga_bad = df_abt.filter(
        (F.col("qtd_recargas_m1") == F.lit(float('inf'))) |
        (F.col("qtd_recargas_m1") == F.lit(float('-inf'))) |
        (F.isnan(F.col("qtd_recargas_m1")))
    ).count()
    
    if recarga_bad > 0:
        raise AssertionError(
            f"FALHA Gate 10 - {recarga_bad} registros com NaN/Inf em QTD_RECARGAS"
        )
    print(f"    ✓ PASS: Sem NaN/Inf em QTD_RECARGAS_M1")
    
    # =========================================================================
    # GATE 11: Pagamento cobertura ≥2%
    # =========================================================================
    print("  [Gate 11] Verificando cobertura de Pagamento...")
    pag_non_null = df_abt.filter(F.col("qtd_itens_pagamento_m1") > 0).count()
    pag_pct = (pag_non_null / total) * 100
    
    print(f"    Clientes com Pagamento: {pag_non_null:>12,} / {total:,} ({pag_pct:>6.2f}%)")
    
    if pag_pct < 2:
        raise AssertionError(
            f"FALHA Gate 11 - Pagamento com cobertura muito baixa! "
            f"{pag_pct:.2f}% < 2% (esperado 5-10%)"
        )
    print(f"    ✓ PASS: Pagamento em {pag_pct:.2f}% dos registros (esperado 5-10%)")
    
    # =========================================================================
    # GATE 12: Atraso cobertura ≥10%
    # =========================================================================
    print("  [Gate 12] Verificando cobertura de Atraso...")
    atr_non_null = df_abt.filter(F.col("qtd_faturas_abertas_m1") > 0).count()
    atr_pct = (atr_non_null / total) * 100
    
    print(f"    Clientes com Atraso: {atr_non_null:>12,} / {total:,} ({atr_pct:>6.2f}%)")
    
    if atr_pct < 10:
        raise AssertionError(
            f"FALHA Gate 12 - Atraso com cobertura muito baixa! "
            f"{atr_pct:.2f}% < 10% (esperado 20-30%)"
        )
    print(f"    ✓ PASS: Atraso em {atr_pct:.2f}% dos registros (esperado 20-30%)")
    
    # =========================================================================
    # GATE 13: QTD_ITENS_PAGAMENTO_M1 sanidade
    # =========================================================================
    print("  [Gate 13] Verificando sanidade de QTD_ITENS_PAGAMENTO_M1...")
    pag_bad = df_abt.filter(
        (F.col("qtd_itens_pagamento_m1") < 0) |
        (F.col("qtd_itens_pagamento_m1") > 1000)
    ).count()
    
    if pag_bad > 0:
        raise AssertionError(
            f"FALHA Gate 13 - {pag_bad} registros com QTD_ITENS_PAGAMENTO fora do range [0-1000]"
        )
    
    min_pag = df_abt.agg(F.min("qtd_itens_pagamento_m1")).collect()[0][0]
    max_pag = df_abt.agg(F.max("qtd_itens_pagamento_m1")).collect()[0][0]
    print(f"    Range: [{min_pag}, {max_pag}]")
    print(f"    ✓ PASS: QTD_ITENS_PAGAMENTO_M1 sanidade OK")
    
    # =========================================================================
    # GATE 14: QTD_FATURAS_ABERTAS_M1 sanidade
    # =========================================================================
    print("  [Gate 14] Verificando sanidade de QTD_FATURAS_ABERTAS_M1...")
    atr_bad = df_abt.filter(
        (F.col("qtd_faturas_abertas_m1") < 0) |
        (F.col("qtd_faturas_abertas_m1") > 500)
    ).count()
    
    if atr_bad > 0:
        raise AssertionError(
            f"FALHA Gate 14 - {atr_bad} registros com QTD_FATURAS_ABERTAS fora do range [0-500]"
        )
    
    min_atr = df_abt.agg(F.min("qtd_faturas_abertas_m1")).collect()[0][0]
    max_atr = df_abt.agg(F.max("qtd_faturas_abertas_m1")).collect()[0][0]
    print(f"    Range: [{min_atr}, {max_atr}]")
    print(f"    ✓ PASS: QTD_FATURAS_ABERTAS_M1 sanidade OK")
    
    # =========================================================================
    # SUMÁRIO
    # =========================================================================
    print("\n>>> [Validate ABT v6] TODOS OS 14 GATES PASSARAM ✓")
    print(f"    Total de registros: {total:,}")
    print(f"    Retenção vs ABT v5: {total*100/count_abt_v5:.2f}%")
    print(f"    Cobertura Score_01: {score01_pct:.2f}%")
    print(f"    Cobertura Score_02: {score02_pct:.2f}%")
    print(f"    Cobertura Telco: {telco_pct:.2f}%")
    print(f"    Cobertura Cadastro: {cadastro_pct:.2f}%")
    print(f"    Cobertura Recarga: {recarga_pct:.2f}%")
    print(f"    Cobertura Pagamento: {pag_pct:.2f}%")
    print(f"    Cobertura Atraso: {atr_pct:.2f}%\n")
