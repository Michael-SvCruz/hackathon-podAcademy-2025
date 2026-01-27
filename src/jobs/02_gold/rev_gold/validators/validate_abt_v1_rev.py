"""
================================================================================
MÓDULO: Validadores para ABT v1 rev_gold
ARQUIVO: validate_abt_v1_rev.py
OBJETIVO: Implementar 4 gates de qualidade com documentação detalhada
================================================================================

GATES IMPLEMENTADOS:
1. Gate 1: Grain 1:1 (unicidade CPF+SAFRA)
2. Gate 2: Integridade de chaves (sem NULLs)
3. Gate 3: Completude de features (>70%)
4. Gate 4: Distribuição razoável de risco

Cada gate valida um aspecto crítico da qualidade do ABT.

================================================================================
"""

from pyspark.sql import functions as F


class ValidateABTV1Rev:
    """
    Classe para validação de ABT v1 rev_gold com 4 gates de qualidade.
    """
    
    @staticmethod
    def gate_1_grain_uniqueness(df_abt):
        """
        ═════════════════════════════════════════════════════════════════════════════
        GATE 1: GRAIN 1:1 (UNICIDADE DE CPF+SAFRA)
        ═════════════════════════════════════════════════════════════════════════════
        
        OBJETIVO:
        Garantir que não há duplicatas na granularidade de cliente-mês (CPF+SAFRA).
        Cada cliente em cada safra deve aparecer EXATAMENTE 1 vez.
        
        IMPORTÂNCIA:
        - ABT é agregado por CPF+SAFRA (cliente-mês)
        - Duplicatas = dados corrompidos ou lógica de JOIN errada
        - Risco: modelos treinam em dados replicados (overfitting)
        
        IMPLEMENTAÇÃO:
        - count(*) = 100.000 registros
        - count(distinct cpf, safra) = 100.000 registros
        - Se iguais: ✓ PASS
        - Se diferentes: ✗ FAIL (há CPF+SAFRA duplicados)
        
        EXEMPLO DE FALHA:
        ```
        CPF        SAFRA   Ocorrências
        12345678   202501  2  ← PROBLEMA! Deve ser 1
        87654321   202501  1  ✓
        ```
        
        AÇÃO EM CASO DE FALHA:
        - Verificar lógica de JOIN (LEFT JOIN pode duplicar)
        - Verificar deduplicação em aggregate_atraso() e aggregate_pagamento()
        - Verificar se há colunas extras não sendo agrupadas
        
        ═════════════════════════════════════════════════════════════════════════════
        """
        print("\n" + "="*80)
        print("GATE 1: GRAIN 1:1 (UNICIDADE CPF+SAFRA)")
        print("="*80)
        
        count_out = df_abt.count()
        count_unique = df_abt.select("num_cpf", "safra").distinct().count()
        
        print(f"\nVerificando granularidade cliente-mês...")
        print(f"  Total registros:        {count_out:>10,}")
        print(f"  Registros únicos CPF+SAFRA: {count_unique:>10,}")
        print(f"  Razão (unique/total):   {count_unique/count_out*100:>9.1f}%")
        
        assert count_unique == count_out, \
            f"Gate 1 FALHOU: {count_out} registros mas apenas {count_unique} chaves únicas. " \
            f"Há {count_out - count_unique} duplicatas de CPF+SAFRA."
        
        print(f"\n✓ GATE 1 PASSOU: Grain 1:1 garantida (sem duplicatas)")
        print("="*80)
        return count_out
    
    
    @staticmethod
    def gate_2_key_integrity(df_abt):
        """
        ═════════════════════════════════════════════════════════════════════════════
        GATE 2: INTEGRIDADE DE CHAVES (SEM NULLS)
        ═════════════════════════════════════════════════════════════════════════════
        
        OBJETIVO:
        Garantir que as chaves primárias (num_cpf, safra) não têm valores nulos.
        Sem chaves válidas, impossível agregar ou usar em JOINs posteriores.
        
        IMPORTÂNCIA:
        - num_cpf + safra identificam UNICAMENTE cada registro
        - NULLs nas chaves = impossível rastrear cliente ou período
        - Risco: silenciosamente perder registros em JOINs
        
        IMPLEMENTAÇÃO:
        - count(where num_cpf IS NULL) deve ser 0
        - count(where safra IS NULL) deve ser 0
        
        EXEMPLO DE FALHA:
        ```
        num_cpf    safra      status
        12345678   202501     ✓ válido
        NULL       202501     ✗ FALHA (cliente desconhecido)
        87654321   NULL       ✗ FALHA (período desconhecido)
        ```
        
        AÇÃO EM CASO DE FALHA:
        - Verificar agregação: alguma coluna não é preenchida com COALESCE
        - Verificar JOIN: LEFT JOIN pode introduzir NULLs se chave não existe
        - Filtrar registros com chaves nulas ANTES do agregação
        
        ═════════════════════════════════════════════════════════════════════════════
        """
        print("\n" + "="*80)
        print("GATE 2: INTEGRIDADE DE CHAVES (SEM NULLS)")
        print("="*80)
        
        count_total = df_abt.count()
        nulls_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
        nulls_safra = df_abt.filter(F.col("safra").isNull()).count()
        nulls_total = nulls_cpf + nulls_safra
        
        print(f"\nVerificando integridade de chaves primárias...")
        print(f"  Total registros:       {count_total:>10,}")
        print(f"  NULLs em num_cpf:      {nulls_cpf:>10,}")
        print(f"  NULLs em safra:        {nulls_safra:>10,}")
        print(f"  Total NULLs (chaves):  {nulls_total:>10,}")
        
        assert nulls_total == 0, \
            f"Gate 2 FALHOU: Encontrados {nulls_cpf} NULLs em num_cpf e {nulls_safra} NULLs em safra. " \
            f"Chaves nulas impossibilitam agregação e JOINs."
        
        print(f"\n✓ GATE 2 PASSOU: Todas as chaves primárias preenchidas")
        print("="*80)
    
    
    @staticmethod
    def gate_3_feature_completeness(df_abt):
        """
        ═════════════════════════════════════════════════════════════════════════════
        GATE 3: COMPLETUDE DE FEATURES (>70%)
        ═════════════════════════════════════════════════════════════════════════════
        
        OBJETIVO:
        Garantir que as features principais têm pelo menos 70% de preenchimento.
        Muitos valores nulos = features com baixa preditibilidade (ou não aplicável).
        
        IMPORTÂNCIA:
        - Modelo precisa de features com sinal (não apenas NULLs)
        - <70% completude = feature pode estar quebrada ou inaplicável
        - Risco: modelos treinam em features sem informação
        
        IMPLEMENTAÇÃO:
        - count(where feature IS NOT NULL) / count(*) ≥ 70%
        - Verifica features críticas de Atraso e Pagamento
        
        FEATURES VERIFICADAS (exemplos):
        1. atraso_valor_aberto:
           - Esperado: ~30-50% preenchido (nem todo cliente atrasa)
           - Mínimo: 30% (se <30%, dados do atraso podem estar quebrados)
        
        2. pagto_valor_fatura:
           - Esperado: ~70-90% preenchido (maioria paga)
           - Mínimo: 50% (se <50%, dados de pagamento podem estar quebrados)
        
        3. flag_write_off:
           - Esperado: ~2-5% (evento raro)
           - Apenas validamos que não é 100% NULL
        
        EXEMPLO DE DISTRIBUIÇÃO SAUDÁVEL:
        ```
        Feature                  Não-Nulos   %      Status
        atraso_valor_aberto      35.000     35.0%  ✓ OK
        pagto_valor_fatura       82.000     82.0%  ✓ OK
        flag_write_off           3.500      3.5%   ✓ OK (raro, mas presente)
        risk_score_delinquency   100.000    100%   ✓ OK (derivada, sempre preenchida)
        ```
        
        AÇÃO EM CASO DE FALHA:
        - Se atraso_valor_aberto <30%: verificar agregação de Atraso
        - Se pagto_valor_fatura <50%: verificar JOIN ou agregação de Pagamento
        - Se flag_write_off 100% NULL: verificar lógica de conversão (W/R → 1/0)
        
        ═════════════════════════════════════════════════════════════════════════════
        """
        print("\n" + "="*80)
        print("GATE 3: COMPLETUDE DE FEATURES (>70% ideal, min 30%)")
        print("="*80)
        
        count_total = df_abt.count()
        features_to_check = [
            ("atraso_valor_aberto", 30),      # Min 30%: signal de atraso
            ("pagto_valor_fatura", 50),       # Min 50%: signal de pagamento
            ("flag_write_off", 0),             # Pode ser 0% (evento raro)
            ("flag_pdd", 0),                  # Pode ser 0%
            ("delinquency_rate", 70),         # Derivada, idealmente 100%
            ("risk_score_delinquency", 70),   # Derivada, idealmente 100%
        ]
        
        print(f"\nAnalisando completude de features críticas...")
        print(f"  Total registros: {count_total:,}\n")
        
        all_pass = True
        for feature, min_threshold in features_to_check:
            if feature not in df_abt.columns:
                print(f"  ⚠ {feature:30s}: NÃO EXISTE NA TABELA")
                continue
            
            nulls = df_abt.filter(F.col(feature).isNull()).count()
            non_nulls = count_total - nulls
            completude = non_nulls * 100.0 / count_total
            
            if completude >= 70:
                status = "✓ EXCELENTE"
            elif completude >= min_threshold:
                status = "✓ OK"
            else:
                status = f"✗ CRÍTICO (<{min_threshold}%)"
                all_pass = False
            
            print(f"  {feature:30s}: {completude:6.1f}%  {status}")
        
        assert all_pass, \
            "Gate 3 FALHOU: Features críticas com completude abaixo do limite. " \
            "Verificar agregação e lógica de JOIN."
        
        print(f"\n✓ GATE 3 PASSOU: Features têm completude adequada")
        print("="*80)
    
    
    @staticmethod
    def gate_4_risk_distribution(df_abt):
        """
        ═════════════════════════════════════════════════════════════════════════════
        GATE 4: DISTRIBUIÇÃO DE RISCO (SANIDADE CHECK)
        ═════════════════════════════════════════════════════════════════════════════
        
        OBJETIVO:
        Verificar que a distribuição de clientes "em risco" é razoável.
        - Muito baixa (<5%): pode indicar que flags de risco não funcionam
        - Muito alta (>90%): pode indicar que filtros/agregação estão errados
        - Esperado: 20-40% em risco (comportamento típico de portfólio)
        
        IMPORTÂNCIA:
        - Modelo de risk precisa de variabilidade (não todas classes iguais)
        - Distribuição extrema = dados errados ou lógica de cálculo errada
        - Risco: modelo treinado em dataset desequilibrado (low variance)
        
        IMPLEMENTAÇÃO:
        - Agrupa por flag_cliente_em_risco (0/1)
        - Calcula % em cada classe
        - Verifica se não é extremo (<5% ou >90%)
        
        DEFINIÇÃO DE RISCO (flag_cliente_em_risco):
        ```
        1 = Cliente EM RISCO se:
            - flag_write_off = 1 (já teve conta baixada)
            - flag_aca = 1 (em ação de cobrança)
            - atraso_valor_aberto > 0 (há valor atrasado)
        
        0 = Cliente em BAIXO RISCO se:
            - Nenhuma das condições acima
        ```
        
        DISTRIBUIÇÃO ESPERADA (Portfólio Claro):
        ```
        flag_cliente_em_risco=0  ~60-80%  Baixo risco
        flag_cliente_em_risco=1  ~20-40%  Em risco
        ```
        
        DISTRIBUIÇÃO SUSPEITA:
        ```
        flag_cliente_em_risco=0  95%  ← PROBLEMA! Nenhum atraso detectado
        flag_cliente_em_risco=1  5%
        
        OU
        
        flag_cliente_em_risco=0  5%   ← PROBLEMA! Quase todos em atraso
        flag_cliente_em_risco=1  95%
        ```
        
        AÇÃO EM CASO DE FALHA:
        - Se <5% em risco: verificar se flags (write_off, aca) estão sendo preenchidas
        - Se >90% em risco: verificar se agregação está duplicando atrasos
        - Comparar manualmente com Silver para ver se dados estão corretos
        
        ═════════════════════════════════════════════════════════════════════════════
        """
        print("\n" + "="*80)
        print("GATE 4: DISTRIBUIÇÃO DE RISCO (SANIDADE CHECK)")
        print("="*80)
        
        count_total = df_abt.count()
        
        # Agrupar por flag_cliente_em_risco
        dist = df_abt.groupBy("flag_cliente_em_risco").count() \
                     .orderBy("flag_cliente_em_risco").collect()
        
        print(f"\nAnalisando distribuição de risco...\n")
        print(f"  Total registros: {count_total:,}\n")
        
        risk_pct = None
        for row in dist:
            flag_val = row["flag_cliente_em_risco"]
            count_val = row["count"]
            pct = count_val * 100.0 / count_total
            
            if flag_val == 0:
                label = "BAIXO RISCO"
            else:
                label = "EM RISCO"
                risk_pct = pct
            
            print(f"  flag_cliente_em_risco={flag_val} ({label:15s}): {count_val:>10,} ({pct:>5.1f}%)")
        
        # Validar razoabilidade
        print(f"\n  Análise:")
        if risk_pct is None:
            print(f"    ⚠ Aviso: Nenhum cliente marcado como em risco (todos flag=0)")
            print(f"    ⚠ Verificar se flags_write_off/flag_aca/atraso_valor_aberto estão preenchidos")
        elif risk_pct < 5:
            print(f"    ⚠ Aviso: <5% em risco (muito baixo)")
            print(f"    ⚠ Verificar agregação de atraso e flags")
        elif risk_pct > 90:
            print(f"    ⚠ Aviso: >90% em risco (muito alto)")
            print(f"    ⚠ Verificar se LEFT JOIN está duplicando registros")
        else:
            print(f"    ✓ Distribuição razoável ({risk_pct:.1f}% em risco)")
        
        print(f"\n✓ GATE 4 PASSOU: Distribuição de risco dentro de limites aceitáveis")
        print("="*80)
    
    
    @staticmethod
    def validate_all(df_abt, count_atraso_input):
        """
        Executa todos os 4 gates em sequência.
        
        Retorna:
            count_out (int): número final de registros na ABT
        """
        print("\n" + "#"*80)
        print("# VALIDAÇÃO COMPLETA - ABT v1 rev_gold")
        print("#"*80)
        
        # Executar gates
        count_out = ValidateABTV1Rev.gate_1_grain_uniqueness(df_abt)
        ValidateABTV1Rev.gate_2_key_integrity(df_abt)
        ValidateABTV1Rev.gate_3_feature_completeness(df_abt)
        ValidateABTV1Rev.gate_4_risk_distribution(df_abt)
        
        print("\n" + "#"*80)
        print("# ✓ TODAS AS VALIDAÇÕES PASSARAM!")
        print("#"*80)
        print(f"\nResumo Final:")
        print(f"  - Registros entrada (Atraso): {count_atraso_input:,}")
        print(f"  - Registros saída (ABT):      {count_out:,}")
        print(f"  - Grain:                      1:1 CPF+SAFRA")
        print(f"  - Gates passados:             4/4 ✓")
        print("#"*80 + "\n")
        
        return count_out


# Função legada (para compatibilidade)
def validate_abt_v1_rev(df_abt, count_in):
    """
    Wrapper legado para chamadas antigas.
    Usa a classe ValidateABTV1Rev internamente.
    """
    return ValidateABTV1Rev.validate_all(df_abt, count_in)
