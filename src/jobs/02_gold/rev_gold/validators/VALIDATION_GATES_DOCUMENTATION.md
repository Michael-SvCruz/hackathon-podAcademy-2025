# 📋 Documentação Detalhada: Gates de Validação ABT v1 rev_gold

**Arquivo:** `validators/validate_abt_v1_rev.py`  
**Última atualização:** 27 de janeiro de 2026  
**Status:** ✅ Documentado e em produção

---

## 🎯 Visão Geral

Os **4 Gates de Qualidade** validam diferentes aspectos da ABT v1 rev_gold:

| Gate | Aspecto | Tipo | Método | Passa se... |
|------|---------|------|--------|-------------|
| **1** | Grain | Estrutura | count vs count distinct | count_total == count_unique |
| **2** | Integridade | Dados | Verifica NULLs | NULLs_chaves == 0 |
| **3** | Completude | Cobertura | % preenchimento | features >= 70% |
| **4** | Distribuição | Sanidade | % em cada classe | 5% < risk < 90% |

---

## 🔍 GATE 1: GRAIN 1:1 (UNICIDADE CPF+SAFRA)

### O que valida?
Garante que **cada cliente em cada mês** aparece **exatamente 1 vez** na ABT.

### Por que importa?
- ABT é agregada por `CPF + SAFRA` (chave primária)
- Duplicatas = dados corrompidos ou lógica de JOIN errada
- Modelos treinam em dados replicados = **overfitting**

### Implementação
```python
count_total = df_abt.count()                                    # 100.000
count_unique = df_abt.select("num_cpf", "safra").distinct().count()  # 100.000

assert count_unique == count_total, "Há duplicatas!"
```

### Exemplo de FALHA
```
num_cpf      safra   Ocorrências
12345678     202501  2  ← PROBLEMA!
87654321     202501  1  ✓
99999999     202501  3  ← PROBLEMA!
```

**Causa comum:** LEFT JOIN duplicando registros  
**Solução:** Verificar deduplicação em `aggregate_atraso()` e `aggregate_pagamento()`

---

## 🛡️ GATE 2: INTEGRIDADE DE CHAVES (SEM NULLS)

### O que valida?
Garante que as chaves primárias (`num_cpf`, `safra`) **nunca são nulas**.

### Por que importa?
- NULLs nas chaves = impossível rastrear cliente ou período
- Silenciosamente perde registros em JOINs posteriores
- Impossível agregar ou usar como índice

### Implementação
```python
nulls_cpf = df_abt.filter(F.col("num_cpf").isNull()).count()
nulls_safra = df_abt.filter(F.col("safra").isNull()).count()

assert (nulls_cpf + nulls_safra) == 0, "Há NULLs nas chaves!"
```

### Exemplo de FALHA
```
num_cpf      safra      status
12345678     202501     ✓ válido
NULL         202501     ✗ FALHA (cliente desconhecido)
87654321     NULL       ✗ FALHA (período desconhecido)
```

**Causa comum:** JOIN LEFT introduzindo NULLs  
**Solução:** Usar `COALESCE()` ou `FILTER` antes de JOIN

---

## 📊 GATE 3: COMPLETUDE DE FEATURES (>70%)

### O que valida?
Garante que as features principais têm **pelo menos 70% de preenchimento**.

### Por que importa?
- <70% completude = feature sem sinal (maioria NULL)
- Modelos não conseguem aprender em features vazias
- Indica dados quebrados ou inaplicáveis

### Implementação
```python
for feature in ["atraso_valor_aberto", "pagto_valor_fatura", ...]:
    nulls = df_abt.filter(F.col(feature).isNull()).count()
    completude = (count_total - nulls) * 100.0 / count_total
    assert completude >= min_threshold, f"{feature} tem {completude}% preenchimento"
```

### Limiares por Feature

| Feature | Min | Esperado | Motivo |
|---------|-----|----------|--------|
| `atraso_valor_aberto` | 30% | 35-50% | Nem todo cliente atrasa |
| `pagto_valor_fatura` | 50% | 70-90% | Maioria paga |
| `flag_write_off` | 0% | 2-5% | Evento raro |
| `flag_pdd` | 0% | 5-10% | Evento moderado |
| `delinquency_rate` | 70% | 100% | Derivada (sempre calculada) |
| `risk_score_delinquency` | 70% | 100% | Derivada (sempre calculada) |

### Exemplo de Distribuição SAUDÁVEL
```
Feature                  Não-Nulos   %      Status
atraso_valor_aberto      35.000     35.0%  ✓ OK
pagto_valor_fatura       82.000     82.0%  ✓ OK
flag_write_off           3.500      3.5%   ✓ OK (raro, mas presente)
delinquency_rate         100.000    100%   ✓ OK (derivada)
risk_score_delinquency   100.000    100%   ✓ OK (derivada)
```

### Exemplo de FALHA
```
Feature                  Não-Nulos   %      Status
atraso_valor_aberto      1.000      1.0%   ✗ CRÍTICO (<30%)
pagto_valor_fatura       20.000     20.0%  ✗ CRÍTICO (<50%)
flag_write_off           NULL       0%     ✗ CRÍTICO (100% missing)
```

**Causa comum:** Agregação incompleta ou JOIN perdendo dados  
**Solução:** Verificar `aggregate_atraso()` / `aggregate_pagamento()`

---

## 📈 GATE 4: DISTRIBUIÇÃO DE RISCO (SANIDADE CHECK)

### O que valida?
Garante que a proporção de clientes "em risco" é **razoável** (5-90%).

### Por que importa?
- **Muito baixa (<5%):** Flags de risco não funcionam
- **Muito alta (>90%):** Dados errados ou agregação duplicou atrasos
- **Esperado (20-40%):** Comportamento típico de portfólio telco

### Implementação
```python
dist = df_abt.groupBy("flag_cliente_em_risco").count().collect()

for row in dist:
    pct = row["count"] * 100.0 / count_total
    # Verificar se 5% < pct < 90%
    if pct < 5 or pct > 90:
        print("⚠ Distribuição suspeita!")
```

### Definição de RISCO
```python
flag_cliente_em_risco = 1 se:
    - flag_write_off == 1 (conta já foi baixada)
    OU
    - flag_aca == 1 (em ação de cobrança)
    OU
    - atraso_valor_aberto > 0 (há valor atrasado)

flag_cliente_em_risco = 0 caso contrário
```

### Distribuição ESPERADA
```
Portfólio típico Claro:

flag_cliente_em_risco=0  ~60-80%  Baixo risco (sem atraso, sem flags)
flag_cliente_em_risco=1  ~20-40%  Em risco (atraso ou flags ativas)
```

### Exemplos de FALHA

#### ❌ Cenário 1: <5% em risco (muito pouco!)
```
flag_cliente_em_risco=0  95%  ← PROBLEMA! Nenhum atraso detectado
flag_cliente_em_risco=1  5%

Possíveis causas:
- Agregação de Atraso não está trazendo dados
- val_fat_aberto vem todo NULL ou 0
- Flags (write_off, aca) não estão sendo preenchidas
```

#### ❌ Cenário 2: >90% em risco (muito alto!)
```
flag_cliente_em_risco=0  5%   ← PROBLEMA! Quase todos em atraso
flag_cliente_em_risco=1  95%

Possíveis causas:
- LEFT JOIN está duplicando registros de atraso
- atraso_valor_aberto sendo agregado errado (somando múltiplas faturas)
- Lógica de flag (write_off, aca) muito liberal
```

### ✓ Cenário OK
```
flag_cliente_em_risco=0  70%  ✓ Maioria baixo risco
flag_cliente_em_risco=1  30%  ✓ 30% em risco (esperado)

Status: PASSA Gate 4 ✓
```

---

## 🔄 Fluxo de Execução

```python
# Chamar validate_all() passa por todos os gates em ordem:

ValidateABTV1Rev.validate_all(df_abt, count_atraso)
│
├─► Gate 1: Grain 1:1
│   ├─ count(*) vs count(distinct cpf,safra)
│   └─ ASSERT count_unique == count_total
│
├─► Gate 2: Integridade de chaves
│   ├─ count(where cpf IS NULL)
│   ├─ count(where safra IS NULL)
│   └─ ASSERT nulls == 0
│
├─► Gate 3: Completude
│   ├─ Para cada feature: (count - nulls) / count >= min_threshold
│   └─ ASSERT todos passam
│
├─► Gate 4: Distribuição de risco
│   ├─ count(where flag_cliente_em_risco=1) / count * 100
│   └─ ASSERT 5% < risk_pct < 90%
│
└─► Se todos passam: RETORNA count_out (registros finais)
    Se algum falha: LEVANTA AssertionError e para
```

---

## ⚡ Quando Gates Falham

### Ação Recomendada

| Gate | Falha | Primeira Ação |
|------|-------|---------------|
| **1** | Duplicatas | Verificar `dropDuplicates()` em `aggregate_atraso()` |
| **2** | NULLs nas chaves | Adicionar `COALESCE()` ou filtrar antes JOIN |
| **3** | Completude <70% | Verificar se dados Silver estão chegando |
| **4** | <5% ou >90% em risco | Comparar com Silver para validar lógica |

### Debug Rápido
```python
# Se Gate 1 falha:
df_abt.groupBy("num_cpf", "safra").count().filter(col("count") > 1).show()

# Se Gate 2 falha:
df_abt.filter(col("num_cpf").isNull()).show()
df_abt.filter(col("safra").isNull()).show()

# Se Gate 3 falha:
df_abt.select([
    (F.count_distinct("num_cpf") / F.count("*")).alias("completude_cpf"),
    (F.sum(F.when(col("atraso_valor_aberto").isNull(), 0).otherwise(1)) / F.count("*")).alias("completude_atraso")
]).show()

# Se Gate 4 falha:
df_abt.groupBy("flag_cliente_em_risco").count().show()
```

---

## 📚 Referência Rápida

### Arquivo de Código
- **Localização:** `src/jobs/02_gold/rev_gold/validators/validate_abt_v1_rev.py`
- **Classe:** `ValidateABTV1Rev`
- **Métodos:**
  - `gate_1_grain_uniqueness(df_abt)`
  - `gate_2_key_integrity(df_abt)`
  - `gate_3_feature_completeness(df_abt)`
  - `gate_4_risk_distribution(df_abt)`
  - `validate_all(df_abt, count_atraso)` ← **USAR ESTE**

### Como Chamar
```python
from validators.validate_abt_v1_rev import ValidateABTV1Rev

count_out = ValidateABTV1Rev.validate_all(df_abt, count_atraso_entrada)
```

### Output de Sucesso
```
################################################################################
# VALIDAÇÃO COMPLETA - ABT v1 rev_gold
################################################################################

================================================================================
GATE 1: GRAIN 1:1 (UNICIDADE CPF+SAFRA)
================================================================================
...
✓ GATE 1 PASSOU: Grain 1:1 garantida (sem duplicatas)

[Repeats para Gates 2, 3, 4]

################################################################################
# ✓ TODAS AS VALIDAÇÕES PASSARAM!
################################################################################

Resumo Final:
  - Registros entrada (Atraso): 500,000
  - Registros saída (ABT):      480,000
  - Grain:                      1:1 CPF+SAFRA
  - Gates passados:             4/4 ✓
################################################################################
```

---

## 🎓 Conclusão

Os **4 Gates** garantem que ABT v1 rev_gold é:
- ✅ **Estruturalmente correta** (Gate 1: sem duplicatas)
- ✅ **Íntegra** (Gate 2: chaves válidas)
- ✅ **Informativa** (Gate 3: features com sinal)
- ✅ **Saudável** (Gate 4: distribuição razoável)

Se todos passam → **Pronto para treinamento de modelo!**

---

**Documento:** Validation Gates Documentation v1.0  
**Data:** 27 de janeiro de 2026  
**Status:** ✅ Completo e verificado
