# Regras de Negócio Críticas

Este documento lista as regras que **NÃO podem ser violadas** sob pena de invalidar o modelo ou a análise.

## Regras de Anti-Leakage (Vazamento de Dados)

### Regra 1: FPD Nunca é Feature

| ❌ PROIBIDO | ✓ CORRETO |
|-------------|-----------|
| Usar `fpd_int` como variável preditora | Usar `fpd_int` apenas como target (Y) |

**Por quê?** FPD é o que queremos prever. Usá-lo como feature seria "trapacear" — o modelo teria 100% de acerto mas zero utilidade.

### Regra 2: FLAG_INSTALACAO Nunca é Feature

| ❌ PROIBIDO | ✓ CORRETO |
|-------------|-----------|
| Usar `flag_instalacao_int` como variável preditora | Usar para filtrar dados de treino |

**Por quê?** FLAG_INSTALACAO é a decisão atual que queremos melhorar. Usá-lo como feature impediria o modelo de encontrar clientes que deveriam ter decisão diferente.

### Regra 3: Dados do Futuro Não Podem Prever o Passado

| ❌ PROIBIDO | ✓ CORRETO |
|-------------|-----------|
| Feature de Mar/2024 para prever decisão de Jan/2024 | Feature até Dez/2023 para prever decisão de Jan/2024 |

**Regra geral:** `safra_feature < safra_decisao`

**Exemplo:**
```
Cliente com SAFRA = 202401 (decisão em Jan/2024)
  ✓ Pode usar: recarga de Nov/2023, Dez/2023
  ❌ Não pode usar: recarga de Jan/2024, Fev/2024
```

---

## Regras de Treino e Validação

### Regra 4: Treinar Apenas Onde FPD é Observado

```python
# CORRETO
df_train = df_abt.filter(F.col("flag_instalacao_int") == 1)

# ERRADO - inclui clientes onde FPD é desconhecido
df_train = df_abt  # ❌
```

**Por quê?** Clientes com FLAG=0 não contrataram, então não sabemos se pagariam ou não. Incluí-los introduziria ruído.

### Regra 5: OOT é Intocável

| Conjunto | Uso Permitido | Uso Proibido |
|----------|---------------|--------------|
| **Train** | Treinar modelo | - |
| **Test** | Ajustar hiperparâmetros | Treinar modelo |
| **OOT** | Validação final única | Qualquer ajuste |

**Por quê?** Se usarmos OOT para ajustar o modelo, ele deixa de ser "out-of-time" e a validação fica contaminada.

### Regra 6: Split Temporal, Não Aleatório

```python
# CORRETO - split temporal
df_train = df.filter(F.col("safra") < "202402")
df_test = df.filter(F.col("safra") == "202402")
df_oot = df.filter(F.col("safra") == "202403")

# ERRADO - split aleatório vazaria informação
train, test = df.randomSplit([0.8, 0.2])  # ❌
```

---

## Regras de Grão (Granularidade)

### Regra 7: Uma Linha por CPF + SAFRA

A ABT tem granularidade **1:1** por combinação de NUM_CPF e SAFRA:

```sql
-- Validação obrigatória
SELECT num_cpf, safra, COUNT(*) as n
FROM gold_abt_v6
GROUP BY num_cpf, safra
HAVING n > 1;
-- Deve retornar 0 linhas
```

**Por quê?** Duplicatas causariam vazamento de dados e inflação de métricas.

### Regra 8: Chaves Nunca Nulas

```python
# Validação obrigatória
assert df.filter(F.col("num_cpf").isNull()).count() == 0
assert df.filter(F.col("safra").isNull()).count() == 0
```

---

## Regras de Tratamento de Dados

### Regra 9: Sentinelas Vão para NULL

Valores especiais que significam "não informado" devem ser convertidos para NULL:

| Fonte | Sentinela | Significado | Tratamento |
|-------|-----------|-------------|------------|
| Score_01 | 0 | Sem score | NULL + FLAG_SCORE_01_MISSING |
| Telco | 304 | Não determinado | NULL |
| Recarga | -1, -2, -3 | Não aplica/informado | NULL |

**Por quê?** Valores sentinela criam padrões espúrios. Ex: média de scores seria distorcida por zeros.

### Regra 10: SOS e Bônus Não São Dinheiro Real

Ao calcular métricas de valor de recarga:

```python
# CORRETO - descontar SOS
valor_real = valor_recarga - valor_sos

# ERRADO - contar SOS como receita
valor_total = valor_recarga  # ❌ se inclui SOS embutido
```

---

## Regras de Apresentação

### Regra 11: Ordem Incremental de KS é Obrigatória

A apresentação DEVE seguir esta ordem:

1. Score_01 (baseline)
2. + Score_02
3. + Telco
4. + Cadastro
5. + Book Recarga
6. + Book Pagamento + Atraso

**Não é negociável.** Foi determinado pela coordenação.

### Regra 12: Matriz de Confusão com Swap Analysis

Além do KS, a apresentação deve incluir:

```
                    MODELO
                 Aprova    Rejeita
POLÍTICA  Aprova   A          B
ATUAL     Rejeita  C          D

A = Concordância (ambos aprovam)
B = Swap-out (modelo rejeita quem política aprova)
C = Swap-in (modelo aprova quem política rejeita)
D = Concordância (ambos rejeitam)
```

**Foco da análise:** Quadrante B (quantos "maus" seriam evitados) e C (quantos "bons" seriam recuperados).

### Regra 13: Foco na Metade Inferior da ROC

```
      │
 TPR  │     ┌───────── Zona de corte alto
      │    /           (poucos aprovam, menos relevante)
      │   /
      │  /
      │ /────────────── Zona de aprovação
      │/                (impacto financeiro real)
      └───────────────
        FPR →
```

**Por quê?** O impacto financeiro está nos clientes de score moderado, onde a decisão realmente muda.

---

## Checklist de Validação

Antes de considerar a ABT pronta para modelagem:

- [ ] FPD e FLAG_INSTALACAO **não** são features
- [ ] Grão 1:1 por NUM_CPF + SAFRA (sem duplicatas)
- [ ] Sem NULLs em chaves (num_cpf, safra)
- [ ] Sentinelas convertidos para NULL
- [ ] Janelas temporais respeitadas (safra_feature < safra_decisao)
- [ ] Split temporal configurado (não aleatório)
- [ ] Treino apenas em FLAG=1
- [ ] OOT reservado e intocável

---

## Consequências de Violar as Regras

| Regra Violada | Consequência |
|---------------|--------------|
| FPD como feature | Modelo inutilizável, métricas falsas |
| FLAG como feature | Modelo não consegue melhorar política |
| Dados do futuro | Leakage, performance irreal |
| Treino em FLAG=0 | Viés de seleção, FPD inventado |
| OOT contaminado | Validação inválida |
| Duplicatas | Métricas infladas |
| Sentinelas não tratados | Padrões espúrios |
| Ordem de KS errada | Apresentação reprovada |

**Regra de ouro:**
> "Na dúvida, pergunte. Melhor atrasar do que entregar errado."
