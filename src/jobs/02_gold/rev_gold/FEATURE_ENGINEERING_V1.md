# 📊 ESPECIFICAÇÃO TÉCNICA: v1 rev_gold Features

**Criado:** 27 de janeiro de 2026  
**Baseado em:** Reunião 07/01/2026 com Fernando Parahyba (Claro)

---

## 🎯 Estratégia: Por que essas features?

Fernando Parahyba foi **explícito** sobre a importância de dados de delinquência/comportamento:

### Citações Diretas da Reunião

1. **"O comportamento de pagamento é crucial"**
   - ✅ Atraso: captura inadimplência (histórico 12M)
   - ✅ Pagamento: captura padrão de regularização
   
2. **"As informações de recarga permitem reclassificar clientes de alto risco para médio ou baixo"**
   - Implicação: Atraso/Pagamento é o discriminador BASE
   - Recarga refina
   
3. **"O processo envolve adicionar uma fonte por vez para avaliar ganho individual de KS"**
   - v1_rev teste isolado: Atraso + Pagamento
   - Medir: quanto contribuem à discriminação?
   
4. **"Não tente replicar testes de scores existentes"**
   - Scores (Score_01, Score_02) ficam para ÚLTIMA FASE
   - BASE deve ser comportamento interno

---

## 📐 DESIGN DAS FEATURES (Detalhado)

### ATRASO: Indicadores de Inadimplência

#### 1. `atraso_faixa_aging` (int)
**Fonte:** `IND_PCCR` (PCCR = "payment currency clarification recent"?) ou `DW_FAIXA_AGING_FATURA`

**Domínio:** Faixa de antigüidade
- `0` = 0-30 dias (recente)
- `1` = 30-60 dias (preocupante)
- `2` = 60-90 dias (crítico)
- `3` = 90+ dias (write-off)
- `-1` = missing/não se aplica

**Por quê criamos:**
- Indica **quanto tempo** o cliente fica em atraso
- 90+ dias = cliente NÃO paga há 3+ meses = RED FLAG
- Granularidade: faixa, não valor bruto

**Correlação esperada com FPD:** ↑ (positiva) - quanto mais dias, maior risco

---

#### 2. `flag_write_off` (int)
**Fonte:** `IND_WO` (write-off indicator)

**Domínio:** 0/1
- `1` = cliente teve conta baixada/write-off
- `0` = não

**Por quê:**
- Write-off = a empresa desistiu de cobrar (perda total)
- Qualquer cliente com write-off NO PASSADO = RISCO ALTÍSSIMO
- Muito mais preditivo que simples atraso

**Correlação com FPD:** ↑↑ (muito forte)

---

#### 3. `flag_pdd` (int)
**Fonte:** `IND_PDD` (Possibly Defaulted)

**Domínio:** 0/1 (convertido de S/N)
- `1` = Claro o considera "provavelmente defaultado"
- `0` = não

**Por quê:**
- PDD é score interno Claro de risco de default
- Sintetiza múltiplos sinais internos
- Muito preditivo

**Correlação com FPD:** ↑↑

---

#### 4. `flag_aca` (int)
**Fonte:** `IND_ACA` (Ação de cobrança ativa)

**Domínio:** 0/1
- `1` = cliente em processo de cobrança judicial/ativa
- `0` = não

**Por quê:**
- Escalação: atraso → notificação → ação → write-off
- Ação ativa = cliente já foi notificado múltiplas vezes
- Muito predictor de reincidência

**Correlação com FPD:** ↑↑

---

#### 5. `atraso_faixa_tempo_base` (int)
**Fonte:** `DW_FAIXA_TEMPO_BASE` (tempo como cliente)

**Domínio:** Faixa (encoded)
- `0` = 0-3 meses (novo)
- `1` = 3-6 meses
- `2` = 6-12 meses
- `3` = 12+ meses (estabelecido)
- `-1` = missing

**Por quê:**
- Clientes NOVOS têm risco maior
- Falta de histórico = desconhecido
- Fernando: "informações internas de comportamento permitem potencializar o modelo"

**Correlação com FPD:** ↓ (negativa) - cliente mais novo = maior risco

---

#### 6. `atraso_valor_aberto` (double)
**Fonte:** `VAL_FAT_ABERTO` (valor de fatura aberta/pendente)

**Unidade:** Reais (R$)

**Por quê:**
- Valor em risco = dimensão da inadimplência
- Cliente com R$ 10 em atraso ≠ cliente com R$ 1000
- Magnitude importa

**Interpretação:**
- 0 = nenhum atraso pendente (bom)
- > 0 = há valor em aberto

**Correlação com FPD:** ↑ (quantidade de moeda em risco)

---

#### 7. `atraso_valor_multa_juros` (double)
**Fonte:** `VAL_MULTA_JUROS` (multas + juros incididos)

**Unidade:** Reais (R$)

**Por quê:**
- Juros/multas = consequência de atrasos prévios
- Se há juros = cliente JÁ atrasou antes
- Reincidência = sinal de problema crônico

**Interpretação:**
- 0 = nunca atrasou
- > 0 = já atrasou (e foi cobrado)

**Correlação com FPD:** ↑↑ (atrasos passados → atrasos futuros)

---

#### 8-10. Flags de Sentinela
**Exemplo:** `flag_ind_wo_sentinela`, `flag_ind_pdd_sentinela`, `flag_status_fat_missing`

**Por quê:**
- Dados faltantes NÃO são aleatórios
- `-1/-2/-3` em DW = "não informado" ou "não se aplica"
- Flag preserva sinal: "não temos informação" é informativo

**Uso em modelo:**
- Modelo pode aprender: "missing em PDD = +2% risco"
- Sem flag, perderíamos esse sinal

---

### PAGAMENTO: Padrão de Regularização

#### 1. `pagto_valor_atual` (double)
**Fonte:** `VAL_ATUAL_PAGAMENTO`

**Unidade:** Reais (R$)

**Por quê:**
- Valor que cliente está pagando AGORA
- Regularidade de pagamentos = confiabilidade

**Interpretação:**
- > 0 = cliente ainda paga
- == 0 = cliente desapareceu (deve estar atrasado)

---

#### 2. `pagto_valor_original` (double)
**Fonte:** `VAL_ORIGINAL_PAGAMENTO`

**Por quê:**
- Contexto: qual era o valor original comprometido?
- Permite validar se pagamento é integral ou parcial

---

#### 3. `pagto_valor_fatura` (double)
**Fonte:** `VAL_PAGAMENTO_FATURA`

**Por quê:**
- Total pago por fatura
- Maior valor = cliente regulariza contas
- KPI: (qtd_faturas_pagas / total_faturas) = taxa de adimplência

**Derivada possível:**
```
taxa_pagamento = pagto_valor_fatura / (atraso_valor_aberto + pagto_valor_fatura)
```
Quanto percentual da obrigação o cliente paga?

---

#### 4. `pagto_desconto_total` (double)
**Fonte:** `VAL_DESCONTO_ITEM`

**Por quê:**
- Descontos/abonos concedidos
- Alto desconto = cliente teve dificuldades → negocia
- Pode indicar cliente em risco moderado (negocia) vs alto (para)

**Interpretação:**
- 0 = nunca negocia (bom ou mau?)
- > 0 = já precisou de ajuda

---

#### 5. `pagto_juros_total` (double)
**Fonte:** `VAL_JUROS_POS` (juros positivos, não negativo)

**Por quê:**
- Juros pagos = atrasos PRÉVIOS que foram regularizados
- Cliente com histórico de atrasos que PAGOU

**Diferença de Atraso:**
- `atraso_valor_multa_juros` = juros INCIDINDO agora
- `pagto_juros_total` = juros que PAGOU antes
- Ambos = cliente problemático

---

#### 6. `flag_pagto_pendente` (int)
**Fonte:** `IND_STATUS_PAGAMENTO == "P"` (P = Pending?)

**Por quê:**
- Pagamento marcado como pendente = cliente NÃO pagou
- Red flag em última data de snapshot

---

#### 7. `flag_juros_incidido` (int)
**Fonte:** `VAL_JUROS_MULTAS_ITEM < 0` (negativo = crédito, abono)

**Por quê:**
- Juros incididos = cliente atrasou
- Flag ativa este conhecimento

---

#### 8. `cod_metodo_pagto` (string)
**Fonte:** `COD_METODO_PAGAMENTO`

**Domínio:** Débito automático, Boleto, PIX, Cartão, etc.

**Por quê:**
- Débito automático = cliente confia no sistema (ou não pode parar)
- Boleto = cliente controla ativamente

**Derivada possível:**
```
flag_pagto_automatico = cod_metodo_pagto == "DEBITO_AUTOMATICO" ? 1 : 0
```

Débito automático = sinal positivo (aderiu a automação)

---

### DERIVADAS: Features Compostas Inteligentes

#### 1. `delinquency_rate` (double)
```
delinquency_rate = (atraso_valor_aberto / (atraso_valor_aberto + pagto_valor_fatura)) * 100
```

**Interpretação:** % de fatura NÃO paga

**Exemplo:**
- Cliente 1: atraso=R$100, pagto=R$900 → rate=10% (ok)
- Cliente 2: atraso=R$500, pagto=R$500 → rate=50% (risco!)
- Cliente 3: atraso=R$1000, pagto=R$0 → rate=100% (crítico)

**Por quê:**
- Taxa é mais interpretável que valor absoluto
- R$100 de atraso para cliente rico ≠ R$100 para pobre
- Taxa normaliza contexto

---

#### 2. `risk_score_delinquency` (double)
```
risk_score_delinquency = atraso_faixa_aging * delinquency_rate / 100.0
```

**Interpretação:** Score composto = (dias × taxa)

**Exemplo:**
- Cliente 1: faixa=3 (90+ dias), rate=50% → score=1.5 (CRÍTICO)
- Cliente 2: faixa=1 (30-60 dias), rate=50% → score=0.5 (moderado)
- Cliente 3: faixa=0 (0-30 dias), rate=10% → score=0 (baixo)

**Por quê:**
- Combine dois sinais fortes em 1 feature
- Modelo pode usar diretamente ou ignorar se redundante

---

#### 3. `flag_cliente_em_risco` (int)
```
flag = (write_off == 1) OR (aca == 1) OR (atraso_valor_aberto > 0)
```

**Interpretação:** Flag agregada = cliente em ALGUM tipo de risco

**Uso:**
- Segmentação rápida
- Relatório de negócio
- Modelo pode aprender weight dessa flag

---

## 🔎 Validações de Qualidade

### Q1: Valores fazem sentido?

```sql
SELECT
  COUNT(*) as total,
  COUNT(CASE WHEN atraso_valor_aberto >= 0 THEN 1 END) as atraso_valido,
  COUNT(CASE WHEN pagto_valor_fatura >= 0 THEN 1 END) as pagto_valido,
  COUNT(CASE WHEN delinquency_rate BETWEEN 0 AND 100 THEN 1 END) as delinq_valido
FROM abt_v1_rev
-- Esperado: todos validam
```

### Q2: Features têm variância?

```sql
SELECT
  MIN(delinquency_rate) as min_delinq,
  MAX(delinquency_rate) as max_delinq,
  AVG(delinquency_rate) as avg_delinq,
  STDDEV(delinquency_rate) as stddev_delinq
FROM abt_v1_rev
-- Esperado: min ~0, max ~100, stddev > 10
```

### Q3: Correlação com target?

```python
# Pós-join com target
df_with_target = abt_v1_rev.join(target_labels, on=['num_cpf', 'safra'])
df_with_target.select(['atraso_valor_aberto', 'flag_cliente_em_risco', 'delinquency_rate', 'fpd']).corr()
# Esperado: correlações positivas 0.3-0.7 com FPD
```

---

## 🚀 Próxima Fase: v2 (Recarga)

Após validar v1_rev:

1. **Medir KS v1_rev:** Treinar modelo, calcular KS (esperado: 40-42%)
2. **Comparar com v1 original:** KS Score_01 apenas (33.1%)
3. **Ganho esperado:** ΔKS = 40-42% - 33.1% = +7-9pp
4. **Análise:** Se ganho > 5pp, adicionar Recarga (v2)

**v2 vai trazer:**
- Features de recarga (M1, M3, M6): frequência, valor, tendência
- Esperado ΔKS +5pp (total 45-47%)
- Insight: recarga refina base delinquência

---

## 📚 Referências

### Documentação Interna
- [docs/01_data_dictionary/atraso.md](../../../docs/01_data_dictionary/atraso.md)
- [docs/01_data_dictionary/pagamento.md](../../../docs/01_data_dictionary/pagamento.md)
- [docs/03_silver_rules/atraso.md](../../../docs/03_silver_rules/atraso.md)

### Reunião 07/01/2026 - Transcrição
- Fernando Parahyba (Claro): "O comportamento de pagamento é crucial"
- Fernando: "A principal beleza do modelo de migração é que as informações de recarga permitem reclassificar clientes"
- Fernando: "O processo envolve adicionar uma fonte por vez para avaliar ganho individual de KS"

---

## ✅ Checklist de Implementação

- [x] Feature 1: atraso_faixa_aging
- [x] Feature 2: flag_write_off
- [x] Feature 3: flag_pdd
- [x] Feature 4: flag_aca
- [x] Feature 5: atraso_faixa_tempo_base
- [x] Feature 6: atraso_valor_aberto
- [x] Feature 7: atraso_valor_multa_juros
- [x] Feature 8-10: Flags de sentinela
- [x] Feature 1: pagto_valor_atual
- [x] Feature 2: pagto_valor_original
- [x] Feature 3: pagto_valor_fatura
- [x] Feature 4: pagto_desconto_total
- [x] Feature 5: pagto_juros_total
- [x] Feature 6: flag_pagto_pendente
- [x] Feature 7: flag_juros_incidido
- [x] Feature 8: cod_metodo_pagto
- [x] Derivada 1: delinquency_rate
- [x] Derivada 2: risk_score_delinquency
- [x] Derivada 3: flag_cliente_em_risco
- [x] Validações
- [x] Documentação

**Status:** ✅ PRONTA PARA TESTE

---

**Próximo:** Executar script, validar grain, medir KS, comparar com v1 original.
