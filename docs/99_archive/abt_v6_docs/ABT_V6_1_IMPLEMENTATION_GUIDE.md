# ABT v6.1 — Implementação do Enhancement
## Feature Enhancement Script Criado

**Data:** 25 de janeiro de 2026  
**Status:** ✅ SCRIPT CRIADO E PRONTO PARA EXECUÇÃO  
**Objetivo:** Adicionar 3 features críticas a ABT v6

---

## 📦 O que foi criado

### Novo Script: `06_gold_abt_v6_1_builder.py`
- **Localização:** `src/jobs/02_gold/06_gold_abt_v6_1_builder.py`
- **Linhas:** 620+ linhas de código
- **Função:** Estender ABT v6 com 3 features críticas
- **Status:** Pronto para rodar

---

## 🎯 3 Features Implementadas

### 1. **DESCONTO_RATE** (Pagamento)
```python
desconto_rate_m1/m3/m6 = SUM(desconto) / (SUM(desconto) + SUM(pago))
```
- **Range:** 0-1 (ou NULL se sem pagamentos)
- **Lógica:** Proporção de descontos no total pago
- **Interpretação:**
  - 0% = cliente paga cheio (bom)
  - 5-10% = desconto ocasional (normal)
  - >20% = negociador agressivo (risco)
- **Ganho esperado:** +0.3pp KS
- **Anti-leakage:** ✅ Dados históricos

---

### 2. **DELINQUENCY_RATE** (Atraso)
```python
delinquency_rate_m1/m3/m6 = QTD_FATURAS_ABERTAS / QTD_TOTAL_FATURAS
```
- **Range:** 0-1 (ou NULL)
- **Lógica:** Proporção de faturas abertas vs total
- **Interpretação:**
  - 0% = sem atrasos (ótimo)
  - 10-30% = alguns atrasos (normal)
  - >50% = maioria em atraso (muito risco)
- **Ganho esperado:** +0.4pp KS
- **Anti-leakage:** ✅ Snapshot mensal

---

### 3. **MAX_DIAS_ATRASO** (Atraso)
```python
max_dias_atraso_m1/m3/m6 = MAX(DATEDIFF(DATA_REFERENCIA, DATA_VENCIMENTO))
```
- **Range:** 0+ dias (inteiro)
- **Lógica:** Idade da dívida mais antiga
- **Interpretação:**
  - 0-30 dias = atraso pequeno (aceitável)
  - 30-60 dias = atraso moderado (alerta)
  - 60+ dias = atraso grave (risco alto)
  - 180+ dias = insolvência provável (muito risco)
- **Ganho esperado:** +0.4pp KS
- **Anti-leakage:** ✅ Baseado em datas do snapshot

---

## 🔧 Como Executar

### 1️⃣ Databricks (Simples)
```python
%run /Workspace/src/jobs/02_gold/06_gold_abt_v6_1_builder.py
```

### 2️⃣ Spark Submit (Com argumentos)
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/06_gold_abt_v6_1_builder.py \
  --gold_abt_v6_path /Volumes/hackathon_2025/default/gold/abt_v6_delta/ \
  --silver_pagamento_path /Volumes/hackathon_2025/default/silver/pagamento_silver_delta/ \
  --silver_atraso_path /Volumes/hackathon_2025/default/silver/atraso_silver_delta/ \
  --output_path /Volumes/hackathon_2025/default/gold/abt_v6_1_delta/
```

### 3️⃣ Python Local (Dev)
```bash
python src/jobs/02_gold/06_gold_abt_v6_1_builder.py
```

**Tempo esperado:** 15-20 minutos

---

## 📊 Resultado Esperado

### Input
```
ABT v6:                3.795.310 registros
Silver Pagamento:      21.829.628 linhas
Silver Atraso:         31.611.316 linhas
```

### Output
```
ABT v6.1:              3.795.310 registros (1:1 mantido)
Features:              72 (v6) + 9 (enhancement) = 81 total
Colunas totais:        259+
Validations:           14 (herdadas v6) + 3 (novas v6.1) = 17 gates
```

### KS Esperado
```
v4:          40.2%
v5:          42-43% (delta +2-2.5pp)
v6:          44-45% (delta +3-5pp total)
v6.1:        45-46% (delta +4.5-6pp total) ← TARGET
```

---

## ✅ Validações Implementadas

### Gates 1-14 (Herdadas de v6)
✅ Unicidade 1:1  
✅ Anti-leakage FPD  
✅ Integridade chaves  
✅ Distribuições labels  
✅ Cobertura scores/telco/cadastro/recarga  
✅ Sanidade valores  

### Gates 15-17 (Novas v6.1)
✅ **Gate 15:** DESCONTO_RATE entre 0-1  
✅ **Gate 16:** DELINQUENCY_RATE entre 0-1  
✅ **Gate 17:** MAX_DIAS_ATRASO >= 0  

---

## 📈 Estrutura do Script

```
1. CONFIGURAÇÃO
   - Caminhos padrão (UC Databricks)
   - Versão Gold: gold_abt_v6_1

2. LEITURA
   - Gold ABT v6 (spine)
   - Silver Pagamento (para ratios)
   - Silver Atraso (para aging)

3. AGREGAÇÕES COM ENHANCEMENTS
   - aggregate_pagamento_with_ratios()
     → Calcula DESCONTO_RATE M1/M3/M6
   
   - aggregate_atraso_with_enhancements()
     → Calcula DELINQUENCY_RATE M1/M3/M6
     → Calcula MAX_DIAS_ATRASO M1/M3/M6

4. JOIN
   - ABT v6 LEFT JOIN (Desconto Rates)
   - ABT v6.1 LEFT JOIN (Delinquency + Max Days)

5. VALIDAÇÕES
   - validate_abt_v6() [Gates 1-14]
   - validate_enhancements() [Gates 15-17]

6. ESCRITA
   - Delta Lake: /abt_v6_1_delta/
   - Unity Catalog: gold_abt_v6_1

7. RELATÓRIO
   - Distribuição labels
   - Cobertura features v6 (herdadas)
   - Cobertura features v6.1 (novas)
   - Estatísticas de Desconto/Delinquency/MaxDias
```

---

## 🎓 Detalhes Técnicos

### Desconto Rate Logic
```python
# Para cada período (M1, M3, M6):
SUM(val_desconto_item) / (SUM(val_desconto_item) + SUM(val_atual_pagamento))

# Quando denominador = 0:
# NULL (cliente sem pagamento, desconto_rate fica NULL)
# Depois coalesce para 0.0

# Validação:
# Resultado deve estar entre 0-1 ou ser NULL (nunca negativo ou > 1)
```

### Delinquency Rate Logic
```python
# Para cada período (M1, M3, M6):
COUNT(DISTINCT fatura com val_aberto > 0) / COUNT(DISTINCT todas as faturas)

# Quando total de faturas = 0:
# NULL (cliente sem faturas)
# Depois coalesce para 0.0

# Validação:
# Resultado deve estar entre 0-1 ou ser NULL
```

### Max Days in Arrears Logic
```python
# Para cada período (M1, M3, M6):
# Apenas para FATURAS ABERTAS (val_fat_aberto > 0):
MAX(DATEDIFF(snapshot_date, vencimento_original))

# DATEDIFF em dias (positivo = em atraso)
# Quando sem faturas abertas: NULL
# Depois coalesce para 0

# Validação:
# Resultado deve ser >= 0 (nunca negativo)
```

---

## 🔒 Anti-Leakage Validation

✅ **Pagamento:**
- Usa transações históricas (m1/m3/m6 = até 6 meses atrás)
- Não usa data futura
- Não usa resultado de aprovação

✅ **Atraso:**
- Usa snapshot mensal (DATA_REFERENCIA sempre dia 01)
- Não usa status pós-análise
- Vencimento é data original (não alterada)

---

## 📋 Checklist de Uso

- [ ] Confirmar ABT v6 existe e está validada
- [ ] Confirmar Silver Pagamento existe
- [ ] Confirmar Silver Atraso existe
- [ ] Executar script em Databricks/Spark
- [ ] Monitorar execução (15-20 min)
- [ ] Validar 17 gates passam
- [ ] Revisar estatísticas no relatório final
- [ ] Confirmar 3.795.310 registros em v6.1
- [ ] Confirmar 81 features (72+9)
- [ ] Proceder para KS measurement

---

## 📚 Documentação Relacionada

- [ABT_V6_FEATURE_ENHANCEMENT_ANALYSIS.md](ABT_V6_FEATURE_ENHANCEMENT_ANALYSIS.md) — Análise completa de todas as oportunidades
- [abt_v6.md](abt_v6.md) — Especificação técnica de v6
- [target_definition.md](../target_definition.md) — Definições críticas de anti-leakage

---

## 🚀 Próximos Passos

1. ✅ **AGORA:** Executar 06_gold_abt_v6_1_builder.py
2. ⏳ **DEPOIS:** Medir KS em OOT (esperado 45-46%)
3. ⏳ **SE ✅:** Considerar Fase 2 (Trends + Persistência)
4. ⏳ **FINAL:** Treinar modelo com v6.1 + features selecionadas

---

## ⚡ Performance

| Operação | Tempo | Recursos |
|----------|-------|----------|
| Leitura v6 | 2-3 min | Read Delta |
| Leitura Pag+Atr | 3-5 min | Read Delta |
| Agregações | 5-8 min | Spark agg |
| Joins | 2-3 min | Left joins |
| Validações | 1-2 min | Count/agg |
| Escrita | 2-3 min | Write Delta |
| **TOTAL** | **15-24 min** | **Standard cluster** |

---

**Status:** ✅ PRONTO PARA EXECUTAR  
**Data de Criação:** 25 janeiro 2026  
**Versão:** 1.0
