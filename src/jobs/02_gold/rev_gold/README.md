# rev_gold v1: Atraso + Pagamento (Baseline)

**Status:** ✅ Implementado  
**Data:** 27 de janeiro de 2026  
**Sequência:** Conforme Fernando Parahyba (Reunião 07/01/2026)

---

## 📋 Visão Geral

O v1 rev_gold é o **baseline da sequência proposta**, construindo a ABT com:

1. **ATRASO** (inadimplência/delinquência)
   - Snapshot mensal de faturas atrasadas
   - Comportamento histórico (até 12 meses)
   
2. **PAGAMENTO** (comportamento de pagamento)
   - Transações de pagamento agregadas
   - Métodos, frequência, valores

**Objetivo:** Testar se dados de delinquência/pagamento conseguem superar baseline KS 33,1% isoladamente.

---

## 🎯 Features Criadas (v1)

### ATRASO (12 features principais)

| Feature | Nome Campo | Tipo | Descrição | Sinal |
|---------|-----------|------|-----------|-------|
| 1 | `atraso_faixa_aging` | int | Faixa de antigüidade (0-3: 0-30d, 30-60d, 60-90d, 90d+) | ↑ = risco |
| 2 | `flag_write_off` | int | Indicador de write-off (contas baixadas) | ↑ = RISCO |
| 3 | `flag_pdd` | int | Possibly Defaulted (indicador de risco) | ↑ = RISCO |
| 4 | `flag_aca` | int | Ação de cobrança ativa | ↑ = RISCO |
| 5 | `atraso_faixa_tempo_base` | int | Tempo como cliente (faixa) | ↓ = cliente novo |
| 6 | `atraso_valor_aberto` | double | Valor total em atraso (R$) | ↑ = risco |
| 7 | `atraso_valor_multa_juros` | double | Multas + juros incididos | ↑ = atrasos prévios |
| 8-10 | `flag_*_sentinela` | int | Flags de dados faltantes | marcador |

### PAGAMENTO (8 features principais)

| Feature | Nome Campo | Tipo | Descrição | Sinal |
|---------|-----------|------|-----------|-------|
| 1 | `pagto_valor_atual` | double | Valor atual de pagamento (R$) | ↑ = regularidade |
| 2 | `pagto_valor_original` | double | Valor original da obrigação | contexto |
| 3 | `pagto_valor_fatura` | double | Total pago em fatura | ↑ = bom pagador |
| 4 | `pagto_desconto_total` | double | Descontos/abonos obtidos | ↓ = dificuldades |
| 5 | `pagto_juros_total` | double | Juros incididos (R$) | ↑ = atrasos prévios |
| 6 | `flag_pagto_pendente` | int | Indicador: pagamento pendente | ↑ = RISCO |
| 7 | `flag_juros_incidido` | int | Indicador: houve juros/multas | ↑ = atrasos |
| 8 | `cod_metodo_pagto` | string | Método (débito automático, boleto, etc) | débito auto = ✓ |

### DERIVADAS (3 features compostas)

| Feature | Fórmula | Descrição |
|---------|---------|-----------|
| 1 | `delinquency_rate` | (atraso_valor_aberto / (atraso_valor_aberto + pagto_valor_fatura)) * 100 | % de faturas em atraso |
| 2 | `risk_score_delinquency` | atraso_faixa_aging * delinquency_rate / 100 | Score composto (dias × taxa) |
| 3 | `flag_cliente_em_risco` | write_off OR aca OR atraso > 0 | Flag agregada de risco |

---

## 📊 Grain & Cardinalidade

**Grain esperado:** 1:1 por `NUM_CPF + SAFRA`

- **NUM_CPF:** Identificador do cliente (string)
- **SAFRA:** YYYYMM (mês de referência da fotografia)
- **Exemplo:** CPF 12345678900, SAFRA 202401 = 1 linha

**Validação (Gate 1):**
```
count(*) == count(distinct num_cpf, safra)
```

---

## 🔍 Dados de Entrada (Silver)

### Silver Atraso
- **Fonte:** `atraso_silver_delta/`
- **Grão original:** Múltiplas linhas por CPF (faturas)
- **Agregação:** Deduplicação por CPF+SAFRA (último snapshot)
- **Features usadas:** `IND_WO`, `IND_PDD`, `IND_ACA`, valores de atraso

### Silver Pagamento
- **Fonte:** `pagamento_silver_delta/`
- **Grão original:** Transacional (múltiplos pagamentos)
- **Agregação:** Deduplicação por CPF+SAFRA (última versão por TS_STATUS_FATURA DESC)
- **Features usadas:** Valores, métodos, flags de status

**JOIN:** `LEFT JOIN` em CPF + SAFRA (manter todos de Atraso)

---

## ✅ Validações (Gates)

### Gate 1: Grain (1:1 CPF+SAFRA)
```sql
SELECT COUNT(*) as total, 
       COUNT(DISTINCT num_cpf, safra) as unique_keys
FROM abt_v1_rev
-- ESPERADO: total == unique_keys
```

### Gate 2: Nenhum NULL em chaves
```sql
SELECT COUNT(*) as nulls
FROM abt_v1_rev
WHERE num_cpf IS NULL OR safra IS NULL
-- ESPERADO: 0
```

### Gate 3: Distribuição de risco
```sql
SELECT flag_cliente_em_risco, COUNT(*) as qtd, COUNT(*)*100.0/total as pct
FROM abt_v1_rev
GROUP BY flag_cliente_em_risco
-- ESPERADO: ~70-80% baixo risco, 20-30% em risco
```

### Gate 4: Completude de features
```sql
SELECT 
  COUNT(CASE WHEN atraso_valor_aberto > 0 THEN 1 END) * 100.0 / COUNT(*) as pct_com_atraso,
  COUNT(CASE WHEN pagto_valor_fatura > 0 THEN 1 END) * 100.0 / COUNT(*) as pct_com_pagto
FROM abt_v1_rev
-- ESPERADO: >70% em ambas
```

---

## 📁 Output

**Caminho:** `/Volumes/hackathon_2025/default/gold/rev_abt/abt_v1_rev_delta/`

**Tabela UC:** `hackathon_2025.rev_gold.gold_abt_v1_rev`

**Colunas (Total: ~28 features + metadata)**
```
num_cpf                      (string)
safra                        (string, YYYYMM)
atraso_faixa_aging           (int)
flag_write_off               (int)
flag_pdd                     (int)
flag_aca                     (int)
atraso_faixa_tempo_base      (int)
atraso_valor_aberto          (double)
atraso_valor_multa_juros     (double)
flag_ind_wo_sentinela        (int)
flag_ind_pdd_sentinela       (int)
flag_status_fat_missing      (int)
pagto_valor_atual            (double)
pagto_valor_original         (double)
pagto_valor_fatura           (double)
pagto_desconto_total         (double)
pagto_juros_total            (double)
flag_pagto_pendente          (int)
flag_juros_incidido          (int)
cod_metodo_pagto             (string)
delinquency_rate             (double)
risk_score_delinquency       (double)
flag_cliente_em_risco        (int)
gold_version                 (string)
gold_build_date              (timestamp)
gold_feature_blocks          (string)
num_atraso_features          (int)
num_pagamento_features       (int)
num_derivadas                (int)
```

---

## 🎓 Referências

### Reunião 07/01/2026 (Fernando Parahyba - Claro)

**Sobre Atraso/Pagamento:**
> "O comportamento de pagamento é crucial. O comportamento de recarga permite reclassificar clientes de alto risco para médio ou baixo."

**Sobre ordem incremental:**
> "Adicione uma fonte por vez para avaliar ganho individual de KS. Por exemplo, teste atraso+pagamento, depois adicione recarga, depois cadastro, Telco e, por último, scores."

**Sobre delinquência:**
> "As informações de atraso e pagamento capturam se o cliente atrasa a fatura nos últimos 12 meses, que é um sinal forte de inadimplência."

### Documentação

- [docs/01_data_dictionary/atraso.md](../../../docs/01_data_dictionary/atraso.md)
- [docs/01_data_dictionary/pagamento.md](../../../docs/01_data_dictionary/pagamento.md)
- [docs/03_silver_rules/atraso.md](../../../docs/03_silver_rules/atraso.md)
- [docs/03_silver_rules/pagamento.md](../../../docs/03_silver_rules/pagamento.md)

---

## 🔄 Próximos Passos

### v2: +Recarga
- Adicionar features temporais de recarga (M1, M3, M6)
- Comportamento de uso/consumo
- Esperado: +5pp KS

### Medições
1. Treinar modelo em v1_rev (Atraso+Pagamento)
2. Medir KS baseline
3. Comparar com v1 original (Score_01 apenas)
4. Validar diferença e ganho

### Análise
- Qual fonte contribui mais: Atraso ou Pagamento?
- Possível: treinar v1a (só Atraso) e v1b (só Pagamento) para isolamento

---

## 📝 Notas de Implementação

### Dedup em Atraso
```python
df_atraso.dropDuplicates(["num_cpf", "safra_atraso"])
```
- Atraso é snapshot mensal, pode haver múltiplas linhas (itens)
- Selecionamos a última por SAFRA (dedupd manter versão mais recente)

### Dedup em Pagamento
```python
window = Window.partitionBy("num_cpf", "safra_pagamento") \
               .orderBy(F.desc("ts_status_fatura"))
df_pagamento.withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1)
```
- Pagamento é transacional, pode haver múltiplas versões
- Mantemos a MAIS RECENTE por `ts_status_fatura`

### Anti-leakage
- Nenhum dado futuro incluído
- Ambos (Atraso e Pagamento) refletem **estado pré-evento** (anterior à migração)
- SAFRA = data de snapshot = pré-migração

---

## 🚀 Execução

### Modo Interativo (Databricks Notebook)
```python
%run /Workspace/src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py
```

### Linha de Comando
```bash
python src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py \
  --silver_atraso "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/" \
  --silver_pagamento "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/" \
  --output_path "/Volumes/hackathon_2025/default/gold/rev_abt/abt_v1_rev_delta/" \
  --target_table "hackathon_2025.rev_gold.gold_abt_v1_rev"
```

---

**Pronto para modelagem! ✓**
