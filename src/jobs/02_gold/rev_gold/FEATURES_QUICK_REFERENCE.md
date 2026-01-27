# 📊 QUICK REFERENCE: Features v1 rev_gold

**Última atualização:** 27 de janeiro de 2026

---

## 🔑 Chaves e Identificadores

| Campo | Tipo | Descrição | Grain |
|-------|------|-----------|-------|
| `num_cpf` | string | Identificador do cliente | Chave |
| `safra` | string (YYYYMM) | Mês de referência (snapshot) | Chave |

**Grain Total:** 1:1 por NUM_CPF + SAFRA

---

## 🚨 ATRASO (Inadimplência) - 12 Features

### Flags de Risco Direto

| Nome | Tipo | Valores | Interpretação |
|------|------|--------|----------------|
| `flag_write_off` | int | 0/1 | Conta baixada (write-off). 1 = RISCO CRÍTICO |
| `flag_pdd` | int | 0/1 | Possibly Defaulted. 1 = risco previsto Claro |
| `flag_aca` | int | 0/1 | Ação de cobrança ativa. 1 = escalação |

### Valores/Quantidades

| Nome | Tipo | Unidade | Interpretação |
|------|------|---------|----------------|
| `atraso_valor_aberto` | double | R$ | Total em atraso. 0 = bom, >0 = risco |
| `atraso_valor_multa_juros` | double | R$ | Juros incididos. Indica atrasos prévios |

### Dimensões Categorias

| Nome | Tipo | Domínio | Interpretação |
|------|------|--------|----------------|
| `atraso_faixa_aging` | int | -1,0,1,2,3 | Antigüidade: 0-30d(0), 30-60d(1), 60-90d(2), 90d+(3) |
| `atraso_faixa_tempo_base` | int | -1,0,1,2,3 | Tempo cliente: novo(0), 3-6m(1), 6-12m(2), 12m+(3) |

### Flags de Missing

| Nome | Tipo | 0/1 | Uso |
|------|------|-----|-----|
| `flag_ind_wo_sentinela` | int | 0/1 | Write-off missing |
| `flag_ind_pdd_sentinela` | int | 0/1 | PDD missing |
| `flag_status_fat_missing` | int | 0/1 | Status fatura missing |

---

## 💳 PAGAMENTO (Regularização) - 8 Features

### Valores Monetários

| Nome | Tipo | Unidade | Interpretação |
|------|------|---------|----------------|
| `pagto_valor_atual` | double | R$ | Valor pago agora (atual) |
| `pagto_valor_original` | double | R$ | Valor original da obrigação |
| `pagto_valor_fatura` | double | R$ | Total pago por fatura |
| `pagto_desconto_total` | double | R$ | Descontos/abonos. >0 = dificuldade |
| `pagto_juros_total` | double | R$ | Juros pagos (atrasos regularizados) |

### Flags de Status

| Nome | Tipo | 0/1 | Interpretação |
|------|------|-----|----------------|
| `flag_pagto_pendente` | int | 0/1 | Pagamento pendente. 1 = não pagou |
| `flag_juros_incidido` | int | 0/1 | Houve juros. 1 = atrasou antes |

### Categorias

| Nome | Tipo | Exemplo | Interpretação |
|------|------|---------|----------------|
| `cod_metodo_pagto` | string | "DEBITO_AUTO", "BOLETO" | Método. Débito automático = melhor |

---

## 🔧 DERIVADAS (Features Compostas) - 3 Features

| Nome | Fórmula | Interpretação |
|------|---------|----------------|
| `delinquency_rate` | (atraso / (atraso+pagto)) × 100 | % não pago. 0-100 |
| `risk_score_delinquency` | faixa_aging × delinquency_rate / 100 | Score composto (dias × taxa) |
| `flag_cliente_em_risco` | write_off OR aca OR atraso>0 | Flag agregada 0/1 |

---

## 📋 METADADOS - 4 Campos

| Nome | Tipo | Valor | Uso |
|------|------|-------|-----|
| `gold_version` | string | "rev_gold_abt_v1" | Rastreabilidade |
| `gold_build_date` | timestamp | Agora | Quando foi criado |
| `gold_feature_blocks` | string | "atraso_pagamento" | Quais blocos |
| `num_atraso_features` | int | 12 | Contagem |
| `num_pagamento_features` | int | 8 | Contagem |
| `num_derivadas` | int | 3 | Contagem |

---

## 📊 RESUMO QUANTITATIVO

| Categoria | Qtd | Tipo | Total |
|-----------|-----|------|-------|
| Chaves | 2 | string | - |
| Atraso | 12 | int/double | 12 features |
| Pagamento | 8 | int/double | 8 features |
| Derivadas | 3 | int/double | 3 features |
| Metadados | 6 | mixed | - |
| **TOTAL** | | | **23 features + 6 metadados** |

---

## ⚡ CORRELAÇÕES ESPERADAS COM FPD

| Feature | Esperado | Força |
|---------|----------|-------|
| `flag_write_off` | +++ | Muito forte (positivo) |
| `flag_pdd` | ++ | Forte |
| `flag_aca` | ++ | Forte |
| `atraso_valor_aberto` | ++ | Forte |
| `atraso_valor_multa_juros` | ++ | Moderada (reincidência) |
| `atraso_faixa_tempo_base` | -- | Negativa (novo=risco) |
| `pagto_valor_fatura` | -- | Negativa (pagador=bom) |
| `delinquency_rate` | +++ | Muito forte |
| `risk_score_delinquency` | ++ | Forte |
| `flag_cliente_em_risco` | ++ | Forte |

---

## 🎯 VALORES TÍPICOS (Esperados)

### Distribuição de Atraso
```
flag_write_off: 0=95%, 1=5%
flag_pdd: 0=85%, 1=15%
flag_aca: 0=90%, 1=10%
atraso_valor_aberto: 0=60%, >0=40%
atraso_faixa_aging: 0=30%, 1=20%, 2=20%, 3=30%
```

### Distribuição de Pagamento
```
pagto_valor_fatura: median=R$150, max=R$5000
pagto_desconto_total: 0=85%, >0=15%
flag_pagto_pendente: 0=85%, 1=15%
cod_metodo_pagto: BOLETO=60%, DEBITO_AUTO=35%, OUTROS=5%
```

### Derivadas
```
delinquency_rate: 0-30% (60% clientes), 30-70% (30%), >70% (10%)
risk_score_delinquency: median=0.5, max=3.0
flag_cliente_em_risco: 0=70%, 1=30%
```

---

## ✅ COMPLETUDE ESPERADA

| Feature | Cobertura | Status |
|---------|-----------|--------|
| Chaves (num_cpf, safra) | 100% | ✅ Obrigatório |
| Atraso_valor_aberto | 60-70% | ✅ Aceitável |
| Pagto_valor_fatura | 90%+ | ✅ Bom |
| Derivadas | 100% | ✅ Calculadas |
| Missing flags | 100% | ✅ Criadas |

---

## 🔍 VERIFICAÇÃO RÁPIDA (SQL)

```sql
-- Grain
SELECT COUNT(*) as total, COUNT(DISTINCT num_cpf, safra) as unique_keys
FROM abt_v1_rev
-- Esperado: total == unique_keys

-- Distribuição de risco
SELECT flag_cliente_em_risco, COUNT(*) as qtd, 
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) as pct
FROM abt_v1_rev GROUP BY flag_cliente_em_risco

-- Completude
SELECT 
  ROUND(100.0*COUNT(CASE WHEN atraso_valor_aberto > 0 THEN 1 END)/COUNT(*), 1) as pct_atraso,
  ROUND(100.0*COUNT(CASE WHEN pagto_valor_fatura > 0 THEN 1 END)/COUNT(*), 1) as pct_pagto
FROM abt_v1_rev

-- Features criadas
SELECT COUNT(*) as total_records, COUNT(DISTINCT num_cpf) as unique_cpf
FROM abt_v1_rev
```

---

## 🚀 UTILIZAÇÃO NO MODELO

### Features para Score/Ranking
```python
# Top features para modelo LGBMClassifier
feature_importance_order = [
    'delinquency_rate',           # Taxa é mais preditiva que valor
    'flag_write_off',              # Direto: conta baixada
    'flag_cliente_em_risco',       # Agregada
    'atraso_valor_aberto',         # Magnitude
    'flag_pdd',                    # Claro's internal score
    'atraso_faixa_tempo_base',     # Novo = risco
    'pagto_valor_fatura',          # Regularidade
]
```

### Features para Interpretabilidade
```python
# Fáceis de explicar ao negócio
interpretable = [
    'flag_write_off',              # "Cliente teve conta baixada?"
    'atraso_faixa_aging',          # "Há quanto está atrasado?"
    'delinquency_rate',            # "Qual % não paga?"
    'pagto_valor_fatura',          # "Quanto pagou?"
]
```

---

## 📞 Perguntas Frequentes

**P: O que significa atraso_faixa_aging = 3?**  
R: Cliente tem 90+ dias em atraso. RED FLAG.

**P: flag_write_off = 1 é automático nega?**  
R: Deve ser. Cliente com conta baixada = não creditável.

**P: Posso usar delinquency_rate diretamente?**  
R: Sim. Modelo vai usar + derivadas juntas.

**P: E se pagto_valor_fatura = 0?**  
R: Cliente não pagou nada (ou é novo). Combined com atraso_valor_aberto.

**P: Missing data é problema?**  
R: Não. Criamos flags para capturar signal.

---

**Versão:** v1.0  
**Próxima:** v2 (+ Recarga)  
**Status:** ✅ Pronto
