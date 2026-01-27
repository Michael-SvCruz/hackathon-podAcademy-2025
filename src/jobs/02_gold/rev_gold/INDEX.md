# 📑 rev_gold v1: ÍNDICE COMPLETO

**Última atualização:** 27 de janeiro de 2026

---

## 📂 Arquivos Criados

### Scripts Executáveis

| Arquivo | Localização | Propósito | Status |
|---------|-----------|----------|--------|
| `00_gold_abt_v1_base.py` | `src/jobs/02_gold/rev_gold/` | **Script principal** - Constrói ABT v1 | ✅ Pronto |

---

### Documentação Técnica

| Arquivo | Localização | Conteúdo | Público |
|---------|-----------|----------|---------|
| `README.md` | `src/jobs/02_gold/rev_gold/` | Especificação v1 (grain, features, validações) | Técnico |
| `FEATURE_ENGINEERING_V1.md` | `src/jobs/02_gold/rev_gold/` | Design detalhado de cada feature (12+8+3) | Técnico |
| `IMPLEMENTACAO_V1.md` | `src/jobs/02_gold/rev_gold/` | Sumário de implementação + hipótese KS | Técnico |
| `FEATURES_QUICK_REFERENCE.md` | `src/jobs/02_gold/rev_gold/` | Tabelas de referência rápida | Técnico |
| `SUMARIO_FINAL.md` | `src/jobs/02_gold/rev_gold/` | Executivo: O que foi entregue | Ambos |

---

## 🎯 Roteiros de Uso

### "Quero entender rev_gold rapidamente"
→ Leia: **`SUMARIO_FINAL.md`** (5 min) + **`FEATURES_QUICK_REFERENCE.md`** (3 min)

### "Quero entender o design de cada feature"
→ Leia: **`FEATURE_ENGINEERING_V1.md`** (20 min)

### "Quero saber como executar o script"
→ Leia: **`README.md`** → seção "Execução" + docstring do script

### "Quero ver tabelas de referência"
→ Vá para: **`FEATURES_QUICK_REFERENCE.md`**

### "Quero entender próximas fases"
→ Leia: **`PLANO_REV_GOLD.md`** (projeto raiz)

---

## 📊 Features Criadas (Sumário)

### ATRASO (12 features)
- `atraso_faixa_aging` - Faixa de antigüidade em dias
- `flag_write_off` - Conta baixada (CRÍTICO)
- `flag_pdd` - Possibly Defaulted (Claro's score)
- `flag_aca` - Ação de cobrança ativa
- `atraso_faixa_tempo_base` - Tempo como cliente
- `atraso_valor_aberto` - Valor em atraso (R$)
- `atraso_valor_multa_juros` - Juros incididos
- `flag_ind_wo_sentinela` - Missing flag
- `flag_ind_pdd_sentinela` - Missing flag
- `flag_status_fat_missing` - Missing flag
- (+ 2 adicionais internos)

### PAGAMENTO (8 features)
- `pagto_valor_atual` - Valor pago agora
- `pagto_valor_original` - Valor original obrigação
- `pagto_valor_fatura` - Total por fatura
- `pagto_desconto_total` - Descontos/abonos
- `pagto_juros_total` - Juros pagos
- `flag_pagto_pendente` - Pendente
- `flag_juros_incidido` - Houve juros
- `cod_metodo_pagto` - Método (débito, boleto, etc)

### DERIVADAS (3 features)
- `delinquency_rate` - (atraso / (atraso+pagto)) × 100
- `risk_score_delinquency` - faixa × rate / 100
- `flag_cliente_em_risco` - write_off OR aca OR atraso>0

**Total: 23 features + 6 metadados**

---

## 🔄 Fluxo de Execução

```mermaid
ENTRADA
  ├─ silver/atraso_silver_delta/
  └─ silver/pagamento_silver_delta/
       ↓
AGREGAÇÃO
  ├─ Por CPF+SAFRA (1:1 grain)
  ├─ 12 features Atraso
  ├─ 8 features Pagamento
  └─ 3 features Derivadas
       ↓
VALIDAÇÕES
  ├─ Gate 1: Grain 1:1
  ├─ Gate 2: Sem NULLs chaves
  ├─ Gate 3: Distribuição
  └─ Gate 4: Completude >70%
       ↓
SAÍDA
  ├─ Delta: /Volumes/.../abt_v1_rev_delta/
  ├─ Table: gold_abt_v1_rev (Databricks)
  └─ Relatório: Estatísticas + distribuição
```

---

## 🎯 KS Esperado

| Versão | Features | KS | Δ |
|--------|----------|-----|-----|
| **v1_rev** | Atraso + Pagamento | **40-42%** | **+7-9pp** vs v1_orig |
| v1_orig | Score_01 | 33.1% | baseline |

---

## ✅ Validações

### Gate 1: Grain 1:1 CPF+SAFRA ✅
```
count(*) == count(distinct num_cpf, safra)
```

### Gate 2: Nenhum NULL chaves ✅
```
count(where num_cpf IS NULL OR safra IS NULL) == 0
```

### Gate 3: Distribuição ✅
```
~70% flag_cliente_em_risco=0
~30% flag_cliente_em_risco=1
```

### Gate 4: Completude ✅
```
atraso_valor_aberto: >60%
pagto_valor_fatura: >90%
```

---

## 🚀 Como Executar

### 1. Databricks Notebook
```python
%run /Workspace/src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py
```

### 2. Linha de Comando
```bash
python src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py
```

### 3. Com Args Customizados
```bash
python src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py \
  --silver_atraso "/Volumes/.../silver/atraso_silver_delta/" \
  --silver_pagamento "/Volumes/.../silver/pagamento_silver_delta/" \
  --output_path "/Volumes/.../gold/abt_v1_rev_delta/"
```

---

## 📋 Perguntas Frequentes

**P: Qual a diferença entre v1 rev e v1 original?**
- Original: Score_01 (baseline) → Telco → Cadastro → Recarga → Atraso
- rev_gold: Atraso + Pagamento (baseline) → Recarga → Cadastro → Telco → Scores
- Impacto: Testa se delinquência é melhor preditor que score

---

**P: Por que começar com Atraso + Pagamento?**
- Fernando Parahyba (Claro): "O comportamento de pagamento é crucial"
- Delinquência = sinal direto vs Score = sinal indireto

---

**P: Quando sair para v2 (Recarga)?**
- Se KS v1_rev > 38% (KS v1_orig + 5pp)
- Validar Δ ≈ +5-7pp vs v1_orig

---

**P: Preciso alterar Silver?**
- Não. Silver é agnóstico. Ambas estratégias compartilham.

---

**P: Como interpretar delinquency_rate?**
- (atraso / (atraso + pagto)) × 100
- 0% = pagou tudo (bom)
- 50% = metade não paga (risco)
- 100% = nada pagou (crítico)

---

## 🔗 Links Internos

**rev_gold:**
- [PLANO_REV_GOLD.md](../../PLANO_REV_GOLD.md) - Plano geral

**Projeto:**
- [docs/target_definition.md](../../../../docs/target_definition.md)
- [docs/04_gold_rules/abt_v1.md](../../../../docs/04_gold_rules/abt_v1.md)
- [informacoes_adicionais/](../../../../informacoes_adicionais/) - Reuniões

---

## 📅 Timeline

| Data | Evento |
|------|--------|
| 27/01 | ✅ Implementação v1 |
| 27/01 | → Testes + KS |
| 28/01 | → v2 (Recarga) |
| 29/01 | → v3 (Cadastro) |
| 30/01 | → v4 (Telco) |
| 31/01 | → v5 (Score_01) |

---

## 🎓 Próximos Passos

1. **Executar** `00_gold_abt_v1_base.py`
2. **Validar** grain, completude, distribuição
3. **Treinar** modelo (LGBMClassifier)
4. **Medir** KS (validação)
5. **Comparar** com v1_orig (33.1%)
6. **Decidir** → v2 se Δ > 5pp

---

## 📞 Contato / Dúvidas

Documentação completa em:
- `src/jobs/02_gold/rev_gold/`

Perguntas sobre features:
- `FEATURE_ENGINEERING_V1.md` → "Design das Features"

Perguntas sobre execução:
- `README.md` → "Execução"

---

**Status:** ✅ **PRONTO**  
**Próximo:** Executar e testar 🚀

---

*Documento gerado automaticamente em 27 de janeiro de 2026*
