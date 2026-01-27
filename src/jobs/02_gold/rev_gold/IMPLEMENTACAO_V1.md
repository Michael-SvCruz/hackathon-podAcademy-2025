# ✅ IMPLEMENTAÇÃO: rev_gold v1 (Atraso + Pagamento)

**Data:** 27 de janeiro de 2026  
**Status:** ✅ COMPLETA  
**Próximo:** Executar e testar script

---

## 📋 Entregáveis Criados

### 1. Estrutura de Pastas
```
src/jobs/02_gold/rev_gold/
├── __init__.py                           ✅ Criado
├── 00_gold_abt_v1_base.py               ✅ Criado (main script)
├── README.md                             ✅ Criado (especificação v1)
├── FEATURE_ENGINEERING_V1.md            ✅ Criado (design detalhado)
└── validators/
    └── __init__.py                       ✅ Criado
```

### 2. Script Principal: `00_gold_abt_v1_base.py`

**Funcionalidades:**
- ✅ Lê Silver Atraso
- ✅ Lê Silver Pagamento
- ✅ Agrega por CPF + SAFRA
- ✅ Cria 12 features de Atraso
- ✅ Cria 8 features de Pagamento
- ✅ Cria 3 features derivadas
- ✅ Validações de grain e completude
- ✅ Escrita em Delta + Unity Catalog
- ✅ Relatório final

**Total de features:** ~28 (23 features + metadados)

---

## 🎯 Features Implementadas (Conforme Reuniões)

### ATRASO (Inadimplência)

| # | Feature | Tipo | Descrição | Sinal |
|---|---------|------|-----------|-------|
| 1 | `atraso_faixa_aging` | int | Faixa de antigüidade (dias em atraso) | ↑ risco |
| 2 | `flag_write_off` | int | Conta baixada | ↑↑ RISCO |
| 3 | `flag_pdd` | int | Provavelmente defaultado | ↑↑ RISCO |
| 4 | `flag_aca` | int | Ação de cobrança ativa | ↑↑ RISCO |
| 5 | `atraso_faixa_tempo_base` | int | Tempo como cliente | ↓ novo = risco |
| 6 | `atraso_valor_aberto` | double | Valor em atraso (R$) | ↑ risco |
| 7 | `atraso_valor_multa_juros` | double | Multas + juros | ↑ atrasos prev. |
| 8-10 | `flag_*_sentinela` | int | Missing data flags | marcador |

**Cobertura esperada:** ~60-80% (nem todo cliente tem atraso)

---

### PAGAMENTO (Regularização)

| # | Feature | Tipo | Descrição | Sinal |
|---|---------|------|-----------|-------|
| 1 | `pagto_valor_atual` | double | Valor pago agora (R$) | ↑ bom |
| 2 | `pagto_valor_original` | double | Valor original | contexto |
| 3 | `pagto_valor_fatura` | double | Total por fatura (R$) | ↑ bom |
| 4 | `pagto_desconto_total` | double | Descontos/abonos (R$) | ↓ dificuldade |
| 5 | `pagto_juros_total` | double | Juros pagos (R$) | ↑ atrasos prev. |
| 6 | `flag_pagto_pendente` | int | Pagamento pendente | ↑ risco |
| 7 | `flag_juros_incidido` | int | Houve juros | ↑ atrasos |
| 8 | `cod_metodo_pagto` | string | Método (débito, boleto) | débito = ✓ |

**Cobertura esperada:** ~90%+ (quase todos clientes têm histórico)

---

### DERIVADAS (Features Compostas)

| # | Feature | Fórmula | Interpretação |
|---|---------|---------|----------------|
| 1 | `delinquency_rate` | (atraso / (atraso + pagto)) × 100 | % não pago |
| 2 | `risk_score_delinquency` | faixa_aging × delinquency_rate / 100 | Score composto |
| 3 | `flag_cliente_em_risco` | write_off OR aca OR atraso > 0 | Flag agregada |

**Objetivo:** Capturar sinais compostos que modelo pode explorar

---

## 📊 Design Decisions (Justificado)

### 1. Por que começar com Atraso + Pagamento?

**Fernando Parahyba (Claro) foi EXPLÍCITO:**
> "O comportamento de pagamento é crucial. As informações de atraso e pagamento capturam se o cliente atrasa a fatura."

**Racional:**
- Atraso/Pagamento = **comportamento OBSERVADO** (não especulativo)
- Score_01/Score_02 = histórico de terceiros (BIrô)
- Começar por observado = validação mais robusta

---

### 2. Por que agregar em CPF + SAFRA?

**Conforme target_definition.md:**
- Grain = cliente-mês
- CPF + SAFRA = chave única
- Atraso é transacional → agregar para 1:1

**Anti-leakage:**
- Não há dados futuros
- Snapshot mensal = pré-evento

---

### 3. Por que derivadas?

**Modelo precisa de sinais diferentes:**
- `atraso_valor_aberto` = valor absoluto (magnitude)
- `delinquency_rate` = taxa relativa (proporção)
- `risk_score_delinquency` = composto (dias × taxa)

**Modelo LGBMClassifier pode:**
- Usar todas 3, descobrir combinação ótima
- Ou ignorar redundâncias
- Deixar modelo decidir

---

### 4. Por que flags de sentinela?

**Missing data não é aleatório:**
- `-1` em DW = campo não se aplica (pode significar)
- Exemplo: `DW_FAIXA_TEMPO_BASE = -1` → cliente novo?
- Flag preserva sinal em presença de NaN

**Estratégia:**
- Tratar `-1` como NULL
- Criar `FLAG_*_MISSING`
- Modelo aprende: "missing nesse campo = +X% risco"

---

## 🔄 Fluxo de Execução

```
ENTRADA:
├─ silver/atraso_silver_delta/ (múltiplas linhas/CPF)
└─ silver/pagamento_silver_delta/ (transacional)

PROCESSAMENTO:
├─ Leitura de ambos
├─ Agregação em CPF+SAFRA
│  ├─ Atraso: dedup (manter último snapshot)
│  └─ Pagamento: dedup (manter versão mais recente por TS)
├─ Feature engineering (12 atraso + 8 pagamento)
├─ Derivadas (3 compostas)
├─ JOIN (LEFT em atraso)
└─ Preenchimento de NULLs (0 para somas, booleans)

VALIDAÇÕES:
├─ Gate 1: Grain 1:1 CPF+SAFRA
├─ Gate 2: Sem NULLs nas chaves
├─ Gate 3: Distribuição de risco
└─ Gate 4: Completude > 70%

SAÍDA:
├─ Delta: /Volumes/.../gold/abt_v1_rev_delta/
├─ Table: hackathon_2025.default.gold_abt_v1_rev
└─ Relatório: estatísticas + distribuição
```

---

## 📈 KS Esperado (Hipótese)

**Conforme padrões do mercado e reunião:**

| Versão | Features | KS Esperado | Racional |
|--------|----------|------------|----------|
| **v1 rev** | Atraso + Pagamento | **40-42%** | Comportamento observado = forte sinal |
| v1 orig | Score_01 | 33.1% | Baseline atual (conhecidoa) |
| **Δ** | Atraso+Pagamento vs Score | **+7-9pp** | Ganho esperado |

**Por quê esperar 40-42%?**
- Atraso/Pagamento captura inadimplência diretamente
- Score_01 é histórico (BIrô), menos específico para crédito
- Fernando: "comportamento = principal sinal"

---

## ✅ Checklist de Qualidade

- [x] Features criadas conforme reunião
- [x] Nenhum overlap com target (FPD_INT não usado)
- [x] Anti-leakage garantido (pré-evento)
- [x] Grain 1:1 validado em código
- [x] Derivadas inteligentes adicionadas
- [x] Documentação técnica completa
- [x] Docstrings em português
- [x] Error handling
- [x] Relatório final automático
- [x] Unity Catalog integration

---

## 🚀 Próximos Passos

### Imediato (hoje)
1. **Executar script:**
   ```bash
   python src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py
   ```
   Ou em Databricks:
   ```python
   %run /Workspace/src/jobs/02_gold/rev_gold/00_gold_abt_v1_base.py
   ```

2. **Validar output:**
   - Grain: 1:1 CPF+SAFRA?
   - Completude: >70%?
   - Distribuição: balanço razoável?

### Curto prazo (próxima reunião)
1. **Treinar modelo em v1_rev:**
   - LGBMClassifier
   - KS validation set
   
2. **Medir KS v1_rev:**
   - Comparar com v1 original (33.1%)
   - Validar Δ ≈ +7-9pp?

3. **Análise de importância:**
   - Qual feature mais importante?
   - Write-off, PDD, ou delinquency_rate?

4. **Se Δ > 5pp:** Prosseguir para v2 (Recarga)

---

## 📚 Documentação Produzida

| Documento | Uso | Status |
|-----------|-----|--------|
| `00_gold_abt_v1_base.py` | Script executável | ✅ |
| `README.md` | Especificação v1 | ✅ |
| `FEATURE_ENGINEERING_V1.md` | Design detalhado | ✅ |
| `IMPLEMENTACAO_V1.md` | Este documento | ✅ |

---

## 🎓 Insights para Modelo

### Esperamos encontrar:

1. **`flag_write_off` forte preditor:**
   - Qualquer cliente com write-off = risco altíssimo
   - Feature importance: muito alta

2. **`delinquency_rate` captura taxa:**
   - Melhor que valor absoluto
   - Normaliza contexto (cliente rico vs pobre)

3. **`atraso_faixa_tempo_base` negativa:**
   - Cliente novo (0-3m) = risco maior
   - Esperado coef negativo

4. **`pagto_valor_fatura` positiva:**
   - Cliente que paga = menor risco
   - Esperado coef positivo

---

## 🔗 Referências Internas

- [PLANO_REV_GOLD.md](../../PLANO_REV_GOLD.md) - Plano geral
- [docs/04_gold_rules/abt_v1.md](../../../../docs/04_gold_rules/abt_v1.md) - Spec original v1
- [informacoes_adicionais/01_reuniao_tira-duvidas-claro-gustavoLenin-20260107.txt](../../../../informacoes_adicionais/01_reuniao_tira-duvidas-claro-gustavoLenin-20260107.txt) - Transcrição reunião

---

**Status:** ✅ **PRONTO PARA EXECUÇÃO**

Próximo: Executar script e validar resultados!
