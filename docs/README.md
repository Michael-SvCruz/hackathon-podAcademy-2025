# 📚 Documentação do Hackathon 2025 — Índice Completo

**Data:** 25 de janeiro de 2026  
**Status:** ✅ ORGANIZADO POR VERSÃO  

---

## 📁 Estrutura de Pastas

```
docs/
├── 00_overview.md                    (Visão geral do projeto)
├── 05_recarga_silver_quality_report.md (Quality insights)
├── glossary_credit_risk.md           (Glossário)
├── target_definition.md              (Definição de target)
│
├── 01_data_dictionary/               (Dicionários de dados)
│   ├── atraso.md
│   ├── bureau.md
│   ├── bureau_full.md
│   ├── cadastro.md
│   ├── pagamento.md
│   ├── recarga.md
│   └── telco.md
│
├── 02_data_quality/                  (Regras de qualidade)
│   ├── atraso.md
│   ├── bureau.md
│   ├── bureau_full.md
│   ├── cadastro.md
│   ├── pagamento.md
│   ├── recarga.md
│   └── telco.md
│
├── 03_silver_rules/                  (Regras de transformação Silver)
│   ├── atraso.md
│   ├── bureau.md
│   ├── bureau_full.md
│   ├── cadastro.md
│   ├── pagamento.md
│   ├── recarga.md
│   └── telco.md
│
├── 04_gold_rules/                    (Especificações Gold ABT)
│   ├── 00_QUICK_START.md             (Como rodar v1-v6)
│   ├── README.md                     (Índice de specs)
│   ├── abt_v1.md                     (Baseline)
│   ├── abt_v2.md
│   ├── abt_v3.md
│   ├── abt_v4.md
│   ├── abt_v5.md                     (⬇️ MOVED)
│   └── abt_v6.md                     (⬇️ MOVED)
│
├── 05_abt_v5_docs/                   ← 🆕 ABT v5 DOCUMENTAÇÃO COMPLETA
│   ├── README.md                     (Índice e how-to)
│   ├── abt_v5.md                     (Spec técnica)
│   ├── ABT_V5_IMPLEMENTATION_SUMMARY.md
│   ├── ABT_V5_QUICK_REFERENCE.md
│   ├── ABT_V5_DELIVERABLES_CHECKLIST.md
│   ├── ABT_V5_SUMMARY.txt
│   ├── ABT_V5_INDEX.md
│   └── 05_recarga_silver_quality_report.md
│
└── 06_abt_v6_docs/                   ← 🆕 ABT v6 DOCUMENTAÇÃO COMPLETA
    ├── README.md                     (Índice e how-to)
    ├── abt_v6.md                     (Spec técnica)
    ├── ABT_V6_IMPLEMENTATION_SUMMARY.md
    ├── ABT_V6_QUICK_REFERENCE.md
    ├── ABT_V6_DELIVERABLES_CHECKLIST.md
    ├── ABT_V6_SUMMARY.txt
    └── ABT_V6_INDEX.md
```

---

## 🎯 Por Onde Começar?

### Para Entender o Projeto
1. Leia: **00_overview.md**
2. Leia: **target_definition.md**
3. Leia: **glossary_credit_risk.md**

### Para Implementar v1-v6
1. Leia: **04_gold_rules/00_QUICK_START.md** (guia de execução)
2. Leia: **04_gold_rules/README.md** (índice de specs)
3. Escolha sua versão (v1-v6)

### Para v5 Específicamente
1. Leia: **05_abt_v5_docs/README.md** (how-to)
2. Escolha documento baseado em seu papel (tech/science/lead/pm)
3. Ver pasta completa: **05_abt_v5_docs/**

### Para v6 Específicamente
1. Leia: **06_abt_v6_docs/README.md** (how-to)
2. Escolha documento baseado em seu papel
3. Ver pasta completa: **06_abt_v6_docs/**

---

## 📊 Documentação por Versão

### ABT v1 (Baseline)
- **Arquivo:** `04_gold_rules/abt_v1.md`
- **Status:** ✅ Baseline
- **Features:** Score_01
- **KS:** ~33.1%

### ABT v2
- **Arquivo:** `04_gold_rules/abt_v2.md`
- **Status:** ✅ Done
- **Features:** Score_01 + Score_02
- **KS:** ~34-35%

### ABT v3
- **Arquivo:** `04_gold_rules/abt_v3.md`
- **Status:** ✅ Done
- **Features:** Scores + Telco
- **KS:** ~36%

### ABT v4
- **Arquivo:** `04_gold_rules/abt_v4.md`
- **Status:** ✅ Done
- **Features:** Scores + Telco + Cadastro
- **KS:** ~36-38%

### ABT v5 ⭐
- **Pasta:** `05_abt_v5_docs/`
- **Status:** ✅ COMPLETO (22 jan 2026)
- **Features:** +18 (Recarga M1/M3/M6)
- **KS:** ~42%
- **Documentação:** 7 arquivos (2000+ linhas)
- **Key Docs:**
  - `abt_v5.md` — Spec técnica
  - `ABT_V5_IMPLEMENTATION_SUMMARY.md` — Tech lead view
  - `ABT_V5_QUICK_REFERENCE.md` — One-page lookup
  - `ABT_V5_DELIVERABLES_CHECKLIST.md` — QA matrix

### ABT v6 ⭐⭐
- **Pasta:** `06_abt_v6_docs/`
- **Status:** ✅ PRODUCTION READY (23 jan 2026)
- **Features:** +72 (36 Pagamento + 36 Atraso, M1/M3/M6)
- **KS:** ~44-45% (esperado)
- **Validação:** 14/14 gates PASS ✅
- **Documentação:** 6 arquivos (2000+ linhas)
- **Key Docs:**
  - `abt_v6.md` — Spec técnica (anti-leakage crítico)
  - `ABT_V6_IMPLEMENTATION_SUMMARY.md` — Métricas confirmadas
  - `ABT_V6_QUICK_REFERENCE.md` — Features detalhadas
  - `ABT_V6_DELIVERABLES_CHECKLIST.md` — Sign-off matrix

---

## 👥 Documentação por Público

### 👨‍💻 Engenheiros de Dados

**Quick Start:**
1. `04_gold_rules/00_QUICK_START.md` (todos os v1-v6)
2. Para v5: `05_abt_v5_docs/abt_v5.md` (seções 4-6)
3. Para v6: `06_abt_v6_docs/abt_v6.md` (seções 4-6)

**Referência:**
- `05_abt_v5_docs/ABT_V5_QUICK_REFERENCE.md`
- `06_abt_v6_docs/ABT_V6_QUICK_REFERENCE.md`

**Código:**
- `src/jobs/02_gold/` (scripts)
- `src/jobs/01_silver/` (transformações)
- `src/jobs/00_bronze/` (ingestão)

---

### 📊 Data Scientists

**Features (v5):**
1. `05_abt_v5_docs/abt_v5.md` (seção 4)
2. `05_abt_v5_docs/05_recarga_silver_quality_report.md`
3. `05_abt_v5_docs/ABT_V5_IMPLEMENTATION_SUMMARY.md`

**Features (v6):**
1. `06_abt_v6_docs/abt_v6.md` (seção 4)
2. `06_abt_v6_docs/ABT_V6_QUICK_REFERENCE.md`
3. `06_abt_v6_docs/ABT_V6_IMPLEMENTATION_SUMMARY.md`

**Próximo:** Variable Book (⏳ pending)

---

### 🎓 Tech Leads

**v5:**
1. `05_abt_v5_docs/ABT_V5_IMPLEMENTATION_SUMMARY.md` (início)
2. `05_abt_v5_docs/ABT_V5_DELIVERABLES_CHECKLIST.md` (cobertura)
3. `05_abt_v5_docs/ABT_V5_INDEX.md` (roadmap)

**v6:**
1. `06_abt_v6_docs/ABT_V6_IMPLEMENTATION_SUMMARY.md` (início)
2. `06_abt_v6_docs/ABT_V6_DELIVERABLES_CHECKLIST.md` (cobertura)
3. `06_abt_v6_docs/ABT_V6_INDEX.md` (roadmap)

**Crítico (v6):**
- Anti-leakage: `06_abt_v6_docs/abt_v6.md` (seção 10)
- Gates: `06_abt_v6_docs/abt_v6.md` (seção 8)

---

### 📈 Project Managers

**v5:**
1. `05_abt_v5_docs/ABT_V5_SUMMARY.txt` (status visual)
2. `05_abt_v5_docs/ABT_V5_QUICK_REFERENCE.md` (quick stats)
3. `05_abt_v5_docs/ABT_V5_DELIVERABLES_CHECKLIST.md` (matriz)

**v6:**
1. `06_abt_v6_docs/ABT_V6_SUMMARY.txt` (status visual)
2. `06_abt_v6_docs/ABT_V6_QUICK_REFERENCE.md` (quick stats)
3. `06_abt_v6_docs/ABT_V6_DELIVERABLES_CHECKLIST.md` (sign-off)

---

## 📋 Data Dictionary & Quality

### Dados Brutos (Bronze)
- Dicionários: `01_data_dictionary/`
- Qualidade: `02_data_quality/`

### Dados Transformados (Silver)
- Regras: `03_silver_rules/`
- Qualidade: `02_data_quality/`

### Dados Analíticos (Gold)
- v1-v6: `04_gold_rules/abt_vX.md`
- v5: `05_abt_v5_docs/abt_v5.md`
- v6: `06_abt_v6_docs/abt_v6.md`

---

## 🔗 Referências Cruzadas

### Links Importantes
- **Target Definition:** `target_definition.md`
- **Glossário:** `glossary_credit_risk.md`
- **Overview:** `00_overview.md`

### Scripts Relacionados
- **v5 Builder:** `src/jobs/02_gold/04_gold_abt_v5_builder.py`
- **v6 Builder:** `src/jobs/02_gold/05_gold_abt_v6_builder.py`
- **Validators:** `src/jobs/02_gold/validators/validate_abt.py`

### Meeting Notes
- **informacoes_adicionais/**: PDFs das reuniões do hackathon

---

## ✅ Status Geral

| Versão | Status | Data | KS | Documentação |
|--------|--------|------|----|----|
| v1 | ✅ Done | - | 33.1% | `04_gold_rules/abt_v1.md` |
| v2 | ✅ Done | - | 34-35% | `04_gold_rules/abt_v2.md` |
| v3 | ✅ Done | - | 36% | `04_gold_rules/abt_v3.md` |
| v4 | ✅ Done | - | 36-38% | `04_gold_rules/abt_v4.md` |
| v5 | ✅ Complete | 22 jan | 42% | `05_abt_v5_docs/` (7 docs) |
| v6 | ✅ Ready | 23 jan | 44-45% | `06_abt_v6_docs/` (6 docs) |
| v7 | ⏳ Planned | - | 45%+ | (future) |

---

## 🚀 Próximas Ações

1. **Validação KS v6** (⏳ próxima semana)
   - Esperado: 44-45%
   - Se OK: Production approval

2. **Variable Book** (⏳ próximas 2 semanas)
   - 250+ features documentadas
   - Para Data Scientists

3. **Model Development** (⏳ próximo mês)
   - XGBoost/Logistic Regression
   - OOT validation

---

## 📞 Como Navegar

- **Novo no projeto?** Comece com `00_overview.md`
- **Quer rodar v5/v6?** Vá para `04_gold_rules/00_QUICK_START.md`
- **Quer ver docs v5?** Abra `05_abt_v5_docs/README.md`
- **Quer ver docs v6?** Abra `06_abt_v6_docs/README.md`
- **Quer data dictionary?** Abra `01_data_dictionary/`

---

**Última Atualização:** 25 janeiro 2026  
**Próxima Revisão:** Quando Variable Book for criado
