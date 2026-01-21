# 🚀 Hackathon PodAcademy 2025 - Projeto de Risco de Crédito

**Status:** Em desenvolvimento | **Início:** dez/2025 | **Término:** mar/2026 | **Grupo:** Hackathon PodAcademy 2025

---

## 📋 Visão Geral

Projeto de **Engenharia de Dados + Modelagem de Risco de Crédito** seguindo metodologia **CRISP-DM** com pipeline incremental de features para suportar decisões de elegibilidade (aprovação/reprovação) no contexto de **Telecom**.

### 🎯 Objetivo Principal
Construir um modelo de **risco de crédito** com:
- ✅ Reprodutibilidade (pipeline + documentação)
- ✅ Rastreabilidade (versionamento de datasets/ABTs)
- ✅ Avaliação orientada a impacto (swap-in/swap-out, mudanças de aprovação)
- ✅ Incremento de features (Score_01 → Score_02 → Telco → ... → Atraso)

**Benchmark:** KS = 33,1 no OOT (fev/mar)

---

## 📊 Arquitetura de Dados

```
LANDING (Raw)
    ↓
BRONZE (Leitura + Metadados)
    ├── bureau_full_delta/         ✅ PRONTO
    └── telco_delta/               ✅ PRONTO
    
SILVER (Tipagem + Validação)
    ├── bureau_full_silver_delta/  ✅ PRONTO
    └── telco_silver_delta/        ✅ PRONTO
    
GOLD (ABTs para Modelagem)
    ├── abt_v1_delta/              ✅ PRONTO (Score_01)
    ├── abt_v2_delta/              ⏳ PRÓXIMO (Score_02)
    ├── abt_v3_delta/              ⏳ (Score_02 + Telco)
    ├── abt_v4_delta/              ⏳ (+ Cadastro)
    ├── abt_v5_delta/              ⏳ (+ Recarga)
    └── abt_v6_delta/              ⏳ (+ Pagamento + Atraso)
```

---

## ✅ O QUE JÁ FOI IMPLEMENTADO

### **1️⃣ Bronze Layer - Ingestão**

| Base | Script | Status | Descrição |
|------|--------|--------|-----------|
| **Bureau Full** | `00_ingest_bureau_full.py` | ✅ | Spine oficial (FLAG_INSTALACAO + FPD + SCORE_01/02) |
| **Telco** | `01_ingest_telco.py` | ✅ | Features anonimizadas (var_26-93) com sentinela 304 |

**Localização:** `src/jobs/00_bronze/`

**O que faz:**
- Lê dados brutos (Parquet) da Landing
- Adiciona metadados de auditoria (data ingestão, origem, sistema)
- Salva em Delta Lake (Bronze)
- Registra tabelas no Unity Catalog

---

### **2️⃣ Silver Layer - Transformação**

| Base | Arquivo | Status | Descrição |
|------|---------|--------|-----------|
| **Bureau Full** | `00_bronze_silver_bureau.ipynb` | ✅ | Tipagem explícita, scores ajustados, deduplicação |
| **Telco** | `01_bronze_silver_telco.py` | ✅ | Tipagem var_*, tratamento sentinela 304, flags missing |

**Localização:** `src/jobs/01_silver/`

**O que faz:**
- Tipagem explícita (string → int/double)
- Criação de variáveis derivadas (DT_SAFRA, flags de missing)
- Tratamento de sentinelas (0 em Score_01, 304 em Telco)
- Deduplicação garantindo grão 1:1 NUM_CPF + SAFRA
- Validações de domínio (quality gates)

**Funções Reutilizáveis:** `src/utils/spark_utils.py`
- `standardize_column_names()` - Padroniza para snake_case
- `to_int_safe()` - Cast string→int seguro
- `to_double_safe()` - Cast string→double seguro
- `treat_sentinel_value()` - Trata sentinelas automaticamente

---

### **3️⃣ Gold Layer v1 - ABT Score_01 Baseline**

| Componente | Arquivo | Status | Descrição |
|------------|---------|--------|-----------|
| **Builder** | `00_gold_abt_builder.py` | ✅ | Orquestra build de ABT v1 |
| **Validator** | `validators/validate_abt.py` | ✅ | 6 gates de validação automática |
| **Docs Técnicas** | `docs/04_gold_rules/abt_v1.md` | ✅ | Especificação formal |
| **Quick Start** | `docs/04_gold_rules/00_QUICK_START.md` | ✅ | Guia prático de uso |

**Localização:** `src/jobs/02_gold/`

**ABT v1 Estrutura:**
```
Chaves:
  ├── num_cpf
  ├── safra (YYYYMM)
  └── dt_safra

Labels (Auditoria):
  ├── flag_instalacao_int (decisão: 0/1)
  └── fpd_int (target risco: 0/1)

Features v1:
  ├── score_01_adj (score histórico)
  └── flag_score01_missing (sentinela)

Metadados:
  ├── prod, flag_mig2
  └── versão, data build
```

**Validações Automáticas (6 Gates):**
1. ✅ Unicidade 1:1 NUM_CPF + SAFRA
2. ✅ FPD observado SÓ em FLAG_INSTALACAO=1
3. ✅ Sem NULLs em chaves
4. ✅ FLAG_INSTALACAO com valores 0 e 1
5. ✅ FPD com valores 0 e 1 (balanceado)
6. ✅ Score_01 com cobertura > 90%

---

### **4️⃣ Documentação**

| Tipo | Localização | Status |
|------|------------|--------|
| **Data Dictionary** | `docs/01_data_dictionary/` | ✅ (bureau_full, telco, cadastro, pagamento, recarga, atraso) |
| **Data Quality** | `docs/02_data_quality/` | ✅ (relatórios de qualidade) |
| **Silver Rules** | `docs/03_silver_rules/` | ✅ (regras de transformação) |
| **Gold Rules** | `docs/04_gold_rules/` | ✅ (especificação ABT v1) |
| **Glossário** | `docs/glossary_credit_risk.md` | ✅ |
| **Target Definition** | `docs/target_definition.md` | ✅ (evento âncora + labels) |
| **Overview** | `docs/00_overview.md` | ✅ (CRISP-DM + metodologia) |

---

## 📈 Roadmap Incremental (v1→v6)

**Estratégia:** Avaliação incremental de KS por bloco de features

| Versão | Features | Status | KS Esperado | Próximas Ações |
|--------|----------|--------|------------|---|
| **v1** | Score_01 | ✅ PRONTO | ≈ 33,1 | Treinar modelo, medir KS |
| **v2** | + Score_02 | ⏳ Próximo | ΔKS = ? | Replicar padrão v1 |
| **v3** | + Telco (var_*) | ⏳ | ΔKS = ? | Adicionar 68 features |
| **v4** | + Cadastro | ⏳ | ΔKS = ? | Implementar Silver Cadastro |
| **v5** | + Recarga | ⏳ | ΔKS = ? | Agregar events Recarga |
| **v6** | + Pagamento + Atraso | ⏳ | ΔKS = ? | Agregar events Pag/Atraso |

---

## 🚀 Como Usar

### **Executar Gold v1**

```bash
# Opção 1: Databricks Notebook
%run /Workspace/src/jobs/02_gold/00_gold_abt_builder.py

# Opção 2: Spark Submit
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/00_gold_abt_builder.py

# Opção 3: Python direto
python src/jobs/02_gold/00_gold_abt_builder.py
```

### **Saída Esperada**

```
>>> [Leitura] Carregando Silver Bureau (Spine)
>>> [Info] Registros no Silver Bureau: 1,200,000

>>> [Transform] Construindo ABT v1 (Score_01)...
>>> [Validate] Executando gates de qualidade...
  [Gate 1] Unicidade: PASS
  [Gate 2] FPD observado SÓ em FLAG=1: PASS
  [Gate 3] Sem NULLs em chaves: PASS
  [Gate 4] FLAG com 0 e 1: PASS
  [Gate 5] FPD com 0 e 1: PASS
  [Gate 6] Score_01 cobertura: PASS (100%)

>>> [Sucesso] Tabela salva: gold_abt_v1

RELATÓRIO FINAL - ABT v1 (Score_01)
FLAG_INSTALACAO (decisão observada):
  FLAG=0: 500K (42%) - Reprovados
  FLAG=1: 700K (58%) - Aprovados

FPD (target, observado SÓ em FLAG=1):
  FPD=0: 600K (86%) - Bom pagador
  FPD=1: 100K (14%) - Risco
```

---

## 🔒 Anti-Leakage Garantido

**Regras Críticas Implementadas:**

```python
✗ FPD_INT NUNCA como feature (é o target!)
✗ FLAG_INSTALACAO_INT NUNCA como feature (é leakage!)
✓ Ambas incluídas SÓ para auditoria/swaps
✓ Treino SÓ em FLAG_INSTALACAO=1 (onde FPD observado)
```

---

## 📁 Estrutura do Projeto

```
hackathon-podAcademy-2025/
├── src/
│   ├── utils/
│   │   ├── spark_utils.py              ← Funções reutilizáveis
│   │   └── common.py
│   └── jobs/
│       ├── 00_bronze/                  ← Ingestão (✅)
│       │   ├── 00_ingest_bureau_full.py
│       │   └── 01_ingest_telco.py
│       ├── 01_silver/                  ← Transformação (✅)
│       │   ├── 00_bronze_silver_bureau.ipynb
│       │   └── 01_bronze_silver_telco.py
│       └── 02_gold/                    ← ABTs (✅ v1)
│           ├── 00_gold_abt_builder.py
│           └── validators/
│               └── validate_abt.py
│
├── docs/
│   ├── 00_overview.md                  ← Metodologia CRISP-DM
│   ├── target_definition.md            ← Labels + Timeline
│   ├── glossary_credit_risk.md
│   ├── 01_data_dictionary/             ← Dicionários
│   ├── 02_data_quality/                ← Relatórios QA
│   ├── 03_silver_rules/                ← Regras transformação
│   └── 04_gold_rules/                  ← Especificação ABTs
│       ├── abt_v1.md
│       └── 00_QUICK_START.md
│
├── notebooks/
│   ├── AED_*.ipynb                     ← Análises exploratórias
│   └── 20260106 - EDA Inicial/         ← EDAs por base
│
├── ANALISE_GOLD_v1.md                  ← Análise estratégica
├── IMPLEMENTATION_SUMMARY.txt          ← Resumo implementação
├── README.md                           ← Este arquivo
└── LICENSE

```

---

## ⏳ Próximos Passos (Fase 2)

### **Imediato (Próximas semanas)**

1. **Testar ABT v1 no Databricks**
   ```bash
   spark-submit src/jobs/02_gold/00_gold_abt_builder.py
   ```

2. **Validar 6 gates de qualidade**
   - Todos devem passar
   - Se falhar, revisar dados/código

3. **Treinar modelo com v1**
   - Features: `score_01_adj`, `flag_score01_missing`
   - Target: `fpd_int` (SÓ em `flag_instalacao_int=1`)
   - Métrica: KS no OOT (fev/mar)

4. **Medir KS baseline**
   - Esperado: ≈ 33,1
   - Se KS >> 33,1 → melhor que histórico! 🎉
   - Se KS << 33,1 → debugar

### **Médio Prazo (Próximas 4 semanas)**

5. **Criar ABT v2**
   - Adicionar `SCORE_02_ADJ`
   - Replicar padrão v1
   - Comparar ΔKS vs v1

6. **Criar Silver + Gold v3 (Telco)**
   - Silver Telco já está pronto
   - JOIN com bureau_full
   - Adicionar 68 features var_*

7. **Criar Silver + Gold v4 (Cadastro)**
   - Estudar dados Cadastro
   - Implementar regras Silver
   - Agregar por NUM_CPF + SAFRA

### **Longo Prazo (Próximas 8+ semanas)**

8. **Criar Gold v5 (Recarga)**
   - Base event-level
   - Agregação temporal (M1, M3, M6)
   - Features de trend + recência

9. **Criar Gold v6 (Pagamento + Atraso)**
   - Bases event-level
   - Agregação temporal similar
   - Features de cadência

10. **Documentar ganhos incrementais**
    - ΔKS por versão
    - Feature importance
    - Análise de swaps

---

## 📚 Documentação de Referência

### **Para entender o projeto:**
1. [Overview CRISP-DM](docs/00_overview.md) - Metodologia e estrutura
2. [Target Definition](docs/target_definition.md) - Labels e timeline
3. [Glossário](docs/glossary_credit_risk.md) - Conceitos de risco

### **Para dados específicos:**
- [Data Dictionary](docs/01_data_dictionary/) - Schema e tipos
- [Data Quality](docs/02_data_quality/) - Relatórios de qualidade
- [Silver Rules](docs/03_silver_rules/) - Regras de transformação

### **Para implementação Gold v1:**
- [Quick Start](docs/04_gold_rules/00_QUICK_START.md) - Como rodar
- [ABT v1 Specs](docs/04_gold_rules/abt_v1.md) - Especificação formal
- [Implementation Summary](IMPLEMENTATION_SUMMARY.txt) - Resumo

---

## 🔍 Key Decisions

### **1. Spine Oficial: Bureau_Full (v2)**
✅ Inclui reprovados (FLAG_INSTALACAO=0/1)
✅ FPD observado SÓ em FLAG=1
✅ Grão 1:1 NUM_CPF + SAFRA

### **2. Roadmap Incremental: Score_01 → v6**
✅ Começar com score histórica
✅ Adicionar Score_02 como validação
✅ Depois features externas (Telco→Cadastro→...)

### **3. Validações Automáticas: 6 Gates**
✅ Garantem qualidade sem intervalo manual
✅ Previnem leakage
✅ Rastreabilidade completa

---

## 📞 Suporte & Dúvidas

Para dúvidas sobre:
- **Pipeline:** Ver scripts em `src/jobs/`
- **Dados:** Ver documentação em `docs/`
- **Gold v1:** Ver `docs/04_gold_rules/00_QUICK_START.md`
- **Features futuras:** Ver `ANALISE_GOLD_v1.md`

---

## ✨ Status Resumido

| Componente | Status | Documentação |
|------------|--------|--------------|
| Bronze Bureau | ✅ | ✅ |
| Bronze Telco | ✅ | ✅ |
| Silver Bureau | ✅ | ✅ |
| Silver Telco | ✅ | ✅ |
| Gold v1 (Score_01) | ✅ | ✅ |
| Funções Reutilizáveis | ✅ | ✅ |
| Gold v2+ | ⏳ | 📋 |

---

## 📄 Licença

[Consulte LICENSE](LICENSE)

---

**Última atualização:** 21 jan 2026 | **Versão:** 1.0 | **Status:** Em desenvolvimento
