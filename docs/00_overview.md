# Overview — Projeto de Risco/Crédito (CRISP-DM + Roteiro Incremental de KS)

## 1) 🎯 Objetivo do projeto
Construir um modelo de **risco de crédito** para suportar decisões de elegibilidade (aprovação/reprovação) no contexto de telecom, com:
- **reprodutibilidade** (pipeline + documentação),
- **rastreabilidade** (versionamento de datasets/ABTs),
- **avaliação orientada a impacto** (swap-in/swap-out e mudanças de aprovação).

---

## 2) 🧭 Metodologia oficial: CRISP-DM
O projeto segue CRISP-DM com artefatos documentais + pipeline.

- Business Understanding: `docs/target_definition.md`
- Data Understanding: `docs/01_data_dictionary/`, `docs/02_data_quality/`
- Data Preparation: `docs/03_silver_rules/` + ETLs Bronze/Silver/Gold
- Modeling/Evaluation: KS incremental + matriz de confusão + swaps

---

## 3) 🧱 Bases principais (spine) — versão atual

### 3.1) `base_score_bureau_movel_full` (spine oficial v2)
A base `bureau_full` é o **spine oficial** do projeto por conter:
- universo com **aprovados e reprovados** (via `FLAG_INSTALACAO`)
- scores (`SCORE_01`, `SCORE_02`)
- chave temporal (`NUM_CPF + SAFRA`)
- target observado para instalados (`FPD` quando `FLAG_INSTALACAO=1`)

**Grão e chave canônica (confirmado):**
- **1:1 por `NUM_CPF + SAFRA`** (sem duplicidade)

### 3.2) `base_score_bureau_movel` (spine legado v1)
A base anterior (`bureau`) tinha:
- `FLAG_INSTALACAO = 1` constante
- ou seja, representava apenas **aprovados/contratados**

Ela permanece útil como subset, mas o spine do projeto passa a ser a versão **full**.

---

## 4) 📌 Achados essenciais da base `bureau_full` (e implicações)

### 4.1) `FLAG_INSTALACAO` e observação de `FPD`
Foi observado:
- Quando `FLAG_INSTALACAO = 0`: **`FPD` é sempre nulo**  
- Quando `FLAG_INSTALACAO = 1`: **`FPD` é sempre não nulo** (0/1)

**Implicação prática (separação de objetivos):**
- O modelo de **risco** (target `FPD`) é treinado/avaliado em:
  - universo `FLAG_INSTALACAO=1` (ou `FPD is not null`)
- A análise de **impacto de aprovação/reprovação** usa:
  - universo completo (inclui `FLAG_INSTALACAO=0/1`)
  - simulação de decisão via score do modelo (cutoff) para calcular swaps

### 4.2) Qualidade e distribuição dos scores
Evidências de qualidade:
- `SCORE_01` range: 0–778 | `SCORE_02` range: 1–926
- missing:
  - `SCORE_01` nulo/vazio: 54.035
  - `SCORE_01=0`: 15.226 (outlier/sentinela provável)
  - `SCORE_02` nulo/vazio: 1.876

Implicação:
- manter flags de missing/sentinela na Silver
- validar impacto de `SCORE_01=0` via KS incremental

---

## 5) 📈 Diretrizes de avaliação (pontos obrigatórios)
- Benchmark de referência: **KS = 33,1** no **OOT** (fev/mar)
- Obrigatório incluir:
  - matriz de confusão
  - análise de **swap-in** e **swap-out**
- Incremental obrigatório:
  1. Scores (Score 1 → Score 2)
  2. Telco
  3. Cadastro
  4. Recarga
  5. Pagamento
  6. Atraso

---

## 6) ✅ Status atual (documentação pronta)
- Bureau (v1) e Bureau_Full: dicionário/qualidade/regras Silver prontos
- Telco: dicionário/qualidade/regras Silver prontos
- Cadastro: dicionário/qualidade/regras Silver prontos
- Recarga: dicionário/qualidade/regras Silver prontos (event-level + dimensões)
- target_definition pronto (mas sujeito a alterações)

---
