# Visão Geral do Projeto — Risco de Crédito Telecom

**Hackathon PodAcademy 2025** | **Início:** Dez/2025 | **Status:** Engenharia de Dados COMPLETA

---

## 1. Objetivo do Projeto

Construir um modelo de **risco de crédito** para suportar decisões de elegibilidade (aprovação/reprovação) no contexto de telecom, com:

- **Reprodutibilidade:** Pipeline documentado e versionado
- **Rastreabilidade:** Versionamento de datasets e ABTs
- **Avaliação orientada a impacto:** Análise de swap-in/swap-out
- **Adição incremental de features:** Score → Telco → Cadastro → Recarga → Pagamento → Atraso

### Target

| Item | Valor |
|------|-------|
| **Variável** | `fpd_int` (First Payment Default) |
| **Definição** | Cliente inadimpliu no primeiro pagamento |
| **Valores** | 0 = Pagou, 1 = Inadimpliu |
| **Observação** | Apenas observado quando `flag_instalacao_int = 1` |

### Benchmark

| Métrica | Valor | Período |
|---------|-------|---------|
| **KS** | 33,1 | OOT (Fev/Mar 2024) |

> O modelo desenvolvido deve **superar** este benchmark.

---

## 2. Metodologia: CRISP-DM

O projeto segue a metodologia **CRISP-DM** (Cross-Industry Standard Process for Data Mining):

| Fase | Artefatos | Localização |
|------|-----------|-------------|
| **Business Understanding** | Definição de target, regras de negócio | `docs/00_project/target_definition.md` |
| **Data Understanding** | Dicionários de dados, qualidade | `docs/01_data_dictionary/`, `docs/02_data_quality/` |
| **Data Preparation** | Regras Silver, ETLs Bronze/Silver/Gold | `docs/03_silver_rules/`, `src/jobs/` |
| **Modeling** | Seleção de features, treinamento | (próxima fase) |
| **Evaluation** | KS incremental, matriz de confusão, swaps | (próxima fase) |
| **Deployment** | Pipeline produtivo | (fase final) |

---

## 3. Arquitetura de Dados: Medallion

```
LANDING (Parquet Bruto)
    │
    ▼
BRONZE (+ metadados de ingestão)
    │   Fontes: bureau_full, telco, cadastro, recarga, pagamento, atraso
    │
    ▼
SILVER (tipado, validado, deduplicado)
    │   Transformações: type casting, tratamento de sentinelas, flags de missing
    │
    ▼
GOLD
    ├── Feature Tables (agregações)
    │   └── recarga_features_v2, pagamento_features_v2, atraso_features_v2
    │
    └── ABT (Analytical Base Table)
        └── abt_v1 → v2 → v3 → v4 → v5 → v6 (incremental)
```

### Camadas Detalhadas

| Camada | Propósito | Grão | Exemplo |
|--------|-----------|------|---------|
| **Landing** | Dados brutos originais | Variado | Arquivos Parquet |
| **Bronze** | + metadados de ingestão | Mesmo do landing | `bureau_full_delta/` |
| **Silver** | Tipado, validado | Mesmo do landing | `bureau_full_silver_delta/` |
| **Gold Features** | Agregações por cliente-mês | `num_cpf + safra` | `recarga_features_v2_delta/` |
| **Gold ABT** | Tabela analítica final | `num_cpf + safra` (1:1) | `abt_v6_v2_delta/` |

---

## 4. Fontes de Dados

### 4.1 Bureau (Spine Oficial)

A base `bureau_full` é o **spine oficial** do projeto por conter:

| Característica | Descrição |
|----------------|-----------|
| **Universo** | Aprovados E reprovados (via `FLAG_INSTALACAO`) |
| **Scores** | `SCORE_01`, `SCORE_02` |
| **Chave temporal** | `NUM_CPF + SAFRA` |
| **Target** | `FPD` observado quando `FLAG_INSTALACAO=1` |
| **Grão** | 1:1 por `NUM_CPF + SAFRA` (sem duplicidade) |

**Regra crítica:**
- `FLAG_INSTALACAO = 0`: FPD é **sempre nulo** (reprovado ou não contratou)
- `FLAG_INSTALACAO = 1`: FPD é **sempre não nulo** (0 ou 1)

### 4.2 Outras Fontes

| Fonte | Tipo | Volume | Conteúdo |
|-------|------|--------|----------|
| **Telco** | Snapshot mensal | ~3,8M | 68 variáveis anônimas (var_26-93) |
| **Cadastro** | Snapshot mensal | ~3,8M | Dados demográficos, idade, CEP |
| **Recarga** | Event-level | ~95M | Transações de recarga (SOS, crédito) |
| **Pagamento** | Event-level | ~21M | Histórico de pagamentos |
| **Atraso** | Event-level | ~31M | Faturas em aberto, aging |

---

## 5. Versões da ABT (Incremental)

Cada versão adiciona um bloco de features, permitindo medir o **lift incremental de KS**:

| Versão | Features Adicionadas | Colunas | KS Esperado |
|--------|---------------------|---------|-------------|
| **v1** | Score_01 (baseline) | ~10 | ~33,1 |
| **v2** | + Score_02 | ~12 | ~34,5 |
| **v3** | + Telco (68 vars) | ~82 | ~36,0 |
| **v4** | + Cadastro (33 vars) | 185 | ~37,0 |
| **v5** | + Recarga (M1/M3/M6) | 311 | ~37,5 |
| **v6** | + Pagamento + Atraso | **614** | ~38,0+ |

### Ordem Obrigatória de Apresentação

Conforme diretriz da coordenação, a apresentação deve mostrar o KS incremental **nesta ordem exata**:

1. Score_01 → baseline
2. \+ Score_02 → incremento
3. \+ Telco → incremento
4. \+ Cadastro → incremento
5. \+ Book Recarga → incremento
6. \+ Book Pagamento + Atraso → incremento

---

## 6. Regras Anti-Vazamento (Anti-Leakage)

### Colunas Proibidas como Features

| Coluna | Papel | Regra |
|--------|-------|-------|
| `fpd_int` | **TARGET** | NUNCA usar como feature |
| `flag_instalacao_int` | **Decisão** | NUNCA usar como feature (apenas para filtro/audit) |

### Regras Temporais

- **Treinamento:** Apenas em registros com `flag_instalacao_int = 1` (onde FPD é observado)
- **Features comportamentais:** Sempre usar `safra_feature < safra` (apenas dados passados)
- **Janelas temporais:** M1 (1 mês), M3 (3 meses), M6 (6 meses) **anteriores** à safra

### Separação de Objetivos

| Objetivo | Universo | Uso |
|----------|----------|-----|
| **Modelo de Risco** | `FLAG_INSTALACAO = 1` | Treino/validação do modelo |
| **Análise de Impacto** | Universo completo (0 e 1) | Swap-in/swap-out, simulação de cutoff |

---

## 7. Grupo de Controle

### Definição

O grupo de controle é identificado pelos dígitos 6 e 7 do CPF:

| Dígitos 6-7 | Grupo |
|-------------|-------|
| `ZZ` | Controle |
| `ZX` | Controle |
| Outros | Tratamento |

### Propósito

- Clientes que **seriam aprovados naturalmente** mas foram dados oportunidade de avaliação
- Permite **reject inference** (inferir comportamento de reprovados)
- Permite análise de **swap-in/swap-out**

---

## 8. Diretrizes de Avaliação

### Métricas Obrigatórias

| Métrica | Descrição |
|---------|-----------|
| **KS** | Kolmogorov-Smirnov (principal métrica de discriminação) |
| **Matriz de Confusão** | TP, FP, TN, FN por cutoff |
| **Swap-in/Swap-out** | Análise de mudança de aprovação |

### Períodos de Avaliação

| Período | Uso |
|---------|-----|
| Até Jan/2024 | Treino |
| Fev/2024 | Teste |
| Mar/2024 | **OOT** (Out-of-Time) |

### Foco da Avaliação

> Focar na **metade inferior da curva ROC** (zona de aprovação) para impacto financeiro.

---

## 9. Regra de Negócio do SOS

### O que é SOS?

> **SOS** é um empréstimo/adiantamento (R$3-20, tipicamente R$5) descontado da próxima recarga.

### Regras Importantes

| Regra | Descrição |
|-------|-----------|
| **Valor SOS** | R$3 a R$20 (tipicamente R$5) |
| **Desconto** | Embutido na próxima recarga |
| **Contagem** | SOS e bônus **NÃO** contam como "dinheiro real" |
| **Indicador** | Alta frequência de SOS = **estresse financeiro** |

**Exemplo:** Uma recarga de R$20 com R$5 de SOS significa R$20 de dinheiro real, não R$25.

---

## 10. Status Atual do Projeto

### Engenharia de Dados - COMPLETO

- [x] Bronze/Silver: bureau, telco, cadastro, recarga, pagamento, atraso
- [x] Gold ABT: v1-v6 implementadas
- [x] Feature generators: recarga_v2, pagamento_v2, atraso_v2
- [x] ABT v6 v2: 614 colunas, 3.795.310 registros
- [x] Todos gates de validação PASSOU
- [x] Book de Variáveis documentado

### Modelagem - PRÓXIMOS PASSOS

- [ ] Seleção de features (reduzir 614 → top features)
- [ ] Split Train/Test/OOT por SAFRA
- [ ] Modelo baseline (Regressão Logística)
- [ ] Modelo XGBoost/LightGBM
- [ ] Avaliação KS por versão da ABT (lift incremental)
- [ ] Interpretação do modelo (SHAP)
- [ ] Análise de swap-in/swap-out

---

## 11. Documentação Relacionada

| Documento | Localização | Descrição |
|-----------|-------------|-----------|
| **Definição de Target** | `target_definition.md` | Regras detalhadas do FPD |
| **Glossário** | `glossary.md` | Termos de risco de crédito |
| **Book de Variáveis** | `../04_gold_rules/BOOK_VARIABLES_ABT_V6.md` | Dicionário das 614 features |
| **Dicionários de Dados** | `../01_data_dictionary/` | Schema de cada fonte |
| **Regras Silver** | `../03_silver_rules/` | Transformações por fonte |

---

## 12. Infraestrutura

### Ambiente de Desenvolvimento

| Item | Tecnologia |
|------|------------|
| **Plataforma** | Databricks (AWS) |
| **Storage** | Delta Lake |
| **Catálogo** | Unity Catalog |
| **Orquestração** | Airflow (Astronomer) |

### Ambiente de Produção (Defesa Final)

| Item | Tecnologia |
|------|------------|
| **Plataforma** | Oracle Cloud |
| **Prazo** | 30 dias de acesso |

> Usar Databricks/AWS até a defesa de qualificação. Defesa final requer Oracle Cloud.

---

**Última Atualização:** 30 Jan 2026 | **Autor:** Equipe Hackathon PodAcademy 2025
