# Hackathon PodAcademy 2025 - Modelagem de Risco de Crédito


**Status:** Engenharia de Dados COMPLETA | **Início:** Dez/2025 | **Grupo:** Hackathon PodAcademy 2025

---

## Referência Rápida

| Item | Valor |
|------|-------|
| **ABT Final** | `hackathon_2025.default.gold_abt_v6_v2` |
| **Registros** | 3.795.310 |
| **Features** | 614 colunas (~250+ engenheiradas) |
| **Target** | `fpd_int` (First Payment Default) |
| **Grão** | `num_cpf + safra` (1:1) |
| **Benchmark** | KS = 33,1 no OOT (Fev/Mar) |

---

## Visão Geral do Projeto

Projeto de **Modelagem de Risco de Crédito** para Telecom utilizando **Arquitetura Medallion** (Bronze - Silver - Gold) com versionamento incremental de ABT (Analytical Base Table). Objetivo: prever inadimplência no primeiro pagamento (FPD) para decisões de elegibilidade de clientes.

### Objetivos
- **Reprodutibilidade:** Pipeline documentado e versionado
- **Rastreabilidade:** Versionamento de datasets e ABTs
- **Avaliação orientada a impacto:** Análise de swap-in/swap-out
- **Adição incremental de features:** Score → Telco → Cadastro → Recarga → Pagamento → Atraso

---

## Arquitetura

```
LANDING (Parquet Bruto)
    │
    ▼
BRONZE (+ metadados)
    ├── bureau_full_delta/
    ├── telco_delta/
    ├── cadastro_delta/
    ├── recarga_delta/
    ├── pagamento_delta/
    └── atraso_delta/
    │
    ▼
SILVER (tipado, validado)
    ├── bureau_full_silver_delta/
    ├── telco_silver_delta/
    ├── cadastro_silver_delta/
    ├── recarga_silver_delta/
    ├── pagamento_silver_delta/
    └── atraso_silver_delta/
    │
    ▼
GOLD (Tabelas de Features)              GOLD (ABTs)
    ├── recarga_features_v2_delta/          ├── abt_v1_delta/ (Score_01)
    ├── pagamento_features_v2_delta/        ├── abt_v2_delta/ (+ Score_02)
    └── atraso_features_v2_delta/           ├── abt_v3_delta/ (+ Telco 68 vars)
                                            ├── abt_v4_delta/ (+ Cadastro 33 vars) → 185 cols
                                            ├── abt_v5_v2_delta/ (+ Recarga M1/M3/M6) → 311 cols
                                            └── abt_v6_v2_delta/ (+ Pagamento + Atraso) → 614 cols
```

---

## Versões da ABT (Incremental)

| Versão | Features Adicionadas | Colunas | Status | KS Esperado |
|--------|---------------------|---------|--------|-------------|
| **v1** | Score_01 | ~10 | COMPLETO | ~33,1 |
| **v2** | + Score_02 | ~12 | COMPLETO | ~34,5 |
| **v3** | + Telco (68 vars) | ~82 | COMPLETO | ~36,0 |
| **v4** | + Cadastro (33 vars) | 185 | COMPLETO | ~37,0 |
| **v5 v2** | + Recarga (M1/M3/M6) | 311 | COMPLETO | ~37,5 |
| **v6 v2** | + Pagamento + Atraso | **614** | **COMPLETO** | ~38,0+ |

---

## Resultados da Execução do Pipeline

### Geradores de Features

| Camada | Entrada (Eventos) | Saída (Cliente-Mês) | Compressão | Colunas |
|--------|-------------------|---------------------|------------|---------|
| Recarga v2 | 95.210.519 | 32.882.218 | 2,9x | 51 |
| Pagamento v2 | 21.821.465 | 12.634.799 | 1,7x | 49 |
| Atraso v2 | 31.611.316 | 15.023.012 | 2,1x | 58 |

### Construtores de ABT

| ABT | Registros | Colunas | Validação |
|-----|-----------|---------|-----------|
| v5 v2 | 3.795.310 | 311 | 11/11 gates PASSOU |
| v6 v2 | 3.795.310 | 614 | Todos gates PASSOU |

### Cobertura por Bloco de Features (Janela M1)

| Bloco de Features | Cobertura |
|-------------------|-----------|
| Score_01 | 98,18% |
| Score_02 | 99,95% |
| Telco | 35,46% |
| Recarga M1 | 56,12% |
| Pagamento M1 | 16,13% |
| Atraso M1 | 21,79% |

### Distribuição de Labels (ABT v6 Final)

- **FLAG=1 (Aprovado):** 2.633.900 (69,40%)
- **FLAG=0 (Reprovado):** 1.161.410 (30,60%)
- **FPD=1 (em FLAG=1):** 559.229 (21,23%)

---

## Como Executar

### Pipeline Completo (Databricks)

```bash
# Passo 1: Gerar features de Recarga (60+ features comportamentais)
%run /Workspace/src/jobs/02_gold/gold_recarga_features_v2.py

# Passo 2: Construir ABT v5 (v4 + Recarga M1/M3/M6)
%run /Workspace/src/jobs/02_gold/04_gold_abt_v5_builder_v2.py

# Passo 3: Gerar features de Pagamento (50+ features)
%run /Workspace/src/jobs/02_gold/gold_pagamento_features_v2.py

# Passo 4: Gerar features de Atraso (60+ features)
%run /Workspace/src/jobs/02_gold/gold_atraso_features_v2.py

# Passo 5: Construir ABT v6 (v5 + Pagamento + Atraso) - FINAL
%run /Workspace/src/jobs/02_gold/05_gold_abt_v6_builder_v2.py
```

### Acessar ABT Final (SQL)

```sql
-- ABT final para modelagem (614 colunas)
SELECT * FROM hackathon_2025.default.gold_abt_v6_v2;

-- Tabelas de features
SELECT * FROM hackathon_2025.default.gold_recarga_features_v2;
SELECT * FROM hackathon_2025.default.gold_pagamento_features_v2;
SELECT * FROM hackathon_2025.default.gold_atraso_features_v2;
```

### Caminhos Delta

```
/Volumes/hackathon_2025/default/gold/abt_v6_v2_delta/
/Volumes/hackathon_2025/default/gold/abt_v5_v2_delta/
/Volumes/hackathon_2025/default/gold/recarga_features_v2_delta/
/Volumes/hackathon_2025/default/gold/pagamento_features_v2_delta/
/Volumes/hackathon_2025/default/gold/atraso_features_v2_delta/
```

---

## Principais Features por Bloco

### Recarga (Indicadores de Estresse Financeiro)
- `freq_sos_m1` - Frequência de uso do SOS (empréstimo/adiantamento)
- `pct_sos_sobre_credito_m1` - Razão SOS/Crédito
- `coef_variacao_val_m1` - Instabilidade de valores
- `dias_max_entre_recargas_m1` - Períodos de inatividade
- `ticket_medio_m1` - Valor médio de recarga

### Pagamento (Comportamento de Atraso Passado)
- `pct_pagamentos_com_juros_m1` - % com juros (atrasos passados)
- `flag_sempre_com_juros_m1` - Padrão de sempre pagar atrasado
- `ratio_juros_pago_m1` - Intensidade de juros
- `sum_val_juros_pos_m1` - Volume de juros pagos

### Atraso (Risco Atual)
- `pct_aging_90_plus_m1` - % inadimplência grave (>90 dias)
- `flag_risco_alto_m1` - Flag WO/PDD/Fraude
- `sum_val_aberto_m1` - Saldo em aberto
- `ratio_aberto_faturado_m1` - Taxa de inadimplência

---

## Regras Anti-Vazamento (Anti-Leakage)

| Coluna | Papel | Regra |
|--------|-------|-------|
| `fpd_int` | **TARGET** | NUNCA usar como feature |
| `flag_instalacao_int` | **Decisão** | NUNCA usar como feature |

**Treinamento:** Apenas em registros com `flag_instalacao_int=1` (onde FPD é observado)

**Temporal:** Todas features comportamentais usam `safra_feature < safra` (apenas dados passados)

---

## Estrutura do Projeto

```
hackathon-podAcademy-2025/
│
├── README.md                          # Este arquivo
├── LICENSE
├── .claude/                           # Configuração do assistente IA
│   └── CLAUDE.md                      # Instruções para Claude Code
│
├── docs/                              # Toda documentação
│   ├── README.md                      # Guia de navegação da documentação
│   ├── 00_project/                    # Visão geral, target, glossário
│   ├── 01_data_dictionary/            # Dicionários de dados por fonte
│   ├── 02_data_quality/               # Relatórios de qualidade
│   ├── 03_silver_rules/               # Regras de transformação Silver
│   ├── 04_gold_rules/                 # Especificações ABT + book de variáveis
│   ├── 05_abt_v5_docs/                # Documentação Recarga (ABT v5)
│   ├── 06_abt_v6_docs/                # Documentação Pagamento/Atraso (ABT v6)
│   ├── 07_troubleshooting/            # Guias de correção e diagnósticos
│   └── 99_archive/                    # Documentação histórica/obsoleta
│
├── src/
│   ├── utils/
│   │   ├── spark_utils.py             # Funções reutilizáveis
│   │   └── validate_abt.py            # Gates de validação
│   └── jobs/
│       ├── 00_bronze/                 # Landing → Bronze
│       ├── 01_silver/                 # Bronze → Silver
│       └── 02_gold/
│           ├── 00_gold_abt_builder.py        # ABT v1
│           ├── 01_gold_abt_v2_builder.py     # ABT v2
│           ├── 02_gold_abt_v3_builder.py     # ABT v3
│           ├── 03_gold_abt_v4_builder.py     # ABT v4
│           ├── 04_gold_abt_v5_builder_v2.py  # ABT v5 v2
│           ├── 05_gold_abt_v6_builder_v2.py  # ABT v6 v2
│           ├── gold_recarga_features_v2.py   # Features Recarga (60+)
│           ├── gold_pagamento_features_v2.py # Features Pagamento (50+)
│           └── gold_atraso_features_v2.py    # Features Atraso (60+)
│
├── notebooks/                         # Notebooks Jupyter
├── tests/                             # Suite de testes
├── infrastructure/                    # Configurações de infraestrutura
└── astro_airflow/                     # DAGs do Airflow
```

---

## Documentação

| Documento | Localização | Descrição |
|-----------|-------------|-----------|
| **Guia de Navegação** | `docs/README.md` | Como encontrar documentação |
| **Book de Variáveis** | `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md` | Dicionário completo (614 vars) |
| **Definição de Target** | `docs/00_project/target_definition.md` | Labels + regras anti-vazamento |
| **Visão Geral** | `docs/00_project/overview.md` | Metodologia CRISP-DM |
| **Glossário** | `docs/00_project/glossary.md` | Termos de risco de crédito |
| **Dicionários de Dados** | `docs/01_data_dictionary/` | Schemas das fontes |
| **Troubleshooting** | `docs/07_troubleshooting/` | Guias de correção de problemas |

---

## Status do Projeto

### Engenharia de Dados - COMPLETO
- [x] Bronze/Silver: bureau, telco, cadastro, recarga, pagamento, atraso
- [x] Gold ABT: v1-v6 implementadas
- [x] Recarga v2: 60+ features comportamentais
- [x] Pagamento v2: 50+ features de pagamento
- [x] Atraso v2: 60+ features de atraso
- [x] ABT v6 v2: 614 colunas, todos gates PASSOU
- [x] Book de Variáveis documentado

### Modelagem - PRÓXIMOS PASSOS
- [ ] Seleção de features (reduzir 614 → top features)
- [ ] Split Train/Test/OOT por SAFRA
- [ ] Modelo baseline (Regressão Logística)
- [ ] Modelo XGBoost/LightGBM
- [ ] Avaliação KS por versão da ABT (lift incremental)
- [ ] Interpretação do modelo (SHAP)

---

## Split Train/Test/OOT (Recomendado)

```python
# Filtrar apenas clientes aprovados (onde FPD é observado)
df_train_eligible = df_abt.filter(F.col("flag_instalacao_int") == 1)

# Split temporal por SAFRA
df_train = df_train_eligible.filter(F.col("safra") < "202402")  # Até Jan 2024
df_test = df_train_eligible.filter(F.col("safra") == "202402")  # Fev 2024
df_oot = df_train_eligible.filter(F.col("safra") == "202403")   # Mar 2024 (OOT)

# Target
target = "fpd_int"

# Excluir das features
exclude_cols = ["num_cpf", "safra", "dt_safra", "fpd_int", "flag_instalacao_int",
                "prod", "flag_mig2", "abt_version", "build_date", "spine_version",
                "gold_version", "gold_build_date"]
```

---

## Top Features para Modelagem (Recomendadas)

| Rank | Feature | Bloco | Relevância |
|------|---------|-------|------------|
| 1 | `score_01` | Score | Score de bureau baseline |
| 2 | `freq_sos_m1` | Recarga | Estresse financeiro |
| 3 | `pct_aging_90_plus_m1` | Atraso | Inadimplência grave |
| 4 | `pct_pagamentos_com_juros_m1` | Pagamento | Comportamento de atraso |
| 5 | `flag_risco_alto_m1` | Atraso | WO/PDD/Fraude |
| 6 | `coef_variacao_val_m1` | Recarga | Instabilidade de valores |
| 7 | `sum_val_aberto_m1` | Atraso | Saldo em aberto |
| 8 | `ratio_juros_pago_m1` | Pagamento | Intensidade de juros |
| 9 | `dias_max_entre_recargas_m1` | Recarga | Inatividade |
| 10 | `ticket_medio_m1` | Recarga | Capacidade de pagamento |

---

## Regra de Negócio do SOS

> **SOS é um empréstimo/adiantamento (R$3-20, tipicamente R$5) descontado da próxima recarga. SOS e bônus NÃO contam como "dinheiro real". Alta frequência de SOS = indicador de estresse financeiro.**

---

## Licença

[Ver LICENSE](LICENSE)

---

**Última Atualização:** 30 Jan 2026 | **Versão:** 3.0 | **Status:** Engenharia de Dados COMPLETO | **Próximo:** Modelagem
