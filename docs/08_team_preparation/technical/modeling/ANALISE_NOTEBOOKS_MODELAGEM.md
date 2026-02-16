# Análise dos Notebooks de Modelagem

> **Data da Análise:** 02/02/2026
> **Pasta Analisada:** `src/jobs/04_modeling/`
> **Objetivo:** Documentar a evolução dos modelos de regressão logística para predição de FPD (First Payment Default)

---

## Resumo Executivo

O time de Data Science desenvolveu uma série de modelos incrementais utilizando **Regressão Logística com Statsmodels**. A abordagem seguiu a metodologia de adição incremental de features para demonstrar o ganho marginal de cada bloco de variáveis.

### Resultado Final

| Métrica | Valor | Status |
|---------|-------|--------|
| **KS OOT** | 30.73% | Abaixo do benchmark |
| **AUC OOT** | 0.7104 | - |
| **GINI OOT** | 42.08% | - |
| **Benchmark** | 33.1% | Não atingido |
| **Diferença** | -2.37 p.p. | - |
| **Features Finais** | 26 | 100% significantes |

### Principais Conclusões

1. **Score_02 é a variável dominante** (~65-76% de importância)
2. **Features de Recarga agregaram valor significativo** (+1.94 p.p. no KS)
3. **Variáveis Telco numéricas não agregaram valor** (descartadas)
4. **Modelo é estável** (diferença Train-OOT < 2 p.p.)
5. **Benchmark não atingido** - faltam 2.37 p.p. para 33.1%

---

## Notebooks Analisados

| # | Notebook | Data | Descrição |
|---|----------|------|-----------|
| 1 | `20260125 - ABT V1.ipynb` | 25/01/2026 | Modelo baseline com Score_01 categorizado |
| 2 | `20260125 - ABT V2.ipynb` | 25/01/2026 | Adição de Score_02 categorizado |
| 3 | `20260125 - ABT V3.ipynb` | 25/01/2026 | Teste com variáveis Telco numéricas |
| 4 | `20260125 - ABT V4.ipynb` | 25/01/2026 | Adição de Idade categorizada |
| 5 | `20260129 - ABT V5.ipynb` | 29/01/2026 | Adição de features de Recarga |
| 6 | `20260131 - ABT V6.ipynb` | 31/01/2026 | Adição de Pagamento e Atraso |
| 7 | `20260131 - Modelo Final.ipynb` | 31/01/2026 | Modelo final consolidado |
| 8 | `20260131 - Modelo Final - Teste CATE.ipynb` | 31/01/2026 | Comparação categórico vs contínuo |

---

## Evolução Incremental do KS

### Gráfico de Evolução

```
KS OOT (%)
   |
33 +--------------------.........BENCHMARK.........
   |
32 +
   |
31 +                           ●─────●
   |                          /  V5   Final
30 +                         /
   |                        /
29 +            ●──●───●   /
   |           / V2 V3 V4 /
28 +          /
   |         /
27 +        /
   |       /
26 +      /
   |     /
25 +    /
   |   /
24 +  ●
   |  V1
23 +──┼────┼────┼────┼────┼────┼────┼──
      V1   V2   V3   V4   V5   V6  Final
```

### Tabela de Evolução

| Versão | Features | KS OOT | AUC OOT | Ganho KS | Status |
|--------|----------|--------|---------|----------|--------|
| **V1** (Score_01) | 7 | 24.00% | 0.6640 | - | Baseline |
| **V2** (+Score_02) | 16 | 28.75% | 0.6974 | +4.75 p.p. | Melhorou |
| **V3** (+Telco) | 18 | 28.76% | 0.6978 | +0.01 p.p. | Descartado |
| **V4** (+Idade) | 19 | 28.93% | 0.6981 | +0.17 p.p. | Marginal |
| **V5** (+Recarga) | 25 | 30.87% | 0.7109 | +1.94 p.p. | Melhorou |
| **Final** | 26 | 30.73% | 0.7104 | - | Melhor modelo |

---

## Análise Detalhada por Versão

### 1. ABT V1 - Modelo Baseline (Score_01)

**Objetivo:** Estabelecer baseline com score de bureau categorizado

**Metodologia:**
- Score_01 categorizado em 8 faixas (referência: categoria 8 = pior score)
- Regressão Logística com Statsmodels
- Train: safras 202410-202501 | OOT: safras 202502-202503

**Categorização Score_01:**
| Categoria | Faixa Score | Taxa FPD |
|-----------|-------------|----------|
| 1 | > 675 | 5.22% |
| 2 | 651-675 | 8.52% |
| 3 | 626-650 | 12.06% |
| 4 | 601-625 | 16.48% |
| 5 | 576-600 | 22.63% |
| 6 | 551-575 | 29.42% |
| 7 | 501-550 | 36.60% |
| 8 | ≤500 ou NULL | 40.28% |

**Resultados:**
```
TREINO:
  Volume: 879,991 registros
  Taxa FPD: 23.80%
  KS: 24.47%
  AUC: 0.6617

OOT:
  Volume: 410,535 registros
  Taxa FPD: 23.07%
  KS: 24.00%
  AUC: 0.6640

ESTABILIDADE: Diff KS = 0.47 p.p. (muito estável)
```

**Coeficientes (todos significantes p < 0.001):**
- Categoria 1: -2.50 (OR = 0.08)
- Categoria 2: -1.98 (OR = 0.14)
- Categoria 3: -1.59 (OR = 0.20)
- Categoria 4: -1.23 (OR = 0.29)
- Categoria 5: -0.84 (OR = 0.43)
- Categoria 6: -0.48 (OR = 0.62)
- Categoria 7: -0.16 (OR = 0.86)

---

### 2. ABT V2 - Adição de Score_02

**Objetivo:** Avaliar ganho incremental do Score_02 de bureau

**Metodologia:**
- Score_02 categorizado em 10 faixas (referência: categoria 10)
- Correlação entre scores: 0.66 (moderada - bom equilíbrio)

**Categorização Score_02:**
| Categoria | Faixa Score | Taxa FPD |
|-----------|-------------|----------|
| 1 | > 800 | 4.13% |
| 2 | 751-800 | 6.77% |
| 3 | 701-750 | 10.51% |
| 4 | 651-700 | 15.47% |
| 5 | 626-650 | 19.72% |
| 6 | 576-625 | 25.10% |
| 7 | 551-575 | 31.44% |
| 8 | 531-550 | 35.09% |
| 9 | 491-530 | 40.79% |
| 10 | ≤490 ou NULL | 52.12% |

**Resultados:**
```
TREINO:
  Volume: 879,991 registros
  KS: 30.17%
  AUC: 0.7068

OOT:
  Volume: 410,535 registros
  KS: 28.75%
  AUC: 0.6974

GANHO vs V1: +4.75 p.p. no KS
ESTABILIDADE: Diff KS = 1.42 p.p. (estável)
```

**Importância das Variáveis:**
| Score | Categorias | Importância |
|-------|------------|-------------|
| Score_01 | 7 | 24.0% |
| Score_02 | 9 | **76.0%** |

**Conclusão:** Score_02 agrega valor significativo e é a variável dominante.

---

### 3. ABT V3 - Teste com Variáveis Telco

**Objetivo:** Avaliar ganho de variáveis Telco numéricas (var_26_adj, var_73_adj)

**Resultados:**
```
OOT:
  KS: 28.76%
  AUC: 0.6978

GANHO vs V2: +0.01 p.p. (irrelevante)
```

**Importância das Variáveis:**
| Variável | Importância |
|----------|-------------|
| Score_01 | 25.5% |
| Score_02 | 74.5% |
| var_26_adj | 0.0019% |
| var_73_adj | 0.0003% |

**Recomendação:** **DESCARTAR variáveis numéricas Telco** - ganho < 0.5 p.p. e importância < 0.1%

---

### 4. ABT V4 - Adição de Idade

**Objetivo:** Avaliar ganho da variável demográfica Idade categorizada

**Metodologia:**
- Testadas 2 versões: 5 categorias e 3 categorias
- Versão com 3 categorias selecionada (mais parcimoniosa)

**Resultados (Modelo 4F - Idade 3 cats):**
```
OOT:
  KS: 28.93%
  AUC: 0.6981

GANHO vs V2: +0.18 p.p. (marginal, mas mantido)
```

**Importância das Variáveis:**
| Variável | Importância |
|----------|-------------|
| Score_01 | 24.3% |
| Score_02 | 74.2% |
| Idade | 1.4% |

**Conclusão:** Idade contribui marginalmente mas é mantida por interpretabilidade.

---

### 5. ABT V5 - Adição de Features de Recarga

**Objetivo:** Avaliar ganho das features comportamentais de recarga

**Features de Recarga Incluídas:**
| Variável | Janela | Categorias |
|----------|--------|------------|
| sum_val_credito | M3 | 2 |
| qtd_recargas | M3 | 1 |
| dias_medio | M1 | 2 |
| flag_teve_sos | M6 | 1 (flag) |
| flag_sem_recarga | M6 | 1 (flag) |

**Resultados:**
```
TREINO:
  Volume: 879,991 registros
  KS: 32.72%
  AUC: 0.7231

OOT:
  Volume: 410,535 registros
  KS: 30.87%
  AUC: 0.7109

GANHO vs V4: +1.94 p.p. (significativo!)
ESTABILIDADE: Diff KS = 1.85 p.p. (muito estável)
```

**Importância das Variáveis:**
| Variável | Importância |
|----------|-------------|
| Score_02 | 65.5% |
| Score_01 | 20.5% |
| sum_val_credito_m3 | 6.1% |
| Outras recarga | ~8% |

**Conclusão:** Features de recarga agregaram **valor significativo** (+1.94 p.p.)

---

### 6. ABT V6 - Adição de Pagamento e Atraso

**Objetivo:** Avaliar ganho das features de pagamento e atraso

**Features Testadas:**
- `flag_teve_fraude_mes_atr_m1` (alto impacto)
- `qtd_pagamentos_validos_mes_pag_m3`
- `qtd_transacoes_mes_pag_m3`
- `qtd_contratos_distintos_mes_pag_m3`

**Resultados:**
```
Calibração do Modelo:
  MAE (Erro Absoluto Médio): 0.66%
  MAPE (Erro Percentual Médio): 5.0%
  → Modelo BEM CALIBRADO

Spread por Decil:
  Decil 1 (Pior): 50.04% real vs 51.17% previsto
  Decil 10 (Melhor): 5.06% real vs 4.46% previsto
```

**Top Coeficientes Positivos (Aumentam FPD):**
| Feature | Coeficiente | Odds Ratio |
|---------|-------------|------------|
| flag_teve_fraude_mes_atr_m1 | 0.93 | 2.53 |
| flag_teve_sos_m6 | 0.38 | 1.46 |
| qtd_recargas_m3_cate_1 | 0.25 | 1.28 |
| IDADE_cate_v1_1 | 0.19 | 1.21 |

---

### 7. Modelo Final

**Objetivo:** Consolidar modelo final com melhor performance

**Modelos Testados:**
| Modelo | Features | KS OOT | AUC OOT |
|--------|----------|--------|---------|
| Sem Região | 26 | 30.73% | 0.7104 |
| Com Região | 29 | 30.74% | 0.7104 |

**Recomendação Final:** **Modelo SEM Região** (simplicidade)

**Resultados Finais:**
```
MODELO FINAL:
  Features: 26
  KS Train: 33.05%
  KS Valid: 31.59%
  KS OOT: 30.73%
  AUC OOT: 0.7104
  GINI OOT: 42.08%
  Estabilidade: 2.32 p.p. (aceitável)

BENCHMARK: NÃO ATINGIDO (-2.37 p.p.)
```

---

### 8. Teste Categórico vs Contínuo

**Objetivo:** Comparar performance de variáveis categorizadas vs contínuas

**Resultados:**
| Modelo | Features | KS OOT | AUC OOT |
|--------|----------|--------|---------|
| **Categórico** | 8 | **30.72%** | **0.7102** |
| Contínuo | 8 | 30.09% | 0.7057 |

**Conclusão:** **Modelo categórico é superior** (+0.63 p.p. no KS)

---

## Metodologia de Modelagem

### Configuração de Dados

```
FILTROS APLICADOS:
  - PROD = 'CMV'
  - flag_mig2 = 'PRE'
  - flag_instalacao_int = 1 (apenas aprovados)

SPLIT TEMPORAL:
  - Treino: safras 202410, 202411, 202412, 202501
  - OOT: safras 202502, 202503

VOLUMES:
  - Treino: 879,991 registros
  - OOT: 410,535 registros
```

### Técnica de Modelagem

- **Algoritmo:** Regressão Logística (Statsmodels)
- **Método de otimização:** BFGS
- **Categorização:** WoE-based binning com monotonicidade
- **Referência:** Pior categoria de cada score

### Métricas Utilizadas

| Métrica | Descrição |
|---------|-----------|
| **KS** | Kolmogorov-Smirnov - separação máxima entre bons e maus |
| **AUC** | Área sob a curva ROC |
| **GINI** | 2*AUC - 1 |
| **Pseudo R²** | McFadden R-squared |
| **AIC/BIC** | Critérios de informação para comparação de modelos |

---

## Importância das Variáveis (Modelo Final)

### Composição por Bloco

```
           Scores (86%)
    ┌──────────────────────────────┐
    │  Score_02: 65.5%             │
    │  Score_01: 20.5%             │
    └──────────────────────────────┘

           Recarga (8%)
    ┌──────────────────────────────┐
    │  sum_val_credito_m3: 6.1%    │
    │  qtd_recargas: 1.2%          │
    │  dias_medio: 0.7%            │
    └──────────────────────────────┘

        Demográfica (2%)
    ┌──────────────────────────────┐
    │  Idade: ~2%                  │
    └──────────────────────────────┘

      Pagamento/Atraso (4%)
    ┌──────────────────────────────┐
    │  flag_fraude: 2%             │
    │  qtd_pagamentos: 1%          │
    │  Outros: 1%                  │
    └──────────────────────────────┘
```

---

## Recomendações e Próximos Passos

### Para Atingir o Benchmark (33.1%)

1. **Explorar Interações:** Criar features de interação entre Score_01 e Score_02
2. **Testar XGBoost/LightGBM:** Algoritmos de gradient boosting podem capturar não-linearidades
3. **Feature Engineering Adicional:**
   - Ratios entre variáveis de recarga
   - Features de tendência temporal
   - Variáveis de comportamento SOS mais granulares
4. **Análise de Subpopulações:** Verificar se há segmentos com KS superior

### Pontos de Atenção

1. **Overfitting:** Diferença Train-OOT de 2.32 p.p. deve ser monitorada
2. **Calibração:** Modelo tende a subestimar ligeiramente (verificar em produção)
3. **Variáveis Descartadas:** Telco numéricas não agregaram valor - investigar por quê

### Documentação Adicional Sugerida

- [ ] Análise SHAP para interpretabilidade
- [ ] Curvas de swap-in/swap-out
- [ ] Análise de estabilidade por safra
- [ ] Matriz de confusão com diferentes thresholds

---

## Anexos

### A. Funções Auxiliares Utilizadas

Os notebooks implementam funções reutilizáveis para:
- `categorizar()`: Cálculo de WoE, IV e métricas por categoria
- `calcular_ks()`: Cálculo do KS com curvas acumuladas
- `plot_curva_ks()`: Visualização da curva KS
- `salvar_resultados_modelo()`: Tracking de experimentos em CSV

### B. Caminhos dos Dados

```
ABT V1: /Volumes/workspace/hackathon_2025/default/source/abt_v1_delta/
ABT V2: /Volumes/workspace/hackathon_2025/default/source/abt_v2_delta/
ABT V6: /Volumes/workspace/hackathon_2025/default/source/abt_v6_v2_delta/
```

### C. Histórico de Resultados

Os resultados são salvos em:
```
/Workspace/Users/eduardoandrechuk@outlook.com/resultados/comparacao_modelos.csv
```

---

**Documento gerado automaticamente pela análise dos notebooks de modelagem.**
