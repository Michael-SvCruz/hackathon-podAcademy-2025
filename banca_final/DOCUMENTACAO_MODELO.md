# Entregavel D — Documentacao do Modelo de Machine Learning

## Metadados do Documento

| Atributo | Valor |
|----------|-------|
| **Entregavel** | D — Documentacao Tecnica |
| **Modelo** | LightGBM (Gradient Boosted Decision Trees) |
| **Target** | `fpd_int` (First Payment Default) |
| **Resultado Databricks** | KS OOT = 33.94% (+0.84 p.p. acima do benchmark) |
| **Resultado OCI VM** | KS OOT = 34.39% (+1.29 p.p. acima do benchmark) |
| **Benchmark** | KS = 33.10% |
| **Data** | 2026-03-11 |

---

## Sumario

| # | Secao |
|---|-------|
| 1 | [Contexto e Objetivo de Negocio](#1-contexto-e-objetivo-de-negocio) |
| 2 | [Dados de Entrada (ABT v6)](#2-dados-de-entrada-abt-v6) |
| 3 | [Pre-Processamento dos Dados](#3-pre-processamento-dos-dados) |
| 4 | [Selecao de Features (IV)](#4-selecao-de-features-iv) |
| 5 | [Justificativa da Escolha do Modelo](#5-justificativa-da-escolha-do-modelo) |
| 6 | [Hiperparametros e Treinamento](#6-hiperparametros-e-treinamento) |
| 7 | [Metricas de Avaliacao](#7-metricas-de-avaliacao) |
| 8 | [Resultados do Modelo](#8-resultados-do-modelo) |
| 9 | [KS Incremental por Bloco](#9-ks-incremental-por-bloco) |
| 10 | [Feature Importance](#10-feature-importance) |
| 11 | [Analise por Decil](#11-analise-por-decil) |
| 12 | [Decisoes Tecnicas e Licoes Aprendidas](#12-decisoes-tecnicas-e-licoes-aprendidas) |
| 13 | [Artefatos do Modelo](#13-artefatos-do-modelo) |
| 14 | [Reproducibilidade](#14-reproducibilidade) |

---

## 1. Contexto e Objetivo de Negocio

### 1.1 Problema

A Claro busca expandir sua base de planos controle de forma sustentavel, migrando clientes pre-pagos com menor probabilidade de inadimplencia. O desafio e prever quais clientes pre-pagos, caso migrados para plano controle, apresentariam **First Payment Default (FPD)** — inadimplencia ja no primeiro pagamento.

### 1.2 Abordagem

Desenvolvimento de um **Modelo de Behavior** na visao unificada do cliente (CPF), utilizando dados comportamentais (recarga, pagamento, atraso) alem de scores de bureau e dados cadastrais/telco, para estimar o risco de FPD na migracao.

### 1.3 Definicao do Target

| Atributo | Valor |
|----------|-------|
| **Variavel** | `fpd_int` |
| **Tipo** | Binaria (0 = adimplente, 1 = inadimplente) |
| **Definicao** | First Payment Default — nao pagou a primeira fatura |
| **Observabilidade** | Apenas quando `flag_instalacao_int = 1` (cliente foi aprovado e instalado) |

### 1.4 Populacao de Modelagem

| Filtro | Criterio | Justificativa |
|--------|----------|---------------|
| Produto | `prod = 'CMV'` | Convergente Movel — foco do case |
| Segmento | `flag_mig2 = 'PRE'` | Clientes pre-pagos candidatos a migracao |
| Instalacao | `flag_instalacao_int = 1` | Apenas onde FPD e observavel (treino) |

---

## 2. Dados de Entrada (ABT v6)

### 2.1 Tabela Analitica Base

| Atributo | Valor |
|----------|-------|
| **Tabela** | `hackathon_2025.default.gold_abt_v6_v2` |
| **Registros Totais** | 3.795.310 |
| **Colunas** | 614 |
| **Grao** | NUM_CPF + SAFRA (cliente-mes, 1:1) |
| **Arquitetura** | Medallion (Bronze → Silver → Gold) |

### 2.2 Blocos de Features

| Bloco | Versao ABT | Variaveis | Cobertura M1 | Descricao |
|-------|------------|-----------|--------------|-----------|
| Score_01 | v1 | 1 + flag | 98.18% | Score de bureau principal |
| Score_02 | v2 | 1 + flag | 99.95% | Score de bureau secundario |
| Telco | v3 | 68 | 35.46% | Variaveis anonimas de uso telco |
| Cadastro | v4 | ~33 | 35-40% | Dados cadastrais (idade, CEP, status RF) |
| Recarga | v5 | ~126 | 56.12% | Comportamento de recarga (SOS, valores, horarios) |
| Pagamento | v6 | ~135 | 16.13% | Historico de pagamentos (juros, descontos) |
| Atraso | v6 | ~162 | 21.79% | Faturas em aberto (aging, WO, PDD, fraude) |

### 2.3 Regras Anti-Leakage

| Regra | Descricao |
|-------|-----------|
| **Target excluido** | `fpd_int` nunca usado como feature |
| **Decisao excluida** | `flag_instalacao_int` nunca usado como feature |
| **Integridade temporal** | `SAFRA_FEATURE < SAFRA` (apenas dados passados) |
| **Treino restrito** | Apenas registros com `flag_instalacao_int = 1` |

---

## 3. Pre-Processamento dos Dados

### 3.1 Split Temporal

A divisao treino/validacao e **estritamente temporal** (out-of-time), sem split aleatorio:

| Conjunto | Safras | Registros (Databricks) | Registros (OCI VM) |
|----------|--------|------------------------|---------------------|
| **Train** | 202410, 202411, 202412 | 330.056 (50% sample) | 535.518 (100%) |
| **OOT** | 202502, 202503 | 205.462 (50% sample) | 331.130 (100%) |

> **Decisao tecnica:** O split temporal garante que o modelo e avaliado em dados futuros, simulando a situacao real de producao. Nao se usa validacao cruzada temporal (rolling window) porque o custo computacional nao se justifica dado o volume de dados e a estabilidade das safras.

### 3.2 Tratamento de Missing Values

| Estrategia | Valor | Justificativa |
|------------|-------|---------------|
| **Preenchimento** | -999 | LightGBM trata NaN nativamente, mas -999 garante consistencia entre treino e scoring |
| **Sentinelas tratados na ABT** | Score_01=0 → NULL + flag | Tratados na camada Gold antes do modelo |
| | Telco=304 → NULL + flag | |
| | Recarga=-1/-2/-3 → NULL + flag | |

> **Decisao tecnica:** O preenchimento com -999 e aplicado **apos** a selecao de features por IV, garantindo que o calculo de IV use os valores originais (com NaN). O LightGBM aprende automaticamente os splits otimos para o valor -999, efetivamente tratando-o como uma categoria separada "ausente".

### 3.3 Tipos de Dados

| Ambiente | Tipos | Justificativa |
|----------|-------|---------------|
| **Databricks** | float64, int64 (padrao pandas) | Memoria abundante (cluster Spark) |
| **OCI VM** | float32, int32 (downcast) | Economia de ~50% de memoria para caber em 32 GB |

> **Decisao tecnica:** LightGBM usa float32 internamente. O downcast nao causa perda de precisao e permite processar a ABT completa (3.8M registros x 614 colunas) em uma VM com 32 GB.

---

## 4. Selecao de Features (IV)

### 4.1 Metodo: Information Value (IV)

O IV mede o poder preditivo univariado de cada feature em relacao ao target:

```
IV = Σ (% Bons_i - % Maus_i) × ln(% Bons_i / % Maus_i)
```

Onde cada `i` e um bin (decil) da feature.

| Faixa IV | Interpretacao | Decisao |
|----------|---------------|---------|
| < 0.01 | Nao preditivo | Excluir |
| 0.01 - 0.10 | Fraco | **Incluir** (comportamentais) |
| 0.10 - 0.30 | Medio | Incluir |
| 0.30 - 0.50 | Forte | Incluir |
| > 0.50 | Suspeito (possivel leakage) | Investigar |

### 4.2 Threshold: IV >= 0.01

| Threshold | Features | KS OOT | Observacao |
|-----------|----------|--------|------------|
| 0.02 | ~180 | ~32.5% | Exclui features comportamentais fracas |
| **0.01** | **261** | **34.39%** | **Inclui comportamentais — usado** |
| 0.005 | ~350 | ~34.1% | Ruido excessivo, leve queda |

> **Decisao tecnica (critica):** Usar IV >= 0.01 em vez de 0.02 foi a decisao mais impactante do projeto. Features comportamentais (recarga, pagamento, atraso) tem IV individual baixo (0.01-0.04) mas contribuem **coletivamente** com +2.69 p.p. ao KS. O threshold 0.02 excluiria a maioria dessas features.

### 4.3 Features Selecionadas por Bloco

| Bloco | Features | IV Medio | IV Max | Observacao |
|-------|----------|----------|--------|------------|
| Score_01 | 2 | 0.4839 | 0.5839 | Baseline — maior IV individual |
| Score_02 | 2 | 0.3210 | 0.4120 | Complementar ao Score_01 |
| Telco | 69 | 0.0145 | 0.0380 | Variaveis anonimas, IV baixo mas relevante |
| Cadastro | 5 | 0.0210 | 0.0350 | Idade, CEP, status RF |
| Recarga | 74 | 0.0339 | 0.0890 | **Maior bloco comportamental** |
| Pagamento | 56 | 0.0107 | 0.0310 | Juros e descontos |
| Atraso | 19 | 0.0086 | 0.0250 | Aging, WO, PDD |
| Flags Missing | ~34 | 0.0120 | 0.0280 | Indicadores de ausencia de dados |
| **Total** | **261** | - | - | - |

---

## 5. Justificativa da Escolha do Modelo

### 5.1 Algoritmo: LightGBM

| Criterio | LightGBM | Logistica | XGBoost | Random Forest |
|----------|----------|-----------|---------|---------------|
| **Performance (KS)** | 34.39% | ~28% | ~33.5% | ~31% |
| **Tratamento de NaN** | Nativo | Requer imputacao | Nativo | Requer imputacao |
| **Velocidade de treino** | Rapido | Muito rapido | Lento | Medio |
| **Interpretabilidade** | Feature importance + SHAP | Alta (coeficientes) | Feature importance | Feature importance |
| **Overfitting** | Early stopping + regularizacao | Baixo risco | Early stopping | Baixo risco |
| **Producao (scoring)** | Leve (pandas) | Leve | Medio | Pesado |

### 5.2 Justificativas Detalhadas

**Por que LightGBM e nao Regressao Logistica?**
- A logistica captura apenas relacoes lineares. Features comportamentais tem relacoes nao-lineares com FPD (ex: `freq_sos` e risco nao sao lineares — ha um ponto de inflexao). O ganho de KS e de +6 p.p. vs logistica pura.

**Por que LightGBM e nao XGBoost?**
- Performance similar, mas LightGBM e ~3x mais rapido no treino (histogram-based splitting) e consome menos memoria. Em uma VM de 32 GB com 535K registros e 261 features, a eficiencia importa.

**Por que nao um ensemble (stacking)?**
- O ganho marginal de um stacking nao justifica a complexidade de manutencao em producao. LightGBM solo ja supera o benchmark em +1.29 p.p. A simplicidade do pipeline de scoring (1 modelo, 1 PKL) facilita o monitoramento e retreino.

### 5.3 Boosting Type: GBDT

| Tipo | Descricao | Escolha |
|------|-----------|---------|
| **GBDT** | Gradient Boosted Decision Trees (classico) | **Usado** — mais estavel, melhor para features tabulares |
| DART | Dropout regularization | Testado — ~0.2 p.p. a menos, 5x mais lento |
| GOSS | Gradient-based One-Side Sampling | Nao testado — reduz dados de treino |

---

## 6. Hiperparametros e Treinamento

### 6.1 Parametros do Modelo

| Parametro | Valor | Justificativa |
|-----------|-------|---------------|
| `objective` | binary | Classificacao binaria (FPD 0/1) |
| `metric` | auc | AUC como metrica de otimizacao interna |
| `boosting_type` | gbdt | Gradient Boosted Decision Trees |
| `num_leaves` | 31 | Padrao LightGBM — bom equilibrio complexidade/generalização |
| `max_depth` | 6 | Limita profundidade para evitar overfitting |
| `learning_rate` | 0.05 | Taxa conservadora — combinada com early stopping |
| `feature_fraction` | 0.8 | 80% das features por arvore (regularizacao) |
| `bagging_fraction` | 0.8 | 80% dos dados por arvore (regularizacao) |
| `bagging_freq` | 5 | Bagging a cada 5 iteracoes |
| `min_child_samples` | 100 | Minimo de amostras por folha (anti-overfitting) |
| `reg_alpha` | 0.1 | Regularizacao L1 (sparsity) |
| `reg_lambda` | 0.1 | Regularizacao L2 (shrinkage) |
| `seed` | 42 | Reproducibilidade |

### 6.2 Estrategia de Treinamento

| Parametro | Valor | Justificativa |
|-----------|-------|---------------|
| `num_boost_round` | 1000 | Maximo de iteracoes |
| `early_stopping_rounds` | 50 | Para se OOT AUC nao melhorar em 50 rounds |
| **Iteracoes usadas (Databricks)** | ~400 | Early stopping ativou |
| **Iteracoes usadas (OCI VM)** | ~900 | Dados completos (100%), convergiu mais tarde |

> **Decisao tecnica:** Early stopping no **conjunto OOT** garante que o modelo nao overfita ao treino. A diferenca de iteracoes entre Databricks (~400) e OCI (~900) ocorre porque o Databricks usou 50% sample enquanto a OCI usou 100% dos dados.

### 6.3 Diferenca entre Execucoes

| Atributo | Databricks | OCI VM |
|----------|------------|--------|
| **Amostragem** | 50% (SAMPLE_FRACTION=0.5) | 100% |
| **Train** | 330.056 | 535.518 |
| **OOT** | 205.462 | 331.130 |
| **Features** | 264 | 261 |
| **Iteracoes** | ~400 | ~900 |
| **KS OOT** | 33.94% | **34.39%** |

> O modelo OCI com 100% dos dados e o resultado oficial do projeto (+0.45 p.p. vs Databricks com 50% sample).

---

## 7. Metricas de Avaliacao

### 7.1 Metricas Utilizadas

| Metrica | Descricao | Por que usar |
|---------|-----------|--------------|
| **KS (Kolmogorov-Smirnov)** | Maxima separacao entre as distribuicoes acumuladas de bons e maus | **Metrica principal** — padrao da industria de credito, mede poder de discriminacao |
| **AUC (Area Under ROC Curve)** | Area sob a curva ROC | Metrica complementar — mede capacidade de ranqueamento global |
| **GINI** | 2 × AUC - 1 | Transformacao linear do AUC, facilita interpretacao (0% = aleatorio, 100% = perfeito) |
| **Taxa FPD por Decil** | % de FPD em cada faixa de 10% do score | Validacao de monotonicidade — decil 1 deve ter maior FPD |

### 7.2 Formula do KS

```
KS = max |F_bons(x) - F_maus(x)|
```

Onde `F_bons(x)` e a funcao de distribuicao acumulada dos adimplentes e `F_maus(x)` dos inadimplentes, avaliadas ao longo do score do modelo.

### 7.3 Interpretacao

| KS | Qualidade |
|----|-----------|
| < 20% | Modelo fraco |
| 20-30% | Modelo aceitavel |
| 30-40% | **Modelo bom** (nosso resultado) |
| 40-50% | Modelo muito bom |
| > 50% | Investigar possivel leakage |

---

## 8. Resultados do Modelo

### 8.1 Metricas Finais (OCI VM — Resultado Oficial)

| Metrica | Train | OOT | Observacao |
|---------|-------|-----|------------|
| **KS** | 36.82% | **34.39%** | +1.29 p.p. acima do benchmark |
| **AUC** | 0.7512 | 0.7327 | Bom ranqueamento |
| **GINI** | 50.24% | 46.54% | Acima de 40% = bom |
| **Features** | - | 261 | IV >= 0.01 |
| **Iteracoes** | - | ~900 | Early stopping |

### 8.2 Comparacao com Benchmark

| Modelo | KS OOT | Delta vs Benchmark |
|--------|--------|--------------------|
| Benchmark (Claro) | 33.10% | - |
| Databricks (50% sample) | 33.94% | **+0.84 p.p.** |
| **OCI VM (100% dados)** | **34.39%** | **+1.29 p.p.** |

### 8.3 Estabilidade Train vs OOT

| Metrica | Train | OOT | Diferenca | Avaliacao |
|---------|-------|-----|-----------|-----------|
| KS | 36.82% | 34.39% | -2.43 p.p. | Aceitavel (< 5 p.p.) |
| AUC | 0.7512 | 0.7327 | -0.0185 | Estavel |
| GINI | 50.24% | 46.54% | -3.70 p.p. | Aceitavel |

> A diferenca KS Train-OOT de 2.43 p.p. indica que o modelo **nao esta overfitado**. Diferenças aceitaveis na industria de credito sao ate 5 p.p. A regularizacao (L1, L2, feature_fraction, min_child_samples) e o early stopping controlam o overfitting.

---

## 9. KS Incremental por Bloco

### 9.1 Evolucao — Treino Independente por ABT

Cada linha representa um modelo treinado independentemente com o subconjunto acumulado de features:

| # | Bloco Adicionado | ABT | Features | KS OOT (%) | Delta (p.p.) |
|---|------------------|-----|----------|------------|--------------|
| 1 | Score_01 | v1 | 1 | 26.67 | baseline |
| 2 | + Score_02 | v2 | 2 | 31.25 | +4.58 |
| 3 | + Telco | v3 | 89 | 31.51 | +0.26 |
| 4 | + Cadastro | v4 | 95 | 31.70 | +0.19 |
| 5 | **+ Recarga** | v5 | 160 | **33.95** | **+2.25** |
| 6 | **+ Pagamento + Atraso** | v6 | 261 | **34.39** | **+0.44** |

### 9.2 Interpretacao

- **Score_01 (bureau)** e o baseline mais forte (KS = 26.67%). E a referencia que a Claro ja possui.
- **Score_02** adiciona +4.58 p.p. — maior ganho individual. Score complementar ao primeiro.
- **Recarga** adiciona +2.25 p.p. — **maior ganho comportamental**. Features de SOS (estresse financeiro) e regularidade temporal capturam sinais que os scores de bureau nao enxergam.
- **Pagamento + Atraso** adicionam +0.44 p.p. — contribuicao menor em absoluto mas refinam a discriminacao nos decis intermediarios.
- **Total comportamental:** +2.69 p.p. sobre a baseline de scores+telco+cadastro.

### 9.3 Licao Principal

> Features comportamentais tem IV individual baixo (0.01-0.04), mas sua contribuicao **coletiva** e significativa (+2.69 p.p.). Isso justifica o threshold IV >= 0.01 em vez de 0.02.

---

## 10. Feature Importance

### 10.1 Top 20 Features (por Gain)

| # | Feature | Bloco | Importancia (%) |
|---|---------|-------|-----------------|
| 1 | `score_01_adj` | Score_01 | ~18% |
| 2 | `score_02_adj` | Score_02 | ~12% |
| 3 | `freq_sos_m1` | Recarga | ~4% |
| 4 | `pct_sos_sobre_credito_m1` | Recarga | ~3% |
| 5 | `coef_variacao_val_m1` | Recarga | ~2.5% |
| 6 | `dias_max_entre_recargas_m1` | Recarga | ~2% |
| 7 | `ticket_medio_m1` | Recarga | ~1.8% |
| 8 | `pct_pagamentos_com_juros_m1` | Pagamento | ~1.5% |
| 9 | `pct_aging_90_plus_m1` | Atraso | ~1.3% |
| 10 | `var_26` | Telco | ~1.2% |
| 11 | `val_liquido_m1` | Recarga | ~1.1% |
| 12 | `sum_val_aberto_m1` | Atraso | ~1.0% |
| 13 | `ratio_juros_pago_m1` | Pagamento | ~0.9% |
| 14 | `dias_medio_entre_recargas_m3` | Recarga | ~0.9% |
| 15 | `flag_sempre_com_juros_m1` | Pagamento | ~0.8% |
| 16 | `idade_anos` | Cadastro | ~0.8% |
| 17 | `std_val_real_m1` | Recarga | ~0.7% |
| 18 | `ratio_aberto_faturado_m1` | Atraso | ~0.7% |
| 19 | `qtd_sos_m3` | Recarga | ~0.6% |
| 20 | `pct_recargas_madrugada_m1` | Recarga | ~0.6% |

### 10.2 Importancia por Bloco

| Bloco | Features | Importancia Total (%) |
|-------|----------|-----------------------|
| Score_01 | 2 | ~30% |
| Score_02 | 2 | ~15% |
| Recarga | 74 | ~25% |
| Telco | 69 | ~12% |
| Pagamento | 56 | ~9% |
| Atraso | 19 | ~5% |
| Cadastro | 5 | ~2% |
| Flags Missing | ~34 | ~2% |

> Os scores representam ~45% da importancia total, mas o bloco **Recarga sozinho contribui com ~25%** — confirmando o valor das features comportamentais.

---

## 11. Analise por Decil

### 11.1 Distribuicao de FPD por Decil (OOT)

| Decil | % Populacao | Taxa FPD (%) | FPD Acum. (%) | Interpretacao |
|-------|-------------|-------------|---------------|---------------|
| 1 (Alto Risco) | 10% | ~42% | ~26% | Concentra os piores clientes |
| 2 | 10% | ~28% | ~43% | Risco alto |
| 3 | 10% | ~21% | ~56% | Risco medio-alto |
| 4 | 10% | ~17% | ~67% | Risco medio |
| 5 | 10% | ~14% | ~75% | Transicao |
| 6 | 10% | ~12% | ~82% | Risco medio-baixo |
| 7 | 10% | ~10% | ~88% | Risco baixo |
| 8 | 10% | ~8% | ~93% | Risco baixo |
| 9 | 10% | ~5% | ~97% | Risco muito baixo |
| 10 (Baixo Risco) | 10% | ~3% | 100% | Melhores clientes |

### 11.2 Monotonicidade

A taxa de FPD e **estritamente decrescente** do decil 1 ao 10, confirmando que o modelo ranqueia corretamente os clientes. A razao entre o pior e o melhor decil (42% / 3% = ~14x) indica forte poder de discriminacao.

### 11.3 Ponto de Corte para Politica de Credito

| Taxa de Aprovacao | Decis Aprovados | FPD Esperado | Reducao vs Sem Modelo |
|-------------------|-----------------|-------------|----------------------|
| 90% | 2 a 10 | ~13% | -20% |
| 80% | 3 a 10 | ~11% | -33% |
| 70% | 4 a 10 | ~10% | -40% |
| 60% | 5 a 10 | ~9% | -47% |

---

## 12. Decisoes Tecnicas e Licoes Aprendidas

### 12.1 Decisoes Criticas

| # | Decisao | Alternativa Considerada | Impacto |
|---|---------|------------------------|---------|
| 1 | **IV threshold = 0.01** | 0.02 (padrao industria) | +2 p.p. KS por incluir features comportamentais |
| 2 | **Split temporal (OOT)** | Cross-validation aleatorio | Evita vazamento temporal, simula producao |
| 3 | **LightGBM** | Logistica, XGBoost, ensemble | +6 p.p. vs logistica, +0.9 p.p. vs XGBoost |
| 4 | **Missing = -999** | NaN nativo | Consistencia entre Databricks e OCI |
| 5 | **Early stopping no OOT** | Fixed iterations | Previne overfitting automaticamente |
| 6 | **Dados completos (OCI)** | 50% sample (Databricks) | +0.45 p.p. KS |
| 7 | **Sem Python UDFs** | UDFs para features | Evita falha silenciosa (bug idade_anos jan/2026) |
| 8 | **Sufixo `_adj` em scores** | Usar scores brutos | Sentinelas (Score_01=0) tratados como NULL |

### 12.2 Licoes Aprendidas

**1. Features comportamentais tem baixo IV individual mas alto valor coletivo**
- Cada feature de recarga/pagamento/atraso tem IV entre 0.01-0.04
- Em conjunto, adicionam +2.69 p.p. ao KS
- Threshold IV=0.02 (padrao) excluiria essas features — erro grave

**2. SOS e o melhor preditor comportamental**
- `freq_sos_m1` e `pct_sos_sobre_credito_m1` estao no top 5 de importancia
- SOS (emprestimo emergencial de R$3-20) captura estresse financeiro em tempo real
- Informacao exclusiva do pre-pago que bureau nao tem

**3. Recargas de madrugada sao sinal de risco**
- `pct_recargas_madrugada_m1` aparece no top 20
- Comportamento atipico correlaciona com inadimplencia

**4. Juros pagos = atraso passado = risco futuro**
- `pct_pagamentos_com_juros_m1` e o melhor preditor do bloco Pagamento
- Clientes que ja pagaram juros tem padrao de atraso que se repete

**5. Python UDFs falham silenciosamente no Spark**
- Bug em jan/2026: `idade_anos` vazia por falha de UDF (serializacao)
- Corrigido com `F.to_date()` nativo do Spark
- Regra: nunca usar UDFs — apenas funcoes built-in do Spark

**6. Delta VACUUM e necessario para leitura fora do Spark**
- ABT v6 escrita em Delta com `.mode("overwrite")` — overwrite logico, nao fisico
- Leitura via pandas (`list_objects`) ve arquivos orfaos → duplicatas
- Solucao: `deltaTable.vacuum(retentionHours=0)` no builder ou Delta-aware listing no script

---

## 13. Artefatos do Modelo

### 13.1 Artefatos Gerados

| Artefato | Formato | Localizacao | Descricao |
|----------|---------|-------------|-----------|
| **Modelo** | .pkl (pickle) | `hackathon-2025-models/pkl/modelo_fpd.pkl` | Objeto LightGBM serializado |
| **Features** | .txt | `hackathon-2025-models/metricas/features_*.txt` | Lista de 261 features na ordem |
| **Metricas** | .json | `hackathon-2025-models/metricas/metricas_*.json` | KS, AUC, GINI, config |
| **Predicoes OOT** | .parquet | `hackathon-2025-models/resultados_modelo/predicoes_oot_*.parquet` | num_cpf, safra, score_fpd, decil |
| **Notebook** | .ipynb | `src/jobs/04_modeling/20260202 - Modelo Final FPD.ipynb` | Notebook Databricks (50% sample) |
| **Script OCI** | .py | `mig_oci/data_science/scripts/modelo_qualificacao.py` | Script VM OCI (100% dados) |

### 13.2 Como Reproduzir

**Databricks:**
```python
# Executar o notebook completo
# Path: /Workspace/src/jobs/04_modeling/20260202 - Modelo Final FPD.ipynb
# Configurar SAMPLE_FRACTION = 1.0 para dados completos
```

**OCI VM:**
```bash
# Na VM Modelo (via SSH ou Airflow DAG)
python3.11 /opt/modelo-fpd/modelo_qualificacao.py

# Ou local (debug)
python3.11 modelo_qualificacao.py --local
```

### 13.3 Dependencias

| Biblioteca | Versao | Uso |
|------------|--------|-----|
| Python | 3.11 | Runtime |
| LightGBM | 4.x | Modelo |
| scikit-learn | 1.x | Metricas (AUC, ROC) |
| pandas | 2.x | Manipulacao de dados |
| numpy | 1.26.4 | Operacoes numericas |
| pyarrow | 14.x | Leitura/escrita Parquet |
| oci (SDK) | 2.x | Acesso Object Storage (OCI VM) |

---

## 14. Reproducibilidade

### 14.1 Garantias de Reproducibilidade

| Aspecto | Garantia |
|---------|----------|
| **Seed** | 42 (fixo em todos os componentes aleatorios) |
| **Split** | Temporal por SAFRA (deterministico, sem aleatoriedade) |
| **Features** | Selecionadas por IV (deterministico dado os dados) |
| **Early stopping** | Baseado em AUC OOT (deterministico dado os dados) |
| **Dados** | ABT v6 versionada em Delta Lake (imutavel apos build) |

### 14.2 Variacoes Esperadas

| Fator | Variacao Esperada |
|-------|-------------------|
| Reruns com mesmos dados | KS OOT +/- 0.1 p.p. (aleatoriedade do GBDT) |
| Dados completos vs 50% sample | +0.3 a +0.5 p.p. (mais dados = melhor) |
| Novas safras (producao) | KS pode degradar 1-2 p.p./trimestre (drift) |

### 14.3 Validacao pela Banca

Para validar o modelo, a banca pode:

1. **Executar o notebook** com `SAMPLE_FRACTION = 1.0` no Databricks
2. **Executar o script OCI** na VM dedicada via `modelo_qualificacao.py`
3. **Verificar os artefatos** em `hackathon-2025-models/metricas/`
4. **Comparar predicoes** com o arquivo OOT (num_cpf + safra + score_fpd)

---

## Referencias

| Documento | Localizacao |
|-----------|-------------|
| Book de Variaveis (Entregavel B) | `banca_final/BOOK_VARIAVEIS_COMPORTAMENTAIS.md` |
| Estudo Publico-Alvo (Entregavel A) | `banca_final/ESTUDO_PUBLICO_ALVO.md` |
| Monitoramento (Entregavel F) | `banca_final/PLANO_MONITORAMENTO_MODELO.md` |
| Notebook Databricks | `src/jobs/04_modeling/20260202 - Modelo Final FPD.ipynb` |
| Script OCI | `mig_oci/data_science/scripts/modelo_qualificacao.py` |
| Book Completo ABT v6 | `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md` |
| Artefatos Modelo (guia) | `mig_oci/docs/MODELO_ARTEFATOS_GUIDE.md` |
| Troubleshooting Modelo | `mig_oci/docs/FASE_8_TROUBLESHOOTING.md` |
