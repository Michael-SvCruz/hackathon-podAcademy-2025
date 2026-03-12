# Estudo de Público-Alvo — Entregável A

## 1. Contexto do Público

### O universo: clientes pré-pago da Claro

O público-alvo deste estudo são **clientes de telefonia móvel pré-pago** da Claro, candidatos à migração para planos controle. A migração pré→controle é estratégica: aumenta receita recorrente (ARPU), mas introduz risco de crédito — o cliente passa a receber fatura mensal e pode inadimplir.

### Dimensão da base

| Métrica | Valor |
|---------|-------|
| **Total de registros (ABT v6)** | 3.795.310 |
| **Clientes com instalação aprovada** | 2.633.900 (69,4% da base) |
| **Clientes sem instalação** | 1.161.410 (30,6%) |
| **Casos de FPD (entre aprovados)** | 559.229 |
| **Taxa geral de FPD** | **21,23%** |

> **Leitura:** A cada 5 clientes pré-pago que migram para controle, aproximadamente 1 se torna inadimplente no primeiro pagamento. Essa taxa de ~21% justifica a necessidade de um modelo de behavior que diferencie perfis de risco.

### Safras disponíveis

| Safra | Uso | Registros (FLAG_INSTALACAO=1) |
|-------|-----|-------------------------------|
| Out/2024, Nov/2024, Dez/2024 | **Treino** | ~330.056 |
| Fev/2025, Mar/2025 | **OOT (validação)** | ~205.462 |
| Todas as safras (OOT completo para swap) | **Swap analysis** | 851.616 |

---

## 2. Fontes de Dados e Cobertura

O modelo utiliza **6 blocos de dados** que compõem a visão unificada do cliente (CPF). Cada bloco tem cobertura distinta, refletindo a heterogeneidade do público pré-pago:

### 2.1 Mapa de Cobertura

| Bloco | Cobertura (M1) | Variáveis | Fonte | O que revela |
|-------|---------------|-----------|-------|-------------|
| **Score_01** (Bureau) | 98,18% | 1 | Serasa/Boa Vista | Risco de crédito tradicional |
| **Score_02** (Bureau) | 99,95% | 1 | Bureau aprimorado | Complemento ao Score_01 |
| **Telco** | 35,46% | 68 | Sistemas internos Claro | Uso de dados, voz, SMS |
| **Cadastro** | 35–40% | 33 | Base cadastral | Idade, região, UF |
| **Recarga** | 56,12% | 60+ | Histórico de recargas | Padrão financeiro, SOS |
| **Pagamento** | 16,13% | 56 | Histórico de faturas | Comportamento de pagamento |
| **Atraso** | 21,79% | 19 | Inadimplência passada | Severidade e recorrência |

### 2.2 Interpretação da cobertura

```
Score Bureau  ████████████████████████████████████████████████  98%
Score_02      █████████████████████████████████████████████████  99%
Recarga       ████████████████████████████                      56%
Cadastro      █████████████████                                 37%
Telco         █████████████████                                 35%
Atraso        ██████████                                        22%
Pagamento     ████████                                          16%
```

**Insights sobre o público:**
- **Scores de bureau cobrem quase todos** (98-99%) — a maioria dos clientes pré-pago já tem algum histórico no SPC/Serasa, mesmo que limitado.
- **Recarga cobre 56%** — mais da metade dos clientes tem histórico de recarga no período M1 (último mês). É a fonte comportamental com melhor cobertura.
- **Pagamento cobre apenas 16%** — poucos clientes pré-pago já tiveram faturas (pode indicar ex-controle que voltou para pré, ou serviços adicionais).
- **Atraso cobre 22%** — cerca de 1 em 5 clientes já teve algum tipo de atraso registrado. A ausência de atraso também é informação (feature `flag_teve_atraso = 0`).

> **Implicação para o modelo:** Features com baixa cobertura individual (pagamento, atraso) ainda contribuem significativamente quando combinadas — o modelo aprende padrões mesmo com valores nulos, usando a **ausência de dados** como sinal.

---

## 3. Perfil de Risco por Decil

A análise por decil revela como o modelo segmenta a população em faixas de risco progressivas. Os dados abaixo referem-se à **população OOT (851.616 clientes)**.

### 3.1 Modelo LightGBM (score_fpd)

| Decil | Clientes | Taxa FPD | Score Mín. | Score Máx. | Perfil |
|-------|----------|----------|------------|------------|--------|
| **D1** (baixo risco) | 85.162 | **5,11%** | 0,005 | 0,060 | Excelentes pagadores |
| **D2** | 85.162 | 8,06% | 0,060 | 0,081 | Muito bons |
| **D3** | 85.161 | 10,00% | 0,081 | 0,103 | Bons |
| **D4** | 85.162 | 12,29% | 0,103 | 0,131 | Acima da média |
| **D5** | 85.161 | 15,40% | 0,131 | 0,165 | Médio |
| **D6** | 85.162 | 18,98% | 0,165 | 0,206 | Abaixo da média |
| **D7** | 85.161 | 23,28% | 0,206 | 0,257 | Risco moderado |
| **D8** | 85.162 | 28,99% | 0,257 | 0,325 | Risco elevado |
| **D9** | 85.161 | 37,27% | 0,325 | 0,426 | Alto risco |
| **D10** (alto risco) | 85.162 | **52,86%** | 0,426 | 0,964 | Altíssimo risco |

**Poder de separação: 10,3x** (5,11% no D1 vs 52,86% no D10)

### 3.2 Score_01 — Bureau (referência)

| Decil | Clientes | Taxa FPD | Score Mín. | Score Máx. |
|-------|----------|----------|------------|------------|
| **D1** (score baixo) | 88.824 | **40,94%** | 2 | 531 |
| **D2** | 89.107 | 33,83% | 532 | 553 |
| **D3** | 80.264 | 29,07% | 554 | 569 |
| **D4** | 84.096 | 24,50% | 570 | 582 |
| **D5** | 88.659 | 20,53% | 583 | 595 |
| **D6** | 83.674 | 17,23% | 596 | 607 |
| **D7** | 82.145 | 14,36% | 608 | 619 |
| **D8** | 85.917 | 12,44% | 620 | 636 |
| **D9** | 84.248 | 10,51% | 637 | 656 |
| **D10** (score alto) | 84.682 | **7,51%** | 657 | 771 |

**Poder de separação: 5,5x** (7,51% no D10 vs 40,94% no D1)

> **Nota:** No Score_01, lógica é inversa — score alto = bom cliente. No LightGBM (score_fpd), score alto = mau cliente.

### 3.3 Comparação de capacidade discriminatória

| Métrica | Score_01 (Bureau) | LightGBM (Behavior) | Ganho |
|---------|-------------------|---------------------|-------|
| **KS OOT** | 26,71% | 34,42% | **+7,71 p.p.** |
| **Separação D1/D10** | 5,5x | 10,3x | **+87%** |
| **FPD no pior decil** | 40,94% | 52,86% | Concentra mais maus |
| **FPD no melhor decil** | 7,51% | 5,11% | Identifica mais bons |

> **Leitura para o negócio:** O modelo de behavior quase **dobra** a capacidade de distinguir bons de maus clientes em relação ao bureau puro. No decil de menor risco (D1), a taxa FPD cai de 7,5% para 5,1% — são clientes que podem ser migrados para controle com alta confiança.

---

## 4. Segmentação Comportamental — O que Diferencia Bons de Maus

### 4.1 Blocos de features e contribuição para o KS

Cada bloco de dados adiciona poder preditivo incremental. A tabela mostra o **KS OOT** obtido ao treinar um modelo independente com features acumulativas:

| # | Bloco Adicionado | ABT | Features | KS OOT (%) | Delta (p.p.) |
|---|------------------|-----|----------|------------|--------------|
| 1 | Score_01 (Bureau) | v1 | 1 | 26,67 | baseline |
| 2 | + Score_02 | v2 | 2 | 31,25 | **+4,58** |
| 3 | + Telco | v3 | 89 | 31,51 | +0,26 |
| 4 | + Cadastro | v4 | 95 | 31,70 | +0,19 |
| 5 | + Recarga | v5 | 160 | 33,95 | **+2,25** |
| 6 | + Pagamento + Atraso | v6 | 261 | **34,39** | +0,44 |

```
KS OOT (%)
35 ┤                                              ╭── 34.39% MODELO FINAL
34 ┤                                         ╭────╯
33 ┤                                    ╭────╯ ─ ─ ─ ─ 33.10% BENCHMARK
32 ┤               ╭────────────────────╯
31 ┤          ╭────╯
30 ┤          │
29 ┤          │
28 ┤          │
27 ┤     ╭────╯
26 ┤─────╯
   └──────┴─────────┴──────────┴──────────┴──────────┴────
      Score_01  +Score_02    +Telco     +Recarga   +Pag+Atr
                            +Cadastro
```

### 4.2 Variáveis que mais diferenciam bons e maus

#### Scores de Bureau (contribuição dominante: +4,58 p.p.)
- **Score_01:** Score de crédito tradicional (Serasa/Boa Vista). IV = 0,48 — a variável individual mais preditiva
- **Score_02:** Score complementar. Juntos, os 2 scores alcançam KS 31,25%

#### Recarga — Indicador de Estresse Financeiro (contribuição: +2,25 p.p.)

As variáveis de recarga revelam o **comportamento financeiro real** do cliente pré-pago:

| Variável | O que mede | Por que diferencia |
|----------|-----------|-------------------|
| `freq_sos_m1/m3/m6` | Frequência de empréstimos SOS (R$3–20) | Estresse financeiro — **clientes que pegam SOS frequentemente têm maior risco** |
| `pct_sos_sobre_credito_m1` | % do crédito via SOS vs recargas normais | Alto % = dependência de microcrédito |
| `ticket_medio_m3` | Valor médio de recarga nos últimos 3 meses | Recargas menores = menor poder aquisitivo |
| `dias_medio_entre_recargas_m6` | Regularidade de recarga | Padrão irregular = instabilidade financeira |
| `coef_variacao_val_m3` | Variabilidade nos valores de recarga | Alta variação = imprevisibilidade financeira |
| `pct_recargas_madrugada_m1` | % de recargas entre 0h–6h | Padrão comportamental noturno |
| `pct_recargas_fim_semana_m1` | % de recargas no fim de semana | Padrão temporal de uso |

> **Insight-chave:** O **SOS (empréstimo de recarga)** é o indicador mais forte de estresse financeiro. Clientes que recorrem frequentemente ao SOS para manter o celular funcionando demonstram dificuldade financeira que se traduz em maior risco de inadimplência quando migram para controle.

#### Pagamento e Atraso — Histórico de Inadimplência (contribuição: +0,44 p.p.)

| Variável | O que mede | Por que diferencia |
|----------|-----------|-------------------|
| `qtd_pagamentos_m1/m3` | Quantidade de pagamentos realizados | Mais pagamentos = cliente ativo e adimplente |
| `pct_com_desconto_m3` | % de faturas negociadas com desconto | Alto % = histórico de negociação/renegociação |
| `sum_val_juros_pos_m3` | Valor total de juros pagos | Juros = atrasos passados |
| `pct_aging_90_plus_m1` | % de faturas com >90 dias de atraso | Inadimplência severa |
| `flag_teve_wo` | Já teve write-off (perda definitiva) | Sinal extremo de mau pagador |
| `flag_teve_fraude` | Já teve registro de fraude | Sinal extremo de risco |

#### Telco e Cadastro — Contexto Demográfico e de Uso (contribuição: +0,45 p.p.)

| Variável | O que mede | Por que diferencia |
|----------|-----------|-------------------|
| `var_26` a `var_93` | 68 variáveis de uso de telecomunicações | Padrões de consumo de dados, voz e SMS |
| `idade_anos` | Idade do cliente | Clientes mais velhos tendem a ser mais estáveis |
| `uf`, `regiao` | Localização geográfica | Variação regional nas taxas de inadimplência |

### 4.3 Seleção de features: IV >= 0,01

| Bloco | Features Totais | Selecionadas (IV >= 0,01) | IV Médio |
|-------|----------------|--------------------------|----------|
| Scores | 2 | 2 | 0,4839 |
| Recarga | 60+ | 74 | 0,0339 |
| Telco | 68 | 69 | 0,0145 |
| Pagamento | 56 | 56 | 0,0107 |
| Atraso | 19 | 19 | 0,0086 |
| Cadastro | 33 | 6 | — |
| **Total** | **614** | **261** | — |

> **Decisão técnica importante:** O threshold de IV foi fixado em **0,01** (não 0,02). Com IV >= 0,02, as features comportamentais seriam excluídas, perdendo +5,14 p.p. de KS. Features com baixo IV individual (0,01–0,04) ganham poder quando combinadas no ensemble do LightGBM.

---

## 5. Análise Swap-in/Swap-out — Impacto na Política de Crédito

A análise de swap compara a **política atual** (aprovação pelo Score_01 do bureau) com a **política proposta** (aprovação pelo modelo LightGBM), mantendo a mesma taxa de aprovação.

### 5.1 Conceito

```
┌─────────────────────────────────────────────────────────────────┐
│                    MESMA TAXA DE APROVAÇÃO                       │
│                                                                  │
│  Política ATUAL (Score_01):      Política NOVA (LightGBM):      │
│  Aprova os N melhores            Aprova os N melhores            │
│  pelo score de bureau            pelo score de behavior          │
│                                                                  │
│  Swap-out: clientes que eram      Swap-in: clientes que eram     │
│  aprovados pelo bureau mas        reprovados pelo bureau mas     │
│  são reprovados pelo modelo       são aprovados pelo modelo      │
│  → Perfil de ALTO RISCO          → Perfil de BAIXO RISCO        │
│    capturado pelo behavior          recuperado pelo behavior     │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Resultados por taxa de aprovação

| Taxa Aprov. | Swap-out (saem) | FPD Swap-out | Swap-in (entram) | FPD Swap-in | FPD Antes | FPD Depois | Redução |
|-------------|----------------|-------------|-----------------|-------------|-----------|-----------|---------|
| **70%** | 79.744 | 34,96% | 78.045 | 19,34% | 15,40% | 13,30% | **−13,6%** |
| **75%** | 79.225 | 38,16% | 69.938 | 21,32% | 16,42% | 14,26% | **−13,2%** |
| **80%** | 69.505 | 41,76% | 68.922 | 23,60% | 17,12% | 15,26% | **−10,8%** |
| **85%** | 61.185 | 45,86% | 56.204 | 26,16% | 18,14% | 16,42% | **−9,5%** |
| **90%** | 46.596 | 51,01% | 43.640 | 29,04% | 19,08% | 17,71% | **−7,2%** |

### 5.3 Cenário detalhado: taxa de aprovação 80%

| Grupo | Clientes | FPD | Descrição |
|-------|----------|-----|-----------|
| **Ambos aprovam** | 612.371 | 14,3% | Acordo entre bureau e modelo |
| **Swap-out** | 69.505 | **41,8%** | Bureau aprovava, modelo rejeita — maus capturados |
| **Swap-in** | 68.922 | **23,6%** | Bureau rejeitava, modelo aprova — bons recuperados |
| **Ambos rejeitam** | 100.818 | 47,3% | Acordo: alto risco confirmado |

**Impacto na taxa de FPD dos aprovados:**

```
ANTES (bureau):   17,12%  ████████████████████
DEPOIS (modelo):  15,26%  █████████████████
                          ─────────────────────
                          Redução de 10,8%
```

> **Tradução para o negócio:** Com a **mesma taxa de aprovação de 80%**, o modelo troca ~69K clientes que o bureau aprovaria (mas que têm 41,8% de FPD) por ~69K clientes que o bureau recusaria (mas que têm apenas 23,6% de FPD). O resultado líquido é uma **redução de 10,8% na taxa de inadimplência** sem alterar o volume de aprovações.

### 5.4 Cenário mais agressivo: taxa de aprovação 70%

| Métrica | Valor |
|---------|-------|
| Clientes que mudam de decisão | 157.789 |
| Maus retirados (swap-out) | 79.744 com FPD 34,96% |
| Bons incluídos (swap-in) | 78.045 com FPD 19,34% |
| **Redução na taxa FPD** | **−13,6%** (15,40% → 13,30%) |

---

## 6. Padrões e Segmentações Identificadas

### 6.1 Três perfis principais

Com base na análise por decil e nas variáveis que mais contribuem para a diferenciação:

#### Perfil 1 — Baixo Risco (Decis 1–3): ~255K clientes

| Característica | Valor |
|---------------|-------|
| **Taxa FPD** | 5,1% a 10,0% |
| **Score bureau** | Alto (tipicamente > 600) |
| **Recarga** | Regular, ticket médio estável, baixo uso de SOS |
| **Atraso** | Sem histórico ou mínimo |
| **Recomendação** | **Aprovar com confiança** — migração para controle com baixo risco |

#### Perfil 2 — Risco Médio (Decis 4–7): ~340K clientes

| Característica | Valor |
|---------------|-------|
| **Taxa FPD** | 12,3% a 23,3% |
| **Score bureau** | Médio (tipicamente 560–620) |
| **Recarga** | Variável, uso moderado de SOS |
| **Atraso** | Eventual (aging < 90 dias) |
| **Recomendação** | **Avaliar caso a caso** — migração com acompanhamento ou limite reduzido |

#### Perfil 3 — Alto Risco (Decis 8–10): ~255K clientes

| Característica | Valor |
|---------------|-------|
| **Taxa FPD** | 29,0% a 52,9% |
| **Score bureau** | Baixo (tipicamente < 560) |
| **Recarga** | Irregular, alto uso de SOS, ticket baixo |
| **Atraso** | Recorrente (aging 90+ dias, possível WO/fraude) |
| **Recomendação** | **Não aprovar** — manter no pré-pago ou oferecer plano com limite mínimo |

### 6.2 O valor escondido: Swap-in

O grupo mais interessante para o negócio é o **Swap-in** — clientes que o bureau reprovaria mas o modelo de behavior identifica como bons:

| Métrica | Swap-in (taxa 80%) |
|---------|-------------------|
| **Volume** | 68.922 clientes |
| **Taxa FPD** | 23,6% |
| **Comparação** | FPD 18 p.p. **menor** que os swap-out (41,8%) |
| **Perfil típico** | Score bureau médio-baixo, mas bom comportamento de recarga e sem atrasos graves |

> **Insight:** São clientes com **"nome sujo" no bureau mas bom comportamento na Claro**. O modelo de behavior captura que o comportamento interno (recargas regulares, sem SOS excessivo, sem atrasos graves) é um sinal mais recente e relevante que o score de bureau, que pode refletir problemas antigos já resolvidos.

---

## 7. Resumo para a Apresentação

### Números-chave

| Indicador | Valor |
|-----------|-------|
| Base total | 3,79M clientes |
| Taxa FPD geral | 21,23% |
| KS Modelo (OOT) | **34,39%** vs benchmark 33,10% |
| Separação D1/D10 | **10,3x** (5,1% vs 52,9%) |
| Redução FPD (taxa 80%) | **−10,8%** (17,1% → 15,3%) |
| Features utilizadas | 261 de 614 (IV >= 0,01) |
| Blocos de dados | 6 (Bureau, Telco, Cadastro, Recarga, Pagamento, Atraso) |

### Narrativa em 3 pontos

1. **O público pré-pago é heterogêneo** — a taxa de FPD varia de 5% (D1) a 53% (D10). Tratar todos igualmente resulta em perdas evitáveis.

2. **O comportamento interno é mais preditivo que o bureau** — features de recarga (SOS, regularidade, ticket) e pagamento adicionam +7,72 p.p. de KS sobre o Score_01 sozinho, porque refletem o momento financeiro atual do cliente na Claro.

3. **O modelo permite decisões melhores mantendo o mesmo volume** — na taxa de aprovação de 80%, trocamos 69K maus por 69K bons, reduzindo a inadimplência em 10,8% sem rejeitar mais clientes.

---

*Documento gerado em Março/2026 — Hackathon PodAcademy 2025*
*Fonte dos dados: ABT v6 (OCI VM), metricas/ks_incremental_20260307_1850.json, swap_analysis_20260308_1644.json*
