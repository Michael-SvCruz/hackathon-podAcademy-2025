# Métricas de Sucesso

Este documento explica as métricas que serão usadas para avaliar o modelo e como interpretá-las do ponto de vista de negócio.

## Métrica Principal: KS (Kolmogorov-Smirnov)

### O Que É

KS mede a **máxima separação** entre as distribuições cumulativas de "bons" (FPD=0) e "maus" (FPD=1) clientes.

```
100% ─┬──────────────────────┐
      │                    ╱ │
      │        Maus      ╱   │
      │      (FPD=1)   ╱     │
      │              ╱       │
      │            ╱  ◄──────┼── KS = distância máxima
      │          ╱           │
      │        ╱   Bons      │
      │      ╱   (FPD=0)     │
      │    ╱                 │
  0% ─┴──╱───────────────────┘
         Score baixo → alto
```

### Como Interpretar

| KS | Interpretação |
|----|---------------|
| < 20 | Fraco (pouco melhor que aleatório) |
| 20-30 | Razoável (aceitável para produção) |
| 30-40 | Bom (competitivo no mercado) |
| 40-50 | Muito bom (alto poder discriminatório) |
| > 50 | Excelente (raro, verificar se há leakage) |

### Nosso Benchmark

**KS = 33.1** no conjunto OOT (Out-of-Time)

Significa que a política atual consegue separar bons de maus com 33.1% de eficácia. Precisamos superar isso.

### Por Que KS e Não AUC?

| Métrica | Vantagem | Desvantagem |
|---------|----------|-------------|
| **KS** | Interpretável, padrão em crédito | Ponto único, não área |
| **AUC** | Visão completa da curva | Menos intuitivo para negócio |
| **Gini** | Similar ao AUC (Gini = 2*AUC - 1) | Menos comum |

**Decisão:** Usamos KS porque é o padrão do mercado de crédito e a coordenação definiu assim.

---

## Métricas Secundárias

### Taxa de Aprovação

```
Taxa de Aprovação = Aprovados / Total
                  = FLAG_INSTALACAO=1 / Total
```

**Atual:** 69.40% (2,633,900 de 3,795,310)

**Por que importa:** Se o modelo rejeitar muitos clientes, perde receita. Se aprovar demais, aumenta inadimplência.

### Taxa de FPD (entre aprovados)

```
Taxa de FPD = FPD=1 / FLAG=1
            = 559,229 / 2,633,900
            = 21.23%
```

**Por que importa:** Esta é a "taxa de inadimplência" atual. O modelo deve ajudar a reduzi-la.

---

## Análise de Swap-In/Swap-Out

### O Que É

Compara as decisões do **modelo** com as da **política atual**:

```
                          MODELO
                     Aprova    Rejeita
                   ┌─────────┬─────────┐
POLÍTICA   Aprova  │    A    │    B    │
ATUAL              │ Acordo  │ Swap-out│
                   ├─────────┼─────────┤
           Rejeita │    C    │    D    │
                   │ Swap-in │ Acordo  │
                   └─────────┴─────────┘
```

### Interpretação

| Célula | Significado | Impacto de Negócio |
|--------|-------------|-------------------|
| **A** | Ambos aprovam | Sem mudança |
| **B** | Modelo rejeita quem política aprova | Evita maus clientes |
| **C** | Modelo aprova quem política rejeita | Recupera bons clientes |
| **D** | Ambos rejeitam | Sem mudança |

### Métricas Derivadas

```
Swap-out Rate = B / (A + B)
  → % de aprovados atuais que seriam rejeitados

Swap-in Rate = C / (C + D)
  → % de rejeitados atuais que seriam aprovados
```

### Exemplo Numérico

```
Cenário hipotético com modelo no ponto de corte KS máximo:

                          MODELO
                     Aprova    Rejeita
                   ┌─────────┬─────────┐
POLÍTICA   Aprova  │ 2.400k  │  234k   │  ← B: 234k seriam rejeitados
ATUAL              │         │ (swap-  │
                   │         │  out)   │
                   ├─────────┼─────────┤
           Rejeita │  100k   │ 1.061k  │  ← C: 100k seriam aprovados
                   │(swap-in)│         │
                   └─────────┴─────────┘

Swap-out Rate = 234k / 2.634k = 8.9%
Swap-in Rate = 100k / 1.161k = 8.6%
```

### Por Que Importa para o Negócio

**Swap-out (B):**
- Cada mau cliente evitado = economia de ~R$X.XXX (custo de inadimplência)
- Se 234k swap-outs e 50% seriam maus → 117k * custo = economia

**Swap-in (C):**
- Cada bom cliente recuperado = receita adicional
- Se 100k swap-ins e 80% são bons → 80k * receita = ganho

---

## Foco na Metade Inferior da ROC

### O Que Significa

A curva ROC tem duas regiões:

```
      │
 TPR  │     ┌───────── Metade Superior
(Sens)│    /           FPR alto (muitas rejeições)
      │   /            Menos impacto prático
      │  /
      │ /────────────── Metade Inferior
      │/                FPR baixo (muitas aprovações)
      └───────────────  FOCO DO NEGÓCIO
        FPR (1-Spec) →
```

### Por Que Focamos Embaixo

Na metade inferior:
- **Ponto de operação realista:** A empresa não vai rejeitar 80% dos clientes
- **Impacto financeiro:** Decisões marginais (aprovar/rejeitar no limite) têm maior impacto
- **Trade-off real:** Pequenas mudanças no score fazem grande diferença

### Como Reportar

Ao invés de apenas KS global, reportar também:
- TPR@FPR=5% (sensibilidade quando especificidade = 95%)
- TPR@FPR=10%
- TPR@FPR=20%

---

## PSI (Population Stability Index)

### O Que É

Mede se a **população mudou** entre treino e validação.

```
PSI = Σ (Atual% - Esperado%) * ln(Atual% / Esperado%)
```

### Interpretação

| PSI | Interpretação |
|-----|---------------|
| < 0.10 | Estável (sem mudança significativa) |
| 0.10-0.25 | Mudança moderada (investigar) |
| > 0.25 | Mudança significativa (modelo pode estar desatualizado) |

### Por Que Importa

Se PSI alto entre Treino e OOT:
- População mudou (novo perfil de cliente)
- Modelo pode não generalizar bem
- Pode ser necessário retreinar

---

## Resumo das Metas

| Métrica | Meta | Status |
|---------|------|--------|
| **KS OOT** | > 33.1 | A medir |
| **Swap-out Rate** | 5-15% | A medir |
| **Swap-in Rate** | 5-15% | A medir |
| **PSI (Train→OOT)** | < 0.25 | A medir |
| **Taxa de FPD** | Reduzir de 21.23% | A medir |

---

## Como Apresentar os Resultados

### Slide 1: KS Incremental

```
┌────────────────────────────────────────┐
│  KS por Versão da ABT (OOT)            │
│                                        │
│  v1 (Score_01)      │████████ 28.5     │
│  v2 (+Score_02)     │█████████ 30.2    │
│  v3 (+Telco)        │██████████ 31.8   │
│  v4 (+Cadastro)     │███████████ 32.5  │
│  v5 (+Recarga)      │████████████ 34.1 │ ← Supera benchmark
│  v6 (+Pag/Atraso)   │█████████████ 36.2│
│                     ├──────────────────│
│                 Benchmark = 33.1       │
└────────────────────────────────────────┘
```

### Slide 2: Swap Analysis

```
┌────────────────────────────────────────┐
│  Análise Swap-In / Swap-Out            │
│                                        │
│  Swap-out: 234,000 clientes (8.9%)     │
│    → 117,000 seriam maus (50% FPD)     │
│    → Economia estimada: R$ X milhões   │
│                                        │
│  Swap-in: 100,000 clientes (8.6%)      │
│    → 80,000 seriam bons (80% adimpl.)  │
│    → Receita adicional: R$ Y milhões   │
│                                        │
│  Impacto líquido: R$ Z milhões/ano     │
└────────────────────────────────────────┘
```

### Slide 3: Matriz de Confusão Visual

```
┌────────────────────────────────────────┐
│                MODELO                  │
│            Aprova    Rejeita           │
│         ┌─────────┬─────────┐          │
│ POLÍTICA│ 2.400k  │  234k   │ Aprova   │
│ ATUAL   │  ✓ OK   │ Swap-out│          │
│         ├─────────┼─────────┤          │
│         │  100k   │ 1.061k  │ Rejeita  │
│         │ Swap-in │   ✓ OK  │          │
│         └─────────┴─────────┘          │
└────────────────────────────────────────┘
```

---

## Perguntas Esperadas

### "Como vocês definiram o ponto de corte?"
O ponto de corte foi definido no ponto de KS máximo. Para produção, pode ser ajustado baseado em apetite de risco (mais aprovações vs. menos inadimplência).

### "E se o modelo errar?"
Toda decisão automatizada tem erros. O importante é que o modelo erre MENOS que a política atual. Swap analysis mostra exatamente o trade-off.

### "O modelo pode ser discriminatório?"
As variáveis demográficas (idade, região) devem ser analisadas para viés. O modelo não usa gênero ou raça diretamente, mas proxies podem existir. Análise de fairness é recomendada.
