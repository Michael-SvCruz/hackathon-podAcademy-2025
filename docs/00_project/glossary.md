# Glossário — Crédito & Risco (projeto telecom)

Este glossário reúne termos comuns em projetos de **risco de crédito** e **modelos de score**, com foco no que mais aparece em datasets e discussões técnicas do projeto.

---

## 1) Conceitos de dataset e engenharia

### Spine
A **spine** é a tabela “espinha dorsal” do dataset de modelagem (ABT/Gold).  
Ela define:
- **universo** (quem está dentro do estudo),
- **grão** (ex.: cliente-mês),
- **chave canônica** (ex.: `NUM_CPF + SAFRA`).

Todas as demais fontes entram como **enriquecimento** do spine.

### Chave canônica
A **chave canônica** é a chave oficial do projeto para joins e consistência de grão.  
Benefícios:
- padroniza joins entre fontes,
- evita duplicidade e “mudança invisível” de granularidade,
- facilita reprodutibilidade e debugging.

Exemplo típico: `NUM_CPF + SAFRA`.

### Safra (Vintage)
**Safra** (ou **vintage**) é a coorte temporal de referência, geralmente mensal (YYYYMM).  
É usada para:
- split temporal (treino/validação/OOT),
- análises por período,
- construção de features por janela (M1/M3/M6/M12),
- monitoramento de estabilidade ao longo do tempo.

### OOT (Out-Of-Time)
**OOT** é uma validação em período futuro em relação ao treino.  
Exemplo:
- treino: 202410–202501  
- OOT: 202502–202503  

OOT é crítico em risco/crédito por causa de mudanças de comportamento, sazonalidade e drift.

---

## 2) Termos de inadimplência e comportamento de pagamento

### DPD (Days Past Due)
**DPD** = **dias em atraso** (quantos dias a obrigação/pagamento está vencida).  
Exemplos comuns:
- **DPD 0**: em dia  
- **DPD 1–29**: atraso curto  
- **DPD 30+**: atraso relevante (muito usado como “default operacional”)  
- **DPD 60+ / 90+**: atraso severo  

DPD aparece em features e em labels (ex.: “virou 30+ em até 90 dias”).

### Ever (ex.: EVER_30_M3)
**EVER** costuma significar: “já aconteceu **pelo menos uma vez** na janela”.  
Exemplo: **EVER_30_M3** geralmente significa:
- Flag = 1 se o cliente teve **ao menos um evento** de **DPD >= 30** nos **últimos 3 meses** (M3).
- Caso contrário, 0.

Obs.: a janela precisa ser sempre definida “olhando para trás” a partir da data_ref (ex.: SAFRA) para evitar leakage.

### FPD (First Payment Default)
**FPD** (uso comum no mercado): default/atraso logo no **primeiro pagamento** após concessão/contratação.  
Em muitos projetos, FPD é **label** (target), não feature.  
Risco: se usado como feature, pode representar **leakage** (evento pós-decisão).

---

## 3) Métricas e avaliação de modelos em crédito

### KS (Kolmogorov–Smirnov)
**KS** mede a separação entre as distribuições de score dos “bons” e “maus”.  
Em crédito, é uma métrica muito usada por ser:
- interpretável,
- comparável ao longo do tempo,
- compatível com avaliação por faixas de score.

No projeto, existe um **benchmark** (ex.: KS = 33,1 no OOT), e o trabalho deve reportar KS por safra e incrementalmente por fonte.

### Gini (coeficiente de Gini)
**Gini** é uma transformação do AUC:
- $\displaystyle \text{Gini} = 2 \cdot AUC - 1$

É comum em crédito e às vezes aparece como métrica principal em relatórios.

### AUC / ROC
**AUC** mede a capacidade de ranking do modelo (probabilidade de ranquear um “mau” acima de um “bom”).  
É útil, mas em crédito muitas vezes KS é mais “padrão de mercado”.

### Bad Rate
**Bad rate** é a taxa de “maus” (inadimplentes) no público avaliado.  
Exemplo (para uma safra):

- $\displaystyle \text{BadRate} = \frac{\#(FPD=1)}{\#(FPD \in \{0,1\})}$

É essencial para:
- entender desbalanceamento,
- comparar safras,
- avaliar drift de comportamento.

### Matriz de Confusão
Tabela com:
- True Positive, False Positive, True Negative, False Negative

Em crédito, é muito usada para discutir trade-off entre:
- aprovar mais (ganho)
- controlar inadimplência (risco)

### Swap-in / Swap-out
Comparação entre uma política/modelo atual (baseline) e uma nova:

- **Swap-in:** casos que entram (passam a ser aprovados/bons) no novo modelo  
- **Swap-out:** casos que saem (deixam de ser aprovados/bons) no novo modelo  

Ajuda a explicar “quem o modelo está trocando” e o impacto de decisão.

### PSI (Population Stability Index)
**PSI** mede mudança na distribuição de uma variável (ou score) entre dois períodos.  
É usado para **monitorar drift**.

Interpretação comum (regra de bolso):
- PSI < 0,1: estável  
- 0,1–0,25: atenção  
- _>0,25: drift relevante

---

## 4) Estratégias do projeto

### Roteiro incremental de KS
Estratégia exigida de apresentar ganho marginal de performance por blocos de informação:
1. Scores (Score 1, depois Score 2)
2. Telco
3. Cadastral
4. Book de Recarga
5. Book de Pagamento/Atraso

O incremento deve ser calculado sempre comparando com a etapa anterior (modelo N vs N-1).

### Grupo controle
Subconjunto de clientes definido por regra determinística (ex.: padrão em dígitos do CPF), usado para comparações e validações do estudo.

---

## 5) Notas finais
- Em todo termo com janela temporal (M1/M3/M6/M12), documentar explicitamente:
  - o que é “data de referência” (ex.: SAFRA),
  - como a janela é construída (lookback),
  - e quais eventos são considerados.