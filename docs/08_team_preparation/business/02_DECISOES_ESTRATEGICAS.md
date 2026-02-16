# Decisões Estratégicas

Este documento explica **por que** tomamos cada decisão importante do projeto e **quais alternativas** foram consideradas.

## 1. Benchmark KS = 33.1

### A Decisão
O benchmark a ser batido é KS = 33.1 medido no conjunto OOT (Out-of-Time) de Fev/Mar 2024.

### Por Que Este Valor?
- É a performance discriminatória da política atual da Claro
- Representa o "status quo" que precisa ser melhorado
- Foi definido pela coordenação como meta mínima de sucesso

### Por Que OOT e Não Test?
| Validação | Problema | Solução OOT |
|-----------|----------|-------------|
| **Test (mesma época)** | Modelo pode ter "decorado" padrões | OOT é período futuro nunca visto |
| **Cross-validation** | Não garante generalização temporal | OOT simula produção real |

**Exemplo:** Se treinamos até Jan/2024, o OOT (Mar/2024) testa se o modelo funciona 2 meses depois.

### Alternativas Consideradas
- ❌ **AUC como métrica:** KS é mais usado em risco de crédito por ser interpretável
- ❌ **Benchmark mais baixo:** Seria fácil demais, sem valor real
- ❌ **Benchmark muito alto (40+):** Irrealista, desmotivaria a equipe

---

## 2. Grupo de Controle (6º e 7º dígitos do CPF)

### A Decisão
Clientes com padrões "ZZ" e "ZX" nos dígitos 6-7 do CPF são o grupo de controle.

### Por Que Precisamos de Grupo de Controle?
Para fazer **inferência de rejeitados**: entender como clientes rejeitados teriam se comportado se aprovados.

```
Grupo de Controle:
- Seriam aprovados pela política normal
- Mas foram "deixados passar" para avaliação
- Permite análise de swap-in/swap-out
```

### Benefício para o Negócio
Sem grupo de controle, não sabemos se os rejeitados seriam bons ou ruins. Com o grupo de controle:
- Medimos quantos "bons" estamos rejeitando (oportunidade perdida)
- Medimos quantos "ruins" estamos aprovando (risco materializado)

### Alternativas Consideradas
- ❌ **Amostra aleatória:** Poderia introduzir viés de seleção
- ❌ **Todos aprovados:** Inviável operacionalmente (alto risco)
- ✓ **Dígitos do CPF:** Aleatório, sem viés, identificável

---

## 3. Ordem Incremental de KS (Mandatória)

### A Decisão
A apresentação DEVE mostrar o ganho de KS nesta ordem exata:

| Passo | Bloco de Dados | O Que Mostra |
|-------|----------------|--------------|
| 1 | Score_01 | Baseline (apenas bureau) |
| 2 | + Score_02 | Ganho do segundo score |
| 3 | + Telco | Ganho de variáveis de uso |
| 4 | + Cadastro | Ganho de dados demográficos |
| 5 | + Book Recarga | Ganho de comportamento de recarga |
| 6 | + Book Pagamento + Atraso | Ganho de histórico de pagamento |

### Por Que Esta Ordem?

**Resposta curta:** Determinação da coordenação (não negociável).

**Resposta de negócio:** Cada fonte de dados tem um custo de integração/manutenção. Mostrar o ganho incremental permite:
- Justificar o ROI de cada integração
- Priorizar investimentos em dados
- Negociar preços com fornecedores

### Exemplo Prático

```
Cenário: Integração de Recarga custa R$50.000/ano

Se KS com Recarga:    35.5
   KS sem Recarga:    33.1
   Ganho:            +2.4 KS

→ Cada 0.1 KS vale ~R$20.000/ano em redução de inadimplência
→ +2.4 KS = ~R$480.000/ano de benefício
→ ROI = 480.000 / 50.000 = 9.6x

DECISÃO: Integrar Recarga vale muito a pena!
```

### Alternativas Consideradas
- ❌ **Ordem livre:** Não permitida, conforme coordenação
- ❌ **Todas features juntas:** Não mostra valor individual de cada fonte

---

## 4. Janelas Temporais M1/M3/M6

### A Decisão
Todas as features comportamentais têm 3 versões:
- **M1:** Último mês antes da decisão
- **M3:** Últimos 3 meses
- **M6:** Últimos 6 meses

### Por Que Três Janelas?

| Janela | O Que Captura | Exemplo |
|--------|---------------|---------|
| **M1** | Comportamento recente | "Está em crise agora?" |
| **M3** | Tendência de curto prazo | "Está piorando?" |
| **M6** | Padrão estável | "Sempre foi assim?" |

### Benefício para o Modelo
O modelo pode aprender:
- Cliente com SOS alto em M1 mas baixo em M6 = crise temporária (menor risco)
- Cliente com SOS alto em M1, M3 e M6 = padrão crônico (maior risco)

### Alternativas Consideradas
- ❌ **Apenas M1:** Perde contexto histórico
- ❌ **M12 (12 meses):** Muito antigo, menos relevante
- ❌ **Janelas diferentes por feature:** Complexidade desnecessária

---

## 5. Split Temporal (Train/Test/OOT)

### A Decisão
```
TRAIN:  até Jan/2024    (treinar modelo)
TEST:   Fev/2024        (ajustar hiperparâmetros)
OOT:    Mar/2024        (validação final, intocável)
```

### Por Que Split Temporal e Não Aleatório?

**Em risco de crédito, o tempo importa:**
- Economia muda
- Perfil de cliente muda
- Políticas mudam

**Split aleatório vazaria informação do futuro para o passado (leakage).**

### Regra de Ouro
> "Nunca use dados do futuro para prever o passado."

### Alternativas Consideradas
- ❌ **Split aleatório:** Vazamento temporal
- ❌ **Cross-validation:** Não garante generalização temporal
- ✓ **Split temporal fixo:** Simula produção real

---

## 6. SOS como Indicador de Stress Financeiro

### A Decisão
O SOS (empréstimo R$3-20 da Claro) é tratado como **indicador de stress financeiro**, não como receita.

### O Que é SOS?
- Cliente fica sem crédito no celular
- Claro oferece "empréstimo" de R$5 (tipicamente)
- Valor é descontado da próxima recarga
- Cliente que usa muito SOS está constantemente sem dinheiro

### Por Que é Importante?
```
Alta frequência de SOS = Cliente em dificuldade financeira
                       = Maior risco de inadimplência
                       = FPD mais provável
```

### Regra Específica (Fernando/Claro)
> "SOS e bônus NÃO contam como dinheiro real. Recarga de R$20 com R$5 de SOS = R$20 de dinheiro real, não R$25."

### Features Derivadas
- `freq_sos_m1`: Quantas vezes usou SOS no último mês
- `pct_sos_sobre_credito`: % do crédito que veio de SOS

---

## 7. Infraestrutura: Databricks Primeiro, Oracle Depois

### A Decisão
- **Fase de desenvolvimento:** Databricks/AWS (sem limite de tempo)
- **Defesa final:** Oracle Cloud (30 dias de acesso)

### Por Que Não Começar no Oracle?

| Fator | Databricks | Oracle (30 dias) |
|-------|------------|------------------|
| **Tempo** | Ilimitado | 30 dias apenas |
| **Maturidade** | Ambiente familiar | Curva de aprendizado |
| **Risco** | Baixo | Alto se falhar |

### Estratégia
> "Quebre a cabeça com os dados no Databricks. Chegue no Oracle com a solução pronta."

### Alternativas Consideradas
- ❌ **Só Oracle:** Risco de esgotar 30 dias explorando dados
- ❌ **Só Databricks:** Não atende requisito de parceria Oracle

---

## 8. FLAG_INSTALACAO: Só Treinar Onde Observamos

### A Decisão
O modelo é treinado **apenas** em clientes com `FLAG_INSTALACAO = 1` (aprovados que contrataram).

### Por Que Não Usar FLAG=0 no Treino?

```
FLAG=1 (Aprovado + Contratou):
  → FPD é observado (0 ou 1)
  → Podemos treinar aqui

FLAG=0 (Rejeitado OU não contratou):
  → FPD não é observável
  → Não sabemos o que teria acontecido
  → NÃO podemos assumir que seriam bons ou ruins
```

### O Erro Comum a Evitar
❌ "Cliente rejeitado = cliente ruim"
❌ "Cliente que não contratou = cliente bom"

Nenhuma dessas suposições é válida sem evidência.

### Como Usamos FLAG=0?
- Para **reject inference** (com grupo de controle)
- Para análise de **swap-in/swap-out**
- **NÃO** para treino direto do modelo

---

## Resumo das Decisões

| Decisão | Escolha | Justificativa Principal |
|---------|---------|------------------------|
| Benchmark | KS = 33.1 OOT | Meta realista e mensurável |
| Grupo controle | Dígitos CPF | Aleatório e identificável |
| Ordem de KS | Score → Telco → Cadastro → Recarga → Pag/Atraso | Determinação coordenação + ROI |
| Janelas | M1/M3/M6 | Captura comportamento recente e estável |
| Split | Temporal (Train/Test/OOT) | Evita vazamento temporal |
| SOS | Indicador de stress | Regra de negócio da Claro |
| Infra | Databricks → Oracle | Minimiza risco dos 30 dias |
| Treino | Só FLAG=1 | Só onde FPD é observado |
