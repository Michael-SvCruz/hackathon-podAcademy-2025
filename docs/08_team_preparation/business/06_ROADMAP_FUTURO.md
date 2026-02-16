# Roadmap Futuro: Do Presente até a Entrega Final

Este documento descreve o que falta fazer desde o momento atual (30/Jan/2026) até a conclusão do projeto.

## Status Atual

### Concluído (Engenharia de Dados)

| Item | Status | Data |
|------|--------|------|
| Bronze/Silver completo | ✓ | Dez/2025 |
| Gold ABT v1-v4 | ✓ | Dez/2025 |
| Fix do Cadastro (UDF → F.to_date) | ✓ | 26/Jan/2026 |
| Gold Recarga Features v2 | ✓ | Jan/2026 |
| Gold Pagamento Features v2 | ✓ | Jan/2026 |
| Gold Atraso Features v2 | ✓ | Jan/2026 |
| ABT v5 v2 (311 colunas) | ✓ | Jan/2026 |
| **ABT v6 v2 (614 colunas)** | ✓ | **30/Jan/2026** |
| Validação (11 gates) | ✓ | 30/Jan/2026 |
| Variable Book documentado | ✓ | 30/Jan/2026 |

### Em Andamento

| Item | Responsável | Prazo |
|------|-------------|-------|
| Documentação de preparação da equipe | Data Engineering | 31/Jan/2026 |
| EDA (Análise Exploratória) | Data Science | 1ª semana Fev |

### A Fazer (Modelagem)

| Item | Descrição | Prazo Estimado |
|------|-----------|----------------|
| Baseline Model | Logistic Regression com Score_01 | 1ª semana Fev |
| Modelos Incrementais | KS por versão (v1→v2→...→v6) | 1ª-2ª semana Fev |
| Feature Selection | Reduzir 614 → 50-100 features | 2ª semana Fev |
| Modelo Final | XGBoost/LightGBM otimizado | 2ª semana Fev |
| Interpretação | SHAP analysis | 2ª-3ª semana Fev |
| Swap Analysis | Matriz de confusão + impacto | 3ª semana Fev |
| Apresentação | PPT para qualificação | 3ª semana Fev |

---

## Fase 1: Modelagem Incremental (1ª-2ª semana Fev)

### Objetivo
Medir o KS incremental de cada bloco de dados na ordem obrigatória.

### Passos

```
1. Preparar dados:
   df_train = ABT v6 com FLAG=1, SAFRA < 202402
   df_test = ABT v6 com FLAG=1, SAFRA = 202402
   df_oot = ABT v6 com FLAG=1, SAFRA = 202403

2. Treinar modelos incrementais:
   Modelo 1: apenas Score_01 → KS₁
   Modelo 2: Score_01 + Score_02 → KS₂
   Modelo 3: + Telco (var_26-93) → KS₃
   Modelo 4: + Cadastro → KS₄
   Modelo 5: + Recarga (M1/M3/M6) → KS₅
   Modelo 6: + Pagamento + Atraso → KS₆

3. Reportar ganhos incrementais:
   ΔKS₂ = KS₂ - KS₁ (ganho de Score_02)
   ΔKS₃ = KS₃ - KS₂ (ganho de Telco)
   ... etc
```

### Entregável
Tabela com KS por modelo e gráfico de barras mostrando ganho incremental.

---

## Fase 2: Feature Selection (2ª semana Fev)

### Objetivo
Reduzir 614 features para um conjunto gerenciável (50-100) sem perda significativa de KS.

### Abordagens

| Método | Descrição | Quando Usar |
|--------|-----------|-------------|
| **Importância do modelo** | Features mais importantes do XGBoost | Primeira triagem |
| **Correlação** | Remover features muito correlacionadas (>0.9) | Reduzir redundância |
| **VIF** | Variance Inflation Factor | Multicolinearidade |
| **IV (Information Value)** | Poder preditivo individual | Ranking univariado |
| **RFE** | Eliminação recursiva | Refinamento final |

### Passos

```
1. Treinar XGBoost com todas 614 features
2. Extrair importance (gain, cover, frequency)
3. Selecionar top 100 por importance
4. Calcular correlação entre elas
5. Remover redundantes (r > 0.9, manter mais importante)
6. Validar: treinar modelo só com selecionadas
7. Comparar KS: perda < 0.5 é aceitável
```

### Entregável
Lista de 50-100 features finais com justificativa.

---

## Fase 3: Modelo Final (2ª semana Fev)

### Objetivo
Otimizar hiperparâmetros do modelo escolhido.

### Modelos Candidatos

| Modelo | Prós | Contras |
|--------|------|---------|
| **Logistic Regression** | Interpretável, baseline | Menos poder preditivo |
| **XGBoost** | Alto poder, robusto | Menos interpretável |
| **LightGBM** | Rápido, bom para grandes dados | Similar ao XGBoost |

**Recomendação:** XGBoost para performance, Logistic Regression para interpretação.

### Otimização

```python
# Grid Search ou Bayesian Optimization
param_grid = {
    'max_depth': [3, 5, 7],
    'learning_rate': [0.01, 0.05, 0.1],
    'n_estimators': [100, 200, 500],
    'min_child_weight': [1, 5, 10],
    'subsample': [0.8, 1.0],
    'colsample_bytree': [0.8, 1.0]
}
```

### Entregável
Modelo final treinado com hiperparâmetros otimizados.

---

## Fase 4: Interpretação e Explicabilidade (2ª-3ª semana Fev)

### Objetivo
Explicar POR QUE o modelo faz cada predição.

### Ferramentas

| Ferramenta | O Que Faz | Output |
|------------|-----------|--------|
| **SHAP** | Contribuição de cada feature | Gráficos de importância |
| **Partial Dependence** | Efeito marginal de uma feature | Curvas |
| **ICE** | Individual Conditional Expectation | Curvas por cliente |

### Análises Obrigatórias

1. **SHAP Summary Plot:** Quais features mais impactam?
2. **SHAP por grupo:** Features impactam diferente bons vs. maus?
3. **Top 10 features:** Explicação de negócio para cada uma

### Exemplo de Insight

```
"freq_sos_m1 é a 3ª feature mais importante.
Clientes com freq_sos > 5 no último mês têm 3x mais chance de FPD.
Isso confirma que SOS indica stress financeiro."
```

### Entregável
Gráficos SHAP + narrativa explicando as principais features.

---

## Fase 5: Swap Analysis (3ª semana Fev)

### Objetivo
Quantificar o impacto de negócio do modelo.

### Passos

```
1. Definir ponto de corte do modelo (ex: KS máximo)

2. Classificar cada cliente:
   - Política atual: FLAG_INSTALACAO (1=aprovado)
   - Modelo: score > corte (1=aprovaria)

3. Criar matriz:
   swap_out = (FLAG=1) AND (modelo=0)  # Seria rejeitado
   swap_in = (FLAG=0) AND (modelo=1)   # Seria aprovado

4. Calcular FPD em cada grupo:
   - FPD em swap_out: quantos maus evitados?
   - Proxy para swap_in (usar grupo controle)

5. Estimar impacto financeiro:
   - Economia = swap_out_maus * custo_inadimplencia
   - Receita = swap_in_bons * receita_cliente
```

### Entregável
Tabela de swap + estimativa de impacto em R$.

---

## Fase 6: Preparação da Apresentação (3ª semana Fev)

### Objetivo
Criar apresentação para defesa de qualificação.

### Estrutura Sugerida

| Slide | Conteúdo | Tempo |
|-------|----------|-------|
| 1 | Título + Equipe | 1 min |
| 2 | Problema de Negócio | 2 min |
| 3 | Arquitetura de Dados | 3 min |
| 4 | Evolução da ABT (v1→v6) | 3 min |
| 5 | KS Incremental (gráfico) | 5 min |
| 6 | Feature Selection | 3 min |
| 7 | Modelo Final + SHAP | 5 min |
| 8 | Swap Analysis | 5 min |
| 9 | Conclusões + Próximos Passos | 2 min |
| 10 | Q&A | 10 min |

**Tempo total:** ~40 min

### Responsáveis

| Seção | Responsável |
|-------|-------------|
| Problema + Arquitetura | Data Engineering Lead |
| Modelagem + KS | Data Science Lead |
| Swap Analysis | Business Analyst |
| Apresentação (design) | Comunicação |

---

## Fase 7: Defesa de Qualificação (Fev/2026)

### O Que Esperar
- Apresentação para banca
- Perguntas técnicas e de negócio
- Feedback para melhorias

### Critérios de Avaliação (Esperados)

| Critério | Peso |
|----------|------|
| Qualidade técnica | 30% |
| Inovação na abordagem | 20% |
| Clareza da apresentação | 20% |
| Impacto de negócio | 20% |
| Respostas às perguntas | 10% |

---

## Fase 8: Migração para Oracle Cloud (Pós-Qualificação)

### Objetivo
Executar todo o pipeline no Oracle Cloud para defesa final.

### Timeline
- **Janela:** 30 dias a partir da qualificação
- **Estratégia:** Chegar com código pronto, só adaptar infraestrutura

### Tarefas

| Tarefa | Esforço | Risco |
|--------|---------|-------|
| Setup do ambiente Oracle | 2-3 dias | Médio |
| Migração dos dados | 1-2 dias | Baixo |
| Adaptação do código Spark | 1-2 dias | Médio |
| Execução do pipeline | 1 dia | Baixo |
| Testes e validação | 1-2 dias | Baixo |
| **Buffer para imprevistos** | 5 dias | - |

**Recomendação:** Ter o código 100% funcional no Databricks ANTES de iniciar a migração.

---

## Fase 9: Defesa Final (Mar/2026)

### O Que Muda
- Executar no Oracle Cloud
- Apresentação mais polida
- Incorporar feedback da qualificação

### Entregáveis Finais
1. Modelo final (.pkl ou similar)
2. Código fonte (Git)
3. Documentação técnica
4. Apresentação final
5. Análise de impacto de negócio

---

## Resumo do Roadmap

```
30/Jan ──────► 1ª sem Fev ──────► 2ª sem Fev ──────► 3ª sem Fev
    │              │                  │                  │
    ▼              ▼                  ▼                  ▼
COMPLETO       MODELAGEM          MODELO FINAL      APRESENTAÇÃO
ABT v6         Incremental        Feature Sel.      Swap Analysis
614 cols       KS por versão      XGBoost opt       PPT
                                  SHAP


3ª sem Fev ──────► Fev/Mar ──────► Mar/2026
     │                │                │
     ▼                ▼                ▼
 QUALIFICAÇÃO     ORACLE          DEFESA FINAL
 Databricks       Migração        Oracle Cloud
 Feedback         30 dias         Entrega
```

---

## Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| KS não bate benchmark | Média | Alto | Feature engineering adicional |
| Falta de tempo para Oracle | Baixa | Alto | Código pronto antes de migrar |
| Membro ausente | Baixa | Médio | Documentação detalhada |
| Dados corrompidos | Muito baixa | Alto | Validações em cada etapa |

---

## Checklist de Preparação para Qualificação

- [ ] ABT v6 validada (11 gates)
- [ ] Modelos incrementais treinados (v1→v6)
- [ ] KS > 33.1 em OOT
- [ ] Feature selection concluída
- [ ] SHAP analysis pronta
- [ ] Swap analysis com impacto em R$
- [ ] Apresentação revisada
- [ ] Equipe ensaiou apresentação
- [ ] Backup dos dados e código
- [ ] Perguntas frequentes preparadas
