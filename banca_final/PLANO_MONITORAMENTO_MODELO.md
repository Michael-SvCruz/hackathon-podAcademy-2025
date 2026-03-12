# Plano de Monitoramento do Modelo — Entregável F

## Modelo em Produção

| Item | Valor |
|------|-------|
| **Modelo** | LightGBM (gbdt, binary, AUC) |
| **Target** | `fpd_int` — First Payment Default |
| **KS OOT** | **34.39%** (+1.29 p.p. acima do benchmark 33.10%) |
| **AUC OOT** | 0.7327 |
| **GINI OOT** | 46.54% |
| **Features** | 261 (selecionadas por IV >= 0.01 de 614 candidatas) |
| **Treino** | 330.056 registros (safras out/nov/dez 2024) |
| **OOT** | 205.462 registros (safras fev/mar 2025) |
| **Artefatos** | Bucket `hackathon-2025-models` (PKL, predições, métricas) |
| **Orquestração** | Airflow — DAG `pipeline_modelo_qualificacao` (Start/Stop VM) |

---

## 1. Métricas-Chave de Monitoramento

### 1.1 Métricas de Performance do Modelo

| Métrica | Descrição | Baseline (Treino Inicial) | Fonte |
|---------|-----------|---------------------------|-------|
| **KS OOT** | Kolmogorov-Smirnov — capacidade de separação bom/mau | 34.39% | `metricas/metricas_{ts}.json` → `ks_oot` |
| **AUC OOT** | Area Under ROC Curve — discriminação geral | 0.7327 | `metricas/metricas_{ts}.json` → `auc_oot` |
| **GINI OOT** | Coeficiente de Gini (2×AUC − 1) | 46.54% | `metricas/metricas_{ts}.json` → `gini_oot` |
| **Gap vs Benchmark** | KS OOT − Benchmark Claro (33.10%) | +1.29 p.p. | `metricas/metricas_{ts}.json` → `gap_benchmark` |
| **Overfitting Gap** | KS Treino − KS OOT | 3.73 p.p. | Calculado: `ks_train` − `ks_oot` |

### 1.2 Métricas de Estabilidade

| Métrica | Descrição | Threshold Aceitável |
|---------|-----------|---------------------|
| **PSI (Population Stability Index)** | Mede drift na distribuição do score entre períodos | PSI < 0.10 (estável), 0.10–0.25 (atenção), > 0.25 (ação) |
| **Feature Drift** | Mudança na distribuição das top features (KL divergence) | KL < 0.05 por feature |
| **N Features Selecionadas** | Quantidade de features com IV >= 0.01 | Variação < 10% entre runs (±26 features) |
| **Taxa de Cobertura** | % de registros com score válido (não-nulo) | > 95% |

### 1.3 Métricas de Negócio

| Métrica | Descrição | Baseline |
|---------|-----------|----------|
| **Taxa FPD Aprovados** | % de default entre clientes aprovados pelo modelo | 15.26% (taxa 80%) |
| **Swap-out Captura** | Maus capturados que o bureau não pegava | 69K clientes (FPD 41.8%) |
| **Swap-in Recuperação** | Bons recuperados que o bureau recusava | 69K clientes (FPD 23.6%) |
| **Redução FPD** | Queda na taxa FPD dos aprovados vs bureau puro | −10.8% (taxa 80%) |

---

## 2. Periodicidade de Monitoramento

```
┌─────────────────────────────────────────────────────────────────────┐
│                    CICLO DE MONITORAMENTO                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  MENSAL (Scoring Batch)                                             │
│  ├── Pipeline ETL (21 Data Flow apps)                               │
│  ├── Scoring com PKL existente (train-or-load)                      │
│  ├── Geração de métricas (JSON) + predições (Parquet)               │
│  └── Verificação automática: KS, AUC, PSI, n_features              │
│                                                                     │
│  TRIMESTRAL (Análise Aprofundada)                                   │
│  ├── Análise Swap-in/Swap-out atualizada                            │
│  ├── Feature drift (distribuição das top 20 features)               │
│  ├── Comparação de métricas entre últimos 3 runs                    │
│  └── Relatório de estabilidade para stakeholders                    │
│                                                                     │
│  SEMESTRAL (Retreino Programado)                                    │
│  ├── Retreino obrigatório (deletar PKL → treinar do zero)           │
│  ├── Comparação novo modelo vs modelo anterior                      │
│  ├── Validação de anti-leakage e gates                              │
│  └── Decisão: promover novo modelo ou rollback                      │
│                                                                     │
│  SOB DEMANDA (Eventos de Negócio)                                   │
│  ├── Mudança de política da Claro (critérios, planos, público)      │
│  ├── Nova fonte de dados disponível                                 │
│  └── Incidente operacional (falha no pipeline, dados corrompidos)   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Calendário Anual

| Mês | Ação | Responsável |
|-----|------|-------------|
| **Todo mês** | Scoring batch + verificação de métricas | Pipeline automático (Airflow) |
| **Mar, Jun, Set, Dez** | Análise trimestral (swap, drift, relatório) | Cientista de dados |
| **Jun, Dez** | Retreino programado + validação | Cientista de dados + Líder técnico |
| **Quando necessário** | Retreino emergencial (KS caiu > 2 p.p.) | Cientista de dados |

---

## 3. Critérios Objetivos para Re-treinamento

### 3.1 Gatilhos Automáticos

| # | Critério | Threshold | Ação | Urgência |
|---|----------|-----------|------|----------|
| 1 | **KS OOT cai > 2 p.p.** | KS < 32.39% | Retreino emergencial | ALTA |
| 2 | **KS OOT abaixo do benchmark** | KS < 33.10% | Retreino + investigação de causa | ALTA |
| 3 | **PSI > 0.25** | Drift severo na distribuição do score | Investigar + retreinar se confirmado | ALTA |
| 4 | **Gap Overfitting > 8 p.p.** | KS Treino − KS OOT > 8 p.p. | Revisar hiperparâmetros + retreinar | MÉDIA |
| 5 | **PSI entre 0.10 e 0.25** | Drift moderado | Monitorar próximo mês; retreinar se persistir | MÉDIA |
| 6 | **N features muda > 20%** | < 209 ou > 313 features selecionadas | Investigar data drift nas features | MÉDIA |
| 7 | **6 meses sem retreino** | Tempo desde último treino > 180 dias | Retreino programado (governança) | BAIXA |

### 3.2 Fluxo de Decisão de Retreino

```
                    Novo run mensal (scoring)
                           │
                    Gerar métricas JSON
                           │
                   ┌───────▼───────┐
                   │  KS OOT caiu  │
                   │  > 2 p.p. ?   │
                   └───┬───────┬───┘
                   SIM │       │ NÃO
                       │       │
               ┌───────▼──┐  ┌─▼──────────┐
               │ ALERTA   │  │ PSI > 0.25 │
               │ VERMELHO │  │    ?        │
               │ Retreinar│  └──┬──────┬───┘
               │ IMEDIATO │  SIM│      │NÃO
               └──────────┘     │      │
                       ┌────────▼┐  ┌──▼──────────┐
                       │ ALERTA  │  │ 6 meses sem │
                       │ AMARELO │  │ retreino?    │
                       │ Investig│  └──┬───────┬───┘
                       └─────────┘  SIM│       │NÃO
                                       │       │
                              ┌────────▼┐  ┌──▼────────┐
                              │ Retreino│  │ SEMÁFORO  │
                              │ Program.│  │ VERDE     │
                              └─────────┘  │ Continuar │
                                           └───────────┘
```

### 3.3 Procedimento de Retreino

```bash
# 1. Backup do modelo atual (executar LOCAL ou VM)
oci os object copy -bn hackathon-2025-models \
  --source-object-name pkl/modelo_fpd.pkl \
  --destination-bucket hackathon-2025-models \
  --destination-object-name pkl/modelo_fpd_backup_$(date +%Y%m%d).pkl

# 2. Deletar PKL atual para forçar retreino (executar LOCAL ou VM)
oci os object delete -bn hackathon-2025-models \
  --object-name pkl/modelo_fpd.pkl --force

# 3. Trigger do pipeline (executar na VM AIRFLOW, dentro do container)
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_modelo_qualificacao

# 4. Após conclusão: comparar métricas novo vs anterior (executar LOCAL)
# Baixar JSONs e comparar ks_oot, auc_oot, n_features
```

### 3.4 Critério de Rollback

Se o novo modelo apresentar **KS OOT inferior ao anterior**:

```bash
# Restaurar backup (executar LOCAL ou VM)
oci os object copy -bn hackathon-2025-models \
  --source-object-name pkl/modelo_fpd_backup_YYYYMMDD.pkl \
  --destination-bucket hackathon-2025-models \
  --destination-object-name pkl/modelo_fpd.pkl
```

**Regra:** Só promover novo modelo se `KS_novo >= KS_anterior - 0.5 p.p.` (tolerância de 0.5 p.p. para variação amostral).

---

## 4. Mecanismos de Alerta para Detecção de Degradação

### 4.1 Sistema de Semáforo

| Semáforo | Condição | Ação | SLA |
|----------|----------|------|-----|
| **VERDE** | KS OOT >= 32.39% E PSI < 0.10 E gap_benchmark >= 0 | Operação normal. Nenhuma ação necessária | — |
| **AMARELO** | KS OOT entre 31.39% e 32.39% OU PSI entre 0.10 e 0.25 OU gap_benchmark < 0 | Investigar causa. Preparar retreino se persistir no próximo mês | 15 dias |
| **VERMELHO** | KS OOT < 31.39% (queda > 3 p.p.) OU PSI > 0.25 OU KS OOT < benchmark por 2 meses consecutivos | Retreino emergencial. Escalar para líder técnico | 5 dias úteis |

### 4.2 Verificação Automatizada Pós-Scoring

O script `modelo_qualificacao.py` já salva métricas em JSON a cada execução. A verificação pode ser implementada como task adicional na DAG do Airflow:

```python
# Pseudocódigo — task de verificação pós-scoring
def verificar_metricas(**context):
    """Task Airflow: verifica métricas e gera alerta."""
    import json

    # Carregar métricas do run atual
    metricas = json.load(open(f"metricas/metricas_{timestamp}.json"))

    ks_oot = metricas["ks_oot"]
    gap = metricas["gap_benchmark"]
    n_features = metricas["n_features"]

    # Semáforo
    if ks_oot < 0.3139:
        status = "VERMELHO"
        msg = f"KS OOT = {ks_oot:.2%} — ABAIXO DO LIMIAR CRÍTICO (31.39%)"
    elif ks_oot < 0.3239 or gap < 0:
        status = "AMARELO"
        msg = f"KS OOT = {ks_oot:.2%} — ATENÇÃO (gap benchmark = {gap:+.2%})"
    else:
        status = "VERDE"
        msg = f"KS OOT = {ks_oot:.2%} — MODELO ESTÁVEL (gap = {gap:+.2%})"

    print(f"[{status}] {msg}")

    # Alertas adicionais
    if n_features < 209 or n_features > 313:
        print(f"[AMARELO] N features = {n_features} — variação > 20% do baseline (261)")

    return status
```

### 4.3 Matriz de Escalação

| Nível | Quem é Notificado | Canal | Quando |
|-------|-------------------|-------|--------|
| **VERDE** | Ninguém (log automático) | Logs Airflow | Todo scoring |
| **AMARELO** | Cientista de dados | E-mail / Slack | Quando threshold atingido |
| **VERMELHO** | Cientista + Líder técnico + Gerente | E-mail + reunião | Imediatamente |
| **Retreino** | Toda equipe de Data Science | Reunião de validação | Pré e pós retreino |

### 4.4 Indicadores de Data Drift

| Feature | Método de Detecção | Frequência |
|---------|-------------------|------------|
| `score_01_adj` (Bureau) | PSI entre safras consecutivas | Mensal |
| `score_02_adj` (Bureau) | PSI entre safras consecutivas | Mensal |
| `freq_sos_m1` (Recarga SOS) | Média + desvio padrão vs baseline | Mensal |
| `ticket_medio_m3` (Recarga) | Distribuição (percentis 25/50/75) | Mensal |
| `qtd_pagamentos_m1` (Pagamento) | Cobertura (% não-nulo) | Mensal |
| `pct_aging_90_plus_m1` (Atraso) | Proporção vs baseline | Mensal |

**Regra de drift por feature:** Se PSI de qualquer feature do top 10 superar 0.20 por 2 meses consecutivos, investigar a fonte de dados correspondente.

---

## 5. Fluxo Completo de Monitoramento

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLUXO DE MONITORAMENTO MENSAL                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐    │
│  │ Pipeline ETL │────▶│ DAG Modelo   │────▶│ Artefatos Gerados    │    │
│  │ 21 Data Flow │     │ Start→Score  │     │ • metricas_{ts}.json │    │
│  │ apps         │     │ →Stop VM     │     │ • predicoes_{ts}.pqt │    │
│  └──────────────┘     └──────────────┘     │ • features_{ts}.txt  │    │
│                                             └──────────┬───────────┘    │
│                                                        │                │
│                                             ┌──────────▼───────────┐    │
│                                             │ VERIFICAÇÃO AUTO     │    │
│                                             │ • KS OOT vs baseline │    │
│                                             │ • PSI do score       │    │
│                                             │ • N features         │    │
│                                             │ • Gap benchmark      │    │
│                                             └──────────┬───────────┘    │
│                                                        │                │
│                                    ┌───────────────────┼───────────┐    │
│                                    │                   │           │    │
│                             ┌──────▼────┐     ┌───────▼──┐  ┌────▼──┐ │
│                             │  VERDE    │     │ AMARELO  │  │VERMEL.│ │
│                             │  OK       │     │ Atenção  │  │Ação!  │ │
│                             │  (log)    │     │ (e-mail) │  │(escal)│ │
│                             └───────────┘     └──────────┘  └───────┘ │
│                                                                         │
│  TRIMESTRAL ────────────────────────────────────────────────────────    │
│  │                                                                      │
│  ├── Swap analysis atualizado (bureau vs modelo)                        │
│  ├── Relatório de drift (top 20 features)                               │
│  └── Apresentação para stakeholders                                     │
│                                                                         │
│  SEMESTRAL ─────────────────────────────────────────────────────────    │
│  │                                                                      │
│  ├── Retreino programado (backup PKL → deletar → treinar)               │
│  ├── Validação: novo KS >= anterior - 0.5 p.p.?                        │
│  ├── Se sim: promover novo modelo                                       │
│  └── Se não: rollback para PKL anterior                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Dashboard de Acompanhamento Mensal

### Tabela de KPIs — Exemplo de Acompanhamento

| Mês | KS OOT | AUC OOT | PSI | N Features | Gap Benchmark | Semáforo | Ação |
|-----|--------|---------|-----|------------|---------------|----------|------|
| Mar/2026 | 34.39% | 0.7327 | — | 261 | +1.29 p.p. | VERDE | Baseline |
| Abr/2026 | — | — | — | — | — | — | Scoring #2 |
| Mai/2026 | — | — | — | — | — | — | Scoring #3 |
| Jun/2026 | — | — | — | — | — | — | Trimestral + Semestral |
| Jul/2026 | — | — | — | — | — | — | Scoring #5 |
| Ago/2026 | — | — | — | — | — | — | Scoring #6 |
| Set/2026 | — | — | — | — | — | — | Trimestral |
| Out/2026 | — | — | — | — | — | — | Scoring #8 |
| Nov/2026 | — | — | — | — | — | — | Scoring #9 |
| Dez/2026 | — | — | — | — | — | — | Trimestral + Semestral |

### Cálculo do PSI (Population Stability Index)

```
PSI = Σ (% score_atual_i − % score_referência_i) × ln(% score_atual_i / % score_referência_i)
```

Onde `i` são os decis do score. O PSI compara a distribuição do score no mês atual vs o mês de referência (treino).

| PSI | Interpretação |
|-----|---------------|
| < 0.10 | Distribuição estável — nenhuma ação |
| 0.10 – 0.25 | Mudança moderada — investigar causa |
| > 0.25 | Mudança significativa — retreinar modelo |

---

## 7. Governança do Modelo

### 7.1 Papéis e Responsabilidades

| Papel | Responsabilidade | Frequência |
|-------|-----------------|------------|
| **Pipeline Automático (Airflow)** | Executar scoring, gerar métricas, verificação básica | Mensal |
| **Cientista de Dados** | Analisar métricas, investigar alertas, executar retreino | Mensal + sob demanda |
| **Líder Técnico** | Aprovar retreino, validar novo modelo, escalar problemas | Trimestral + alertas |
| **Gerente / PO** | Revisar impacto de negócio, aprovar mudanças de threshold | Trimestral |

### 7.2 Documentação Obrigatória

Cada retreino deve gerar:

| Documento | Conteúdo | Armazenamento |
|-----------|----------|---------------|
| **Métricas antes/depois** | KS, AUC, GINI do modelo antigo vs novo | `metricas/` (JSON automático) |
| **Justificativa do retreino** | Motivo (queda de KS, drift, programado) | Registro no Airflow (log da DAG) |
| **Lista de features** | Features selecionadas no novo modelo | `metricas/features_{ts}.txt` (automático) |
| **Validação anti-leakage** | Confirmação que FPD_INT e FLAG_INSTALACAO não estão nas features | Checklist manual |
| **Decisão de promoção** | Aprovação ou rollback, com responsável | Ata de reunião |

### 7.3 Histórico de Versões do Modelo

| Versão | Data | KS OOT | Motivo | Status |
|--------|------|--------|--------|--------|
| v1.0 | Mar/2026 | 34.39% | Treino inicial (OCI VM) | **Em produção** |
| v0.9 | Fev/2026 | 33.94% | Treino Databricks (referência) | Arquivado |
| v1.1 | Jun/2026 (previsto) | — | Retreino semestral programado | Pendente |

### 7.4 Artefatos de Referência

| Artefato | Path | Descrição |
|----------|------|-----------|
| Modelo PKL | `hackathon-2025-models/pkl/modelo_fpd.pkl` | Modelo LightGBM serializado |
| Métricas JSON | `hackathon-2025-models/metricas/metricas_{ts}.json` | KS, AUC, GINI, gap, n_features |
| Predições OOT | `hackathon-2025-models/resultados_modelo/predicoes_oot_{ts}.parquet` | Score + decil por CPF |
| Feature list | `hackathon-2025-models/metricas/features_{ts}.txt` | Features utilizadas |
| KS incremental | `hackathon-2025-models/metricas/ks_incremental_{ts}.json` | KS por versão da ABT |
| Swap analysis | `hackathon-2025-models/metricas/swap_analysis_{ts}.json` | Impacto de negócio (swap-in/out) |

---

## 8. Resumo Executivo

### Os 4 Pilares do Monitoramento

| Pilar | Implementação | Frequência |
|-------|---------------|------------|
| **Métricas-chave** | KS, AUC, GINI, PSI, feature drift, gap vs benchmark | Automática (cada scoring) |
| **Periodicidade** | Mensal (scoring), trimestral (análise aprofundada), semestral (retreino) | Calendário fixo |
| **Critérios de retreino** | KS cai > 2 p.p., PSI > 0.25, 6 meses sem retreino, mudança de política | Thresholds objetivos |
| **Alertas de degradação** | Semáforo (verde/amarelo/vermelho), escalação por nível, verificação automatizada | Pós-scoring + notificação |

### Compromisso de Estabilidade

O modelo atual supera o benchmark em **+1.29 p.p.** (KS 34.39% vs 33.10%). Com o plano de monitoramento descrito, o compromisso é:

- **Manter KS OOT acima de 32.39%** (margem de 2 p.p. antes de retreino emergencial)
- **Manter KS OOT acima do benchmark** (33.10%) — se abaixo por 2 meses, retreino obrigatório
- **Retreino semestral** mesmo sem degradação (boas práticas de governança em crédito)
- **Rastreabilidade completa** — todo run gera métricas JSON, predições Parquet e lista de features

---

*Documento gerado em Março/2026 — Hackathon PodAcademy 2025*
*Modelo: LightGBM FPD | Infraestrutura: OCI (VM E5.Flex + Airflow + Object Storage)*
