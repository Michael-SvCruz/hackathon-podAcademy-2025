# Scoring API em Tempo Real — Proposta, Viabilidade e Roadmap

## Contexto

A arquitetura OCI do projeto contempla dois cenarios operacionais: **Batch Pipeline** (mensal, implementado) e **Scoring API 24/7** (tempo real, proposto). Este documento detalha a proposta da API REST, justifica por que nao foi implementada nesta fase, e apresenta o roadmap para viabiliza-la.

---

## 1. O que E a Scoring API

### 1.1 Definicao

Uma API REST que recebe os dados de um cliente (CPF + features) via HTTP POST e retorna, em milissegundos, o score de risco de FPD e a decisao de credito (aprovar/negar).

### 1.2 Caso de Uso

```
Claro (sistema de vendas)                    Scoring API (OCI)
━━━━━━━━━━━━━━━━━━━━━━━━                    ━━━━━━━━━━━━━━━━━━

Vendedor inicia migracao
pre → controle             ──POST /v1/score──▶  Recebe CPF + features
                                                Carrega modelo LightGBM
                           ◀── Response (~50ms)  Retorna score + decisao
Exibe decisao ao vendedor
```

**Diferenca fundamental do batch:**
- **Batch (atual):** Processa 851K clientes de uma vez, 1x por mes. Resultado em arquivo parquet.
- **API (proposta):** Processa 1 cliente por vez, sob demanda, 24/7. Resultado em JSON instantaneo.

### 1.3 Endpoints Planejados

| Endpoint | Metodo | Funcao | Latencia Esperada |
|----------|--------|--------|-------------------|
| `/v1/score` | POST | Calcula score + decisao | ~50ms |
| `/health` | GET | Health check (Load Balancer) | ~5ms |
| `/metrics` | GET | Metricas Prometheus | ~10ms |

### 1.4 Exemplo de Request/Response

**Request:**
```json
POST /v1/score
Content-Type: application/json

{
  "cpf": "12345678900",
  "score_01": 650,
  "score_02": 720,
  "freq_sos_m1": 2.0,
  "ticket_medio_m1": 25.50,
  "pct_sos_sobre_credito_m1": 8.3,
  "dias_max_entre_recargas_m1": 12,
  "pct_pagamentos_com_juros_m1": 0.0,
  "pct_aging_90_plus_m1": 0.0
}
```

**Response:**
```json
{
  "cpf": "12345678900",
  "score": 850,
  "probability": 0.15,
  "risk_class": "LOW",
  "decision": "APPROVED",
  "model_version": "20260307_1850",
  "timestamp": "2026-03-12T10:30:00Z"
}
```

### 1.5 Classificacao de Risco

| Probabilidade FPD | Classe | Decisao | Score (0-1000) |
|--------------------|--------|---------|----------------|
| <= 0.15 | LOW | APPROVED | 850-1000 |
| <= 0.21 | MEDIUM | APPROVED | 790-849 |
| <= 0.35 | HIGH | DENIED | 650-789 |
| > 0.35 | VERY_HIGH | DENIED | 0-649 |

> Formula do score: `score = int((1 - probability) * 1000)`

---

## 2. Como Seria Implementada

### 2.1 Arquitetura de Rede

```
Internet (Claro)
    │
    ▼
Load Balancer (public subnet, porta 443, SSL/TLS)
    │
    ▼
VM Scoring API (private subnet 10.0.10.0/24, porta 8000)
    ├── FastAPI + Uvicorn (ASGI server)
    ├── LightGBM (modelo em memoria, ~50 MB)
    └── Acesso ao Object Storage via Service Gateway
```

A VM fica na **private subnet** (sem IP publico), acessivel apenas via Load Balancer. O Service Gateway permite download do modelo do Object Storage sem trafego pela internet.

### 2.2 Stack Tecnologica

| Componente | Tecnologia | Justificativa |
|------------|-----------|---------------|
| **Framework** | FastAPI | Async nativo, documentacao OpenAPI automatica, tipagem |
| **Server** | Uvicorn (ASGI) | Alta performance, suporte async |
| **Modelo** | LightGBM `.txt` (booster) | Mais leve que PKL, carregamento rapido |
| **VM** | E4.Flex (1 OCPU, 8 GB) | Suficiente para scoring single-request |
| **Processo** | systemd service | Auto-restart, logging journald |
| **TLS** | Load Balancer (SSL termination) | Certificado gerenciado pela OCI |

### 2.3 Estrutura da Aplicacao

```
/app/
├── main.py                 # FastAPI application
├── models/
│   └── modelo_fpd.txt      # LightGBM booster (atualizado via deploy)
├── features/
│   └── transformer.py      # Validacao e transformacao de features
├── config.py               # Thresholds, versoes, feature list
├── requirements.txt        # fastapi, uvicorn, lightgbm, numpy
└── tests/
    └── test_scoring.py     # Testes unitarios
```

### 2.4 Codigo Conceitual

```python
from fastapi import FastAPI, HTTPException
import lightgbm as lgb
import numpy as np
import pandas as pd

app = FastAPI(title="FPD Scoring API", version="1.0.0")

# Modelo carregado uma vez na inicializacao (~50 MB em memoria)
MODEL = lgb.Booster(model_file="/app/models/modelo_fpd.txt")
FEATURES = open("/app/features/feature_list.txt").read().strip().split("\n")

@app.post("/v1/score")
def score(payload: dict):
    # Montar DataFrame com as 261 features (missing = -999)
    row = {f: payload.get(f, -999) for f in FEATURES}
    df = pd.DataFrame([row])

    # Predicao
    proba = MODEL.predict(df)[0]
    score_val = int((1 - proba) * 1000)

    # Classificacao
    if proba <= 0.15:
        risk, decision = "LOW", "APPROVED"
    elif proba <= 0.21:
        risk, decision = "MEDIUM", "APPROVED"
    elif proba <= 0.35:
        risk, decision = "HIGH", "DENIED"
    else:
        risk, decision = "VERY_HIGH", "DENIED"

    return {
        "cpf": payload.get("cpf"),
        "score": score_val,
        "probability": round(float(proba), 4),
        "risk_class": risk,
        "decision": decision,
    }

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": MODEL is not None}
```

### 2.5 Fluxo de Atualizacao do Modelo

```
Pipeline Batch (mensal)
    ↓
Nova ABT → Retreino (se KS degradou > 2 p.p.)
    ↓
Validacao: KS OOT >= 33%?
    ├── Sim → Upload modelo_fpd.txt para Object Storage
    └── Nao → Manter modelo anterior, investigar
    ↓
VM Scoring API: download novo modelo
    ↓
Swap atomico + restart systemd (~10 segundos de indisponibilidade)
    ↓
Health check: Load Balancer verifica /health
```

---

## 3. Recursos OCI Necessarios

### 3.1 Recursos Adicionais (alem do que ja existe)

| Recurso | Especificacao | Custo/Mes | Status |
|---------|---------------|-----------|--------|
| **VM Scoring API** | E4.Flex, 1 OCPU, 8 GB | ~$35 | Novo (always-on) |
| **Load Balancer** | Flexible, 10 Mbps | ~$20 | Novo |
| Security List (porta 8000) | Ingress from LB subnet | $0 | Config |
| DNS / Certificado SSL | OCI Certificates | $0 | Config |
| **Subtotal API** | | **~$55/mes** | |

### 3.2 Recursos Existentes (reutilizados)

| Recurso | Uso na API | Ja Provisionado |
|---------|-----------|-----------------|
| VCN + Subnets | Private subnet para VM | Sim (Terraform) |
| Service Gateway | Download modelo do Object Storage | Sim |
| NAT Gateway | Acesso a PyPI (pip install) | Sim |
| Object Storage (models bucket) | Armazenamento do modelo | Sim |
| Dynamic Group + Policy | Instance Principal | Adaptar (nova VM) |

### 3.3 Custo Total Operacional (com API)

| Componente | Custo/Mes |
|------------|-----------|
| Scoring API (VM + LB) | $55 |
| Batch Pipeline (Data Flow 1x/mes) | $242 |
| VM Modelo Batch (Start/Stop) | $40 |
| Storage + Airflow + Network | $100 |
| **Total** | **~$437/mes** |

vs Custo atual sem API: **~$350/mes** (diferenca: +$87/mes)

---

## 4. Por que Nao Foi Implementada Agora

### 4.1 Razoes Principais

| # | Razao | Detalhe |
|---|-------|---------|
| 1 | **Tempo** | A defesa final e amanha (12/03/2026). Implementar, testar e validar uma API REST com Load Balancer, SSL e monitoramento exigiria 3-5 dias adicionais |
| 2 | **Caso de uso atual e batch** | O objetivo do hackathon e qualificar a base de clientes pre-pagos para migracao. Isso e naturalmente um processo batch mensal, nao real-time |
| 3 | **Feature Store inexistente** | A API precisa receber as 261 features pre-calculadas. Hoje nao existe um Feature Store que sirva features comportamentais (recarga, pagamento, atraso) em tempo real para um CPF especifico |
| 4 | **Prioridade no pipeline completo** | O esforco foi direcionado para entregar o pipeline end-to-end funcional na OCI (8 fases), garantindo que o batch scoring rodasse com KS = 34.39% |
| 5 | **Custo vs beneficio** | +$87/mes para uma funcionalidade que nao e necessaria na operacao mensal atual |

### 4.2 O Que Seria Necessario Para Viabilizar

| Requisito | Complexidade | Tempo Estimado |
|-----------|-------------|----------------|
| Desenvolver a FastAPI + testes | Media | 1-2 dias |
| Provisionar VM + Load Balancer (Terraform) | Baixa | 0.5 dia |
| Configurar SSL + DNS | Baixa | 0.5 dia |
| **Implementar Feature Store** | **Alta** | **3-5 dias** |
| Testes de carga e latencia | Media | 1 dia |
| Monitoramento (Prometheus + alertas) | Media | 1 dia |
| **Total** | | **7-10 dias** |

> O **gargalo principal** e o Feature Store. Sem ele, quem invoca a API precisa enviar as 261 features pre-calculadas no payload — o que transfere a complexidade de feature engineering para o sistema chamador (Claro).

---

## 5. Feature Store — O Desafio Central

### 5.1 O Problema

As 261 features do modelo sao calculadas a partir de dados historicos de 3 fontes:

| Fonte | Features | Janela | Problema para Real-Time |
|-------|----------|--------|------------------------|
| **Recarga** | 74 | M1/M3/M6 | Precisa agregar ultimos 1-6 meses de eventos |
| **Pagamento** | 56 | M1/M3/M6 | Precisa agregar historico de faturas |
| **Atraso** | 19 | M1/M3/M6 | Precisa snapshot atual de aging |
| **Scores** | 4 | Pontual | Disponivel em tempo real (bureau) |
| **Telco** | 69 | Pontual | Disponivel no cadastro |
| **Cadastro** | 5 | Pontual | Disponivel no cadastro |

**Scores + Telco + Cadastro** (78 features) sao faceis de servir em tempo real — sao valores pontuais do cliente.

**Recarga + Pagamento + Atraso** (149 features) exigem **agregacao de dados historicos**, que hoje so existem nas tabelas Gold do pipeline batch.

### 5.2 Solucoes Possíveis

| Opcao | Descricao | Complexidade | Latencia |
|-------|-----------|-------------|----------|
| **A. Pre-calculo mensal** | Rodar o pipeline batch e salvar as features pre-calculadas por CPF numa tabela "Feature Store". A API consulta essa tabela. | Baixa | ~100ms (lookup) |
| **B. Calculo on-the-fly** | A API consulta os dados brutos (recarga, pagamento, atraso) e calcula as features em tempo real. | Alta | ~500ms-2s |
| **C. Hibrido** | Features M3/M6 pre-calculadas (mudam pouco). Features M1 calculadas on-the-fly (dados do ultimo mes). | Media | ~200ms |

> **Recomendacao:** Opcao A (pre-calculo mensal). E consistente com o pipeline batch ja implementado e adiciona apenas um step final: salvar as 261 features por CPF numa tabela indexada.

### 5.3 Implementacao da Opcao A

```
Pipeline Batch Mensal (ja implementado)
    ↓
ABT v6 com 614 colunas × 3.8M registros
    ↓
[NOVO] Extrair 261 features selecionadas + NUM_CPF
    ↓
[NOVO] Salvar em tabela "feature_store_fpd" (Object Storage ou NoSQL)
    ↓
API POST /v1/score recebe apenas CPF
    ↓
Lookup na feature_store_fpd → 261 features
    ↓
model.predict() → score + decisao
```

**Limitacao:** As features refletem o ultimo batch (ate 30 dias defasadas). Para a maioria dos casos de migracao pre→controle, isso e aceitavel.

---

## 6. Alternativas ao Scoring Real-Time

### 6.1 Near Real-Time (Recomendado para Proxima Fase)

Em vez de API sincrona, usar um **fluxo assincrono**:

```
Claro envia lote de CPFs    ──▶  Fila (OCI Streaming/Queue)
                                      ↓
                                 Worker processa batch mini (~5 min)
                                      ↓
Claro recebe resultados     ◀──  Callback/Webhook ou polling
```

- **Latencia:** 5-15 minutos (vs 50ms da API sincrona)
- **Vantagem:** Reutiliza o pipeline batch existente, sem Feature Store
- **Custo:** Mesmo do batch (~$0 adicional)

### 6.2 Scoring Pre-Calculado (Mais Simples)

Gerar scores para **toda a base elegivel** no batch mensal e disponibilizar numa tabela de consulta:

```sql
-- Tabela de consulta (gerada pelo batch mensal)
SELECT num_cpf, score_fpd, decil, risk_class, decision
FROM hackathon_2025.default.predicoes_oot_latest
WHERE num_cpf = '12345678900'
```

- **Latencia:** ~10ms (lookup simples)
- **Vantagem:** Nenhuma infra adicional, ja temos as predicoes
- **Limitacao:** So tem score de quem estava na ABT do ultimo batch

---

## 7. Comparacao: Batch vs API vs Alternativas

| Aspecto | Batch (Atual) | API REST (Proposta) | Near Real-Time | Pre-Calculado |
|---------|---------------|---------------------|----------------|---------------|
| **Implementado** | Sim | Nao | Nao | Parcial |
| **Latencia** | Horas | ~50ms | 5-15 min | ~10ms |
| **Complexidade** | Baixa | Alta (Feature Store) | Media | Baixa |
| **Custo adicional** | $0 | +$87/mes | ~$0 | ~$0 |
| **Cobertura** | Toda a base | Por CPF (on-demand) | Por lote | Toda a base |
| **Features atualizadas** | Sim (recalcula tudo) | Depende do Feature Store | Sim | Ultimo batch |
| **Melhor para** | Campanhas mensais | Vendas em tempo real | Filas de aprovacao | Consulta rapida |

---

## 8. Roadmap de Implementacao

### Fase 1 — Score Pre-Calculado (1-2 dias)
- Adicionar step no pipeline batch: salvar predicoes em tabela de consulta
- Disponibilizar via SQL ou API simples de lookup
- **Valor:** Claro pode consultar score de qualquer CPF da base

### Fase 2 — Feature Store Batch (3-5 dias)
- Criar tabela `feature_store_fpd` com as 261 features por CPF
- Atualizar mensalmente junto com o pipeline batch
- **Valor:** Base para a API REST

### Fase 3 — API REST + Load Balancer (3-5 dias)
- FastAPI + Uvicorn na VM OCI
- Load Balancer com SSL
- Terraform para provisionamento
- **Valor:** Scoring em tempo real para vendas

### Fase 4 — Monitoramento e Producao (2-3 dias)
- Prometheus + Grafana para metricas da API
- Alertas de latencia e erro
- Testes de carga
- **Valor:** Operacao 24/7 com SLA

**Total do roadmap: 10-15 dias uteis.**

---

## 9. Resposta Sugerida Para a Banca

> "A arquitetura contempla a Scoring API como evolucao natural do pipeline batch. Nao implementamos nesta fase porque o caso de uso primario — qualificar a base para campanhas de migracao — e inerentemente batch e mensal. O pipeline completo (8 fases, KS 34.39%) foi a prioridade.

> Para viabilizar a API, o principal desafio e o Feature Store: as 261 features comportamentais (recarga, pagamento, atraso) precisam ser servidas em tempo real por CPF. A solucao mais pragmatica e pre-calcular essas features no batch mensal e disponibiliza-las numa tabela de lookup, que a API consultaria em ~100ms.

> Toda a infraestrutura de rede (VCN, subnets, Service Gateway) ja esta provisionada via Terraform. O custo incremental seria de ~$87/mes (VM always-on + Load Balancer). O roadmap completo leva 10-15 dias uteis."

---

## Referencias

| Documento | Localizacao |
|-----------|-------------|
| Arquitetura Operacional (cenarios completos) | `docs/architecture/OCI_OPERACIONAL.md` |
| Guia Arquitetura OCI (FAQ, glossario) | `docs/architecture/GUIA_ARQUITETURA_OCI.md` |
| Arquitetura OCI (rede, compute) | `docs/architecture/OCI_ARCHITECTURE.md` |
| Rotinas Modelo Scoring | `mig_oci/docs/ROTINAS_MODELO_SCORING.md` |
| Monitoramento (Entregavel F) | `banca_final/PLANO_MONITORAMENTO_MODELO.md` |
| Documentacao Modelo (Entregavel D) | `banca_final/DOCUMENTACAO_MODELO.md` |
