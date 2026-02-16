# OCI Arquitetura Operacional

> **Documento complementar a:** `OCI_ARCHITECTURE.md`
>
> Este documento detalha os dois cenários operacionais do sistema em produção:
> 1. **Scoring API (Real-time)** - Disponível 24/7 para requisições da Claro
> 2. **Batch Pipeline (Mensal)** - Processamento quando novos dados são entregues

---

## Índice

1. [Visão Geral dos Cenários](#1-visão-geral-dos-cenários)
2. [Cenário 1: Scoring API (24/7)](#2-cenário-1-scoring-api-247)
3. [Cenário 2: Batch Pipeline (Mensal)](#3-cenário-2-batch-pipeline-mensal)
4. [Arquitetura Híbrida Completa](#4-arquitetura-híbrida-completa)
5. [Custos Operacionais](#5-custos-operacionais)
6. [Fluxo de Atualização do Modelo](#6-fluxo-de-atualização-do-modelo)
7. [Monitoramento e Alertas](#7-monitoramento-e-alertas)
8. [Checklist de Deploy](#8-checklist-de-deploy)

---

## 1. Visão Geral dos Cenários

| Cenário | Frequência | Disponibilidade | Recurso Principal |
|---------|------------|-----------------|-------------------|
| **Scoring API** | Sob demanda | 24/7 | VM Always-On |
| **Batch Pipeline** | Mensal | Agendado/Event-driven | Data Flow (Spark) |

### Diferença Fundamental

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   SCORING API (Real-time)              BATCH PIPELINE (Mensal)         │
│   ━━━━━━━━━━━━━━━━━━━━━━━              ━━━━━━━━━━━━━━━━━━━━━━━          │
│                                                                         │
│   Claro envia CPF ──▶ Retorna score    Claro envia dados ──▶ Atualiza  │
│   em milissegundos                     ABT em horas                     │
│                                                                         │
│   ┌─────┐    ┌─────┐                   ┌─────┐    ┌─────────┐          │
│   │ API │───▶│Score│                   │Files│───▶│ Spark   │          │
│   │ 24/7│    │~50ms│                   │ New │    │ Process │          │
│   └─────┘    └─────┘                   └─────┘    └─────────┘          │
│                                                                         │
│   Latência: ~50-100ms                  Duração: 2-4 horas              │
│   Custo: ~$55/mês (fixo)               Custo: ~$240/execução           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Cenário 1: Scoring API (24/7)

### 2.1 Requisitos

- **Disponibilidade:** 24/7 (Claro pode chamar a qualquer momento)
- **Latência:** < 200ms por requisição
- **Throughput:** ~100-1000 req/min (estimado)
- **Modelo:** LightGBM carregado em memória

### 2.2 Opções de Infraestrutura

| Opção | Custo/Mês | Latência | Complexidade | Recomendação |
|-------|-----------|----------|--------------|--------------|
| **VM Always-On** | ~$35 | ~50ms | Baixa | **Recomendado** |
| Container Instance | ~$25 | ~100ms | Média | Alternativa |
| OCI Functions | Pay-per-call | 1-3s (cold) | Média | Não recomendado |
| OKE (Kubernetes) | ~$70+ | ~50ms | Alta | Overkill |

**Recomendação para Hackathon:** VM.Standard.E4.Flex (1 OCPU, 8 GB RAM)

### 2.3 Componentes da VM Scoring

```
VM Scoring API (private subnet)
│
├── /app/
│   ├── main.py                 # FastAPI application
│   ├── models/
│   │   └── modelo_fpd.txt      # LightGBM model (264 features)
│   ├── features/
│   │   └── transformer.py      # Feature engineering logic
│   ├── config/
│   │   └── settings.py         # Configurações (thresholds, etc.)
│   └── requirements.txt        # Dependencies
│
├── /etc/systemd/system/
│   └── scoring-api.service     # Systemd service (auto-restart)
│
└── Serviços rodando:
    ├── Uvicorn (ASGI server) - porta 8000
    ├── Python 3.9+
    └── LightGBM (modelo em memória ~50MB)
```

### 2.4 Arquitetura de Rede

```
                    Internet
                        │
                        ▼
┌───────────────────────────────────────────────────────────┐
│                      VCN (10.0.0.0/16)                    │
│                                                           │
│   ┌─────────────────────────────────────────────────┐    │
│   │           Public Subnet (10.0.1.0/24)           │    │
│   │                                                  │    │
│   │   ┌──────────────────────────────────────┐      │    │
│   │   │         Load Balancer (LB)           │      │    │
│   │   │         IP Público: X.X.X.X          │      │    │
│   │   │         Porta: 443 (HTTPS)           │      │    │
│   │   │         SSL Termination              │      │    │
│   │   └──────────────────┬───────────────────┘      │    │
│   │                      │                           │    │
│   └──────────────────────┼───────────────────────────┘    │
│                          │                                │
│   ┌──────────────────────┼───────────────────────────┐    │
│   │      Private Subnet (10.0.10.0/24)               │    │
│   │                      │                           │    │
│   │   ┌──────────────────▼───────────────────┐      │    │
│   │   │         VM Scoring API               │      │    │
│   │   │         IP Privado: 10.0.10.10       │      │    │
│   │   │         Porta: 8000                  │      │    │
│   │   │                                      │      │    │
│   │   │   ┌─────────────────────────────┐   │      │    │
│   │   │   │  FastAPI + LightGBM         │   │      │    │
│   │   │   │  - /v1/score (POST)         │   │      │    │
│   │   │   │  - /health (GET)            │   │      │    │
│   │   │   │  - /metrics (GET)           │   │      │    │
│   │   │   └─────────────────────────────┘   │      │    │
│   │   │                                      │      │    │
│   │   └──────────────────────────────────────┘      │    │
│   │                                                  │    │
│   └──────────────────────────────────────────────────┘    │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

### 2.5 Código da API

```python
# main.py - Scoring API
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import lightgbm as lgb
import numpy as np
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FPD Scoring API",
    description="API de scoring de risco de crédito - Hackathon PodAcademy 2025",
    version="1.0.0"
)

# ============================================================
# MODELO - Carregado uma única vez na inicialização
# ============================================================
MODEL_PATH = "/app/models/modelo_fpd.txt"
model = None

@app.on_event("startup")
async def load_model():
    global model
    logger.info(f"Carregando modelo de {MODEL_PATH}...")
    model = lgb.Booster(model_file=MODEL_PATH)
    logger.info(f"Modelo carregado com {model.num_feature()} features")

# ============================================================
# SCHEMAS
# ============================================================
class ScoreRequest(BaseModel):
    """Request para scoring de um cliente"""
    cpf: str
    # Features do bureau (Score_01, Score_02)
    score_01: float = None
    score_02: float = None
    # Features comportamentais (se disponíveis)
    freq_sos_m1: float = None
    ticket_medio_m1: float = None
    qtd_recargas_m1: int = None
    # ... outras features conforme necessidade

    class Config:
        schema_extra = {
            "example": {
                "cpf": "12345678901",
                "score_01": 650.0,
                "score_02": 720.0,
                "freq_sos_m1": 2.0,
                "ticket_medio_m1": 25.50
            }
        }

class ScoreResponse(BaseModel):
    """Response com score e decisão"""
    cpf: str
    score: int                    # Score 0-1000
    probability: float            # Probabilidade de FPD
    risk_class: str               # HIGH, MEDIUM, LOW
    decision: str                 # APPROVED, DENIED
    timestamp: str
    model_version: str = "v1.0.0"

class HealthResponse(BaseModel):
    """Response do health check"""
    status: str
    model_loaded: bool
    model_features: int
    timestamp: str

# ============================================================
# FEATURE ENGINEERING
# ============================================================
def transform_to_features(request: ScoreRequest) -> np.ndarray:
    """
    Transforma o request em vetor de features para o modelo.

    IMPORTANTE: A ordem das features deve ser EXATAMENTE igual
    à ordem usada no treinamento do modelo.
    """
    # Lista de features na ordem do modelo (264 features)
    # Carregar de arquivo ou hardcode conforme necessidade
    features = []

    # Score features
    features.append(request.score_01 if request.score_01 else 0)
    features.append(request.score_02 if request.score_02 else 0)

    # Recarga features
    features.append(request.freq_sos_m1 if request.freq_sos_m1 else 0)
    features.append(request.ticket_medio_m1 if request.ticket_medio_m1 else 0)
    features.append(request.qtd_recargas_m1 if request.qtd_recargas_m1 else 0)

    # ... completar com todas as 264 features
    # Para features não informadas, usar 0 ou valor default

    # Padding para completar 264 features (exemplo)
    while len(features) < 264:
        features.append(0)

    return np.array(features)

def classify_risk(probability: float) -> tuple:
    """
    Classifica o risco baseado na probabilidade de FPD.

    Thresholds baseados na análise do modelo:
    - FPD rate no dataset: 21.23%
    - Threshold de aprovação: <= 0.21
    """
    if probability <= 0.15:
        return "LOW", "APPROVED"
    elif probability <= 0.21:
        return "MEDIUM", "APPROVED"
    elif probability <= 0.35:
        return "HIGH", "DENIED"
    else:
        return "VERY_HIGH", "DENIED"

# ============================================================
# ENDPOINTS
# ============================================================
@app.post("/v1/score", response_model=ScoreResponse)
async def score_customer(request: ScoreRequest):
    """
    Endpoint principal de scoring.

    Recebe dados do cliente e retorna score de risco.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Modelo não carregado")

    try:
        # 1. Transformar input em features
        features = transform_to_features(request)

        # 2. Predição
        probability = model.predict([features])[0]

        # 3. Classificação
        risk_class, decision = classify_risk(probability)

        # 4. Score 0-1000 (inverso: menor = melhor)
        score = int((1 - probability) * 1000)

        logger.info(f"Scored CPF {request.cpf[:3]}***: score={score}, decision={decision}")

        return ScoreResponse(
            cpf=request.cpf,
            score=score,
            probability=round(probability, 4),
            risk_class=risk_class,
            decision=decision,
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erro no scoring: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro no scoring: {str(e)}")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check para Load Balancer e monitoramento."""
    return HealthResponse(
        status="healthy" if model else "unhealthy",
        model_loaded=model is not None,
        model_features=model.num_feature() if model else 0,
        timestamp=datetime.utcnow().isoformat()
    )

@app.get("/metrics")
async def metrics():
    """Métricas para monitoramento (Prometheus format)."""
    # Implementar métricas customizadas
    return {
        "requests_total": 0,  # Implementar contador
        "latency_avg_ms": 0,  # Implementar média
        "model_version": "v1.0.0"
    }

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

### 2.6 Systemd Service

```ini
# /etc/systemd/system/scoring-api.service
[Unit]
Description=FPD Scoring API
After=network.target

[Service]
Type=simple
User=opc
WorkingDirectory=/app
Environment="PATH=/app/venv/bin"
ExecStart=/app/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

**Comandos para gerenciar:**
```bash
# Iniciar serviço
sudo systemctl start scoring-api

# Verificar status
sudo systemctl status scoring-api

# Habilitar auto-start no boot
sudo systemctl enable scoring-api

# Ver logs
sudo journalctl -u scoring-api -f
```

### 2.7 Requirements

```txt
# requirements.txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
lightgbm==4.1.0
numpy==1.24.3
pydantic==2.5.2
python-multipart==0.0.6
```

---

## 3. Cenário 2: Batch Pipeline (Mensal)

### 3.1 Trigger de Execução

Duas opções para disparar o pipeline quando a Claro entrega novos dados:

#### Opção A: Event-Driven (Recomendado)

```
┌─────────────┐    Upload     ┌─────────────────┐
│    Claro    │──────────────▶│  Object Storage │
│   (SFTP)    │               │  landing-zone/  │
└─────────────┘               └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │   OCI Events    │
                              │  (file created) │
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │  OCI Functions  │
                              │ (trigger Airflow│
                              │     DAG)        │
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │    Airflow      │
                              │  DAG triggered  │
                              └─────────────────┘
```

#### Opção B: Scheduled (Mais Simples)

```python
# Airflow DAG - Executa todo dia 5 do mês às 02:00
schedule_interval = "0 2 5 * *"
```

### 3.2 Pipeline Batch Completo

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       BATCH PIPELINE (Mensal)                           │
│                                                                         │
│   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐            │
│   │ Landing │───▶│ Bronze  │───▶│ Silver  │───▶│  Gold   │            │
│   │  Zone   │    │  Layer  │    │  Layer  │    │  Layer  │            │
│   └─────────┘    └─────────┘    └─────────┘    └─────────┘            │
│       │              │              │              │                   │
│       │              │              │              │                   │
│   Parquet        + Metadata     Type cast      Features               │
│   raw files      + Timestamp    Dedupe         ABT v6                 │
│                                 Validate       614 cols               │
│                                                                         │
│   ─────────────────────────────────────────────────────────────────    │
│                                                                         │
│   Data Flow Jobs (Spark):                                              │
│   ┌──────────────────────────────────────────────────────────────┐    │
│   │ Job 1: Bronze Ingestion          │ 2 OCPU  │ 4 exec  │ 30min │    │
│   │ Job 2: Silver Transformation     │ 4 OCPU  │ 8 exec  │ 1-2h  │    │
│   │ Job 3: Gold Feature Generators   │ 4 OCPU  │ 16 exec │ 2-3h  │    │
│   │ Job 4: ABT Builder               │ 4 OCPU  │ 8 exec  │ 1-2h  │    │
│   └──────────────────────────────────────────────────────────────┘    │
│                                                                         │
│   Total: ~5-8 horas                                                    │
│   Custo: ~$240 por execução                                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Airflow DAG para Batch

```python
# dags/monthly_batch_pipeline.py
from airflow import DAG
from airflow.providers.oracle.operators.oci import OCIDataFlowOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['team@podacademy.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'monthly_fpd_pipeline',
    default_args=default_args,
    description='Pipeline mensal de atualização da ABT FPD',
    schedule_interval='0 2 5 * *',  # Dia 5 de cada mês às 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['fpd', 'batch', 'monthly'],
) as dag:

    # Task 1: Verificar se novos arquivos chegaram
    check_new_files = FileSensor(
        task_id='check_landing_zone',
        filepath='oci://landing-zone@namespace/bureau/*.parquet',
        poke_interval=300,  # Verifica a cada 5 min
        timeout=3600,       # Timeout de 1 hora
        mode='poke',
    )

    # Task 2: Bronze Ingestion
    bronze_ingestion = OCIDataFlowOperator(
        task_id='bronze_ingestion',
        application_id='{{ var.value.dataflow_bronze_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='bronze-ingestion-{{ ds }}',
        driver_shape='VM.Standard.E4.Flex',
        executor_shape='VM.Standard.E4.Flex',
        num_executors=4,
    )

    # Task 3: Silver Transformation
    silver_transformation = OCIDataFlowOperator(
        task_id='silver_transformation',
        application_id='{{ var.value.dataflow_silver_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='silver-transform-{{ ds }}',
        driver_shape='VM.Standard.E4.Flex',
        executor_shape='VM.Standard.E4.Flex',
        num_executors=8,
    )

    # Task 4: Gold Feature Generators (em paralelo)
    gold_recarga = OCIDataFlowOperator(
        task_id='gold_recarga_features',
        application_id='{{ var.value.dataflow_recarga_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='gold-recarga-{{ ds }}',
        num_executors=8,
    )

    gold_pagamento = OCIDataFlowOperator(
        task_id='gold_pagamento_features',
        application_id='{{ var.value.dataflow_pagamento_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='gold-pagamento-{{ ds }}',
        num_executors=8,
    )

    gold_atraso = OCIDataFlowOperator(
        task_id='gold_atraso_features',
        application_id='{{ var.value.dataflow_atraso_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='gold-atraso-{{ ds }}',
        num_executors=8,
    )

    # Task 5: ABT Builder
    abt_builder = OCIDataFlowOperator(
        task_id='abt_v6_builder',
        application_id='{{ var.value.dataflow_abt_app_id }}',
        compartment_id='{{ var.value.oci_compartment_id }}',
        display_name='abt-v6-builder-{{ ds }}',
        num_executors=8,
    )

    # Task 6: Validação
    def validate_abt(**context):
        """Valida a ABT gerada antes de disponibilizar."""
        # Conectar ao Object Storage e validar
        # - Count de registros
        # - Unicidade CPF+SAFRA
        # - Coverage das features
        pass

    validation = PythonOperator(
        task_id='validate_abt',
        python_callable=validate_abt,
    )

    # Task 7: Notificação
    def notify_completion(**context):
        """Notifica o time que o pipeline completou."""
        pass

    notification = PythonOperator(
        task_id='notify_completion',
        python_callable=notify_completion,
    )

    # Dependências
    check_new_files >> bronze_ingestion >> silver_transformation
    silver_transformation >> [gold_recarga, gold_pagamento, gold_atraso]
    [gold_recarga, gold_pagamento, gold_atraso] >> abt_builder
    abt_builder >> validation >> notification
```

### 3.4 Atualização do Modelo na API

Após o pipeline batch gerar nova ABT, pode ser necessário retreinar o modelo:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLUXO DE ATUALIZAÇÃO DO MODELO                       │
│                                                                         │
│   ┌─────────────┐                                                       │
│   │ Batch       │                                                       │
│   │ Pipeline    │                                                       │
│   │ (mensal)    │                                                       │
│   └──────┬──────┘                                                       │
│          │                                                              │
│          ▼                                                              │
│   ┌─────────────┐    Retreino     ┌─────────────┐                      │
│   │ Nova ABT    │───────────────▶│ Data Science │                      │
│   │ Gold Layer  │   (se necessário)│ Notebook    │                      │
│   └─────────────┘                 └──────┬──────┘                       │
│                                          │                              │
│                                          ▼                              │
│                                   ┌─────────────┐                       │
│                                   │ Novo Modelo │                       │
│                                   │ modelo_v2.txt│                      │
│                                   └──────┬──────┘                       │
│                                          │                              │
│                                          ▼                              │
│   ┌─────────────┐    Deploy      ┌─────────────┐                       │
│   │ Object      │◀───────────────│ Validação   │                       │
│   │ Storage     │                │ KS >= 33%   │                       │
│   │ /models/    │                └─────────────┘                       │
│   └──────┬──────┘                                                       │
│          │                                                              │
│          ▼                                                              │
│   ┌─────────────┐                                                       │
│   │ VM Scoring  │  Reload modelo (rolling restart)                     │
│   │ API         │                                                       │
│   └─────────────┘                                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Arquitetura Híbrida Completa

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OCI TENANCY (sa-saopaulo-1)                       │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         VCN (10.0.0.0/16)                             │ │
│  │                                                                        │ │
│  │   ┌────────────────────────────────────────────────────────────────┐  │ │
│  │   │              PUBLIC SUBNET (10.0.1.0/24)                       │  │ │
│  │   │                                                                 │  │ │
│  │   │   ┌─────────────────────────────────────────────────────────┐  │  │ │
│  │   │   │              LOAD BALANCER                              │  │  │ │
│  │   │   │              IP: X.X.X.X (público)                      │  │  │ │
│  │   │   │              HTTPS:443 → HTTP:8000                      │  │  │ │
│  │   │   └─────────────────────────┬───────────────────────────────┘  │  │ │
│  │   │                             │                                   │  │ │
│  │   └─────────────────────────────┼───────────────────────────────────┘  │ │
│  │                                 │                                       │ │
│  │   ┌─────────────────────────────┼───────────────────────────────────┐  │ │
│  │   │         PRIVATE SUBNET (10.0.10.0/24)                          │  │ │
│  │   │                             │                                   │  │ │
│  │   │   ┌─────────────────────────▼───────────────────────────────┐  │  │ │
│  │   │   │                 VM SCORING API                          │  │  │ │
│  │   │   │                 (Always-On, 24/7)                       │  │  │ │
│  │   │   │                                                          │  │  │ │
│  │   │   │   ┌────────────────────────────────────────────────┐    │  │  │ │
│  │   │   │   │  FastAPI + LightGBM                            │    │  │  │ │
│  │   │   │   │  - 1 OCPU, 8 GB RAM                            │    │  │  │ │
│  │   │   │   │  - Modelo carregado em memória                 │    │  │  │ │
│  │   │   │   │  - Latência ~50ms                              │    │  │  │ │
│  │   │   │   └────────────────────────────────────────────────┘    │  │  │ │
│  │   │   │                                                          │  │  │ │
│  │   │   │   Custo: ~$35/mês                                       │  │  │ │
│  │   │   └──────────────────────────────────────────────────────────┘  │  │ │
│  │   │                                                                 │  │ │
│  │   │   ┌──────────────────────────────────────────────────────────┐  │  │ │
│  │   │   │                 AIRFLOW (Opcional)                       │  │  │ │
│  │   │   │                 Orquestração do Batch                    │  │  │ │
│  │   │   │                 Pode compartilhar VM ou sob-demanda      │  │  │ │
│  │   │   └──────────────────────────────────────────────────────────┘  │  │ │
│  │   │                                                                 │  │ │
│  │   └─────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │   Gateways:                                                           │ │
│  │   ├── Internet Gateway (entrada pública → LB)                         │ │
│  │   ├── NAT Gateway (saída da VM para internet)                         │ │
│  │   └── Service Gateway (acesso ao Object Storage)                      │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                      │
│                                      │ Service Gateway                      │
│                                      ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    OBJECT STORAGE (Regional Service)                   │ │
│  │                                                                        │ │
│  │   ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐        │ │
│  │   │ landing-   │ │  bronze-   │ │  silver-   │ │   gold-    │        │ │
│  │   │   zone     │ │   layer    │ │   layer    │ │   layer    │        │ │
│  │   │            │ │            │ │            │ │            │        │ │
│  │   │ Parquet    │ │ + Metadata │ │ Type cast  │ │ ABT v6     │        │ │
│  │   │ (Claro)    │ │            │ │ Dedupe     │ │ 614 cols   │        │ │
│  │   └────────────┘ └────────────┘ └────────────┘ └────────────┘        │ │
│  │                                                                        │ │
│  │   ┌────────────┐                                                      │ │
│  │   │  models    │  ◀── Modelo LightGBM (modelo_fpd.txt)               │ │
│  │   └────────────┘                                                      │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    DATA FLOW (Spark) - Sob Demanda                     │ │
│  │                                                                        │ │
│  │   Executa mensalmente quando Claro entrega novos dados                │ │
│  │   Jobs: Bronze → Silver → Gold Features → ABT Builder                 │ │
│  │   Custo: ~$240 por execução mensal                                    │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Custos Operacionais

### 5.1 Custo Mensal em Produção

| Componente | Modo | Custo/Mês |
|------------|------|-----------|
| **VM Scoring API** | Always-on (24/7) | $35 |
| **Load Balancer** | Always-on | $20 |
| **Object Storage** | 295 GB | $7.50 |
| **Data Flow (Batch)** | 1x mensal (~8h) | $240 |
| **NAT Gateway** | Always-on | $35 |
| **Logging/Monitoring** | Basic | $10 |
| | | |
| **TOTAL OPERACIONAL** | | **~$347/mês** |

### 5.2 Comparação: Desenvolvimento vs Produção

| Fase | Custo/Mês | Notas |
|------|-----------|-------|
| **Desenvolvimento** | ~$1,015 | 3x Data Flow, notebooks, experimentação |
| **Produção** | ~$347 | 1x batch mensal, API 24/7 |

### 5.3 Breakdown por Cenário

```
┌─────────────────────────────────────────────────────────────────┐
│                    CUSTOS POR CENÁRIO                          │
│                                                                 │
│   SCORING API (24/7)               BATCH PIPELINE (Mensal)     │
│   ━━━━━━━━━━━━━━━━━━               ━━━━━━━━━━━━━━━━━━━━━━      │
│                                                                 │
│   VM Scoring:     $35              Data Flow:      $240        │
│   Load Balancer:  $20              Storage I/O:    $2          │
│   NAT Gateway:    $35              ─────────────────────        │
│   ─────────────────────            Subtotal:       $242        │
│   Subtotal:       $90                                          │
│                                                                 │
│   ─────────────────────────────────────────────────────────    │
│   Storage (compartilhado):                          $7.50      │
│   Logging/Monitoring:                               $10        │
│   ─────────────────────────────────────────────────────────    │
│                                                                 │
│   TOTAL MENSAL:                                     ~$350      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Fluxo de Atualização do Modelo

### 6.1 Quando Retreinar?

| Trigger | Ação |
|---------|------|
| KS cai abaixo de 30% | Retreino obrigatório |
| Novo mês de dados | Avaliar métricas, retreinar se necessário |
| Mudança de política Claro | Retreino com novos dados |
| Drift detectado | Investigar e retreinar |

### 6.2 Script de Deploy do Modelo

```bash
#!/bin/bash
# deploy_model.sh - Deploy novo modelo na VM Scoring

MODEL_FILE=$1
REMOTE_HOST="10.0.10.10"
REMOTE_PATH="/app/models/"

echo "Deploying model: $MODEL_FILE"

# 1. Upload para Object Storage
oci os object put \
    --bucket-name models \
    --file $MODEL_FILE \
    --name "modelo_fpd_$(date +%Y%m%d).txt"

# 2. Baixar na VM
ssh opc@$REMOTE_HOST "
    cd $REMOTE_PATH
    oci os object get --bucket-name models --name modelo_fpd_$(date +%Y%m%d).txt --file modelo_fpd_new.txt

    # 3. Validar modelo
    python -c \"import lightgbm as lgb; m=lgb.Booster(model_file='modelo_fpd_new.txt'); print(f'Features: {m.num_feature()}')\"

    # 4. Swap atômico
    mv modelo_fpd.txt modelo_fpd_backup.txt
    mv modelo_fpd_new.txt modelo_fpd.txt

    # 5. Restart graceful
    sudo systemctl restart scoring-api
"

echo "Deploy complete!"
```

---

## 7. Monitoramento e Alertas

### 7.1 Métricas da API

| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Latência P99 | > 500ms | Warning |
| Latência P99 | > 1000ms | Critical |
| Error rate | > 1% | Warning |
| Error rate | > 5% | Critical |
| CPU Usage | > 80% | Warning |
| Memory Usage | > 90% | Critical |

### 7.2 Métricas do Batch

| Métrica | Threshold | Alerta |
|---------|-----------|--------|
| Duração total | > 10 horas | Warning |
| Job falhou | Qualquer | Critical |
| ABT count | < 3.5M registros | Warning |
| Coverage Score_01 | < 95% | Warning |

### 7.3 OCI Monitoring Query (exemplo)

```sql
-- Latência média da API por hora
CpuUtilization[1h]{
    resourceId = "ocid1.instance.oc1..."
}.mean()

-- Requests por minuto
HttpRequests[1m]{
    loadBalancerId = "ocid1.loadbalancer.oc1..."
}.count()
```

---

## 8. Checklist de Deploy

### 8.1 Primeira Instalação

- [ ] VCN e subnets criadas
- [ ] Security Lists configuradas (porta 8000 interna, 443 externa)
- [ ] Load Balancer provisionado com certificado SSL
- [ ] VM Scoring criada e configurada
- [ ] Python environment instalado
- [ ] Modelo LightGBM copiado para /app/models/
- [ ] FastAPI configurado e testado
- [ ] Systemd service habilitado
- [ ] Health check respondendo
- [ ] Teste end-to-end com requisição real

### 8.2 Execução Mensal (Batch)

- [ ] Verificar se novos arquivos chegaram no landing-zone
- [ ] Disparar DAG do Airflow (manual ou automático)
- [ ] Monitorar jobs do Data Flow
- [ ] Validar ABT gerada (count, unicidade, coverage)
- [ ] Avaliar métricas do modelo (KS, AUC)
- [ ] Decidir se retreino é necessário
- [ ] Se retreino: deploy novo modelo
- [ ] Notificar equipe

### 8.3 Rollback (se necessário)

```bash
# Na VM Scoring
ssh opc@10.0.10.10

# Restaurar modelo anterior
cd /app/models
mv modelo_fpd.txt modelo_fpd_failed.txt
mv modelo_fpd_backup.txt modelo_fpd.txt

# Restart
sudo systemctl restart scoring-api

# Verificar
curl http://localhost:8000/health
```

---

## Resumo

| Cenário | Infraestrutura | Frequência | Custo |
|---------|----------------|------------|-------|
| **Scoring API** | VM + LB (24/7) | Sob demanda | ~$90/mês |
| **Batch Pipeline** | Data Flow (Spark) | Mensal | ~$242/exec |
| **Total Operacional** | | | **~$350/mês** |

**Documentos relacionados:**
- `OCI_ARCHITECTURE.md` - Arquitetura base (Tenancy, VCN, IAM)
- `OCI_TERRAFORM_AIRFLOW.md` - IaC e orquestração
- `GUIA_ARQUITETURA_OCI.md` - Guia de estudo para o time
