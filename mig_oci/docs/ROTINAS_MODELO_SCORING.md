# Rotinas de Processamento — Modelo de Scoring FPD

Documentação das 4 rotinas operacionais do modelo LightGBM na infraestrutura OCI.

## Índice

1. [Treinamento do Modelo LightGBM](#1-treinamento-do-modelo-lightgbm)
2. [Deploy do Modelo na VM-Scoring (Pós-Treinamento)](#2-deploy-do-modelo-na-vm-scoring-pós-treinamento)
3. [Scoring Batch (Pipeline Operacional)](#3-scoring-batch-pipeline-operacional)
4. [Orquestração Inteligente da VM-Scoring (Start/Stop)](#4-orquestração-inteligente-da-vm-scoring-startstop)

---

## 1. Treinamento do Modelo LightGBM

### Visão Geral

O treinamento segue a lógica **train-or-load**: o script tenta carregar um modelo PKL existente do Object Storage. Se não encontrar, treina um novo modelo do zero e salva o PKL.

### Script

- **Arquivo:** `mig_oci/data_science/scripts/modelo_qualificacao.py`
- **Execução:** Na VM Modelo (E5.Flex, 2 OCPUs / 32 GB RAM)
- **Runtime:** Python 3.11 + LightGBM + pandas + OCI SDK

### Fluxo do Treinamento

```
┌─────────────────────────────────────────────────────────┐
│                 modelo_qualificacao.py                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. Autenticação OCI (Instance Principal)               │
│         ↓                                               │
│  2. Leitura da ABT v6 (Object Storage → pandas)         │
│     • Bucket: hackathon-2025-gold-layer/abt_v6_v2/      │
│     • Filtro: flag_instalacao_int = 1                    │
│     • Safras: [202410, 202411, 202412, 202502, 202503]  │
│     • Dedup incremental a cada 40 arquivos              │
│     • Downcast float64→float32 (economia de memória)    │
│         ↓                                               │
│  3. Split Temporal                                      │
│     • Train: safras 202410, 202411, 202412              │
│     • OOT:   safras 202502, 202503                      │
│         ↓                                               │
│  4. Seleção de Features (IV >= 0.01)                    │
│     • 614 colunas → ~261 features selecionadas          │
│         ↓                                               │
│  5. Train-or-Load                                       │
│     ┌─ Tenta carregar PKL do bucket models              │
│     │  (hackathon-2025-models/pkl/modelo_fpd.pkl)       │
│     │                                                   │
│     ├─ Se PKL existe → Carrega e pula para predição     │
│     │                                                   │
│     └─ Se PKL NÃO existe → Treina novo modelo          │
│        • LightGBM (gbdt, binary, AUC)                  │
│        • 1000 rounds, early_stopping=50                 │
│        • Salva PKL no bucket                            │
│         ↓                                               │
│  6. Predição e Métricas                                 │
│     • KS, AUC, GINI (train + OOT)                      │
│         ↓                                               │
│  7. Salvar Artefatos no Object Storage                  │
│     • Predições OOT (parquet)                           │
│     • Métricas (JSON)                                   │
│     • Feature list (TXT)                                │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Hiperparâmetros do LightGBM

```python
params = {
    "objective": "binary",
    "metric": "auc",
    "boosting_type": "gbdt",
    "num_leaves": 31,
    "max_depth": 6,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 100,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "seed": 42,
    "n_jobs": -1,          # usa todos os cores da VM
}

# Treinamento
model = lgb.train(
    params,
    train_data,
    num_boost_round=1000,
    valid_sets=[train_data, valid_data],
    callbacks=[
        lgb.early_stopping(stopping_rounds=50),  # para se AUC não melhorar em 50 rounds
        lgb.log_evaluation(period=100),           # log a cada 100 rounds
    ],
)
```

### Artefatos Gerados

| Artefato | Bucket | Path | Formato |
|----------|--------|------|---------|
| Modelo serializado | hackathon-2025-models | `pkl/modelo_fpd.pkl` | Pickle |
| Predições OOT | hackathon-2025-models | `resultados_modelo/predicoes_oot_<timestamp>.parquet` | Parquet |
| Métricas | hackathon-2025-models | `metricas/metricas_<timestamp>.json` | JSON |
| Feature list | hackathon-2025-models | `metricas/features_<timestamp>.txt` | TXT |

### Quando Retreinar

- **Periodicidade recomendada:** A cada 3-6 meses
- **Gatilho:** KS OOT cair mais de 2 p.p. em relação ao treinamento original
- **Como forçar retreino:** Deletar o arquivo `pkl/modelo_fpd.pkl` do bucket `hackathon-2025-models`. Na próxima execução, o script treinará do zero automaticamente.

```bash
# Deletar PKL para forçar retreino (executar LOCAL ou VM)
oci os object delete \
  --namespace <NAMESPACE> \
  --bucket-name hackathon-2025-models \
  --object-name pkl/modelo_fpd.pkl \
  --force
```

### Comando de Execução

```bash
# Na VM Modelo (Instance Principal - automático)
python3.11 -u /opt/modelo-fpd/modelo_qualificacao.py

# Local/debug (usa ~/.oci/config)
python3.11 modelo_qualificacao.py --local

# Com amostragem (para debug rápido)
python3.11 modelo_qualificacao.py --local --sample 0.1
```

### Gestão de Memória

A VM tem 32 GB de RAM. O script usa técnicas para não estourar:

| Técnica | Impacto |
|---------|---------|
| Filtro durante leitura (`row_filter`) | Carrega só flag_instalacao=1 + safras válidas |
| Dedup incremental a cada 40 arquivos | Evita acúmulo de duplicatas de re-execução |
| Downcast float64→float32 / int64→int32 | Reduz memória em ~50% |
| `del df` + `gc.collect()` após split | Libera ~8 GB após o split temporal |
| `del df_train, df_oot` após criar X/y | Libera 614 cols, mantém só ~261 cols |

**Pico de memória:** ~24 GB (durante leitura + concat). Após split: ~12 GB.

---

## 2. Deploy do Modelo na VM-Scoring (Pós-Treinamento)

### Visão Geral

A VM do Modelo está numa **subnet privada** (sem IP público). O deploy usa a VM do Airflow como **jump host** (bastion):

```
Local (WSL)  →  VM Airflow (subnet pública)  →  VM Modelo (subnet privada)
                10.0.1.x (IP público)             10.0.2.x (IP privado)
```

### Script de Deploy

- **Arquivo:** `mig_oci/airflow/deploy_modelo.sh`
- **Executar em:** LOCAL (WSL/Linux/Mac)

### Fluxo do Deploy

```
┌──────────────────────────────────────────────────────────┐
│                   deploy_modelo.sh                        │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  [1/5] Verifica conexão SSH com Airflow VM               │
│         ↓                                                │
│  [2/5] Copia chave SSH do Modelo para Airflow VM         │
│        (para a DAG poder conectar via SSH)               │
│        → /opt/airflow-fpd/config/modelo_vm_key           │
│         ↓                                                │
│  [3/5] Verifica conexão Airflow → Modelo VM              │
│        (testa o caminho completo do jump host)           │
│         ↓                                                │
│  [4/5] Copia script para Airflow VM (staging)            │
│        modelo_qualificacao.py → /tmp/                    │
│         ↓                                                │
│  [5/5] Copia script de Airflow para Modelo VM            │
│        /tmp/ → /opt/modelo-fpd/modelo_qualificacao.py    │
│         ↓                                                │
│  Verificação: ls + py_compile na VM Modelo               │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Comando

```bash
# Executar LOCAL
cd mig_oci/airflow
chmod +x deploy_modelo.sh

./deploy_modelo.sh <AIRFLOW_IP> <MODELO_PRIVATE_IP> [AIRFLOW_SSH_KEY] [MODELO_SSH_KEY]

# Exemplo:
./deploy_modelo.sh 137.131.199.10 10.0.2.5 ~/.ssh/airflow_vm ~/.ssh/modelo_vm
```

### Parâmetros

| Parâmetro | Descrição | Default |
|-----------|-----------|---------|
| AIRFLOW_IP | IP público da VM Airflow | (obrigatório) |
| MODELO_PRIVATE_IP | IP privado da VM Modelo (na VCN) | (obrigatório) |
| AIRFLOW_SSH_KEY | Chave SSH para acessar Airflow | `~/.ssh/airflow_vm` |
| MODELO_SSH_KEY | Chave SSH para acessar Modelo | `~/.ssh/modelo_vm` |

### Pré-requisitos

1. **VM Airflow RUNNING** (IP público acessível)
2. **VM Modelo RUNNING** (acessível pela VCN interna)
3. **Security List** da subnet privada permite tráfego na porta 22 dentro da VCN
4. **Chaves SSH** geradas e autorizadas em ambas as VMs
5. **Python 3.11 + LightGBM + OCI SDK** instalados na VM Modelo

### Atualização sem Re-deploy Completo

Para atualizar apenas o script (sem chaves SSH), um `scp` direto é suficiente:

```bash
# Executar LOCAL — Atualização rápida (via jump host)
scp -i ~/.ssh/airflow_vm -o ProxyJump=opc@<AIRFLOW_IP> \
  mig_oci/data_science/scripts/modelo_qualificacao.py \
  opc@<MODELO_IP>:/opt/modelo-fpd/modelo_qualificacao.py
```

### Verificação Pós-Deploy

```bash
# Executar na VM Modelo (via jump host)
ssh -i ~/.ssh/airflow_vm opc@<AIRFLOW_IP> \
  "ssh -i /opt/airflow-fpd/config/modelo_vm_key opc@<MODELO_IP> \
   'ls -la /opt/modelo-fpd/ && python3.11 -c \"import lightgbm; print(lightgbm.__version__)\"'"
```

---

## 3. Scoring Batch (Pipeline Operacional)

### Importante: Batch, não Tempo Real

O modelo atual opera em modo **batch** (mensal), não em tempo real. O scoring é executado como parte do pipeline ETL:

```
Pipeline ETL (21 Data Flow apps)  →  Modelo Scoring (VM dedicada)
     Landing → Bronze → Silver        Lê ABT v6 → Predições OOT
     → Gold → ABT v1-v6               → Métricas → Object Storage
```

> **Nota:** Uma API de scoring em tempo real (REST) não foi implementada nesta fase do projeto. O pipeline batch é suficiente para a operação mensal de qualificação de clientes. Caso seja necessário no futuro, o modelo PKL pode ser encapsulado num endpoint FastAPI/Flask — ver seção "Evolução para API REST" abaixo.

### Fluxo do Scoring Batch

```
┌─────────────────────────────────────────────────────────┐
│              Scoring Batch (Mensal)                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. DAG ETL finaliza                                    │
│     (21 Data Flow apps: Landing → Bronze → Silver       │
│      → Gold Features → ABT v1-v6)                       │
│         ↓                                               │
│  2. TriggerDagRunOperator dispara DAG Modelo            │
│     (automático, wait_for_completion=True)              │
│         ↓                                               │
│  3. DAG Modelo executa (ver seção 4):                   │
│     Start VM → SSH scoring → Stop VM                    │
│         ↓                                               │
│  4. Script carrega PKL existente (train-or-load)        │
│     • Se PKL existe → scoring direto (~15 min)          │
│     • Se PKL não existe → treina + scoring (~45 min)    │
│         ↓                                               │
│  5. Artefatos salvos no Object Storage:                 │
│     • Predições OOT com score + decil (parquet)         │
│     • Métricas: KS, AUC, GINI (JSON)                   │
│     • Feature list selecionada (TXT)                    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Output do Scoring

O arquivo de predições contém:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| num_cpf | string | Identificador do cliente |
| safra | string | Safra da observação (AAAAMM) |
| fpd_int | int | Target real (0/1) — para validação |
| score_fpd | float | Probabilidade de FPD (0.0 a 1.0) |
| decil | int | Decil de risco (1=maior risco, 10=menor risco) |

### Exemplo de Consumo

```python
# Leitura das predições mais recentes
import pandas as pd

df = pd.read_parquet("predicoes_oot_20260307_1850.parquet")

# Clientes de alto risco (decis 1-3)
alto_risco = df[df["decil"] <= 3]
print(f"Clientes alto risco: {len(alto_risco):,}")
print(f"Taxa FPD real: {alto_risco['fpd_int'].mean()*100:.1f}%")
```

### Evolução para API REST (Futuro)

Caso seja necessário scoring em tempo real, o modelo PKL pode ser encapsulado:

```python
# Exemplo conceitual — NÃO implementado
from fastapi import FastAPI
import pickle, pandas as pd

app = FastAPI()
model = pickle.load(open("modelo_fpd.pkl", "rb"))

@app.post("/score")
def score(features: dict):
    df = pd.DataFrame([features])
    proba = model.predict(df)[0]
    return {"score_fpd": float(proba), "decil": int(proba * 10) + 1}
```

**Requisitos para API REST:**
- VM Modelo sempre RUNNING (sem Start/Stop) — custo ~$120/mês
- Load balancer ou API Gateway na frente
- Monitoramento de latência e disponibilidade
- Feature store para servir features em tempo real

---

## 4. Orquestração Inteligente da VM-Scoring (Start/Stop)

### Visão Geral

A VM do Modelo (E5.Flex, 2 OCPUs / 32 GB) fica **STOPPED** entre execuções para reduzir custos. O Airflow controla o ciclo de vida:

```
STOPPED → START → RUNNING → SSH scoring → STOP → STOPPED
  ($0)     (~2min)  (~30min scoring)       (~1min)   ($0)

Custo: ~$40/mês (vs ~$120/mês se ficasse RUNNING 24/7)
```

### DAG: pipeline_modelo_qualificacao

- **Arquivo:** `mig_oci/airflow/dags/dag_modelo_qualificacao.py`
- **Versão:** v2.1.0
- **Agendamento:** None (sem schedule fixo)

### Fluxo da DAG

```
┌──────────────────────────────────────────────────────────┐
│           DAG: pipeline_modelo_qualificacao               │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Trigger:                                                │
│  ┌─ Automático: TriggerDagRunOperator da DAG ETL         │
│  └─ Manual: Airflow UI ou CLI                            │
│         ↓                                                │
│  ┌──────────────┐                                        │
│  │  start_vm    │  Liga a VM via OCI ComputeClient       │
│  │              │  • instance_action("START")             │
│  │              │  • Poll a cada 15s até RUNNING          │
│  │              │  • Timeout: 10 min                      │
│  │              │  • +30s extra para SSH ficar pronto     │
│  └──────┬───────┘                                        │
│         ↓                                                │
│  ┌──────────────┐                                        │
│  │  run_modelo  │  Executa scoring via SSH                │
│  │              │  • ssh opc@<IP_PRIVADO>                 │
│  │              │  • python3.11 modelo_qualificacao.py    │
│  │              │  • Timeout: 3h                          │
│  │              │  • Logs capturados (stdout + stderr)    │
│  └──────┬───────┘                                        │
│         ↓                                                │
│  ┌──────────────┐                                        │
│  │  stop_vm     │  Desliga a VM (SEMPRE executa)         │
│  │              │  • trigger_rule=ALL_DONE                │
│  │              │  • instance_action("STOP")              │
│  │              │  • Mesmo se scoring falhar → VM desliga │
│  └──────────────┘                                        │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Mecanismos de Proteção

| Mecanismo | Implementação | Por quê |
|-----------|---------------|---------|
| **stop_vm SEMPRE executa** | `trigger_rule=ALL_DONE` | Evita VM ligada indefinidamente se scoring falhar (custo!) |
| **Tratamento de estados** | Verifica STOPPED/RUNNING/STOPPING/STARTING | VM pode estar em transição de execução anterior |
| **Poll com timeout** | 15s polling, 10 min timeout | Evita espera infinita se VM não ligar |
| **SSH keep-alive** | `ServerAliveInterval=60`, `ServerAliveCountMax=10` | Conexão SSH não cai durante scoring longo (~30 min) |
| **Retries** | `retries=1`, `retry_delay=5min` | Uma tentativa extra se houver falha transitória |

### Modos de Execução

#### A) Automático (após pipeline ETL)

A DAG principal do ETL (`pipeline_credit_risk_fpd`) encadeia automaticamente com a DAG do modelo:

```python
# Na dag_pipeline_fpd.py (última task)
trigger_modelo = TriggerDagRunOperator(
    task_id="trigger_modelo_scoring",
    trigger_dag_id="pipeline_modelo_qualificacao",
    wait_for_completion=True,   # aguarda scoring finalizar
    reset_dag_run=True,         # permite re-trigger no mesmo dia
)
```

Fluxo completo:
```
DAG ETL (21 apps: Landing → Bronze → Silver → Gold → ABT)
    ↓ TriggerDagRunOperator
DAG Modelo (Start VM → SSH scoring → Stop VM)
```

#### B) Manual (re-scoring sem re-ETL)

Para re-scoring sem executar o pipeline ETL inteiro:

```bash
# Via CLI (executar na VM Airflow, dentro do container)
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_modelo_qualificacao

# Via UI
# Abrir http://<AIRFLOW_IP>:8080
# → DAGs → pipeline_modelo_qualificacao → Trigger (botão play)
```

### Autenticação OCI

O Airflow autentica na API OCI via **Instance Principal**:

```
VM Airflow
  ↓ (Dynamic Group matching por OCID da instância)
OCI IAM
  ↓ (Policy: manage instance-family in compartment compute)
ComputeClient → instance_action(START/STOP)
```

**Variáveis necessárias no Airflow:**

| Variable | Valor | Origem |
|----------|-------|--------|
| `oci_modelo_vm_id` | OCID da VM Modelo | `terraform output modelo_vm_info` |
| `oci_modelo_vm_ip` | IP privado da VM | `terraform output modelo_vm_info` |
| `oci_compute_compartment_id` | OCID do compartment | `terraform output compartment_ids` |

### Monitoramento

```bash
# Ver status da DAG (executar na VM Airflow)
docker compose exec airflow-scheduler \
  airflow dags list-runs -d pipeline_modelo_qualificacao

# Ver logs de uma execução específica
docker compose exec airflow-scheduler \
  airflow tasks logs pipeline_modelo_qualificacao run_modelo <EXECUTION_DATE>

# Ver estado da VM (executar LOCAL ou VM)
oci compute instance get --instance-id <MODELO_VM_OCID> \
  --query 'data."lifecycle-state"' --raw-output
```

### Custos da Orquestração

| Componente | Custo/mês | Observação |
|-----------|-----------|------------|
| VM Modelo (Start/Stop) | ~$40 | ~1h/mês ligada (3 runs × ~20 min) |
| VM Modelo (24/7) | ~$120 | Se ficasse sempre RUNNING |
| VM Airflow (24/7) | ~$60 | Precisa estar sempre RUNNING para orquestrar |
| **Economia Start/Stop** | **~$80/mês** | 67% menor que VM 24/7 |

---

## Resumo dos Comandos

### Deploy Completo (primeira vez)

```bash
# 1. Deploy do script para VM Modelo (executar LOCAL)
cd mig_oci/airflow
./deploy_modelo.sh <AIRFLOW_IP> <MODELO_IP> ~/.ssh/airflow_vm ~/.ssh/modelo_vm

# 2. Deploy das DAGs para Airflow (executar LOCAL)
./deploy_to_vm.sh <AIRFLOW_IP> ~/.ssh/airflow_vm

# 3. Importar variáveis Terraform no Airflow (executar na VM AIRFLOW)
docker compose exec airflow-scheduler \
  airflow variables import /opt/airflow-fpd/config/airflow_variables_filled.json

# 4. Trigger da DAG (executar na VM AIRFLOW ou via UI)
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_modelo_qualificacao
```

### Atualização do Script (já deployado antes)

```bash
# Atualização rápida via jump host (executar LOCAL)
scp -i ~/.ssh/airflow_vm -o ProxyJump=opc@<AIRFLOW_IP> \
  mig_oci/data_science/scripts/modelo_qualificacao.py \
  opc@<MODELO_IP>:/opt/modelo-fpd/modelo_qualificacao.py
```

### Forçar Retreino

```bash
# Deletar PKL do bucket (executar LOCAL ou VM)
oci os object delete \
  --namespace <NAMESPACE> \
  --bucket-name hackathon-2025-models \
  --object-name pkl/modelo_fpd.pkl \
  --force

# Trigger scoring (vai treinar do zero)
docker compose exec airflow-scheduler \
  airflow dags trigger pipeline_modelo_qualificacao
```

---

## Diagrama Geral

```
                        ┌─────────────────┐
                        │   Airflow UI    │
                        │  :8080          │
                        └────────┬────────┘
                                 │ trigger
                        ┌────────▼────────┐
                        │  VM Airflow     │
                        │  E3.Flex        │
                        │  Subnet Pública │
                        │  10.0.1.x       │
                        └────────┬────────┘
                     OCI API │        │ SSH
              (Start/Stop)   │        │ (scoring)
                             │        │
                    ┌────────▼────────▼────────┐
                    │    VM Modelo             │
                    │    E5.Flex (2 OCPU/32GB) │
                    │    Subnet Privada        │
                    │    10.0.2.x              │
                    │                          │
                    │  /opt/modelo-fpd/        │
                    │  └── modelo_qualificacao │
                    └────────────┬─────────────┘
                                 │ OCI SDK
                     Instance Principal
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                   │
    ┌─────────▼──────┐ ┌────────▼───────┐ ┌────────▼───────┐
    │  gold-layer    │ │    models      │ │    models      │
    │  (ABT v6)      │ │  (PKL)        │ │  (métricas)    │
    │  abt_v6_v2/    │ │  pkl/         │ │  metricas/     │
    └────────────────┘ │  resultados/  │ │  features/     │
                       └────────────────┘ └────────────────┘
```
