# Fase 8 — Modelo de Scoring em VM Dedicada OCI

## Visão Geral

Implantação do modelo de qualificação FPD (LightGBM) em VM dedicada OCI com orquestração Start/Stop via Airflow. A VM fica STOPPED entre execuções para reduzir custos, e o Airflow controla o ciclo completo: **ligar VM → executar modelo via SSH → desligar VM**.

**Data:** 2026-03-04
**Status:** ✅ Concluída — Pipeline executado com sucesso

### Resultado Final

| Métrica | Train | OOT |
|---------|-------|-----|
| **KS** | 39.19% | **34.39%** |
| **AUC** | 0.7640 | 0.7321 |
| **GINI** | 52.79% | 46.42% |

**Benchmark:** 33.10% → **Gap: +1.29 p.p. ACIMA**

### Artefatos no OCI Object Storage

| Bucket | Path | Descrição |
|--------|------|-----------|
| `hackathon-2025-models` | `pkl/modelo_fpd.pkl` | Modelo LightGBM serializado |
| `hackathon-2025-models` | `resultados_modelo/predicoes_oot_*.parquet` | Predições OOT (num_cpf, safra, fpd_int, score_fpd, decil) |
| `hackathon-2025-models` | `metricas/metricas_*.json` | Métricas (KS, AUC, GINI, benchmark) |
| `hackathon-2025-models` | `metricas/features_*.txt` | Lista de 261 features selecionadas |

---

## Motivação

O OCI Data Flow (Spark) tem restrições severas para dependências Python customizadas (lightgbm, scikit-learn). Após 3 tentativas com diferentes abordagens (conda pack Python 3.8, `spark.archives`, dependency-packager), o `ModuleNotFoundError` persistiu. Como o modelo é um job **single-node** (pandas + LightGBM, sem necessidade de Spark), a decisão foi migrar para uma VM dedicada.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────┐
│                   Airflow VM (10.0.1.x)              │
│                 public-subnet / E3.Flex               │
│                                                       │
│  DAG: pipeline_modelo_qualificacao (v2.0.0)          │
│  ┌─────────┐    ┌───────────┐    ┌─────────┐        │
│  │start_vm │───▶│run_modelo │───▶│stop_vm  │        │
│  │(OCI SDK)│    │  (SSH)    │    │(OCI SDK)│        │
│  └─────────┘    └─────┬─────┘    └─────────┘        │
│                       │ SSH (porta 22)                │
│                       │ via VCN interna               │
└───────────────────────┼──────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────┐
│                  Modelo VM (10.0.2.6)                 │
│               private-data-subnet / E5.Flex           │
│               1 OCPU, 16 GB RAM                       │
│                                                       │
│  python3.11 /opt/modelo-fpd/modelo_qualificacao.py   │
│                                                       │
│  Dependências (cloud-init):                           │
│    lightgbm==3.3.5, scikit-learn==1.3.2              │
│    pandas==2.0.3, numpy==1.24.4, pyarrow==14.0.2    │
│    oci==2.119.1                                       │
│                                                       │
│  Auth: Instance Principal (Dynamic Group)             │
│  Acesso: Object Storage via Service Gateway           │
│  IP público: NÃO (subnet privada)                    │
└──────────────────────────────────────────────────────┘
```

### Rede

- **Airflow VM:** subnet pública (10.0.1.x) — tem IP público para acesso SSH externo
- **Modelo VM:** subnet privada (10.0.2.x) — sem IP público, acessível apenas via VCN interna
- A Security List da private-data-subnet já permite tráfego SSH (porta 22) da VCN interna
- O NAT Gateway permite pip install durante cloud-init

### Shape E5.Flex

Shape `VM.Standard.E5.Flex` foi escolhido especificamente para **evitar conflito de quota** com os shapes usados pelo pipeline ETL (Data Flow): E3, E4, Standard2, Standard3, A1. Isso permite que a VM do modelo e o Data Flow coexistam sem competição por recursos.

---

## Infraestrutura Terraform

### Módulo `modules/modelo_vm/`

| Arquivo | Descrição |
|---------|-----------|
| `main.tf` | VM E5.Flex + Dynamic Group + Policy Instance Principal |
| `variables.tf` | Inputs: compartment_id, subnet_id, ssh_key, shape, etc. |
| `outputs.tf` | `modelo_vm_id` + `modelo_private_ip` |
| `cloud-init.yaml` | Instala Python 3.11, LightGBM, cria /opt/modelo-fpd/ |

### Recursos criados

```
Terraform apply output:
  + oci_core_instance.modelo
  + oci_identity_dynamic_group.modelo_vm
  + oci_identity_policy.modelo_instance_principal
```

### Integração em `environments/prod/`

**main.tf** — novo módulo:
```hcl
module "modelo_vm" {
  source                   = "../../modules/modelo_vm"
  tenancy_ocid             = var.tenancy_ocid
  compartment_id           = module.iam.compute_compartment_id
  project_compartment_name = var.project_name
  private_data_subnet_id   = module.network.private_data_subnet_id
  ssh_public_key           = var.modelo_ssh_public_key
  project_name             = var.project_name
  tags                     = var.tags
}
```

**outputs.tf** — para uso pelo Airflow:
```hcl
output "modelo_vm_info" {
  value = {
    vm_id      = module.modelo_vm.modelo_vm_id
    private_ip = module.modelo_vm.modelo_private_ip
  }
}
```

**terraform.tfvars** — chave SSH dedicada (ed25519, separada da Airflow):
```
modelo_ssh_public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHZUgUWi... modelo-scoring-hackathon"
```

### IAM: Policy do Airflow atualizada

Adicionada 4ª statement na policy do Airflow (`modules/airflow/main.tf`) para permitir Start/Stop da VM do modelo:

```hcl
"Allow dynamic-group ${...airflow_dynamic_group...} to manage instance-family in compartment ${var.project_compartment_name}"
```

### Remoção do Data Flow

O modelo foi removido do mapa `dataflow_applications` em `environments/prod/variables.tf`. O campo `archive_uri` (que era usado apenas pelo modelo) foi removido do módulo `compute/`.

---

## Script de Scoring: `modelo_qualificacao.py`

### Fluxo do script

```
1. Autenticação OCI (Instance Principal)
2. Leitura ABT v6 (40 parquets, filtro: flag_instalacao=1, safras válidas)
3. Split temporal (Train: Out-Dez/2024, OOT: Fev-Mar/2025)
4. Seleção de features (IV >= 0.01) → 261 features
5. Carregar PKL existente OU treinar novo modelo LightGBM
6. Predições + métricas (KS, AUC, GINI)
7. Salvar resultados no Object Storage
```

### Otimizações de memória (VM de 16 GB)

O dataset completo (3.79M registros × 614 colunas) não cabe em 16 GB. As seguintes otimizações foram implementadas:

| Técnica | Economia estimada |
|---------|-------------------|
| Filtro durante leitura (`row_filter`) | 3.7M → 2.18M registros (-41%) |
| Downcast float64→float32, int64→int32 | -50% por coluna numérica |
| `del df` após split temporal | Libera ~8 GB |
| `del df_train, df_oot` após extrair X/y | Libera ~8.2 GB |
| `gc.collect()` explícito | Força garbage collection |

**Budget de memória final:**

| Etapa | Memória |
|-------|---------|
| Leitura (filtrada, downcast) | ~8.1 GB |
| Após split + del df | ~8.2 GB (train + oot) |
| Após IV + del df_train/oot | ~2.2 GB (X_train + X_oot) |
| LightGBM training | ~5-6 GB pico |
| **Máximo** | **~10 GB de 16 GB** ✅ |

### Parâmetros do LightGBM

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
    "n_jobs": -1,
}
# Early stopping: 50 rounds, max 1000 boost rounds
# Resultado: 900 iterações (best)
```

---

## DAG Airflow: `dag_modelo_qualificacao.py` (v2.0.0)

### Tasks

| Task | Função | Detalhes |
|------|--------|---------|
| `start_vm` | Liga a VM | `ComputeClient.instance_action(vm_id, "START")` + poll até RUNNING (timeout 10 min) + 30s para SSH |
| `run_modelo` | Executa scoring | SSH via `subprocess.run()` com `python3.11 -u` (unbuffered). Timeout: 3h |
| `stop_vm` | Desliga a VM | `trigger_rule=ALL_DONE` — **sempre executa**, mesmo se run falhar |

### Variáveis Airflow necessárias

| Variável | Valor | Origem |
|----------|-------|--------|
| `oci_modelo_vm_id` | `ocid1.instance.oc1.sa-saopaulo-1.antxeljr...` | `terraform output modelo_vm_info` |
| `oci_modelo_vm_ip` | `10.0.2.6` | `terraform output modelo_vm_info` |
| `oci_compute_compartment_id` | (já existia) | `terraform output` |

### Autenticação OCI do Airflow

- **Instance Principal** (via Dynamic Group) — automático em VM OCI
- **Fallback:** `~/.oci/config` para desenvolvimento local

### SSH

- **Chave privada:** `/opt/airflow/config/modelo_vm_key` (dentro do container Docker)
  - Mapeada de `/opt/airflow-fpd/config/modelo_vm_key` no host
- **Usuário:** `opc` (Oracle Linux default)
- **Opções:** `StrictHostKeyChecking=no`, `ServerAliveInterval=60`, `ServerAliveCountMax=10`

---

## Deploy do Script

### Script de deploy: `deploy_modelo.sh`

O deploy usa a VM do Airflow como **jump host** porque a VM do modelo não tem IP público:

```
Local (WSL) ──SCP──▶ Airflow VM ──SCP──▶ Modelo VM
                    (10.0.1.x)           (10.0.2.x)
```

**Uso:**
```bash
# Executar LOCAL:
cd mig_oci/airflow
./deploy_modelo.sh <AIRFLOW_IP> <MODELO_PRIVATE_IP> [AIRFLOW_SSH_KEY] [MODELO_SSH_KEY]

# Exemplo:
./deploy_modelo.sh 137.131.199.10 10.0.2.6 ~/.ssh/airflow_vm ~/.ssh/modelo_vm
```

**Etapas do script:**
1. Verificar conexão SSH com Airflow VM
2. Copiar chave SSH do modelo para Airflow (`/opt/airflow-fpd/config/modelo_vm_key`)
3. Verificar conectividade Airflow → Modelo VM
4. Copiar script para Airflow (staging em `/tmp/`)
5. Copiar script do Airflow para Modelo VM (`/opt/modelo-fpd/`)

### Verificação pós-deploy

```bash
# Executar LOCAL (verifica via double-hop):
ssh -i ~/.ssh/airflow_vm opc@137.131.199.10 \
  "ssh -i /opt/airflow-fpd/config/modelo_vm_key opc@10.0.2.6 \
   'wc -l /opt/modelo-fpd/modelo_qualificacao.py && \
    python3.11 -m py_compile /opt/modelo-fpd/modelo_qualificacao.py && \
    echo SYNTAX_OK'"
```

---

## O que já existia e precisou mudar

| Componente | Antes | Depois |
|------------|-------|--------|
| **Execução do modelo** | Data Flow (Spark + PySpark) | VM dedicada (pandas + LightGBM) |
| **Script `modelo_qualificacao.py`** | SparkSession, `spark.read.format("delta")`, Resource Principal | pandas, `pd.read_parquet(io.BytesIO)`, Instance Principal |
| **DAG Airflow** | `dag_pipeline_fpd.py` disparava Data Flow App | Nova DAG `dag_modelo_qualificacao.py` com VM Start/Stop + SSH |
| **Autenticação OCI no script** | Resource Principal (Data Flow managed) | Instance Principal (VM Dynamic Group) |
| **Variáveis Airflow** | `oci_app_id_modelo_qualificacao` (OCID do Data Flow App) | `oci_modelo_vm_id` + `oci_modelo_vm_ip` |
| **Data Flow apps** | 21 aplicações (incluindo modelo) | 20 aplicações (modelo removido) |
| **IAM Airflow** | 3 statements (dataflow, object, network) | 4 statements (+instance-family para Start/Stop) |
| **`populate_variables.sh`** | Extraía `oci_app_id_modelo_qualificacao` | Extrai `oci_modelo_vm_id` e `oci_modelo_vm_ip` |

---

## Checklist de Deploy (para execuções futuras)

1. **Terraform apply** (se houve alteração na infra):
   ```bash
   # Executar LOCAL:
   cd mig_oci/terraform/environments/prod
   terraform plan && terraform apply
   ```

2. **Deploy do script** (se houve alteração no modelo):
   ```bash
   # Executar LOCAL:
   cd mig_oci/airflow
   ./deploy_modelo.sh 137.131.199.10 10.0.2.6 ~/.ssh/airflow_vm ~/.ssh/modelo_vm
   ```

3. **Deploy da DAG** (se houve alteração na DAG):
   ```bash
   # Executar LOCAL:
   scp -i ~/.ssh/airflow_vm mig_oci/airflow/dags/dag_modelo_qualificacao.py \
     opc@137.131.199.10:/opt/airflow-fpd/dags/
   ```

4. **Atualizar variáveis** (se mudou VM OCID/IP):
   ```bash
   # Executar na VM AIRFLOW:
   docker compose exec airflow-scheduler airflow variables import /opt/airflow-fpd/config/airflow_variables_filled.json
   ```

5. **Trigger da DAG**:
   ```
   Executar no BROWSER: http://137.131.199.10:8080
   DAGs → pipeline_modelo_qualificacao → Trigger DAG
   ```

---

## Custos

| Recurso | Custo estimado/mês | Observação |
|---------|-------------------|------------|
| VM E5.Flex (Start/Stop) | ~$5-15 | Depende do tempo ligada (batch mensal ~1h) |
| Object Storage (modelos) | < $1 | PKL ~50 MB, predições ~100 MB |
| **Total adicional** | **~$15/mês** | Muito menor que Data Flow (~$35/run) |

---

## Arquivos Criados/Modificados

| Arquivo | Ação |
|---------|------|
| `mig_oci/terraform/modules/modelo_vm/main.tf` | Criado |
| `mig_oci/terraform/modules/modelo_vm/variables.tf` | Criado |
| `mig_oci/terraform/modules/modelo_vm/outputs.tf` | Criado |
| `mig_oci/terraform/modules/modelo_vm/cloud-init.yaml` | Criado |
| `mig_oci/terraform/modules/airflow/main.tf` | Editado (+1 policy statement) |
| `mig_oci/terraform/environments/prod/main.tf` | Editado (+1 module block) |
| `mig_oci/terraform/environments/prod/variables.tf` | Editado (+var, -modelo do DF) |
| `mig_oci/terraform/environments/prod/outputs.tf` | Editado (+1 output) |
| `mig_oci/terraform/environments/prod/terraform.tfvars` | Editado (+ssh key) |
| `mig_oci/terraform/modules/compute/main.tf` | Editado (-archive_uri) |
| `mig_oci/terraform/modules/compute/variables.tf` | Editado (-archive_uri) |
| `mig_oci/data_science/scripts/modelo_qualificacao.py` | Reescrito (Spark → pandas) |
| `mig_oci/airflow/dags/dag_modelo_qualificacao.py` | Reescrito (Data Flow → VM SSH) |
| `mig_oci/airflow/config/airflow_variables.json` | Editado (novas variáveis) |
| `mig_oci/airflow/config/populate_variables.sh` | Editado (novos outputs) |
| `mig_oci/airflow/deploy_modelo.sh` | Criado |
