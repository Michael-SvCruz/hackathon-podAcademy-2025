# Migração OCI - Hackathon PodAcademy 2025

Migração do projeto de Databricks/AWS para Oracle Cloud Infrastructure (OCI) usando Terraform.

## Status Atual

✅ **Fase 0 (Setup):** Arquivos de configuração criados
⏳ **Fase 1 (IAM):** Aguardando implementação
⏳ **Fase 2 (Network):** Aguardando implementação
⏳ **Fase 3 (Storage):** Aguardando implementação
⏳ **Fase 4 (Compute):** Aguardando implementação
⏳ **Fase 5 (Security):** Opcional
⏳ **Fase 6 (Upload):** Aguardando implementação

## Quick Start

### 1. Configurar Credenciais OCI

```bash
# 1.1. Copiar template de configuração
cd terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars

# 1.2. Editar terraform.tfvars com suas credenciais OCI
# - tenancy_ocid: Console OCI > Tenancy Details
# - user_ocid: Console OCI > Identity > Users > seu usuário
# - fingerprint: Console OCI > Identity > Users > API Keys
# - private_key_path: Caminho para chave .pem (gerada com API Key)
nano terraform.tfvars
```

### 2. Inicializar Terraform

```bash
cd ../../scripts
./init.sh
```

**Saída esperada:**
```
✅ Terraform inicializado com sucesso!
```

### 3. Executar Fases Incrementalmente

```bash
# Fase 1: IAM (Compartments + Grupos) - 30 min
./apply_phase.sh 1

# Fase 2: Network (VCN + Subnets) - 45 min
./apply_phase.sh 2

# Fase 3: Storage (Buckets + State Remoto) - 30 min
./apply_phase.sh 3
./migrate_state_to_remote.sh

# Fase 4: Compute (Data Flow Applications) - 2-3h
./apply_phase.sh 4

# Fase 5: Security (Vault + Keys) - 15 min - OPCIONAL
./apply_phase.sh 5
```

### 4. Upload de Dados

```bash
cd ../../data_upload
./upload_landing.sh
```

## Estrutura do Projeto

```
mig_oci/
├── terraform/
│   ├── modules/               # Módulos reutilizáveis (iam, network, storage, compute, security)
│   ├── environments/prod/     # Configuração produção
│   └── scripts/               # Scripts auxiliares (init, apply_phase, migrate_state)
├── airflow/                   # Orquestração (configurar após Terraform)
├── data_upload/               # Scripts de upload (OCI CLI)
└── docs/                      # Documentação adicional
```

## Divisão de Responsabilidades

| Ferramenta | O que faz | Quando |
|------------|-----------|--------|
| **Terraform** | Cria infraestrutura OCI (compartments, VCN, buckets, Data Flow apps) | Fases 0-5 |
| **Scripts manuais** | Upload de dados e scripts Python | Fase 6 |
| **Airflow** | Orquestra EXECUÇÃO dos Data Flow jobs | Após Terraform |
| **Console OCI** | Testes manuais, monitoramento | Durante toda migração |

## Custos Estimados

- **Desenvolvimento (30 dias):** ~$1,249
- **Produção (mensal):** ~$350

## Próximos Passos

1. ✅ Configurar credenciais OCI em `terraform.tfvars`
2. ✅ Executar `./scripts/init.sh`
3. ⏳ Executar Fase 1: `./scripts/apply_phase.sh 1`
4. ⏳ Executar Fases 2-4 incrementalmente
5. ⏳ Upload de dados (Fase 6)
6. ⏳ Configurar Airflow
7. ⏳ Testar pipeline completo

## Referências

- **Plano completo:** `.claude/plans/eager-popping-meerkat.md`
- **Documentação OCI:** `docs/architecture/OCI_ARCHITECTURE.md`
- **Terraform + Airflow:** `docs/architecture/OCI_TERRAFORM_AIRFLOW.md`
- **Guia do Time:** `docs/architecture/GUIA_ARQUITETURA_OCI.md`

## Troubleshooting

### Erro: "terraform.tfvars not found"
```bash
cd terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars
# Edite terraform.tfvars com seus valores OCI
```

### Erro: "Private key not found"
1. Gere API Key no Console OCI: Identity > Users > seu usuário > API Keys > Add API Key
2. Baixe a chave privada (.pem)
3. Salve em `~/.oci/oci_api_key.pem`
4. Defina permissões: `chmod 600 ~/.oci/oci_api_key.pem`

### Erro: Quota insuficiente
Verifique quotas OCI antes de aplicar:
- Console OCI > Governance > Limits, Quotas and Usage
- Especialmente: Compute OCPUs, Object Storage

## Suporte

Para dúvidas sobre o projeto, consulte:
- **CLAUDE.md:** `.claude/CLAUDE.md` (quick reference)
- **Docs técnicos:** `docs/08_team_preparation/technical/`
- **Docs business:** `docs/08_team_preparation/business/`
