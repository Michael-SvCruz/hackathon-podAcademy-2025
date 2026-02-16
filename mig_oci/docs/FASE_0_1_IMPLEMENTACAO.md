# Migração OCI - Fases 0 e 1: Setup e IAM

## Contexto

O projeto Hackathon PodAcademy 2025 foi desenvolvido em Databricks/AWS e precisa ser migrado para Oracle Cloud Infrastructure (OCI) para a defesa final (janela de 30 dias). A migração é feita de forma incremental usando Terraform (Infrastructure as Code).

**O que é Terraform?**
Terraform é uma ferramenta que permite criar infraestrutura cloud escrevendo código (arquivos `.tf`). Em vez de clicar no Console OCI para criar cada recurso manualmente, você descreve o que precisa em código e o Terraform cria tudo automaticamente. Isso garante reprodutibilidade, versionamento e facilidade de destruição/recriação.

---

## Fase 0: Setup Inicial

**Objetivo:** Preparar o ambiente Terraform e validar conexão com a OCI.

### O que foi criado

| Arquivo | Propósito |
|---------|-----------|
| `mig_oci/.gitignore` | Protege credenciais de serem commitadas no Git |
| `terraform/environments/prod/versions.tf` | Configura o provider OCI (plugin de conexão) |
| `terraform/environments/prod/backend.tf` | Define onde o Terraform salva o "estado" dos recursos |
| `terraform/environments/prod/terraform.tfvars.example` | Template de configuração (sem segredos) |
| `terraform/scripts/init.sh` | Script que valida credenciais e inicializa o Terraform |
| `mig_oci/README.md` | Guia rápido do projeto de migração |

### Conceitos Importantes

#### Provider OCI (`versions.tf`)
O provider é o "driver de conexão" entre Terraform e a OCI. Sem ele, o Terraform não sabe como se comunicar com a Oracle Cloud. Configuramos com:
- `tenancy_ocid` - ID único da sua conta OCI
- `user_ocid` - ID do seu usuário
- `fingerprint` - Identificador da API Key
- `private_key_path` - Caminho para o arquivo `.pem` (chave privada)
- `region` - Região da OCI (sa-saopaulo-1 para São Paulo)

#### Backend (`backend.tf`)
O Terraform precisa saber **o que já foi criado** para não duplicar recursos. Essa informação é salva em um arquivo chamado `terraform.tfstate`. O backend define onde esse arquivo fica:
- **Fase 0-2:** Localmente (no computador) - simples para começar
- **Fase 3+:** Remoto no OCI Object Storage - permite colaboração em time

#### terraform.tfvars vs terraform.tfvars.example
- `terraform.tfvars.example` - Template commitado no Git (sem valores reais)
- `terraform.tfvars` - Sua configuração real (no `.gitignore`, NUNCA commitado)

Fluxo: copiar `.example` → `.tfvars` → preencher com seus valores OCI

#### API Key e o arquivo .pem
Para o Terraform se autenticar na OCI, ele usa uma chave criptográfica (API Key):
1. Você gera no Console OCI (Identity > Users > API Keys)
2. A OCI fornece um arquivo `.pem` (chave privada)
3. O Terraform usa esse `.pem` para provar que é você

**O nome do arquivo .pem não importa** - o que importa é o conteúdo e o fingerprint associado.

**No WSL**, caminhos do Windows são acessados via `/mnt/<drive>/...`:
```
Windows: D:\pasta\chave.pem
WSL:     /mnt/d/pasta/chave.pem
```

### Validação da Fase 0

```bash
cd mig_oci/terraform/scripts
./init.sh
```

**Saída esperada:**
```
✅ Terraform inicializado com sucesso!
```

Isso confirma:
- Credenciais OCI válidas
- Provider OCI v5.47.0 instalado
- Backend local configurado
- Configuração sintáticamente válida

---

## Fase 1: IAM (Identity and Access Management)

**Objetivo:** Criar a estrutura organizacional na OCI (compartments, grupos, políticas).

### O que é IAM?

IAM é o sistema de "quem pode fazer o quê" na OCI. Composto por 3 conceitos:

| Conceito | Analogia | Exemplo |
|----------|----------|---------|
| **Compartment** | Pasta do Windows | "storage" (guarda buckets) |
| **Group** | Equipe da empresa | "data-engineers" (engenheiros de dados) |
| **Policy** | Crachá de acesso | "data-engineers podem gerenciar storage" |

### Estrutura de Compartments

```
Tenancy Root (sua conta OCI)
└── hackathon-2025 (compartment raiz do projeto)
    ├── network     → VCN, Subnets, Gateways (Fase 2)
    ├── storage     → Buckets Object Storage (Fase 3)
    ├── compute     → Data Flow, Data Science (Fase 4)
    ├── data        → Data Catalog (futuro)
    └── security    → Vault, Keys (Fase 5, opcional)
```

**Por que separar em compartments?**
- **Organização:** Cada tipo de recurso fica "na sua pasta"
- **Segurança:** Políticas de acesso são definidas por compartment
- **Custos:** Facilita ver quanto cada área custa
- **Limpeza:** `terraform destroy` apaga tudo de uma vez

### Grupos e Políticas

| Grupo | Quem é | O que pode fazer |
|-------|--------|------------------|
| `hackathon-2025-administrators` | Admins do projeto | Tudo (manage all-resources) |
| `hackathon-2025-data-engineers` | Eng. de dados | Gerenciar storage, Data Flow, ler network |
| `hackathon-2025-data-scientists` | Cientistas de dados | Gerenciar notebooks, ler dados, escrever modelos |

**Policy especial: `dataflow-service-policy`**
O serviço Data Flow da OCI é um "robô" que executa jobs Spark. Ele precisa de permissão explícita para acessar seus buckets e subnets. Sem essa policy, os jobs falham com erro de acesso negado.

### O que foi criado (Fase 1)

| Arquivo | Propósito |
|---------|-----------|
| `terraform/modules/iam/main.tf` | Define 6 compartments + 3 groups + 4 policies |
| `terraform/modules/iam/variables.tf` | Entradas do módulo (tenancy_ocid, project_name, tags) |
| `terraform/modules/iam/outputs.tf` | Saídas: 6 compartment IDs + 3 group IDs |
| `terraform/environments/prod/main.tf` | Orquestra o módulo IAM (Fases 2-4 comentadas) |
| `terraform/environments/prod/variables.tf` | Todas as variáveis (Fases 1-4 declaradas) |
| `terraform/environments/prod/outputs.tf` | Outputs Fase 1 (Fases 2-4 comentadas) |
| `terraform/scripts/apply_phase.sh` | Script para apply incremental por fase |

### Padrão de Módulos Terraform

Cada módulo segue a mesma estrutura de 3 arquivos:

```
variables.tf (ENTRADAS)     main.tf (RECEITA)       outputs.tf (SAÍDAS)
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│ tenancy_ocid     │ ──→ │ Cria compartments│ ──→ │ project_compartment_id│
│ project_name     │     │ Cria groups      │     │ network_compartment_id│
│ tags             │     │ Cria policies    │     │ storage_compartment_id│
└─────────────────┘     └──────────────────┘     │ compute_compartment_id│
                                                  └──────────────────────┘
```

As **saídas** de um módulo viram **entradas** do próximo. Exemplo:
- Módulo IAM gera `network_compartment_id`
- Módulo Network recebe `network_compartment_id` para saber onde criar a VCN

### Implementação Incremental

O `main.tf` do environment tem as Fases 2-4 **comentadas**. Isso permite:
1. Aplicar Fase 1 (IAM) e validar
2. Descomentar Fase 2 (Network), aplicar e validar
3. Descomentar Fase 3 (Storage), aplicar e validar
4. E assim por diante...

Mesma filosofia do Medallion Architecture do projeto: cada camada é independente e validada antes de avançar.

### Validação da Fase 1

```bash
cd mig_oci/terraform/scripts
./apply_phase.sh 1
```

**O que o Terraform vai criar (13 recursos):**
- 6 compartments (project + 5 sub)
- 3 groups (administrators, data-engineers, data-scientists)
- 4 policies (admin, data-engineers, data-scientists, dataflow-service)

**Validação no Console OCI:**
1. Identity > Compartments > Ver "hackathon-2025" + 5 sub-compartments
2. Identity > Groups > Ver 3 grupos com prefixo "hackathon-2025-"
3. Identity > Policies > Ver 4 policies

**Validação via Terraform:**
```bash
cd mig_oci/terraform/environments/prod
terraform output compartment_ids    # Mostra 6 OCIDs
terraform output group_ids          # Mostra 3 OCIDs
```

---

## Estrutura Atual do Projeto

```
mig_oci/
├── .gitignore                                    # Proteção de credenciais
├── README.md                                     # Guia rápido
│
├── terraform/
│   ├── modules/
│   │   ├── iam/                                  # ✅ Fase 1 (criado)
│   │   │   ├── main.tf                           # Compartments + Groups + Policies
│   │   │   ├── variables.tf                      # Entradas do módulo
│   │   │   └── outputs.tf                        # Saídas (IDs)
│   │   ├── network/                              # ⏳ Fase 2 (vazio)
│   │   ├── storage/                              # ⏳ Fase 3 (vazio)
│   │   ├── compute/                              # ⏳ Fase 4 (vazio)
│   │   └── security/                             # ⏳ Fase 5 (vazio)
│   │
│   ├── environments/prod/
│   │   ├── versions.tf                           # ✅ Provider OCI v5.47.0
│   │   ├── backend.tf                            # ✅ State local
│   │   ├── main.tf                               # ✅ Orquestra IAM (Fases 2-4 comentadas)
│   │   ├── variables.tf                          # ✅ Todas variáveis declaradas
│   │   ├── outputs.tf                            # ✅ Outputs Fase 1
│   │   └── terraform.tfvars.example              # ✅ Template configuração
│   │
│   └── scripts/
│       ├── init.sh                               # ✅ Validação inicial
│       └── apply_phase.sh                        # ✅ Apply incremental
│
├── airflow/                                      # ⏳ Futuro
├── data_upload/                                  # ⏳ Futuro
└── docs/
    └── FASE_0_1_IMPLEMENTACAO.md                 # ✅ Este documento
```

---

## Próximos Passos

| Fase | Status | O que faz | Dependência |
|------|--------|-----------|-------------|
| **Fase 0** | ✅ Concluída | Setup + validação credenciais | - |
| **Fase 1** | ✅ Aplicada | IAM (6 compartments, 3 groups, 4 policies) | Fase 0 |
| **Fase 2** | ✅ Aplicada | Network (VCN, 3 subnets, 3 gateways) | Fase 1 |
| **Fase 3** | ✅ Aplicada | Storage (6 buckets Object Storage) | Fase 1 |
| **Fase 4** | ⏳ Aguardando | Compute (4 Data Flow applications) | Fases 1-3 |
| **Fase 5** | ⏳ Opcional | Security (Vault + Master Key) | Fase 1 |
| **Fase 6** | ⏳ Aguardando | Upload de dados + scripts Python | Fase 3 |

**Documentação das próximas fases:** `mig_oci/docs/FASE_2_3_IMPLEMENTACAO.md`

**Fluxo para avançar:**
1. Executar `./apply_phase.sh 1` (criar IAM na OCI)
2. Validar no Console OCI
3. Descomentar módulo network em `main.tf` e `outputs.tf`
4. Executar `./apply_phase.sh 2`
5. Repetir para cada fase

---

## Glossário

| Termo | Significado |
|-------|-------------|
| **OCID** | Oracle Cloud Identifier - ID único de cada recurso na OCI |
| **Tenancy** | Sua conta OCI (o "prédio" principal) |
| **Compartment** | "Pasta" que organiza recursos dentro da tenancy |
| **IAM** | Identity and Access Management - controle de acesso |
| **Policy** | Regra que define quem pode fazer o quê |
| **Provider** | Plugin que conecta Terraform a um cloud provider (OCI) |
| **State** | Arquivo que o Terraform usa para lembrar o que já criou |
| **Module** | "Receita" reutilizável de Terraform (conjunto de recursos) |
| **Backend** | Onde o state é armazenado (local ou remoto) |
| **Plan** | Preview do que o Terraform vai criar/alterar/destruir |
| **Apply** | Executar o plan e criar os recursos na OCI |
| **API Key** | Chave criptográfica para autenticação programática na OCI |
| **VCN** | Virtual Cloud Network - rede virtual na OCI |
| **Data Flow** | Serviço OCI para executar jobs Apache Spark |
