# IAM — Usuários e Grupos da Equipe

> Documento de referência para gestão de acessos OCI do projeto Hackathon PodAcademy 2025.
> Atualizado em: 2026-02-20

---

## Como criar um usuário no Console OCI

1. Acesse: **Identity & Security → Identity → Users → Create User**
2. Preencha:
   - **Name:** `nome-sobrenome` (padrão consistente, ex: `lucas-melo`)
   - **Email:** e-mail corporativo (receberá link de ativação automático)
   - **Description:** papel no projeto (ex: `Engenheiro de Dados`)
3. Após criar, clique no usuário e acesse **Groups → Add User to Group**
4. O usuário recebe o e-mail de ativação e define a própria senha

> **Nota:** Usuários são criados no nível do **Tenancy** (não dentro de compartments). Os grupos controlam o acesso via policies.

---

## Regra de permissões no OCI IAM

- **Permissões são aditivas**: um usuário em múltiplos grupos herda a **união** de todas as permissões.
- **Não existe DENY explícito**: se qualquer grupo permite uma ação, ela é liberada.
- Um usuário pode estar em **1 ou mais grupos** simultaneamente.

---

## Grupos Criados (Fase 1 — Terraform)

### 1. `hackathon-2025-administrators`

**Descrição:** Acesso total a todos os recursos do projeto.

**Permissões:**
- `manage all-resources` no compartment `hackathon-2025`

**Quando atribuir:**
> Responsável técnico / arquiteto da solução. Gerencia infraestrutura, Terraform, IAM.

---

### 2. `hackathon-2025-data-engineers`

**Descrição:** Engenheiros de pipeline — Bronze, Silver, Gold.

**Permissões:**
| Recurso | Nível |
|---------|-------|
| Object Storage (todos os buckets) | `manage` — ler, escrever, deletar objetos |
| Data Flow (jobs Spark) | `manage` — criar, executar, monitorar |
| Network (VCN, subnets) | `read` + `use subnets` |

**Quando atribuir:**
> Quem executa e monitora o pipeline de dados no Data Flow, faz upload de scripts, acessa buckets diretamente.

---

### 3. `hackathon-2025-data-scientists`

**Descrição:** Cientistas e analistas de dados — notebooks e modelos.

**Permissões:**
| Recurso | Nível |
|---------|-------|
| Data Science (notebooks, modelos) | `manage` — criar e executar notebooks |
| Object Storage (todos os buckets) | `read` — somente leitura |
| Bucket `models` | `manage` — escrever modelos treinados |
| Network (VCN) | `read` |

**Quando atribuir:**
> Quem trabalha com análise exploratória, feature engineering e modelagem nos notebooks OCI Data Science.

---

### 4. `hackathon-2025-developers`

**Descrição:** Membros do time com acesso ao sandbox isolado + leitura em produção.

**Permissões:**
| Recurso | Nível |
|---------|-------|
| Compartment `dev-teste` | `manage all-resources` — acesso total no sandbox |
| Object Storage produção (bronze, silver, gold, models) | `read` — somente leitura |
| Network produção | `read` |

**Quando atribuir:**
> Qualquer membro que precisa explorar a OCI livremente (sem risco de afetar produção) ou consultar dados das camadas Silver/Gold/ABT. Bom perfil padrão para analistas de negócio e membros de suporte técnico.

---

## Combinações Recomendadas

| Perfil na Equipe | Grupos |
|------------------|--------|
| Responsável técnico / arquiteto | `administrators` |
| Engenheiro de dados (pipeline) | `data-engineers` + `developers` |
| Cientista / analista de dados | `data-scientists` + `developers` |
| Analista de negócio / suporte | `developers` |

> **Dica:** O grupo `developers` é um bom complemento para engenheiros e cientistas pois garante acesso ao sandbox dev-teste para testes locais — sem risco de impactar o pipeline de produção.

---

## Matriz de Acesso por Recurso

| Recurso OCI | administrators | data-engineers | data-scientists | developers |
|-------------|:--------------:|:--------------:|:---------------:|:----------:|
| Object Storage — leitura | ✅ | ✅ | ✅ | ✅ |
| Object Storage — escrita/deleção | ✅ | ✅ | ✅ (só models) | ❌ |
| Data Flow — executar jobs | ✅ | ✅ | ❌ | ❌ |
| Data Science — notebooks | ✅ | ❌ | ✅ | ❌ |
| Network — configurar | ✅ | ❌ | ❌ | ❌ |
| IAM — gerenciar usuários | ✅ | ❌ | ❌ | ❌ |
| Sandbox dev-teste — criar recursos | ✅ | ✅ (via developers) | ✅ (via developers) | ✅ |
| Terraform apply | ✅ | ❌ | ❌ | ❌ |

---

## Integrantes da Equipe

> Preencha abaixo após criar os usuários no Console OCI.
> Formato: Nome, e-mail, grupos atribuídos e data de criação.

| # | Nome | E-mail OCI | Grupos Atribuídos | Criado em |
|---|------|------------|-------------------|-----------|
| 1 |Michael Cruz|sv_yaco@hotmail.com|hackathon-2025-administrators|20/02/2026|
| 2 |Clarice Gouveia|clagouveia@gmail.com|hackathon-2025-developers|20/02/2026|
| 3 |Lucas Melo| | | |
| 4 |Cleben Garcia|clebenjuniorcgarcia@hotmail.com|hackathon-2025-developers|20/02/2026|
| 5 |Eric Chao | | | |
| 6 |Silvana Amaral|silvanaamaralpe@gmail.com|hackathon-2025-data-engineers , hackathon-2025-developers|20/02/2026|
| 7 |Alisson Silva|alisson.junio@hotmail.com|hackathon-2025-data-engineers , hackathon-2025-developers|20/02/2026|
| 8 |Eduardo Andrechuk|eduardoandrechuk@outlook.com|hackathon-2025-data-scientists , hackathon-2025-developers|20/02/2026|


---

## Observações de Acesso

> Espaço para registrar decisões específicas, exceções ou acessos temporários concedidos.

- _Nenhuma observação registrada ainda._

---

## Referências

- Terraform IAM: `mig_oci/terraform/modules/iam/main.tf`
- Documentação Fase 1: `mig_oci/docs/FASE_0_1_IMPLEMENTACAO.md`
- Console OCI: Identity & Security → Identity → Users / Groups
