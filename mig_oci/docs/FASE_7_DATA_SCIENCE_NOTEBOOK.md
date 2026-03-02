# Fase 7 — Data Science Notebook (Exploração de Dados na OCI)

> **Status:** ✅ Notebook funcional na `private-data-subnet`. Leitura de buckets via SDK OCI Python + pandas confirmada.

---

## Visão Geral

Configuração do OCI Data Science Notebook Session para exploração interativa dos dados do pipeline Medallion (Bronze, Silver, Gold, ABT). Permite ao cientista de dados acessar os buckets Object Storage diretamente da OCI, sem precisar baixar dados localmente.

```
Cientista de Dados
    │
    ▼
OCI Data Science Notebook (JupyterLab)
    │  Shape: VM.Standard.E4.Flex (1 OCPU, 16 GB)
    │  Subnet: private-data-subnet
    │  Auth: Resource Principal (Dynamic Group)
    │
    ├──→ SDK OCI Python (oci.object_storage) → Buckets
    │       list_buckets, list_objects, get_object
    │
    └──→ PySpark 3.5 (conda env pré-instalado)
            Leitura Delta Lake + Parquet
```

---

## Contexto e Motivação

Após a conclusão do pipeline completo (Fase 6B), a equipe precisa:
1. **Validar dados** nos buckets OCI (contagem, schema, amostras)
2. **Explorar features** para refinamento do modelo
3. **Treinar modelos** diretamente na OCI (sem Databricks)
4. **Salvar artefatos** no bucket `models`

O OCI Data Science oferece JupyterLab gerenciado com conda environments pré-configurados (PySpark, TensorFlow, etc.), eliminando a necessidade de instalar Spark manualmente.

---

## Infraestrutura Criada

### Dynamic Groups (Terraform — módulo IAM)

Dois Dynamic Groups separam permissões por papel, baseado no compartment onde o notebook é criado:

| Dynamic Group | Matching Rule | Papel |
|---------------|---------------|-------|
| `hackathon-2025-datascience-notebooks` | `resource.type = 'datasciencenotebooksession'` em `dev-teste` | Cientistas de dados |
| `hackathon-2025-dataeng-notebooks` | `resource.type = 'datasciencenotebooksession'` em `compute` | Engenheiros de dados |

### Policies (Terraform — módulo IAM)

**Cientistas (acesso restrito):**
```
Allow dynamic-group hackathon-2025-datascience-notebooks to use virtual-network-family in compartment hackathon-2025
Allow dynamic-group hackathon-2025-datascience-notebooks to read object-family in compartment hackathon-2025 where target.bucket.name='hackathon-2025-gold-layer'
Allow dynamic-group hackathon-2025-datascience-notebooks to manage object-family in compartment hackathon-2025 where target.bucket.name='hackathon-2025-models'
```

**Engenheiros (acesso completo):**
```
Allow dynamic-group hackathon-2025-dataeng-notebooks to use virtual-network-family in compartment hackathon-2025
Allow dynamic-group hackathon-2025-dataeng-notebooks to manage object-family in compartment hackathon-2025
```

**Serviço Data Science (acesso à rede):**
```
Allow service datascience to use virtual-network-family in compartment hackathon-2025
```

> **Sem a policy do serviço**, o notebook falha com "The specified subnet is not accessible" — mesmo que o usuário tenha permissão total.

---

## Criação do Notebook Session

### Via Console OCI (UI)

O Console exige **Private Endpoint** para subnets privadas, o que adiciona complexidade desnecessária. Use a **OCI CLI** para criar diretamente.

### Via OCI CLI (Recomendado)

```bash
oci data-science notebook-session create \
  --compartment-id <OCID_COMPARTMENT_DEV_TESTE> \
  --project-id <OCID_PROJETO_DATA_SCIENCE> \
  --display-name <NOME> \
  --config-details '{
    "shape": "VM.Standard.E4.Flex",
    "blockStorageSizeInGBs": 50,
    "subnetId": "<OCID_PRIVATE_DATA_SUBNET>",
    "notebookSessionShapeConfigDetails": {
      "ocpus": 1,
      "memoryInGBs": 16
    }
  }'
```

**OCIDs do ambiente prod:**
```
compartment dev-teste:    ocid1.compartment.oc1..aaaaaaaa6o7pyvs5pigb6v5fjob5auipem5gpnppkhznkcimdezwtn5fhymq
private-data-subnet:      ocid1.subnet.oc1.sa-saopaulo-1.aaaaaaaayuarz7jupmubfqk5u5g7uwmb7vt4otm5kadolp4sluuuamh53vgq
```

### Configuração da Subnet

O notebook **deve** ser criado na `private-data-subnet` porque:
- Tem **Service Gateway** → acessa Object Storage pela rede interna Oracle (rápido, sem internet)
- Tem **NAT Gateway** → permite `pip install` e download de conda environments
- A `public-subnet` **não funciona** → OCI não permite IGW + Service Gateway "All Services" na mesma route table

---

## Ambiente PySpark

### Instalação do Conda Environment

No **Environment Explorer** do JupyterLab, instale **"PySpark 3.5 and Data Flow"** (1.34 GB). Inclui:
- PySpark 3.5.0
- Delta Lake
- OCI SDK Python
- PyArrow / Pandas

### Libs Adicionais

No terminal do notebook (ou célula com `!`):
```bash
!pip install deltalake
```

A lib `deltalake` permite ler tabelas Delta Lake sem Spark (mais leve para exploração).

---

## Padrão de Leitura de Dados

### Autenticação (Resource Principal)

```python
import oci

signer = oci.auth.signers.get_resource_principals_signer()
client = oci.object_storage.ObjectStorageClient({}, signer=signer)
namespace = client.get_namespace().data
```

### Listar Buckets

```python
# Buckets estão no compartment "storage" (sub de hackathon-2025)
compartment_storage = "ocid1.compartment.oc1..aaaaaaaa3w4bmj5ikhoxs6i5uh2xjvhje2ykaa4l5dk2q7tdd2s74tsgomzq"
response = client.list_buckets(namespace, compartment_storage)
for b in response.data:
    print(b.name)
```

> **Atenção:** `list_buckets` não é recursivo. Use o OCID do compartment `storage` (não do `hackathon-2025` raiz).

### Listar Objetos em um Prefixo

```python
bucket = "hackathon-2025-bronze-layer"
prefix = "atraso/"
response = client.list_objects(namespace, bucket, prefix=prefix, fields="name,size")

for obj in response.data.objects:
    size_mb = obj.size / (1024 * 1024)
    print(f"{obj.name:70s} {size_mb:8.1f} MB")
```

### Ler Parquet Puro (Gold Features)

```python
import pandas as pd
import io

bucket = "hackathon-2025-gold-layer"
prefix = "gold_recarga_features/"

response = client.list_objects(namespace, bucket, prefix=prefix, fields="name,size")
parquet_files = [o.name for o in response.data.objects if o.name.endswith(".parquet")]

# Ler primeiro arquivo para amostra
obj = client.get_object(namespace, bucket, parquet_files[0])
df = pd.read_parquet(io.BytesIO(obj.data.content))
print(f"Colunas: {df.shape[1]}, Registros (1 arquivo): {len(df):,}")
df.head(5)
```

### Ler Delta Lake (Bronze, Silver, ABT)

Para Delta, os arquivos `.parquet` são gerenciados pelo `_delta_log/`. Ler diretamente com pandas pode incluir dados deletados. Use:

```python
# Filtrar apenas parquet (excluir _delta_log)
parquet_files = [o.name for o in response.data.objects
                 if o.name.endswith(".parquet") and "_delta_log" not in o.name]
```

> **Nota:** Para leitura 100% correta de Delta, use a lib `deltalake` ou PySpark com o HDFS connector configurado.

---

## Formatos por Camada

| Bucket | Prefixo | Formato | Motivo |
|--------|---------|---------|--------|
| bronze-layer | `atraso/`, `bureau/`, etc. | **Delta Lake** | Scripts Bronze usam `.format("delta").save()` |
| silver-layer | `recarga/`, `telco/`, etc. | **Delta Lake** | Scripts Silver usam `.format("delta").save()` |
| gold-layer | `gold_recarga_features/`, `gold_pagamento_features/`, `gold_atraso_features/` | **Parquet puro** | Scripts Gold usam `.parquet(path)` |
| gold-layer | `abt_v1/` ... `abt_v6_v2/` | **Delta Lake** | Scripts ABT usam `.format("delta").save()` |

### Como Identificar

Verifique se existe `_delta_log/` no prefixo:
```python
resp = client.list_objects(namespace, bucket, prefix=prefix + "_delta_log/", limit=1)
is_delta = len(resp.data.objects) > 0
```

---

## Erros Encontrados e Soluções

### 1. "The specified subnet is not accessible"

**Causa:** Faltava a policy `Allow service datascience to use virtual-network-family in compartment hackathon-2025`.

**Solução:** Adicionada no módulo IAM (Terraform) junto com a policy do Data Flow service.

**Explicação:** Na OCI existem dois níveis de permissão:
- Policy do **usuário/Dynamic Group** → permite ao recurso criado acessar outros recursos
- Policy do **serviço** → permite ao serviço OCI provisionar infraestrutura (VMs, VNICs) na sua rede

### 2. Console OCI exige Private Endpoint para subnet privada

**Causa:** A UI do Console força Private Endpoint quando a subnet é privada.

**Solução:** Criar o notebook via **OCI CLI** (`oci data-science notebook-session create`), que permite especificar a subnet diretamente sem Private Endpoint.

### 3. IGW + Service Gateway "All Services" na mesma route table

**Causa:** Tentativa de adicionar Service Gateway à route table pública (que já tem Internet Gateway).

**Erro:** `400-InvalidParameter: Internet Gateway target cannot be used together with Service Gateway target for All Services`

**Solução:** Não mexer na route table pública. Usar a `private-data-subnet` (que já tem Service Gateway + NAT Gateway).

### 4. Spark HDFS connector — "Must specify tenantId, userId, fingerprint"

**Causa:** O HDFS connector OCI Java tenta autenticação via config file (`~/.oci/config`). No notebook Data Science, a autenticação é via Resource Principal.

**Solução alternativa:** Usar o SDK OCI Python (que suporta Resource Principal nativamente) + pandas para leitura, em vez de `spark.read.format("delta").load("oci://...")`.

### 5. `list_buckets` retorna vazio

**Causa:** Buckets estão no sub-compartment `storage`, não no compartment raiz `hackathon-2025`. A API não é recursiva.

**Solução:** Usar o OCID do compartment `storage` no `list_buckets()`.

### 6. `pip install` timeout (public subnet)

**Causa:** Notebook na public subnet não tem NAT Gateway para saída à internet.

**Solução:** Usar a `private-data-subnet` (tem NAT Gateway).

### 7. Environment Explorer vazio ("no results to show")

**Causa:** Mesma que #6 — sem rota para Service Gateway, o notebook não consegue baixar a lista de conda environments do Object Storage interno Oracle.

**Solução:** Usar a `private-data-subnet`.

---

## Lições Aprendidas

### 1. OCI CLI > Console para Data Science

O Console OCI adiciona restrições (Private Endpoint obrigatório) que a API/CLI não tem. Para cenários não-padrão, a CLI dá mais controle.

### 2. Private Data Subnet é o lugar certo

A `private-data-subnet` já tem Service Gateway + NAT Gateway — resolve todos os problemas de acesso. A `public-subnet` não serve para Data Science (sem Service Gateway, e não pode adicionar por restrição do IGW).

### 3. Dois níveis de IAM para serviços gerenciados

Para cada serviço OCI gerenciado (Data Science, Data Flow), são necessárias:
1. **Policy do serviço:** `Allow service <X> to use virtual-network-family` — provisionar infra
2. **Policy do recurso:** `Allow dynamic-group <X> to read/manage object-family` — acessar dados

### 4. SDK Python vs Spark HDFS para leitura

| Abordagem | Prós | Contras |
|-----------|------|---------|
| SDK OCI + pandas | Resource Principal funciona nativo, leve | Download sequencial, não escala |
| PySpark + HDFS connector | Paralelo, escala | Requer config de delegation token |
| Data Flow Session | Spark remoto, escala | Custo adicional, mais complexo |

Para exploração (amostras, contagem, schema), o SDK Python é suficiente. Para processamento pesado, usar Data Flow.

### 5. Segurança por compartment

O compartment onde o notebook é criado define suas permissões via Dynamic Group. Cientistas criam notebooks em `dev-teste` (acesso restrito), engenheiros em `compute` (acesso completo). Sem precisar gerenciar credenciais individuais.

---

## Verificação (Checklist)

- [ ] Notebook session ACTIVE na `private-data-subnet`
- [ ] `oci.auth.signers.get_resource_principals_signer()` sem erro
- [ ] `list_buckets` retorna 7 buckets (usando compartment `storage`)
- [ ] `list_objects` retorna arquivos no bronze/silver/gold
- [ ] `get_object` + `pd.read_parquet` lê conteúdo dos arquivos
- [ ] `pip install deltalake` funciona (NAT Gateway ativo)
- [ ] Conda environment "PySpark 3.5 and Data Flow" instalado

---

## Glossário

| Termo | Definição |
|-------|-----------|
| **Notebook Session** | VM gerenciada com JupyterLab, provisionada pelo serviço Data Science |
| **Resource Principal** | Mecanismo de autenticação para recursos OCI (notebooks, VMs) via Dynamic Group |
| **Dynamic Group** | Agrupa recursos OCI por regra (tipo + compartment) para aplicar policies |
| **Service Gateway** | Rota direta para serviços OCI internos (Object Storage) sem passar pela internet |
| **Private Endpoint** | Recurso de rede que expõe um serviço dentro da VCN (não necessário para nosso caso) |
| **HDFS Connector** | Componente Java que permite Spark ler `oci://` paths como se fossem HDFS |
| **Delegation Token** | Token temporário que o HDFS connector usa para autenticar via Resource Principal |
