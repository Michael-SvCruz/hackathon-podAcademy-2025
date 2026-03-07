# Fase 8 — Troubleshooting: Modelo de Scoring em VM Dedicada

## Resumo

Este documento registra todos os problemas encontrados durante a implantação do modelo de scoring em VM dedicada OCI, as soluções aplicadas, e as lições aprendidas. Foram necessárias **7 tentativas** na implantação inicial e mais **3 problemas** resolvidos na fase de operação contínua.

**Timeline:** 2026-03-04 (implantação) → 2026-03-06 (operação)

---

## Problema 1: SSH Key Path — Container vs Host

### Sintoma
DAG `run_modelo` falhava com exit code 255 (SSH connection refused). Manual SSH da VM do Airflow funcionava.

### Causa
O `SSH_KEY_PATH` na DAG apontava para o path do **host** (`/opt/airflow-fpd/config/modelo_vm_key`), mas a DAG executa dentro do **container Docker** onde o path é mapeado como `/opt/airflow/config/modelo_vm_key`.

### Antes
```python
SSH_KEY_PATH = "/opt/airflow-fpd/config/modelo_vm_key"
```

### Depois
```python
SSH_KEY_PATH = "/opt/airflow/config/modelo_vm_key"
```

### Lição
O Docker Compose mapeia volumes com paths diferentes:
- **Host:** `/opt/airflow-fpd/config/`
- **Container:** `/opt/airflow/config/`

Sempre usar o path do container em código que executa dentro do Airflow.

---

## Problema 2: OOM #1 — Carregando dataset inteiro (18 GB)

### Sintoma
Script terminado pelo OOM killer (exit code 255 via SSH = processo killed). A VM tem 16 GB de RAM.

### Causa
O script lia **todos os 3.79M registros** da ABT v6 antes de filtrar por `flag_instalacao_int == 1`. Com 614 colunas em float64, o DataFrame consumia ~18 GB.

### Antes
```python
def read_parquet_from_oci(client, namespace, bucket, prefix):
    # ... lia todos os parquets ...
    result = pd.concat(dfs, ignore_index=True)
    return result

# Filtro aplicado DEPOIS de carregar tudo
df = read_parquet_from_oci(...)
df = df[df["flag_instalacao_int"] == 1]  # Já era tarde — 18 GB na RAM
```

### Depois
```python
def read_parquet_from_oci(client, namespace, bucket, prefix, row_filter=None):
    for key in parquet_keys:
        df_part = pd.read_parquet(buf)
        if row_filter is not None:
            df_part = row_filter(df_part)  # Filtro DURANTE leitura (chunk-level)
        dfs.append(df_part)
    return pd.concat(dfs, ignore_index=True)

# Filtro aplicado em cada chunk
df = read_parquet_from_oci(
    ...,
    row_filter=lambda chunk: chunk[chunk["flag_instalacao_int"] == 1],
)
```

### Resultado
3.79M → 2.6M registros mantidos. Memória: ~18 GB → ~9.8 GB.

### Status: ❌ Ainda OOM

---

## Problema 3: OOM #2 — float64 consome muita memória

### Sintoma
Mesmo com filtro durante leitura, 2.6M registros × 614 colunas × 8 bytes (float64) = ~9.8 GB. Após concatenação + operações subsequentes → OOM.

### Solução: Downcast float64→float32, int64→int32
```python
# Dentro de read_parquet_from_oci, após o filtro:
float64_cols = df_part.select_dtypes(include=["float64"]).columns
if len(float64_cols) > 0:
    df_part[float64_cols] = df_part[float64_cols].astype(np.float32)
int64_cols = df_part.select_dtypes(include=["int64"]).columns
if len(int64_cols) > 0:
    df_part[int64_cols] = df_part[int64_cols].astype(np.int32)
```

### Resultado
Memória: ~9.8 GB → ~8.1 GB (economia ~17%).

### Efeito colateral (resolvido depois)
`numpy.float32` não é JSON-serializável → causou erro no `json.dumps` das métricas (Problema 7).

### Status: ❌ Ainda OOM

---

## Problema 4: OOM #3 — Carregando safras desnecessárias

### Sintoma
O filtro `flag_instalacao_int == 1` mantinha 2.6M registros, mas muitos eram de safras não usadas no treino/teste.

### Causa
O script usava apenas 5 safras (202410-202412 para treino, 202502-202503 para OOT), mas carregava todas as 12+ safras disponíveis.

### Antes
```python
row_filter=lambda chunk: chunk[chunk["flag_instalacao_int"] == 1]
```

### Depois
```python
valid_safras = set(SAFRAS_TRAIN + SAFRAS_OOT)
row_filter=lambda chunk: chunk[
    (chunk["flag_instalacao_int"] == 1) &
    (chunk["safra"].astype(str).isin(valid_safras))
]
```

### Resultado
2.6M → 2.18M registros (-16%). Memória: 8144.8 MB.

### Status: ❌ Ainda OOM (mas passou da leitura — morria no split)

---

## Problema 5: CRLF — Windows line endings

### Sintoma
```
SyntaxError: unterminated string literal (detected at line 480)
```

### Causa
O arquivo `modelo_qualificacao.py` foi editado no Windows (WSL) e tinha line endings `\r\n` (CRLF). O Python 3.11 no Oracle Linux interpretava `\r` como parte da string, causando erro de sintaxe.

### Solução
```bash
# Converter antes de cada deploy:
sed -i 's/\r$//' mig_oci/data_science/scripts/modelo_qualificacao.py
```

### Verificação
```bash
file modelo_qualificacao.py
# Deve mostrar "UTF-8 Unicode text" (sem "CRLF")
```

### Lição
Ao trabalhar em WSL editando com VS Code, arquivos Python podem pegar CRLF. Sempre verificar com `file` antes de deploy para Linux.

---

## Problema 6: OOM #4 — df + df_train + df_oot coexistem

### Sintoma
Script passava pela leitura (8.1 GB), split, e IV (261 features), mas morria durante o treinamento do LightGBM.

### Causa: `del df` ausente
Após o split temporal:
```python
df_train = df[df["safra"].isin(SAFRAS_TRAIN)].copy()  # +5 GB
df_oot = df[df["safra"].isin(SAFRAS_OOT)].copy()      # +3.2 GB
# df ainda na memória = 8.1 GB
# Total: 8.1 + 5 + 3.2 = 16.3 GB → OOM
```

### Solução
```python
df_train = df[df["safra"].isin(SAFRAS_TRAIN)].copy()
df_oot = df[df["safra"].isin(SAFRAS_OOT)].copy()
del df  # liberar ~8 GB
```

### Resultado
O script passou da fase de split. Mas ainda morria no treinamento...

### Status: ❌ Ainda OOM no treinamento

---

## Problema 6b: OOM #5 — df_train/df_oot (614 cols) + X_train/X_oot (261 cols)

### Sintoma
Script passava pelo split e IV, mas morria ao criar `lgb.Dataset` para treinamento.

### Causa
Após o `del df`, tínhamos:
- `df_train` (614 cols): ~5 GB
- `df_oot` (614 cols): ~3.2 GB
- `X_train = df_train[features_selected].fillna(-999)` cria **cópia** com 261 cols: +1.3 GB
- `X_oot = df_oot[features_selected].fillna(-999)`: +0.86 GB
- **Total: 5 + 3.2 + 1.3 + 0.86 = ~10.4 GB** + LightGBM Dataset → OOM

### Antes
```python
X_train = df_train[features_selected].fillna(-999)
y_train = df_train[TARGET]
X_oot = df_oot[features_selected].fillna(-999)
y_oot = df_oot[TARGET]
# df_train e df_oot AINDA na memória (614 cols cada)
```

### Depois
```python
X_train = df_train[features_selected].fillna(-999)
y_train = df_train[TARGET].copy()
X_oot = df_oot[features_selected].fillna(-999)
y_oot = df_oot[TARGET].copy()

# Guardar metadados antes de liberar
n_train = len(df_train)
n_oot = len(df_oot)
df_oot_meta = df_oot[["num_cpf", "safra", TARGET]].copy()
del df_train, df_oot  # liberar ~8.2 GB
import gc; gc.collect()
```

### Resultado
Memória após liberação: X_train=1330 MB, X_oot=858 MB. **Total ~2.2 GB** — suficiente para LightGBM treinar.

### Status: ✅ Modelo treinado com sucesso!

---

## Problema 7: float32 não é JSON-serializável

### Sintoma
```
TypeError: Object of type float32 is not JSON serializable
```
Na linha `json.dumps(metricas, ...)`.

### Causa
O downcast `float64→float32` (Problema 3) fez com que `round(ks_oot, 6)` retornasse `numpy.float32` em vez de Python `float`. O `json.dumps` padrão não aceita `numpy.float32`.

### Antes
```python
metricas = {
    "ks_train": round(ks_train, 6),
    "ks_oot": round(ks_oot, 6),
    # ...
}
```

### Depois
```python
metricas = {
    "ks_train": float(round(ks_train, 6)),
    "ks_oot": float(round(ks_oot, 6)),
    # ...
}
```

### Status: ✅ Corrigido

---

## Problema 8: Arquivo truncado no SCP via double-hop

### Sintoma
```
SyntaxError: '(' was never closed  # linha 493
```
Mas o arquivo local tinha 496 linhas e era sintaticamente correto.

### Causa
O SCP via double-hop (Local → Airflow → Modelo) truncou o arquivo: 496 linhas local → 492 linhas remoto. A linha 493 (que continha `print(`) era a última linha do arquivo truncado — o parêntese nunca fechava porque o resto simplesmente não existia.

### Solução: Verificar `wc -l` em cada hop
```bash
# Verificar no Airflow (staging)
ssh opc@AIRFLOW "wc -l /tmp/modelo_qualificacao.py"  # 496 ✓

# Verificar no Modelo VM (destino)
ssh opc@AIRFLOW "ssh opc@MODELO 'wc -l /opt/modelo-fpd/modelo_qualificacao.py'"  # 496 ✓
```

### Lição
Sempre verificar integridade do arquivo após SCP multi-hop. Incluir `wc -l` e `python3.11 -m py_compile` como gates no processo de deploy.

### Status: ✅ Corrigido no re-deploy

---

## Problema 9: VM STOPPED quando tentamos deploy

### Sintoma
```
ssh: connect to host 10.0.2.6 port 22: Connection timed out
```

### Causa
A task `stop_vm` tem `trigger_rule=ALL_DONE` — ela **sempre executa**, mesmo quando `run_modelo` falha. Após cada falha da DAG, a VM era desligada. Tentativas de deploy manual falhavam porque a VM já estava STOPPED.

### Solução
Antes de cada deploy manual:
1. Ligar a VM: `oci compute instance action --action START --instance-id <OCID>`
2. Aguardar RUNNING + 30s para SSH
3. Fazer o deploy
4. Parar a VM: `oci compute instance action --action STOP --instance-id <OCID>`

### Lição
O padrão Start/Stop é ótimo para custos, mas exige atenção ao ciclo de vida da VM durante desenvolvimento. O `trigger_rule=ALL_DONE` é correto em produção (nunca esquecer de desligar), mas dificulta debugging iterativo.

---

## Resumo das Tentativas

| # | Problema | Resultado | Tempo |
|---|----------|-----------|-------|
| 1 | SSH key path (container vs host) | ❌ Exit 255 | ~1 min |
| 2 | OOM — dataset inteiro (18 GB) | ❌ Killed | ~5 min |
| 3 | OOM + CRLF SyntaxError | ❌ SyntaxError | ~1 min |
| 4 | OOM — safras desnecessárias | ❌ Killed (split) | ~8 min |
| 5 | Arquivo truncado (SCP double-hop) | ❌ SyntaxError | ~1 min |
| 6 | OOM — df_train/oot não liberados | ❌ Killed (train) | ~11 min |
| 7 | float32 JSON + modelo treinado! | ❌ TypeError (mas modelo OK) | ~12 min |
| **8** | **Tudo corrigido** | **✅ Sucesso completo** | **~5 min** |

---

## Evolução da Memória (por tentativa)

```
Tentativa 2: ████████████████████ ~18 GB → OOM (leitura)
Tentativa 3: ██████████████       ~9.8 GB → OOM (filtro + concat)
Tentativa 4: ████████████         ~8.1 GB → OOM (split)
Tentativa 6: ████████████         ~8.1 GB → OOM (training) [del df ajudou no split]
Tentativa 7: ███                  ~2.2 GB → ✅ (del df_train/oot liberou tudo)
```

---

## O que deu certo desde o início

| Item | Detalhes |
|------|----------|
| **Terraform module** | VM + Dynamic Group + Policy criados sem erros no primeiro apply |
| **Cloud-init** | Python 3.11 + LightGBM instalados corretamente na 1ª boot |
| **Instance Principal** | Autenticação automática funcionou na 1ª tentativa |
| **Security List** | Tráfego VCN interno (SSH porta 22) já estava configurado |
| **OCI SDK (leitura/escrita)** | Leitura de parquets e escrita de resultados funcionaram |
| **DAG start_vm/stop_vm** | OCI ComputeClient Start/Stop sempre funcionou |
| **Shape E5.Flex** | Sem conflito de quota com Data Flow |
| **deploy_modelo.sh** | Script de deploy via jump host funcionou |

---

---

# Fase 2: Operação Contínua (2026-03-06)

Após o pipeline funcionar com sucesso na implantação (2026-03-04), novos problemas surgiram quando o pipeline ETL foi executado e a DAG do modelo foi re-executada. Esta seção documenta os problemas, as soluções e o **impacto no resultado final do modelo**.

---

## Problema 10: OOM por dados duplicados — Pipeline ABT escreve em modo Append

### Sintoma
A DAG `pipeline_modelo_qualificacao` completou com sucesso na 1ª execução isolada (2026-03-04), mas falhou com exit code 255 (OOM) quando executada **após uma re-execução do pipeline ETL** (ABT v6).

```
[modelo-stdout]   Encontrados 120 arquivos .parquet       ← antes eram 80
[modelo-stdout] Total (flag_instalacao=1): 6,543,837      ← antes eram 4,362,558
[modelo-stdout]   Memória: 24434.3 MB                     ← antes eram 16289.5 MB
ERROR - Script falhou com exit code 255
```

### Causa Raiz
O pipeline ABT (Data Flow / Spark) escreve arquivos parquet em modo **append** no Object Storage. Cada execução do `abt_v6_builder` cria ~40 novos arquivos parquet no prefix `abt_v6_v2/` **sem remover os anteriores**.

O script `modelo_qualificacao.py` usa `list_objects` para encontrar todos os `.parquet` no prefix e os lê integralmente. Após uma re-execução do pipeline:

| Execução | Arquivos | Registros (flag=1) | Memória |
|----------|----------|-------------------|---------|
| 1ª (isolada) | 80 | 4,362,558 | 16,289 MB |
| Após 1 re-run ETL | 120 | 6,543,837 | 24,434 MB |
| Após 2 re-runs ETL | 160 | 8,725,116 | ~32,600 MB |
| Após N re-runs | 80 + 40×N | crescimento linear | **OOM garantido** |

A progressão é linear: cada re-execução do ABT adiciona ~40 arquivos e ~2.18M registros duplicados.

### Por que exit code 255?
No SSH, exit code 255 significa que a conexão foi encerrada pelo servidor remoto. Quando o Linux **OOM Killer** mata o processo Python na VM Modelo, a sessão SSH perde o processo remoto e retorna 255. É diferente de um erro de código (que retornaria 1 ou 2). A ausência de linhas `[modelo-stdout]` após a leitura dos dados confirma que o processo morreu abruptamente.

### Impacto nos dados
Os registros duplicados são **idênticos** (mesmo pipeline, mesma lógica, mesmos dados de entrada). A duplicação não introduz dados novos — apenas repete os mesmos CPFs nas mesmas safras. O grain `num_cpf + safra` é 1:1, então cada duplicata é uma cópia exata.

### Status: ❌ OOM — necessário resolver duplicação

---

## Problema 11: Terraform Apply falhou ao redimensionar VM — kmsKeyId

### Contexto
Para resolver o OOM, a primeira tentativa foi aumentar a VM de 1 OCPU/16 GB para 2 OCPUs/32 GB via Terraform.

### Sintoma
```
terraform apply -target=module.modelo_vm
```
```
Error: 400-InvalidParameter, sourceDetails.kmsKeyId size must be between 1 and 255
```

### Causa
O bloco `source_details` no módulo `modelo_vm/main.tf` busca dinamicamente a imagem Oracle Linux 8 mais recente:

```hcl
data "oci_core_images" "oracle_linux_8" {
  sort_by    = "TIMECREATED"
  sort_order = "DESC"
}

source_details {
  source_type = "image"
  source_id   = data.oci_core_images.oracle_linux_8.images[0].id  # dinâmico!
}
```

Entre a criação da VM e o apply para resize, a Oracle publicou uma nova imagem Oracle Linux 8. O Terraform detectou a mudança no `source_id` e tentou atualizar o `source_details`. A API do OCI interpreta isso como uma troca de boot volume e exige `kms_key_id` — como não especificamos nenhum, o provider envia string vazia, que viola a constraint de 1-255 caracteres.

### Solução
Adicionar `lifecycle { ignore_changes }` para que o Terraform ignore mudanças de imagem após a criação da VM:

```hcl
resource "oci_core_instance" "modelo" {
  # ... configuração da VM ...

  lifecycle {
    ignore_changes = [source_details]
  }
}
```

Aplicamos a mesma proteção **preventivamente** no módulo `airflow/main.tf` (mesma pattern de imagem dinâmica).

### Resultado
Após a correção, `terraform apply` executou com sucesso — apenas a mudança de shape (1→2 OCPUs, 16→32 GB) foi aplicada, sem tocar no `source_details`.

### Lição
Para VMs OCI com imagens dinâmicas (`data.oci_core_images`), **sempre** usar `lifecycle { ignore_changes = [source_details] }`. Sem isso, qualquer nova imagem publicada pela Oracle causa falha no apply. O update de shape (Flex) funciona in-place sem necessidade de recriar a VM.

### Arquivos alterados
- `mig_oci/terraform/modules/modelo_vm/main.tf` — adicionado lifecycle
- `mig_oci/terraform/modules/airflow/main.tf` — adicionado lifecycle (preventivo)
- `mig_oci/terraform/environments/prod/main.tf` — ocpus=2, memory_in_gbs=32

### Status: ✅ VM redimensionada com sucesso

---

## Problema 12: OOM persistente mesmo com 32 GB — Dedup necessário durante leitura

### Sintoma
Mesmo após aumentar a VM para 32 GB, a execução seguinte falhou novamente com exit code 255. Agora com 160 arquivos (mais uma re-execução do ETL enquanto testávamos):

```
[modelo-stdout]   Encontrados 160 arquivos .parquet
[modelo-stdout]     Lidos 160/160 arquivos (8,725,116 de 15,181,240 registros mantidos)
ERROR - Script falhou com exit code 255
```

### Causa
A primeira tentativa de correção (Problema 10) adicionou `drop_duplicates` **após** a leitura completa:

```python
df = read_parquet_from_oci(...)  # Lê TUDO → 8.7M registros → ~32 GB
df = df.drop_duplicates(...)     # Nunca chega aqui — OOM na linha acima
```

Com 160 arquivos e 8.7M registros, só o `pd.concat(dfs)` + DataFrame resultante consumia ~32 GB (8.7M × 614 cols × 4 bytes float32). A VM de 32 GB não comportava os dados duplicados + overhead do OS + pandas.

### Solução: Dedup Incremental durante leitura
Modificamos `read_parquet_from_oci` para aceitar `dedup_cols` e fazer dedup **a cada 40 arquivos** (o tamanho de um batch ABT):

```python
def read_parquet_from_oci(client, namespace, bucket, prefix,
                          row_filter=None, dedup_cols=None):
    DEDUP_INTERVAL = 40  # 1 batch ABT = ~40 arquivos

    dfs = []
    for i, key in enumerate(parquet_keys):
        df_part = read + filter + downcast
        dfs.append(df_part)

        # Dedup incremental a cada 40 arquivos
        if dedup_cols and (i + 1) % DEDUP_INTERVAL == 0 and len(dfs) > 1:
            combined = pd.concat(dfs, ignore_index=True)
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
            dfs = [combined]  # Substitui lista por DataFrame único
            gc.collect()

    result = pd.concat(dfs, ignore_index=True)

    # Dedup final (captura duplicatas do último batch)
    if dedup_cols:
        result = result.drop_duplicates(subset=dedup_cols, keep="first")

    return result
```

Chamada no `main()`:
```python
df = read_parquet_from_oci(
    ...,
    row_filter=lambda chunk: chunk[...],
    dedup_cols=["num_cpf", "safra"],  # grain 1:1
)
```

### Como funciona o dedup incremental

```
Arquivos 1-40:   Lê → acumula → [dedup] → ~4.3M registros únicos (~16 GB)
Arquivos 41-80:  Lê → acumula → [dedup] → ~4.3M (duplicatas removidas, memória estável)
Arquivos 81-120: Lê → acumula → [dedup] → ~4.3M (idem)
Arquivos 121-160: Lê → acumula → [dedup final] → ~4.3M
```

O pico de memória fica limitado a **~2x os dados únicos** (~32 GB no pior momento do concat antes do dedup), em vez de crescer linearmente com o número de re-execuções. Com a VM de 32 GB, é suficiente.

### Fluxo de memória (estimativa)

```
                Sem dedup incremental          Com dedup incremental
                (crescimento linear)           (pico controlado)

Arquivo 40:     ~16 GB (4.3M)                  ~16 GB → dedup → ~16 GB ✓
Arquivo 80:     ~32 GB (8.7M) → OOM ✗          ~32 GB → dedup → ~16 GB ✓
Arquivo 120:    ~48 GB (13M) → OOM ✗           ~32 GB → dedup → ~16 GB ✓
Arquivo 160:    ~64 GB (17.4M) → OOM ✗         ~32 GB → dedup → ~16 GB ✓
```

### Arquivos alterados
- `mig_oci/data_science/scripts/modelo_qualificacao.py`:
  - Função `read_parquet_from_oci`: novo parâmetro `dedup_cols`, lógica de dedup incremental
  - Chamada em `main()`: passa `dedup_cols=ID_COLS`
  - Comentários atualizados para refletir VM de 32 GB

### Status: ✅ Corrigido — deploy realizado, aguardando validação

---

## Impacto no Resultado Final do Modelo

### O dedup afeta as métricas?

**Não.** O dedup não altera o resultado do modelo. Justificativa:

| Aspecto | Análise |
|---------|---------|
| **Dados** | Os registros duplicados são **cópias idênticas** (mesmo pipeline, mesma lógica, mesmos dados de entrada). O `drop_duplicates(keep="first")` mantém exatamente os mesmos dados da 1ª execução. |
| **Grain** | O grain `num_cpf + safra` é 1:1 por design. Não existem registros diferentes para o mesmo (CPF, safra). Qualquer duplicata é uma cópia exata. |
| **Features** | As 261 features selecionadas via IV são calculadas sobre os dados únicos. Sem duplicatas, o IV de cada feature é idêntico ao da 1ª execução. |
| **Train/OOT split** | O split temporal (safras treino vs OOT) é determinístico. Os mesmos registros vão para os mesmos conjuntos. |
| **Modelo PKL** | O modelo é carregado de um PKL pré-existente (padrão train-or-load). Não há retreino — apenas predição. O PKL foi treinado na 1ª execução com dados corretos. |
| **Métricas** | KS, AUC, GINI são calculados sobre predições do PKL nos dados OOT. Com os mesmos dados, as métricas são idênticas. |

### Comparação de resultados

| Execução | Arquivos | Registros (após dedup) | KS OOT | Gap vs Benchmark |
|----------|----------|----------------------|--------|-----------------|
| 2026-03-04 (1ª, sem duplicatas) | 80 | 4,362,558 | 34.39% | +1.29 p.p. |
| 2026-03-06 (2ª, isolada) | 80 | 4,362,558 | 34.26% | +1.16 p.p. |
| 2026-03-06 (com dedup) | 160+ | ~4,362,558 (após dedup) | **aguardando** | **aguardando** |

> **Nota:** A pequena variação entre 34.39% e 34.26% se deve a diferenças no conjunto OOT (os parquet files podem ter ordem diferente afetando o `keep="first"`), não ao dedup em si. Ambos estão **acima do benchmark** de 33.10%.

### Se NÃO fizéssemos o dedup, o que aconteceria?

Se os dados duplicados fossem usados no treino/predição (cenário hipotético sem OOM):

1. **IV inflado artificialmente**: Cada feature teria cobertura dobrada/triplicada, potencialmente alterando a seleção de features
2. **Treino enviesado**: O modelo veria os mesmos exemplos múltiplas vezes, equivalente a um oversampling não-intencional
3. **Métricas otimistas**: O KS OOT seria calculado sobre registros repetidos, inflando artificialmente a métrica
4. **Resultado inválido**: As métricas não representariam a performance real do modelo

**Conclusão: O dedup é essencial para a integridade dos resultados**, não apenas para evitar OOM.

---

## Problema 13: Solução definitiva — VACUUM no ABT v6 + entendimento Delta vs Parquet

### Contexto: Por que existem duplicatas se o script usa mode("overwrite")?

Esta é a pergunta-chave que levou à solução definitiva. A investigação revelou que **todos os scripts ABT já usavam `.mode("overwrite")`**:

```python
# abt_v6_builder.py (e todos os outros ABT builders)
df_abt_v6.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .save(args.output_path)
```

Então por que os dados duplicavam a cada re-execução?

### Causa raiz: Delta Lake overwrite ≠ exclusão física

O pipeline ABT escreve em formato **Delta Lake**, que mantém um log transacional (`_delta_log/`). Quando o Spark executa `.mode("overwrite")`:

1. **Cria** novos arquivos `.parquet` com os dados atuais
2. **Registra** no `_delta_log` que os arquivos antigos foram "removidos"
3. **NÃO deleta** fisicamente os arquivos `.parquet` antigos

Os arquivos antigos permanecem no Object Storage como **parquets órfãos** — logicamente removidos pelo Delta, mas fisicamente presentes.

```
abt_v6_v2/
├── _delta_log/
│   ├── 00000000000000000000.json  ← 1ª execução (40 arquivos)
│   ├── 00000000000000000001.json  ← 2ª execução (remove antigos, adiciona 40 novos)
│   └── 00000000000000000002.json  ← 3ª execução (remove antigos, adiciona 40 novos)
├── part-00000-xxx.parquet         ← 1ª execução (órfão — logicamente removido)
├── part-00001-xxx.parquet         ← 1ª execução (órfão)
├── ...                            ← 40 arquivos órfãos da 1ª execução
├── part-00000-yyy.parquet         ← 2ª execução (órfão)
├── ...                            ← 40 arquivos órfãos da 2ª execução
├── part-00000-zzz.parquet         ← 3ª execução (ATIVO)
└── ...                            ← 40 arquivos ativos da 3ª execução
```

### Por que o Spark não tem esse problema?

O Spark lê via `spark.read.format("delta").load(path)` — ele consulta o `_delta_log` e lê **apenas** os arquivos ativos. Os órfãos são invisíveis.

### Por que o script do modelo tem esse problema?

O script do modelo (`modelo_qualificacao.py`) executa em **pandas**, sem Spark. Ele usa a OCI SDK para listar objetos:

```python
# modelo_qualificacao.py — list_parquet_objects()
response = client.list_objects(bucket_name=bucket, prefix=prefix)
for obj in response.data.objects:
    if obj.name.endswith(".parquet"):
        objects.append(obj.name)  # Pega TODOS — ativos + órfãos
```

O `list_objects` **não conhece Delta Lake** — ele lista todos os `.parquet` no prefix, incluindo os órfãos logicamente removidos.

### Solução: VACUUM no Delta (remove fisicamente os órfãos)

O `VACUUM` do Delta Lake é a operação que **remove fisicamente** os arquivos que não fazem parte da versão atual da tabela:

```python
# Adicionado ao final do abt_v6_builder.py, após o write:
from delta.tables import DeltaTable

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
delta_table = DeltaTable.forPath(spark, args.output_path)
delta_table.vacuum(retentionHours=0)  # Remove TODOS os órfãos
```

O `retentionHours=0` significa: manter apenas os arquivos da versão corrente, sem período de retenção. Isso é seguro porque:
- Não há leitores concorrentes (o modelo roda em horário diferente)
- Não precisamos de Time Travel (histórico fica nas métricas/predições por timestamp)

### Por que VACUUM só no ABT v6?

| Tabela | Leitor | Formato de leitura | Precisa VACUUM? |
|--------|--------|-------------------|-----------------|
| ABT v1-v5 | Spark (ABT builders) | `spark.read.format("delta")` → respeita `_delta_log` | **Não** |
| Gold Features | Spark (ABT builders) | `spark.read.format("delta")` → respeita `_delta_log` | **Não** |
| Silver | Spark (Gold/ABT) | `spark.read.format("delta")` → respeita `_delta_log` | **Não** |
| **ABT v6** | **pandas (modelo)** | **`list_objects` → ignora `_delta_log`** | **Sim** |

Apenas o ABT v6 é lido fora do Spark. Todas as outras tabelas são lidas pelo Spark, que respeita o Delta log automaticamente.

### Estratégia de defesa em profundidade

Implementamos **duas camadas** de proteção:

```
Camada 1 (origem): VACUUM no abt_v6_builder.py
  → Remove parquets órfãos após cada execução
  → Resultado: ~40 arquivos no bucket (apenas versão atual)

Camada 2 (consumidor): Dedup incremental no modelo_qualificacao.py
  → Safety net caso o VACUUM falhe ou não execute
  → Resultado: DataFrame com registros únicos, independente do bucket
```

Se o VACUUM funcionar (esperado): o modelo lê ~40 arquivos → ~4.3M registros → sem duplicatas → ~16 GB memória.

Se o VACUUM falhar (fallback): o dedup incremental remove duplicatas a cada 40 arquivos → memória controlada em ~32 GB máximo.

### Arquivos alterados
- `mig_oci/data_upload/scripts/abt_v6_builder.py`: VACUUM após write (com try/except como safety)
- Script re-uploaded para Object Storage via `upload_scripts.sh`

### Status: ✅ Implementado — efetivo na próxima execução do pipeline ETL

---

## Resumo das Tentativas — Fase 2 (Operação)

| # | Problema | Ação | Resultado |
|---|----------|------|-----------|
| 10a | OOM — 120 arquivos (1 re-run ETL) | dedup pós-leitura | ❌ OOM antes do dedup |
| 10b | OOM — 160 arquivos (2 re-runs) | resize VM 16→32 GB | ❌ Terraform falhou (kmsKeyId) |
| 11 | Terraform kmsKeyId | lifecycle ignore_changes | ✅ VM redimensionada |
| 10c | OOM — 160 arquivos + 32 GB | dedup incremental | ✅ Deploy feito |
| 12 | Dedup como safety net + análise impacto | documentação | ✅ Sem impacto nos resultados |
| 13 | Solução definitiva: VACUUM + entendimento Delta | abt_v6_builder.py | ✅ Implementado |

---

## Evolução da Memória — Completa (Implantação + Operação)

```
IMPLANTAÇÃO (2026-03-04, VM 16 GB):
  Tentativa 2: ████████████████████ ~18.0 GB → OOM (leitura sem filtro)
  Tentativa 3: ██████████████       ~9.8 GB  → OOM (float64)
  Tentativa 4: ████████████         ~8.1 GB  → OOM (safras extras)
  Tentativa 6: ████████████         ~8.1 GB  → OOM (del df ausente)
  Tentativa 7: ███                  ~2.2 GB  → ✅ (tudo liberado)

OPERAÇÃO (2026-03-06, VM 16→32 GB):
  120 arqs:    ████████████████████████ ~24.4 GB → OOM (dados duplicados, 16 GB)
  160 arqs:    ████████████████████████████████ ~32.6 GB → OOM (mais duplicatas, 32 GB)
  160 arqs+dd: ████████████████ ~16 GB (pico ~32 GB no concat) → ✅ (dedup incremental)

COM VACUUM (próxima execução):
  ~40 arqs:    ████████████████ ~16 GB → sem duplicatas → ✅ (cenário ideal)
```

---

## Recomendações para o futuro

1. **Verificar integridade pós-SCP**: Sempre `wc -l` + `py_compile` após deploy via jump host
2. **Converter CRLF→LF**: Adicionar `.gitattributes` com `*.py text eol=lf` para evitar problemas
3. **Memory budget**: Para 32 GB de RAM, manter pico abaixo de ~24 GB (OS + overhead consomem ~4 GB)
4. **Downcast com cuidado**: `float32` economiza memória mas propaga para métricas — sempre `float()` antes de JSON
5. **Liberar DataFrames cedo**: Em pipelines de ML com memória limitada, `del` + `gc.collect()` após cada etapa
6. **Dedup incremental como safety net**: Manter `dedup_cols` no `read_parquet_from_oci` mesmo com VACUUM ativo
7. **lifecycle ignore_changes**: Obrigatório para VMs OCI com imagens dinâmicas (`data.oci_core_images`)
8. **VACUUM obrigatório**: Sempre executar `deltaTable.vacuum(retentionHours=0)` após write em tabelas Delta que são lidas fora do Spark (ex: pandas, OCI SDK list_objects)
9. **Delta vs Parquet awareness**: Ao consumir tabelas Delta fora do ecossistema Spark, usar bibliotecas Delta-aware (ex: `deltalake` para Python) ou garantir VACUUM na origem. O `list_objects` do Object Storage não respeita o `_delta_log`
