# Scripts de Otimizacao — Gold Layer

Scripts otimizados para reduzir small files na Gold Layer.
**Os scripts originais em `scripts/` NAO sao modificados.**

## Problema

Apos execucao do pipeline, a Gold Layer tem ~5.884 objetos em apenas ~4.5 GB:

| Tabela | Arquivos | Tamanho/arquivo | Causa |
|--------|----------|-----------------|-------|
| gold_recarga_features | Centenas | < 1 MB | `partitionBy("safra_recarga")` + shuffle 200 |
| gold_pagamento_features | Centenas | < 1 MB | `partitionBy("safra_pagamento")` + shuffle 200 |
| gold_atraso_features | Centenas | < 1 MB | `partitionBy("safra_atraso")` + shuffle 200 |
| abt_v3 | 11 | ~22 MB | Sem coalesce |
| abt_v4 | 14 | ~17 MB | Sem coalesce |
| abt_v5 | 30 | ~19 MB | Sem coalesce |
| abt_v6 | 42 | ~22 MB | Sem coalesce |

## Solucao

### Abordagem 1: Scripts otimizados (substitui originais no pipeline)

9 scripts com coalesce/repartition dinamico (mesmo padrao do Silver):

| Script | Tipo | Otimizacao | Target |
|--------|------|------------|--------|
| `gold_recarga.py` | Features | `repartition(N, safra_recarga)` | ~128 MB/arq |
| `gold_pagamento.py` | Features | `repartition(N, safra_pagamento)` | ~128 MB/arq |
| `gold_atraso.py` | Features | `repartition(N, safra_atraso)` | ~128 MB/arq |
| `abt_v1_builder.py` | ABT | `coalesce(N)` | ~64 MB/arq |
| `abt_v2_builder.py` | ABT | `coalesce(N)` | ~64 MB/arq |
| `abt_v3_builder.py` | ABT | `coalesce(N)` | ~64 MB/arq |
| `abt_v4_builder.py` | ABT | `coalesce(N)` | ~64 MB/arq |
| `abt_v5_builder.py` | ABT | `coalesce(N)` | ~64 MB/arq |
| `abt_v6_builder.py` | ABT | `coalesce(N)` + VACUUM | ~64 MB/arq |

N e calculado em runtime: `max(1, int(count * BYTES_PER_ROW / 1024 / 1024 / TARGET_MB))`

### Abordagem 2: Compactacao pos-pipeline (legado)

`compactar_gold.py` — roda **apos** o pipeline como Data Flow app separado.
Util se os scripts originais nao puderem ser substituidos.

## Upload dos scripts otimizados

```bash
# Executar no terminal LOCAL com OCI CLI configurado
# A partir da raiz do projeto: mig_oci/data_upload/scripts_otimizacao/
cd mig_oci/data_upload/scripts_otimizacao

NAMESPACE=$(oci os ns get --query 'data' --raw-output)
BUCKET="hackathon-2025-pipeline-ops"
SCRIPTS_DIR="."

# Gold Features
for SCRIPT in gold_recarga.py gold_pagamento.py gold_atraso.py; do
  oci os object put -bn $BUCKET \
    --name "scripts/$SCRIPT" \
    --file "$SCRIPTS_DIR/$SCRIPT" --force
  echo "Uploaded: $SCRIPT"
done

# ABTs
for V in 1 2 3 4 5 6; do
  SCRIPT="abt_v${V}_builder.py"
  oci os object put -bn $BUCKET \
    --name "scripts/$SCRIPT" \
    --file "$SCRIPTS_DIR/$SCRIPT" --force
  echo "Uploaded: $SCRIPT"
done
```

## BYTES_PER_ROW — Calibracao

Estimativas iniciais (calibrar apos 1a execucao):

| Script | BYTES_PER_ROW | Justificativa |
|--------|---------------|---------------|
| gold_recarga | 200 | Muitas features M1/M3/M6 |
| gold_pagamento | 150 | Features pagamento |
| gold_atraso | 120 | Features atraso |
| abt_v1 | 50 | 1 feature (Score_01) |
| abt_v2 | 50 | 2 features |
| abt_v3 | 100 | 89 features |
| abt_v4 | 120 | 95 features |
| abt_v5 | 200 | 160 features |
| abt_v6 | 400 | 261 features (~614 colunas) |

**Formula de calibracao:** `BYTES = total_MB * 1024 * 1024 / count_records`
