# Camada Silver - Índice de Documentação Técnica

## Visão Geral

A camada **Silver** é a segunda camada na arquitetura Medallion. Sua função é:
- Aplicar tipagem explícita (string → int, double, date)
- Tratar valores sentinela (converter para NULL + criar flags)
- Criar colunas derivadas (DT_SAFRA, IDADE_ANOS, etc.)
- Garantir grão correto (deduplicação quando necessário)
- Validar domínios (quality gates)

**Princípio fundamental:** Dados limpos, tipados e validados, prontos para a camada Gold.

---

## Scripts e Documentação

| # | Script | Documentação | Descrição |
|---|--------|--------------|-----------|
| 00 | `00_bronze_silver_bureau.py` | [01_BUREAU_EXPLAINED.md](01_BUREAU_EXPLAINED.md) | Spine principal: scores, labels, grão 1:1 |
| 01 | `01_bronze_silver_telco.py` | [02_TELCO_EXPLAINED.md](02_TELCO_EXPLAINED.md) | Variáveis anônimas de uso (var_26-93), sentinela 304 |
| 02 | `02_bronze_silver_cadastro.py` | [03_CADASTRO_EXPLAINED.md](03_CADASTRO_EXPLAINED.md) | Dados demográficos, parsing de data de nascimento |
| 03 | `03_bronze_silver_recarga.py` | [04_RECARGA_EXPLAINED.md](04_RECARGA_EXPLAINED.md) | Eventos de recarga, SOS, valores monetários |
| 04 | `04_bronze_silver_pagamento.py` | [05_PAGAMENTO_EXPLAINED.md](05_PAGAMENTO_EXPLAINED.md) | Pagamentos de faturas, juros, descontos |
| 05 | `05_bronze_silver_atraso.py` | [06_ATRASO_EXPLAINED.md](06_ATRASO_EXPLAINED.md) | Faturas em aberto, aging, snapshot mensal |

---

## Ordem de Execução do Pipeline

```
00_bronze_silver_bureau.py    ← PRIMEIRO (cria o spine)
        │
        ├── 01_bronze_silver_telco.py
        ├── 02_bronze_silver_cadastro.py
        ├── 03_bronze_silver_recarga.py
        ├── 04_bronze_silver_pagamento.py
        └── 05_bronze_silver_atraso.py
```

**Nota:** Bureau é o spine (universo oficial). Os demais podem rodar em paralelo.

---

## Padrões Comuns (Todos os Scripts)

### Imports Padrão

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import (
    get_spark_session,
    standardize_column_names,
    to_int_safe,
    to_double_safe,
    to_date_safe,
    treat_sentinel_value
)
```

### Configuração de Caminhos

```python
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/<fonte>_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/<fonte>_silver_delta/"
DEFAULT_FORMAT = "delta"
```

### Estrutura do main()

```python
def main():
    # 1. Parse de argumentos (parse_known_args para compatibilidade Databricks)
    # 2. Leitura da Bronze
    # 3. Padronização de nomes (standardize_column_names)
    # 4. Transformações Silver (build_silver)
    # 5. Deduplicação (se necessário)
    # 6. Quality checks
    # 7. Escrita em Delta + Unity Catalog
```

---

## Diferenças Entre Scripts

| Script | Grão | Deduplicação | Sentinelas | Particularidade |
|--------|------|--------------|------------|-----------------|
| **Bureau** | 1:1 CPF+SAFRA | row_number by ingestão | Score=0 → NULL | Spine oficial |
| **Telco** | 1:1 CPF+SAFRA | row_number by ingestão | 304 → NULL | 68 variáveis anônimas |
| **Cadastro** | 1:1 CPF+SAFRA | row_number by ingestão | N/A | Parse de data de nascimento |
| **Recarga** | Evento-level | hash de event_key | -1/-2/-3 → flag | Base transacional (100M+) |
| **Pagamento** | Evento-level | row_number by versão | -1/-2/-3 → flag | Versionamento de faturas |
| **Atraso** | Evento-level | Sem dedup agressiva | -1/-2/-3 → flag | Snapshot mensal |

---

## Funções Utilitárias (`src/utils/spark_utils.py`)

| Função | Propósito | Uso |
|--------|-----------|-----|
| `get_spark_session(app_name)` | Cria SparkSession com Delta Lake | Em todos os scripts |
| `standardize_column_names(df)` | snake_case + remove acentos | Logo após leitura |
| `to_int_safe(colname)` | String → int (NULL se inválido) | Labels, flags |
| `to_double_safe(colname)` | String → double (NULL se inválido) | Valores monetários |
| `to_date_safe(colname, format)` | String → date (tolerante) | Datas |
| `treat_sentinel_value(col, sentinels)` | Cria coluna adj + flag | Telco (304) |

---

## Quality Gates Comuns

Todos os scripts Silver implementam validações:

```python
# Gate 1: Chaves não nulas
assert df.filter(F.col("num_cpf").isNull()).count() == 0

# Gate 2: Labels em domínio válido
df = df.withColumn(
    "flag_instalacao_invalida",
    F.when(~F.col("flag_instalacao_int").isin(0, 1), 1).otherwise(0)
)

# Gate 3: Grão 1:1 (onde aplicável)
distinct_key = df.select("num_cpf", "safra").distinct().count()
assert distinct_key == df.count()
```

---

## Metadados Adicionados

Cada script Silver adiciona:

```python
df = df.withColumn("metadata_data_transformacao", F.current_timestamp())
df = df.withColumn("metadata_versao_regra", F.lit("silver_<fonte>_v1"))
```

E preserva os metadados da Bronze:
- `metadata_data_ingestao`
- `metadata_nome_arquivo_origem`
- `metadata_sistema_origem`

---

## Lições Aprendidas (Problemas e Soluções)

### 1. UDFs Falhando Silenciosamente

**Problema:** Python UDFs não funcionam bem no Unity Catalog / Photon.
**Solução:** Usar funções nativas do Spark (`F.to_date`, `F.regexp_extract`).
**Exemplo:** Cadastro - `idade_anos` estava 0% cobertura até trocar UDF por `F.to_date`.

### 2. Sentinela 304 em Telco

**Problema:** Valor 304 aparece em ~50% dos registros de variáveis Telco.
**Solução:** Converter para NULL + criar `flag_var_XX_missing`.
**Função:** `treat_sentinel_value(colname, sentinel_values=[304])`

### 3. Deduplicação de Eventos

**Problema:** Recarga tem duplicatas (~0.3%), Pagamento tem versionamento.
**Solução:**
- Recarga: Hash de colunas-chave → `event_key` → row_number
- Pagamento: Chave natural → row_number by timestamp DESC

### 4. Datas em Formatos Variados

**Problema:** `DATADENASCIMENTO` vem em múltiplos formatos.
**Solução:** `F.coalesce` com múltiplos `F.to_date`:

```python
F.coalesce(
    F.to_date(F.col("col"), "dd/MM/yyyy"),
    F.to_date(F.col("col"), "dd-MM-yyyy"),
    F.to_date(F.col("col"), "yyyy-MM-dd"),
    F.lit(None).cast("date")
)
```

---

## Checklist de Revisão (Silver)

Ao revisar ou criar um script Silver, verifique:

- [ ] `parse_known_args()` (não `parse_args()`)
- [ ] `standardize_column_names()` aplicado
- [ ] Tipagem explícita para todas as colunas importantes
- [ ] Sentinelas tratados (NULL + flag)
- [ ] DT_SAFRA derivada (YYYYMM → YYYY-MM-01)
- [ ] Deduplicação apropriada ao grão
- [ ] Quality gates implementados
- [ ] Metadados de transformação adicionados
- [ ] Escrita em Delta + Unity Catalog
- [ ] Logs com print (>>> prefixo)

---

## Próximo Passo

Após executar todos os scripts Silver, a camada Gold pode ser construída:

```
Silver Bureau ─────┐
Silver Telco ──────┤
Silver Cadastro ───┼──► Gold ABT v1-v6
Silver Recarga ────┤
Silver Pagamento ──┤
Silver Atraso ─────┘
```

Ver documentação da [Camada Gold](../gold/00_INDEX.md).
