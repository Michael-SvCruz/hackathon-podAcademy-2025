# Gold Atraso Features v2 - Documentacao Tecnica Detalhada

## Informacoes do Arquivo

| Item | Valor |
|------|-------|
| **Script** | `src/jobs/02_gold/gold_atraso_features_v2.py` |
| **Tipo** | Feature Generator (Silver → Gold) |
| **Input** | `silver/atraso_silver_delta/` (31.6M registros) |
| **Output** | `gold/atraso_features_v2_delta/` (15M registros) |
| **Grao Input** | Multiplas linhas por CPF (faturas em diferentes estados) |
| **Grao Output** | 1 linha por NUM_CPF + SAFRA_ATRASO |
| **Features Geradas** | 58 colunas |

---

## Arquitetura do Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SILVER ATRASO                                       │
│                                                                             │
│  ts_referencia  num_cpf    val_fat_aberto  dw_faixa_aging  ind_wo  ind_pdd │
│  2024-01-01     123...     150.00          0-30 dias       N       N        │
│  2024-01-01     123...     200.00          31-60 dias      N       N        │
│  2024-01-01     123...     500.00          >90 dias        S       S        │
│  2024-01-01     456...     80.00           0-30 dias       N       N        │
│                                                                             │
│  Grao: SNAPSHOT MENSAL - Estado de TODAS as faturas de um cliente          │
│  Volume: ~31.6 milhoes de registros                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOLD ATRASO FEATURES V2 (este script)                    │
│                                                                             │
│  1. Preparacao: safra_atraso, conversao de indicadores                     │
│  2. Classificacao de aging buckets (0-30, 31-60, 61-90, 90+)              │
│  3. Agregacao por NUM_CPF + SAFRA_ATRASO                                   │
│  4. Features derivadas (percentuais, ratios)                               │
│  5. Flags de comportamento (risco alto, atraso grave)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GOLD ATRASO FEATURES                               │
│                                                                             │
│  num_cpf  safra_atraso  qtd_faturas  pct_aging_90_plus  flag_risco_alto    │
│  123...   202401        3            58.82%             1                   │
│  456...   202401        1            0.00%              0                   │
│                                                                             │
│  Grao: 1 linha por cliente-mes (agregado)                                   │
│  Volume: ~15 milhoes de registros (compressao 2.1x)                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Conceito Chave: Snapshot Mensal vs Event-Level

### O que e Snapshot Mensal?

Diferente de Recarga (eventos de transacao) ou Pagamento (eventos de pagamento), **Atraso e um snapshot**:

| Caracteristica | Recarga/Pagamento | Atraso |
|----------------|-------------------|--------|
| **Tipo de dado** | Eventos (transacoes) | Snapshot (foto) |
| **Momento** | Quando ocorre | Sempre dia 01 do mes |
| **Multiplas linhas** | Cada transacao | Cada fatura em aberto |
| **Deduplicacao** | Necessaria (eventos duplicados) | Nao necessaria |

```
SNAPSHOT = "Fotografia" do estado de todas as faturas
           em um determinado momento (dia 01 de cada mes)

Cliente 123 em Janeiro/2024:
  - Fatura A: R$150 (0-30 dias)    ← Uma linha no snapshot
  - Fatura B: R$200 (31-60 dias)   ← Outra linha no snapshot
  - Fatura C: R$500 (>90 dias, WO) ← Outra linha no snapshot

O script AGREGA todas essas linhas em UMA linha por cliente-mes
```

### Por que isso importa para Machine Learning?

O snapshot captura o **estado acumulado** de inadimplencia, nao apenas eventos pontuais:
- Quantas faturas abertas?
- Qual a distribuicao de aging?
- Tem write-off (WO)?
- Quanto deve no total?

---

## Glossario de Termos de Atraso (Negocio)

| Termo | Significado | Relevancia ML |
|-------|-------------|---------------|
| **Aging** | Tempo de atraso da fatura | Quanto maior, pior o risco |
| **WO (Write-Off)** | Fatura "baixada" como perda | **Maximo risco** - ja deu default |
| **PDD** | Provisao para Devedores Duvidosos | **Alto risco** - empresa ja provisionou perda |
| **Fraude** | Indicador de fraude | **Critico** - comportamento fraudulento |
| **ACA** | Acordo de Pagamento | Sinal misto - tentando regularizar |
| **PCCR** | Programa de Recuperacao de Credito | Ja foi para cobranca |
| **Fatura Aberta** | Fatura nao paga | val_fat_aberto > 0 |

### Hierarquia de Gravidade (Aging)

```
     BAIXO RISCO                                      ALTO RISCO
          │                                                │
          ▼                                                ▼
    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────────┐
    │ 0-30    │    │ 31-60   │    │ 61-90   │    │ >90 dias    │
    │ dias    │───▶│ dias    │───▶│ dias    │───▶│ (GRAVE)     │
    └─────────┘    └─────────┘    └─────────┘    └─────────────┘
         │                                              │
         │                                              ▼
    "Atrasinho"                                ┌─────────────────┐
    (pode regularizar)                         │  WO / PDD       │
                                               │  (ja e perda)   │
                                               └─────────────────┘
```

---

## Codigo Explicado Linha por Linha

### 1. Imports e Configuracao

```python
import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

**Por que esses imports?**
- `sys`: Para `sys.exit(1)` em caso de erro fatal
- `argparse`: Flexibilidade de paths via linha de comando
- `Window`: Importado mas **nao usado** nesta versao (pronto para features temporais futuras)

```python
try:
    from src.utils.spark_utils import get_spark_session
except ImportError:
    def get_spark_session(app_name="Hackathon_App"):
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
```

**Por que try/except para import?**
- **Databricks**: `src.utils` esta no path → usa funcao do projeto
- **Local/Notebook**: Import falha → usa fallback inline
- Garante que o script roda em qualquer ambiente

### 2. Constantes de Configuracao

```python
DEFAULT_SILVER_ATRASO_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/atraso_features_v2_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_atraso_features_v2"
```

**Nomenclatura `_v2`:**
- Indica segunda versao do gerador de features
- Permite manter `_v1` para comparacao/rollback
- Pattern consistente: `gold_*_features_v2`

---

### 3. Funcao Principal: criar_features_atraso()

#### 3.1 Preparacao dos Dados

```python
def criar_features_atraso(df_silver: DataFrame) -> DataFrame:
    df = df_silver
```

**Por que `df = df_silver`?**
- Cria alias local para facilitar encadeamento
- Convencao: `df` para DataFrame em transformacao
- Nao faz copia - apenas referencia (Spark e lazy)

#### 3.2 Criacao de safra_atraso

```python
    if "safra_atraso" not in df.columns:
        df = df.withColumn(
            "safra_atraso",
            F.date_format(F.col("ts_referencia"), "yyyyMM")
        )
```

**O que faz?**
- Converte `ts_referencia` (timestamp do snapshot) para formato safra "202401"
- Condicional `if` evita recriar se ja existir

**Por que `date_format` e nao `substring`?**

| Metodo | Codigo | Problema |
|--------|--------|----------|
| `date_format` | `F.date_format(ts, "yyyyMM")` | **Correto** - entende DateType |
| `substring` | `F.substring(ts, 1, 6)` | **Errado** - assume string |

```python
    df = df.withColumn(
        "dt_safra_atraso",
        F.to_date(F.concat(F.col("safra_atraso"), F.lit("01")), "yyyyMMdd")
    )
```

**Por que criar `dt_safra_atraso`?**
- `safra_atraso` e STRING "202401" (para agrupamento)
- `dt_safra_atraso` e DATE 2024-01-01 (para calculos temporais no ABT builder)

```
safra_atraso: "202401"  (string)
                  │
                  ▼
concat("202401" + "01") = "20240101"
                  │
                  ▼
to_date("20240101", "yyyyMMdd") = 2024-01-01 (date)
```

#### 3.3 Garantir Valores Numericos com F.coalesce()

```python
    val_aberto = F.coalesce(F.col("val_fat_aberto"), F.lit(0.0))
    val_bruto = F.coalesce(F.col("val_fat_bruto"), F.lit(0.0))
    val_liquido = F.coalesce(F.col("val_fat_liquido"), F.lit(0.0))
    val_pagamento = F.coalesce(F.col("val_fat_pagamento_bruto"), F.lit(0.0))
    val_multa_juros = F.coalesce(F.col("val_multa_juros"), F.lit(0.0))
```

**Por que criar variaveis intermediarias?**
1. **Reutilizacao**: Cada `val_*` e usado multiplas vezes na agregacao
2. **Leitura**: Codigo mais limpo que repetir `F.coalesce(...)` 10x
3. **Manutencao**: Mudar tratamento de NULL em um lugar so

**Por que `F.coalesce(col, 0.0)` e nao `F.when(col.isNull(), 0)`?**

```python
# Opcao 1: F.coalesce (PREFERIDO)
F.coalesce(F.col("val"), F.lit(0.0))  # Mais conciso, mesmo resultado

# Opcao 2: F.when (mais verboso)
F.when(F.col("val").isNull(), 0.0).otherwise(F.col("val"))
```

- `F.coalesce`: Funcao especifica para substituir NULL → mais semantico
- `F.when`: Condicional generico → mais verboso para este caso

#### 3.4 Conversao de Indicadores String para Binario

```python
    def str_to_binary(col_name):
        return F.when(
            F.upper(F.col(col_name)).isin(['1', 'S', 'Y', 'SIM', 'YES', 'TRUE']),
            F.lit(1)
        ).otherwise(F.lit(0))
```

**Por que essa funcao?**

Os indicadores na Silver vem como STRING com diversos formatos:

| Valor Original | Significado | Resultado |
|----------------|-------------|-----------|
| `'S'` | Sim (portugues) | 1 |
| `'Y'` | Yes (ingles) | 1 |
| `'1'` | Um (numerico) | 1 |
| `'SIM'` | Sim por extenso | 1 |
| `'N'`, `'0'`, NULL | Nao | 0 |

**Por que `F.upper()` antes do `isin()`?**
```python
F.upper(F.col(col_name))  # Converte 's' → 'S', 'sim' → 'SIM'
```
- Dados reais podem ter `'s'` ou `'S'`
- `F.upper()` normaliza para comparacao case-insensitive

**Por que funcao local e nao UDF?**

```python
# ERRADO: UDF (Python) - problemas de serializacao
def str_to_binary_udf(val):
    return 1 if val in ['S', 'Y', '1'] else 0
udf_func = F.udf(str_to_binary_udf, IntegerType())  # Nao fazer!

# CORRETO: Funcao que retorna expressao Spark
def str_to_binary(col_name):
    return F.when(...).otherwise(...)  # Retorna Column, nao valor
```

- `str_to_binary` retorna uma **expressao Spark** (Column)
- **Nao e UDF** - e uma funcao Python que monta a expressao
- Otimizado pelo Catalyst, sem overhead de serializacao

#### 3.5 Aplicacao dos Indicadores

```python
    ind_wo = str_to_binary("ind_wo") if "ind_wo" in df.columns else F.lit(0)
    ind_pdd = str_to_binary("ind_pdd") if "ind_pdd" in df.columns else F.lit(0)
    ind_fraude = str_to_binary("ind_fraude") if "ind_fraude" in df.columns else F.lit(0)
    ind_aca = str_to_binary("ind_aca") if "ind_aca" in df.columns else F.lit(0)
    ind_pccr = str_to_binary("ind_pccr") if "ind_pccr" in df.columns else F.lit(0)
```

**Por que verificar `if col in df.columns`?**
- **Robustez**: Schema pode variar entre ambientes/versoes
- **Default seguro**: Se coluna nao existe, assume 0 (sem indicador)
- **Evita erro**: `df.columns` e lista, verificacao e rapida

```
Se "ind_wo" existe:    str_to_binary("ind_wo")  → expressao de conversao
Se "ind_wo" NAO existe: F.lit(0)                → sempre 0
```

#### 3.6 Flag de Fatura em Aberto

```python
    df = df.withColumn(
        "flag_fatura_aberta",
        F.when(val_aberto > 0, 1).otherwise(0)
    )
```

**Logica de negocio:**
- `val_fat_aberto > 0` = fatura em aberto
- `val_fat_aberto = 0` = fatura quitada ou inexistente

**Por que nao usar `val_aberto.isNotNull()`?**
- Uma fatura pode existir com valor 0 (zerada por ajuste)
- Queremos contar apenas faturas com **divida real** (> 0)

#### 3.7 Classificacao de Aging Buckets

```python
    df = df.withColumn(
        "aging_bucket",
        F.when(F.col("dw_faixa_aging_fatura") == "0-30 dias", "0_30")
         .when(F.col("dw_faixa_aging_fatura") == "31-60 dias", "31_60")
         .when(F.col("dw_faixa_aging_fatura") == "61-90 dias", "61_90")
         .when(F.col("dw_faixa_aging_fatura") == ">90 dias", "90_plus")
         .otherwise("missing")
    )
```

**Por que padronizar para "0_30", "31_60", etc?**

| Original | Padronizado | Motivo |
|----------|-------------|--------|
| `"0-30 dias"` | `"0_30"` | Sem espacos/hifen para nomes de colunas |
| `">90 dias"` | `"90_plus"` | Caractere `>` invalido em nomes |
| NULL/outro | `"missing"` | Explicito sobre dados faltantes |

**Encadeamento de `.when()`:**
```python
F.when(cond1, val1)
 .when(cond2, val2)  # Se cond1 False, testa cond2
 .when(cond3, val3)  # Se cond1 e cond2 False, testa cond3
 .otherwise(default) # Se todas False
```
- **Ordem importa**: Primeira condicao True vence
- Neste caso, faixas sao mutuamente exclusivas (sem overlap)

---

### 4. Agregacao Mensal (groupBy)

#### 4.1 Estrutura do groupBy

```python
    df_gold = df.groupBy("num_cpf", "safra_atraso", "dt_safra_atraso").agg(
```

**Por que esses 3 campos no groupBy?**
- `num_cpf`: Identificador do cliente
- `safra_atraso`: Mes do snapshot (string para particao)
- `dt_safra_atraso`: Data do snapshot (date para joins temporais)

**Grao resultante:** 1 linha por cliente-mes (todas as faturas agregadas)

#### 4.2 Metricas de Faturas Abertas

```python
        F.count("*").alias("qtd_registros_mes"),
```
**Total de linhas** (faturas/registros) para o cliente no mes.

```python
        F.sum("flag_fatura_aberta").alias("qtd_faturas_abertas_mes"),
```
**Quantas faturas tem saldo > 0.** Como `flag_fatura_aberta` e 0 ou 1, `sum` conta os 1s.

```python
        F.countDistinct(F.when(val_aberto > 0, F.col("contrato"))).alias("qtd_contratos_com_atraso_mes"),
```
**Quantos contratos DISTINTOS tem atraso.**
- `F.when(val_aberto > 0, contrato)`: So considera contratos com saldo
- `countDistinct`: Remove duplicatas (um contrato pode ter multiplas faturas)

**Por que contar contratos e nao apenas faturas?**
- Um cliente pode ter 5 faturas de 1 contrato (1 problema)
- Ou 5 faturas de 5 contratos (5 problemas)
- `qtd_contratos_com_atraso` e mais representativo do tamanho do problema

#### 4.3 Valores em Aberto

```python
        F.sum(val_aberto).alias("sum_val_aberto_mes"),
        F.avg(F.when(val_aberto > 0, val_aberto)).alias("avg_val_aberto_mes"),
        F.max(val_aberto).alias("max_val_aberto_mes"),
        F.min(F.when(val_aberto > 0, val_aberto)).alias("min_val_aberto_mes"),
        F.stddev(F.when(val_aberto > 0, val_aberto)).alias("std_val_aberto_mes"),
```

**Por que `F.when(val_aberto > 0, val_aberto)` em avg/min/stddev?**

```
Cenario:
  - Fatura A: R$100 (aberta)
  - Fatura B: R$0 (quitada)
  - Fatura C: R$200 (aberta)

COM filtro (val > 0):     avg = (100 + 200) / 2 = R$150  ← CORRETO
SEM filtro:               avg = (100 + 0 + 200) / 3 = R$100  ← ERRADO
```

- **avg** com zeros dilui a media real
- **min** com zeros sempre seria 0 (inutil)
- **sum** mantem zeros (total real de divida)
- **max** mantem zeros (maximo e maximo)

#### 4.4 Distribuicao de Aging (Contagem)

```python
        F.sum(F.when(F.col("aging_bucket") == "0_30", 1).otherwise(0)).alias("qtd_aging_0_30_mes"),
        F.sum(F.when(F.col("aging_bucket") == "31_60", 1).otherwise(0)).alias("qtd_aging_31_60_mes"),
        F.sum(F.when(F.col("aging_bucket") == "61_90", 1).otherwise(0)).alias("qtd_aging_61_90_mes"),
        F.sum(F.when(F.col("aging_bucket") == "90_plus", 1).otherwise(0)).alias("qtd_aging_90_plus_mes"),
        F.sum(F.when(F.col("aging_bucket") == "missing", 1).otherwise(0)).alias("qtd_aging_missing_mes"),
```

**Pattern: Contagem condicional com sum(when())**
```python
F.sum(F.when(condicao, 1).otherwise(0))
# Equivalente a: COUNT(*) WHERE condicao
```

**Por que nao usar `F.count(F.when(...))`?**
```python
# ERRADO: count() ignora o valor, conta nao-NULLs
F.count(F.when(cond, 1).otherwise(0))  # Conta TODOS (0 e 1 sao nao-NULL)

# CORRETO: sum() soma os valores (0s nao contribuem)
F.sum(F.when(cond, 1).otherwise(0))  # Soma apenas os 1s
```

#### 4.5 Distribuicao de Aging (Valores)

```python
        F.sum(F.when(F.col("aging_bucket") == "0_30", val_aberto).otherwise(0)).alias("sum_val_aging_0_30_mes"),
        F.sum(F.when(F.col("aging_bucket") == "31_60", val_aberto).otherwise(0)).alias("sum_val_aging_31_60_mes"),
        F.sum(F.when(F.col("aging_bucket") == "61_90", val_aberto).otherwise(0)).alias("sum_val_aging_61_90_mes"),
        F.sum(F.when(F.col("aging_bucket") == "90_plus", val_aberto).otherwise(0)).alias("sum_val_aging_90_plus_mes"),
```

**Diferenca entre qtd_aging e sum_val_aging:**

```
Cliente com 3 faturas:
  - Fatura A: R$50 (0-30 dias)
  - Fatura B: R$100 (0-30 dias)
  - Fatura C: R$1000 (>90 dias)

qtd_aging_0_30 = 2      (duas faturas)
sum_val_aging_0_30 = R$150   (soma dos valores)

qtd_aging_90_plus = 1   (uma fatura)
sum_val_aging_90_plus = R$1000  (valor alto!)
```

**Por que ambas metricas?**
- `qtd`: Numero de problemas
- `sum_val`: Magnitude financeira do problema
- Um cliente com 1 fatura de R$10.000 e diferente de 10 faturas de R$100

#### 4.6 Indicadores de Risco (WO, PDD, Fraude)

```python
        F.max(ind_wo).alias("flag_teve_wo_mes"),
        F.max(ind_pdd).alias("flag_teve_pdd_mes"),
        F.max(ind_fraude).alias("flag_teve_fraude_mes"),
```

**Por que `F.max()` para flags?**
```
ind_wo para faturas: [0, 0, 1, 0, 0]

F.max([0, 0, 1, 0, 0]) = 1  → "Teve pelo menos um WO"
F.sum([0, 0, 1, 0, 0]) = 1  → Funcionaria, mas semanticamente max e mais claro
F.min([0, 0, 1, 0, 0]) = 0  → ERRADO - diria que nao teve WO
```

- `F.max()` para flag: "Teve pelo menos um?"
- `F.sum()` para contagem: "Quantos teve?"

```python
        F.sum(F.when(ind_wo == 1, 1).otherwise(0)).alias("qtd_com_wo_mes"),
        F.sum(F.when(ind_pdd == 1, 1).otherwise(0)).alias("qtd_com_pdd_mes"),
        F.sum(F.when(ind_fraude == 1, 1).otherwise(0)).alias("qtd_com_fraude_mes"),
```

**Contagem de faturas** com cada indicador (nao apenas flag booleano).

```python
        F.sum(F.when(ind_wo == 1, val_aberto).otherwise(0)).alias("sum_val_wo_mes"),
        F.sum(F.when(ind_pdd == 1, val_aberto).otherwise(0)).alias("sum_val_pdd_mes"),
```

**Valor total** em WO e PDD - quanto dinheiro ja foi "perdido".

#### 4.7 Indicadores de Recuperacao (ACA, PCCR)

```python
        F.max(ind_aca).alias("flag_teve_aca_mes"),
        F.max(ind_pccr).alias("flag_teve_pccr_mes"),
        F.sum(F.when(ind_aca == 1, 1).otherwise(0)).alias("qtd_com_aca_mes"),
        F.sum(F.when(ind_pccr == 1, 1).otherwise(0)).alias("qtd_com_pccr_mes"),
```

**Mesma logica dos indicadores de risco**, mas para recuperacao.

**Interpretacao de negocio:**
- `flag_teve_aca = 1`: Cliente fez acordo de pagamento
- `flag_teve_pccr = 1`: Cliente entrou em programa de recuperacao
- Podem ser **positivos** (tentando regularizar) ou **negativos** (precisou de acao de cobranca)

---

### 5. Features Derivadas

#### 5.1 Percentual por Aging Bucket

```python
    df_gold = df_gold.withColumn(
        "pct_aging_0_30_mes",
        F.when(
            F.col("qtd_faturas_abertas_mes") > 0,
            F.round((F.col("qtd_aging_0_30_mes") / F.col("qtd_faturas_abertas_mes")) * 100, 2)
        ).otherwise(0.0)
    )
```

**Formula:**
```
pct_aging_0_30 = (qtd_aging_0_30 / qtd_faturas_abertas) * 100
```

**Por que `F.when(qtd > 0, ...).otherwise(0.0)`?**
- Evita divisao por zero
- Se nao tem faturas abertas, percentual e 0 (nao NULL)

**Por que `F.round(..., 2)`?**
- Arredonda para 2 casas decimais
- Evita valores como `33.333333333333336%`
- Mais legivel e evita problemas de precisao float

**Encadeamento `.withColumn()`:**
```python
    ).withColumn(
        "pct_aging_31_60_mes",
        ...
    ).withColumn(
        "pct_aging_61_90_mes",
        ...
    ).withColumn(
        "pct_aging_90_plus_mes",
        ...
    )
```

**Por que encadeamento e nao withColumns()?**
- `withColumns()` (plural) foi adicionado no Spark 3.3
- Encadeamento funciona em todas as versoes
- Ambos geram o mesmo plano de execucao

#### 5.2 Feature Chave: pct_aging_90_plus

```python
    df_gold = df_gold.withColumn(
        "pct_val_aging_90_plus_mes",
        F.when(
            F.col("sum_val_aberto_mes") > 0,
            F.round((F.col("sum_val_aging_90_plus_mes") / F.col("sum_val_aberto_mes")) * 100, 2)
        ).otherwise(0.0)
    )
```

**Esta e uma das features MAIS IMPORTANTES para ML:**

```
Cenario A:                          Cenario B:
  2 faturas em >90 dias              2 faturas em >90 dias
  Valor: R$100 cada                  Valor: R$5.000 cada
  Total aberto: R$1.000              Total aberto: R$10.000

  pct_val_aging_90_plus = 20%        pct_val_aging_90_plus = 100%

  RISCO MEDIO                        RISCO ALTISSIMO
```

- Captura tanto **quantidade** quanto **concentracao de valor** em aging grave

#### 5.3 Ratios Financeiros

```python
    df_gold = df_gold.withColumn(
        "ratio_aberto_faturado_mes",
        F.when(
            F.col("sum_val_fat_bruto_mes") > 0,
            F.round(F.col("sum_val_aberto_mes") / F.col("sum_val_fat_bruto_mes"), 4)
        ).otherwise(0.0)
    )
```

**Formula:**
```
ratio_aberto_faturado = aberto / faturado

Exemplo:
  Faturado: R$1.000
  Aberto: R$300
  Ratio: 0.30 (30% nao foi pago)
```

**Por que `F.round(..., 4)`?**
- Ratios sao tipicamente < 1
- 4 casas decimais = precisao de 0.01%
- Mais que 4 casas e ruido

```python
    df_gold = df_gold.withColumn(
        "ratio_pagamento_faturado_mes",
        F.when(
            F.col("sum_val_fat_bruto_mes") > 0,
            F.round(F.col("sum_val_pagamento_mes") / F.col("sum_val_fat_bruto_mes"), 4)
        ).otherwise(0.0)
    )
```

**Complemento do ratio_aberto:**
```
ratio_pagamento + ratio_aberto ≈ 1 (se nao houver ajustes)
```

#### 5.4 Coeficiente de Variacao

```python
    df_gold = df_gold.withColumn(
        "coef_variacao_aberto_mes",
        F.when(
            (F.col("avg_val_aberto_mes").isNotNull()) & (F.col("avg_val_aberto_mes") > 0),
            F.round(F.col("std_val_aberto_mes") / F.col("avg_val_aberto_mes"), 4)
        ).otherwise(None)
    )
```

**Formula:** `CV = desvio_padrao / media`

**Interpretacao:**
- CV baixo (< 0.5): Faturas com valores similares
- CV alto (> 1): Grande variacao nos valores

**Por que manter NULL quando nao calculavel?**
```python
.otherwise(None)  # Nao 0.0!
```
- CV = 0 significaria "zero variacao" (todas faturas iguais)
- NULL significa "nao calculavel" (sem dados suficientes)
- Modelo pode tratar NULLs diferentemente de zeros

---

### 6. Flags de Comportamento

#### 6.1 Flag Sem Atraso

```python
    df_gold = df_gold.withColumn(
        "flag_sem_atraso_mes",
        F.when(F.col("qtd_faturas_abertas_mes") == 0, 1).otherwise(0)
    )
```

**Cliente adimplente no mes** - nenhuma fatura em aberto.

#### 6.2 Flag Atraso Grave

```python
    df_gold = df_gold.withColumn(
        "flag_atraso_grave_mes",
        F.when(F.col("pct_aging_90_plus_mes") > 50, 1).otherwise(0)
    )
```

**Mais de 50% das faturas em >90 dias.**

**Por que 50% como threshold?**
- Decisao de negocio (pode ser ajustada)
- Indica que a **maioria** da divida e grave
- Threshold muito baixo (10%) seria sensivel demais
- Threshold muito alto (90%) perderia casos importantes

#### 6.3 Flag Risco Alto (FEATURE CHAVE)

```python
    df_gold = df_gold.withColumn(
        "flag_risco_alto_mes",
        F.when(
            (F.col("flag_teve_wo_mes") == 1) |
            (F.col("flag_teve_pdd_mes") == 1) |
            (F.col("flag_teve_fraude_mes") == 1),
            1
        ).otherwise(0)
    )
```

**Combinacao de indicadores de alto risco:**
- WO (Write-Off): Divida ja foi baixada
- PDD: Empresa ja provisionou como perda
- Fraude: Comportamento fraudulento identificado

**Por que combinar em um unico flag?**
- Simplifica modelagem (feature sintetica)
- Qualquer um dos 3 ja indica alto risco
- Modelo pode usar tanto `flag_risco_alto` quanto flags individuais

#### 6.4 Flag Em Recuperacao

```python
    df_gold = df_gold.withColumn(
        "flag_em_recuperacao_mes",
        F.when(
            (F.col("flag_teve_aca_mes") == 1) |
            (F.col("flag_teve_pccr_mes") == 1),
            1
        ).otherwise(0)
    )
```

**Cliente em processo de regularizacao.**

**Interpretacao ambigua para ML:**
- **Positivo**: Esta tentando pagar
- **Negativo**: Precisou de acordo/cobranca (ja teve problema)

#### 6.5 Flag Alto Valor em Aberto

```python
    df_gold = df_gold.withColumn(
        "flag_alto_valor_aberto_mes",
        F.when(F.col("sum_val_aberto_mes") > 500, 1).otherwise(0)
    )
```

**Threshold de R$500 como "alto valor".**

**Por que R$500?**
- Decisao de negocio baseada no ticket medio do setor telecom
- Pode ser parametrizado em versoes futuras

#### 6.6 Flag Muitas Faturas Abertas

```python
    df_gold = df_gold.withColumn(
        "flag_muitas_faturas_abertas_mes",
        F.when(F.col("qtd_faturas_abertas_mes") > 3, 1).otherwise(0)
    )
```

**Mais de 3 faturas abertas = problema recorrente.**

#### 6.7 Flag Concentracao em Aging Grave

```python
    df_gold = df_gold.withColumn(
        "flag_concentrado_aging_grave_mes",
        F.when(F.col("pct_val_aging_90_plus_mes") > 70, 1).otherwise(0)
    )
```

**Mais de 70% do VALOR em >90 dias.**

**Diferenca de `flag_atraso_grave`:**
- `flag_atraso_grave`: >50% das FATURAS em aging grave
- `flag_concentrado_aging_grave`: >70% do VALOR em aging grave

---

### 7. Metadados e Funcao main()

```python
    df_gold = df_gold.withColumn("gold_version", F.lit(GOLD_VERSION))
    df_gold = df_gold.withColumn("gold_build_date", F.current_timestamp())
```

**Rastreabilidade** - saber quando e qual versao gerou os dados.

```python
def main():
    parser = argparse.ArgumentParser(description="Gerar Gold Atraso Features v2")
    parser.add_argument("--input_path", default=DEFAULT_SILVER_ATRASO_PATH)
    parser.add_argument("--output_path", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--format", default=DEFAULT_FORMAT)
    parser.add_argument("--skip_save", action="store_true")

    args, unknown = parser.parse_known_args()
```

**`parse_known_args()` vs `parse_args()`:**
- Databricks/Jupyter injetam argumentos extras
- `parse_known_args()` ignora argumentos desconhecidos
- Evita erro "unrecognized arguments"

```python
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("safra_atraso") \
        .option("mergeSchema", "true") \
        .save(args.output_path)
```

**Opcoes de escrita:**
- `partitionBy("safra_atraso")`: Particiona por mes para queries eficientes
- `mergeSchema`: Permite adicionar colunas em versoes futuras
- `overwrite`: Substitui dados anteriores (idempotente)

---

## Comparacao com Outros Feature Generators

| Aspecto | Recarga | Pagamento | Atraso |
|---------|---------|-----------|--------|
| **Tipo de dado** | Eventos | Eventos | Snapshot |
| **Deduplicacao** | Sim (hash) | Sim (hash) | Nao |
| **Principal metrica** | SOS (estresse) | Juros (atraso) | Aging (gravidade) |
| **Volume** | 95M → 33M | 22M → 13M | 32M → 15M |
| **Indicadores especiais** | SOS, Bonus | Desconto, Juros | WO, PDD, Fraude |
| **Window temporal** | Nao (evento) | Nao (evento) | Nao (snapshot) |

---

## Features Mais Importantes para ML

| Rank | Feature | Interpretacao |
|------|---------|---------------|
| 1 | `pct_aging_90_plus_mes` | % faturas em atraso grave |
| 2 | `flag_risco_alto_mes` | WO ou PDD ou Fraude |
| 3 | `sum_val_aberto_mes` | Total devido |
| 4 | `pct_val_aging_90_plus_mes` | % valor em atraso grave |
| 5 | `flag_concentrado_aging_grave_mes` | >70% valor em >90 dias |
| 6 | `ratio_aberto_faturado_mes` | % nao pago do faturado |
| 7 | `qtd_com_wo_mes` | Quantidade de write-offs |
| 8 | `flag_em_recuperacao_mes` | Em acordo/cobranca |
| 9 | `ticket_medio_aberto_mes` | Valor medio por fatura |
| 10 | `coef_variacao_aberto_mes` | Variabilidade dos valores |

---

## Erros Comuns e Solucoes

### 1. Coluna ind_* Nao Existe

**Erro:**
```
AnalysisException: cannot resolve 'ind_wo'
```

**Solucao implementada:**
```python
ind_wo = str_to_binary("ind_wo") if "ind_wo" in df.columns else F.lit(0)
```

### 2. Divisao por Zero em Percentuais

**Erro:**
```
Division by zero (NaN ou Infinity)
```

**Solucao implementada:**
```python
F.when(F.col("qtd_faturas_abertas_mes") > 0, ...).otherwise(0.0)
```

### 3. Valores String em Indicadores

**Erro:**
```
sum de strings nao funciona
```

**Solucao implementada:**
```python
def str_to_binary(col_name):
    return F.when(F.upper(F.col(col_name)).isin([...]), 1).otherwise(0)
```

---

## Fluxo de Dados Completo (Silver → ABT)

```
┌──────────────────┐
│  Silver Atraso   │  31.6M registros
│  (Snapshot)      │  Multiplas faturas por cliente
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ gold_atraso_     │  15M registros
│ features_v2.py   │  1 linha por cliente-mes
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 05_gold_abt_v6_  │  JOIN com ABT v5
│ builder_v2.py    │  Windows M1/M3/M6
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│    ABT v6 v2     │  3.79M registros
│  (614 colunas)   │  + Atraso M1/M3/M6
└──────────────────┘
```

---

## Checklist de Revisao

- [ ] Entendi a diferenca entre Snapshot e Eventos
- [ ] Sei o que significa WO, PDD, ACA, PCCR
- [ ] Compreendo a hierarquia de aging (0-30 → 90+)
- [ ] Entendi por que str_to_binary NAO e UDF
- [ ] Sei a diferenca entre qtd_aging e sum_val_aging
- [ ] Compreendo por que usar F.max() para flags e F.sum() para contagens
- [ ] Entendi a importancia de pct_aging_90_plus para ML
- [ ] Sei explicar os thresholds dos flags (50%, 70%, R$500, 3 faturas)

---

## Proximos Passos

Apos este documento, prosseguir para:
- **06_ABT_V6_EXPLAINED.md** - Como o ABT v6 combina ABT v5 + Pagamento + Atraso com janelas temporais M1/M3/M6
