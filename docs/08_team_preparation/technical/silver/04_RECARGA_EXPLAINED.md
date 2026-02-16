# Script 03: Bronze → Silver Recarga

**Arquivo:** `src/jobs/01_silver/03_bronze_silver_recarga.py`
**Ordem no Pipeline:** 4º (após Cadastro)
**Função:** Processar eventos de recarga (transacional, 100M+ registros)

---

## Visão Geral

A base Recarga é **transacional** (evento-level), diferente das anteriores que são cliente-mês. Cada linha representa uma recarga individual.

**Grão:** 1 linha por **EVENTO** (múltiplos por cliente)
**Volume:** ~100 milhões de registros

**Particularidades:**
- Base transacional (não 1:1 por CPF+SAFRA)
- Timestamp do evento: `DAT_INSERCAO_CREDITO`
- Sentinelas: -1, -2, -3 em códigos dimensionais
- Valores negativos em colunas monetárias (~6%)
- SOS (empréstimo) como indicador de stress financeiro
- Deduplicação por hash de `event_key`

---

## Diferença Fundamental: Evento-Level vs Cliente-Mês

| Aspecto | Bureau/Telco/Cadastro | Recarga |
|---------|----------------------|---------|
| Grão | 1 linha por CPF+SAFRA | 1 linha por EVENTO |
| Volume | ~3.8M registros | ~100M registros |
| Chave | num_cpf + safra | event_key (hash) |
| Agregação | Já agregado | Agregação na Gold |

---

## Código Completo Explicado

### Bloco 1: Configuração (Linhas 60-80)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/recarga_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/"
DEFAULT_FORMAT = "delta"

# Colunas numéricas de valor (montantes)
VALOR_COLUMNS = ["val_credito_inserido", "val_bonus", "val_real", "valor_sos"]

# Colunas dimensionais (códigos com sentinelas -1/-2/-3)
CODIGO_COLUMNS = [
    "cod_tecnologia_dw", "cod_tipo_credito", "dw_tipo_insercao",
    "dw_tipo_recarga", "dw_forma_pagamento", "cod_plataforma_atu",
    "cod_status_plataforma", "cod_canal_aquisicao", "dw_instituicao",
    "dw_plano_tarifacao", "cod_promocao"
]

# Sentinelas padronizadas
SENTINELAS = [-1, -2, -3]
# =============================================================================
```

**Por que separar colunas por tipo?**
- **VALOR_COLUMNS:** Tratamento de valores negativos
- **CODIGO_COLUMNS:** Tratamento de sentinelas -1/-2/-3

**O que significam os sentinelas?**

| Valor | Significado |
|-------|-------------|
| -1 | Não se aplica |
| -2 | Não determinado |
| -3 | Não informado |

---

### Bloco 2: Função build_silver (Linhas 82-252)

#### 2.1: Tipagem Básica (Linhas 96-107)

```python
def build_silver(df_bronze):
    print(">>> [Transform] Tipagem + regras Silver (recarga evento-level)...")

    # 1) Tipagem básica (chaves + flags)
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("dw_num_cliente", F.col("dw_num_cliente").cast("string"))
        .withColumn("dw_num_ntc", F.col("dw_num_ntc").cast("string"))
        .withColumn("flag_sos", to_int_safe("flag_sos"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao") if "flag_instalacao" in df_bronze.columns else F.lit(None).cast("int"))
        .withColumn("fpd_int", to_int_safe("fpd") if "fpd" in df_bronze.columns else F.lit(None).cast("int"))
    )
```

**Diferença:** Verifica se coluna existe antes de tipar.

```python
.withColumn("flag_instalacao_int",
    to_int_safe("flag_instalacao") if "flag_instalacao" in df_bronze.columns
    else F.lit(None).cast("int"))
```

**Por que essa verificação?**
Recarga pode não ter FLAG_INSTALACAO em todos os registros (é transacional).

#### 2.2: Parse de Timestamp do Evento (Linhas 109-139) ⭐

```python
    # 2) Parsing tolerante de data/hora do evento
    print(">>> [Transform] Parseando DAT_INSERCAO_CREDITO com tolerância a inválidos...")

    # Parse TS_RECARGA (timestamp do evento)
    df = df.withColumn(
        "ts_recarga",
        F.when(
            F.col("dat_insercao_credito").isNotNull() & (F.trim(F.col("dat_insercao_credito")) != F.lit("")),
            F.to_timestamp(F.col("dat_insercao_credito"), "ddMMMyyyy:HH:mm:ss")
        ).otherwise(None)
    )

    # Flag de parsing inválido
    df = df.withColumn(
        "flag_ts_recarga_invalida",
        F.when(
            (F.col("dat_insercao_credito").isNotNull()) &
            (F.trim(F.col("dat_insercao_credito")) != F.lit("")) &
            (F.col("ts_recarga").isNull()),
            1
        ).otherwise(0)
    )

    # Derivar DT_RECARGA (data) e SAFRA_RECARGA (YYYYMM)
    df = df.withColumn(
        "dt_recarga",
        F.when(F.col("ts_recarga").isNotNull(), F.to_date(F.col("ts_recarga"))).otherwise(None)
    ).withColumn(
        "safra_recarga",
        F.when(F.col("dt_recarga").isNotNull(), F.date_format(F.col("dt_recarga"), "yyyyMM")).otherwise(None)
    )
```

**Explicação do formato de data:**

```python
F.to_timestamp(F.col("dat_insercao_credito"), "ddMMMyyyy:HH:mm:ss")
```

| Formato | Exemplo | Significado |
|---------|---------|-------------|
| dd | 15 | Dia (2 dígitos) |
| MMM | JAN | Mês abreviado (3 letras) |
| yyyy | 2024 | Ano (4 dígitos) |
| HH:mm:ss | 14:30:25 | Hora:Minuto:Segundo |

**Exemplo completo:** `15JAN2024:14:30:25`

**Por que derivar SAFRA_RECARGA?**

```python
df = df.withColumn(
    "safra_recarga",
    F.date_format(F.col("dt_recarga"), "yyyyMM")
)
```

Na Gold, precisaremos agregar por cliente-mês. `safra_recarga` permite:
- Filtrar eventos anteriores à safra do cliente
- Criar janelas M1, M3, M6

#### 2.3: Tratamento de Valores Monetários (Linhas 141-157) ⭐

```python
    # 3) Casting de valores monetários (com flags de negativos)
    print(">>> [Transform] Tipando valores monetários e criando flags de negativos...")

    for val_col in VALOR_COLUMNS:
        if val_col in df.columns:
            df = (
                df
                .withColumn(f"{val_col}", to_double_safe(val_col))
                .withColumn(
                    f"flag_{val_col}_negativo",
                    F.when(F.col(val_col) < 0, 1).otherwise(0)
                )
                .withColumn(
                    f"{val_col}_clean",
                    F.when(F.col(val_col) < 0, None).otherwise(F.col(val_col))
                )
            )
```

**O que esse código faz para cada coluna de valor:**

1. **Tipagem:** Converte para double com `to_double_safe`
2. **Flag:** Cria `flag_<col>_negativo` (1 se negativo)
3. **Clean:** Cria `<col>_clean` (NULL se negativo)

**Por que valores negativos?**

| Situação | Valor | Significado |
|----------|-------|-------------|
| Recarga normal | +20.00 | Cliente recarregou R$20 |
| Estorno | -20.00 | Recarga cancelada/revertida |
| Ajuste | -5.00 | Correção contábil |

**Por que criar versão "clean"?**
- Agregações usam `_clean` para evitar distorção
- Flag permite análise separada de estornos

**Exemplo:**
```python
# ❌ Sem tratamento: soma distorcida
SUM(val_real)  # Inclui estornos, resultado menor que real

# ✓ Com tratamento: soma correta
SUM(val_real_clean)  # Ignora estornos (NULL)
```

#### 2.4: Tratamento de Sentinelas em Códigos (Linhas 159-174)

```python
    # 4) Tratamento de sentinelas em códigos dimensionais (-1/-2/-3)
    print(">>> [Transform] Tratando sentinelas em códigos dimensionais...")

    for cod_col in CODIGO_COLUMNS:
        if cod_col in df.columns:
            # Usar SQL try_cast para tolerar valores não-numéricos
            df = (
                df
                .withColumn(cod_col, F.expr(f"try_cast({cod_col} as int)"))
                .withColumn(
                    f"flag_{cod_col}_sentinela",
                    F.when(F.col(cod_col).isin(*SENTINELAS), 1).otherwise(0)
                )
            )
```

**O que é `try_cast`?**

```python
F.expr(f"try_cast({cod_col} as int)")
```

| Função | Comportamento com "abc" |
|--------|------------------------|
| `cast("int")` | ERRO ou NULL (inconsistente) |
| `try_cast(...as int)` | NULL (sempre tolerante) |

**Por que usar `try_cast`?**
Colunas dimensionais podem ter valores não-numéricos (ex: "PE", "MG").
`try_cast` retorna NULL para esses casos sem quebrar o pipeline.

**Por que `*SENTINELAS` com asterisco?**

```python
F.col(cod_col).isin(*SENTINELAS)
# Equivale a:
F.col(cod_col).isin(-1, -2, -3)
```

O asterisco "desempacota" a lista como argumentos separados.

---

### Bloco 3: Deduplicação por Event Key (Linhas 254-296) ⭐

```python
def dedupe_by_event_key(df_silver):
    """
    Garante 1 registro por evento (deduplicação robusta).

    Estratégia:
    - Cria EVENT_KEY via hash SHA2 de colunas-chave do evento
    - Desempata por TS_RECARGA DESC (mais recente)
    """
    print(">>> [Transform] Deduplicação por EVENT_KEY (evento)...")

    # Construir EVENT_KEY (hash de colunas-chave)
    event_key_cols = [
        "num_cpf",
        "dw_num_ntc",
        "ts_recarga",
        "val_real",
        "val_credito_inserido",
        "cod_tipo_credito",
        "cod_status_plataforma"
    ]

    # Filtrar apenas colunas que existem
    event_key_cols = [col for col in event_key_cols if col in df_silver.columns]

    # Criar EVENT_KEY via hash
    df_silver = df_silver.withColumn(
        "event_key",
        F.sha2(
            F.concat_ws("||", *[F.col(col).cast("string") for col in event_key_cols]),
            256
        )
    )

    # Dedupe: row_number por EVENT_KEY, ordenado por TS_RECARGA DESC
    w = Window.partitionBy("event_key").orderBy(F.col("ts_recarga").desc())
    df_ranked = df_silver.withColumn("rn", F.row_number().over(w))
    df_out = df_ranked.filter(F.col("rn") == 1).drop("rn", "event_key")

    return df_out
```

**Por que usar hash ao invés de chave natural?**

```python
# ❌ Chave natural: pode ser muito longa
Window.partitionBy("num_cpf", "dw_num_ntc", "ts_recarga", "val_real", ...)

# ✓ Hash: compacto e único
Window.partitionBy("event_key")  # 64 caracteres hex
```

**Explicação do hash:**

```python
F.sha2(
    F.concat_ws("||", *[F.col(col).cast("string") for col in event_key_cols]),
    256
)
```

1. `F.col(col).cast("string")` → Converte cada coluna para string
2. `*[...]` → Desempacota lista como argumentos
3. `F.concat_ws("||", ...)` → Concatena com separador "||"
   - Exemplo: `"12345678901||ABC123||2024-01-15 14:30:00||20.0||..."`
4. `F.sha2(..., 256)` → Gera hash SHA-256 (64 caracteres hex)

**Por que separador "||"?**
Evita colisões. Sem separador:
- `("AB", "C")` e `("A", "BC")` gerariam mesmo hash "ABC"
- Com "||": `"AB||C"` ≠ `"A||BC"`

---

### Bloco 4: Quality Checks (Linhas 377-432)

```python
    # 5) QUALITY CHECKS
    print("\n" + "="*80)
    print(">>> [Quality] RELATÓRIO DE QUALIDADE - SILVER RECARGA")
    print("="*80)

    # Parsing issues
    ts_invalida = df_silver_dedup.filter(F.col("flag_ts_recarga_invalida") == 1).count()
    ts_coverage = 100 * (count_out - ts_invalida) / count_out

    print(f"\n>>> [Quality] Parsing de data:")
    print(f"    TS_RECARGA válida: {count_out - ts_invalida:>12} ({ts_coverage:.2f}%)")

    # Valores negativos
    print(f"\n>>> [Quality] Valores negativos:")
    for val_col in VALOR_COLUMNS:
        neg_count = df_silver_dedup.filter(F.col(f"flag_{val_col}_negativo") == 1).count()
        neg_pct = 100 * neg_count / count_out
        print(f"    {val_col}_negativo: {neg_count:>12} ({neg_pct:.2f}%)")

    # SOS
    sos_presente = df_silver_dedup.filter(F.col("flag_sos") == 1).count()
    sos_pct = 100 * sos_presente / count_out
    print(f"\n>>> [Quality] SOS (serviço especial):")
    print(f"    Eventos com SOS: {sos_presente:>12} ({sos_pct:.2f}%)")
```

**Métricas monitoradas:**

| Métrica | Esperado | Ação se Fora |
|---------|----------|--------------|
| TS_RECARGA válida | >99% | Investigar formato |
| Valores negativos | ~6% | Normal (estornos) |
| SOS | ~6.5% | Feature importante |
| Sentinelas | Varia | Tratar como missing |

---

## SOS: Indicador de Stress Financeiro

### O Que É SOS?

SOS é um **empréstimo de crédito** oferecido pela Claro:
- Valor: R$3 a R$20 (tipicamente R$5)
- Cliente fica sem crédito → recebe SOS
- Descontado na próxima recarga

### Por Que É Importante?

```
Alta frequência de SOS = Cliente constantemente sem dinheiro
                       = Maior risco de inadimplência
                       = FPD mais provável
```

### Colunas Relacionadas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `flag_sos` | int | 1 se evento é SOS |
| `valor_sos` | double | Valor do SOS (R$) |

### Na Gold

Agregações como:
- `freq_sos_m1`: Quantos SOS no último mês
- `pct_sos_sobre_credito`: % do crédito vindo de SOS

---

## Diagrama de Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  recarga        │
│  (~100M eventos)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ standardize     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ build_silver()          │
│                         │
│ ├─ Tipagem básica       │
│ ├─ Parse timestamp      │  ← ddMMMyyyy:HH:mm:ss
│ ├─ Derivar SAFRA_RECARGA│
│ ├─ Valores monetários   │  ← flag + clean
│ ├─ Sentinelas -1/-2/-3  │  ← try_cast + flag
│ └─ Select               │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│ dedupe_by_event_key()   │
│                         │
│ ├─ Concat colunas-chave │
│ ├─ SHA-256 hash         │
│ ├─ row_number           │
│ └─ filter rn == 1       │
└────────┬────────────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  recarga        │
│  (evento-level) │
└─────────────────┘
```

---

## Colunas de Saída

| Categoria | Colunas | Quantidade |
|-----------|---------|------------|
| Chaves | num_cpf, dw_num_cliente, dw_num_ntc | 3 |
| Timestamp | ts_recarga, dt_recarga, safra_recarga | 3 |
| Flags de parse | flag_ts_recarga_invalida | 1 |
| Labels | flag_instalacao_int, fpd_int | 2 |
| Valores | val_* (4) + flag_negativo (4) + clean (4) | 12 |
| SOS | flag_sos, valor_sos | 2 |
| Códigos | cod_* (11) + flag_sentinela (11) | 22 |
| Auditoria | metadata_* | 5 |
| **Total** | | **~50** |

---

## Comparativo com Scripts Anteriores

| Aspecto | Bureau/Telco/Cadastro | Recarga |
|---------|----------------------|---------|
| Grão | 1:1 CPF+SAFRA | Evento-level |
| Volume | ~3.8M | ~100M |
| Timestamp | DT_SAFRA (mês) | TS_RECARGA (segundo) |
| Dedup | row_number by CPF+SAFRA | hash event_key |
| Sentinela | 0 ou 304 | -1, -2, -3 |
| Valores negativos | N/A | flag + clean |

---

## Checklist de Validação

- [x] Verificação de coluna antes de tipar (`if col in df.columns`)
- [x] Parse de timestamp com formato `ddMMMyyyy:HH:mm:ss`
- [x] Flag de timestamp inválido
- [x] SAFRA_RECARGA derivada do timestamp
- [x] Valores negativos: flag + versão clean
- [x] Sentinelas -1/-2/-3: `try_cast` + flag
- [x] Deduplicação por hash SHA-256 de event_key
- [x] Separador "||" no concat para evitar colisões
- [x] Quality report com métricas de SOS
- [x] Metadados de transformação
