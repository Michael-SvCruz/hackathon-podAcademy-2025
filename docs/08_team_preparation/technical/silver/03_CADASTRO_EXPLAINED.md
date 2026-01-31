# Script 02: Bronze → Silver Cadastro

**Arquivo:** `src/jobs/01_silver/02_bronze_silver_cadastro.py`
**Ordem no Pipeline:** 3º (após Telco)
**Função:** Processar dados demográficos (idade, CEP, status cadastral)

---

## Visão Geral

A base Cadastro contém **dados demográficos** dos clientes. O principal desafio é o **parsing tolerante de data de nascimento** em múltiplos formatos.

**Grão:** 1 linha por NUM_CPF + SAFRA (cliente-mês)

**Particularidades:**
- DATADENASCIMENTO em múltiplos formatos (dd/MM/yyyy, dd-MM-yyyy, etc.)
- Derivação de IDADE_ANOS com sanity checks
- Variáveis mistas (numéricas, categóricas, datas)
- **Lição aprendida:** UDFs falhavam silenciosamente (corrigido em Jan/2026)

---

## ⚠️ Lição Crítica: Nunca Usar Python UDFs

### O Problema (Dez/2025)

O código original usava Python UDF para parsear datas:

```python
# ❌ CÓDIGO ORIGINAL (QUEBRADO)
def safe_parse_date(date_str, date_format="dd/MM/yyyy"):
    from datetime import datetime
    return datetime.strptime(date_str.strip(), date_format).date()

safe_parse_date_udf = F.udf(safe_parse_date, DateType())
df = df.withColumn("dt_nasc", safe_parse_date_udf(F.col("datadenascimento")))
```

**Resultado:** `idade_anos` tinha **0% de cobertura** (tudo NULL).

### Por Que Falhou?

1. **Serialização:** UDFs Python são serializados e enviados aos workers
2. **Unity Catalog:** Ambiente restrito bloqueia certas operações
3. **Falha silenciosa:** Retorna NULL sem erro visível
4. **Performance:** 10-100x mais lento que funções nativas

### A Solução (Jan/2026)

```python
# ✓ CÓDIGO CORRIGIDO (FUNCIONA)
df = df.withColumn(
    "dt_nasc",
    F.coalesce(
        F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"),
        F.to_date(F.col("datadenascimento"), "dd-MM-yyyy"),
        F.to_date(F.col("datadenascimento"), "yyyy-MM-dd"),
        F.to_date(F.col("datadenascimento"), "ddMMyyyy"),
        F.lit(None).cast("date")
    )
)
```

**Resultado:** `idade_anos` passou para **99.57% de cobertura**.

---

## Código Completo Explicado

### Bloco 1: Configuração de Variáveis (Linhas 44-65)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/cadastro_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/cadastro_silver_delta/"
DEFAULT_FORMAT = "delta"

# Constants para sanity checks de idade
IDADE_MINIMA_VALIDA = 18
IDADE_MAXIMA_ESPERADA = 100

# Variáveis numéricas confirmadas
NUMERIC_VARS = ["var_03", "var_04", "var_05", "var_06", "var_07", "var_08", "var_09"]

# Variáveis categóricas
CATEGORICAL_VARS = ["var_15", "var_22", "var_23", "var_24", "var_25"]

# Variáveis mistas (possível data, texto, ou parse customizado)
MIXED_VARS = ["var_02", "var_10", "var_11", "var_12", "var_13", "var_14",
              "var_16", "var_17", "var_18", "var_19", "var_20", "var_21"]
# =============================================================================
```

**Por que separar variáveis por tipo?**
- **Numéricas:** Converter para double
- **Categóricas:** Normalizar (trim + upper)
- **Mistas:** Apenas trim (tratamento manual posterior)

**Por que constantes para idade?**

```python
IDADE_MINIMA_VALIDA = 18   # Menor de 18 não pode contratar
IDADE_MAXIMA_ESPERADA = 100  # Acima de 100 é outlier/erro
```

Facilita mudança centralizada e documenta regras de negócio.

---

### Bloco 2: Função build_silver (Linhas 68-252)

#### 2.1: Tipagem Básica (Linhas 82-94)

```python
def build_silver(df_bronze):
    print(">>> [Transform] Tipagem + regras Silver (cadastro)...")

    # 1) Tipagem básica
    df = (
        df_bronze
        .withColumn("num_cpf", F.col("num_cpf").cast("string"))
        .withColumn("safra", F.col("safra").cast("string"))
        .withColumn("prod", F.col("prod").cast("string"))
        .withColumn("flag_mig2", F.col("flag_mig2").cast("string"))
        .withColumn("flag_instalacao_int", to_int_safe("flag_instalacao"))
        .withColumn("fpd_int", to_int_safe("fpd"))
    )
```

**Idêntico ao Bureau:** Tipagem das colunas-chave e labels.

#### 2.2: DT_SAFRA (Linhas 96-101)

```python
    # 2) DT_SAFRA (primeiro dia do mês)
    df = df.withColumn(
        "dt_safra",
        F.to_date(F.concat(F.col("safra"), F.lit("01")), "yyyyMMdd")
    )
```

**Idêntico ao Bureau:** YYYYMM → YYYY-MM-01.

#### 2.3: Parse Tolerante de Data de Nascimento (Linhas 103-124) ⭐

```python
    # 3) Parse tolerante de DATADENASCIMENTO → DT_NASC
    # Usa to_date() do Spark (nativo, sem UDF) para garantir execução distribuída
    print(">>> [Transform] Parseando DATADENASCIMENTO com to_date() nativo do Spark...")

    # Tentar múltiplos formatos de data (tolerante a variações)
    df = df.withColumn(
        "dt_nasc",
        F.coalesce(
            # Formato padrão: dd/MM/yyyy (ex: 15/03/1985)
            F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"),
            # Formato alternativo: dd-MM-yyyy (ex: 15-03-1985)
            F.to_date(F.col("datadenascimento"), "dd-MM-yyyy"),
            # Formato alternativo: yyyy-MM-dd (ex: 1985-03-15)
            F.to_date(F.col("datadenascimento"), "yyyy-MM-dd"),
            # Formato alternativo: ddMMyyyy (ex: 15031985)
            F.to_date(F.col("datadenascimento"), "ddMMyyyy"),
            # Se nenhum formato funcionar, retorna NULL
            F.lit(None).cast("date")
        )
    )
```

**Explicação linha a linha:**

```python
F.coalesce(
```
Retorna o **primeiro valor não-NULL** da lista de argumentos.

```python
    F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"),
```
Tenta parsear como "15/03/1985". Se falhar, retorna NULL.

```python
    F.to_date(F.col("datadenascimento"), "dd-MM-yyyy"),
```
Se o primeiro falhou, tenta "15-03-1985".

```python
    F.to_date(F.col("datadenascimento"), "yyyy-MM-dd"),
```
Se os anteriores falharam, tenta "1985-03-15" (formato ISO).

```python
    F.to_date(F.col("datadenascimento"), "ddMMyyyy"),
```
Último formato: "15031985" (sem separadores).

```python
    F.lit(None).cast("date")
)
```
Se todos falharem, retorna NULL explícito.

**Por que essa ordem de formatos?**
1. `dd/MM/yyyy` → Mais comum no Brasil
2. `dd-MM-yyyy` → Variação com hífen
3. `yyyy-MM-dd` → Formato ISO (sistemas)
4. `ddMMyyyy` → Formato compacto (legado)

**Alternativa comum (ERRADA):**
```python
# ❌ Apenas um formato: falha se dados variarem
df = df.withColumn("dt_nasc", F.to_date(F.col("datadenascimento"), "dd/MM/yyyy"))
```

#### 2.4: Flag de Data Inválida (Linhas 126-135)

```python
    # Flag de data inválida (preenchida mas não conseguiu fazer parse)
    df = df.withColumn(
        "flag_dt_nasc_invalida",
        F.when(
            (F.col("datadenascimento").isNotNull()) &
            (F.trim(F.col("datadenascimento")) != F.lit("")) &
            (F.col("dt_nasc").isNull()),
            1
        ).otherwise(0)
    )
```

**Lógica:**
- Coluna original está preenchida (`isNotNull`)
- Coluna original não é string vazia (`!= ""`)
- Mas o parse falhou (`dt_nasc.isNull`)
- → Marcar como inválida

**Por que criar essa flag?**
Identificar registros com dados de nascimento corrompidos para análise posterior.

#### 2.5: Derivação de IDADE_ANOS (Linhas 137-153) ⭐

```python
    # 4) Derivação de IDADE_ANOS
    df = df.withColumn(
        "idade_anos",
        F.when(
            F.col("dt_nasc").isNotNull(),
            F.floor(F.months_between(F.col("dt_safra"), F.col("dt_nasc")) / 12)
        ).otherwise(None)
    )

    # Flags de sanity check (idade)
    df = df.withColumn(
        "flag_idade_menor_18",
        F.when(F.col("idade_anos") < IDADE_MINIMA_VALIDA, 1).otherwise(0)
    ).withColumn(
        "flag_idade_muito_alta",
        F.when(F.col("idade_anos") > IDADE_MAXIMA_ESPERADA, 1).otherwise(0)
    )
```

**Explicação do cálculo de idade:**

```python
F.months_between(F.col("dt_safra"), F.col("dt_nasc"))
```
Calcula meses entre data de nascimento e data da safra.
- Exemplo: dt_nasc = 1985-03-15, dt_safra = 2024-01-01
- Resultado: ~466 meses

```python
F.floor(... / 12)
```
Divide por 12 e arredonda para baixo.
- Exemplo: 466 / 12 = 38.83 → floor = 38 anos

**Por que usar DT_SAFRA e não data atual?**
A idade deve ser calculada **no momento da decisão** (safra), não hoje.

**Flags de sanidade:**

| Flag | Condição | Significado |
|------|----------|-------------|
| `flag_idade_menor_18` | idade < 18 | Inelegível (menor de idade) |
| `flag_idade_muito_alta` | idade > 100 | Outlier/erro de cadastro |

#### 2.6: CEP e Status RF (Linhas 155-166)

```python
    # 5) CEP como feature regional (string)
    df = df.withColumn("cep_3_digitos", F.trim(F.col("cep_3_digitos")))
    df = df.withColumn(
        "flag_cep_missing",
        F.when(
            (F.col("cep_3_digitos").isNull()) | (F.col("cep_3_digitos") == F.lit("")),
            1
        ).otherwise(0)
    )

    # STATUSRF (categórico)
    df = df.withColumn("statusrf", F.trim(F.upper(F.col("statusrf"))))
```

**Por que CEP com 3 dígitos?**
Os 3 primeiros dígitos do CEP identificam a **região** (ex: 010 = Centro de SP).

**Por que normalizar STATUSRF?**
```python
F.trim(F.upper(F.col("statusrf")))
```
- `trim` → Remove espaços nas pontas
- `upper` → Padroniza para maiúsculas

Evita que "ativo", "ATIVO", " Ativo " sejam tratados como valores diferentes.

#### 2.7: Tipagem de Variáveis por Categoria (Linhas 168-184)

```python
    # 6) Casting de var_* (numéricas)
    print(">>> [Transform] Tipando variáveis numéricas...")
    for var in NUMERIC_VARS:
        if var in df.columns:
            df = df.withColumn(var, to_double_safe(var))

    # Categóricas var_* (trim + upper)
    print(">>> [Transform] Normalizando variáveis categóricas...")
    for var in CATEGORICAL_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.upper(F.col(var))))

    # Variáveis mistas/outras (trim simples)
    print(">>> [Transform] Normalizando variáveis mistas...")
    for var in MIXED_VARS:
        if var in df.columns:
            df = df.withColumn(var, F.trim(F.col(var)))
```

**Tratamento por tipo:**

| Tipo | Variáveis | Tratamento |
|------|-----------|------------|
| Numérica | var_03 a var_09 | `to_double_safe()` |
| Categórica | var_15, var_22-25 | `trim(upper())` |
| Mista | var_02, var_10-14, var_16-21 | `trim()` apenas |

**Por que tratamento diferente?**
- **Numéricas:** Precisam de tipo double para cálculos
- **Categóricas:** Precisam de normalização para consistência
- **Mistas:** Podem conter datas ou textos, tratamento manual depois

#### 2.8: Seleção Dinâmica de Colunas (Linhas 199-250)

```python
    # 9) Seleção final de colunas (Silver "clean")
    columns_to_select = [
        # Chaves
        "num_cpf", "safra", "dt_safra",

        # Labels
        "flag_instalacao_int", "fpd_int",

        # Features cadastrais
        "dt_nasc", "idade_anos",
        "flag_dt_nasc_invalida", "flag_idade_menor_18", "flag_idade_muito_alta",
        "cep_3_digitos", "flag_cep_missing",
        "statusrf",

        # Metadados
        "prod", "flag_mig2",
        "flag_instalacao_invalida", "fpd_invalido",

        # Auditoria
        "metadata_data_ingestao", "metadata_nome_arquivo_origem",
        "metadata_sistema_origem", "metadata_data_transformacao",
        "metadata_versao_regra"
    ]

    # Adicionar todas as var_* que existem no dataframe
    var_columns = [col for col in df.columns if col.startswith("var_")]
    columns_to_select.extend(var_columns)

    # Remover duplicatas mantendo ordem
    columns_to_select = list(dict.fromkeys(columns_to_select))

    # Selecionar apenas colunas que existem no DF
    existing_columns = [col for col in columns_to_select if col in df.columns]
    df_silver = df.select(existing_columns)
```

**Explicação das técnicas:**

```python
var_columns = [col for col in df.columns if col.startswith("var_")]
```
Pega dinamicamente todas as colunas que começam com "var_".

```python
columns_to_select = list(dict.fromkeys(columns_to_select))
```
Remove duplicatas mantendo a ordem original.
- **Por que `dict.fromkeys`?** É mais eficiente que `list(set(...))` e preserva ordem.

```python
existing_columns = [col for col in columns_to_select if col in df.columns]
```
Filtra apenas colunas que existem (proteção contra schema incompleto).

---

### Bloco 3: Quality Checks Específicos (Linhas 334-357)

```python
    # 5) Quality checks
    print(">>> [Quality] Checando domínios e unicidade...")

    invalid_flag = df_silver_dedup.filter(F.col("flag_instalacao_invalida") == 1).count()
    invalid_fpd = df_silver_dedup.filter(F.col("fpd_invalido") == 1).count()

    idade_menor_18 = df_silver_dedup.filter(F.col("flag_idade_menor_18") == 1).count()
    idade_muito_alta = df_silver_dedup.filter(F.col("flag_idade_muito_alta") == 1).count()
    dt_nasc_invalida = df_silver_dedup.filter(F.col("flag_dt_nasc_invalida") == 1).count()

    cep_missing = df_silver_dedup.filter(F.col("flag_cep_missing") == 1).count()
    cep_coverage = 100 * (count_out - cep_missing) / count_out if count_out > 0 else 0

    print(f">>> [Quality] invalid flag_instalacao: {invalid_flag}")
    print(f">>> [Quality] invalid fpd: {invalid_fpd}")
    print(f">>> [Quality] data nascimento inválida: {dt_nasc_invalida}")
    print(f">>> [Quality] idade_menor_18 (ineligível): {idade_menor_18} ({idade_menor_18*100/count_out:.2f}%)")
    print(f">>> [Quality] idade_muito_alta (outlier >100): {idade_muito_alta}")
    print(f">>> [Quality] cep missing: {cep_missing} | cobertura: {cep_coverage:.1f}%")
```

**Checks específicos do Cadastro:**

| Check | Métrica | Ação se Alto |
|-------|---------|--------------|
| dt_nasc_invalida | Datas corrompidas | Investigar fonte |
| idade_menor_18 | Menores de idade | Filtrar na modelagem |
| idade_muito_alta | Outliers | Investigar ou cap |
| cep_missing | Cobertura regional | Pode ser feature preditiva |

---

## Diagrama de Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  cadastro       │
│  (raw demog.)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ standardize     │  ← snake_case
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ build_silver()  │
│                 │
│ ├─ Tipagem      │
│ ├─ DT_SAFRA     │
│ ├─ Parse data   │  ← F.coalesce (múltiplos formatos)
│ ├─ IDADE_ANOS   │  ← F.months_between
│ ├─ CEP/StatusRF │
│ ├─ Vars por tipo│
│ └─ Select       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ dedupe_by_key() │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  cadastro       │
│  (typed, clean) │
└─────────────────┘
```

---

## Colunas de Saída

| Categoria | Colunas | Quantidade |
|-----------|---------|------------|
| Chaves | num_cpf, safra, dt_safra | 3 |
| Labels | flag_instalacao_int, fpd_int | 2 |
| Idade | dt_nasc, idade_anos, flags | 5 |
| Regional | cep_3_digitos, flag_cep_missing | 2 |
| Status | statusrf | 1 |
| Variáveis | var_02 ... var_25 | ~24 |
| Quality flags | flag_instalacao_invalida, fpd_invalido | 2 |
| Auditoria | metadata_* | 5 |
| **Total** | | **~45** |

---

## Comparativo: Antes vs Depois do Fix

| Métrica | Antes (UDF) | Depois (F.to_date) |
|---------|-------------|-------------------|
| idade_anos coverage | 0.00% | 99.57% |
| Performance | Lento (UDF serialization) | Rápido (Catalyst optimized) |
| Erros | Silenciosos | Explícitos (NULL) |
| Manutenibilidade | Baixa (código Python) | Alta (Spark nativo) |

---

## Checklist de Validação

- [x] **NÃO usar Python UDFs** (usar F.to_date, F.coalesce)
- [x] Parse tolerante com múltiplos formatos de data
- [x] Flag para datas que não parsearam
- [x] IDADE_ANOS calculada com F.months_between
- [x] Sanity checks de idade (< 18, > 100)
- [x] CEP normalizado + flag de missing
- [x] STATUSRF normalizado (trim + upper)
- [x] Variáveis tipadas por categoria
- [x] Seleção dinâmica de colunas
- [x] Deduplicação por row_number
