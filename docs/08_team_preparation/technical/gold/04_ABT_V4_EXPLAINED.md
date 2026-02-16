# 04 - ABT v4 Builder Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/03_gold_abt_v4_builder.py` |
| **Função** | Construir ABT v4 - adiciona Cadastro (33 variáveis demográficas) |
| **Input** | Gold ABT v3 (spine) + Silver Cadastro (enriquecimento) |
| **Output** | Gold ABT v4 |
| **Registros** | 3,795,310 (1:1 com spine) |
| **Colunas** | ~185 |
| **Feature Blocks** | Score_01 + Score_02 + Telco + Cadastro |

---

## Contexto de Negócio

A ABT v4 adiciona **dados demográficos e cadastrais** do cliente:

1. **Idade do cliente:** Feature numérica derivada da data de nascimento
2. **Localização (CEP):** Proxy regional para análise geográfica
3. **Status cadastral:** Situação do cliente na Receita Federal
4. **Variáveis anônimas:** var_02 a var_25 (comportamento cadastral)

**Por que Cadastro é importante para risco de crédito?**
- Idade correlaciona com estabilidade financeira
- Região (CEP) pode indicar renda média
- Status RF indica regularidade fiscal
- Variáveis anônimas capturam padrões de comportamento

**Diferença crucial: Leitura da ABT v3 (não da Silver)**
- v1, v2, v3: Liam da Silver Bureau como spine
- **v4:** Lê da Gold ABT v3 como spine (encadeamento)
- Isso simplifica o código (não precisa repetir JOINs anteriores)

---

## O Que Muda em Relação à v3

| Aspecto | ABT v3 | ABT v4 |
|---------|--------|--------|
| **Spine** | Silver Bureau | **Gold ABT v3** |
| **Novo JOIN** | Telco | Cadastro |
| **Features novas** | 136 (68 vars + 68 flags) | +57 (idade, CEP, 24 vars + flags) |
| **Colunas** | ~85 | ~185 (+100) |
| **Feature Blocks** | "score_01,score_02,telco" | "score_01,score_02,telco,cadastro" |

---

## Código Explicado Linha por Linha

### 1. Configuração - Variáveis com Zero-Padding (Linhas 78-80)

```python
# Variáveis Cadastro esperadas (var_02 a var_25 = 24 variáveis)
CADASTRO_VAR_COLUMNS = [f"var_{i}" for i in range(2, 26)]
```

**Por que range(2, 26)?**
- var_01 não existe no Cadastro (começa em var_02)
- var_26 a var_93 são Telco (já incluídas em v3)
- Cadastro usa var_02 a var_25 = 24 variáveis

**Diferença de nomenclatura: Telco vs Cadastro**

| Fonte | Nomenclatura | Exemplo |
|-------|--------------|---------|
| Telco | Sem zero-padding | var_26, var_27, ..., var_93 |
| Cadastro | **Com zero-padding** | var_02, var_03, ..., var_25 |

Isso é uma inconsistência do source original. O script precisa lidar com ambos os padrões.

---

### 2. Função build_abt_v4 - Preparação do Spine v3 (Linhas 112-124)

```python
def build_abt_v4(df_abt_v3, df_cadastro):
    print(">>> [Transform] JOIN ABT v3 + Cadastro para ABT v4...")

    # Step 1: Preparar subset do ABT v3 (manter como spine)
    v3_cols = ["num_cpf", "safra", "dt_safra", "flag_instalacao_int", "fpd_int"]
    v3_cols.extend(["score_01_adj", "flag_score01_missing"])
    v3_cols.extend(["score_02_adj", "flag_score02_missing"])
    v3_cols.extend([f"var_{i}_adj" for i in range(26, 94)])
    v3_cols.extend([f"flag_var_{i}_missing" for i in range(26, 94)])
    v3_cols.extend(["prod", "flag_mig2"])
    v3_cols.extend(["metadata_data_ingestao", "metadata_nome_arquivo_origem",
                    "metadata_sistema_origem", "metadata_data_transformacao",
                    "metadata_versao_regra"])
    v3_cols.extend(["gold_version", "gold_build_date", "gold_feature_blocks"])

    df_abt_v3_prepared = df_abt_v3.select(*[col for col in v3_cols if col in df_abt_v3.columns])
```

**Por que usar `.extend()` em vez de concatenação?**

```python
# ABORDAGEM ESCOLHIDA: extend() é mais legível para listas longas
v3_cols = ["num_cpf", "safra"]
v3_cols.extend(["score_01_adj", "flag_score01_missing"])
v3_cols.extend([f"var_{i}_adj" for i in range(26, 94)])

# ALTERNATIVA: concatenação em linha única (difícil de ler)
v3_cols = ["num_cpf", "safra"] + ["score_01_adj", "flag_score01_missing"] + [f"var_{i}_adj" for i in range(26, 94)]
```

**Filtro defensivo no select:**
```python
df_abt_v3_prepared = df_abt_v3.select(*[col for col in v3_cols if col in df_abt_v3.columns])
```
- Usa list comprehension para filtrar apenas colunas existentes
- Evita erro se alguma coluna esperada não existir

---

### 3. Preparação do Cadastro - Variáveis Demográficas (Linhas 126-155)

```python
    # Step 2: Preparar subset do Cadastro para join
    cadastro_cols_to_select = ["num_cpf", "safra"]

    # Adicionar variáveis demográficas
    if "idade_anos" in df_cadastro.columns:
        cadastro_cols_to_select.append("idade_anos")
    if "flag_idade_menor_18" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_idade_menor_18")
    if "flag_idade_muito_alta" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_idade_muito_alta")
    if "cep_3_digitos" in df_cadastro.columns:
        cadastro_cols_to_select.append("cep_3_digitos")
    if "flag_cep_missing" in df_cadastro.columns:
        cadastro_cols_to_select.append("flag_cep_missing")
    if "statusrf" in df_cadastro.columns:
        cadastro_cols_to_select.append("statusrf")
```

**Explicação das variáveis demográficas:**

| Variável | Tipo | Descrição | Uso |
|----------|------|-----------|-----|
| `idade_anos` | int | Idade em anos completos | Feature numérica |
| `flag_idade_menor_18` | int (0/1) | Cliente < 18 anos | Sanity check |
| `flag_idade_muito_alta` | int (0/1) | Cliente > 100 anos | Outlier detection |
| `cep_3_digitos` | string | 3 primeiros dígitos do CEP | Proxy regional |
| `flag_cep_missing` | int (0/1) | CEP não informado | Missing indicator |
| `statusrf` | string | Status na Receita Federal | Categórica |

**Por que `cep_3_digitos` e não CEP completo?**
- CEP completo tem alta cardinalidade (muitos valores únicos)
- 3 dígitos agrupa por região geográfica
- Reduz overfitting e melhora generalização

**Por que flags de sanity check para idade?**
- `idade < 18`: Menor de idade não pode contratar sozinho
- `idade > 100`: Possível erro de cadastro ou fraude
- Permite filtrar ou tratar esses casos no modelo

---

### 4. Variáveis Cadastrais com Zero-Padding (Linhas 143-154)

```python
    # Adicionar var_02 a var_25 (com zero-padding: var_02, var_03, ..., var_25)
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_cadastro.columns:
            cadastro_cols_to_select.append(var_col)

    # Adicionar flags de missing para cada var
    for var_idx in range(2, 26):
        flag_col = f"flag_var_{var_idx:02d}_missing"
        if flag_col in df_cadastro.columns:
            cadastro_cols_to_select.append(flag_col)
```

**Explicação do formato `{var_idx:02d}`:**

```python
f"var_{var_idx:02d}"
#          └──┬──┘
#             └── Formato: 2 dígitos, preenchido com zero à esquerda

# Exemplos:
f"var_{2:02d}"   # → "var_02"
f"var_{10:02d}"  # → "var_10"
f"var_{5:02d}"   # → "var_05"
```

**Diferença entre Telco e Cadastro:**
```python
# Telco (v3): SEM zero-padding
[f"var_{i}_adj" for i in range(26, 94)]  # var_26_adj, var_27_adj, ...

# Cadastro (v4): COM zero-padding
[f"var_{i:02d}" for i in range(2, 26)]   # var_02, var_03, ..., var_25
```

Esta inconsistência vem dos dados originais. O script precisa tratar cada fonte conforme seu padrão.

---

### 5. LEFT JOIN - ABT v3 + Cadastro (Linhas 157-164)

```python
    # Step 3: JOIN LEFT ABT v3 + Cadastro (v3 é spine)
    print(">>> [Transform] Executando LEFT JOIN ABT_v3.NUM_CPF+SAFRA = Cadastro.NUM_CPF+SAFRA...")

    df_abt = df_abt_v3_prepared.join(
        df_cadastro_prepared,
        on=["num_cpf", "safra"],
        how="left"
    )
```

**Mudança conceitual importante:**
- v3: Bureau (Silver) JOIN Telco (Silver)
- **v4: ABT v3 (Gold) JOIN Cadastro (Silver)**

**Por que usar ABT v3 como spine e não Silver Bureau?**

| Abordagem | Prós | Contras |
|-----------|------|---------|
| **Encadeamento (v4 lê v3)** | Código simples, não repete JOINs | Depende de v3 estar atualizado |
| Reconstrução (v4 lê Silver) | Independente | Repete todos os JOINs anteriores |

A abordagem de encadeamento foi escolhida por simplicidade.

---

### 6. Atualização de Metadados (Linhas 166-174)

```python
    # Step 4: Atualizar metadados de gold (version, feature blocks, build date)
    print(">>> [Transform] Atualizando metadados de Gold (version, feature blocks, build date)...")
    from datetime import datetime
    build_date = datetime.now().isoformat()

    df_abt = df_abt \
        .withColumn("gold_version", F.lit("gold_abt_v4")) \
        .withColumn("gold_build_date", F.lit(build_date)) \
        .withColumn("gold_feature_blocks", F.lit("score_01,score_02,telco,cadastro"))
```

**Por que importar datetime dentro da função?**
- Import tardio (lazy import)
- Usado apenas neste ponto do código
- Não afeta performance significativamente

**Por que `datetime.now().isoformat()` em vez de `F.current_timestamp()`?**
```python
# ABORDAGEM ESCOLHIDA: string ISO format
build_date = datetime.now().isoformat()  # "2026-01-29T10:45:32.123456"
df = df.withColumn("gold_build_date", F.lit(build_date))

# ALTERNATIVA: timestamp Spark
df = df.withColumn("gold_build_date", F.current_timestamp())
```

Ambas são válidas. A string ISO é mais portável entre sistemas.

---

### 7. Seleção Final com Filtro de Existência (Linhas 176-217)

```python
    # Step 5: Selecionar e ordenar colunas logicamente
    final_cols = [
        # CHAVES
        "num_cpf", "safra", "dt_safra",

        # LABELS
        "flag_instalacao_int", "fpd_int",

        # FEATURES v1-v2 (Scores)
        "score_01_adj", "flag_score01_missing",
        "score_02_adj", "flag_score02_missing",

        # FEATURES v3 (Telco 68 vars)
        *[f"var_{i}_adj" for i in range(26, 94)],
        *[f"flag_var_{i}_missing" for i in range(26, 94)],

        # FEATURES v4 (Cadastro - Demográficas)
        "idade_anos", "flag_idade_menor_18", "flag_idade_muito_alta",
        "cep_3_digitos", "flag_cep_missing", "statusrf",

        # FEATURES v4 (Cadastro - Variáveis anonimizadas)
        *[f"var_{i:02d}" for i in range(2, 26) if f"var_{i:02d}" in df_abt.columns],
        *[f"flag_var_{i:02d}_missing" for i in range(2, 26) if f"flag_var_{i:02d}_missing" in df_abt.columns],

        # METADADOS + AUDITORIA + GOLD
        ...
    ]

    # Filtrar apenas colunas que existem no dataframe
    final_cols = [col for col in final_cols if col in df_abt.columns]

    df_abt = df_abt.select(*final_cols)
```

**Filtro duplo de existência:**

1. **Inline na list comprehension:** `if f"var_{i:02d}" in df_abt.columns`
2. **Após construir a lista:** `[col for col in final_cols if col in df_abt.columns]`

**Por que dois filtros?**
- O primeiro é necessário porque a list comprehension é expandida ANTES do JOIN
- O segundo é uma rede de segurança para colunas fixas (idade_anos, etc.)
- Código defensivo contra mudanças no schema do source

---

### 8. Relatório - Detalhamento por Variável (Linhas 377-386)

```python
    # Breakdown por variável de Cadastro (para debug)
    print(f"\n>>> [Features v4 - Cadastro Detail] Distribuição por variável:")
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            coverage_pct = ((count_out - null_count) / count_out) * 100 if count_out > 0 else 0
            print(f"      {var_col}: {coverage_pct:>6.2f}% ({count_out - null_count:>10} não-NULL)")
        else:
            print(f"      {var_col}: NÃO ENCONTRADO NA TABELA")
```

**Novo em v4: Detalhamento individual de cobertura**

- v3 mostrava apenas cobertura agregada para Telco
- v4 mostra cobertura por variável de Cadastro
- Útil para identificar variáveis problemáticas

**Formato de saída:**
```
var_02:  35.42% (  1,344,532 não-NULL)
var_03:  35.42% (  1,344,532 não-NULL)
...
var_25:  35.42% (  1,344,532 não-NULL)
```

---

### 9. Análise de Match com Condição OR (Linhas 388-405)

```python
    # Impacto do join (quantos registros encontraram match em Cadastro)
    cadastro_match_conditions = []
    for var_idx in range(2, 26):
        var_col = f"var_{var_idx:02d}"
        if var_col in df_abt.columns:
            cadastro_match_conditions.append(F.col(var_col).isNotNull())

    if cadastro_match_conditions:
        # Se houver alguma condição, usar OR
        match_condition = cadastro_match_conditions[0]
        for cond in cadastro_match_conditions[1:]:
            match_condition = match_condition | cond
        cadastro_match_count = df_abt.filter(match_condition).count()
    else:
        cadastro_match_count = 0
```

**Por que usar OR em vez de verificar apenas var_02?**

Em v3, usamos apenas `var_26_adj.isNotNull()` como proxy de match. Isso funciona se todas as variáveis têm o mesmo padrão de missing.

Em v4, as variáveis de Cadastro podem ter missings diferentes (algumas preenchidas, outras não). O OR garante que contamos registros que têm **pelo menos uma** variável de Cadastro.

**Construção dinâmica da condição OR:**
```python
# Inicia com a primeira condição
match_condition = cadastro_match_conditions[0]  # var_02.isNotNull()

# Adiciona OR para cada condição subsequente
for cond in cadastro_match_conditions[1:]:
    match_condition = match_condition | cond
    # Resultado: var_02.isNotNull() | var_03.isNotNull() | ... | var_25.isNotNull()
```

---

## Diagrama de Fluxo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       03_gold_abt_v4_builder.py                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐              ┌──────────────────┐                     │
│  │  LEITURA ABT v3  │              │ LEITURA CADASTRO │                     │
│  │  (Gold - spine)  │              │ (Silver - enriq) │                     │
│  │  3,795,310 reg   │              │  ~1,300,000 reg  │                     │
│  │  ~85 colunas     │              │  ~30 colunas     │                     │
│  └────────┬─────────┘              └────────┬─────────┘                     │
│           │                                 │                               │
│           ▼                                 ▼                               │
│  ┌──────────────────┐              ┌──────────────────┐                     │
│  │ PREPARAR ABT v3  │              │ PREPARAR CADASTRO│                     │
│  │ - Todas colunas  │              │ - Chaves         │                     │
│  │   existentes     │              │ - idade_anos     │                     │
│  │ - Filtro         │              │ - cep_3_digitos  │                     │
│  │   defensivo      │              │ - var_02..var_25 │                     │
│  └────────┬─────────┘              └────────┬─────────┘                     │
│           │                                 │                               │
│           └──────────────┬──────────────────┘                               │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │      LEFT JOIN        │                                      │
│              │ ON (num_cpf, safra)   │                                      │
│              │                       │                                      │
│              │ ABT v3: 3,795,310     │                                      │
│              │ Cadastro match: ~35%  │                                      │
│              │ Resultado: 3,795,310  │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ ATUALIZAR METADADOS   │                                      │
│              │ gold_version = v4     │                                      │
│              │ feature_blocks += cad │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ SELECT FINAL          │                                      │
│              │ - Chaves (3)          │                                      │
│              │ - Labels (2)          │                                      │
│              │ - Scores (4)          │                                      │
│              │ - Telco (136)         │                                      │
│              │ - Cadastro (~57)      │  ← NOVO                              │
│              │ - Metadados (~10)     │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ VALIDAÇÃO + ESCRITA   │                                      │
│              │ validate_abt_v4()     │                                      │
│              │ Delta + Unity Catalog │                                      │
│              └───────────────────────┘                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Colunas de Saída (ABT v4)

### Resumo por Grupo

| Grupo | Colunas | Quantidade | Origem |
|-------|---------|------------|--------|
| Chaves | num_cpf, safra, dt_safra | 3 | Bureau |
| Labels | flag_instalacao_int, fpd_int | 2 | Bureau |
| Features v1 | score_01_adj, flag | 2 | Bureau |
| Features v2 | score_02_adj, flag | 2 | Bureau |
| Features v3 | var_26..var_93 + flags | 136 | Telco |
| **Features v4 (demo)** | idade, CEP, statusrf + flags | **9** | **Cadastro** |
| **Features v4 (vars)** | var_02..var_25 + flags | **48** | **Cadastro** |
| Metadados | prod, flag_mig2, metadata_*, gold_* | ~13 | Mixed |
| **TOTAL** | | **~215** | |

### Detalhamento das Novas Colunas (Cadastro)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| idade_anos | int | Idade em anos completos |
| flag_idade_menor_18 | int | 1 se idade < 18 |
| flag_idade_muito_alta | int | 1 se idade > 100 |
| cep_3_digitos | string | Primeiros 3 dígitos do CEP |
| flag_cep_missing | int | 1 se CEP não informado |
| statusrf | string | Status Receita Federal |
| var_02 ... var_25 | mixed | 24 variáveis anônimas |
| flag_var_02_missing ... | int | 24 flags de missing |

---

## Problema Histórico: idade_anos Vazia

Durante o desenvolvimento, descobriu-se que `idade_anos` estava 100% NULL.

**Causa:** A Silver Cadastro usava Python UDF para calcular idade, que falhou silenciosamente no Databricks.

**Solução:** Substituir UDF por funções nativas do Spark (`F.to_date()`, `F.datediff()`).

**Documentação:** Ver `docs/07_troubleshooting/FIX_IDADE_ANOS_EXECUTION_GUIDE.md`

**Lição:** Nunca usar Python UDFs em pipelines Databricks. Preferir funções nativas do Spark.

---

## Validações Específicas de v4

A função `validate_abt_v4()` adiciona:

```python
# Gate 8: Cobertura Cadastro > 20%
cadastro_coverage = df.filter(F.col("idade_anos").isNotNull()).count() / total
assert cadastro_coverage > 0.20, f"Cadastro coverage muito baixa: {cadastro_coverage}"

# Gate 9: Sanity check idade (não deve ter menores de 18 com FPD)
menores_com_fpd = df.filter(
    (F.col("flag_idade_menor_18") == 1) &
    (F.col("fpd_int") == 1)
).count()
# Apenas warning, não bloqueia
if menores_com_fpd > 0:
    print(f"WARNING: {menores_com_fpd} menores de 18 com FPD")
```

---

## Lições Aprendidas

### 1. Encadeamento de ABTs

**Escolha:** v4 lê de ABT v3 (Gold), não de Silver Bureau.
**Vantagem:** Código mais simples, sem repetir JOINs.
**Cuidado:** Requer que v3 esteja atualizado antes de rodar v4.

### 2. Nomenclatura Inconsistente (Zero-Padding)

**Problema:** Telco usa `var_26`, Cadastro usa `var_02`.
**Solução:** Tratar cada fonte com seu padrão específico.
**Ideal:** Padronizar na Silver para evitar confusão.

### 3. UDFs Falham Silenciosamente

**Problema:** `idade_anos` era 100% NULL por falha de UDF.
**Solução:** Usar funções nativas do Spark.
**Regra:** NUNCA usar Python UDFs em Databricks.

### 4. Filtro Defensivo de Colunas

**Problema:** Colunas podem não existir se source mudar.
**Solução:** `if col in df.columns` antes de usar.
**Benefício:** Script não quebra com mudanças de schema.

---

## Exemplo de Saída do Relatório

```
================================================================================
RELATÓRIO FINAL - ABT v4 (Score_01 + Score_02 + Telco + Cadastro)
================================================================================

>>> [Stats] FLAG_INSTALACAO (decisão observada):
    FLAG=0:    1161410 (30.60%)
    FLAG=1:    2633900 (69.40%)

>>> [Features v1-v2] Completude:
    SCORE_01_ADJ: 98.18%
    SCORE_02_ADJ: 99.95%

>>> [Features v3 - Telco] Completude:
    Cobertura agregada Telco (var_26-93): 35.46%

>>> [Features v4 - Cadastro] Completude:
    Cobertura agregada Cadastro (var_02-25): 35.42%

>>> [Features v4 - Cadastro Detail] Distribuição por variável:
      var_02:  35.42% (  1,344,532 não-NULL)
      var_03:  35.42% (  1,344,532 não-NULL)
      ...
      var_25:  35.42% (  1,344,532 não-NULL)

>>> [JOIN] Impacto Cadastro:
    Total ABT v3 (spine):              3,795,310
    Total Cadastro (enriquecimento):   1,345,892
    Resultados ABT v4 (ABT v3 LEFT JOIN):   3,795,310
    Registros com match Cadastro:      1,344,532 (35.42%)

================================================================================
✓ ABT v4 PRONTA PARA MODELAGEM
  - Versão: gold_abt_v4
  - Feature blocks: Score_01, Score_02, Telco (68 variáveis), Cadastro (24 variáveis)
  - Total registros: 3,795,310
  - Status: Incremental (Cadastro adiciona 35.4% cobertura, 35.4% match)
================================================================================
```

---

## Checklist de Revisão

- [x] Lê de ABT v3 (Gold), não de Silver Bureau
- [x] LEFT JOIN preserva todos os registros do spine
- [x] Zero-padding correto para variáveis Cadastro (`var_{i:02d}`)
- [x] Flags demográficos incluídos (idade, CEP)
- [x] Sanity checks para idade (< 18, > 100)
- [x] Condição OR para calcular match de Cadastro
- [x] Detalhamento por variável no relatório
- [x] Metadados gold_feature_blocks atualizados

---

## Próximo Passo

A ABT v4 serve como base para a ABT v5, que adiciona features de Recarga:

```
ABT v4 (Scores + Telco + Cadastro) → 04_gold_abt_v5_builder_v2.py → ABT v5 (+ Recarga M1/M3/M6)
```

**Nota importante:** A partir de v5, features são **temporais** (janelas M1, M3, M6), o que requer cuidado especial com anti-leakage.

Ver [05_ABT_V5_EXPLAINED.md](05_ABT_V5_EXPLAINED.md).
