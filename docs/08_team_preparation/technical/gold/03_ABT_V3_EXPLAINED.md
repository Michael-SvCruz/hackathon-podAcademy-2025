# 03 - ABT v3 Builder Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/02_gold_abt_v3_builder.py` |
| **Função** | Construir ABT v3 - adiciona Telco (68 variáveis) |
| **Input** | Silver Bureau (spine) + Silver Telco (enriquecimento) |
| **Output** | Gold ABT v3 |
| **Registros** | 3,795,310 (1:1 com spine) |
| **Colunas** | ~85 |
| **Feature Blocks** | Score_01 + Score_02 + Telco |

---

## Contexto de Negócio

A ABT v3 é o **primeiro script com JOIN externo**:

1. **Bureau continua como spine:** Define o universo oficial
2. **Telco enriquece o spine:** Adiciona 68 variáveis de comportamento telecom
3. **LEFT JOIN preserva todos:** Registros sem match em Telco ficam com NULL

**O que são as variáveis Telco?**
- Variáveis anonimizadas (var_26 a var_93)
- Representam comportamentos de uso de telefonia/dados
- Calculadas pela Claro com metodologia proprietária
- Contêm valor sentinela **304** = "não informado"

**Por que 68 variáveis?**
- var_26 a var_93 = 68 variáveis (93 - 26 + 1 = 68)
- Numeração começa em 26 porque var_01 a var_25 não foram disponibilizadas

---

## O Que Muda em Relação à v2

| Aspecto | ABT v2 | ABT v3 |
|---------|--------|--------|
| **Fontes** | Bureau apenas | Bureau + Telco (JOIN) |
| **Features** | 4 (2 scores + 2 flags) | 4 + 136 (68 vars + 68 flags) |
| **Colunas** | ~18 | ~85 (+67) |
| **Operação** | SELECT | LEFT JOIN |
| **Feature Blocks** | "score_01,score_02" | "score_01,score_02,telco" |

---

## Código Explicado Linha por Linha

### 1. Configuração - Lista de Variáveis Telco (Linhas 75-77)

```python
# Variáveis Telco esperadas (var_26 a var_93 = 68 colunas)
TELCO_VAR_COLUMNS = [f"var_{i}" for i in range(26, 94)]
```

**Explicação da list comprehension:**

```python
[f"var_{i}" for i in range(26, 94)]
# Equivale a:
# ["var_26", "var_27", "var_28", ..., "var_92", "var_93"]
```

**Por que range(26, 94) e não range(26, 93)?**
- `range(a, b)` em Python gera valores de `a` até `b-1`
- Para incluir 93, usamos `range(26, 94)` → 26 até 93

**Por que definir como constante?**
- Documentação implícita (quantas variáveis existem)
- Reutilização em múltiplos lugares do código
- Fácil manutenção se o range mudar

---

### 2. Função build_abt_v3 - Preparação do Bureau (Linhas 104-132)

```python
def build_abt_v3(df_bureau, df_telco):
    """
    Constrói ABT v3: estende v2 com features Telco (var_26-93).
    """
    print(">>> [Transform] JOIN Bureau + Telco para ABT v3...")

    # Step 1: Preparar subset do Bureau para join (apenas chaves + v1/v2 features)
    df_bureau_prepared = df_bureau.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",

        # Labels
        "flag_instalacao_int",
        "fpd_int",

        # Features v1
        "score_01_adj",
        "flag_score01_missing",

        # Features v2 (raw, será ajustado aqui em v3)
        "score_02_dbl",

        # Metadados Bureau
        "prod",
        "flag_mig2",

        # Auditoria
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
```

**Por que preparar um subset antes do JOIN?**
- **Performance:** Menos colunas = menos dados trafegando no shuffle
- **Clareza:** Documenta exatamente quais colunas vêm de cada fonte
- **Prevenção de conflitos:** Evita colunas duplicadas após JOIN

**Por que `score_02_dbl` e não `score_02_adj`?**
- O tratamento de sentinela do Score_02 será feito neste script (Step 4)
- Isso é uma inconsistência de design (idealmente seria na Silver)
- Mantido por compatibilidade com pipeline existente

---

### 3. Preparação Dinâmica do Telco (Linhas 134-149)

```python
    # Step 2: Preparar subset do Telco para join (apenas var_*_adj + flags)
    telco_cols_to_select = ["num_cpf", "safra"]

    # Adicionar var_*_adj (ajustadas com sentinela tratada)
    for var_idx in range(26, 94):
        var_col_adj = f"var_{var_idx}_adj"
        if var_col_adj in df_telco.columns:
            telco_cols_to_select.append(var_col_adj)

    # Adicionar flags de missing
    for var_idx in range(26, 94):
        flag_col = f"flag_var_{var_idx}_missing"
        if flag_col in df_telco.columns:
            telco_cols_to_select.append(flag_col)

    df_telco_prepared = df_telco.select(*telco_cols_to_select)
```

**Explicação do padrão dinâmico:**

| Etapa | Código | Resultado |
|-------|--------|-----------|
| 1. Inicializa lista | `["num_cpf", "safra"]` | Chaves para JOIN |
| 2. Loop var_*_adj | `for var_idx in range(26, 94)` | Adiciona 68 colunas ajustadas |
| 3. Loop flags | `for var_idx in range(26, 94)` | Adiciona 68 flags de missing |
| 4. Select final | `df_telco.select(*telco_cols_to_select)` | Aplica seleção |

**Por que verificar `if var_col_adj in df_telco.columns`?**
- **Defensivo:** Protege contra colunas faltantes no source
- **Flexibilidade:** Permite rodar mesmo se algumas variáveis não existirem
- Em produção, todas as 68 devem existir

**O que significa o `*` em `select(*telco_cols_to_select)`?**
```python
# Com asterisco: desempacota a lista como argumentos posicionais
df.select(*["col1", "col2", "col3"])
# Equivale a:
df.select("col1", "col2", "col3")

# Sem asterisco: passaria a lista como um único argumento (ERRO!)
df.select(["col1", "col2", "col3"])  # TypeError!
```

---

### 4. LEFT JOIN - Operação Central (Linhas 151-158)

```python
    # Step 3: JOIN LEFT Bureau + Telco (Bureau é spine)
    print(">>> [Transform] Executando LEFT JOIN Bureau.NUM_CPF+SAFRA = Telco.NUM_CPF+SAFRA...")

    df_abt = df_bureau_prepared.join(
        df_telco_prepared,
        on=["num_cpf", "safra"],
        how="left"
    )
```

**Por que LEFT JOIN e não INNER JOIN?**

| Tipo JOIN | Resultado | Uso |
|-----------|-----------|-----|
| **LEFT** | Mantém TODOS do Bureau, NULLs onde não há match | ✅ **CORRETO** |
| INNER | Mantém apenas registros com match em ambos | ❌ Perderia registros |
| OUTER | Mantém todos de ambos os lados | ❌ Adicionaria registros extras |

**O Bureau é o spine (universo oficial).** Não podemos perder registros.

**Diagrama do LEFT JOIN:**

```
BUREAU (spine)              TELCO (enriquecimento)
┌─────────┬────────┐        ┌─────────┬────────┬─────────┐
│ num_cpf │ safra  │        │ num_cpf │ safra  │ var_26  │
├─────────┼────────┤        ├─────────┼────────┼─────────┤
│ AAA     │ 202401 │◄──────►│ AAA     │ 202401 │   500   │  ✓ Match
│ BBB     │ 202401 │        │ CCC     │ 202401 │   600   │
│ CCC     │ 202401 │◄──────►│ CCC     │ 202401 │   600   │  ✓ Match
│ DDD     │ 202401 │        │         │        │         │  ✗ Sem match
└─────────┴────────┘        └─────────┴────────┴─────────┘

RESULTADO (LEFT JOIN):
┌─────────┬────────┬─────────┐
│ num_cpf │ safra  │ var_26  │
├─────────┼────────┼─────────┤
│ AAA     │ 202401 │   500   │  ← Match encontrado
│ BBB     │ 202401 │  NULL   │  ← Sem match → NULL
│ CCC     │ 202401 │   600   │  ← Match encontrado
│ DDD     │ 202401 │  NULL   │  ← Sem match → NULL
└─────────┴────────┴─────────┘
```

**Por que `on=["num_cpf", "safra"]` e não `.join(..., df_bureau.num_cpf == df_telco.num_cpf)`?**

```python
# ABORDAGEM ESCOLHIDA: Lista de colunas (recomendada)
df_abt = df_bureau.join(df_telco, on=["num_cpf", "safra"], how="left")
# Resultado: colunas num_cpf e safra aparecem UMA vez

# ALTERNATIVA: Condição explícita
df_abt = df_bureau.join(
    df_telco,
    (df_bureau.num_cpf == df_telco.num_cpf) & (df_bureau.safra == df_telco.safra),
    how="left"
)
# Resultado: colunas num_cpf e safra aparecem DUAS vezes (ambíguo!)
```

A lista de colunas evita duplicação e é mais legível.

---

### 5. Tratamento de Sentinela Score_02 (Linhas 160-169)

```python
    # Step 4: Tratar sentinela em SCORE_02 (0 → NULL) - CRÍTICO
    # Score_02 vem como score_02_dbl da Silver Bureau
    print(">>> [Transform] Tratando sentinela Score_02 (0 → NULL)...")
    df_abt = df_abt.withColumn(
        "score_02_adj",
        F.when(F.col("score_02_dbl") == 0, F.lit(None)).otherwise(F.col("score_02_dbl"))
    ).withColumn(
        "flag_score02_missing",
        F.when(F.col("score_02_dbl").isNull() | (F.col("score_02_dbl") == 0), F.lit(1)).otherwise(F.lit(0))
    ).drop("score_02_dbl")
```

**Por que tratar Score_02 novamente aqui?**
- Na v2, o tratamento foi feito na própria v2
- Na v3, lemos diretamente da Silver Bureau (não da ABT v2)
- Portanto, precisamos repetir o tratamento

**Isso é redundância?**
- Sim, é uma inconsistência de design
- Idealmente, Score_02 deveria ser tratado na Silver Bureau
- Mantido assim por compatibilidade e para não modificar a Silver

---

### 6. Seleção Final com Expansão de Lista (Linhas 171-204)

```python
    # Step 5: Selecionar e ordenar colunas logicamente
    df_abt = df_abt.select(
        # CHAVES
        "num_cpf",
        "safra",
        "dt_safra",

        # LABELS
        "flag_instalacao_int",
        "fpd_int",

        # FEATURES v1 (SCORE_01)
        "score_01_adj",
        "flag_score01_missing",

        # FEATURES v2 (SCORE_02)
        "score_02_adj",
        "flag_score02_missing",

        # FEATURES v3 (TELCO) - NOVAS - Variáveis anonimizadas var_26 a var_93
        *[f"var_{i}_adj" for i in range(26, 94)],
        *[f"flag_var_{i}_missing" for i in range(26, 94)],

        # METADADOS
        "prod",
        "flag_mig2",

        # AUDITORIA
        "metadata_data_ingestao",
        ...
    )
```

**Explicação da sintaxe `*[...]` dentro do select:**

```python
# Esta linha:
*[f"var_{i}_adj" for i in range(26, 94)]

# É equivalente a escrever manualmente:
"var_26_adj", "var_27_adj", "var_28_adj", ..., "var_93_adj"
```

**Por que usar expansão de lista em vez de listar tudo?**
- **Concisão:** 1 linha em vez de 68
- **Manutenção:** Alterar range é mais fácil que editar 68 linhas
- **Consistência:** Garante que todas as variáveis seguem o padrão

---

### 7. Leitura de Duas Fontes no main() (Linhas 236-260)

```python
    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_bureau_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_bureau_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA BUREAU: {e}")
        sys.exit(1)

    count_in_bureau = df_bureau.count()
    print(f">>> [Info] Registros no Silver Bureau: {count_in_bureau}")

    # =========================================================================
    # 2) LEITURA SILVER TELCO (ENRIQUECIMENTO)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Telco (Enriquecimento): {args.silver_telco_path}")
    try:
        df_telco = spark.read.format(args.format).load(args.silver_telco_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA TELCO: {e}")
        sys.exit(1)

    count_in_telco = df_telco.count()
    print(f">>> [Info] Registros no Silver Telco: {count_in_telco}")
```

**Padrão de leitura com try/except separados:**
- Permite identificar QUAL fonte falhou
- Mensagens de erro específicas para cada fonte
- `sys.exit(1)` interrompe execução se qualquer fonte falhar

---

### 8. Relatório - Cobertura Telco (Linhas 336-352)

```python
    # Completude de features v3 (Telco)
    telco_var_nulls = {}
    for var_idx in range(26, 94):
        var_col = f"var_{var_idx}_adj"
        if var_col in df_abt.columns:
            null_count = df_abt.filter(F.col(var_col).isNull()).count()
            telco_var_nulls[var_idx] = null_count

    telco_total_nulls = sum(telco_var_nulls.values())
    telco_total_cells = len(telco_var_nulls) * count_out
    telco_coverage = ((telco_total_cells - telco_total_nulls) / telco_total_cells) * 100 if telco_total_cells > 0 else 0

    print(f"\n>>> [Features v3 - Telco] Completude:")
    print(f"    Cobertura agregada Telco (var_26-93): {telco_coverage:.2f}%")
```

**Explicação do cálculo de cobertura agregada:**

```
telco_total_cells = 68 variáveis × 3,795,310 registros = 258,081,080 células
telco_total_nulls = soma de NULLs em todas as 68 variáveis
telco_coverage = (células_não_nulas / total_células) × 100
```

**Por que calcular cobertura agregada e não por variável?**
- 68 variáveis individuais seria muito verboso
- Cobertura agregada dá visão geral
- Se necessário, pode-se expandir para mostrar por variável

---

### 9. Análise de Match do JOIN (Linhas 353-363)

```python
    # Impacto do join (quantos registros encontraram match em Telco)
    telco_match_count = df_abt.filter(
        F.col("var_26_adj").isNotNull() if "var_26_adj" in df_abt.columns else F.lit(False)
    ).count()
    telco_match_pct = (telco_match_count / count_out) * 100 if count_out > 0 else 0

    print(f"\n>>> [JOIN] Impacto Telco:")
    print(f"    Total Bureau (spine): {count_in_bureau:>12}")
    print(f"    Total Telco (enriquecimento): {count_in_telco:>12}")
    print(f"    Resultados ABT v3 (Bureau LEFT JOIN): {count_out:>12}")
    print(f"    Registros com match Telco: {telco_match_count:>12} ({telco_match_pct:.2f}%)")
```

**Por que usar var_26_adj como indicador de match?**
- Se var_26_adj é NOT NULL, significa que houve match no JOIN
- var_26 é a primeira variável Telco, serve como proxy
- Alternativa: criar coluna explícita `flag_telco_found`

**Por que verificar `if "var_26_adj" in df_abt.columns`?**
- Defensivo contra caso onde Telco está vazia
- Evita erro se a coluna não existir

---

## Diagrama de Fluxo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       02_gold_abt_v3_builder.py                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐              ┌──────────────────┐                     │
│  │  LEITURA BUREAU  │              │  LEITURA TELCO   │                     │
│  │  (spine)         │              │  (enriquecimento)│                     │
│  │  3,795,310 reg   │              │  ~1,300,000 reg  │                     │
│  └────────┬─────────┘              └────────┬─────────┘                     │
│           │                                 │                               │
│           ▼                                 ▼                               │
│  ┌──────────────────┐              ┌──────────────────┐                     │
│  │ PREPARAR BUREAU  │              │ PREPARAR TELCO   │                     │
│  │ - Chaves         │              │ - Chaves         │                     │
│  │ - Labels         │              │ - var_*_adj (68) │                     │
│  │ - Scores         │              │ - flags (68)     │                     │
│  │ - Metadados      │              │                  │                     │
│  └────────┬─────────┘              └────────┬─────────┘                     │
│           │                                 │                               │
│           └──────────────┬──────────────────┘                               │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │      LEFT JOIN        │                                      │
│              │ ON (num_cpf, safra)   │                                      │
│              │                       │                                      │
│              │ Bureau: 3,795,310     │                                      │
│              │ Telco match: ~35%     │                                      │
│              │ Resultado: 3,795,310  │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ TRATAR SENTINELA      │                                      │
│              │ Score_02: 0 → NULL    │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ SELECT FINAL          │                                      │
│              │ - Chaves (3)          │                                      │
│              │ - Labels (2)          │                                      │
│              │ - Scores (4)          │                                      │
│              │ - Telco (136)         │                                      │
│              │ - Metadados (~10)     │                                      │
│              └───────────┬───────────┘                                      │
│                          │                                                  │
│                          ▼                                                  │
│              ┌───────────────────────┐                                      │
│              │ VALIDAÇÃO + ESCRITA   │                                      │
│              │ validate_abt_v3()     │                                      │
│              │ Delta + Unity Catalog │                                      │
│              └───────────────────────┘                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Colunas de Saída (ABT v3)

### Resumo por Grupo

| Grupo | Colunas | Quantidade |
|-------|---------|------------|
| Chaves | num_cpf, safra, dt_safra | 3 |
| Labels | flag_instalacao_int, fpd_int | 2 |
| Features v1 | score_01_adj, flag_score01_missing | 2 |
| Features v2 | score_02_adj, flag_score02_missing | 2 |
| **Features v3** | var_26_adj ... var_93_adj | **68** |
| **Flags v3** | flag_var_26_missing ... flag_var_93_missing | **68** |
| Metadados | prod, flag_mig2, metadata_* | ~10 |
| Gold | gold_version, gold_build_date, gold_feature_blocks | 3 |
| **TOTAL** | | **~158** |

### Detalhamento das Variáveis Telco

| Coluna | Tipo | Sentinela | Descrição |
|--------|------|-----------|-----------|
| var_26_adj | double | 304 → NULL | Variável anônima Telco 26 |
| var_27_adj | double | 304 → NULL | Variável anônima Telco 27 |
| ... | ... | ... | ... |
| var_93_adj | double | 304 → NULL | Variável anônima Telco 93 |
| flag_var_26_missing | int | 0/1 | Missing indicator para var_26 |
| ... | ... | ... | ... |
| flag_var_93_missing | int | 0/1 | Missing indicator para var_93 |

**Nota:** As variáveis Telco são anonimizadas pela Claro. Não temos descrição do significado de cada uma.

---

## Sentinela 304 - Tratamento na Silver Telco

O tratamento do sentinela 304 é feito na **Silver Telco**, não neste script.

**Por que 304?**
- Valor específico definido pela Claro para indicar "não disponível"
- Não é um valor válido para nenhuma variável
- Similar ao conceito de -999 em outras bases

**O que a Silver Telco faz:**
```python
# Em 01_bronze_silver_telco.py (Silver)
for var_idx in range(26, 94):
    df = df.withColumn(
        f"var_{var_idx}_adj",
        F.when(F.col(f"var_{var_idx}") == 304, F.lit(None))
         .otherwise(F.col(f"var_{var_idx}"))
    ).withColumn(
        f"flag_var_{var_idx}_missing",
        F.when(F.col(f"var_{var_idx}").isNull() | (F.col(f"var_{var_idx}") == 304), 1)
         .otherwise(0)
    )
```

**Neste script (Gold v3):** Apenas selecionamos as colunas já tratadas.

---

## Validações Específicas de v3

A função `validate_abt_v3()` adiciona:

```python
# Gate 7: Cobertura Telco > 20%
telco_first_var = "var_26_adj"
telco_coverage = df.filter(F.col(telco_first_var).isNotNull()).count() / total
assert telco_coverage > 0.20, f"Telco coverage muito baixa: {telco_coverage}"

# Gate 8: JOIN não aumentou registros (LEFT JOIN mantém spine)
assert count_out == count_in_bureau, "JOIN alterou número de registros!"
```

---

## Lições Aprendidas

### 1. LEFT JOIN Preserva o Spine

**Regra de ouro:** O spine (Bureau) define o universo. JOINs de enriquecimento devem ser LEFT.

### 2. Preparar DataFrames Antes do JOIN

**Best practice:** Fazer select() antes do join() para:
- Reduzir dados no shuffle
- Evitar colunas duplicadas
- Documentar quais colunas vêm de cada fonte

### 3. Expansão de Lista para Muitas Colunas

**Padrão útil:** `*[f"col_{i}" for i in range(a, b)]` é mais manutenível que listar 68 colunas.

### 4. Verificação Defensiva de Colunas

**Best practice:** `if col in df.columns` antes de usar, especialmente para colunas dinâmicas.

---

## Exemplo de Saída do Relatório

```
================================================================================
RELATÓRIO FINAL - ABT v3 (Score_01 + Score_02 + Telco)
================================================================================

>>> [Stats] FLAG_INSTALACAO (decisão observada):
    FLAG=0:    1161410 (30.60%)
    FLAG=1:    2633900 (69.40%)

>>> [Stats] FPD (target, observado SÓ em FLAG_INSTALACAO=1):
    FPD=0:    2074671 (54.66%)
    FPD=1:     559229 (14.73%)

>>> [Features v1-v2] Completude:
    SCORE_01_ADJ: 98.18%
    SCORE_02_ADJ: 99.95%

>>> [Features v3 - Telco] Completude:
    Cobertura agregada Telco (var_26-93): 35.46%
    Total células Telco:    258,081,080
    Células NULLs:          166,563,821

>>> [JOIN] Impacto Telco:
    Total Bureau (spine):              3,795,310
    Total Telco (enriquecimento):      1,345,892
    Resultados ABT v3 (Bureau LEFT JOIN):   3,795,310
    Registros com match Telco:         1,345,892 (35.46%)

================================================================================
✓ ABT v3 PRONTA PARA MODELAGEM
  - Versão: gold_abt_v3
  - Feature blocks: Score_01, Score_02, Telco (68 variáveis)
  - Total registros: 3,795,310
  - Grão: 1:1 NUM_CPF + SAFRA
  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)
  - Status: Incremental (Telco adiciona 35.5% cobertura, 35.5% match)
================================================================================
```

---

## Checklist de Revisão

- [x] Dois inputs (Bureau + Telco) lidos separadamente
- [x] Preparação de subsets antes do JOIN
- [x] LEFT JOIN preserva todos os registros do spine
- [x] Seleção dinâmica de 68 variáveis via list comprehension
- [x] Tratamento de Score_02 (consistência com v2)
- [x] Metadados gold_feature_blocks = "score_01,score_02,telco"
- [x] Validação confirma que JOIN não alterou contagem
- [x] Relatório mostra taxa de match do JOIN

---

## Próximo Passo

A ABT v3 serve como base para a ABT v4, que adiciona variáveis de Cadastro:

```
ABT v3 (Scores + Telco) → 03_gold_abt_v4_builder.py → ABT v4 (+ Cadastro 33 vars)
```

**Nota:** v4 adiciona outro LEFT JOIN, desta vez com Silver Cadastro.

Ver [04_ABT_V4_EXPLAINED.md](04_ABT_V4_EXPLAINED.md).
