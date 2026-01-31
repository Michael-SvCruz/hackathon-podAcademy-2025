# 01 - ABT v1 Builder Explicado

## Informações do Script

| Item | Valor |
|------|-------|
| **Arquivo** | `src/jobs/02_gold/00_gold_abt_builder.py` |
| **Função** | Construir ABT v1 - baseline com Score_01 |
| **Input** | Silver Bureau (spine) |
| **Output** | Gold ABT v1 |
| **Registros** | 3,795,310 (1:1 com spine) |
| **Colunas** | ~15 |
| **Feature Blocks** | Score_01 |

---

## Contexto de Negócio

A ABT v1 é a **base fundacional** do pipeline de modelagem. Ela:

1. **Estabelece o spine:** Define o universo oficial de clientes-mês (NUM_CPF + SAFRA)
2. **Inclui o primeiro score:** SCORE_01 como feature baseline
3. **Preserva labels para auditoria:** FPD_INT (target) e FLAG_INSTALACAO_INT (decisão)
4. **Implementa anti-leakage:** Labels são incluídos mas NUNCA usados como features

**Por que começar com Score_01?**
- É o score de bureau mais básico
- Permite medir o incremento de cada bloco adicional (Score_02, Telco, etc.)
- Segue a ordem obrigatória definida pela coordenação

---

## Código Explicado Linha por Linha

### 1. Docstring e Documentação (Linhas 1-40)

```python
"""
--------------------------------------------------------------------------------
PROJETO HACKATHON 2025 - ENGENHARIA DE DADOS
SCRIPT: 00_gold_abt_builder.py
OBJETIVO: Construir Analytical Base Table (ABT) v1 para modelagem.
--------------------------------------------------------------------------------
"""
```

**Por que essa estrutura de docstring?**
- Cabeçalho padronizado identifica projeto, script e objetivo
- Documenta o roadmap incremental (v1 → v6) no próprio arquivo
- Lista definições críticas (evento âncora, target, anti-leakage)
- Serve como documentação viva para novos membros da equipe

**Alternativa descartada:**
```python
# Docstring mínima (NÃO RECOMENDADO para projetos de equipe)
"""Build ABT v1."""
```
Motivo: Em projetos de equipe, documentação detalhada economiza tempo de onboarding.

---

### 2. Imports (Linhas 42-48)

```python
import sys
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

from src.utils.spark_utils import get_spark_session
from src.utils.validate_abt import validate_abt_v1
```

**Explicação de cada import:**

| Import | Propósito | Uso no Script |
|--------|-----------|---------------|
| `sys` | Controle de execução | `sys.exit(1)` em caso de erro crítico |
| `argparse` | Parse de argumentos CLI | Permite passar caminhos via linha de comando |
| `F` (functions) | Funções PySpark | `F.lit()`, `F.col()`, `F.current_timestamp()` |
| `types` | Tipos PySpark | Documentação (não usado diretamente neste script) |
| `get_spark_session` | Utilitário interno | Cria SparkSession com configuração Delta Lake |
| `validate_abt_v1` | Validação específica | Quality gates obrigatórios para v1 |

**Por que `from pyspark.sql import functions as F`?**
- Convenção padrão da comunidade PySpark
- Evita conflito com `built-in functions` do Python
- Código mais legível: `F.col()` vs `pyspark.sql.functions.col()`

**Alternativa descartada:**
```python
from pyspark.sql.functions import col, lit, when, current_timestamp
```
Motivo: Requer listar cada função individualmente. Com `F.`, todas ficam disponíveis.

---

### 3. Configuração de Caminhos (Linhas 50-57)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO (DESENVOLVIMENTO / DATABRICKS COMMUNITY)
# =============================================================================
DEFAULT_SILVER_BUREAU_PATH = "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/gold/abt_v1_delta/"
DEFAULT_FORMAT = "delta"
GOLD_VERSION = "gold_abt_v1"
# =============================================================================
```

**Explicação de cada constante:**

| Constante | Valor | Propósito |
|-----------|-------|-----------|
| `DEFAULT_SILVER_BUREAU_PATH` | Caminho do spine Silver | Input: tabela Bronze→Silver do bureau |
| `DEFAULT_OUTPUT_PATH` | Caminho do Gold ABT v1 | Output: onde salvar a ABT |
| `DEFAULT_FORMAT` | `"delta"` | Formato de arquivo (Delta Lake) |
| `GOLD_VERSION` | `"gold_abt_v1"` | Metadado de rastreabilidade |

**Por que usar constantes no topo do arquivo?**
- Facilita manutenção (alterar caminho em um único lugar)
- Documentação implícita dos valores padrão
- Permite override via argumentos CLI

**Por que Delta Lake?**
- Suporta ACID transactions
- Time travel (versionamento automático)
- Schema evolution
- Integração nativa com Databricks

---

### 4. Função build_abt_v1 (Linhas 59-109)

```python
def build_abt_v1(df_bureau):
    """
    Constrói ABT v1: seleção de colunas do spin (bureau) para modelagem.

    Incluções:
    - Chaves: num_cpf, safra, dt_safra
    - Labels (auditoria/impacto): flag_instalacao_int, fpd_int
    - Features v1: score_01_adj, flag_score01_missing
    - Metadados: prod, flag_mig2, versão gold

    Exclusões (anti-leakage):
    - FPD_INT não pode ser feature
    - FLAG_INSTALACAO_INT não pode ser feature
    - SCORE_02 fica para v2
    """
```

**Por que uma função separada para build?**
- Separação de responsabilidades (leitura ≠ transformação ≠ escrita)
- Facilita testes unitários (pode testar a função isoladamente)
- Reutilização (outras versões podem chamar esta função)

---

#### 4.1 Log de Início

```python
    print(">>> [Transform] Selecionando colunas para ABT v1...")
```

**Padrão de logging adotado:**
- Prefixo `>>>` para fácil identificação nos logs
- Tag `[Transform]` indica a fase do pipeline
- Mensagem descritiva do que está acontecendo

**Por que print() e não logging?**
- Em Databricks notebooks, `print()` aparece diretamente no output
- `logging` requer configuração adicional
- Para produção, recomenda-se migrar para `logging` com níveis (INFO, WARNING, ERROR)

---

#### 4.2 Seleção de Colunas

```python
    # Seleção de colunas ordenadas logicamente
    df_abt = df_bureau.select(
        # CHAVES (obrigatórias para identificação)
        "num_cpf",
        "safra",
        "dt_safra",

        # LABELS (para auditoria e análise de impacto - NÃO usar como features)
        "flag_instalacao_int",     # Decisão observada (0/1)
        "fpd_int",                 # Target de risco (0/1, observado SÓ em flag_instalacao_int=1)

        # FEATURES v1 (SCORE_01)
        "score_01_adj",            # Score 1 ajustado (sentinela 0 → NULL)
        "flag_score01_missing",    # Flag de missing/sentinela para score_01

        # METADADOS DE ORIGEM
        "prod",
        "flag_mig2",

        # AUDITORIA (rastreabilidade)
        "metadata_data_ingestao",
        "metadata_nome_arquivo_origem",
        "metadata_sistema_origem",
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
```

**Explicação de cada grupo de colunas:**

| Grupo | Colunas | Propósito |
|-------|---------|-----------|
| **CHAVES** | num_cpf, safra, dt_safra | Identificação única do registro (grão 1:1) |
| **LABELS** | flag_instalacao_int, fpd_int | Target e decisão - NUNCA features |
| **FEATURES v1** | score_01_adj, flag_score01_missing | Primeiro bloco de features |
| **METADADOS ORIGEM** | prod, flag_mig2 | Contexto do produto/migração |
| **AUDITORIA** | metadata_* | Rastreabilidade do pipeline |

**Por que incluir LABELS se não são features?**
- `fpd_int`: É o TARGET que o modelo vai predizer (necessário para treinamento)
- `flag_instalacao_int`: Necessário para análise de swap-in/swap-out
- Incluir ≠ usar como feature. O modelo NUNCA vê essas colunas como X

**Por que `score_01_adj` e não `score_01`?**
- `_adj` indica que valores sentinela (0) foram convertidos para NULL
- O modelo recebe NULL, não 0 (que poderia ser interpretado como score válido)
- `flag_score01_missing` indica quando o score original era sentinela

**Por que select() explícito em vez de drop()?**
```python
# ABORDAGEM ESCOLHIDA: select() explícito
df_abt = df_bureau.select("col1", "col2", "col3")

# ALTERNATIVA DESCARTADA: drop() implícito
df_abt = df_bureau.drop("col_indesejada1", "col_indesejada2")
```
Motivos para preferir select():
1. **Documentação implícita:** Lista exatamente o que está na ABT
2. **Segurança:** Novas colunas no source não entram automaticamente
3. **Ordem controlada:** Colunas ficam na ordem definida

---

#### 4.3 Adição de Metadados Gold

```python
    # Adicionar metadados de gold
    df_abt = df_abt \
        .withColumn("gold_version", F.lit(GOLD_VERSION)) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_feature_blocks", F.lit("score_01"))

    return df_abt
```

**Explicação de cada metadado:**

| Coluna | Função | Valor Exemplo |
|--------|--------|---------------|
| `gold_version` | Versão da ABT | "gold_abt_v1" |
| `gold_build_date` | Timestamp da construção | 2026-01-29 10:45:00 |
| `gold_feature_blocks` | Blocos incluídos | "score_01" |

**Por que F.lit() para valores constantes?**
```python
# CORRETO: F.lit() cria uma coluna literal
df = df.withColumn("versao", F.lit("v1"))

# ERRADO: string direta não funciona
df = df.withColumn("versao", "v1")  # TypeError!
```

**Por que F.current_timestamp()?**
- Captura o momento exato da execução
- Útil para auditoria e debug
- Permite rastrear quando cada versão foi construída

**Por que encadear withColumn com `\`?**
```python
# ABORDAGEM ESCOLHIDA: backslash para continuação
df_abt = df_abt \
    .withColumn("col1", ...) \
    .withColumn("col2", ...)

# ALTERNATIVA: parênteses
df_abt = (df_abt
    .withColumn("col1", ...)
    .withColumn("col2", ...))
```
Ambas são válidas. O backslash é mais comum em código PySpark legado.

---

### 5. Função main() - Parse de Argumentos (Linhas 111-128)

```python
def main():
    parser = argparse.ArgumentParser(description="Build Gold ABT v1 - Score_01 baseline")
    parser.add_argument("--silver_path", help="Caminho da Silver Bureau (Delta)")
    parser.add_argument("--output_path", help="Caminho de destino do Gold ABT (Delta)")
    parser.add_argument("--format", default=DEFAULT_FORMAT, help="Formato (delta)")

    args_parsed, unknown_args = parser.parse_known_args()
```

**Por que `parse_known_args()` em vez de `parse_args()`?**

```python
# CORRETO para Databricks: parse_known_args()
args_parsed, unknown_args = parser.parse_known_args()

# PROBLEMÁTICO em Databricks: parse_args()
args = parser.parse_args()  # Falha com argumentos do kernel Jupyter!
```

**Motivo:** Databricks/Jupyter injeta argumentos extras (`-f`, `--ip`, etc.) que `parse_args()` não reconhece e gera erro. `parse_known_args()` ignora argumentos desconhecidos.

---

#### 5.1 Fallback para Modo Interativo

```python
    if args_parsed.silver_path:
        args = args_parsed
    else:
        print(">>> [Config] AVISO: Rodando em modo interativo/DEV. Usando caminhos padrão.")
        class Args:
            silver_path = DEFAULT_SILVER_BUREAU_PATH
            output_path = DEFAULT_OUTPUT_PATH
            format = DEFAULT_FORMAT
        args = Args()
```

**Explicação do padrão:**
- Se argumentos foram passados via CLI → usa os argumentos
- Se não foram passados → usa defaults (modo notebook/interativo)

**Por que criar uma classe Args interna?**
- Mantém a mesma interface (`args.silver_path`) independente da origem
- Evita condicionais em todo o código
- Padrão comum em scripts PySpark híbridos (CLI + notebook)

**Alternativa descartada:**
```python
# Usar defaults diretamente no argparse
parser.add_argument("--silver_path", default=DEFAULT_SILVER_BUREAU_PATH)
```
Motivo: Não permite diferenciar "usuário não passou" de "usuário passou valor igual ao default".

---

### 6. Leitura do Silver Bureau (Linhas 129-143)

```python
    spark = get_spark_session("Gold_ABT_Builder")

    # =========================================================================
    # 1) LEITURA SILVER BUREAU (SPINE)
    # =========================================================================
    print(f">>> [Leitura] Carregando Silver Bureau (Spine): {args.silver_path}")
    try:
        df_bureau = spark.read.format(args.format).load(args.silver_path)
    except Exception as e:
        print(f"!!! ERRO CRÍTICO NA LEITURA: {e}")
        sys.exit(1)

    count_in = df_bureau.count()
    print(f">>> [Info] Registros no Silver Bureau: {count_in}")
```

**Por que get_spark_session() em vez de SparkSession.builder?**
```python
# ABORDAGEM ESCOLHIDA: função utilitária
spark = get_spark_session("Gold_ABT_Builder")

# ALTERNATIVA: builder direto
spark = SparkSession.builder \
    .appName("Gold_ABT_Builder") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()
```
Motivos para função utilitária:
1. **Centralização:** Configurações Delta Lake em um único lugar
2. **Consistência:** Todos os scripts usam a mesma configuração
3. **Manutenção:** Alterar configuração em um arquivo afeta todos

**Por que try/except com sys.exit(1)?**
- Leitura do spine é crítica (sem spine, não há ABT)
- `sys.exit(1)` retorna código de erro para o sistema operacional
- Databricks interpreta exit code != 0 como falha do job

**Por que count() logo após leitura?**
- Valida que a leitura funcionou (action força execução)
- Armazena contagem para validação posterior
- Útil para logs e debugging

---

### 7. Build ABT v1 (Linhas 144-149)

```python
    # =========================================================================
    # 2) BUILD ABT v1
    # =========================================================================
    print(">>> [Transform] Construindo ABT v1 (Score_01)...")
    df_abt = build_abt_v1(df_bureau)
```

**Simplicidade intencional:**
- Toda a lógica está encapsulada em `build_abt_v1()`
- O main() apenas orquestra as etapas
- Facilita leitura e manutenção

---

### 8. Validações (Linhas 150-159)

```python
    # =========================================================================
    # 3) VALIDAÇÕES (obrigatórias conforme target_definition.md)
    # =========================================================================
    print(">>> [Validate] Executando gates de qualidade...")
    try:
        validate_abt_v1(df_abt, count_in)
    except AssertionError as e:
        print(f"!!! ERRO DE VALIDAÇÃO: {e}")
        sys.exit(1)

    count_out = df_abt.count()
    print(f">>> [Info] Registros no ABT v1: {count_out}")
```

**O que validate_abt_v1() verifica?**
1. Unicidade: 1:1 por NUM_CPF + SAFRA
2. Chaves não nulas
3. Labels em domínio válido {0, 1}
4. FPD observado apenas em FLAG_INSTALACAO=1
5. Score_01 coverage > 90%

**Por que passar count_in como parâmetro?**
- Permite validar que não houve perda de registros
- `count_in` (spine) deve ser igual a `count_out` (ABT v1)
- Se diferente, indica problema no pipeline

**Por que AssertionError em vez de exceção customizada?**
- `assert` é padrão Python para validações
- Mensagem de erro descritiva no próprio assert
- Para produção, considerar exceções customizadas com mais contexto

---

### 9. Escrita Delta Lake (Linhas 160-184)

```python
    # =========================================================================
    # 4) ESCRITA (DELTA LAKE)
    # =========================================================================
    print(f">>> [Escrita] Salvando Gold ABT v1 (Delta): {args.output_path}")

    df_abt.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(args.output_path)
```

**Explicação de cada opção:**

| Opção | Valor | Propósito |
|-------|-------|-----------|
| `format("delta")` | Delta Lake | Formato de arquivo com ACID |
| `mode("overwrite")` | Sobrescreve | Substitui dados existentes |
| `mergeSchema` | true | Permite adicionar novas colunas |
| `overwriteSchema` | true | Permite alterar tipos de colunas |

**Por que overwrite em vez de append?**
- ABT é reconstruída do zero a cada execução
- Evita duplicatas acidentais
- Simplifica debugging (estado limpo)

**Por que mergeSchema + overwriteSchema?**
- Durante desenvolvimento, schema pode mudar
- Evita erros de incompatibilidade
- Em produção, considerar remover para maior segurança

---

#### 9.1 Escrita Unity Catalog

```python
    # =========================================================================
    # ESCRITA TABLE PARA DATABRICKS (RETIRAR QUANDO PASSAR PARA OCI)
    # =========================================================================
    target_table = "hackathon_2025.default.gold_abt_v1"
    df_abt.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)
    print(f">>> [Sucesso] Tabela salva no Unity-Catalog: {target_table}")
```

**Por que duas escritas (Delta + Unity Catalog)?**
1. **Delta Lake (arquivo):** Portável, funciona em qualquer ambiente Spark
2. **Unity Catalog (tabela):** Permite consultas SQL diretas no Databricks

**Por que o comentário "RETIRAR QUANDO PASSAR PARA OCI"?**
- O projeto será migrado para Oracle Cloud Infrastructure
- OCI não tem Unity Catalog (é específico do Databricks)
- Lembrete para remover essa parte na migração

**Padrão de nomenclatura Unity Catalog:**
```
hackathon_2025.default.gold_abt_v1
     │            │         │
     │            │         └── Nome da tabela
     │            └── Schema (default)
     └── Catalog
```

---

### 10. Relatório Final (Linhas 185-219)

```python
    # =========================================================================
    # 5) RELATÓRIO FINAL
    # =========================================================================
    print("\n" + "="*80)
    print("RELATÓRIO FINAL - ABT v1 (Score_01)")
    print("="*80)

    # Distribuição de labels
    dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()
    dist_fpd = df_abt.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().collect()

    print("\n>>> [Stats] FLAG_INSTALACAO (decisão observada):")
    for row in dist_flag:
        pct = row["count"] * 100 / count_out
        print(f"    FLAG={row['flag_instalacao_int']}: {row['count']:>10} ({pct:>5.2f}%)")
```

**Por que collect() após groupBy?**
```python
# collect() traz dados para o driver
dist_flag = df_abt.groupBy("flag_instalacao_int").count().collect()

# Sem collect(), seria um DataFrame (não iterável diretamente)
dist_flag = df_abt.groupBy("flag_instalacao_int").count()  # DataFrame
```

**Atenção:** `collect()` deve ser usado apenas para resultados pequenos. Para DataFrames grandes, usar `show()` ou `take(n)`.

---

#### 10.1 Completude de Features

```python
    # Completude de features
    score01_null = df_abt.filter(F.col("score_01_adj").isNull()).count()
    print(f"\n>>> [Features] SCORE_01_ADJ completude: {(count_out - score01_null)*100/count_out:.2f}%")
```

**Por que calcular completude?**
- Verifica se o tratamento de sentinelas funcionou
- Score_01 deve ter ~98% de cobertura
- Valores muito baixos indicam problema no pipeline

---

#### 10.2 Sumário Final

```python
    print("\n" + "="*80)
    print(f"✓ ABT v1 PRONTA PARA MODELAGEM")
    print(f"  - Versão: {GOLD_VERSION}")
    print(f"  - Feature blocks: Score_01")
    print(f"  - Total registros: {count_out}")
    print(f"  - Grão: 1:1 NUM_CPF + SAFRA")
    print(f"  - Target: FPD_INT (observado em FLAG_INSTALACAO=1)")
    print("="*80 + "\n")
```

**Por que um sumário estruturado?**
- Confirmação visual de sucesso
- Documenta características da ABT produzida
- Útil para logs de produção e debugging

---

### 11. Entry Point (Linha 220-221)

```python
if __name__ == "__main__":
    main()
```

**Por que esse padrão?**
- Permite importar funções do módulo sem executar main()
- Padrão Python para scripts executáveis
- `__name__` é `"__main__"` apenas quando executado diretamente

---

## Diagrama de Fluxo

```
┌─────────────────────────────────────────────────────────────────┐
│                    00_gold_abt_builder.py                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐                                                │
│  │ ARGUMENTOS  │  --silver_path, --output_path, --format        │
│  └──────┬──────┘                                                │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │   LEITURA   │────▶│ Silver Bureau (spine)                │   │
│  │             │     │ 3,795,310 registros                  │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │   BUILD     │────▶│ build_abt_v1()                       │   │
│  │   ABT v1    │     │ - Select colunas (chaves, labels,    │   │
│  │             │     │   features, metadados)               │   │
│  │             │     │ - Adiciona gold_version, build_date  │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │  VALIDAÇÃO  │────▶│ validate_abt_v1()                    │   │
│  │             │     │ - Unicidade 1:1                      │   │
│  │             │     │ - Chaves não nulas                   │   │
│  │             │     │ - Labels em {0, 1}                   │   │
│  │             │     │ - Anti-leakage FPD                   │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐     ┌──────────────────────────────────────┐   │
│  │   ESCRITA   │────▶│ Delta Lake + Unity Catalog           │   │
│  │             │     │ gold_abt_v1_delta/                   │   │
│  │             │     │ hackathon_2025.default.gold_abt_v1   │   │
│  └──────┬──────┘     └──────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  RELATÓRIO  │  Stats, distribuições, completude              │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Colunas de Saída (ABT v1)

| # | Coluna | Tipo | Origem | Papel |
|---|--------|------|--------|-------|
| 1 | num_cpf | string | Bureau | Chave (cliente) |
| 2 | safra | string | Bureau | Chave (mês YYYYMM) |
| 3 | dt_safra | date | Bureau | Safra como data |
| 4 | flag_instalacao_int | int | Bureau | **LABEL** (decisão) |
| 5 | fpd_int | int | Bureau | **TARGET** (risco) |
| 6 | score_01_adj | double | Bureau | **FEATURE** (score ajustado) |
| 7 | flag_score01_missing | int | Bureau | Flag de missing |
| 8 | prod | string | Bureau | Produto |
| 9 | flag_mig2 | int | Bureau | Flag de migração |
| 10 | metadata_data_ingestao | timestamp | Bureau | Metadado |
| 11 | metadata_nome_arquivo_origem | string | Bureau | Metadado |
| 12 | metadata_sistema_origem | string | Bureau | Metadado |
| 13 | metadata_data_transformacao | timestamp | Bureau | Metadado |
| 14 | metadata_versao_regra | string | Bureau | Metadado |
| 15 | gold_version | string | Script | Versão Gold |
| 16 | gold_build_date | timestamp | Script | Data de build |
| 17 | gold_feature_blocks | string | Script | Blocos incluídos |

---

## Lições Aprendidas

### 1. Select Explícito vs Drop

**Escolha:** Select explícito com todas as colunas listadas.
**Motivo:** Documentação implícita e segurança contra novas colunas indesejadas.

### 2. parse_known_args() para Databricks

**Escolha:** `parse_known_args()` em vez de `parse_args()`.
**Motivo:** Compatibilidade com argumentos injetados pelo kernel Jupyter/Databricks.

### 3. Duas Escritas (Delta + Unity Catalog)

**Escolha:** Escrever em Delta Lake (arquivo) E Unity Catalog (tabela).
**Motivo:** Portabilidade (Delta) + Conveniência (SQL queries no Databricks).

### 4. Validação como Gate Obrigatório

**Escolha:** `sys.exit(1)` se validação falhar.
**Motivo:** Não propagar dados inválidos para etapas posteriores.

---

## Checklist de Revisão

- [x] Docstring completa com roadmap e definições
- [x] `parse_known_args()` para compatibilidade Databricks
- [x] Select explícito de todas as colunas
- [x] Labels incluídos mas documentados como NÃO-features
- [x] Metadados de versão Gold adicionados
- [x] Validação obrigatória com sys.exit() em falha
- [x] Escrita Delta Lake + Unity Catalog
- [x] Relatório final com distribuições
- [x] Logs com prefixo `>>>`

---

## Próximo Passo

A ABT v1 serve como input para a ABT v2, que adiciona Score_02:

```
ABT v1 (Score_01) → 01_gold_abt_v2_builder.py → ABT v2 (Score_01 + Score_02)
```

Ver [02_ABT_V2_EXPLAINED.md](02_ABT_V2_EXPLAINED.md).
