# Patch de documentação — Nota sobre `bureau_full` (spine v2)

A partir da disponibilização da tabela **`base_score_bureau_movel_full`** (`bureau_full`), o projeto passa a ter um spine com:
- `FLAG_INSTALACAO` variando em {0,1} (inclui reprovados)
- `FPD` observado apenas quando `FLAG_INSTALACAO=1`

**Implicações:**
- A avaliação de **swap-in/swap-out** e impacto de aprovação/reprovação deve ser feita usando `bureau_full`.
- Os documentos anteriores:
  - `bureau` representam o recorte legado (majoritariamente `FLAG_INSTALACAO=1`).
- Para consistência do projeto, recomenda-se tratar `bureau_full` como **fonte de verdade** para:
  - universo (spine),
  - `FLAG_INSTALACAO` (decisão/política),
  - `FPD` (label de risco, quando observado).

**Reforço (anti-leakage):**
- `FPD` e `FLAG_INSTALACAO` não devem ser usados como features.

---

# Silver Rules — `base_telco` (`telco`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** da Telco, garantindo:
- tipagem explícita (evitar cast implícito)
- preservação do grão `NUM_CPF + SAFRA` (1:1 confirmado)
- padronização de missing/sentinelas (especialmente `304`)
- preparação para enriquecimento incremental no spine (`bureau`)

---

## 2) Entradas e saídas

### 2.1) Entrada (Bronze)
- Fonte: `/Volumes/hackathon_2025/default/source/base_telco/`
- Tabela lógica: `telco` (raw/bronze)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_telco_features` (nome sugestão)
- Grão: **1 linha por `NUM_CPF + SAFRA`**

---

## 3) Tipagem (casting explícito)

### 3.1) Regras gerais
- `NUM_CPF`: manter como **string**
- `SAFRA`: manter como **string** `YYYYMM`
- `DT_SAFRA`: criar como **date** derivada (ex.: primeiro dia do mês)
- `FLAG_INSTALACAO`: cast para **int** (0/1), se aplicável
- `FPD`: cast para **int** (0/1) (quando não nulo)
- `PROD`: manter **string**
- `flag_mig2`: manter **string**
- `var_*`: cast explícito para **double** (ou int quando fizer sentido, mas como há decimais, double é seguro)

### 3.2) Exemplo base (Spark SQL)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA, '01'), 'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(FLAG_INSTALACAO), '') AS INT) AS FLAG_INSTALACAO_INT,
  CAST(NULLIF(TRIM(FPD), '') AS INT) AS FPD_INT,

  PROD,
  flag_mig2,

  CAST(NULLIF(TRIM(var_26), '') AS DOUBLE) AS var_26,
  CAST(NULLIF(TRIM(var_27), '') AS DOUBLE) AS var_27,
  CAST(NULLIF(TRIM(var_28), '') AS DOUBLE) AS var_28,
  CAST(NULLIF(TRIM(var_29), '') AS DOUBLE) AS var_29,
  CAST(NULLIF(TRIM(var_30), '') AS DOUBLE) AS var_30
  -- repetir padrão para var_31 ... var_93
FROM telco
```

## 4) Tratamento de missing e sentinelas
### 4.1) Padrão para var_* com sentinela 304
Observou-se que muitas colunas var_* possuem incidência alta de 304 (provável “não informado/não aplicável”).
**Regra recomendada (base)**

Para cada var_xx:
- criar FLAG_var_xx_MISSING quando:
	- ar_xx é nulo/vazio, ou
	- var_xx = 304

Exemplo (para um subset)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA, '01'), 'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(var_29), '') AS DOUBLE) AS var_29,
  CASE
    WHEN var_29 IS NULL OR TRIM(var_29) = '' OR CAST(NULLIF(TRIM(var_29), '') AS DOUBLE) = 304 THEN 1
    ELSE 0
  END AS FLAG_var_29_MISSING

FROM telco;
```

## 5) Validações (Data Quality Gates) na Silver
## 5.1) Unicidade
Deve permanecer **1:1 por NUM_CPF + SAFRA:**
- COUNT(*) = COUNT(DISTINCT NUM_CPF, SAFRA)

### 5.2) Domínios e formatos
- SAFRA deve seguir YYYYMM
- FPD_INT deve estar em {0,1} quando não nulo
- FLAG_INSTALACAO_INT deve estar em {0,1} quando não nulo

### 5.3) Monitoramento de completude
- medir % FPD nulo por safra
- medir % 304 por var_* por safra (drift de missing)

## 6) Observações de risco (leakage)
- FPD deve ser tratado como **label candidato** e não como feature.
- FLAG_INSTALACAO pode representar “contratação/instalação” e, dependendo do evento âncora, pode se tornar **leakage**.
Recomendação: manter na Silver para auditoria, mas **evitar usar como feature** até definição formal.

## 7) Capítulo extra — Sugestão (boa prática): gerar flags de missing automaticamente para todas as var_*
### 7.1) Por que isso faz sentido aqui
- As var_* são anonimizadas (sem conceito), então o ganho costuma vir de:
	- **sinal numérico** (quando existe)
	- **padrões de ausência** (missing), que em risco/crédito frequentemente carregam informação relevante (ex.: cliente sem histórico, sem uso, sem registro, etc.)
- Como 304 aparece com alta frequência em várias colunas, tratar isso como missing e gerar flags permite:
	- capturar informação de “presença vs ausência”
	- melhorar performance sem precisar interpretar semanticamente

### 7.2) Como aplicar
- Para cada coluna var_xx, criar:
	- FLAG_var_xx_MISSING = 1 se var_xx é NULL/vazio/304
- Opcional: criar também um agregador:
	- CNT_VARS_MISSING = soma das flags
	- PCT_VARS_MISSING = CNT_VARS_MISSING / N

### 7.3) Observação de custo
- Criar flags para todas as 68 colunas aumenta a dimensionalidade, mas:
	- modelos baseados em árvore (ex.: GBMs) lidam bem
	- e isso pode ser controlado por seleção posterior (feature importance / KS incremental)

## 8) Conexão com a visão incremental do projeto
- A Silver Telco alimenta o Bloco “Telco” no incremental:
	- gold_abt_v2_scores_telco = spine (bureau_silver) + features Telco (var_* + features derivadas/flags)
	- medir **ΔKS** vs versão anterior (somente Scores)