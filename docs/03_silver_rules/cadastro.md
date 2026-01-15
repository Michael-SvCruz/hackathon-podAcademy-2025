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

# Silver Rules — `base_dados_cadastrais` (`cadastro`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** do Cadastro, garantindo:
- tipagem explícita (evitar cast implícito)
- preservação do grão `NUM_CPF + SAFRA` (1:1 confirmado)
- parsing robusto de datas (com tolerância a inválidos)
- organização de `var_*` em numéricas vs categóricas
- preparo para enriquecimento incremental no spine (`bureau`)

---

## 2) Entradas e saídas

### 2.1) Entrada (Landing/Bronze)
- Fonte: `/Volumes/hackathon_2025/default/source/base_dados_cadastrais/`
- Tabela lógica: `cadastro` (raw/bronze)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_cadastro_features` (nome sugestão)
- Grão: **1 linha por `NUM_CPF + SAFRA`**

---

## 3) Tipagem e normalização (regras)

### 3.1) Regras gerais
- `NUM_CPF`: **string**
- `SAFRA`: **string** `YYYYMM`
- `DT_SAFRA`: **date** derivada (`yyyyMMdd` com dia 01)
- `FPD`: **int** (0/1) quando não nulo (label candidato; manter para auditoria)
- `FLAG_INSTALACAO`: **int** (0/1)
- `PROD`, `flag_mig2`, `STATUSRF`: **string** (normalizar `TRIM` e, se quiser, `UPPER`)
- `DATADENASCIMENTO`: parse para `date` com tolerância e derivar idade
- `CEP_3_digitos`: manter como **string** (categórico) e criar flag de missing

### 3.2) Datas (parsing tolerante)
Devido a entradas inválidas, usar parsing tolerante:
- `DT_NASC = try_to_date(DATADENASCIMENTO,'dd/MM/yyyy')`
- para `var_12` (se for data): `DT_VAR_12 = try_to_date(var_12,'dd/MM/yyyy')`

---

## 4) Exemplo de transformação base (Spark SQL)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA,'01'),'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(FLAG_INSTALACAO), '') AS INT) AS FLAG_INSTALACAO_INT,
  CAST(NULLIF(TRIM(FPD), '') AS INT) AS FPD_INT,

  TRIM(PROD) AS PROD,
  TRIM(flag_mig2) AS flag_mig2,
  TRIM(STATUSRF) AS STATUSRF,

  try_to_date(DATADENASCIMENTO,'dd/MM/yyyy') AS DT_NASC,

  CASE
    WHEN try_to_date(DATADENASCIMENTO,'dd/MM/yyyy') IS NULL THEN NULL
    ELSE floor(months_between(to_date(concat(SAFRA,'01'),'yyyyMMdd'), try_to_date(DATADENASCIMENTO,'dd/MM/yyyy'))/12)
  END AS IDADE_ANOS,

  TRIM(CEP_3_digitos) AS CEP_3_digitos,
  CASE WHEN CEP_3_digitos IS NULL OR TRIM(CEP_3_digitos)='' THEN 1 ELSE 0 END AS FLAG_CEP3_MISSING,

  -- Numéricas (confirmadas como numéricas pelo check de não numéricos = 0)
  CAST(NULLIF(TRIM(var_03), '') AS DOUBLE) AS var_03,
  CAST(NULLIF(TRIM(var_04), '') AS DOUBLE) AS var_04,
  CAST(NULLIF(TRIM(var_05), '') AS DOUBLE) AS var_05,
  CAST(NULLIF(TRIM(var_06), '') AS DOUBLE) AS var_06,
  CAST(NULLIF(TRIM(var_07), '') AS DOUBLE) AS var_07,
  CAST(NULLIF(TRIM(var_08), '') AS DOUBLE) AS var_08,
  CAST(NULLIF(TRIM(var_09), '') AS DOUBLE) AS var_09,
  CAST(NULLIF(TRIM(var_10), '') AS DOUBLE) AS var_10,
  CAST(NULLIF(TRIM(var_11), '') AS DOUBLE) AS var_11,
  CAST(NULLIF(TRIM(var_16), '') AS DOUBLE) AS var_16,
  CAST(NULLIF(TRIM(var_17), '') AS DOUBLE) AS var_17,

  -- Possível data (mista) — manter string + parse tolerante
  TRIM(var_12) AS var_12_raw,
  try_to_date(var_12,'dd/MM/yyyy') AS DT_var_12,

  -- Mistas/categóricas (alto volume de não numéricos)
  TRIM(var_15) AS var_15,
  TRIM(var_22) AS var_22,
  TRIM(var_23) AS var_23,
  TRIM(var_24) AS var_24,
  TRIM(var_25) AS var_25

FROM cadastro;
```

---

## 5) Regras adicionais recomendadas (flags e sanity)
### 5.1) Flag de data inválida (para auditoria)
- FLAG_DT_NASC_INVALID = 1 quando DATADENASCIMENTO está preenchido mas DT_NASC é NULL
- FLAG_DT_VAR12_INVALID = 1 quando var_12 está preenchida mas DT_var_12 é NULL

## 5.2) Idade fora do esperado (sanity)
Como foram observadas idades entre 4 e 131:
- criar FLAG_IDADE_OUTLIER = 1 quando IDADE_ANOS < 14 ou IDADE_ANOS > 100 (limites ajustáveis)
Observação: não remover registros na Silver; apenas sinalizar. A decisão final (cap/filtrar) fica para a Gold/Modeling.

### 5.3) Negativos em var_11Como var_11 possui valores negativos:
- criar FLAG_var_11_NEG = 1 quando var_11 < 0
- opcional: var_11_clean = CASE WHEN var_11 < 0 THEN NULL ELSE var_11 END

---

## 6) Validações (Data Quality Gates) na saída Silver
### 6.1) Unicidade
- Deve permanecer 1:1 por NUM_CPF + SAFRA.

### 6.2) Domínios
- FPD_INT deve estar em {0,1} quando não nulo.
- FLAG_INSTALACAO_INT deve estar em {0,1} quando não nulo.
- SAFRA deve seguir YYYYMM.

### 6.3) Monitoramento contínuo
- % FPD nulo por safra (decidir padrão de treino).
- taxa de inválidos em datas (DT_NASC, DT_var_12).
- outliers em IDADE_ANOS, var_07, var_10, var_11.

---

## 7) Conexão com a visão incremental do projeto
A Silver do Cadastro alimenta o bloco Cadastral do incremental:
- gold_abt_v3_scores_telco_cadastro = spine (bureau_silver) + Telco Silver + Cadastro Silver
- medir ΔKS vs versão anterior (Scores + Telco)

### Ajuste sugerido para a query que deu erro (opcional, para você rodar agora)
Se quiser medir `invalid_dn/invalid_var12` sem quebrar, use `try_to_date`:
```sql
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN DATADENASCIMENTO IS NULL OR TRIM(DATADENASCIMENTO)='' THEN 1 ELSE 0 END) AS null_dn,
  SUM(CASE WHEN DATADENASCIMENTO IS NOT NULL AND TRIM(DATADENASCIMENTO)<>'' AND try_to_date(DATADENASCIMENTO,'dd/MM/yyyy') IS NULL THEN 1 ELSE 0 END) AS invalid_dn,
  SUM(CASE WHEN var_12 IS NULL OR TRIM(var_12)='' THEN 1 ELSE 0 END) AS null_var12,
  SUM(CASE WHEN var_12 IS NOT NULL AND TRIM(var_12)<>'' AND try_to_date(var_12,'dd/MM/yyyy') IS NULL THEN 1 ELSE 0 END) AS invalid_var12
FROM cadastro;
```