%md
# Silver Rules — `base_score_bureau_movel_full` (`bureau_full`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** do `bureau_full`, garantindo:
- tipagem explícita
- flags de missing e sentinelas
- preservação do grão `NUM_CPF + SAFRA`
- separação clara entre colunas de risco (FPD) e decisão (FLAG_INSTALACAO)

---

## 2) Entradas e saídas

### 2.1) Entrada (Landing/Bronze)
- Fonte: `/Volumes/hackathon_2025/default/source/base_score_bureau_movel_full/`
- Tabela lógica: `bureau_full` (raw/bronze)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_bureau_full_scores`
- Grão: **1 linha por `NUM_CPF + SAFRA`**

---

## 3) Tipagem (casting explícito)

### 3.1) Regras
- `NUM_CPF`: string
- `SAFRA`: string `YYYYMM`
- `DT_SAFRA`: date derivada
- `FLAG_INSTALACAO`: int
- `FPD`: int (0/1) quando não nulo
- `SCORE_01`: int
- `SCORE_02`: int
- `PROD`: string
- `flag_mig2`: string

### 3.2) Exemplo (Spark SQL)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA,'01'),'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(FLAG_INSTALACAO), '') AS INT) AS FLAG_INSTALACAO_INT,
  CAST(NULLIF(TRIM(FPD), '') AS INT) AS FPD_INT,

  CAST(NULLIF(TRIM(SCORE_01), '') AS INT) AS SCORE_01,
  CAST(NULLIF(TRIM(SCORE_02), '') AS INT) AS SCORE_02,

  CASE
    WHEN SCORE_01 IS NULL OR TRIM(SCORE_01) = '' OR CAST(NULLIF(TRIM(SCORE_01), '') AS INT) = 0 THEN 1
    ELSE 0
  END AS FLAG_SCORE01_MISSING,

  CASE
    WHEN SCORE_02 IS NULL OR TRIM(SCORE_02) = '' THEN 1
    ELSE 0
  END AS FLAG_SCORE02_MISSING,

  PROD,
  flag_mig2
FROM bureau_full;
```
---

## 4) Tratamento de SCORE_01=0 (sentinela/outlier)
Observação de qualidade:
- SCORE_01=0 ocorre e é raro o suficiente para suspeita de sentinela.
Recomendação:
- manter SCORE_01 como int
- criar FLAG_SCORE01_MISSING incluindo o caso SCORE_01=0
- avaliar via KS incremental:
	- versão A: manter 0
	- versão B: 0 → NULL

---

## 5) Regras de uso (para evitar leakage)
- FPD_INT é label de risco e não deve ser feature.
- FLAG_INSTALACAO_INT é label de decisão/política (para impacto/swaps) e não deve ser feature.

---

## 6) Gates de qualidade
- Unicidade: COUNT(*) = COUNT(DISTINCT NUM_CPF, SAFRA)
- Domínios:
	- FLAG_INSTALACAO_INT em {0,1}
	- FPD_INT em {0,1} quando não nulo
- Monitoramento:
	- % missing SCORE_01, % SCORE_01=0
	- % missing SCORE_02