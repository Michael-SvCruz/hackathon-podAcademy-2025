# Data Quality Report — `base_score_bureau_movel` (`bureau`)

## 1) Objetivo
Registrar as evidências de qualidade de dados da tabela `bureau`, com foco em:
- **completude** (nulos/vazios)
- **faixa/distribuição** (percentis)
- **unicidade** (grão)
- **investigação de sentinelas** (ex.: `SCORE_01=0`)

---

## 2) Unicidade do grão (CPF + SAFRA)

### 2.1) Query
```sql
SELECT
  COUNT(*) AS total_linhas,
  COUNT(DISTINCT CONCAT(NUM_CPF, '#', SAFRA)) AS chaves_unicas_cpf_safra,
  COUNT(*) - COUNT(DISTINCT CONCAT(NUM_CPF, '#', SAFRA)) AS linhas_duplicadas
FROM bureau;
```

### 2.2)Resultado (evidência)
- **total_linhas:** 1.290.526
- **chaves_unicas_cpf_safra:** 1.290.526
- **linhas_duplicadas:** 0

### 2.3)Conclusão
- O grão é **1:1 por NUM_CPF + SAFRA** (sem duplicidade).

---

## 3) Completude (nulos e vazios)
### 3.1)Query
```sql
SELECT
  COUNT(*) AS total_linhas,

  SUM(CASE WHEN SCORE_01 IS NULL OR TRIM(SCORE_01) = '' THEN 1 ELSE 0 END) AS score01_null_ou_vazio,
  SUM(CASE WHEN CAST(SCORE_01 AS INT) = 0 THEN 1 ELSE 0 END) AS score01_igual_zero,

  SUM(CASE WHEN SCORE_02 IS NULL OR TRIM(SCORE_02) = '' THEN 1 ELSE 0 END) AS score02_null_ou_vazio
FROM bureau;
```

### 3.2)Resultado (evidência)
- **total_linhas:** 1.290.526
- **SCORE_01 nulo/vazio:** 9.439 (~0,73%)
- **SCORE_01 = 0:** 1.864 (~0,14%)
- **SCORE_02 nulo/vazio:** 576 (~0,045%)

### 3.3) Conclusão
- Scores possuem **alta completude**, principalmente SCORE_02.
- SCORE_01=0 é raro e deve ser tratado como **sentinela/outlier suspeito**.

---
## 4) Distribuição (percentis)
### 4.2) Resultado (evidência)
- SCORE_01 percentis: **[468, 503, 554, 587, 621, 674, 711]**
- SCORE_02 percentis: **[433, 481, 558, 622, 697, 790, 835]**

### 4.3) Conclusão
- Distribuição **estável**.
- Como p1 é muito maior que 0, SCORE_01=0 é um **outlier forte**.

---

## 5) Investigação de sentinela (SCORE_01 = 0) vs FPD
### 5.1) Query
```sql
SELECT
  FPD,
  COUNT(*) AS qtd
FROM bureau
WHERE CAST(SCORE_01 AS INT) = 0
GROUP BY FPD
ORDER BY FPD;
```

### 5.2) Resultado (evidência)
- **FPD=0:** 1.396
- **FPD=1:** 468

### 5.3) Interpretação
- A proporção de FPD=1 entre registros com SCORE_01=0 é **~25,1%**.
- Isso sugere que SCORE_01=0 não é um “marcador perfeito” de risco; pode ser **missing codificado** ou um valor raro legítimo.
- Recomendação: tratar com uma **flag de missing** e decidir a transformação 0 → NULL com base em evidência de impacto no **KS incremental**.

### 6) Recomendações de Data Quality (para automatizar)
- Checar que FPD pertence ao domínio {0,1}.
- Checar que SAFRA respeita padrão YYYYMM.
- Checar que NUM_CPF e SAFRA não são nulos.
- Monitorar taxas de missing de SCORE_01 e SCORE_02 ao longo das safras.