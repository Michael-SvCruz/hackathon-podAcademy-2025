# Silver Rules — `base_score_bureau_movel` (`bureau`)

## 1) Objetivo
Definir as regras de transformação para a camada **Silver** (dados padronizados, tipados e prontos para enriquecimento/modelagem), garantindo:
- consistência de tipos
- tratamento de missing/sentinelas
- preservação do grão `NUM_CPF + SAFRA`
- preparo para visão incremental (Bloco Scores)

---

## 2) Entradas e saídas

### 2.1) Entrada (Bronze)
- Tabela: `bureau` (raw/bronze)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_bureau_scores` (nome sugestão)
- Grão: **1 linha por `NUM_CPF + SAFRA`**

---

## 3) Tipagem (casting explícito)

### 3.1) Regras
- `NUM_CPF`: manter como **string**
- `SAFRA`: manter como **string** `YYYYMM`
- `DT_SAFRA`: criar como **date** derivado de `SAFRA`
- `SCORE_01`: converter para **int**
- `SCORE_02`: converter para **int**
- `FPD`: converter para **int** (0/1)

### 3.2) Exemplo de transformação (Spark SQL)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA, '01'), 'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(SCORE_01), '') AS INT) AS SCORE_01_INT,
  CAST(NULLIF(TRIM(SCORE_02), '') AS INT) AS SCORE_02_INT,

  CAST(NULLIF(TRIM(FPD), '') AS INT) AS FPD_INT,

  FLAG_INSTALACAO,
  PROD,
  flag_mig2
FROM bureau;
```

## 4) Tratamento de missing e sentinelas
### 4.1) SCORE_01
- Criar FLAG_SCORE01_MISSING quando:
	- SCORE_01 é NULL ou vazio, **ou**
	- SCORE_01_INT = 0 (sentinela/outlier suspeito)

### 4.2) SCORE_02
- Criar FLAG_SCORE02_MISSING quando:
	- SCORE_02 é NULL ou vazio

### 4.3) Exemplo (Spark SQL)
```sql
SELECT
  NUM_CPF,
  SAFRA,
  to_date(concat(SAFRA, '01'), 'yyyyMMdd') AS DT_SAFRA,

  CAST(NULLIF(TRIM(SCORE_01), '') AS INT) AS SCORE_01,
  CAST(NULLIF(TRIM(SCORE_02), '') AS INT) AS SCORE_02,
  CAST(NULLIF(TRIM(FPD), '') AS INT) AS FPD,

  CASE
    WHEN SCORE_01 IS NULL OR TRIM(SCORE_01) = '' OR CAST(NULLIF(TRIM(SCORE_01), '') AS INT) = 0 THEN 1
    ELSE 0
  END AS FLAG_SCORE01_MISSING,

  CASE
    WHEN SCORE_02 IS NULL OR TRIM(SCORE_02) = '' THEN 1
    ELSE 0
  END AS FLAG_SCORE02_MISSING,

  FLAG_INSTALACAO,
  PROD,
  flag_mig2
FROM bureau;
```

### 4.4) Decisão (por enquanto)
- **Não** transformar SCORE_01=0 em NULL diretamente (ainda).
- Estratégia recomendada: testar duas versões no treino e comparar efeito no **KS incremental**:
	- Versão A: mantém SCORE_01=0 + flag
	- Versão B: SCORE_01=0 → NULL + flag

---

## 5) Validações (Data Quality Gates) na saída Silver
### 5.1) Unicidade
- Deve permanecer **1:1 por NUM_CPF + SAFRA**.

### 5.2) Domínios
- FPD deve estar em {0,1}.
- SAFRA deve seguir YYYYMM.

### 5.3) Ranges recomendados
- SCORE_01 esperado em intervalo amplo (ex.: 0–1000)
- SCORE_02 esperado em intervalo amplo (ex.: 0–1000)

### 5.4) Controles de completude
- Monitorar percentuais de FLAG_SCORE01_MISSING e FLAG_SCORE02_MISSING por safra.

---

## 6) Observações de risco (leakage)
- FPD é label/target e não deve ser usado como feature sem definição temporal formal.
- FLAG_INSTALACAO, PROD, flag_mig2 estão **constantes no recorte atual** e devem ser tratados como **metadados de população**, não features.

---

## 7) Conexão com a visão incremental do projetoA Silver desta tabela viabiliza o Bloco “Scores”:
Modelo com SCORE_01 (e flags)
Modelo com SCORE_01 + SCORE_02 (e flags)
Reportar incremento de KS etapa a etapa, sempre comparando com a versão anterior