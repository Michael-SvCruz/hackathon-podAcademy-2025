# Data Quality Report — `bases_recarga` (`recarga`)

## 1) Objetivo
Registrar evidências de qualidade da base de recarga, com foco em:
- volumetria
- parsing de data
- duplicidades
- sentinelas em códigos (`-1/-2/-3`)
- valores negativos e SOS

---

## 2) Volumetria e período

### 2.1) Evidência (resultado)
- **total_linhas:** 100.213.651
- `DAT_INSERCAO_CREDITO` (string):
  - **min (string):** `01APR2024:00:00:00`
  - **max (string):** `31OCT2024:00:00:00`

### 2.2) Nota importante (qualidade do min/max)
Como `DAT_INSERCAO_CREDITO` é **string com mês em texto**, `MIN/MAX` por string pode não refletir o min/max cronológico real se existirem meses/anos fora do intervalo ou entradas em outros anos.

**Recomendação:** calcular min/max usando timestamp parseado:
- `TS_RECARGA = to_timestamp(DAT_INSERCAO_CREDITO,'ddMMMyyyy:HH:mm:ss')`
- e então `MIN(TS_RECARGA)` / `MAX(TS_RECARGA)`.

---

## 3) Parsing de data (sem inválidos)

### 3.1) Evidência (resultado)
- **null_dat:** 0
- **invalid_dat:** 0

### 3.2) Conclusão
O parsing no formato `ddMMMyyyy:HH:mm:ss` está consistente (na checagem executada).

---

## 4) Duplicidades (potenciais)

### 4.1) Evidência (resultado)
Foi avaliada uma chave composta candidata:
- `NUM_CPF + DAT_INSERCAO_CREDITO + HOR_INSERCAO_CREDITO + VAL_REAL + VAL_CREDITO_INSERIDO + VAL_BONUS`

Resultados:
- **chaves_distintas:** 99.892.881
- **possiveis_duplicadas:** 320.770

### 4.2) Exemplos (top duplicadas)
Foram observados casos com repetição da mesma chave composta (qtd 8–14), incluindo valores como `390.00`, `10000.00`, `1.00`, `-1.00`, `0.00`.

### 4.3) Conclusão
Há indícios de duplicidade no nível de evento, que podem ser:
- duplicação de fonte/log
- reprocessamento
- ou eventos tecnicamente diferentes mas com mesmos campos (menos provável)

**Recomendação:** definir dedupe na Silver com chave mais robusta (ver `silver_rules`).

---

## 5) Sentinelas em códigos e valores negativos

### 5.1) Evidência (resultado)
Contagem de sentinelas `-1/-2/-3`:
- `COD_CANAL_AQUISICAO`: 71.136.603
- `DW_TIPO_RECARGA`: 93.509.722
- `DW_FORMA_PAGAMENTO`: 98.061.639
- `DW_INSTITUICAO`: 71.230.364

Valores negativos:
- `VAL_BONUS < 0`: 13.466.007
- `VAL_REAL < 0`: 12.392.854

### 5.2) Conclusão
- Há uso massivo de sentinelas em colunas dimensionais.
- Valores negativos em montantes sugerem:
  - sentinela/ajuste
  - erro de registro
  - ou operação especial (precisa regra de limpeza e flags).

---

## 6) SOS

### 6.1) Evidência (resultado)
- `FLAG_SOS = 0`: 93.679.237 (VALOR_SOS nulo)
- `FLAG_SOS = 1`: 6.534.414
  - `min_valor_sos`: 3
  - `max_valor_sos`: 20

### 6.2) Conclusão
SOS aparece com volume relevante (~6,5M eventos) e deve ser modelado como:
- presença de SOS no mês (flag)
- soma/avg do valor de SOS por mês

---

## 7) Recomendações (gates para automatizar)
- Validar parsing de `DAT_INSERCAO_CREDITO` para timestamp.
- Monitorar taxa de duplicidade por chave de evento (antes e depois de dedupe).
- Monitorar taxas de sentinela (`-1/-2/-3`) por coluna (drift).
- Monitorar proporção de valores negativos em `VAL_REAL` e `VAL_BONUS`.
- Monitorar volume de SOS por safra derivada.