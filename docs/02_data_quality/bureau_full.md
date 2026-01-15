# Data Quality Report — `base_score_bureau_movel_full` (`bureau_full`)

## 1) Objetivo
Registrar evidências de qualidade da base `bureau_full`, com foco em:
- unicidade do grão `NUM_CPF + SAFRA`
- completude de scores
- comportamento de `FPD` condicionado a `FLAG_INSTALACAO`
- distribuição por safra (para splits temporais e OOT)

---

## 2) Unicidade do grão (CPF + SAFRA)

### 2.1) Evidência (resultado)
- **total_linhas:** 3.795.310  
- **chaves_unicas_cpf_safra:** 3.795.310  
- **linhas_duplicadas:** 0

### 2.2) Conclusão
A base está em **grão 1:1 por `NUM_CPF + SAFRA`** (spine consistente).

---

## 3) Distribuição por safra (volumetria)
Evidência (resultado):
- 202410: 636.951
- 202411: 647.199
- 202412: 626.744
- 202501: 648.554
- 202502: 602.344
- 202503: 633.518

---

## 4) Completude de `FPD` (e interpretação correta)

### 4.1) Evidência (global)
- `FPD null`: 1.161.410
- `FPD = 0`: 2.074.671
- `FPD = 1`: 559.229

### 4.2) Evidência (condicionada a instalação)
- `FLAG_INSTALACAO=0`: `FPD null` = 1.161.410 | `FPD not null` = 0 | total = 1.161.410
- `FLAG_INSTALACAO=1`: `FPD null` = 0 | `FPD not null` = 2.633.900 | total = 2.633.900

### 4.3) Conclusão
- `FPD` é um label observado **somente para instalados**.
- Para treinar o modelo de risco, o universo correto é `FLAG_INSTALACAO=1`.

---

## 5) Taxa de `FPD` por safra (somente observados)
Evidência (resultado, `FPD is not null`):
|safra|pct_FPD|
|:-----|:-----|     
|202410| 0,2041 (n=426.104)|
|202411| 0,2163 (n=454.572)|
|202412| 0,2153 (n=445.154)|
|202501| 0,2128 (n=452.621)|
|202502| 0,2102 (n=419.453)|
|202503| 0,2147 (n=435.996)|

---

## 6) Completude e sentinelas dos scores

### 6.1) `SCORE_01`
- nulo/vazio: 54.035
- igual a 0: 15.226 (outlier/sentinela provável)

### 6.2) `SCORE_02`
- nulo/vazio: 1.876

### 6.3) Percentis (distribuição)
- `SCORE_01`: [459, 495, 544, 582, 619, 668, 702]
- `SCORE_02`: [407, 464, 550, 627, 713, 800, 841]

### 6.4) Conclusão
- Scores possuem boa completude, com atenção especial a:
  - `SCORE_01=0` (provável sentinela/missing codificado)
  - missing em `SCORE_01` (mais alto que `SCORE_02`)

---

## 7) Recomendações (gates automatizáveis)
- Garantir grão 1:1 por `NUM_CPF + SAFRA`.
- Criar flags de missing:
  - `FLAG_SCORE01_MISSING` (incluindo `SCORE_01=0`)
  - `FLAG_SCORE02_MISSING`
- Definir claramente dois universos:
  - treino risco: `FLAG_INSTALACAO=1`
  - impacto/swaps: universo completo