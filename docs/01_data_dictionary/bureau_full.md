# Data Dictionary — `base_score_bureau_movel_full` (`bureau_full`)

## 1) Contexto
Esta documentação descreve a tabela **`base_score_bureau_movel_full`** (referenciada como **`bureau_full`**).

Ela segue o mesmo formato da base `bureau` (v1), mas com uma diferença funcional crítica:
- inclui registros com `FLAG_INSTALACAO = 0` (reprovados/não contratados)
- mantém `FLAG_INSTALACAO = 1` (aprovados/contratados)

Isso viabiliza:
- **análise de impacto** (aprovação/reprovação),
- matriz de confusão de decisão,
- e análise de **swap-in / swap-out**.

---

## 2) Identificação da tabela
- **Nome lógico:** `base_score_bureau_movel_full`
- **Nome no ambiente (queries):** `bureau_full`
- **Formato de coorte:** `SAFRA` no padrão **YYYYMM**
- **Chave canônica para joins:** **`NUM_CPF + SAFRA`**
- **Grão (confirmado):** **1 linha por CPF por SAFRA**

---

## 3) Grão e chaves (confirmado)

### 3.1) Evidência
- `COUNT(*)` = `COUNT(DISTINCT (NUM_CPF, SAFRA))`
- Não existem duplicidades no par `NUM_CPF + SAFRA`

### 3.2) Conclusão
- **Grão:** `NUM_CPF + SAFRA` (**1:1**)
- **Uso:** esta base é o **spine oficial** para ABTs incrementais e análises de impacto.

---

## 4) Dicionário de colunas (v1)

### 4.1) Colunas de identificação/tempo

#### `NUM_CPF` (string)
- **Descrição:** identificador do CPF do cliente (provavelmente hash/ofuscado).
- **Papel:** **chave** (join key).

#### `SAFRA` (string)
- **Descrição:** coorte mensal no formato **YYYYMM**.
- **Papel:** **tempo / coorte / data_ref**.
- **Recomendação:** criar `DT_SAFRA` (date) derivada.

---

### 4.2) Colunas de decisão/política (para impacto e swaps)

#### `FLAG_INSTALACAO` (string → int)
- **Descrição:** indicador de resultado do processo de decisão/contratação.
- **Valores observados:** `0`, `1`
- **Interpretação operacional:**
  - `1` = aprovado/contratado (instalado)
  - `0` = reprovado (não contratado)
- **Papel recomendado:** **label de decisão/política** para análise de impacto e swaps.
- **Regra crítica:** **não usar como feature** do modelo de risco.

---

### 4.3) Coluna de resultado (label de risco)

#### `FPD` (string → int)
- **Descrição provável:** indicador de inadimplência/atraso após contratação (label de risco).
- **Valores observados:** `0`, `1`, `null`
- **Regra observada nos dados:**
  - quando `FLAG_INSTALACAO = 0`: `FPD` é **sempre nulo** (não observado)
  - quando `FLAG_INSTALACAO = 1`: `FPD` é **sempre observado** (0/1)
- **Papel recomendado:** **label/target de risco** (treinar em `FLAG_INSTALACAO=1`).
- **Regra crítica:** não usar como feature.

---

### 4.4) Colunas de score (features)

#### `SCORE_01` (string → int)
- **Descrição:** score numérico (Score 1).
- **Papel:** feature do bloco “Scores”.
- **Evidências coletadas:**
  - range observado: **0 a 778**
  - média observada: **~578,99**
  - percentis (p1/p5/p25/p50/p75/p95/p99): **[459, 495, 544, 582, 619, 668, 702]**
  - nulo/vazio: **54.035**
  - `SCORE_01=0`: **15.226** (outlier/sentinela provável)
- **Recomendação:** criar flag de missing incluindo `0`.

#### `SCORE_02` (string → int)
- **Descrição:** score numérico (Score 2).
- **Papel:** feature do bloco “Scores”.
- **Evidências coletadas:**
  - range observado: **1 a 926**
  - média observada: **~628,51**
  - percentis (p1/p5/p25/p50/p75/p95/p99): **[407, 464, 550, 627, 713, 800, 841]**
  - nulo/vazio: **1.876**
- **Recomendação:** casting explícito e flag de missing.

---

### 4.5) Colunas de recorte/população

#### `PROD` (string)
- **Valores observados:** `CMV`
- **Papel:** metadado de público (constante no recorte atual).

#### `flag_mig2` (string)
- **Valores observados:** `PRE`, `FLEX`, `Aquisição`, `null`
- **Papel:** feature categórica potencial (segmentação/jornada).

---

## 5) Notas para uso no projeto (separação risco vs impacto)
- **Treino do modelo de risco:** usar universo `FLAG_INSTALACAO=1` (pois `FPD` está observado).
- **Análise de impacto (swap-in/out):** usar universo completo, comparando:
  - baseline: `FLAG_INSTALACAO`
  - proposta: decisão do modelo via cutoff

---

## 6) Pendências para consolidar v2
- Definir formalmente o cutoff/política para simular aprovação do modelo (para swaps).
- Confirmar regras de grupo controle (6º/7º dígitos) sobre `NUM_CPF` ofuscado (indexação/extração).