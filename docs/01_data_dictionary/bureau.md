
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
# Data Dictionary — `base_score_bureau_movel` (`bureau`)
## 1) Contexto
Esta documentação descreve a tabela **`base_score_bureau_movel`** (referenciada como **`bureau`**), que contém **scores** e variáveis associadas ao bureau/score para o público do projeto.

O objetivo é padronizar:
- **significado provável** das colunas (com base em evidências coletadas)
- **papel da coluna** (chave, feature, label, filtro de população)
- **regras de uso** (incluindo risco de leakage)

---

## 2) Identificação da tabela
- **Nome lógico:** `base_score_bureau_movel`
- **Nome no ambiente (queries):** `bureau`
- **Formato de coorte:** `SAFRA` no padrão **YYYYMM**
- **Chave canônica para joins:** **`NUM_CPF + SAFRA`**
- **Grão (confirmado):** **1 linha por CPF por SAFRA**

---

## 3) Grão e chaves (confirmado)

### 3.1) Evidência
Foi validado que:
- **`COUNT(*)` = `COUNT(DISTINCT (NUM_CPF, SAFRA))`**
- Não existem duplicidades no par **`NUM_CPF + SAFRA`**

### 3.2) Conclusão
- **Grão:** `NUM_CPF + SAFRA` (**1:1**)
- **Uso:** essa chave deve ser o padrão para enriquecer a ABT/Gold com outras fontes (telco, cadastral, book recarga, book pagamento e book atraso).

---

## 4) Dicionário de colunas (v1)

### 4.1) Colunas de identificação/tempo

#### `NUM_CPF` (string)
- **Descrição:** identificador do CPF do cliente (provavelmente **hash/ofuscado**).
- **Papel:** **chave** (join key).
- **Regras recomendadas:**
  - não nulo (esperado)
  - não deve sofrer transformações que alterem o valor original

#### `SAFRA` (string)
- **Descrição:** coorte mensal no formato **YYYYMM**.
- **Papel:** **tempo / coorte / data_ref**.
- **Regras recomendadas:**
  - formato esperado: 6 dígitos (ex.: `202410`)
  - criar coluna derivada `DT_SAFRA` (date) para splits temporais e filtros por período

---

### 4.2) Colunas de score (features)

#### `SCORE_01` (string → int)
- **Descrição:** score numérico (Score 1).
- **Papel:** **feature** (Bloco “Scores” — primeira etapa do incremental).
- **Evidências coletadas:**
  - range observado: **0 a 778**
  - média observada: **~586,90**
  - percentis (p1/p5/p25/p50/p75/p95/p99): **[468, 503, 554, 587, 621, 674, 711]**
  - ocorrência de `0`: **1.864** registros (**~0,14%**)
- **Observação crítica:** `SCORE_01=0` é um **outlier extremo** (p1 muito acima de 0), com alta chance de ser **sentinela/missing codificado**.
- **Ação recomendada:**
  - criar `FLAG_SCORE01_MISSING` quando `SCORE_01` é nulo/vazio **ou** `SCORE_01=0`
  - manter `SCORE_01` como int por enquanto; decidir depois (via KS incremental) se faz `0 → NULL`

#### `SCORE_02` (string → int)
- **Descrição:** score numérico (Score 2).
- **Papel:** **feature** (Bloco “Scores” — segunda etapa do incremental).
- **Evidências coletadas:**
  - range observado: **1 a 917**
  - média observada: **~627,55**
  - percentis (p1/p5/p25/p50/p75/p95/p99): **[433, 481, 558, 622, 697, 790, 835]**
  - nulo/vazio: **576** registros (**~0,045%**)
- **Ação recomendada:** casting explícito + flag de missing simples.

---

### 4.3) Coluna de resultado (label candidato)

#### `FPD` (string → int/boolean)
- **Descrição provável:** flag de comportamento de crédito (“First Payment Default”).
- **Papel:** **label/target**.
- **Domínio observado:** `0` e `1` (existente em todas as safras).
- **Regra crítica (anti-leakage):**
  - **não usar como feature**.

---

### 4.4) Colunas constantes (filtro/população no recorte atual)

#### `FLAG_INSTALACAO` (string)
- **Valores distintos observados:** apenas **`1`**
- **Papel:** **filtro/população** (constante no recorte atual).
- **Regra:** não usar como feature (coluna constante não agrega sinal).

#### `PROD` (string)
- **Valores distintos observados:** apenas **`CMV`**
- **Papel:** **filtro/população** (segmento do público entregue).
- **Regra:** não usar como feature.

#### `flag_mig2` (string)
- **Valores distintos observados:** apenas **`PRE`**
- **Papel:** **filtro/população** (indica recorte).
- **Regra:** não usar como feature.

---

## 5) Notas de uso (incremental do projeto)
Para a visão incremental obrigatória:
1. Treinar modelo apenas com `SCORE_01` (e opcionalmente `FLAG_SCORE01_MISSING`)
2. Treinar modelo com `SCORE_01 + SCORE_02` (e flag)
3. Reportar **incremento de KS** etapa a etapa (sempre comparando com a versão anterior)

---
