# Data Quality Report — Pagamento (`pagamento`)

## 1) Objetivo
Registrar evidências de qualidade da base de Pagamento, com foco em:
- volumetria e range temporal
- parsing de datas
- “duplicidade” (interpretada como versionamento)
- domínios principais
- comportamento de valores monetários (negativos/outliers)

---

## 2) Volumetria e range temporal (datas parseadas)

### 2.1) Evidência (resultado)
- **total_linhas:** 21.829.628

Datas (timestamp parseado):
- `DAT_STATUS_FATURA`: min **2023-10-01**, max **2025-03-31**
- `DAT_STATUS_PAGAMENTO`: min **2021-08-18**, max **2025-04-02**

---

## 3) Qualidade de parsing e completude das datas

### 3.1) `DAT_STATUS_FATURA`
- nulos: 0
- inválidos: 0

### 3.2) `DAT_STATUS_PAGAMENTO`
- nulos: 6.118.973
- inválidos: 0

### 3.3) Conclusão
- `DAT_STATUS_FATURA` é a melhor candidata para derivar safra (completude total).
- `DAT_STATUS_PAGAMENTO` deve ser usada quando disponível, mas não pode ser a única referência temporal.

---

## 4) “Duplicidade” no nível item (interpretação: versionamento)

### 4.1) Chave de item (final)
Chave refinada:
- `NUM_CPF + CONTRATO + SEQ_FATURA + NUM_SUB_SEQ_FATURA + NUM_CREDITO_SEQ`

Evidência (resultado):
- **num_chaves_duplicadas:** 8.163
- **total_linhas_em_chaves_duplicadas:** 16.326
- **max_repeticao:** 2

### 4.2) Desempate (consistência)
Foi verificado que ordenar por `TS_STATUS_FATURA DESC` resolve todos os casos:
- **chaves_com_empate_no_ts_max:** 0

### 4.3) Evidência de versionamento (mudanças entre versões)
Comparando rn=1 (mais recente) vs rn=2 (mais antiga), foram observadas diferenças:

- `dif_status_pagamento`: 8.107 de 8.163 (~99,3%)
- `dif_val_atual_pag`: 7.870 de 8.163 (~96,4%)
- `dif_status_fatura`: 2.737 de 8.163 (~33,5%)
- `dif_juros`: 1.274 de 8.163 (~15,6%)
- `dif_seq_entidade_pagamento`: 256 de 8.163 (~3,1%)

### 4.4) Padrão predominante de mudança de status (pagamento)
Top transições (`status_old → status_new`):
- `NULL → B`: 8.072
- `NULL → R`: 31
- `B → B`: 24
- `NULL → NULL`: 16
- `R → R`: 10
- `C → C`: 6
- `NULL → C`: 2
- `P → C`: 2

### 4.5) Conclusão
As “duplicidades” são, majoritariamente, **duas versões do mesmo item**, com evolução de status/valores.  
Portanto, a regra correta na Silver é: **manter a versão mais recente** (por `TS_STATUS_FATURA DESC`).

---

## 5) Valores monetários — negativos e outliers

### 5.1) Negativos (evidência)
- `VAL_PAGAMENTO_FATURA` &lt; 0: 0
- `VAL_PAGAMENTO_ITEM` &lt; 0: 0
- `VAL_ATUAL_PAGAMENTO` &lt; 0: 0
- `VAL_PAGAMENTO_CREDITO` &lt; 0: 0
- `VAL_ORIGINAL_PAGAMENTO` &lt; 0: 0
- `VAL_JUROS_MULTAS_ITEM` &lt; 0: 13.750.268 (**alto**)

Interpretação:
- `VAL_JUROS_MULTAS_ITEM` deve ser tratado como campo contábil com sinal (ajuste/abatimento) e não como “erro” automaticamente.

### 5.2) Outliers (evidência em `VAL_ATUAL_PAGAMENTO`)
- min: 0,01
- max: 3.407.335,19
- média: 98,0530
- percentis (p1/p5/p50/p95/p99): [2.28, 25.37, 59.89, 225.48, 407.28]

---

## 6) Recomendações (gates automatizáveis)
- Parsing de datas: manter 0 inválidos.
- Safra derivada: `DAT_STATUS_FATURA` como padrão.
- Versionamento: manter rn=1 por item com ordenação `TS_STATUS_FATURA DESC`.
- Flags:
  - `FLAG_TS_STATUS_PAGAMENTO_MISSING`
  - `FLAG_JUROS_NEG`