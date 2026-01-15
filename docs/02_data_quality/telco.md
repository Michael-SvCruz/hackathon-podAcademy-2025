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

# Data Quality Report — `base_telco` (`telco`)

## 1) Objetivo
Registrar evidências de qualidade dos dados da tabela `telco`, com foco em:
- unicidade do grão `NUM_CPF + SAFRA`
- completude e domínio do target candidato (`FPD`)
- presença de sentinelas (ex.: `304`) em `var_*`
- validação de tipagem (numérico vs não numérico)

---

## 2) Unicidade do grão (CPF + SAFRA)

### 2.1) Evidência (resultado)
- **total_linhas:** 1.367.104  
- **chaves_unicas_cpf_safra:** 1.367.104  
- **linhas_duplicadas:** 0

### 2.2) Conclusão
A Telco está em **grão 1:1 por `NUM_CPF + SAFRA`**, permitindo join seguro no spine.

---

## 3) Domínios e completude de metadados (resumo observado)
### 3.1) Domínios observados (queries exploratórias)
- `FLAG_INSTALACAO`: `0`, `1`
- `FPD`: `0`, `1`, `null`
- `PROD`: `CMV`, `DTH`, `NET`
- `flag_mig2`: `PRE`, `FLEX`, `Aquisição`, `null`

### 3.2) `FPD` (completude global)
Resultado observado:
- `FPD null`: 45.936
- `FPD = 0`: 1.006.838
- `FPD = 1`: 314.330

Conclusão:
- existe **missing relevante** em `FPD` (~3,36% do total)
- para modelagem, recomenda-se filtrar `FPD IS NOT NULL` ou usar target do bureau como padrão.

---

## 4) Sentinela `304` e missing em variáveis `var_*` (amostra var_26 a var_36)

### 4.1) Evidência (resultado)
Para as colunas abaixo, foi medido:
- `null_var_xx`: quantidade de nulos/vazios
- `sentinel304_var_xx`: quantidade de ocorrências do valor 304

Resultados coletados (amostra):
- `var_26`: null 1295 | sentinel304 0
- `var_27`: null 1295 | sentinel304 0
- `var_28`: null 1295 | sentinel304 360.978
- `var_29`: null 1295 | sentinel304 796.407
- `var_30`: null 1295 | sentinel304 364.503
- `var_31`: null 1295 | sentinel304 201.387
- `var_32`: null 1295 | sentinel304 389.066
- `var_33`: null 1295 | sentinel304 224.548
- `var_34`: null 1295 | sentinel304 531.118
- `var_35`: null 1295 | sentinel304 384.468
- `var_36`: null 1295 | sentinel304 955.157

### 4.2) Conclusão
- Existe uma sentinela muito forte **`304`** em várias `var_*`, sugerindo “não informado/não aplicável”.
- Isso deve ser tratado na Silver com:
  - **flag de missing**
  - decisão de manter `304` como valor (padrão) ou convertê-lo em `NULL` (para algoritmos que lidam melhor com missing).

---

## 5) Validação de tipagem (não numéricos)

### 5.1) Evidência (resultado)
Para amostra de colunas:
- `nao_numericos_var_29`: 0
- `nao_numericos_var_30`: 0
- `nao_numericos_var_34`: 0
- `nao_numericos_var_36`: 0

### 5.2) Conclusão
As colunas amostradas são **integralmente numéricas** (apesar de estarem como `string` no schema).  
Recomendação: casting explícito para `double`/`int` na Silver.

---

## 6) Recomendações de Data Quality (para automatizar)
- Validar `SAFRA` no formato `YYYYMM`.
- Validar não nulos de `NUM_CPF` e `SAFRA`.
- Monitorar `% FPD nulo` por safra.
- Monitorar `% sentinela 304` por coluna `var_*` ao longo das safras (drift de completude).