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

# Data Dictionary — `base_dados_cadastrais` (`cadastro`)

## 1) Contexto
Esta documentação descreve a base **Cadastro** (referenciada como **`cadastro`**), fornecida no path:

- `/Volumes/hackathon_2025/default/source/base_dados_cadastrais/`

A base contém variáveis cadastrais, status e atributos anonimizados `var_*`, além de colunas comuns ao spine do projeto.

---

## 2) Identificação da tabela
- **Nome lógico:** `base_dados_cadastrais`
- **Nome no ambiente (queries):** `cadastro`
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
- **Uso:** pode ser enriquecida diretamente no spine (bureau) por join em `NUM_CPF + SAFRA`, sem duplicar linhas.

---

## 4) Dicionário de colunas (v1)

### 4.1) Identificação/tempo (chave)
#### `NUM_CPF` (string)
- **Descrição:** identificador do CPF do cliente (provavelmente hash/ofuscado).
- **Papel:** **chave** (join key).

#### `SAFRA` (string)
- **Descrição:** coorte mensal no formato **YYYYMM**.
- **Papel:** **tempo / data_ref**.
- **Recomendação:** criar `DT_SAFRA` (date) derivada.

---

### 4.2) Metadados do funil/público (atenção a leakage)
#### `FLAG_INSTALACAO` (string)
- **Valores observados:** `0`, `1`
- **Papel recomendado:** **metadado de processo/público**.
- **Risco:** pode virar **leakage** dependendo do evento âncora; usar com cuidado.

#### `PROD` (string)
- **Valores observados:** `CMV`, `DTH`, `NET`
- **Papel:** feature categórica potencial (ou metadado de produto).

#### `flag_mig2` (string)
- **Valores observados:** `PRE`, `FLEX`, `Aquisição`, `null`
- **Papel:** feature categórica potencial (segmentação/jornada).

---

### 4.3) Label/target candidato
#### `FPD` (string)
- **Valores observados:** `0`, `1`, `null`
- **Papel recomendado:** **label candidato**, não feature.
- **Nota:** nesta base existe volume alto de `FPD` nulo; recomenda-se padronizar o target usando a base central (bureau) e tratar `FPD` do cadastro apenas para auditoria.

---

### 4.4) Variáveis cadastrais explícitas
#### `STATUSRF` (string)
- **Valores observados (exemplos):** `REGULAR`, `PENDENTE DE REGULARIZACAO`, `SUSPENSA`, `CANCELADA`, `TITULAR FALECIDO`, `NULA`, `null`
- **Papel:** feature categórica (status cadastral).

#### `DATADENASCIMENTO` (string)
- **Formato observado:** `dd/MM/yyyy`
- **Papel:** feature (gerar **idade na safra**).
- **Recomendação:** converter para `date` e derivar `IDADE_ANOS`.

#### `CEP_3_digitos` (string)
- **Descrição:** 3 primeiros dígitos do CEP (proxy geográfico).
- **Papel:** feature categórica (região).
- **Recomendação:** manter como string; criar flag de missing.

---

### 4.5) Variáveis anonimizadas `var_*` (Cadastro)
A base possui `var_02` a `var_25` (ordem conforme schema). Estas variáveis são um **mix** de:
- numéricas (inteiros/decimais),
- categóricas (texto),
- e possivelmente campos de data (há evidência de strings `dd/MM/yyyy` em amostras).

**Recomendação geral:**
- classificar `var_*` em **numéricas**, **categóricas** e **datas**;
- aplicar casting explícito na Silver;
- documentar sentinelas/missing e outliers por coluna (progressivo).

Exemplos de comportamento observado:
- `var_07` aparenta ser **numérica contínua** (com grande assimetria e outliers).
- `var_15`, `var_22`, `var_23`, `var_24` apresentam grande volume **não numérico**, indicando natureza **categórica** (ex.: ocupação/cargo/status).
- `var_12` contém valores com aparência de data na amostra (ex.: `21/06/2017`), mas também possui entradas não parseáveis para `dd/MM/yyyy` (ex.: `2807`), exigindo parsing tolerante (`try_to_date`).

---

## 5) Notas para o incremental do projeto
Cadastro é o **bloco 3** do roteiro incremental (após Scores e Telco).  
Recomendação de ABT:
- `gold_abt_v3_scores_telco_cadastro` = spine (bureau) + features Telco + features Cadastro

---

## 6) Pendências para consolidar v2
- Formalizar regra de parsing/semântica para `var_12` e `var_13` (campos mistos).
- Definir estratégia de tratamento de **idade mínima/máxima** (outliers).
- Definir estratégia de encoding para variáveis categóricas de alta cardinalidade (se houver).