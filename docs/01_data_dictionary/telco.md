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

# Data Dictionary — `base_telco` (`telco`)

## 1) Contexto
Esta documentação descreve a tabela **Telco** (referenciada como **`telco`**), fornecida no path:

- `/Volumes/hackathon_2025/default/source/base_telco/`

A base contém:
- chaves (`NUM_CPF`, `SAFRA`)
- metadados (`PROD`, `flag_mig2`, `FLAG_INSTALACAO`)
- target candidato (`FPD`)
- um conjunto de variáveis **anonimizadas** (`var_26` a `var_93`) sem dicionário de negócio.

**Nota importante (orientação da reunião):**
As colunas `var_*` foram apresentadas como variáveis **anônimas** (sem conceito divulgado). A recomendação é **não tentar interpretá-las semanticamente**, e sim tratá-las como bloco de features e validar contribuição via **KS incremental**.

---

## 2) Identificação da tabela
- **Nome lógico:** `base_telco`
- **Nome no ambiente (queries):** `telco`
- **Formato de coorte:** `SAFRA` no padrão **YYYYMM**
- **Chave canônica para joins:** **`NUM_CPF + SAFRA`**
- **Grão (confirmado):** **1 linha por CPF por SAFRA**

---

## 3) Grão e chaves (confirmado)

### 3.1) Evidência
Foi validado que:
- `COUNT(*)` = `COUNT(DISTINCT (NUM_CPF, SAFRA))`
- Não existem duplicidades no par `NUM_CPF + SAFRA`

### 3.2) Conclusão
- **Grão:** `NUM_CPF + SAFRA` (**1:1**)
- **Uso:** Telco pode ser enriquecida no spine (bureau) por join direto em `NUM_CPF + SAFRA`, sem risco de duplicar linhas.

---

## 4) Dicionário de colunas (v1)

### 4.1) Colunas de identificação/tempo

#### `NUM_CPF` (string)
- **Descrição:** identificador do CPF do cliente (provavelmente hash/ofuscado).
- **Papel:** **chave** (join key).
- **Regras recomendadas:** não nulo, não alterar o valor original.

#### `SAFRA` (string)
- **Descrição:** coorte mensal no formato **YYYYMM**.
- **Papel:** **tempo / coorte / data_ref**.
- **Regras recomendadas:** criar `DT_SAFRA` (date) derivada.

---

### 4.2) Metadados / variáveis de população (potencial risco de leakage)

#### `FLAG_INSTALACAO` (string)
- **Valores observados:** `0`, `1`
- **Interpretação provável:** indicador associado a contratação/instalação.
- **Papel recomendado:** **metadado de processo / população**.
- **Risco:** dependendo do evento âncora, pode se tornar uma variável com **alto risco de leakage** (ex.: marcar um resultado do processo).
- **Recomendação inicial:** **não usar como feature** até confirmar definição temporal.

#### `PROD` (string)
- **Valores observados:** `CMV`, `DTH`, `NET`
- **Interpretação provável:** tipo/família de produto.
- **Papel:** feature categórica potencial (mas também pode refletir recortes/política).
- **Recomendação:** testar no incremental do bloco Telco.

#### `flag_mig2` (string)
- **Valores observados:** `PRE`, `FLEX`, `Aquisição`, `null`
- **Interpretação provável:** segmentação/jornada (pré, flex, aquisição).
- **Papel:** feature categórica potencial.
- **Recomendação:** manter como variável candidata e avaliar impacto.

---

### 4.3) Coluna de resultado (label candidato)

#### `FPD` (string)
- **Valores observados:** `0`, `1`, `null`
- **Papel recomendado:** **label candidato**, não feature.
- **Observação:** existe missing em `FPD` nesta fonte; para treino, recomenda-se filtrar `FPD IS NOT NULL` ou utilizar target vindo da base central (bureau) para consistência do projeto.

---

### 4.4) Variáveis anonimizadas Telco (`var_*`)

#### `var_26` a `var_93` (string → numérico)
- **Descrição:** variáveis anonimizadas (sem dicionário de negócio).
- **Papel:** **features numéricas/categóricas** (a depender do domínio real de cada `var_`).
- **Evidências iniciais:**
  - várias `var_*` possuem valores numéricos (ex.: decimais, inteiros)
  - existe sentinela forte **`304`** em muitas colunas (provável “não informado/não aplicável”)

**Recomendação de uso:**
- tratar `var_*` como bloco de features do incremental Telco
- na Silver, aplicar:
  - casting explícito para numérico
  - padronização de missing (incluindo sentinela 304)
  - criação de flags de missing quando aplicável

---

## 5) Notas para o incremental do projeto
Para a visão incremental obrigatória, Telco é o **próximo bloco** após Scores.

Recomendação de versão de ABT:
- `gold_abt_v2_scores_telco` = `bureau_silver` + features Telco (var_* + metadados selecionados)

