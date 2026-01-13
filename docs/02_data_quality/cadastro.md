# Data Quality Report — `base_dados_cadastrais` (`cadastro`)

## 1) Objetivo
Registrar evidências de qualidade dos dados do **Cadastro**, focando em:
- **unicidade** do grão `NUM_CPF + SAFRA`
- **completude** de colunas core (chave/tempo/target/metadados)
- **qualidade de datas** (nascimento e possíveis datas em `var_*`)
- **tipagem real** (numéricas vs categóricas)
- **ranges e outliers** em variáveis numéricas principais

---

## 2) Unicidade do grão (CPF + SAFRA)

### 2.1) Evidência (resultado)
- **total_linhas:** 3.900.378  
- **chaves_unicas_cpf_safra:** 3.900.378  
- **linhas_duplicadas:** 0

### 2.2) Conclusão
A base está em **grão 1:1 por `NUM_CPF + SAFRA`** (join seguro no spine).

---

## 3) Completude (nulos/vazios) — colunas core

### 3.1) Evidência (resultado)
- `NUM_CPF`: 0 nulos/vazios
- `SAFRA`: 0 nulos/vazios
- `FPD`: 1.203.757 nulos/vazios (**volume alto**)
- `PROD`: 0 nulos/vazios
- `flag_mig2`: 1.266.478 nulos/vazios (**volume alto**)
- `FLAG_INSTALACAO`: 0 nulos/vazios
- `STATUSRF`: 15.154 nulos/vazios
- `DATADENASCIMENTO`: 16.831 nulos/vazios
- `CEP_3_digitos`: 292.051 nulos/vazios

### 3.2) Conclusão
- `FPD` e `flag_mig2` possuem **missing relevante** (impacta treino se usarem essa base como fonte de label).
- `CEP_3_digitos` possui missing moderado (recomendado criar flag).
- `DATADENASCIMENTO` tem missing baixo (boa candidata a feature via idade).

---

## 4) Sanity check — Idade na safra (derivada de `DATADENASCIMENTO`)
### 4.1) Evidência (resultado)
- **min_idade:** 4  
- **max_idade:** 131  

### 4.2) Interpretação
- Existem valores extremos (idade muito baixa e muito alta).
- Recomendação: manter a idade na Silver e criar flags/validações para outliers, com regra de negócio na Gold (ex.: cap/winsorize ou tratar fora do intervalo esperado).

---

## 5) Tipagem real das `var_*` (numérico vs categórico)

### 5.1) Evidência (resultado — contagem de não numéricos)
Para algumas colunas:
- `var_03` a `var_09`: 0 não numéricos (candidatas a numéricas)
- `var_15`: 585.003 não numéricos (candidata a categórica)
- `var_22`: 366.770 não numéricos (candidata a categórica/mista)
- `var_23`: 585.003 não numéricos (candidata a categórica)
- `var_24`: 2.410.043 não numéricos (forte candidata a categórica)

### 5.2) Conclusão
A base tem `var_*` **mistas**. Necessário:
- casting para numéricas apenas nas colunas com 0 não numéricos
- manter como string e padronizar nas colunas categóricas/mistas

---

## 6) Estatísticas de variáveis numéricas (amostra)

### 6.1) `var_07` (numérica, assimétrica com outliers)
Evidências:
- min: 0
- max: 37.975.401
- média: 46.166,7850
- percentis (p1/p5/p50/p95/p99): [0, 478.66, 2151.07, 142758, 277059]

Interpretação:
- distribuição altamente assimétrica (provável variável monetária/renda/valor).
- recomendação: considerar `log1p` e/ou caps na Gold.

### 6.2) `var_08` (numérica discreta)
- range: 1 a 98
- percentis: [21, 21, 32, 91, 92]

### 6.3) `var_09` (numérica discreta)
- range: 1 a 18
- percentis: [1, 4, 9, 9, 9]

### 6.4) `var_10` (numérica com concentração em valores baixos e cauda alta)
- range: 0 a 922.019
- percentis: [0, 0, 1, 3022, 922007]

### 6.5) `var_11` (numérica com valores negativos)
- min: -2791.67
- max: 49.329,24
- percentis: [0, 0, 2467.5, 13756.93, 22516.88]

Interpretação:
- existência de negativos sugere sentinela/ajuste/erro; recomendação: tratar negativos como inválidos ou criar flag.

---

## 7) Datas e parsing tolerante (erro identificado)

### 7.1) Problema observado
O parsing com `to_date(...,'dd/MM/yyyy')` falhou devido a valores inválidos como `'2807'`.

### 7.2) Recomendação
Usar funções tolerantes:
- `try_to_date(col,'dd/MM/yyyy')` (quando disponível)  
ou
- `try_cast(to_date(...))` dependendo do runtime.

**Query recomendada (para medir inválidos sem quebrar):**
```sql
SELECT
  COUNT(*) AS total,

  SUM(CASE WHEN DATADENASCIMENTO IS NULL OR TRIM(DATADENASCIMENTO)='' THEN 1 ELSE 0 END) AS null_dn,
  SUM(CASE WHEN DATADENASCIMENTO IS NOT NULL AND TRIM(DATADENASCIMENTO)<>'' AND try_to_date(DATADENASCIMENTO,'dd/MM/yyyy') IS NULL THEN 1 ELSE 0 END) AS invalid_dn,

  SUM(CASE WHEN var_12 IS NULL OR TRIM(var_12)='' THEN 1 ELSE 0 END) AS null_var12,
  SUM(CASE WHEN var_12 IS NOT NULL AND TRIM(var_12)<>'' AND try_to_date(var_12,'dd/MM/yyyy') IS NULL THEN 1 ELSE 0 END) AS invalid_var12

FROM cadastro;
```

## 8) Distribuição de STATUSRF (categoria)
### 8.1) Evidência (resultado)
- REGULAR: 3.848.697
- PENDENTE DE REGULARIZACAO: 31.217
- SUSPENSA: 2.689
- TITULAR FALECIDO: 2.398
- CANCELADA: 222
- NULA: 1
- null: 15.154

### 8.2) Conclusão
Variável categórica de baixa cardinalidade, adequada para encoding e com sinal potencial.

## 9) Recomendações de Data Quality (para automatizar)
- Validar SAFRA (YYYYMM) e não nulos de NUM_CPF e SAFRA.
- Monitorar % FPD nulo por safra (decidir regra de treino).
- Criar monitoramento de outliers em IDADE_ANOS, var_07, var_10 e negativos em var_11.
- Padronizar parsing de datas com funções tolerantes (try_to_date) e medir taxa de inválidos.