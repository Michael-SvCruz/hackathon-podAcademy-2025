# Data Dictionary — `bases_recarga` (`recarga`)

## 1) Contexto
Esta documentação descreve a base de **Recarga** (referenciada como `recarga`), fornecida no path:

- `/Volumes/hackathon_2025/default/source/bases_recarga/`

A base representa **eventos/transações de recarga** e traz códigos (DW/COD) que podem ser decodificados via tabelas dimensão `BI_DIM_*`.

> Importante: Recarga **não vem no grão cliente-mês**. Ela é **event-level** (uma linha por evento). A `SAFRA` usada no projeto será **derivada** a partir da data da recarga (`DAT_INSERCAO_CREDITO`).

---

## 2) Identificação da tabela (fato)
- **Nome lógico (fato):** recarga (tabela de eventos)
- **Chave natural do projeto (para features):** `NUM_CPF + SAFRA` (derivada)
- **Tempo do evento:** `DAT_INSERCAO_CREDITO` + `HOR_INSERCAO_CREDITO`

---

## 3) Colunas principais (fato)

### 3.1) Identificação
- `NUM_CPF` (string)  
  - **Papel:** chave de cliente (join por CPF no spine)
- `DW_NUM_CLIENTE` (string)  
  - **Papel:** identificador interno de cliente (potencial chave alternativa/auditoria)
- `DW_NUM_NTC` (string)  
  - **Papel:** identificador de linha/terminal (útil para auditoria; não é a chave canônica do projeto)

### 3.2) Tempo (evento)
- `DAT_INSERCAO_CREDITO` (string)  
  - **Formato observado:** `ddMMMyyyy:HH:mm:ss` (ex.: `09OCT2023:00:00:00`)
  - **Papel:** data do evento (base para criar `DT_RECARGA` e `SAFRA_RECARGA`)
- `HOR_INSERCAO_CREDITO` (string)  
  - **Formato observado:** numérico tipo `HHMMSS` sem separador (ex.: `170410`)
  - **Papel:** horário do evento (pode compor timestamp)

### 3.3) Valores monetários (features numéricas)
- `VAL_CREDITO_INSERIDO` (string → double)  
  - **Papel:** valor de crédito inserido (pode ser 0 em algumas situações)
- `VAL_BONUS` (string → double)  
  - **Papel:** valor de bônus (pode ter outliers e valores negativos/sentinelas)
- `VAL_REAL` (string → double)  
  - **Papel:** “dinheiro real”/valor real associado (pode ter outliers e valores negativos/sentinelas)

> Observação: bônus e SOS podem não representar “dinheiro real” no mesmo sentido de recarga tradicional; por isso recomenda-se criar features separadas (ex.: soma de `VAL_REAL` vs soma de `VAL_BONUS`).

### 3.4) SOS (serviço associado)
- `FLAG_SOS` (string)  
  - **Papel:** flag indicando ocorrência de SOS
- `VALOR_SOS` (string → double)  
  - **Papel:** valor associado ao SOS (observado entre 3 e 20)

### 3.5) Códigos para decodificação (dimensões)
Estas colunas são chaves para `BI_DIM_*`:
- `COD_TECNOLOGIA_DW` → `BI_DIM_TECNOLOGIA`
- `COD_TIPO_CREDITO` → `BI_DIM_TIPO_CREDITO`
- `DW_TIPO_INSERCAO` → `BI_DIM_TIPO_INSERCAO`
- `DW_TIPO_RECARGA` → `BI_DIM_TIPO_RECARGA`
- `DW_FORMA_PAGAMENTO` → `BI_DIM_FORMA_PAGAMENTO`
- `COD_PLATAFORMA_ATU` → `BI_DIM_PLATAFORMA` (atenção: na fato é string; na dimensão é inteiro)
- `COD_STATUS_PLATAFORMA` → `BI_DIM_STATUS_PLATAFORMA`
- `COD_CANAL_AQUISICAO` → `BI_DIM_CANAL_AQUISICAO_CREDITO` (dim sensível; schema ok)
- `DW_INSTITUICAO` → `BI_DIM_INSTITUICAO` (dim sensível; schema ok)
- `DW_PLANO_TARIFACAO` → `BI_DIM_PLANO_PRECO` (dim sensível; schema ok)
- `COD_PROMOCAO` → `BI_DIM_PROMOCAO_CREDITO`

### 3.6) Sentinelas / “não informado”
Em várias colunas de código, aparecem valores **`-1`, `-2`, `-3`** com significado padronizado nas dimensões:
- `-1` = “Não se aplica”
- `-2` = “Não determinado”
- `-3` = “Não informado”

**Recomendação:** tratar `-1/-2/-3` como “missing categórico” (com flag), e opcionalmente mapear para o rótulo da dimensão.

---

## 4) Dimensões disponíveis (BI_DIM_*)
Dimensões confirmadas na pasta:
- `BI_DIM_FORMA_PAGAMENTO`
- `BI_DIM_PLATAFORMA`
- `BI_DIM_PROMOCAO_CREDITO`
- `BI_DIM_STATUS_PLATAFORMA`
- `BI_DIM_TECNOLOGIA`
- `BI_DIM_TIPO_CREDITO`
- `BI_DIM_TIPO_INSERCAO`
- `BI_DIM_TIPO_RECARGA`
- `BI_DIM_CANAL_AQUISICAO_CREDITO`             
- `BI_DIM_INSTITUICAO`
- `BI_DIM_PLANO_PRECO` 

---

## 5) Como essa base entra no incremental (visão do projeto)
Recarga entra como bloco de features no incremental após Cadastro. O caminho recomendado é:

1. Silver Recarga (event-level): padroniza e limpa eventos
2. Gold Recarga Features (cliente-mês): agrega para `NUM_CPF + SAFRA_RECARGA`
3. ABT incremental: join dessas features no spine (`bureau`) por `NUM_CPF + SAFRA`

---

## 6) Pendências para v2
- Definir regra final de deduplicação (evento repetido vs eventos legítimos iguais).
- Definir tratamento para valores negativos em `VAL_REAL` e `VAL_BONUS` (sentinela vs ajuste).
- Validar consistência de parsing de `SAFRA_RECARGA` a partir de `DT_RECARGA` (min/max por data real, não por string).