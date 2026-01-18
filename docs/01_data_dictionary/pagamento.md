# Data Dictionary — `book_pagamento/dados_pagamento` (`pagamento`)

## 1) Contexto
Esta documentação descreve a base de **Pagamento** (referenciada como `pagamento`).

A tabela é **transacional** e contém informações em múltiplos níveis (fatura, item, pagamento, crédito e atividades).  
Ela **não possui `SAFRA`** explícita; portanto, para o projeto, a `SAFRA` deve ser **derivada** a partir de uma data de referência (recomendação: `DAT_STATUS_FATURA`, por completude).

---

## 2) Identificação da tabela
- **Nome lógico:** Pagamento (transações / billing)
- **Nome no ambiente (queries):** `pagamento`
- **Chave canônica do projeto (para features):** `NUM_CPF + SAFRA` (derivada)
- **Grão natural:** transacional (múltiplas entidades por CPF)

---

## 3) Observação crítica: múltiplos “subníveis” no mesmo dataset
Pelo schema e evidências de duplicidade, há pelo menos 3 níveis misturados:

1. **Fatura / Item**
   - chaves: `CONTRATO`, `SEQ_FATURA`, `NUM_SUB_SEQ_FATURA`, `NUM_CREDITO_SEQ`
   - atributos: `DW_TIPO_FATURA`, `COD_TIPO_FATURA`, `IND_STATUS_FATURA`, `DAT_STATUS_FATURA`
   - valores: `VAL_PAGAMENTO_FATURA`, `VAL_PAGAMENTO_ITEM`, `VAL_DESCONTO_ITEM`, `VAL_JUROS_MULTAS_ITEM`

2. **Pagamento**
   - chaves: `SEQ_ENTIDADE_PAGAMENTO`, `NUM_FATURA_PAGAMENTO`, `COD_TIPO_PAGAMENTO`
   - atributos: `COD_METODO_PAGAMENTO`, `IND_STATUS_PAGAMENTO`, `DAT_STATUS_PAGAMENTO`
   - valores: `VAL_ORIGINAL_PAGAMENTO`, `VAL_ATUAL_PAGAMENTO`
   - descrições: `DSC_PAGAMENTO`, `DSC_NOME_BANCO_PAGAMENTO`

3. **Crédito / Alocação**
   - chaves: `SEQ_PAGAMENTO_CREDITO`, `SEQ_FATURA_CREDITO`, `SEQ_ENTIDADE_CREDITO`
   - atributos: `IND_TIPO_CREDITO`, `COD_ALOCACAO_CREDITO`, `COD_DESALOCACAO_CREDITO`
   - datas: `DAT_ATIVIDADE_CREDITO`, `DAT_VENCIMENTO_CREDITO`
   - valores: `VAL_PAGAMENTO_CREDITO`

**Implicação para pipeline:**
- Silver padroniza e seleciona o “estado mais recente” por item (ver regras).
- Gold agrega para cliente-mês (`NUM_CPF + SAFRA_PAGAMENTO`).

---

## 4) Colunas principais e domínios (v1)

## 4.1) Identificadores
- `NUM_CPF` (string): identificador do cliente (join key).
- `CONTRATO` (string): identificador do contrato.
- `DW_NUM_CLIENTE` (string): identificador interno.
- Chaves/Sequências:
  - `SEQ_FATURA`, `NUM_SUB_SEQ_FATURA`, `NUM_CREDITO_SEQ`
  - `SEQ_ENTIDADE_PAGAMENTO`, `SEQ_ENTIDADE_ATIVIDADE`, `SEQ_ENTIDADE_CREDITO`
  - `SEQ_PAGAMENTO_CREDITO`, `SEQ_FATURA_CREDITO`

---

## 4.2) Datas (strings no formato `ddMMMyyyy:HH:mm:ss`)
- `DAT_STATUS_FATURA` (referência temporal mais completa para safra)
- `DAT_STATUS_PAGAMENTO` (muito missing; usar quando preenchida)
- `DAT_CRIACAO_DW`
- `DAT_CRIACAO_PAGAMENTO`, `DAT_ATUALIZACAO_PAGAMENTO`
- `DAT_CRIACAO_CREDITO`, `DAT_ATUALIZACAO_CREDITO`
- `DAT_CRIACAO_ATIVIDADE`, `DAT_ATUALIZACAO_ATIVIDADE`, `DAT_BAIXA_ATIVIDADE`, `DAT_DEPOSITO_ATIVIDADE`
- `DAT_ATIVIDADE_CREDITO`, `DAT_VENCIMENTO_CREDITO`

---

## 4.3) Domínios e categorias relevantes (observados)
### Tipos
- `DW_TIPO_PAGAMENTO`: {30001, 30003, 30006, 30007}
- `COD_TIPO_PAGAMENTO`: {P, O, B, E, D, null}
- `DW_TIPO_FATURA`: múltiplos (inclui `-2`)
- `COD_TIPO_FATURA`: múltiplos (inclui `null`)
- `IND_TIPO_CREDITO`: {P, null}

### Método / Forma
- `COD_METODO_PAGAMENTO`: {1,2,3,4,5,6, null}
- `COD_FORMA_PAGAMENTO`: {CA, DD, PB, PA, null}
- `DW_FORMA_PAGAMENTO`: numérico (ex.: 10, 14 etc. na amostra)

### Status (pagamento)
- `IND_STATUS_PAGAMENTO`: {P, R, C, B, null}

---

## 4.4) Valores monetários (strings → double)
Principais:
- `VAL_PAGAMENTO_FATURA`
- `VAL_PAGAMENTO_ITEM`
- `VAL_ORIGINAL_PAGAMENTO`, `VAL_ATUAL_PAGAMENTO`
- `VAL_PAGAMENTO_CREDITO`
- `VAL_DESCONTO_ITEM`
- `VAL_JUROS_MULTAS_ITEM`
- `VAL_MULTA_EQUIP_ITEM`, `VAL_MULTA_EQUIP_TOTAL`, `VAL_MULTA_FID_ITEM`
- `VAL_BAIXA_ATIVIDADE`

---

## 5) Nota sobre “duplicidade” (na prática, versionamento)
Foi observado que algumas chaves de item aparecem 2 vezes. A análise indica que isso representa majoritariamente **mudança de status/valores ao longo do tempo** (ex.: `IND_STATUS_PAGAMENTO` mudando de `NULL` para `B`).  
A estratégia de Silver é tratar esses casos como **versionamento** e manter o registro mais recente por `TS_STATUS_FATURA`.