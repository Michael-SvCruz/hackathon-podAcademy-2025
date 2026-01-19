# Data Dictionary — `book_atraso/dados_faturamento` (`atraso`)

## 1) Contexto
Esta documentação descreve a base de **Atraso / Faturamento** (referenciada como `atraso`), que representa um **snapshot mensal** de informações de fatura e dívida.

Evidência de snapshot:
- `DAT_REFERENCIA` tem sempre dia **01** (primeiro dia do mês), indicando fotografia mensal.

A base será usada para construir **features mensais** de inadimplência/dívida, respeitando o **anti-leakage** (cuidado com campos pós-evento como status e “aberto” em data futura).

---

## 2) Identificação da tabela
- **Nome no ambiente (queries):** `atraso`
- **Chave canônica do projeto (ABT):** `NUM_CPF + SAFRA` (derivada)
- **Safra recomendada:** derivada de `DAT_REFERENCIA`:
  - `SAFRA_ATRASO = date_format(to_date(TS_REFERENCIA),'yyyyMM')`

---

## 3) Colunas principais

### 3.1) Identificação / chaves
- `NUM_CPF` (string)  
  - **Papel:** chave de cliente para agregação
- `NUM_FATURA_HASH` (string)  
  - **Papel:** identificador (hash) da fatura
- `NUM_ENT_SEQ_FATURA` (string)  
  - **Papel:** sequência/entidade de fatura (identificador alternativo)
- `CONTRATO` (string)  
  - **Papel:** identificador de contrato

---

### 3.2) Datas (strings `ddMMMyyyy:HH:mm:ss`)
#### Snapshot / referência (tempo do dataset)
- `DAT_REFERENCIA`  
  - **Papel:** “data da fotografia” (base para safra)

#### Datas de ciclo da fatura
- `DAT_CRIACAO_FAT`
- `DAT_VENCIMENTO_FAT`
- `DAT_ORIGINAL_VCTO_FAT`
- `DAT_ALTERACAO_VCTO_FAT`
- `DAT_STATUS_FAT` (pode ser nula)
- `DAT_CANCELAMENTO_FAT`

#### Datas de registro/transação
- `DAT_CRIACAO_REGISTRO_TRANS`
- `DAT_ALTERACAO_REGISTRO_TRANS`
- `DAT_CRIACAO_REGISTRO_TRANS`, `DAT_ALTERACAO_REGISTRO_TRANS` (auditoria operacional)

#### Conta/cliente (perfil)
- `DAT_ATIVACAO_CONTA_CLI`
- `DAT_CRIACAO_DW`

---

### 3.3) Indicadores (flags)
Campos binários/categóricos (com sentinelas em alguns):
- `IND_WO` (ex.: W, R, -1) — write-off / status especial
- `IND_PDD` (ex.: S, N, -1)
- `IND_PCCR` (ex.: W, A, C, -1)
- `IND_ACA` (ex.: S, A, N)
- `IND_PRIMEIRA_FAT` (S/N)
- `IND_FRAUDE` (S/N)
- `IND_ISENCAO_COB_FAT` (Y/S/N/-2)

> Recomendação: tratar `-1/-2/-3` como sentinela (“não informado/não se aplica”) e criar flags.

---

### 3.4) Atributos categóricos (segmentação/medallion)
- `COD_PLATAFORMA` (valores como AUTOC, POSPG, PREPG, FLEXD, etc. + `-2`, `-3`)
- `DW_TIPO_FATURAMENTO` (código)
- `DW_TIPO_CLIENTE_CONTA` (código; inclui `-1`)
- Faixas de aging/tempo (códigos):
  - `DW_FAIXA_AGING_FATURA`
  - `DW_FAIXA_AGING_DIVIDA`
  - `DW_FAIXA_TEMPO_BASE` (inclui `-1`, `-3`)
  - `DW_FAIXA_AGING_PROX_FECH` (inclui `-1`)

---

### 3.5) Valores monetários (strings → double)
Principais colunas monetárias:
- `VAL_FAT_LIQUIDO`
- `VAL_FAT_BRUTO`
- `VAL_FAT_CREDITO`
- `VAL_FAT_AJUSTE`
- `VAL_FAT_BRUTO_BC`
- `VAL_FAT_PAGAMENTO_BRUTO`
- `VAL_FAT_ABERTO`
- `VAL_FAT_ABERTO_LIQ`
- `VAL_MULTA_JUROS`
- `VAL_MULTA_CANCELAMENTO`
- `VAL_PARC_APARELHO_LIQ`
- `VAL_FAT_LIQ_JM_MC`

---

## 4) Observação crítica de modelagem (anti-leakage)
Como o dataset é um snapshot mensal (`DAT_REFERENCIA` = 1º dia), parte das colunas pode refletir o **estado naquele momento**, e outras podem refletir eventos que ocorrem **depois** (ex.: mudanças de vencimento/status).

Recomendação para features:
- Preferir agregações por `SAFRA_ATRASO` baseada em `DAT_REFERENCIA`.
- Usar `DAT_VENCIMENTO_FAT` e `DAT_ORIGINAL_VCTO_FAT` para calcular atraso relativo ao snapshot (ex.: `days_past_due` no snapshot).
- Tratar `DAT_STATUS_FAT` com cuidado (pode estar ausente e não necessariamente define pagamento).

---

## 5) Pendências para v2
- Definir chave transacional oficial (fatura no snapshot) com base em análise de duplicidade (ver Data Quality e Silver Rules).
- Definir quais features serão permitidas no incremental (para evitar leakage).