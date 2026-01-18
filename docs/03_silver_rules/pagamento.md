# Silver Rules — Pagamento (`pagamento`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** da base de Pagamento, garantindo:
- parsing consistente de datas
- casting numérico de valores monetários
- tratamento correto de “duplicidade” como **versionamento**
- preparo para agregação mensal (Gold Features) por `NUM_CPF + SAFRA` derivada

---

## 2) Entradas e saídas

### 2.1) Entrada (Landing/Bronze)
- Tabela lógica: `pagamento` (transacional)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_pagamento_itens_current`
- Grão: “item de fatura/crédito” por cliente, mantendo **estado mais recente**.

---

## 3) Datas (parsing) e safra derivada

### 3.1) Parsing mínimo viável
- `TS_STATUS_FATURA = to_timestamp(upper(DAT_STATUS_FATURA),'ddMMMyyyy:HH:mm:ss')`
- `TS_STATUS_PAGAMENTO = to_timestamp(upper(DAT_STATUS_PAGAMENTO),'ddMMMyyyy:HH:mm:ss')`

### 3.2) Safra derivada (para Gold/ABT)
Como `DAT_STATUS_FATURA` tem completude total:
- `DT_REF = TS_STATUS_FATURA`
- `SAFRA_PAGAMENTO = date_format(to_date(DT_REF),'yyyyMM')`

Flag recomendada:
- `FLAG_TS_STATUS_PAGAMENTO_MISSING = 1` quando `DAT_STATUS_PAGAMENTO` nulo/vazio.

---

## 4) Casting monetário (double) e flags

### 4.1) Casting mínimo recomendado
- `VAL_PAGAMENTO_FATURA`, `VAL_PAGAMENTO_ITEM`
- `VAL_ATUAL_PAGAMENTO`, `VAL_ORIGINAL_PAGAMENTO`
- `VAL_PAGAMENTO_CREDITO`
- `VAL_DESCONTO_ITEM`
- `VAL_JUROS_MULTAS_ITEM`
- `VAL_MULTA_EQUIP_ITEM`, `VAL_MULTA_EQUIP_TOTAL`, `VAL_MULTA_FID_ITEM`
- `VAL_BAIXA_ATIVIDADE`

### 4.2) Juros/multas com sinal (não tratar como erro automaticamente)
Foi observado grande volume de `VAL_JUROS_MULTAS_ITEM` negativo.

Regras recomendadas:
- manter `VAL_JUROS_MULTAS_ITEM` como double (valor original)
- criar:
  - `FLAG_JUROS_NEG = 1` quando `VAL_JUROS_MULTAS_ITEM < 0`
- opcional para modelagem:
  - `VAL_JUROS_POS = greatest(VAL_JUROS_MULTAS_ITEM, 0)`
  - `VAL_JUROS_NEG_ABS = abs(least(VAL_JUROS_MULTAS_ITEM, 0))`

---

## 5) Versionamento (tratamento correto das duplicidades)

### 5.1) Evidência
Para a chave de item:
- `NUM_CPF + CONTRATO + SEQ_FATURA + NUM_SUB_SEQ_FATURA + NUM_CREDITO_SEQ`

Foi observado:
- 8.163 chaves com repetição (sempre 2 versões).
- Não há empates no timestamp máximo ao ordenar por `TS_STATUS_FATURA DESC`.
- Padrão dominante em `IND_STATUS_PAGAMENTO`: `NULL → B`.

### 5.2) Regra final (estado mais recente por item)
Criar `DEDUP_KEY`:
- `DEDUP_KEY = NUM_CPF + CONTRATO + SEQ_FATURA + NUM_SUB_SEQ_FATURA + NUM_CREDITO_SEQ`

Selecionar “versão current”:
- `row_number() over(partition by DEDUP_KEY order by TS_STATUS_FATURA desc) = 1`

**Auditoria obrigatória:**
- registrar `linhas_removidas = total_in - total_out` (esperado: 8.163).

---

## 6) Exemplo (Spark SQL — versão final)
```sql
WITH typed AS (
  SELECT
    NUM_CPF,
    CONTRATO,
    SEQ_FATURA,
    NUM_SUB_SEQ_FATURA,
    NUM_CREDITO_SEQ,

    to_timestamp(upper(DAT_STATUS_FATURA),'ddMMMyyyy:HH:mm:ss') AS TS_STATUS_FATURA,
    to_timestamp(upper(DAT_STATUS_PAGAMENTO),'ddMMMyyyy:HH:mm:ss') AS TS_STATUS_PAGAMENTO,

    date_format(to_date(to_timestamp(upper(DAT_STATUS_FATURA),'ddMMMyyyy:HH:mm:ss')),'yyyyMM') AS SAFRA_PAGAMENTO,

    IND_STATUS_FATURA,
    IND_STATUS_PAGAMENTO,

    CAST(NULLIF(TRIM(VAL_PAGAMENTO_FATURA),'') AS DOUBLE) AS VAL_PAGAMENTO_FATURA,
    CAST(NULLIF(TRIM(VAL_PAGAMENTO_ITEM),'') AS DOUBLE) AS VAL_PAGAMENTO_ITEM,
    CAST(NULLIF(TRIM(VAL_ATUAL_PAGAMENTO),'') AS DOUBLE) AS VAL_ATUAL_PAGAMENTO,
    CAST(NULLIF(TRIM(VAL_ORIGINAL_PAGAMENTO),'') AS DOUBLE) AS VAL_ORIGINAL_PAGAMENTO,
    CAST(NULLIF(TRIM(VAL_DESCONTO_ITEM),'') AS DOUBLE) AS VAL_DESCONTO_ITEM,
    CAST(NULLIF(TRIM(VAL_JUROS_MULTAS_ITEM),'') AS DOUBLE) AS VAL_JUROS_MULTAS_ITEM,

    CASE WHEN DAT_STATUS_PAGAMENTO IS NULL OR TRIM(DAT_STATUS_PAGAMENTO)='' THEN 1 ELSE 0 END AS FLAG_TS_STATUS_PAGAMENTO_MISSING,
    CASE WHEN CAST(NULLIF(TRIM(VAL_JUROS_MULTAS_ITEM),'') AS DOUBLE) < 0 THEN 1 ELSE 0 END AS FLAG_JUROS_NEG
  FROM pagamento
),
ranked AS (
  SELECT
    *,
    CONCAT(NUM_CPF,'#',CONTRATO,'#',SEQ_FATURA,'#',NUM_SUB_SEQ_FATURA,'#',NUM_CREDITO_SEQ) AS DEDUP_KEY,
    row_number() OVER (
      PARTITION BY CONCAT(NUM_CPF,'#',CONTRATO,'#',SEQ_FATURA,'#',NUM_SUB_SEQ_FATURA,'#',NUM_CREDITO_SEQ)
      ORDER BY TS_STATUS_FATURA DESC
    ) AS rn
  FROM typed
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1;
```

---

## 7) Diretriz para Gold Features (cliente-mês)
Para ABT, agregar por NUM_CPF + SAFRA_PAGAMENTO, exemplos:
- QTD_ITENS_FATURA_MES (count distinct item)
- SUM_VAL_PAGO_MES (sum VAL_ATUAL_PAGAMENTO)
- SUM_DESCONTOS_MES
- SUM_JUROS_POS_MES
- SUM_JUROS_NEG_ABS_MES
- distribuição por COD_FORMA_PAGAMENTO / COD_METODO_PAGAMENTO / status

---

## 8) Gates de qualidade (na Silver)
- TS_STATUS_FATURA parseado (0 inválidos)
- NUM_CPF não nulo
- auditoria de versionamento:
  - linhas_removidas esperado = 8.163
- monitoramento:
  - % DAT_STATUS_PAGAMENTO missing
  - % VAL_JUROS_MULTAS_ITEM negativo