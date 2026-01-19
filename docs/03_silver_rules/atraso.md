# Silver Rules — Atraso/Faturamento (`atraso`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** da base de Atraso/Faturamento, garantindo:
- parsing consistente de datas
- casting numérico de valores
- flags de sentinelas
- estratégia correta para grão transacional (snapshot)
- preparo para Gold Features por `NUM_CPF + SAFRA_ATRASO`
- cuidados explícitos de **anti-leakage**

---

## 2) Entradas e saídas
### 2.1) Entrada (Landing/Bronze)
- tabela lógica: `atraso`

### 2.2) Saída (Silver)
- tabela sugerida: `silver_atraso_snapshot_itens`
- grão: transacional no snapshot (múltiplas linhas por CPF por referência)

---

## 3) Datas: parsing e safra derivada

### 3.1) Parsing mínimo
- `TS_REFERENCIA = to_timestamp(upper(DAT_REFERENCIA),'ddMMMyyyy:HH:mm:ss')`
- `TS_VENCIMENTO = to_timestamp(upper(DAT_VENCIMENTO_FAT),'ddMMMyyyy:HH:mm:ss')`
- `TS_STATUS_FAT = to_timestamp(upper(DAT_STATUS_FAT),'ddMMMyyyy:HH:mm:ss')`

### 3.2) Safra do atraso (tempo do snapshot)
Como `DAT_REFERENCIA` é sempre o dia 01:
- `SAFRA_ATRASO = date_format(to_date(TS_REFERENCIA),'yyyyMM')`

### 3.3) Flags de missing
- `FLAG_STATUS_FAT_MISSING = 1` quando `DAT_STATUS_FAT` nulo/vazio.

---

## 4) Casting monetário (double)
Converter para double (mínimo recomendado):
- `VAL_FAT_LIQUIDO`, `VAL_FAT_BRUTO`
- `VAL_FAT_CREDITO`, `VAL_FAT_AJUSTE`
- `VAL_FAT_BRUTO_BC`
- `VAL_FAT_PAGAMENTO_BRUTO`
- `VAL_FAT_ABERTO`, `VAL_FAT_ABERTO_LIQ`
- `VAL_MULTA_JUROS`, `VAL_MULTA_CANCELAMENTO`
- `VAL_PARC_APARELHO_LIQ`
- `VAL_FAT_LIQ_JM_MC`

---

## 5) Sentinelas e flags
Criar flags para colunas com `-1/-2/-3`:
- `FLAG_IND_WO_SENTINELA`
- `FLAG_IND_PDD_SENTINELA`
- `FLAG_IND_PCCR_SENTINELA`
- `FLAG_DW_TIPO_CLIENTE_SENTINELA`
- `FLAG_COD_PLATAFORMA_SENTINELA`
- `FLAG_FAIXA_TEMPO_BASE_SENTINELA` (observado -1 e -3)
- `FLAG_FAIXA_AGING_PROX_FECH_SENTINELA` (observado -1)

Regra padrão:
- `FLAG_<COL>_SENTINELA = 1` quando `COL IN ('-1','-2','-3')`

---

## 6) Duplicidade (como tratar corretamente)

### 6.1) Evidência
Foram observadas duplicidades relevantes em:
- `NUM_CPF + DAT_REFERENCIA + NUM_FATURA_HASH` (alta)
- `NUM_CPF + DAT_REFERENCIA + NUM_ENT_SEQ_FATURA` (menor, porém > 0)

### 6.2) Estratégia recomendada (conservadora)
Na Silver, **não tentar forçar 1:1 por fatura** sem entender o motivo (itens, ajustes, parcelas, etc.).  
Em vez disso:

- manter todas as linhas como “itens do snapshot”
- aplicar dedupe apenas para duplicatas exatas se forem detectadas (mesma chave + mesmos valores monetários + mesmas datas)

Chave candidata para dedupe exato (se necessário):
- `NUM_CPF + DAT_REFERENCIA + NUM_ENT_SEQ_FATURA + NUM_FATURA_HASH + DAT_VENCIMENTO_FAT + VAL_FAT_LIQUIDO + VAL_FAT_ABERTO`

> Observação: aplicar dedupe agressivo aqui pode apagar sinal real (ex.: componentes diferentes da fatura).

---

## 7) Anti-leakage (regras de uso para features)
Como é snapshot mensal:
- o tempo correto para join na ABT é `SAFRA_ATRASO` (derivada de `DAT_REFERENCIA`).
- features devem refletir o estado **no snapshot**, não eventos futuros.

Regras práticas:
- permitir features como:
  - soma de `VAL_FAT_ABERTO` no snapshot
  - contagem de faturas com `VAL_FAT_ABERTO > 0`
  - indicadores (WO/PDD/PCCR/ACA/FRAUDE) agregados
  - faixas de aging agregadas
- usar `TS_VENCIMENTO` apenas para calcular atraso relativo ao snapshot (ex.: `days_past_due = datediff(TS_REFERENCIA, TS_VENCIMENTO)`), se for coerente ao domínio.

Evitar (ou tratar com cautela):
- usar `DAT_STATUS_FAT` como se fosse “data de pagamento” (ela tem missing e pode refletir evento fora do snapshot).

---

## 8) Exemplo (Spark SQL — estrutura base)
```sql
SELECT
  NUM_CPF,

  to_timestamp(upper(DAT_REFERENCIA),'ddMMMyyyy:HH:mm:ss') AS TS_REFERENCIA,
  date_format(to_date(to_timestamp(upper(DAT_REFERENCIA),'ddMMMyyyy:HH:mm:ss')),'yyyyMM') AS SAFRA_ATRASO,

  NUM_FATURA_HASH,
  NUM_ENT_SEQ_FATURA,
  CONTRATO,

  to_timestamp(upper(DAT_VENCIMENTO_FAT),'ddMMMyyyy:HH:mm:ss') AS TS_VENCIMENTO,
  to_timestamp(upper(DAT_STATUS_FAT),'ddMMMyyyy:HH:mm:ss') AS TS_STATUS_FAT,
  CASE WHEN DAT_STATUS_FAT IS NULL OR TRIM(DAT_STATUS_FAT)='' THEN 1 ELSE 0 END AS FLAG_STATUS_FAT_MISSING,

  IND_WO, IND_PDD, IND_PCCR, IND_ACA, IND_PRIMEIRA_FAT, IND_FRAUDE, IND_ISENCAO_COB_FAT,

  COD_PLATAFORMA,
  DW_TIPO_FATURAMENTO,
  DW_TIPO_CLIENTE_CONTA,
  DW_FAIXA_AGING_FATURA,
  DW_FAIXA_AGING_DIVIDA,
  DW_FAIXA_TEMPO_BASE,
  DW_FAIXA_AGING_PROX_FECH,

  CAST(NULLIF(TRIM(VAL_FAT_LIQUIDO),'') AS DOUBLE) AS VAL_FAT_LIQUIDO,
  CAST(NULLIF(TRIM(VAL_FAT_BRUTO),'') AS DOUBLE) AS VAL_FAT_BRUTO,
  CAST(NULLIF(TRIM(VAL_FAT_ABERTO),'') AS DOUBLE) AS VAL_FAT_ABERTO,
  CAST(NULLIF(TRIM(VAL_FAT_ABERTO_LIQ),'') AS DOUBLE) AS VAL_FAT_ABERTO_LIQ,
  CAST(NULLIF(TRIM(VAL_FAT_PAGAMENTO_BRUTO),'') AS DOUBLE) AS VAL_FAT_PAGAMENTO_BRUTO,
  CAST(NULLIF(TRIM(VAL_MULTA_JUROS),'') AS DOUBLE) AS VAL_MULTA_JUROS
FROM atraso;
```

## 9) Diretriz para Gold Features (cliente-mês)
- Agregações por NUM_CPF + SAFRA_ATRASO, exemplos:
- SUM_ABERTO_MES = sum(VAL_FAT_ABERTO)
- QTD_FATURAS_ABERTO_MES = sum(case when VAL_FAT_ABERTO > 0 then 1 else 0 end)
- MAX_ABERTO_MES, AVG_ABERTO_MES
- SUM_PAGAMENTO_MES = sum(VAL_FAT_PAGAMENTO_BRUTO)
- flags agregadas:
  - TEVE_WO_MES, TEVE_PDD_MES, TEVE_FRAUDE_MES
- features de aging:
  - distribuição por DW_FAIXA_AGING_FATURA e DW_FAIXA_AGING_DIVIDA