# Silver Rules — `bases_recarga` (`recarga`)

## 1) Objetivo
Definir regras de transformação para a camada **Silver** da Recarga, garantindo:
- parsing consistente de data/hora
- tipagem numérica para valores
- tratamento de sentinelas (`-1/-2/-3`)
- deduplicação no nível de evento
- preparo para agregação mensal (Gold Features)

---

## 2) Entradas e saídas

### 2.1) Entrada (Landing/Bronze)
- Fonte: `/Volumes/hackathon_2025/default/source/bases_recarga/`
- Tabela lógica: `recarga` (eventos)

### 2.2) Saída (Silver)
- Tabela sugerida: `silver_recarga_eventos`
- Grão: **1 linha por evento** (após dedupe)

---

## 3) Tipagem e colunas derivadas

### 3.1) Parsing de timestamp e safra derivada
- `TS_RECARGA = to_timestamp(DAT_INSERCAO_CREDITO,'ddMMMyyyy:HH:mm:ss')`
- `DT_RECARGA = to_date(TS_RECARGA)`
- `SAFRA_RECARGA = date_format(DT_RECARGA,'yyyyMM')`

**Observação:** `HOR_INSERCAO_CREDITO` pode ser mantida como string para auditoria, mas o timestamp principal deve vir do `DAT_INSERCAO_CREDITO` (já parseável).

### 3.2) Casting numérico
- `VAL_CREDITO_INSERIDO`, `VAL_BONUS`, `VAL_REAL`, `VALOR_SOS` → `double`

### 3.3) Sentinelas em dimensões
Para chaves dimensionais:
- mapear `-1/-2/-3` para uma categoria “sentinela” (manter o valor) e criar flag:
  - `FLAG_<COL>_SENTINELA = 1` quando `col in (-1,-2,-3)`.

---

## 4) Deduplicação (regra recomendada)

### 4.1) Por que deduplicar?
Foi observado:
- 320.770 possíveis duplicatas para uma chave composta candidata.

### 4.2) Estratégia recomendada
Criar uma chave do evento por hash de um conjunto robusto de colunas, e manter apenas 1 ocorrência.

Sugestão de colunas para `EVENT_KEY` (ajustável):
- `NUM_CPF`
- `DW_NUM_NTC`
- `DW_NUM_CLIENTE`
- `DAT_INSERCAO_CREDITO`
- `HOR_INSERCAO_CREDITO`
- `VAL_REAL`, `VAL_CREDITO_INSERIDO`, `VAL_BONUS`
- `COD_PLATAFORMA_ATU`, `COD_STATUS_PLATAFORMA`
- `COD_CANAL_AQUISICAO`, `COD_TIPO_CREDITO`, `COD_PROMOCAO`
- `DW_TIPO_RECARGA`, `DW_TIPO_INSERCAO`, `DW_FORMA_PAGAMENTO`, `DW_INSTITUICAO`

Exemplo (conceitual):
- `EVENT_KEY = sha2(concat_ws('||', <colunas>), 256)`

Regra de dedupe:
- manter `row_number() over(partition by EVENT_KEY order by TS_RECARGA desc)` = 1

> Observação: se houver coluna de ingestão/arquivo/metadado, usar como desempate.

---

## 5) Tratamento de valores negativos e casos especiais

### 5.1) Valores negativos
Como há volume relevante de:
- `VAL_BONUS < 0`
- `VAL_REAL < 0`

Recomendação na Silver:
- manter valor original em colunas “raw cast”
- criar flags:
  - `FLAG_VAL_BONUS_NEG`
  - `FLAG_VAL_REAL_NEG`
- criar colunas limpas para modelagem (opcional):
  - `VAL_BONUS_CLEAN = CASE WHEN VAL_BONUS < 0 THEN NULL ELSE VAL_BONUS END`
  - `VAL_REAL_CLEAN = CASE WHEN VAL_REAL < 0 THEN NULL ELSE VAL_REAL END`

A decisão de “zerar” vs “nulificar” deve ser validada por impacto no KS incremental.

---

## 6) Dimensões (decode opcional na Silver)

### 6.1) Recomendações
Existem duas abordagens válidas:

1. **Silver mantém códigos + flags**, decode fica para Gold/feature building  
2. **Silver já decodifica** descrições principais (ex.: forma pagamento, tecnologia, status) via join com `BI_DIM_*`

Como as dimensões têm sentinelas bem definidas (`-1/-2/-3`), a abordagem 2 é defensável e facilita análise exploratória. Porém, ela aumenta custo de join.

---

## 7) Gold Features (visão rápida — agregação mensal)
A recarga será transformada em features por `NUM_CPF + SAFRA_RECARGA` (cliente-mês). Exemplos úteis:
- `QTD_RECARGAS_MES`
- `SUM_VAL_REAL_MES` (preferencialmente usando `VAL_REAL_CLEAN`)
- `SUM_VAL_BONUS_MES` (preferencialmente usando `VAL_BONUS_CLEAN`)
- `FLAG_TEVE_SOS_MES` e `SUM_VALOR_SOS_MES`
- `QTD_RECARGAS_CANAL_TOP1` / `CANAL_TOP1` (se fizer decode)
- `FORMA_PAGAMENTO_TOP1` (se fizer decode)

---

## 8) Data Quality Gates (na Silver)
- `TS_RECARGA` não nulo e parseável
- `NUM_CPF` não nulo
- monitorar dedupe: `COUNT(*)` antes vs depois
- monitorar % sentinelas (`-1/-2/-3`) e % negativos (`VAL_REAL`, `VAL_BONUS`)
