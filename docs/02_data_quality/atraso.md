# Data Quality Report — Atraso/Faturamento (`atraso`)

## 1) Objetivo
Registrar evidências de qualidade da base `atraso`, com foco em:
- volumetria e período
- parsing de datas
- grão/duplicidade
- sentinelas em flags e faixas
- comportamento de valores monetários
- confirmação de snapshot mensal (anti-leakage)

---

## 2) Volumetria e período (datas parseadas)

### 2.1) Evidência (resultado)
- **total_linhas:** 31.611.316

Range temporal:
- `DAT_REFERENCIA`: min **2023-10-01**, max **2025-03-01**
- `DAT_VENCIMENTO_FAT`: min **2006-06-19**, max **2025-06-02**
- `DAT_STATUS_FAT`: min **2006-06-03**, max **2025-03-01**

### 2.2) Nota de qualidade (datas muito antigas)
Há registros com `DAT_VENCIMENTO_FAT`/`DAT_STATUS_FAT` desde 2006.  
Isso pode indicar:
- histórico legado ainda ativo no snapshot,
- ou reaproveitamento/long tail de dívidas antigas.

Recomendação:
- monitorar impacto em features (ex.: aging muito alto) e considerar cap por percentil na Gold se necessário.

---

## 3) Parsing e completude de datas

### 3.1) `DAT_REFERENCIA`
- nulos: 0
- inválidos: 0

### 3.2) `DAT_VENCIMENTO_FAT`
- nulos: 0
- inválidos: 0

### 3.3) `DAT_STATUS_FAT`
- nulos: 1.075.903
- inválidos: 0

Conclusão:
- datas são parseáveis; `DAT_STATUS_FAT` possui missing relevante e não pode ser usada como tempo único.

---

## 4) Confirmação de snapshot mensal
Evidência:
- `day(DAT_REFERENCIA)` é sempre **01** (100% das linhas)

Conclusão:
- a base representa uma fotografia mensal, adequada para features por `SAFRA_ATRASO = yyyyMM(DAT_REFERENCIA)`.

---

## 5) Grão e duplicidade

### 5.1) Candidato A — CPF + referência + fatura_hash
Chave:
- `NUM_CPF + DAT_REFERENCIA + NUM_FATURA_HASH`

Evidência:
- distintos: 24.736.187
- duplicadas: 6.875.129

Interpretação:
- há alta repetição desta chave, indicando que `NUM_FATURA_HASH` não é identificador único por CPF no snapshot, ou que a tabela contém múltiplas linhas por fatura (ex.: itens/segmentos/ajustes).

### 5.2) Candidato B — CPF + referência + entidade_seq
Chave:
- `NUM_CPF + DAT_REFERENCIA + NUM_ENT_SEQ_FATURA`

Evidência:
- distintos: 30.782.027
- duplicadas: 829.289

Interpretação:
- esta chave é bem mais próxima de um “grão lógico” do snapshot, mas ainda não é 1:1.

Recomendação:
- tratar o dataset como transacional/snapshot por fatura com múltiplas linhas, e fazer agregação por CPF+safra.
- para Silver, aplicar dedupe apenas se forem duplicatas exatas (ver Silver Rules).

---

## 6) Sentinelas e domínios (amostra)

### 6.1) Flags
- `IND_WO`: W, R, -1
- `IND_PDD`: S, N, -1
- `IND_PCCR`: W, A, C, -1
- `IND_ACA`: S, A, N
- `IND_PRIMEIRA_FAT`: S, N
- `IND_FRAUDE`: S, N
- `IND_ISENCAO_COB_FAT`: Y, S, N, -2

### 6.2) Plataforma
`COD_PLATAFORMA` inclui categorias de produto/plataforma e sentinelas:
- exemplos: AUTOC, POSPG, PREPG, FLEXD, POSTL, POSCW, etc.
- sentinelas: -2, -3

### 6.3) Faixas (top valores)
- `DW_FAIXA_AGING_FATURA`: concentração em 277 e 278
- `DW_FAIXA_AGING_DIVIDA`: concentração em 261 e 262
- `DW_FAIXA_TEMPO_BASE`: concentra em 356/352/355/354/353 e possui -1/-3
- `DW_FAIXA_AGING_PROX_FECH`: concentração em 303 e 304 e possui -1

Conclusão:
- sentinelas estão presentes e devem ser tratadas como “missing categórico” com flags.

---

## 7) Valores monetários (qualidade)
Evidência (nulos e negativos):
- todas as colunas testadas com 0 nulos e 0 negativos:
  - `VAL_FAT_LIQUIDO`, `VAL_FAT_BRUTO`, `VAL_FAT_ABERTO`, `VAL_FAT_PAGAMENTO_BRUTO`, `VAL_MULTA_JUROS`

Sanity check:
- `VAL_FAT_ABERTO > 0`: 31.604.320
- `VAL_FAT_ABERTO = 0`: 6.996
- `VAL_FAT_PAGAMENTO_BRUTO > 0`: 780.738

Interpretação:
- há predominância forte de “aberto” positivo no snapshot (dívida em aberto).
- pagamentos aparecem em subset menor; deve ser modelado como features separadas (pagamento vs aberto).

---

## 8) Recomendações (gates)
- Derivar safra por `DAT_REFERENCIA`.
- `DAT_STATUS_FAT` missing: criar flag e evitar uso como única data.
- Duplicidade: tratar como transacional; dedupe somente para duplicatas exatas caso identificadas por chave+valores.
- Criar flags para sentinelas `-1/-2/-3` em códigos/indicadores.