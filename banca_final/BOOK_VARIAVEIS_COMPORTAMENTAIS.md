# Entregavel B — Book de Variaveis Comportamentais

## Metadados do Documento

| Atributo | Valor |
|----------|-------|
| **Entregavel** | B — Books de Variaveis |
| **Blocos** | Recarga, Pagamento, Atraso |
| **ABT Final** | `hackathon_2025.default.gold_abt_v6_v2` |
| **Registros** | 3.795.310 |
| **Variaveis Comportamentais** | ~423 (126 Recarga + 135 Pagamento + 162 Atraso) |
| **Janelas Temporais** | M1 (1 mes), M3 (3 meses), M6 (6 meses) |
| **Grao** | NUM_CPF + SAFRA (cliente-mes, estritamente 1:1) |
| **Data** | 2026-03-11 |

---

## Sumario

| # | Secao | Pagina |
|---|-------|--------|
| 1 | [Visao Geral e Arquitetura](#1-visao-geral-e-arquitetura) | - |
| 2 | [Regras Anti-Leakage](#2-regras-anti-leakage) | - |
| 3 | [Janelas Temporais (M1/M3/M6)](#3-janelas-temporais-m1m3m6) | - |
| 4 | [Book de Recarga](#4-book-de-recarga) | - |
| 5 | [Book de Pagamento](#5-book-de-pagamento) | - |
| 6 | [Book de Atraso](#6-book-de-atraso) | - |
| 7 | [Cobertura por Bloco](#7-cobertura-por-bloco) | - |
| 8 | [Contribuicao ao Modelo (KS Incremental)](#8-contribuicao-ao-modelo-ks-incremental) | - |
| 9 | [Combinacoes de Features e Insights de Dominio](#9-combinacoes-de-features-e-insights-de-dominio) | - |
| 10 | [Glossario](#10-glossario) | - |

---

## 1. Visao Geral e Arquitetura

### 1.1 Pipeline de Construcao

```
Silver (evento/snapshot)
    │
    ▼
Gold Feature Scripts (agregacao mensal)
    │  gold_recarga_features_v2.py    → gold_recarga_features_v2
    │  gold_pagamento_features_v2.py  → gold_pagamento_features_v2
    │  gold_atraso_features_v2.py     → gold_atraso_features_v2
    │
    ▼
ABT Builder (join temporal + janelas M1/M3/M6)
    │  05_gold_abt_v6_builder_v2.py   → gold_abt_v6_v2 (614 colunas)
    │
    ▼
Modelo LightGBM (IV > 0.01 → 261 features selecionadas)
```

### 1.2 Estrategia de Agregacao

Cada script Gold agrega os dados transacionais/snapshot da camada Silver em **grao mensal** (1 registro por CPF por mes-fonte). O ABT Builder aplica o filtro temporal `SAFRA_FEATURE < SAFRA` e agrega as features mensais em tres janelas:

| Janela | Meses Retroativos | Sufixo | Descricao |
|--------|-------------------|--------|-----------|
| M1 | 1 | `_m1` | Comportamento recente (ultimo mes) |
| M3 | 3 | `_m3` | Tendencia de curto prazo |
| M6 | 6 | `_m6` | Padrao consolidado |

### 1.3 Tratamento de Valores Especiais

| Fonte | Valor Sentinela | Significado | Tratamento |
|-------|-----------------|-------------|------------|
| Recarga | -1 | Nao se aplica | → NULL + FLAG_SENTINELA |
| Recarga | -2 | Nao determinado | → NULL + FLAG_SENTINELA |
| Recarga | -3 | Nao informado | → NULL + FLAG_SENTINELA |
| Pagamento | NULL em monetarios | Dado ausente | → COALESCE(col, 0.0) |
| Atraso | NULL em monetarios | Dado ausente | → COALESCE(col, 0.0) |
| Atraso | Indicadores binarios | Texto variado | '1','S','Y','SIM','YES','TRUE' → 1, demais → 0 |

---

## 2. Regras Anti-Leakage

### 2.1 Variaveis Proibidas como Features

| Variavel | Papel | Regra |
|----------|-------|-------|
| `fpd_int` | **TARGET** | Nunca usar como feature. So observado quando `flag_instalacao_int = 1` |
| `flag_instalacao_int` | **Decisao** | Nunca usar como feature. Apenas para swap analysis |

### 2.2 Integridade Temporal

Todas as features comportamentais respeitam a regra:

```
SAFRA_FEATURE < SAFRA
```

Onde `SAFRA_FEATURE` e o mes de origem do evento (recarga, pagamento ou atraso) e `SAFRA` e o mes de referencia do registro na ABT. Isso garante que **apenas dados passados** sao usados como features.

### 2.3 Treino vs Scoring

| Contexto | Populacao | FPD Observado? |
|----------|-----------|----------------|
| **Treino** | Apenas `flag_instalacao_int = 1` | Sim |
| **Scoring** | Todos os registros | Nao (previsao) |

---

## 3. Janelas Temporais (M1/M3/M6)

### 3.1 Definicao

As janelas temporais sao relativas a `SAFRA` (mes de referencia do cliente na ABT):

| Janela | Periodo | Exemplo (SAFRA = 202501) |
|--------|---------|--------------------------|
| M1 | 1 mes anterior | 202412 |
| M3 | 3 meses anteriores | 202410, 202411, 202412 |
| M6 | 6 meses anteriores | 202407 a 202412 |

### 3.2 Agregacao Multi-Mes

Para janelas M3 e M6, as features mensais sao re-agregadas:
- **Contagens:** SUM das contagens mensais
- **Valores monetarios:** SUM dos totais mensais
- **Medias:** Recalculadas como SUM(total) / SUM(contagem)
- **Flags:** MAX (1 se ocorreu em qualquer mes da janela)
- **Desvio padrao:** Recalculado sobre os valores da janela completa

### 3.3 Limiares de Baixa Atividade

| Janela | Limiar Recarga | Limiar Pagamento |
|--------|----------------|------------------|
| M1 | < 2 recargas | < 2 pagamentos |
| M3 | < 5 recargas | < 2 pagamentos |
| M6 | < 10 recargas | < 2 pagamentos |

---

## 4. Book de Recarga

### 4.1 Resumo do Bloco

| Atributo | Valor |
|----------|-------|
| **Fonte Silver** | `silver_recarga` (~95M eventos) |
| **Grao de Entrada** | 1 linha por evento de recarga |
| **Grao de Saida** | 1 linha por NUM_CPF + SAFRA_RECARGA (mensal) |
| **Total de Features** | ~42 base x 3 janelas = ~126 variaveis |
| **Cobertura M1** | 56.12% |
| **Tabela Gold** | `gold_recarga_features_v2` |
| **Relevancia** | Estresse financeiro (SOS), padroes de consumo, regularidade |

### 4.2 Regra de Negocio — SOS (Emprestimo Emergencial)

> **SOS e um emprestimo/adiantamento de R$3 a R$20 (tipicamente R$5) descontado na proxima recarga. Alta frequencia de SOS e forte indicador de estresse financeiro.**

**Regra de ajuste do valor real:**

```
SE flag_sos = 1 E valor_sos = val_credito_inserido:
    val_real_ajustado = -valor_sos          # recarga inteira era SOS
SENAO SE flag_sos = 1 E valor_sos != val_credito_inserido:
    val_real_ajustado = val_credito - valor_sos   # parte era SOS
SENAO:
    val_real_ajustado = val_real             # recarga normal
```

**Ajuste adicional para bonus:**
```
SE tipo_transacao IN ('COMBO_PAGO_BONUS', 'BONUS_PURO'):
    val_final = val_real_ajustado - val_bonus   # bonus nao e dinheiro real
SENAO:
    val_final = val_real_ajustado
```

### 4.3 Classificacao de Tipo de Transacao

| Tipo | Regra | Significado |
|------|-------|-------------|
| `PAGO_PURO` | val_credito > 0 AND val_bonus = 0 | Recarga tradicional (so credito) |
| `BONUS_PURO` | val_credito = 0 AND val_bonus > 0 | Apenas bonus (promocao) |
| `COMBO_PAGO_BONUS` | val_credito > 0 AND val_bonus > 0 | Recarga + bonus |
| `ZERO_TOTAL` | val_credito = 0 AND val_bonus = 0 AND val_real = 0 | Evento sem valor |
| `VALOR_NEGATIVO` | val_real < 0 | Estorno ou ajuste |
| `OUTROS` | Demais casos | Inclassificavel |

> `flag_recarga_valida = 1` quando `tipo_transacao != 'ZERO_TOTAL'`

### 4.4 Classificacao de Periodo do Dia

| Periodo | Horario | Relevancia para Risco |
|---------|---------|----------------------|
| `MADRUGADA` | 00h-05h | Alta — comportamento atipico |
| `MANHA` | 06h-11h | Baixa — horario padrao |
| `TARDE` | 12h-17h | Baixa — horario padrao |
| `NOITE` | 18h-23h | Media — pos-expediente |

> `flag_fim_semana = 1` quando dia da semana e Sabado ou Domingo

### 4.5 Features de Volume

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_recargas` | _m1, _m3, _m6 | int | COUNT(*) | Total de eventos de recarga |
| `qtd_recargas_validas` | _m1, _m3, _m6 | int | SUM(flag_recarga_valida) | Recargas excluindo ZERO_TOTAL |
| `qtd_telefones_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(dw_num_ntc) | Linhas/terminais distintos |
| `qtd_tipos_credito_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(cod_tipo_credito) | Diversidade de tipos (exclui sentinelas) |
| `qtd_status_plataforma_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(cod_status_plataforma) | Diversidade de plataformas |

### 4.6 Features de Valores Monetarios

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `sum_val_credito` | _m1, _m3, _m6 | double | SUM(val_credito_inserido_clean) | Soma de credito inserido (R$) | Media |
| `sum_val_bonus` | _m1, _m3, _m6 | double | SUM(val_bonus_clean) | Soma de bonus recebido (R$) | Baixa |
| `sum_val_real` | _m1, _m3, _m6 | double | SUM(val_real_clean) | Soma de valor real original (R$) | Media |
| `sum_val_real_ajustado` | _m1, _m3, _m6 | double | SUM(val_real_ajustado_clean) | Soma apos ajuste SOS+bonus (R$) | **Alta** |
| `avg_val_real` | _m1, _m3, _m6 | double | AVG(val_real_ajustado) WHERE valida | Media por recarga valida (R$) | Alta |
| `min_val_real` | _m1, _m3, _m6 | double | MIN(val_real_ajustado) WHERE valida | Menor valor de recarga (R$) | Media |
| `max_val_real` | _m1, _m3, _m6 | double | MAX(val_real_ajustado) WHERE valida | Maior valor de recarga (R$) | Media |
| `std_val_real` | _m1, _m3, _m6 | double | STDDEV(val_real_ajustado) WHERE valida | Desvio padrao dos valores (R$) | Alta |
| `ticket_medio` | _m1, _m3, _m6 | double | sum_val_real_ajustado / qtd_validas | Valor tipico por recarga (R$) | **Alta** |

### 4.7 Features de SOS (Estresse Financeiro)

| Variavel | Sufixos | Tipo | Agregacao / Formula | Descricao | Relevancia |
|----------|---------|------|---------------------|-----------|------------|
| `qtd_sos` | _m1, _m3, _m6 | int | SUM(1 WHERE flag_sos=1) | Quantidade de eventos SOS | Alta |
| `sum_valor_sos` | _m1, _m3, _m6 | double | SUM(valor_sos WHERE flag_sos=1) | Soma dos valores SOS (R$) | Alta |
| `flag_teve_sos` | _m1, _m3, _m6 | int | MAX(flag_sos) | 1 se usou SOS no periodo | Alta |
| `pct_sos_sobre_credito` | _m1, _m3, _m6 | double | (sum_valor_sos / sum_val_credito) * 100 | Proporcao SOS/Credito (%) | **Muito Alta** |
| `freq_sos` | _m1, _m3, _m6 | double | qtd_sos / qtd_recargas | Frequencia relativa de SOS | **Muito Alta** |

> **Insight de Negocio:** `freq_sos_m1` e `pct_sos_sobre_credito_m1` estao entre as features mais preditivas do bloco comportamental. Clientes com alta frequencia de SOS tem probabilidade significativamente maior de FPD.

### 4.8 Features de Tempo Entre Recargas

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `dias_medio_entre_recargas` | _m1, _m3, _m6 | double | AVG(dias_desde_recarga_anterior) | Media de dias entre recargas | **Alta** |
| `dias_min_entre_recargas` | _m1, _m3, _m6 | double | MIN(dias WHERE > 0) | Menor intervalo (exclui zero) | Media |
| `dias_max_entre_recargas` | _m1, _m3, _m6 | double | MAX(dias_desde_recarga_anterior) | Maior intervalo (periodos de inatividade) | **Alta** |
| `std_dias_entre_recargas` | _m1, _m3, _m6 | double | STDDEV(dias) | Irregularidade no padrao | Alta |

> **Regra de Calculo:** Usa Window Function com LAG sobre `ts_recarga` ordenado por timestamp. `dias_desde_recarga_anterior = DATEDIFF(ts_recarga, ts_recarga_anterior)`. Primeira recarga do historico nao tem valor anterior (NULL).

### 4.9 Features de Horario e Dia da Semana

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `qtd_recargas_madrugada` | _m1, _m3, _m6 | int | SUM(1 WHERE periodo='MADRUGADA') | Recargas entre 00h-05h | Media |
| `qtd_recargas_manha` | _m1, _m3, _m6 | int | SUM(1 WHERE periodo='MANHA') | Recargas entre 06h-11h | Baixa |
| `qtd_recargas_tarde` | _m1, _m3, _m6 | int | SUM(1 WHERE periodo='TARDE') | Recargas entre 12h-17h | Baixa |
| `qtd_recargas_noite` | _m1, _m3, _m6 | int | SUM(1 WHERE periodo='NOITE') | Recargas entre 18h-23h | Baixa |
| `qtd_recargas_fim_semana` | _m1, _m3, _m6 | int | SUM(flag_fim_semana) | Recargas em sabado/domingo | Media |
| `pct_recargas_fim_semana` | _m1, _m3, _m6 | double | (qtd_fim_semana / qtd_recargas) * 100 | % no fim de semana | Media |
| `pct_recargas_madrugada` | _m1, _m3, _m6 | double | (qtd_madrugada / qtd_recargas) * 100 | % na madrugada | Media |
| `qtd_semanas_com_recarga` | _m1, _m3, _m6 | int | COUNT_DISTINCT(semana_ano) | Semanas com atividade | Media |
| `recargas_por_semana` | _m1, _m3, _m6 | double | qtd_recargas_validas / (meses * 4.33) | Frequencia semanal | Media |
| `pct_semanas_com_recarga` | _m1, _m3, _m6 | double | (qtd_semanas / (meses * 4.33)) * 100 | Regularidade semanal (%) | Media |

### 4.10 Features de Tipo de Transacao

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_pago_puro` | _m1, _m3, _m6 | int | SUM(1 WHERE tipo='PAGO_PURO') | Recargas tradicionais |
| `qtd_bonus_puro` | _m1, _m3, _m6 | int | SUM(1 WHERE tipo='BONUS_PURO') | Apenas bonus |
| `qtd_combo` | _m1, _m3, _m6 | int | SUM(1 WHERE tipo='COMBO_PAGO_BONUS') | Recarga + bonus |
| `qtd_valor_negativo` | _m1, _m3, _m6 | int | SUM(1 WHERE tipo='VALOR_NEGATIVO') | Estornos |

### 4.11 Features Derivadas (Ratios e Coeficientes)

| Variavel | Sufixos | Tipo | Formula | Descricao | Relevancia |
|----------|---------|------|---------|-----------|------------|
| `coef_variacao_val` | _m1, _m3, _m6 | double | std_val_real / avg_val_real | Instabilidade de valores (CV) | **Alta** |
| `ratio_max_min_val` | _m1, _m3, _m6 | double | max_val / min_val | Amplitude relativa | Media |
| `ratio_bonus_credito` | _m1, _m3, _m6 | double | sum_bonus / sum_credito | Proporcao bonus/credito | Media |
| `val_liquido` | _m1, _m3, _m6 | double | sum_val_credito - sum_valor_sos | Credito liquido apos SOS (R$) | **Alta** |
| `pct_transacoes_validas` | _m1, _m3, _m6 | double | (qtd_validas / qtd_recargas) * 100 | % de transacoes validas | Baixa |

### 4.12 Flags de Cobertura e Atividade

| Variavel | Tipo | Condicao | Descricao |
|----------|------|----------|-----------|
| `flag_recarga_m1` | int | Tem dados na janela M1 | Cobertura: 56.12% |
| `flag_recarga_m3` | int | Tem dados na janela M3 | Cobertura: 66.86% |
| `flag_recarga_m6` | int | Tem dados na janela M6 | Cobertura: 68.92% |
| `flag_sem_recarga` | int | qtd_recargas = 0 | Sem atividade no periodo |
| `flag_baixa_atividade` | int | Abaixo do limiar por janela | Atividade insuficiente |

---

## 5. Book de Pagamento

### 5.1 Resumo do Bloco

| Atributo | Valor |
|----------|-------|
| **Fonte Silver** | `silver_pagamento` (transacional) |
| **Grao de Entrada** | 1 linha por transacao (fatura/item/pagamento) |
| **Grao de Saida** | 1 linha por NUM_CPF + SAFRA_PAGAMENTO (mensal) |
| **Safra derivada de** | DATE_FORMAT(ts_status_fatura, 'yyyyMM') |
| **Total de Features** | ~45 base x 3 janelas = ~135 variaveis |
| **Cobertura M1** | 16.13% |
| **Tabela Gold** | `gold_pagamento_features_v2` |
| **Relevancia** | Historico de atraso (juros), comportamento de negociacao (descontos) |

### 5.2 Regras de Negocio Embarcadas

| Regra | Criterio | Variavel Gerada | Significado |
|-------|----------|-----------------|-------------|
| **Atraso passado** | val_juros_pos > 0 | `flag_com_juros` | Cliente pagou juros = atrasou antes |
| **Sempre com juros** | pct_com_juros > 80% | `flag_sempre_com_juros` | Padrao cronico de atraso |
| **Alto desconto** | ratio_desconto/pago > 10% | `flag_alto_desconto` | Negociou descontos expressivos |
| **Alta multa** | (multa_equip + multa_fid) / pago > 5% | `flag_alta_multa` | Penalidades significativas |

> **Insight de Negocio:** `pct_pagamentos_com_juros_m1` e `flag_sempre_com_juros_m1` sao indicadores diretos de atraso passado — clientes que ja pagaram juros tem maior probabilidade de FPD na migracoes para plano controle.

### 5.3 Features de Volume

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_transacoes` | _m1, _m3, _m6 | int | COUNT(*) | Total de transacoes |
| `qtd_pagamentos_validos` | _m1, _m3, _m6 | int | SUM(flag_pagamento_valido) | Pagamentos com valor > 0 |
| `qtd_faturas_distintas` | _m1, _m3, _m6 | int | COUNT_DISTINCT(seq_fatura) | Faturas distintas pagas |
| `qtd_contratos_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(contrato) | Contratos distintos |

### 5.4 Features de Valores Pagos

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `sum_val_pago` | _m1, _m3, _m6 | double | SUM(val_pago) | Soma total pago (R$) | Media |
| `avg_val_pago` | _m1, _m3, _m6 | double | AVG(val_pago WHERE valido) | Media por pagamento (R$) | Media |
| `max_val_pago` | _m1, _m3, _m6 | double | MAX(val_pago) | Maior pagamento (R$) | Baixa |
| `min_val_pago` | _m1, _m3, _m6 | double | MIN(val_pago WHERE > 0) | Menor pagamento (R$) | Baixa |
| `std_val_pago` | _m1, _m3, _m6 | double | STDDEV(val_pago WHERE valido) | Desvio padrao (R$) | Media |
| `ticket_medio_pagamento` | _m1, _m3, _m6 | double | sum_val_pago / qtd_validos | Valor tipico por pagamento (R$) | Alta |

### 5.5 Features de Desconto (Comportamento de Negociacao)

| Variavel | Sufixos | Tipo | Agregacao / Formula | Descricao | Relevancia |
|----------|---------|------|---------------------|-----------|------------|
| `sum_val_desconto` | _m1, _m3, _m6 | double | SUM(val_desconto) | Total de descontos obtidos (R$) | Media |
| `qtd_com_desconto` | _m1, _m3, _m6 | int | SUM(flag_com_desconto) | Quantidade com desconto | Media |
| `avg_val_desconto` | _m1, _m3, _m6 | double | AVG(val_desconto WHERE flag=1) | Media por desconto (R$) | Baixa |
| `max_val_desconto` | _m1, _m3, _m6 | double | MAX(val_desconto) | Maior desconto (R$) | Baixa |
| `pct_pagamentos_com_desconto` | _m1, _m3, _m6 | double | (qtd_com_desconto / qtd_transacoes) * 100 | % com desconto | Media |
| `ratio_desconto_pago` | _m1, _m3, _m6 | double | sum_val_desconto / sum_val_pago | Proporcao desconto/pago | Media |

### 5.6 Features de Juros e Multas (Indicador de Atraso Passado)

> **Juros pagos indicam que o cliente atrasou pagamentos anteriores. Esta e a informacao mais valiosa do bloco Pagamento.**

| Variavel | Sufixos | Tipo | Agregacao / Formula | Descricao | Relevancia |
|----------|---------|------|---------------------|-----------|------------|
| `sum_val_juros_pos` | _m1, _m3, _m6 | double | SUM(val_juros_pos) | Juros/multas pagos (R$) | **Alta** |
| `sum_val_juros_neg` | _m1, _m3, _m6 | double | SUM(val_juros_neg_abs) | Juros negativos/estornos (R$) | Baixa |
| `qtd_com_juros` | _m1, _m3, _m6 | int | SUM(flag_com_juros) | Qtd transacoes com juros | Alta |
| `avg_val_juros` | _m1, _m3, _m6 | double | AVG(val_juros_pos WHERE flag=1) | Media dos juros (R$) | Media |
| `max_val_juros` | _m1, _m3, _m6 | double | MAX(val_juros_pos) | Maior juros pago (R$) | Media |
| `pct_pagamentos_com_juros` | _m1, _m3, _m6 | double | (qtd_com_juros / qtd_transacoes) * 100 | % com juros | **Alta** |
| `ratio_juros_pago` | _m1, _m3, _m6 | double | sum_val_juros_pos / sum_val_pago | Intensidade de juros | **Alta** |
| `sum_val_multa_equip` | _m1, _m3, _m6 | double | SUM(val_multa_equip_item) | Multa por equipamento (R$) | Media |
| `sum_val_multa_fid` | _m1, _m3, _m6 | double | SUM(val_multa_fid_item) | Multa por fidelidade (R$) | Media |

### 5.7 Features de Formas de Pagamento

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_formas_pagamento_distintas` | _m1, _m3, _m6 | int | COUNT_DISTINCT(cod_forma_pagamento) | Diversidade de formas |
| `sum_pago_forma_01` | _m1, _m3, _m6 | double | SUM(val_pago WHERE forma='01') | Valor pago forma 01 (R$) |
| `sum_pago_forma_02` | _m1, _m3, _m6 | double | SUM(val_pago WHERE forma='02') | Valor pago forma 02 (R$) |
| `sum_pago_forma_03` | _m1, _m3, _m6 | double | SUM(val_pago WHERE forma='03') | Valor pago forma 03 (R$) |
| `sum_pago_forma_missing` | _m1, _m3, _m6 | double | SUM(val_pago WHERE forma IS NULL) | Forma nao informada (R$) |
| `pct_forma_dominante` | _m1, _m3, _m6 | double | MAX(forma_01..03,missing) / sum_pago * 100 | Concentracao na forma principal (%) |
| `qtd_metodos_pagamento_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(cod_metodo_pagamento) | Diversidade de metodos |

### 5.8 Features de Status de Pagamento

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_status_p` | _m1, _m3, _m6 | int | SUM(1 WHERE status='P') | Status "Pago" |
| `qtd_status_r` | _m1, _m3, _m6 | int | SUM(1 WHERE status='R') | Status "Rejeitado" |
| `qtd_status_c` | _m1, _m3, _m6 | int | SUM(1 WHERE status='C') | Status "Cancelado" |
| `qtd_status_b` | _m1, _m3, _m6 | int | SUM(1 WHERE status='B') | Status "Baixado" |
| `qtd_status_pag_missing` | _m1, _m3, _m6 | int | SUM(1 WHERE flag_missing=1) | Status nao informado |

### 5.9 Features Derivadas

| Variavel | Sufixos | Tipo | Formula | Descricao | Relevancia |
|----------|---------|------|---------|-----------|------------|
| `coef_variacao_pagamento` | _m1, _m3, _m6 | double | std_val_pago / avg_val_pago | Instabilidade de valores | Media |
| `val_liquido_pago` | _m1, _m3, _m6 | double | sum_val_pago - sum_val_desconto | Pago liquido (R$) | Media |

### 5.10 Flags de Cobertura e Comportamento

| Variavel | Tipo | Condicao | Descricao |
|----------|------|----------|-----------|
| `flag_pagamento_m1` | int | Tem dados na janela M1 | Cobertura: 16.13% |
| `flag_pagamento_m3` | int | Tem dados na janela M3 | - |
| `flag_pagamento_m6` | int | Tem dados na janela M6 | - |
| `flag_sem_pagamento` | int | qtd_pagamentos_validos = 0 | Sem pagamentos no periodo |
| `flag_sempre_com_juros` | int | pct_com_juros > 80% | **Padrao cronico de atraso** |
| `flag_alto_desconto` | int | ratio_desconto/pago > 10% | Negociacao expressiva |
| `flag_baixo_volume_pagamento` | int | qtd_validos < 2 | Atividade insuficiente |
| `flag_alta_multa` | int | (multa_equip + multa_fid) / pago > 5% | Penalidades significativas |

---

## 6. Book de Atraso

### 6.1 Resumo do Bloco

| Atributo | Valor |
|----------|-------|
| **Fonte Silver** | `silver_atraso` (snapshot mensal) |
| **Grao de Entrada** | Multiplas linhas por CPF (1 por fatura/estado) |
| **Grao de Saida** | 1 linha por NUM_CPF + SAFRA_ATRASO (mensal) |
| **Safra derivada de** | DATE_FORMAT(ts_referencia, 'yyyyMM') |
| **Dedup** | Sem dedup — grain e multiplo por design (cada CPF pode ter faturas em diferentes estados de aging simultaneamente) |
| **Total de Features** | ~54 base x 3 janelas = ~162 variaveis |
| **Cobertura M1** | 21.79% |
| **Tabela Gold** | `gold_atraso_features_v2` |
| **Relevancia** | Risco atual (aging), historico de perda (WO/PDD), fraude, recuperacao |

### 6.2 Classificacao de Aging (Faixas de Atraso)

> **Aging indica ha quantos dias a fatura esta em aberto. Quanto maior o aging, maior o risco de perda.**

| Bucket | Faixa Origem | Dias em Aberto | Severidade |
|--------|-------------|----------------|------------|
| `0_30` | "0-30 dias" | 0 a 30 | Baixa |
| `31_60` | "31-60 dias" | 31 a 60 | Media |
| `61_90` | "61-90 dias" | 61 a 90 | Alta |
| `90_plus` | ">90 dias" | Mais de 90 | **Muito Alta** |
| `missing` | Outros valores | Indeterminado | - |

> `flag_fatura_aberta = 1` quando `val_fat_aberto > 0`

### 6.3 Regras de Negocio — Indicadores Binarios

Os indicadores originais vem como texto variado na Silver. A conversao para binario segue:

```
Valores que geram 1: '1', 'S', 'Y', 'SIM', 'YES', 'TRUE' (case-insensitive)
Todos os demais valores: 0
Se coluna nao existe no DataFrame: 0 (fallback seguro)
```

| Indicador | Significado | Relevancia |
|-----------|-------------|------------|
| `ind_wo` | Write-Off (perda contabil) | **Muito Alta** |
| `ind_pdd` | Provisao para Devedores Duvidosos | **Muito Alta** |
| `ind_fraude` | Indicador de fraude | **Muito Alta** |
| `ind_aca` | Acordo de pagamento | Media |
| `ind_pccr` | Programa de recuperacao de credito | Media |

### 6.4 Regras de Negocio — Flags Compostos

| Flag | Criterio | Significado |
|------|----------|-------------|
| `flag_atraso_grave` | pct_aging_90_plus > 50% | Mais da metade das faturas em atraso grave |
| `flag_risco_alto` | WO = 1 OR PDD = 1 OR Fraude = 1 | Qualquer indicador de perda/fraude |
| `flag_em_recuperacao` | ACA = 1 OR PCCR = 1 | Em processo de negociacao/acordo |
| `flag_alto_valor_aberto` | sum_val_aberto > R$500 | Divida expressiva |
| `flag_muitas_faturas_abertas` | qtd_faturas_abertas > 3 | Multiplos debitos simultaneos |
| `flag_concentrado_aging_grave` | pct_val_aging_90_plus > 70% | Valor concentrado em atraso grave |

### 6.5 Features de Faturas Abertas

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_registros` | _m1, _m3, _m6 | int | COUNT(*) | Total de registros (snapshot) |
| `qtd_faturas_abertas` | _m1, _m3, _m6 | int | SUM(flag_fatura_aberta) | Faturas com valor em aberto |
| `qtd_contratos_com_atraso` | _m1, _m3, _m6 | int | COUNT_DISTINCT(contrato WHERE aberto > 0) | Contratos com divida |

### 6.6 Features de Valores em Aberto

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `sum_val_aberto` | _m1, _m3, _m6 | double | SUM(val_aberto) | Valor total em aberto (R$) | **Alta** |
| `avg_val_aberto` | _m1, _m3, _m6 | double | AVG(val_aberto WHERE > 0) | Media por fatura (R$) | Media |
| `max_val_aberto` | _m1, _m3, _m6 | double | MAX(val_aberto) | Maior fatura em aberto (R$) | Media |
| `min_val_aberto` | _m1, _m3, _m6 | double | MIN(val_aberto WHERE > 0) | Menor fatura em aberto (R$) | Baixa |
| `std_val_aberto` | _m1, _m3, _m6 | double | STDDEV(val_aberto WHERE > 0) | Desvio padrao (R$) | Media |
| `ticket_medio_aberto` | _m1, _m3, _m6 | double | sum_val_aberto / qtd_faturas_abertas | Valor medio por fatura (R$) | Media |

### 6.7 Features de Faturamento

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `sum_val_fat_bruto` | _m1, _m3, _m6 | double | SUM(val_bruto) | Faturamento bruto (R$) |
| `sum_val_fat_liquido` | _m1, _m3, _m6 | double | SUM(val_liquido) | Faturamento liquido (R$) |
| `sum_val_pagamento` | _m1, _m3, _m6 | double | SUM(val_pagamento) | Pagamentos realizados (R$) |
| `sum_val_multa_juros` | _m1, _m3, _m6 | double | SUM(val_multa_juros) | Multas e juros (R$) |
| `ratio_aberto_faturado` | _m1, _m3, _m6 | double | sum_val_aberto / sum_val_fat_bruto | Taxa de inadimplencia | **Alta** |
| `ratio_pagamento_faturado` | _m1, _m3, _m6 | double | sum_val_pagamento / sum_val_fat_bruto | Taxa de adimplencia | Alta |

### 6.8 Features de Aging — Quantidade

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `qtd_aging_0_30` | _m1, _m3, _m6 | int | SUM(1 WHERE bucket='0_30') | Faturas 0-30 dias | Baixa |
| `qtd_aging_31_60` | _m1, _m3, _m6 | int | SUM(1 WHERE bucket='31_60') | Faturas 31-60 dias | Media |
| `qtd_aging_61_90` | _m1, _m3, _m6 | int | SUM(1 WHERE bucket='61_90') | Faturas 61-90 dias | Alta |
| `qtd_aging_90_plus` | _m1, _m3, _m6 | int | SUM(1 WHERE bucket='90_plus') | Faturas >90 dias | **Muito Alta** |
| `qtd_aging_missing` | _m1, _m3, _m6 | int | SUM(1 WHERE bucket='missing') | Faixa nao informada | Baixa |
| `pct_aging_0_30` | _m1, _m3, _m6 | double | (qtd_0_30 / qtd_faturas_abertas) * 100 | % em 0-30 dias | Baixa |
| `pct_aging_31_60` | _m1, _m3, _m6 | double | (qtd_31_60 / qtd_faturas_abertas) * 100 | % em 31-60 dias | Media |
| `pct_aging_61_90` | _m1, _m3, _m6 | double | (qtd_61_90 / qtd_faturas_abertas) * 100 | % em 61-90 dias | Alta |
| `pct_aging_90_plus` | _m1, _m3, _m6 | double | (qtd_90_plus / qtd_faturas_abertas) * 100 | % em >90 dias | **Muito Alta** |

### 6.9 Features de Aging — Valores

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `sum_val_aging_0_30` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE bucket='0_30') | Valor em 0-30 dias (R$) |
| `sum_val_aging_31_60` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE bucket='31_60') | Valor em 31-60 dias (R$) |
| `sum_val_aging_61_90` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE bucket='61_90') | Valor em 61-90 dias (R$) |
| `sum_val_aging_90_plus` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE bucket='90_plus') | Valor em >90 dias (R$) |
| `pct_val_aging_90_plus` | _m1, _m3, _m6 | double | (sum_val_90_plus / sum_val_aberto) * 100 | % do valor em >90 dias | **Muito Alta** |

### 6.10 Indicadores de Risco

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `flag_teve_wo` | _m1, _m3, _m6 | int | MAX(ind_wo) | Write-Off no periodo | **Muito Alta** |
| `flag_teve_pdd` | _m1, _m3, _m6 | int | MAX(ind_pdd) | Provisao PDD | **Muito Alta** |
| `flag_teve_fraude` | _m1, _m3, _m6 | int | MAX(ind_fraude) | Indicador de fraude | **Muito Alta** |
| `qtd_com_wo` | _m1, _m3, _m6 | int | SUM(1 WHERE ind_wo=1) | Quantidade com WO | Alta |
| `qtd_com_pdd` | _m1, _m3, _m6 | int | SUM(1 WHERE ind_pdd=1) | Quantidade com PDD | Alta |
| `qtd_com_fraude` | _m1, _m3, _m6 | int | SUM(1 WHERE ind_fraude=1) | Quantidade com fraude | Alta |
| `sum_val_wo` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE ind_wo=1) | Valor em WO (R$) | Alta |
| `sum_val_pdd` | _m1, _m3, _m6 | double | SUM(val_aberto WHERE ind_pdd=1) | Valor em PDD (R$) | Alta |
| `pct_val_wo_sobre_aberto` | _m1, _m3, _m6 | double | (sum_val_wo / sum_val_aberto) * 100 | % WO / Aberto | Alta |

### 6.11 Indicadores de Recuperacao

| Variavel | Sufixos | Tipo | Agregacao | Descricao | Relevancia |
|----------|---------|------|-----------|-----------|------------|
| `flag_teve_aca` | _m1, _m3, _m6 | int | MAX(ind_aca) | Acordo de pagamento | Media |
| `flag_teve_pccr` | _m1, _m3, _m6 | int | MAX(ind_pccr) | Programa de recuperacao | Media |
| `qtd_com_aca` | _m1, _m3, _m6 | int | SUM(1 WHERE ind_aca=1) | Quantidade com ACA | Media |
| `qtd_com_pccr` | _m1, _m3, _m6 | int | SUM(1 WHERE ind_pccr=1) | Quantidade com PCCR | Media |

### 6.12 Features Derivadas

| Variavel | Sufixos | Tipo | Formula | Descricao |
|----------|---------|------|---------|-----------|
| `coef_variacao_aberto` | _m1, _m3, _m6 | double | std_val_aberto / avg_val_aberto | Instabilidade dos valores em aberto |

### 6.13 Features de Diversidade

| Variavel | Sufixos | Tipo | Agregacao | Descricao |
|----------|---------|------|-----------|-----------|
| `qtd_tipos_cliente_distintos` | _m1, _m3, _m6 | int | COUNT_DISTINCT(dw_tipo_cliente_conta) | Diversidade de tipos de cliente |
| `qtd_plataformas_distintas` | _m1, _m3, _m6 | int | COUNT_DISTINCT(cod_plataforma) | Diversidade de plataformas |
| `qtd_faixas_tempo_base_mes` | _m1, _m3, _m6 | int | COUNT_DISTINCT(dw_faixa_tempo_base) | Diversidade de faixas tempo-base |

### 6.14 Flags de Cobertura e Comportamento

| Variavel | Tipo | Condicao | Descricao |
|----------|------|----------|-----------|
| `flag_atraso_m1` | int | Tem dados na janela M1 | Cobertura: 21.79% |
| `flag_atraso_m3` | int | Tem dados na janela M3 | - |
| `flag_atraso_m6` | int | Tem dados na janela M6 | - |
| `flag_sem_atraso` | int | qtd_faturas_abertas = 0 | Sem faturas abertas |
| `flag_atraso_grave` | int | pct_aging_90_plus > 50% | **Concentracao em atraso grave** |
| `flag_risco_alto` | int | WO OR PDD OR Fraude | **Qualquer indicador de perda** |
| `flag_em_recuperacao` | int | ACA OR PCCR | Em negociacao/acordo |
| `flag_alto_valor_aberto` | int | sum_val_aberto > R$500 | Divida expressiva |
| `flag_muitas_faturas_abertas` | int | qtd_faturas > 3 | Multiplos debitos |
| `flag_concentrado_aging_grave` | int | pct_val_aging_90_plus > 70% | **Valor concentrado em 90+** |

---

## 7. Cobertura por Bloco

### 7.1 Cobertura na Janela M1

| Bloco | Cobertura M1 | Registros com Dados | Interpretacao |
|-------|-------------|---------------------|---------------|
| Recarga | 56.12% | ~2.13M de 3.80M | Maioria dos clientes pre-pago tem recargas |
| Pagamento | 16.13% | ~0.61M de 3.80M | Minoria tem historico de pagamento faturado |
| Atraso | 21.79% | ~0.83M de 3.80M | Subconjunto com faturas em aberto |

### 7.2 Interpretacao da Cobertura

A cobertura reflete a **natureza da populacao**: clientes pre-pagos que podem migrar para plano controle. A maioria:
- **Tem recargas** (56%) — comportamento ativo de pre-pago
- **Nao tem pagamentos** (84%) — nunca tiveram fatura/plano pos-pago
- **Nao tem atraso** (78%) — nunca tiveram divida registrada

> Ausencia de dados de pagamento/atraso e, em si, uma informacao relevante — indica cliente "limpo" sem historico no sistema de cobranca.

---

## 8. Contribuicao ao Modelo (KS Incremental)

### 8.1 Evolucao por Bloco

| # | Bloco | ABT | Features | KS OOT (%) | Delta (p.p.) |
|---|-------|-----|----------|------------|--------------|
| 1 | Score_01 (bureau) | v1 | 1 | 26.67 | baseline |
| 2 | + Score_02 | v2 | 2 | 31.25 | +4.58 |
| 3 | + Telco | v3 | 89 | 31.51 | +0.26 |
| 4 | + Cadastro | v4 | 95 | 31.70 | +0.19 |
| 5 | **+ Recarga** | v5 | 160 | 33.95 | **+2.25** |
| 6 | **+ Pagamento + Atraso** | v6 | 261 | **34.39** | **+0.44** |

**Benchmark:** 33.10% | **Modelo Final:** 34.39% (+1.29 p.p.)

### 8.2 Interpretacao

- **Recarga** e o bloco comportamental com maior contribuicao incremental (+2.25 p.p.). As features de SOS e regularidade capturam sinais de estresse financeiro que os scores de bureau nao enxergam.
- **Pagamento + Atraso** adicionam +0.44 p.p. — contribuicao menor em termos absolutos, mas refinam a discriminacao nos decis intermediarios do modelo.
- O valor das features comportamentais esta na **combinacao**: individualmente cada variavel tem IV baixo (0.01-0.04), mas em conjunto contribuem +2.69 p.p. ao KS.

### 8.3 Features Selecionadas por Bloco (IV > 0.01)

| Bloco | Features Selecionadas | IV Medio |
|-------|----------------------|----------|
| Recarga | 74 | 0.0339 |
| Pagamento | 56 | 0.0107 |
| Atraso | 19 | 0.0086 |
| **Total Comportamental** | **149** | - |

---

## 9. Combinacoes de Features e Insights de Dominio

### 9.1 Padroes de Alto Risco

| Combinacao | Variaveis | Interpretacao | Risco |
|------------|-----------|---------------|-------|
| Estresse financeiro severo | Alto `freq_sos_m1` + Baixo `ticket_medio_m1` | Recargas pequenas com emprestimos frequentes | **Muito Alto** |
| Historico de perda | Alto `pct_aging_90_plus_m1` + `flag_teve_wo_m1 = 1` | Dividas antigas com perda contabil | **Muito Alto** |
| Padrao cronico de atraso | Alto `pct_pagamentos_com_juros_m1` + Alto `sum_val_aberto_m1` | Sempre paga com juros + divida ativa | **Muito Alto** |
| Irregularidade + SOS | Alto `coef_variacao_val_m1` + Alto `pct_sos_sobre_credito_m1` | Valores erraticos + dependencia de emprestimo | Alto |
| Madrugada + estresse | Alto `pct_recargas_madrugada_m1` + `flag_teve_sos_m1 = 1` | Comportamento atipico + estresse financeiro | Alto |

### 9.2 Padroes de Baixo Risco

| Combinacao | Variaveis | Interpretacao | Risco |
|------------|-----------|---------------|-------|
| Cliente estavel | Baixo `coef_variacao_val_m1` + Alto `ticket_medio_m1` | Recargas regulares de bom valor | Baixo |
| Recuperacao ativa | `flag_em_recuperacao_m1 = 1` + Baixo `ratio_aberto_faturado_m1` | Negociando + reduzindo divida | Medio |
| Sem historico negativo | `flag_sem_atraso_m6 = 1` + `flag_sem_pagamento_m6 = 1` | Nunca entrou no sistema de cobranca | Baixo |

---

## 10. Glossario

| Termo | Definicao |
|-------|-----------|
| **ABT** | Analytical Base Table — tabela analitica final usada para treino/scoring |
| **Aging** | Tempo (em dias) que uma fatura esta em aberto/atraso |
| **ACA** | Acordo de pagamento — negociacao para quitar divida |
| **Cobertura** | Percentual de registros na ABT que possuem dados nao-nulos para o bloco |
| **CV (Coef. Variacao)** | Desvio padrao / Media — mede instabilidade relativa |
| **FPD** | First Payment Default — inadimplencia no primeiro pagamento |
| **IV** | Information Value — mede poder preditivo de uma variavel (> 0.01 = relevante) |
| **KS** | Kolmogorov-Smirnov — metrica de discriminacao entre bons e maus |
| **M1/M3/M6** | Janelas temporais de 1, 3 e 6 meses retroativos |
| **OOT** | Out-of-Time — amostra de validacao temporal (safras 202502/202503) |
| **PCCR** | Programa de recuperacao de credito |
| **PDD** | Provisao para Devedores Duvidosos |
| **SAFRA** | Mes de referencia do registro (formato YYYYMM) |
| **SOS** | Emprestimo emergencial de R$3-20, descontado na proxima recarga |
| **WO (Write-Off)** | Perda contabil — divida considerada irrecuperavel |

---

## Referencias

| Documento | Localizacao |
|-----------|-------------|
| Book Completo ABT v6 (todas as variaveis) | `docs/04_gold_rules/BOOK_VARIABLES_ABT_V6.md` |
| Script Recarga | `src/jobs/02_gold/gold_recarga_features_v2.py` |
| Script Pagamento | `src/jobs/02_gold/gold_pagamento_features_v2.py` |
| Script Atraso | `src/jobs/02_gold/gold_atraso_features_v2.py` |
| ABT v6 Builder | `src/jobs/02_gold/05_gold_abt_v6_builder_v2.py` |
| Estudo Publico-Alvo (Entregavel A) | `banca_final/ESTUDO_PUBLICO_ALVO.md` |
| Monitoramento Modelo (Entregavel F) | `banca_final/PLANO_MONITORAMENTO_MODELO.md` |
| Preparacao ABT Resumida | `docs/04_gold_rules/PREPARACAO_ABT_RESUMIDA.md` |
