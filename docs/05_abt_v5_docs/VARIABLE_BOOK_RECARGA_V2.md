# Variable Book — Gold Recarga Features v2

## Metadados

| Atributo | Valor |
|----------|-------|
| **Versão** | v2.0 |
| **Script** | `src/jobs/02_gold/gold_recarga_features_v2.py` |
| **Grão** | NUM_CPF + SAFRA_RECARGA (cliente-mês) |
| **Total de Features** | 60+ |
| **Fonte** | Silver Recarga (event-level) |

---

## Chaves e Identificadores

| # | Variável | Tipo | Descrição | Exemplo |
|---|----------|------|-----------|---------|
| 1 | `num_cpf` | string | CPF do cliente (chave primária) | "12345678901" |
| 2 | `safra_recarga` | string | Mês de referência (YYYYMM) | "202411" |
| 3 | `dt_recarga_safra` | date | Primeiro dia do mês | 2024-11-01 |

---

## Features de Volume

| # | Variável | Tipo | Descrição | Range Típico |
|---|----------|------|-----------|--------------|
| 4 | `qtd_recargas_mes` | int | Total de eventos de recarga no mês | 0 - 50+ |
| 5 | `qtd_recargas_validas_mes` | int | Recargas excluindo ZERO_TOTAL | 0 - 50+ |
| 6 | `qtd_telefones_distintos_mes` | int | Quantidade de linhas/terminais distintos | 1 - 5 |

---

## Features de Valores Brutos

| # | Variável | Tipo | Unidade | Descrição |
|---|----------|------|---------|-----------|
| 7 | `sum_val_credito_mes` | double | R$ | Soma de crédito inserido |
| 8 | `sum_val_bonus_mes` | double | R$ | Soma de bônus recebido |
| 9 | `sum_val_real_mes` | double | R$ | Soma de valor real (original) |
| 10 | `sum_val_real_ajustado_mes` | double | R$ | Soma após ajuste SOS + bônus |

---

## Features de Estatísticas de Valor

| # | Variável | Tipo | Unidade | Descrição | Interpretação |
|---|----------|------|---------|-----------|---------------|
| 11 | `avg_val_real_mes` | double | R$ | Média do valor real por recarga | Ticket médio |
| 12 | `min_val_real_mes` | double | R$ | Menor valor de recarga válida | Comportamento mínimo |
| 13 | `max_val_real_mes` | double | R$ | Maior valor de recarga válida | Capacidade máxima |
| 14 | `std_val_real_mes` | double | R$ | Desvio padrão dos valores | Consistência |
| 15 | `ticket_medio_mes` | double | R$ | sum_val_real_ajustado / qtd_recargas_validas | Valor típico |

---

## Features de SOS (Indicador de Estresse Financeiro)

| # | Variável | Tipo | Descrição | Interpretação para Risco |
|---|----------|------|-----------|--------------------------|
| 16 | `qtd_sos_mes` | int | Quantidade de eventos com SOS | Alto = necessidade frequente |
| 17 | `sum_valor_sos_mes` | double | Soma dos valores SOS (R$) | Volume de empréstimos |
| 18 | `flag_teve_sos_mes` | int | 1 se usou SOS no mês, 0 caso contrário | Indicador binário |
| 19 | `pct_sos_sobre_credito_mes` | double | (sum_sos / sum_credito) * 100 | **Alto % = alto risco** |
| 20 | `freq_sos_mes` | double | qtd_sos / qtd_recargas | **Alto = dependência** |

---

## Features de Tempo Entre Recargas

| # | Variável | Tipo | Unidade | Descrição | Interpretação |
|---|----------|------|---------|-----------|---------------|
| 21 | `dias_medio_entre_recargas_mes` | double | dias | Média de dias entre recargas consecutivas | Regularidade |
| 22 | `dias_min_entre_recargas_mes` | double | dias | Menor intervalo entre recargas | Urgência |
| 23 | `dias_max_entre_recargas_mes` | double | dias | Maior intervalo entre recargas | Inatividade |
| 24 | `std_dias_entre_recargas_mes` | double | dias | Desvio padrão do intervalo | Previsibilidade |

---

## Features de Recência

| # | Variável | Tipo | Descrição |
|---|----------|------|-----------|
| 25 | `dt_ultima_recarga_mes` | date | Data da última recarga no mês |
| 26 | `dt_primeira_recarga_mes` | date | Data da primeira recarga no mês |

---

## Features de Padrão de Horário

| # | Variável | Tipo | Descrição | Interpretação |
|---|----------|------|-----------|---------------|
| 27 | `qtd_recargas_madrugada_mes` | int | Recargas entre 00h-05h | Comportamento atípico |
| 28 | `qtd_recargas_manha_mes` | int | Recargas entre 06h-11h | Padrão matutino |
| 29 | `qtd_recargas_tarde_mes` | int | Recargas entre 12h-17h | Padrão vespertino |
| 30 | `qtd_recargas_noite_mes` | int | Recargas entre 18h-23h | Padrão noturno |
| 31 | `qtd_recargas_fim_semana_mes` | int | Recargas em sábado/domingo | Perfil de lazer |
| 32 | `pct_recargas_fim_semana_mes` | double | % de recargas no fim de semana | Perfil comportamental |
| 33 | `pct_recargas_madrugada_mes` | double | % de recargas na madrugada | **Alto = alerta** |

---

## Features de Frequência e Semanas

| # | Variável | Tipo | Descrição | Interpretação |
|---|----------|------|-----------|---------------|
| 34 | `qtd_semanas_com_recarga_mes` | int | Semanas distintas com pelo menos 1 recarga | Consistência |
| 35 | `recargas_por_semana_mes` | double | qtd_recargas_validas / 4.33 | Intensidade |
| 36 | `pct_semanas_com_recarga_mes` | double | (semanas_ativas / 4.33) * 100 | Regularidade |

---

## Features de Tipo de Transação

| # | Variável | Tipo | Descrição | Interpretação |
|---|----------|------|-----------|---------------|
| 37 | `qtd_pago_puro_mes` | int | Recargas tradicionais (só crédito) | Comportamento padrão |
| 38 | `qtd_bonus_puro_mes` | int | Apenas bônus (promoção) | Dependência de promoções |
| 39 | `qtd_combo_mes` | int | Recarga + bônus | Busca de vantagens |
| 40 | `qtd_valor_negativo_mes` | int | Estornos ou ajustes | Problemas potenciais |

---

## Features de Padrões Derivados

| # | Variável | Tipo | Fórmula | Interpretação |
|---|----------|------|---------|---------------|
| 41 | `coef_variacao_val_mes` | double | std_val / avg_val | **Alto = instabilidade** |
| 42 | `ratio_max_min_val_mes` | double | max_val / min_val | Amplitude comportamental |
| 43 | `ratio_bonus_credito_mes` | double | sum_bonus / sum_credito | Dependência de promoções |
| 44 | `val_liquido_mes` | double | sum_credito - sum_sos | Valor real efetivo |
| 45 | `pct_transacoes_validas_mes` | double | (qtd_validas / qtd_total) * 100 | Qualidade das transações |

---

## Features de Diversidade (Dimensões)

| # | Variável | Tipo | Descrição |
|---|----------|------|-----------|
| 46 | `qtd_tipos_credito_distintos_mes` | int | Tipos de crédito diferentes usados |
| 47 | `qtd_status_plataforma_distintos_mes` | int | Status de plataforma distintos |

---

## Flags de Cobertura

| # | Variável | Tipo | Condição | Uso |
|---|----------|------|----------|-----|
| 48 | `flag_sem_recarga_mes` | int | qtd_recargas = 0 | Missing indicator |
| 49 | `flag_baixa_atividade_mes` | int | qtd_recargas_validas < 2 | Alerta de inatividade |

---

## Metadados do Gold

| # | Variável | Tipo | Descrição |
|---|----------|------|-----------|
| 50 | `gold_version` | string | Versão do script (gold_recarga_features_v2) |
| 51 | `gold_build_date` | timestamp | Data/hora de geração |

---

## Valores Especiais

### Sentinelas (tratados na Silver)
| Código | Significado | Tratamento |
|--------|-------------|------------|
| -1 | Não se aplica | Flag sentinela |
| -2 | Não determinado | Flag sentinela |
| -3 | Não informado | Flag sentinela |

### Valores Nulos
| Cenário | Tratamento |
|---------|------------|
| Cliente sem recarga no mês | `flag_sem_recarga_mes = 1`, features = 0 ou NULL |
| Divisão por zero | NULL (não 0) |
| Cálculo de média/std com < 2 valores | NULL |

---

## Features Mais Relevantes para Risco de Crédito

### Top 10 Features (sugestão baseada em domínio)

| Rank | Feature | Justificativa |
|------|---------|---------------|
| 1 | `freq_sos_mes` | **Indicador direto de estresse financeiro** |
| 2 | `pct_sos_sobre_credito_mes` | Proporção de empréstimos sobre crédito |
| 3 | `ticket_medio_mes` | Capacidade de pagamento típica |
| 4 | `dias_medio_entre_recargas_mes` | Regularidade de comportamento |
| 5 | `coef_variacao_val_mes` | Estabilidade financeira |
| 6 | `qtd_recargas_validas_mes` | Nível de atividade |
| 7 | `dias_max_entre_recargas_mes` | Períodos de inatividade (risco) |
| 8 | `sum_val_real_ajustado_mes` | Volume financeiro real |
| 9 | `pct_recargas_madrugada_mes` | Comportamento atípico |
| 10 | `ratio_bonus_credito_mes` | Dependência de promoções |

### Combinações Úteis

| Combinação | Interpretação |
|------------|---------------|
| Alto `freq_sos` + Baixo `ticket_medio` | Estresse financeiro severo |
| Alto `dias_max_entre_recargas` + Baixo `qtd_recargas` | Cliente em risco de churn |
| Alto `coef_variacao_val` + Alto `qtd_valor_negativo` | Comportamento inconsistente |
| Alto `pct_recargas_madrugada` + Alto `freq_sos` | Perfil de alto risco |

---

## Uso para Janelas Temporais (M1/M3/M6)

Para criar features por janela temporal no ABT v5:

```sql
-- M1 (último mês antes da safra)
SELECT
    spine.num_cpf,
    spine.safra,
    SUM(rec.qtd_recargas_mes) as qtd_recargas_m1,
    AVG(rec.ticket_medio_mes) as ticket_medio_m1,
    MAX(rec.flag_teve_sos_mes) as flag_teve_sos_m1
FROM spine
LEFT JOIN recarga_features rec
    ON spine.num_cpf = rec.num_cpf
    AND rec.safra_recarga >= add_months(spine.dt_safra, -1)
    AND rec.safra_recarga < spine.safra
GROUP BY spine.num_cpf, spine.safra
```

As features mensais (`*_mes`) podem ser somadas ou agregadas conforme a janela desejada.

---

## Validações Recomendadas

| Gate | Condição | Critério |
|------|----------|----------|
| 1 | Unicidade | 1:1 por num_cpf + safra_recarga |
| 2 | Valores sensatos | avg_val_real >= 0 |
| 3 | SOS coerente | freq_sos <= 1.0 |
| 4 | Percentuais | 0 <= pct_* <= 100 |
| 5 | Datas | dt_primeira <= dt_ultima |

---

## Changelog

| Versão | Data | Autor | Alterações |
|--------|------|-------|------------|
| v2.0 | 2026-01-29 | Claude Code | Versão inicial com 50+ features |
