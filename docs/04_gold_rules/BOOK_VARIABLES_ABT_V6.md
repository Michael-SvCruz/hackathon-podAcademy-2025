# Book of Variables - ABT v6 v2

## Metadados do Documento

| Atributo | Valor |
|----------|-------|
| **Versao** | v6.2 |
| **ABT** | `gold_abt_v6_v2` |
| **Registros** | 3,795,310 |
| **Colunas Totais** | 614 |
| **Grao** | NUM_CPF + SAFRA (cliente-mes, 1:1) |
| **Data Atualizacao** | 2026-01-29 |

---

## Sumario de Feature Blocks

| Bloco | Versao ABT | Variaveis | Cobertura M1 | Descricao |
|-------|------------|-----------|--------------|-----------|
| Score | v1, v2 | 2 + flags | 98.2% / 99.95% | Scores de bureau |
| Telco | v3 | 68 | 35.46% | Variaveis anonimas telco |
| Cadastro | v4 | ~33 | - | Dados cadastrais |
| Recarga | v5 | ~126 (42 x 3 janelas) | 56.12% | Comportamento de recarga |
| Pagamento | v6 | ~135 (45 x 3 janelas) | 16.13% | Historico de pagamentos |
| Atraso | v6 | ~162 (54 x 3 janelas) | 21.79% | Faturas em atraso |

---

## 1. Chaves e Identificadores

| # | Variavel | Tipo | Descricao | Regra |
|---|----------|------|-----------|-------|
| 1 | `num_cpf` | string | CPF do cliente (hash/ofuscado) | Chave primaria, nunca NULL |
| 2 | `safra` | string | Mes de referencia (YYYYMM) | Chave primaria, formato 6 digitos |
| 3 | `dt_safra` | date | Primeiro dia da safra | Derivado de SAFRA |

---

## 2. Labels e Metadados de Processo

> **ATENCAO ANTI-LEAKAGE**: Estas variaveis NAO devem ser usadas como features.

| # | Variavel | Tipo | Descricao | Regra de Uso |
|---|----------|------|-----------|--------------|
| 4 | `fpd_int` | int | First Payment Default (0/1) | **TARGET** - Nunca usar como feature |
| 5 | `flag_instalacao_int` | int | Decisao de aprovacao (0/1) | **METADADO** - Usar apenas para swap analysis |
| 6 | `prod` | string | Produto (CMV, DTH, NET) | Metadado de populacao |
| 7 | `flag_mig2` | string | Segmento (PRE, FLEX, Aquisicao) | Metadado de populacao |

**Observacoes:**
- `fpd_int = 1` indica default no primeiro pagamento
- `fpd_int` so e observado quando `flag_instalacao_int = 1`
- Treino: apenas registros com `flag_instalacao_int = 1`

---

## 3. Bloco Score (v1/v2)

### 3.1 Features de Score

| # | Variavel | Tipo | Range | Descricao | Relevancia |
|---|----------|------|-------|-----------|------------|
| 8 | `score_01` | int | 0-778 | Score de bureau 1 | Alta - baseline |
| 9 | `score_02` | int | 1-917 | Score de bureau 2 | Alta - incremental |

### 3.2 Flags de Missing

| # | Variavel | Tipo | Condicao | Uso |
|---|----------|------|----------|-----|
| 10 | `flag_score_01_missing` | int | score_01 = 0 ou NULL | Sentinela |
| 11 | `flag_score_02_missing` | int | score_02 = NULL | Missing indicator |

**Notas:**
- Score_01 = 0 e tratado como sentinela (missing codificado)
- Cobertura Score_01: 98.18%
- Cobertura Score_02: 99.95%

---

## 4. Bloco Telco (v3)

### 4.1 Variaveis Anonimas (var_26 a var_93)

| # | Variavel | Tipo | Sentinela | Descricao |
|---|----------|------|-----------|-----------|
| 12-79 | `var_26` a `var_93` | double | 304 | Variaveis telco anonimas |

**Total: 68 variaveis**

**Regras de Tratamento:**
- Valor `304` = sentinela (nao informado/nao aplicavel) → converter para NULL
- Criar `flag_telco_missing` quando > 50% das vars sao NULL

**Cobertura:** 35.46%

---

## 5. Bloco Cadastro (v4)

### 5.1 Variaveis Cadastrais Explicitas

| # | Variavel | Tipo | Descricao | Tratamento |
|---|----------|------|-----------|------------|
| 80 | `statusrf` | string | Status RF (REGULAR, SUSPENSA, etc) | Categorica |
| 81 | `datadenascimento` | date | Data de nascimento | Derivar idade |
| 82 | `idade_anos` | int | Idade na safra | Derivada |
| 83 | `cep_3_digitos` | string | 3 primeiros digitos do CEP | Proxy geografico |

### 5.2 Variaveis Anonimas Cadastro (var_02 a var_25)

| # | Variavel | Tipo | Natureza | Descricao |
|---|----------|------|----------|-----------|
| 84-107 | `var_02` a `var_25` | mixed | Numerica/Categorica | Variaveis cadastrais anonimas |

**Total: ~33 variaveis**

**Notas:**
- var_07: numerica continua (assimetrica)
- var_15, var_22, var_23, var_24: categoricas
- var_12, var_13: campos mistos (data ou codigo)

---

## 6. Bloco Recarga (v5)

As features de Recarga sao agregadas por janela temporal:
- **M1**: Ultimo 1 mes antes da safra
- **M3**: Ultimos 3 meses antes da safra
- **M6**: Ultimos 6 meses antes da safra

### 6.1 Features de Volume

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 108 | `qtd_recargas` | _m1, _m3, _m6 | int | Total de eventos de recarga |
| 109 | `qtd_recargas_validas` | _m1, _m3, _m6 | int | Recargas excluindo ZERO_TOTAL |
| 110 | `qtd_telefones_distintos` | _m1, _m3, _m6 | int | Linhas/terminais distintos |

### 6.2 Features de Valores

| # | Variavel Base | Sufixos | Tipo | Unidade | Descricao |
|---|---------------|---------|------|---------|-----------|
| 111 | `sum_val_credito` | _m1, _m3, _m6 | double | R$ | Soma de credito inserido |
| 112 | `sum_val_bonus` | _m1, _m3, _m6 | double | R$ | Soma de bonus recebido |
| 113 | `sum_val_real` | _m1, _m3, _m6 | double | R$ | Soma de valor real original |
| 114 | `sum_val_real_ajustado` | _m1, _m3, _m6 | double | R$ | Soma apos ajuste SOS + bonus |
| 115 | `avg_val_real` | _m1, _m3, _m6 | double | R$ | Media do valor por recarga |
| 116 | `min_val_real` | _m1, _m3, _m6 | double | R$ | Menor valor de recarga valida |
| 117 | `max_val_real` | _m1, _m3, _m6 | double | R$ | Maior valor de recarga valida |
| 118 | `std_val_real` | _m1, _m3, _m6 | double | R$ | Desvio padrao dos valores |
| 119 | `ticket_medio` | _m1, _m3, _m6 | double | R$ | Valor tipico por recarga |

### 6.3 Features de SOS (Estresse Financeiro)

> **SOS e um emprestimo/adiantamento (R$3-20) descontado na proxima recarga. Alta frequencia de SOS indica estresse financeiro.**

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia Risco |
|---|---------------|---------|------|-----------|------------------|
| 120 | `qtd_sos` | _m1, _m3, _m6 | int | Quantidade de eventos SOS | Alta |
| 121 | `sum_valor_sos` | _m1, _m3, _m6 | double | Soma dos valores SOS (R$) | Alta |
| 122 | `flag_teve_sos` | _m1, _m3, _m6 | int | 1 se usou SOS, 0 caso contrario | Alta |
| 123 | `pct_sos_sobre_credito` | _m1, _m3, _m6 | double | (sum_sos / sum_credito) * 100 | **Muito Alta** |
| 124 | `freq_sos` | _m1, _m3, _m6 | double | qtd_sos / qtd_recargas | **Muito Alta** |

### 6.4 Features de Tempo Entre Recargas

| # | Variavel Base | Sufixos | Tipo | Unidade | Descricao |
|---|---------------|---------|------|---------|-----------|
| 125 | `dias_medio_entre_recargas` | _m1, _m3, _m6 | double | dias | Media de dias entre recargas |
| 126 | `dias_min_entre_recargas` | _m1, _m3, _m6 | double | dias | Menor intervalo |
| 127 | `dias_max_entre_recargas` | _m1, _m3, _m6 | double | dias | Maior intervalo |
| 128 | `std_dias_entre_recargas` | _m1, _m3, _m6 | double | dias | Desvio padrao do intervalo |

### 6.5 Features de Horario

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 129 | `qtd_recargas_madrugada` | _m1, _m3, _m6 | int | Recargas 00h-05h |
| 130 | `qtd_recargas_manha` | _m1, _m3, _m6 | int | Recargas 06h-11h |
| 131 | `qtd_recargas_tarde` | _m1, _m3, _m6 | int | Recargas 12h-17h |
| 132 | `qtd_recargas_noite` | _m1, _m3, _m6 | int | Recargas 18h-23h |
| 133 | `qtd_recargas_fim_semana` | _m1, _m3, _m6 | int | Recargas em sabado/domingo |
| 134 | `pct_recargas_fim_semana` | _m1, _m3, _m6 | double | % de recargas no fim de semana |
| 135 | `pct_recargas_madrugada` | _m1, _m3, _m6 | double | % de recargas na madrugada |

### 6.6 Features de Tipo de Transacao

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 136 | `qtd_pago_puro` | _m1, _m3, _m6 | int | Recargas tradicionais (so credito) |
| 137 | `qtd_bonus_puro` | _m1, _m3, _m6 | int | Apenas bonus (promocao) |
| 138 | `qtd_combo` | _m1, _m3, _m6 | int | Recarga + bonus |
| 139 | `qtd_valor_negativo` | _m1, _m3, _m6 | int | Estornos ou ajustes |

### 6.7 Features Derivadas Recarga

| # | Variavel Base | Sufixos | Tipo | Formula | Relevancia |
|---|---------------|---------|------|---------|------------|
| 140 | `coef_variacao_val` | _m1, _m3, _m6 | double | std_val / avg_val | Alta |
| 141 | `ratio_max_min_val` | _m1, _m3, _m6 | double | max_val / min_val | Media |
| 142 | `ratio_bonus_credito` | _m1, _m3, _m6 | double | sum_bonus / sum_credito | Media |
| 143 | `val_liquido` | _m1, _m3, _m6 | double | sum_credito - sum_sos | Alta |

### 6.8 Flags de Cobertura Recarga

| # | Variavel | Tipo | Condicao | Uso |
|---|----------|------|----------|-----|
| 144 | `flag_recarga_m1` | int | Tem dados M1 | Cobertura 56.12% |
| 145 | `flag_recarga_m3` | int | Tem dados M3 | Cobertura 66.86% |
| 146 | `flag_recarga_m6` | int | Tem dados M6 | Cobertura 68.92% |

**Total Recarga: ~126 variaveis (42 base x 3 janelas)**

---

## 7. Bloco Pagamento (v6)

As features de Pagamento sao agregadas por janela temporal M1/M3/M6.

### 7.1 Features de Volume

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 147 | `qtd_transacoes` | _m1, _m3, _m6 | int | Total de transacoes |
| 148 | `qtd_pagamentos_validos` | _m1, _m3, _m6 | int | Pagamentos com valor > 0 |
| 149 | `qtd_faturas_distintas` | _m1, _m3, _m6 | int | Faturas distintas pagas |
| 150 | `qtd_contratos_distintos` | _m1, _m3, _m6 | int | Contratos distintos |

### 7.2 Features de Valores Pagos

| # | Variavel Base | Sufixos | Tipo | Unidade | Descricao |
|---|---------------|---------|------|---------|-----------|
| 151 | `sum_val_pago` | _m1, _m3, _m6 | double | R$ | Soma total pago |
| 152 | `avg_val_pago` | _m1, _m3, _m6 | double | R$ | Ticket medio |
| 153 | `max_val_pago` | _m1, _m3, _m6 | double | R$ | Maior pagamento |
| 154 | `min_val_pago` | _m1, _m3, _m6 | double | R$ | Menor pagamento |
| 155 | `std_val_pago` | _m1, _m3, _m6 | double | R$ | Desvio padrao |
| 156 | `ticket_medio_pagamento` | _m1, _m3, _m6 | double | R$ | sum_val_pago / qtd_pagamentos |

### 7.3 Features de Desconto (Comportamento de Negociacao)

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia |
|---|---------------|---------|------|-----------|------------|
| 157 | `sum_val_desconto` | _m1, _m3, _m6 | double | Total de descontos obtidos | Media |
| 158 | `qtd_com_desconto` | _m1, _m3, _m6 | int | Quantidade com desconto | Media |
| 159 | `pct_pagamentos_com_desconto` | _m1, _m3, _m6 | double | % com desconto | Media |
| 160 | `ratio_desconto_pago` | _m1, _m3, _m6 | double | Desconto / Valor pago | Media |
| 161 | `flag_alto_desconto` | _m1, _m3, _m6 | int | Desconto > 10% | Media |

### 7.4 Features de Juros e Multas (Indicador de Atraso Passado)

> **Juros pagos indicam que o cliente atrasou pagamentos anteriores.**

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia |
|---|---------------|---------|------|-----------|------------|
| 162 | `sum_val_juros_pos` | _m1, _m3, _m6 | double | Juros/multas pagos | **Alta** |
| 163 | `qtd_com_juros` | _m1, _m3, _m6 | int | Quantidade com juros | Alta |
| 164 | `pct_pagamentos_com_juros` | _m1, _m3, _m6 | double | % com juros | **Alta** |
| 165 | `ratio_juros_pago` | _m1, _m3, _m6 | double | Juros / Valor pago | **Alta** |
| 166 | `flag_sempre_com_juros` | _m1, _m3, _m6 | int | >80% com juros | **Muito Alta** |

### 7.5 Features de Formas de Pagamento

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 167 | `qtd_formas_pagamento_distintas` | _m1, _m3, _m6 | int | Diversidade de formas |
| 168 | `sum_pago_forma_01` | _m1, _m3, _m6 | double | Valor pago forma 01 |
| 169 | `sum_pago_forma_02` | _m1, _m3, _m6 | double | Valor pago forma 02 |
| 170 | `sum_pago_forma_03` | _m1, _m3, _m6 | double | Valor pago forma 03 |
| 171 | `pct_forma_dominante` | _m1, _m3, _m6 | double | Concentracao na forma principal |

### 7.6 Features Derivadas Pagamento

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 172 | `coef_variacao_pagamento` | _m1, _m3, _m6 | double | std / avg |
| 173 | `val_liquido_pago` | _m1, _m3, _m6 | double | Pago - Desconto |

### 7.7 Flags de Cobertura Pagamento

| # | Variavel | Tipo | Condicao | Uso |
|---|----------|------|----------|-----|
| 174 | `flag_pagamento_m1` | int | Tem dados M1 | Cobertura 16.13% |
| 175 | `flag_pagamento_m3` | int | Tem dados M3 | - |
| 176 | `flag_pagamento_m6` | int | Tem dados M6 | - |
| 177 | `flag_sem_pagamento` | _m1, _m3, _m6 | int | Nenhum pagamento na janela |

**Total Pagamento: ~135 variaveis (45 base x 3 janelas)**

---

## 8. Bloco Atraso (v6)

As features de Atraso sao agregadas por janela temporal M1/M3/M6.

> **Atraso e um snapshot mensal do estado das faturas em aberto, aging, write-offs e provisoes.**

### 8.1 Features de Faturas Abertas

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 178 | `qtd_registros` | _m1, _m3, _m6 | int | Total de registros |
| 179 | `qtd_faturas_abertas` | _m1, _m3, _m6 | int | Faturas em aberto |
| 180 | `qtd_contratos_com_atraso` | _m1, _m3, _m6 | int | Contratos com divida |

### 8.2 Features de Valores em Aberto

| # | Variavel Base | Sufixos | Tipo | Unidade | Descricao |
|---|---------------|---------|------|---------|-----------|
| 181 | `sum_val_aberto` | _m1, _m3, _m6 | double | R$ | Valor total em aberto |
| 182 | `avg_val_aberto` | _m1, _m3, _m6 | double | R$ | Media por fatura |
| 183 | `max_val_aberto` | _m1, _m3, _m6 | double | R$ | Maior fatura em aberto |
| 184 | `min_val_aberto` | _m1, _m3, _m6 | double | R$ | Menor fatura em aberto |
| 185 | `std_val_aberto` | _m1, _m3, _m6 | double | R$ | Desvio padrao |
| 186 | `ticket_medio_aberto` | _m1, _m3, _m6 | double | R$ | sum / qtd |

### 8.3 Features de Faturamento

| # | Variavel Base | Sufixos | Tipo | Unidade | Descricao |
|---|---------------|---------|------|---------|-----------|
| 187 | `sum_val_fat_bruto` | _m1, _m3, _m6 | double | R$ | Faturamento bruto |
| 188 | `sum_val_fat_liquido` | _m1, _m3, _m6 | double | R$ | Faturamento liquido |
| 189 | `sum_val_pagamento` | _m1, _m3, _m6 | double | R$ | Pagamentos realizados |
| 190 | `sum_val_multa_juros` | _m1, _m3, _m6 | double | R$ | Multas e juros |
| 191 | `ratio_aberto_faturado` | _m1, _m3, _m6 | double | Aberto / Faturado |
| 192 | `ratio_pagamento_faturado` | _m1, _m3, _m6 | double | Pago / Faturado |

### 8.4 Features de Aging (Distribuicao de Atraso)

> **Aging indica ha quantos dias a fatura esta em aberto. Maior aging = maior risco.**

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia |
|---|---------------|---------|------|-----------|------------|
| 193 | `qtd_aging_0_30` | _m1, _m3, _m6 | int | Faturas com 0-30 dias | Baixa |
| 194 | `qtd_aging_31_60` | _m1, _m3, _m6 | int | Faturas com 31-60 dias | Media |
| 195 | `qtd_aging_61_90` | _m1, _m3, _m6 | int | Faturas com 61-90 dias | Alta |
| 196 | `qtd_aging_90_plus` | _m1, _m3, _m6 | int | Faturas com >90 dias | **Muito Alta** |
| 197 | `pct_aging_0_30` | _m1, _m3, _m6 | double | % em 0-30 dias | Baixa |
| 198 | `pct_aging_31_60` | _m1, _m3, _m6 | double | % em 31-60 dias | Media |
| 199 | `pct_aging_61_90` | _m1, _m3, _m6 | double | % em 61-90 dias | Alta |
| 200 | `pct_aging_90_plus` | _m1, _m3, _m6 | double | % em >90 dias | **Muito Alta** |

### 8.5 Features de Valores por Aging

| # | Variavel Base | Sufixos | Tipo | Descricao |
|---|---------------|---------|------|-----------|
| 201 | `sum_val_aging_0_30` | _m1, _m3, _m6 | double | Valor em 0-30 dias |
| 202 | `sum_val_aging_31_60` | _m1, _m3, _m6 | double | Valor em 31-60 dias |
| 203 | `sum_val_aging_61_90` | _m1, _m3, _m6 | double | Valor em 61-90 dias |
| 204 | `sum_val_aging_90_plus` | _m1, _m3, _m6 | double | Valor em >90 dias |
| 205 | `pct_val_aging_90_plus` | _m1, _m3, _m6 | double | % do valor em >90 dias |

### 8.6 Indicadores de Risco

> **WO = Write-Off (perda), PDD = Provisao para Devedores Duvidosos, Fraude = Indicador de fraude**

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia |
|---|---------------|---------|------|-----------|------------|
| 206 | `flag_teve_wo` | _m1, _m3, _m6 | int | Write-off no periodo | **Muito Alta** |
| 207 | `flag_teve_pdd` | _m1, _m3, _m6 | int | Provisao PDD | **Muito Alta** |
| 208 | `flag_teve_fraude` | _m1, _m3, _m6 | int | Indicador de fraude | **Muito Alta** |
| 209 | `qtd_com_wo` | _m1, _m3, _m6 | int | Quantidade com WO | Alta |
| 210 | `qtd_com_pdd` | _m1, _m3, _m6 | int | Quantidade com PDD | Alta |
| 211 | `qtd_com_fraude` | _m1, _m3, _m6 | int | Quantidade com fraude | Alta |
| 212 | `sum_val_wo` | _m1, _m3, _m6 | double | Valor em WO | Alta |
| 213 | `sum_val_pdd` | _m1, _m3, _m6 | double | Valor em PDD | Alta |
| 214 | `pct_val_wo_sobre_aberto` | _m1, _m3, _m6 | double | % WO / Aberto | Alta |

### 8.7 Indicadores de Recuperacao

> **ACA = Acordo de pagamento, PCCR = Programa de recuperacao de credito**

| # | Variavel Base | Sufixos | Tipo | Descricao | Relevancia |
|---|---------------|---------|------|-----------|------------|
| 215 | `flag_teve_aca` | _m1, _m3, _m6 | int | Acordo de pagamento | Media |
| 216 | `flag_teve_pccr` | _m1, _m3, _m6 | int | Programa de recuperacao | Media |
| 217 | `qtd_com_aca` | _m1, _m3, _m6 | int | Quantidade com ACA | Media |
| 218 | `qtd_com_pccr` | _m1, _m3, _m6 | int | Quantidade com PCCR | Media |

### 8.8 Flags de Comportamento Atraso

| # | Variavel Base | Sufixos | Tipo | Condicao | Relevancia |
|---|---------------|---------|------|----------|------------|
| 219 | `flag_sem_atraso` | _m1, _m3, _m6 | int | Sem faturas abertas | Positivo |
| 220 | `flag_atraso_grave` | _m1, _m3, _m6 | int | >50% em aging 90+ | **Muito Alta** |
| 221 | `flag_risco_alto` | _m1, _m3, _m6 | int | WO ou PDD ou Fraude | **Muito Alta** |
| 222 | `flag_em_recuperacao` | _m1, _m3, _m6 | int | ACA ou PCCR ativo | Media |
| 223 | `flag_alto_valor_aberto` | _m1, _m3, _m6 | int | Aberto > R$500 | Alta |
| 224 | `flag_muitas_faturas_abertas` | _m1, _m3, _m6 | int | >3 faturas abertas | Alta |
| 225 | `flag_concentrado_aging_grave` | _m1, _m3, _m6 | int | >70% valor em 90+ | **Muito Alta** |

### 8.9 Flags de Cobertura Atraso

| # | Variavel | Tipo | Condicao | Uso |
|---|----------|------|----------|-----|
| 226 | `flag_atraso_m1` | int | Tem dados M1 | Cobertura 21.79% |
| 227 | `flag_atraso_m3` | int | Tem dados M3 | - |
| 228 | `flag_atraso_m6` | int | Tem dados M6 | - |

**Total Atraso: ~162 variaveis (54 base x 3 janelas)**

---

## 9. Metadados e Versao

| # | Variavel | Tipo | Descricao |
|---|----------|------|-----------|
| 229 | `abt_version` | string | Versao do ABT (ex: "v6_v2") |
| 230 | `build_date` | timestamp | Data/hora de geracao |
| 231 | `spine_version` | string | Versao do spine usado |

---

## 10. Top Features por Bloco (Recomendacao)

### 10.1 Score (Baseline)
1. `score_01` - Score principal de bureau
2. `score_02` - Score secundario

### 10.2 Recarga (Estresse Financeiro)
1. `freq_sos_m1` - Frequencia de uso de SOS (emprestimo)
2. `pct_sos_sobre_credito_m1` - Proporcao SOS/Credito
3. `coef_variacao_val_m1` - Instabilidade de valores
4. `dias_max_entre_recargas_m1` - Periodos de inatividade
5. `ticket_medio_m1` - Capacidade de pagamento

### 10.3 Pagamento (Historico de Atraso)
1. `pct_pagamentos_com_juros_m1` - % com juros (atraso passado)
2. `flag_sempre_com_juros_m1` - Padrao de atraso
3. `ratio_juros_pago_m1` - Intensidade de juros
4. `sum_val_juros_pos_m1` - Volume de juros pagos
5. `ticket_medio_pagamento_m1` - Valor tipico

### 10.4 Atraso (Risco Atual)
1. `pct_aging_90_plus_m1` - % em atraso grave
2. `flag_risco_alto_m1` - WO/PDD/Fraude
3. `sum_val_aberto_m1` - Valor em aberto
4. `ratio_aberto_faturado_m1` - Taxa de inadimplencia
5. `flag_atraso_grave_m1` - Concentracao em aging grave

---

## 11. Combinacoes de Features (Insights de Dominio)

| Combinacao | Interpretacao | Risco |
|------------|---------------|-------|
| Alto `freq_sos` + Baixo `ticket_medio` | Estresse financeiro severo | Alto |
| Alto `pct_aging_90_plus` + `flag_teve_wo` | Historico de perda | Muito Alto |
| Alto `pct_pagamentos_com_juros` + Alto `sum_val_aberto` | Padrao cronico de atraso | Muito Alto |
| `flag_em_recuperacao` + Baixo `ratio_aberto_faturado` | Recuperacao em andamento | Medio |
| Baixo `coef_variacao_val` + Alto `ticket_medio` | Cliente estavel | Baixo |

---

## 12. Regras Anti-Leakage

### 12.1 Variaveis Proibidas como Features
- `fpd_int` - TARGET
- `flag_instalacao_int` - Decisao de aprovacao

### 12.2 Regras Temporais
- Recarga: `safra_recarga < safra` (apenas dados passados)
- Pagamento: `safra_pagamento < safra` (apenas dados passados)
- Atraso: `safra_atraso < safra` (apenas dados passados)

### 12.3 Treino vs Scoring
- **Treino:** Apenas registros com `flag_instalacao_int = 1` (onde FPD e observado)
- **Scoring:** Todos os registros (incluindo `flag_instalacao_int = 0`)

---

## 13. Valores Especiais e Sentinelas

| Valor | Fonte | Significado | Tratamento |
|-------|-------|-------------|------------|
| `0` | Score_01 | Missing codificado | → NULL + flag |
| `304` | Telco vars | Nao informado | → NULL + flag |
| `-1` | Recarga | Nao se aplica | → NULL |
| `-2` | Recarga | Nao determinado | → NULL |
| `-3` | Recarga | Nao informado | → NULL |
| `NULL` | Qualquer | Dado ausente | → 0 ou flag |

---

## 14. Changelog

| Versao | Data | Alteracoes |
|--------|------|------------|
| v6.2 | 2026-01-29 | Versao inicial completa do Book of Variables |
| v5.2 | 2026-01-28 | Adicao do bloco Recarga v2 |
| v4.0 | 2026-01-27 | Adicao do bloco Cadastro |
| v3.0 | 2026-01-26 | Adicao do bloco Telco |
| v2.0 | 2026-01-25 | Adicao de Score_02 |
| v1.0 | 2026-01-24 | Baseline com Score_01 |

---

## 15. Referencias

| Documento | Localizacao |
|-----------|-------------|
| CLAUDE.md | `/CLAUDE.md` |
| Target Definition | `/docs/target_definition.md` |
| Recarga Variable Book | `/docs/05_abt_v5_docs/VARIABLE_BOOK_RECARGA_V2.md` |
| Data Dictionaries | `/docs/01_data_dictionary/` |
| ABT Rules | `/docs/04_gold_rules/` |
| Execution Reports | `/docs/00_execution_outputs_reports/` |
