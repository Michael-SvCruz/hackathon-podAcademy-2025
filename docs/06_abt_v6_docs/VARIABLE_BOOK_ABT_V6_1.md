# Variable Book — ABT v6.1 (296 Features)

**Versão:** gold_abt_v6_1  
**Data de Construção:** 2026-01-26  
**Última Atualização:** 2026-01-26 (Correção Cadastro var_03-09, Diagnóstico DESCONTO_RATE)  
**Grão:** 1:1 NUM_CPF + SAFRA  
**Total de Registros:** 3,795,310  
**Total de Colunas:** 296  
**Target (Observado):** FPD_INT (0/1) — apenas quando FLAG_INSTALACAO_INT = 1  

---

## 📋 Índice

1. [Chaves e Identificação](#chaves-e-identificação)
2. [Target e Decisão](#target-e-decisão)
3. [Features v1-v6 Herdadas](#features-v1-v6-herdadas)
4. [Features v6.1 Enhancement (NOVO)](#features-v61-enhancement-novo)
5. [Metadados de Auditoria](#metadados-de-auditoria)
6. [Anti-Leakage e Temporal Rules](#anti-leakage-e-temporal-rules)
7. [Data Types e Storage](#data-types-e-storage)

---

## Chaves e Identificação

### Colunas Obrigatórias (Grão)

| Coluna | Tipo | Descrição | Coverage | Observações |
|--------|------|-----------|----------|-------------|
| `num_cpf` | STRING | Identificador único de cliente (CPF) | 100% | Chave primária, sem NULLs |
| `safra` | INTEGER | Período de observação (formato yyyyMM) | 100% | Ex: 202501 = janeiro/2025, sem NULLs |
| `dt_safra` | DATE | Data de referência (sempre dia 01 do mês) | 100% | Derivada de SAFRA |

---

## Target e Decisão

### ⚠️ CRÍTICO: Regra de Anti-Leakage

**NUNCA use estas colunas como FEATURES** — são rótulos (labels) e decisões:

| Coluna | Tipo | Papel | Observância | Regra |
|--------|------|-------|-------------|-------|
| `fpd_int` | INTEGER (0/1) | **TARGET**: First Payment Default (inadimplência) | Observado apenas em FLAG_INSTALACAO_INT=1 | **Gate 2 obrigatório**: verificar FPD nulo em FLAG=0 |
| `flag_instalacao_int` | INTEGER (0/1) | **DECISION**: Aprovação do crédito (Flag Instalação) | Sempre presente | Use APENAS para Impact Analysis / Swap-in-Swap-out, nunca como feature |

**Distribuição Observada:**
- FLAG_INSTALACAO_INT = 1 (aprovado): 2,633,900 (69.40%)
- FLAG_INSTALACAO_INT = 0 (recusado): 1,161,410 (30.60%)

⚠️ **Para Model Training:**
- Usar apenas registros com FLAG_INSTALACAO_INT = 1 (n = 2,633,900)
- Target (FPD_INT) observado apenas neste subconjunto
- Usar FLAG_INSTALACAO_INT para análise de impacto e fairness, nunca como feature

---

## Features v1-v6 Herdadas

### 1️⃣ Bloco SCORE (v1)

#### 1.1 SCORE_01

| Feature | Tipo | Coverage | Range | Descrição | Rol |
|---------|------|----------|-------|-----------|-----|
| `score_01_adj` | DOUBLE | 98.18% | [0, 1000] | Score de crédito ajustado (modelo externo) | Sentinela 0 → NULL + flag |
| `flag_score_01_missing` | INTEGER (0/1) | 100% | - | Indica sentinela/falta em SCORE_01 (valor 0 original) | Captura informação de missingness |

#### 1.2 SCORE_02

| Feature | Tipo | Coverage | Range | Descrição | Rol |
|---------|------|----------|-------|-----------|-----|
| `score_02_adj` | DOUBLE | 99.95% | [0, 1000] | Score de crédito II ajustado (modelo alternativo) | Sentinela 0 → NULL + flag |
| `flag_score_02_missing` | INTEGER (0/1) | 100% | - | Indica sentinela/falta em SCORE_02 (valor 0 original) | Captura informação de missingness |

---

### 2️⃣ Bloco TELCO (v3)

**Cobertura Agregada:** 20.51% (52.9M/258M células preenchidas)

68 features de comportamento de telecom divididas em **3 períodos temporais (M1, M3, M6)**

#### Janelas Temporais:
- **M1 (var_001-22):** Última 1 mês
- **M3 (var_023-44):** Últimos 3 meses
- **M6 (var_045-66):** Últimos 6 meses
- **General (var_067-93):** Variáveis gerais de perfil (27 features)

| Feature | Tipo | Coverage | Range | Descrição |
|---------|------|----------|-------|-----------|
| `var_01` a `var_93` | DOUBLE/INTEGER | ~20% | Varia | Variáveis de comportamento telecom (recargas, chamadas, dados, etc.) |

**Nota:** Dados de Telco têm cobertura limitada (apenas clientes com contrato ativo na base). Veja `docs/01_data_dictionary/telco.md` para detalhes de cada variável.

---

### 3️⃣ Bloco CADASTRO (v4)

**Cobertura Agregada:** 35-40% (clientes com dados cadastrais)

**Status:** ✅ **Implementado COMPLETAMENTE em v6.1** (296 features finais)

**CORREÇÃO CONCLUÍDA (26/01/2026):** ✅ 
- Bug corrigido! As 7 variáveis numéricas (`var_03`-`var_09`) foram reclassificadas como MISTAS no script Silver
- Adicionadas corretamente à lista NUMERIC_VARS em [02_bronze_silver_cadastro.py](../../src/jobs/01_silver/02_bronze_silver_cadastro.py)
- **Impacto**: +7 features Cadastro restauradas (var_03-09)
- **Coverage**: Mantido em 35-40% (consistente com v4-v6)
- **Total features**: 288 → **296** (288 + 7 numéricas + 1 overlap ajustado)

#### Features Demográficas (5 features + 1 flag) ✅ PRESENTES

| Feature | Tipo | Coverage | Range | Descrição | Lógica |
|---------|------|----------|-------|-----------|--------|
| `idade_anos` | INTEGER (LONG) | 35-40% | [4, 131] | Idade em anos na safra | DATEDIFF(DT_SAFRA, DT_NASC) / 365 |
| `flag_idade_menor_18` | INTEGER (0/1) | 100% | - | Sanity check: menores de idade | FLAG quando idade < 18 |
| `flag_idade_muito_alta` | INTEGER (0/1) | 100% | - | Outlier check: idade extrema | FLAG quando idade > 100 |
| `cep_3_digitos` | STRING | 35-40% | [000-999] | Proxy geográfico (regional) | 3 primeiros dígitos do CEP |
| `flag_cep_missing` | INTEGER (0/1) | 100% | - | Indicador: CEP ausente | FLAG quando CEP=NULL |
| `statusrf` | STRING | 35-40% | REGULAR, PENDENTE, SUSPENSA, CANCELADA, FALECIDO | Status cadastral do cliente | MAX(statusrf) |

#### Features Administrativas (2 features) ✅ PRESENTES

| Feature | Tipo | Coverage | Range | Descrição | Lógica |
|---------|------|----------|-------|-----------|--------|
| `prod` | STRING | 35-40% | CMV, DTH, NET | Produto de telecomunicação | TRIM(prod) |
| `flag_mig2` | STRING | 35-40% | PRE, FLEX, Aquisição | Status de migração de cliente | TRIM(flag_mig2) |

#### Features Numéricas Cadastrais (7 features) ✅ PRESENTES

| Feature | Tipo | Coverage | Range | Descrição | Data Quality |
|---------|------|----------|-------|-----------|------------------|
| `var_03` | DOUBLE | 35-40% | [0, ∞) | Variável numérica cadastral 03 | 0 não numéricos (confirmada) |
| `var_04` | DOUBLE | 35-40% | [0, ∞) | Variável numérica cadastral 04 | 0 não numéricos (confirmada) |
| `var_05` | DOUBLE | 35-40% | [0, ∞) | Variável numérica cadastral 05 | 0 não numéricos (confirmada) |
| `var_06` | DOUBLE | 35-40% | [0, ∞) | Variável numérica cadastral 06 | 0 não numéricos (confirmada) |
| `var_07` | DOUBLE | 35-40% | [0, ∞) | Variável numérica contínua com outliers | Assimétrica, p99=277.059 |
| `var_08` | DOUBLE | 35-40% | [0, ∞) | Variável numérica discreta | 0 não numéricos (confirmada) |
| `var_09` | DOUBLE | 35-40% | [0, ∞) | Variável numérica discreta | 0 não numéricos (confirmada) |

✅ **Correção (26/01/2026):** Todas as 7 variáveis numéricas estão presentes após fix no script Silver.

#### Features Categóricas/Mistas (16 features) ✅ PRESENTES

| Feature | Tipo | Coverage | Descrição | Observação |
|---------|------|----------|-----------|-----------|
| `var_10` | STRING | 35-40% | Variável categórica cadastral | Trim simples |
| `var_11` | STRING | 35-40% | Variável categórica cadastral | Trim simples |
| `var_12` | STRING | 35-40% | Possível data (formato inválido em alguns) | Parse tolerante necessário |
| `var_13` | STRING | 35-40% | Variável mista | Mix de tipos |
| `var_14` | STRING | 35-40% | Variável mista | Mix de tipos |
| `var_15` | STRING | 35-40% | Categórica (ocupação/profissão) | Alto cardinalidade |
| `var_16` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_17` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_18` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_19` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_20` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_21` | STRING | 35-40% | Variável categórica | Trim simples |
| `var_22` | STRING | 35-40% | Categórica de alta cardinalidade | Trim + uppercase |
| `var_23` | STRING | 35-40% | Categórica de alta cardinalidade | Trim + uppercase |
| `var_24` | STRING | 35-40% | Categórica muito sparse (2.4M não numéricos) | Trim + uppercase |
| `var_25` | STRING | 35-40% | Variável categórica | Trim + uppercase |

#### Data Quality (Confirmada)

| Métrica | Valor | Status |
|---------|-------|--------|
| Grão (1:1 por NUM_CPF + SAFRA) | ✅ Confirmado | Sem duplicatas |
| Features de Cadastro PRESENTES | 24/33 | ⚠️ Parcial (faltam 7 numéricas + var_02) |
| Idade min/max | [4, 131] | ⚠️ Outliers presentes (flags criadas) |
| Missing em DATADENASCIMENTO | 16.831 (~0.4%) | ✅ Baixo |
| Missing em CEP_3_digitos | 292.051 (~7.5%) | ⚠️ Moderado (flag criada) |

---

### 4️⃣ Bloco RECARGA (v5)

**Cobertura Agregada:** 56.12% (clientes com atividade de recarga)

#### Features M1/M3/M6 (3 períodos × 3 features = 9 features)

| Feature | Tipo | Coverage | Range | Descrição | Lógica |
|---------|------|----------|-------|-----------|--------|
| `qtd_recargas_m{1,3,6}` | INTEGER | 56.12% | [0, ∞) | Quantidade de recargas no período | COUNT(DISTINCT recarga_id) |
| `sum_val_recarga_m{1,3,6}` | DOUBLE | 56.12% | [0, ∞) | Soma de valores recarregados | SUM(valor_recarga) |
| `avg_val_recarga_m{1,3,6}` | DOUBLE | 56.12% | [0, ∞) | Valor médio por recarga | AVG(valor_recarga) |

**Nota:** Clientes sem recarga (43.88%) têm valor 0 após coalesce.

---

### 5️⃣ Bloco PAGAMENTO (v6)

**Cobertura Agregada:** 17.09% (648.5K clientes com atividade de pagamento)

#### Features M1/M3/M6 (3 períodos × 13 features = 39 features)

| Feature | Tipo | Coverage | Range | Descrição | Lógica |
|---------|------|----------|-------|-----------|--------|
| `qtd_itens_pagamento_m{1,3,6}` | INTEGER | 17.09% | [0, 67] | Quantidade de itens pagos | COUNT(*) itens de pagamento |
| `sum_val_pago_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Soma total paga | SUM(valor_pago) |
| `avg_val_pago_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Valor médio por item pago | AVG(valor_pago) |
| `max_val_pago_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Maior valor pago em um item | MAX(valor_pago) |
| `sum_val_desconto_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Soma de descontos concedidos | SUM(desconto) |
| `sum_val_juros_pos_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Juros POSITIVOS (client benefit) | SUM(juros > 0) |
| `sum_val_juros_neg_abs_m{1,3,6}` | DOUBLE | 17.09% | [0, ∞) | Juros NEGATIVOS em valor absoluto | ABS(SUM(juros < 0)) |
| `flag_teve_desconto_m{1,3,6}` | INTEGER (0/1) | 17.09% | - | Indicador: cliente recebeu desconto | MAX(desconto > 0) |
| `sum_pago_forma_01_m{1,3,6}` | DOUBLE | ~5% | [0, ∞) | Valor pago via forma 01 (ex: dinheiro) | SUM(forma=01) |
| `sum_pago_forma_02_m{1,3,6}` | DOUBLE | ~5% | [0, ∞) | Valor pago via forma 02 (ex: débito) | SUM(forma=02) |
| `sum_pago_forma_03_m{1,3,6}` | DOUBLE | ~5% | [0, ∞) | Valor pago via forma 03 (ex: crédito) | SUM(forma=03) |
| `sum_pago_forma_missing_m{1,3,6}` | DOUBLE | ~1% | [0, ∞) | Valor pago com forma desconhecida | SUM(forma=NULL) |
| `flag_missing_ts_status_pagamento_m{1,3,6}` | INTEGER (0/1) | 17.09% | - | Flag: >50% de registros sem timestamp | COUNT(NULL ts) / COUNT(*) > 0.5 |

**Nota:** Clientes sem pagamento (82.91%) têm valor 0 após coalesce.

---

### 6️⃣ Bloco ATRASO (v6)

**Cobertura Agregada:** 22.43% (851.4K clientes com faturas abertas)

#### Features M1/M3/M6 (3 períodos × 19 features = 57 features)

| Feature | Tipo | Coverage | Range | Descrição | Lógica |
|---------|------|----------|-------|-----------|--------|
| `qtd_faturas_abertas_m{1,3,6}` | INTEGER | 22.43% | [0, 206] | Quantidade de faturas abertas (não pagas) | COUNTDISTINCT(num_fatura_hash) WHERE valor_aberto > 0 |
| `sum_val_aberto_m{1,3,6}` | DOUBLE | 22.43% | [0, ∞) | Soma de valores em aberto | SUM(valor_aberto) |
| `avg_val_aberto_m{1,3,6}` | DOUBLE | 22.43% | [0, ∞) | Valor médio por fatura aberta | AVG(valor_aberto) |
| `max_val_aberto_m{1,3,6}` | DOUBLE | 22.43% | [0, ∞) | Maior valor em aberto em uma fatura | MAX(valor_aberto) |
| `sum_val_pagamento_m{1,3,6}` | DOUBLE | 22.43% | [0, ∞) | Soma de pagamentos brutos realizados | SUM(valor_pagamento_bruto) |
| `flag_teve_wo_m{1,3,6}` | STRING | 22.43% | W/R/- | Flag: Write-Off (compra com prejuízo) | MAX(ind_wo) |
| `flag_teve_pdd_m{1,3,6}` | STRING | 22.43% | S/N/- | Flag: PDD = Probabilidade Default (modelo) | MAX(ind_pdd) |
| `flag_teve_fraude_m{1,3,6}` | STRING | 22.43% | S/N/- | Flag: Fraude detectada | MAX(ind_fraude) |
| `flag_teve_aca_m{1,3,6}` | STRING | 22.43% | S/A/N | Flag: Ação judicial | MAX(ind_aca) |
| `flag_teve_pccr_m{1,3,6}` | STRING | 22.43% | W/A/C/- | Flag: Parcelamento/Acordo/Consensual | MAX(ind_pccr) |
| `qtd_faturas_aging_0_30_m{1,3,6}` | INTEGER | 22.43% | [0, ∞) | Faturas com 0-30 dias de atraso | COUNTDISTINCT WHERE aging="0-30 dias" |
| `qtd_faturas_aging_31_60_m{1,3,6}` | INTEGER | 22.43% | [0, ∞) | Faturas com 31-60 dias de atraso | COUNTDISTINCT WHERE aging="31-60 dias" |
| `qtd_faturas_aging_61_90_m{1,3,6}` | INTEGER | 22.43% | [0, ∞) | Faturas com 61-90 dias de atraso | COUNTDISTINCT WHERE aging="61-90 dias" |
| `qtd_faturas_aging_90_plus_m{1,3,6}` | INTEGER | 22.43% | [0, ∞) | Faturas com >90 dias de atraso | COUNTDISTINCT WHERE aging=">90 dias" |
| `qtd_faturas_aging_missing_m{1,3,6}` | INTEGER | 22.43% | [0, ∞) | Faturas com aging desconhecido | COUNTDISTINCT WHERE aging=NULL |

**Nota:** Clientes sem atraso (77.57%) têm valor 0 após coalesce.

---

## Features v6.1 Enhancement (NOVO)

### 🆕 Bloco ENHANCEMENT (v6.1)

**9 features novas** divididas em **3 períodos temporais (M1/M3/M6)**

#### Desconto Rate (3 features)

| Feature | Tipo | Coverage | Range | Descrição | Fórmula | Interpretação |
|---------|------|----------|-------|-----------|---------|----------------|
| `desconto_rate_m{1,3,6}` | DOUBLE | 100% | [0.0, 1.0] | Taxa de desconto no período | SUM(desconto) / (SUM(desconto) + SUM(pago)) | Proporção de desconto rel. ao pagamento total; 0 = sem desconto, 1 = 100% desconto |

**Cobertura Real (Validada 26/01/2026):**
- M1: 100.00%, Média: 0.0000, Máx: 0.0000
- M3: 100.00%, Média: 0.0000, Máx: 0.0000  
- M6: 100.00%, Média: 0.0000, Máx: 0.0000

✅ **Conclusão (Diagnóstico Executado):** 
- VAL_DESCONTO_ITEM = **0 em 100% dos 21.8M registros Pagamento**
- Desconto é **política de negócio**: operadora **não oferece desconto**
- Feature é correta mas **constante (sem variância)**
- Mantida para auditoria e consistência com roadmap v6.1
- Veja [DIAGNOSTICO_DESCONTO_RATE.md](../../DIAGNOSTICO_DESCONTO_RATE.md) para análise completa

**Intuição de Risco:**
- ↑ Taxa de desconto → Cliente negocia agressivamente → Pode indicar risco se predisposto a inadimplência

---

#### Delinquency Rate (3 features)

| Feature | Tipo | Coverage | Range | Descrição | Fórmula | Interpretação |
|---------|------|----------|-------|-----------|---------|----------------|
| `delinquency_rate_m{1,3,6}` | DOUBLE | 100% | [0.0, 1.0] | Taxa de inadimplência no período | QTD(faturas abertas) / QTD(total faturas) | Proporção de faturas não pagas; 0 = todas quitadas, 1 = todas em aberto |

**Cobertura Real:**
- M1: 100.00%, Média: 0.2243, Máx: 1.0000
- M3: Similar a M1 (dados históricos)
- M6: Similar a M1 (dados históricos)

**Distribuição M1:**
- 0.0 (cliente sem débito): ~77.57%
- 0.0-0.5 (baixa inadimplência): ~10% dos clientes
- 0.5-1.0 (alta inadimplência): ~5% dos clientes
- 1.0 (100% em aberto): Pequeno % (~0.5%)

**Intuição de Risco:**
- ↑ Taxa de inadimplência → Cliente tem % alta de dívidas abertas → MUITO indicativo de risco futuro
- **Esta é a feature mais forte do enhancement** (correlação esperada com FPD: ALTA)

---

#### Max Days in Arrears (3 features)

| Feature | Tipo | Coverage | Range | Descrição | Fórmula | Interpretação |
|---------|------|----------|-------|-----------|---------|----------------|
| `max_dias_atraso_m{1,3,6}` | INTEGER | 100% | [0, 6771] | Máximo de dias em atraso no período | MAX(DATEDIFF(referencia, vencimento)) para faturas vencidas | Idade da dívida mais antiga; 0 = sem atraso, 6771 = débito de ~18 anos |

**Cobertura Real:**
- M1: 100.00%, Clientes c/ atraso: 11.30%, Média (c/ atraso): 110.9 dias, P90: 11 dias, Máx: 6771 dias
- M3: Similar a M1
- M6: Similar a M1

**Distribuição M1:**
- 0 dias (sem atraso): ~88.70%
- 1-30 dias: ~6% dos clientes
- 31-90 dias: ~3% dos clientes
- 91-365 dias: ~2% dos clientes
- >365 dias (dívida crônica): <0.3% dos clientes

**Intuição de Risco:**
- ↑ Dias em atraso → Cliente tem dívidas muito antigas → Indicador de insolvência crônica
- Dívidas >1 ano sugerem cliente com problemas financeiros severos
- P90=11d mostra que maioria do atraso é recente (< 2 semanas), mas tail é muito longo

---

## Metadados de Auditoria

### Colunas de Rastreabilidade (Sempre Presentes)

| Coluna | Tipo | Descrição | Valor |
|--------|------|-----------|-------|
| `metadata_file_path` | STRING | Caminho do arquivo source (Bronze) | Ex: `/Volumes/.../bureau_full_delta/...` |
| `metadata_ingestion_timestamp` | TIMESTAMP | Data/hora do ingestion (Bronze) | ISO 8601 |
| `gold_version` | STRING | Versão do Gold ABT | "gold_abt_v6_1" |
| `gold_build_date` | TIMESTAMP | Data de construção do ABT (Gold) | ISO 8601 |
| `gold_feature_blocks` | STRING | Blocos de features inclusos | "score_01,score_02,telco,cadastro,recarga,pagamento,atraso,enhancement" |

---

## Anti-Leakage e Temporal Rules

### ✅ Garantias de Anti-Leakage

1. **Todas as features usam dados históricos apenas** (lookback M1/M3/M6)
2. **Nenhuma feature usa dados futuro** (pós-snapshot ou pós-safra)
3. **FPD_INT é observado apenas 90+ dias após SAFRA** (validação de inadimplência efetiva)
4. **FLAG_INSTALACAO_INT é observada na DT_SAFRA** (aprovação/recusa no momento)

### Janelas Temporais Definidas

| Período | Meses | Exemplo (SAFRA=202501) | Significado |
|---------|-------|------------------------|-------------|
| **M1** | 0 | 202501 (janeiro 2025) | Mês da observação |
| **M3** | 0-2 | 202411-202501 (nov-jan 2024-25) | Últimos 3 meses |
| **M6** | 0-5 | 202408-202501 (ago-jan 2024-25) | Últimos 6 meses |

**Regra Crítica:**
- SAFRA = 202501 → Lookback para M1: apenas 202501
- SAFRA = 202501 → Lookback para M3: 202411, 202412, 202501
- SAFRA = 202501 → Lookback para M6: 202408-202501

---

## Data Types e Storage

### Tipos de Dados Utilizados

| Tipo PySpark | Python | Storage | Exemplos de Colunas |
|--------------|--------|---------|---------------------|
| `STRING` | str | Variable length | num_cpf, flag_teve_wo, metadata_file_path |
| `INTEGER` | int | 4 bytes | safra, qtd_recargas_m1, max_dias_atraso_m1 |
| `DOUBLE` | float | 8 bytes (IEEE 754) | score_01_adj, sum_val_recarga_m1, desconto_rate_m1 |
| `TIMESTAMP` | datetime | 8 bytes | gold_build_date, metadata_ingestion_timestamp |
| `DATE` | date | 4 bytes | dt_safra |

### Estratégia de Null Handling

**Coalesce para Zero (Count-based features):**
```python
F.coalesce(F.col("qtd_recargas_m1"), F.lit(0))
```
→ Indica "cliente sem atividade no período"

**Coalesce para 0.0 (Proportion/Rate features):**
```python
F.coalesce(F.col("desconto_rate_m1"), F.lit(0.0))
```
→ Indica "cliente sem transações / proporção nula"

**NULL Preservado (Flags e Categorias):**
```python
-- flag_teve_wo sem coalesce
```
→ NULL = "informação não disponível", mantém sinal de missingness

---

## 📊 Resumo Estatístico por Bloco

| Bloco | Features | Coverage Min | Coverage Max | Grão | Status |
|-------|----------|--------------|--------------|------|--------|
| **Identificação** | 3 | 100% | 100% | NUM_CPF + SAFRA | ✅ Completo |
| **Target/Decision** | 2 | 100% | 100% | NUM_CPF + SAFRA | ✅ Completo |
| **Score (v1-v2)** | 4 | 98.18% | 99.95% | NUM_CPF + SAFRA | ✅ Completo |
| **Telco (v3)** | 136 | 20.51% | 20.51% | NUM_CPF + SAFRA | ✅ Completo |
| **Cadastro (v4)** | 33/33 | 35-40% | 35-40% | NUM_CPF + SAFRA | ✅ **COMPLETO** (var_03-09 restauradas) |
| **Recarga (v5)** | 18 | 56.12% | 56.12% | NUM_CPF + SAFRA | ✅ Completo |
| **Pagamento (v6)** | 39 | 17.09% | 17.09% | NUM_CPF + SAFRA | ✅ Completo |
| **Atraso (v6)** | 57 | 22.43% | 22.43% | NUM_CPF + SAFRA | ✅ Completo |
| **Enhancement (v6.1)** | 9 | 100% | 100% | NUM_CPF + SAFRA | ✅ Completo (DESCONTO_RATE=const) |
| **Metadados** | 5 | 100% | 100% | Registro | ✅ Completo |
| **TOTAL** | **296** | — | — | — | **✅ FINAL** |

**✅ ENTREGA FINAL (26/01/2026):**
- **Cadastro**: 7 variáveis numéricas (var_03-09) restauradas via correção no Silver
- **DESCONTO_RATE**: Validado como constante=0.0 (política de negócio, sem descontos)
- **Total Features**: 296 (v6: 287 + v6.1: 9)
- **Status**: Pronto para modelagem | KS esperado: 45-46% OOT

---

## 🎯 Guia de Uso para Data Scientists

### 1. Preparação para Model Training

```python
# Carregar ABT v6.1
df_abt = spark.read.table("hackathon_2025.default.gold_abt_v6_1")

# CRÍTICO: Filtrar apenas registros com FLAG_INSTALACAO_INT = 1
df_train = df_abt.filter(F.col("flag_instalacao_int") == 1)
print(f"Registros para training: {df_train.count():,}")  # Expected: 2,633,900

# CRÍTICO: Target é observado neste subconjunto
target = "fpd_int"
features = [col for col in df_train.columns 
            if col not in ["num_cpf", "safra", "dt_safra", "fpd_int", "flag_instalacao_int", "gold_*", "metadata_*"]]
print(f"Features para modelo: {len(features)}")  # Expected: ~280
```

### 2. Feature Selection

**Recomendado iniciar por:**
1. Score_01 + Score_02 (baseline)
2. Adicionar Atraso (delinquency_rate + max_dias_atraso)
3. Adicionar Pagamento (qtd/sum values)
4. Adicionar Recarga (quantidade e valores)
5. Adicionar Telco (para refino)

**Features Enhancement (v6.1) a considerar:**
- `delinquency_rate_m{1,3,6}` ← **FORTE, espera-se alta correlação com FPD**
- `max_dias_atraso_m{1,3,6}` ← **FORTE, dívida crônica = risco**
- `desconto_rate_m{1,3,6}` ← Validar importância (todos zeros no dado atual)

### 3. Validação de Cobertura

Verificar antes de treinar:

```python
coverage_threshold = 0.90  # Mínimo 90%

for col in features:
    non_null_pct = df_train.filter(F.col(col).isNotNull()).count() / df_train.count()
    if non_null_pct < coverage_threshold:
        print(f"⚠️  {col}: {non_null_pct*100:.2f}% coverage")
```

### 4. Expected Model Performance

**Baseline (v6):** KS ≈ 44-45% OOT  
**Com Enhancement (v6.1):** KS ≈ 45-46% OOT (delta: +1-1.5pp)

---

## 📚 Referências

- [docs/target_definition.md](../target_definition.md) — Definição temporal do target e anti-leakage
- [docs/01_data_dictionary/](../01_data_dictionary/) — Dicionários detalhados por fonte
- [docs/04_gold_rules/abt_v1.md](./abt_v1.md) — v1 base
- [docs/06_abt_v6_docs/ABT_V6_1_IMPLEMENTATION_GUIDE.md](./ABT_V6_1_IMPLEMENTATION_GUIDE.md) — Guia técnico de implementação
- [src/utils/validate_abt.py](../../src/utils/validate_abt.py) — Validação com 17 gates

---

**Criado em:** 2026-01-26  
**Próximo Step:** Treinar modelo com target FPD_INT (FLAG_INSTALACAO_INT=1)  
**KS Esperado:** 45-46%
