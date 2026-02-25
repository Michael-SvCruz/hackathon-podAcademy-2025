# Fase 6B — Gold Features (Silver → Gold)

> **Status:** ✅ Scripts prontos (3 scripts optz + 3 originais em opc_standby). Aguardando execução no Data Flow após Silver concluída.

---

## Visão Geral

A camada Gold Features agrega os dados Silver (grain múltiplo por CPF) em features comportamentais mensais (grain 1:1 por NUM_CPF + SAFRA). Essas features são consumidas pelos scripts ABT via LEFT JOIN com o spine (bureau).

```
Silver Layer
    │
    ├── silver_atraso    ──→  gold_atraso.py    → gold_atraso_features/
    ├── silver_pagamento ──→  gold_pagamento.py → gold_pagamento_features/
    └── silver_recarga   ──→  gold_recarga.py   → gold_recarga_features/
                                    │
                                    ▼
                           ABT v5+ (recarga)
                           ABT v6  (+ pagamento + atraso)
```

> **Nota:** Bureau, Telco e Cadastro têm grain 1:1 na Silver — não precisam de Gold Features. São usados diretamente nos ABTs.

---

## Padrão dos Scripts (optz — Principal)

Todos os scripts Gold seguem o padrão **optz**: sem materialização prévia, quality check no arquivo já gravado.

### Arquitetura (2 actions)

```
action 1: groupBy(...).agg(...) + write()  → agrega Silver e grava Gold
action 2: agg(Gold escrita)               → quality check no arquivo real
```

**Versus padrão original (5 actions no gold_recarga):**
```
action 1: count(Silver)              → log de volume de entrada
action 2: count(Gold pré-escrita)    → log de compressão
action 3: groupBy safras + count()   → distribuição de safras
action 4: select stats               → médias e contagens
action 5: write()                    → grava Gold
```

---

## Scripts e Particularidades por Fonte

### gold_atraso.py

| Item | Detalhe |
|------|---------|
| **Input** | `oci://.../silver-layer@{ns}/atraso/` |
| **Output** | `oci://.../gold-layer@{ns}/gold_atraso_features/` |
| **Grain Silver → Gold** | N linhas/CPF → 1 linha por NUM_CPF + SAFRA_ATRASO |
| **Dedup** | Nenhum — groupBy já consolida |
| **Particularidade** | Sem dedup pré-gold (atraso tem grain múltiplo por design) |
| **Particionamento** | `partitionBy("safra_atraso")` |
| **appName** | `gold_atraso_optz` |

**Features geradas (principais):**

| Categoria | Features |
|-----------|---------|
| Faturas abertas | `qtd_faturas_abertas_mes`, `qtd_contratos_com_atraso_mes` |
| Valores | `sum_val_aberto_mes`, `avg_val_aberto_mes`, `ticket_medio_aberto_mes` |
| Aging | `qtd_aging_0_30_mes`, `pct_aging_90_plus_mes`, `pct_val_aging_90_plus_mes` |
| Risco | `flag_teve_wo_mes`, `flag_teve_pdd_mes`, `flag_teve_fraude_mes` |
| Recuperação | `flag_teve_aca_mes`, `flag_teve_pccr_mes` |
| Comportamento | `flag_atraso_grave_mes`, `flag_risco_alto_mes`, `flag_em_recuperacao_mes` |

**Quality gates:**
- Unicidade: `count == countDistinct(num_cpf, safra_atraso)`
- Gate 1: `num_cpf` nulos = 0
- Métricas: % risco alto, % sem atraso, % atraso grave (>50% em 90+)

---

### gold_pagamento.py

| Item | Detalhe |
|------|---------|
| **Input** | `oci://.../silver-layer@{ns}/pagamento/` |
| **Output** | `oci://.../gold-layer@{ns}/gold_pagamento_features/` |
| **Grain Silver → Gold** | N linhas/CPF → 1 linha por NUM_CPF + SAFRA_PAGAMENTO |
| **Particionamento** | `partitionBy("safra_pagamento")` |
| **appName** | `gold_pagamento_optz` |

**Features geradas (principais):**

| Categoria | Features |
|-----------|---------|
| Volume | `qtd_pagamentos_validos_mes`, `qtd_faturas_distintas_mes` |
| Valores | `sum_val_pago_mes`, `ticket_medio_pagamento_mes` |
| Descontos | `sum_val_desconto_mes`, `pct_pagamentos_com_desconto_mes`, `ratio_desconto_pago_mes` |
| Juros (atraso passado) | `sum_val_juros_pos_mes`, `pct_pagamentos_com_juros_mes`, `ratio_juros_pago_mes` |
| Formas de pagamento | `qtd_formas_pagamento_distintas_mes`, `pct_forma_dominante_mes` |
| Comportamento | `flag_sem_pagamento_mes`, `flag_sempre_com_juros_mes`, `flag_alto_desconto_mes` |

**Quality gates:**
- Unicidade: `count == countDistinct(num_cpf, safra_pagamento)`
- Gate 1: `num_cpf` nulos = 0
- Métricas: % sem pagamento, % sempre com juros, % alto desconto

---

### gold_recarga.py

| Item | Detalhe |
|------|---------|
| **Input** | `oci://.../silver-layer@{ns}/recarga/` |
| **Output** | `oci://.../gold-layer@{ns}/gold_recarga_features/` |
| **Grain Silver → Gold** | ~95M eventos → 1 linha por NUM_CPF + SAFRA_RECARGA |
| **Particularidade** | Script mais complexo: SOS adjustment, métricas temporais com Window, classificação de tipo de transação |
| **Particionamento** | `partitionBy("safra_recarga")` |
| **appName** | `gold_recarga_optz` |
| **Actions reduzidas** | **5 → 2** (maior ganho dos 3 Gold) |

**Features geradas (principais):**

| Categoria | Features |
|-----------|---------|
| Volume | `qtd_recargas_mes`, `qtd_recargas_validas_mes`, `qtd_telefones_distintos_mes` |
| SOS (estresse financeiro) | `qtd_sos_mes`, `freq_sos_mes`, `pct_sos_sobre_credito_mes`, `flag_teve_sos_mes` |
| Valores ajustados | `sum_val_real_ajustado_mes`, `ticket_medio_mes`, `coef_variacao_val_mes` |
| Tempo entre recargas | `dias_medio_entre_recargas_mes`, `dias_max_entre_recargas_mes` |
| Horário/período | `pct_recargas_madrugada_mes`, `pct_recargas_fim_semana_mes` |
| Tipos de transação | `qtd_pago_puro_mes`, `qtd_bonus_puro_mes`, `qtd_combo_mes` |

**Quality gates:**
- Unicidade: `count == countDistinct(num_cpf, safra_recarga)`
- Gate 1: `num_cpf` nulos = 0
- Métricas: % com SOS, % sem recarga, % baixa atividade, média recargas/mês, ticket médio

**Regras de negócio críticas (Claro/Fernando — 07/01/2026):**
- **SOS** = empréstimo R$3-20, descontado da próxima recarga → não é "dinheiro real"
- **Frequência de SOS** = indicador de estresse financeiro
- `val_real_ajustado` = crédito real desconsiderando SOS e bonus

---

## Estrutura de Arquivos

```
mig_oci/data_upload/scripts/
├── gold_atraso.py          ← principal (optz): 2 actions
├── gold_pagamento.py       ← principal (optz): 2 actions
├── gold_recarga.py         ← principal (optz): 2 actions (reduzido de 5)
└── opc_standby/
    ├── gold_atraso_original.py     ← 3 actions, sem quality check
    ├── gold_pagamento_original.py  ← 3 actions, sem quality check
    └── gold_recarga_original.py    ← 5 actions (com gerar_relatorio_qualidade())
```

---

## Convenção de Nomes

| Item | Padrão |
|------|--------|
| **appName (principal)** | `gold_X_optz` |
| **appName (original)** | `gold_X_original` |
| **Output path** | `oci://hackathon-2025-gold-layer@{namespace}/gold_X_features/` |

O `appName` é visível nos logs do Data Flow Console → rastreabilidade imediata de qual versão rodou.

---

## Ordem de Execução

Os Gold Features devem ser executados **após** as respectivas Silvers:

```
silver_recarga   ──→  gold_recarga   (usado em ABT v5+)
silver_pagamento ──→  gold_pagamento (usado em ABT v6)
silver_atraso    ──→  gold_atraso    (usado em ABT v6)
```

O `gold_recarga` pode rodar em paralelo com `gold_pagamento` e `gold_atraso` (fontes independentes).

---

## Lições Aprendidas

| Problema | Solução |
|----------|---------|
| `count_silver` + `count_gold` antes da escrita = actions extras | Removidos no optz — escrever direto e validar na Gold gravada |
| `gerar_relatorio_qualidade()` fazia 3 actions separadas | Consolidado em 1 `agg()` na Gold escrita (gold_recarga: 5 → 2 actions) |
| Quality check no DataFrame em memória não valida dado persistido | Sempre ler da Gold gravada (`spark.read.format("delta").load(...)`) |
| Output paths com sufixo `_v2` (ex: `pagamento_features_v2/`) | Padronizado para `gold_X_features/` (sem versão no path) |
