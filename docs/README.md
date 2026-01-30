# Guia de Navegação da Documentação

Este guia ajuda você a encontrar a documentação correta para seu papel e tarefa.

**Última Atualização:** 30 Jan 2026

---

## Links Rápidos

| Eu quero... | Ir para |
|-------------|---------|
| Entender o projeto | `00_project/overview.md` |
| Saber qual é o target | `00_project/target_definition.md` |
| Consultar definição de uma feature | `04_gold_rules/BOOK_VARIABLES_ABT_V6.md` |
| Executar o pipeline | Ver `README.md` na raiz |
| Corrigir um problema de dados | `07_troubleshooting/` |
| Entender uma fonte de dados | `01_data_dictionary/` |

---

## Estrutura de Pastas

```
docs/
├── README.md                        # Este guia de navegação
│
├── 00_project/                      # Documentação do projeto
│   ├── overview.md                  # Metodologia CRISP-DM, objetivos
│   ├── target_definition.md         # Target FPD, regras anti-vazamento
│   └── glossary.md                  # Glossário de risco de crédito
│
├── 01_data_dictionary/              # Dicionários de dados (7 arquivos)
│   ├── bureau.md                    # Score_01, Score_02, spine
│   ├── bureau_full.md               # Versão completa com aprovados/reprovados
│   ├── telco.md                     # 68 variáveis anônimas (var_26-93)
│   ├── cadastro.md                  # Dados cadastrais, idade, CEP
│   ├── recarga.md                   # Eventos de recarga (95M+)
│   ├── pagamento.md                 # Histórico de pagamentos (21M+)
│   └── atraso.md                    # Faturas em aberto (31M+)
│
├── 02_data_quality/                 # Relatórios de qualidade (7 arquivos)
│   └── [mesmo padrão por fonte]
│
├── 03_silver_rules/                 # Regras de transformação Silver (7 arquivos)
│   └── [mesmo padrão por fonte]
│
├── 04_gold_rules/                   # Especificações Gold ABT
│   ├── 00_QUICK_START.md            # Como executar v1-v6
│   ├── README.md                    # Índice das especificações
│   ├── BOOK_VARIABLES_ABT_V6.md     # Dicionário completo (614 vars)
│   ├── abt_v1.md                    # Especificação ABT v1 (baseline)
│   ├── abt_v2.md                    # Especificação ABT v2
│   ├── abt_v3.md                    # Especificação ABT v3
│   └── abt_v4.md                    # Especificação ABT v4
│
├── 05_abt_v5_docs/                  # Documentação Recarga (ABT v5)
│   ├── abt_v5.md                    # Especificação técnica ABT v5
│   ├── VARIABLE_BOOK_RECARGA_V2.md  # Book de variáveis de Recarga
│   └── GOLD_RECARGA_FEATURES_V2.md  # Documentação do gerador de features
│
├── 06_abt_v6_docs/                  # Documentação Pagamento/Atraso (ABT v6)
│   ├── abt_v6.md                    # Especificação técnica ABT v6
│   ├── VARIABLE_BOOK_ABT_V6_1.md    # Book de variáveis v6.1
│   └── VARIABLE_BOOK_ABT_V6_1.pdf   # Versão PDF
│
├── 07_troubleshooting/              # Guias de correção e diagnósticos
│   ├── FIX_IDADE_ANOS_EXECUTION_GUIDE.md   # Correção de colunas vazias (UDF)
│   ├── CADASTRO_NUMERIC_VARS_FIX.md        # Correção vars numéricas cadastro
│   └── DIAGNOSTICO_DESCONTO_RATE.md        # Diagnóstico taxa de desconto
│
└── 99_archive/                      # Documentação histórica/obsoleta
    ├── abt_v5_docs/                 # Docs redundantes de v5
    ├── abt_v6_docs/                 # Docs redundantes de v6
    └── [arquivos arquivados da raiz]
```

---

## Por Papel

### Engenheiros de Dados

**Comece aqui:**
1. `00_project/overview.md` - Entenda a arquitetura Medallion
2. `04_gold_rules/00_QUICK_START.md` - Como executar os pipelines

**Referência:**
- `03_silver_rules/` - Lógica de transformação por fonte
- `07_troubleshooting/` - Problemas comuns e correções

**Scripts relacionados:**
```
src/jobs/
├── 00_bronze/     # Landing → Bronze
├── 01_silver/     # Bronze → Silver
└── 02_gold/       # Silver → Gold (ABT builders + feature generators)
```

---

### Cientistas de Dados

**Comece aqui:**
1. `00_project/target_definition.md` - O que estamos prevendo (FPD)
2. `04_gold_rules/BOOK_VARIABLES_ABT_V6.md` - Dicionário de features (614 vars)

**Referência:**
- `05_abt_v5_docs/VARIABLE_BOOK_RECARGA_V2.md` - Features de Recarga
- `06_abt_v6_docs/VARIABLE_BOOK_ABT_V6_1.md` - Features de Pagamento/Atraso

**Tabelas principais:**
```sql
-- ABT final para modelagem
SELECT * FROM hackathon_2025.default.gold_abt_v6_v2;

-- Features individuais
SELECT * FROM hackathon_2025.default.gold_recarga_features_v2;
SELECT * FROM hackathon_2025.default.gold_pagamento_features_v2;
SELECT * FROM hackathon_2025.default.gold_atraso_features_v2;
```

---

### Novos Membros do Time

**Sequência recomendada:**
1. Leia `00_project/overview.md` primeiro
2. Leia `00_project/target_definition.md`
3. Consulte `00_project/glossary.md` para terminologia
4. Veja `README.md` na raiz para status do projeto

---

## Conceitos-Chave

### Arquitetura Medallion

```
LANDING (Bruto) → BRONZE (+ metadados) → SILVER (tipado) → GOLD (ABT)
```

| Camada | Propósito | Exemplo |
|--------|-----------|---------|
| **Landing** | Dados brutos (Parquet) | Arquivos originais |
| **Bronze** | + metadados de ingestão | `bureau_delta/` |
| **Silver** | Tipado, validado, deduplicado | `bureau_silver_delta/` |
| **Gold** | Features agregadas, ABT final | `abt_v6_v2_delta/` |

### Versões da ABT (Incremental)

| Versão | Adicionado | Colunas | Documentação |
|--------|------------|---------|--------------|
| v1 | Score_01 | ~10 | `04_gold_rules/abt_v1.md` |
| v2 | + Score_02 | ~12 | `04_gold_rules/abt_v2.md` |
| v3 | + Telco (68 vars) | ~82 | `04_gold_rules/abt_v3.md` |
| v4 | + Cadastro (33 vars) | 185 | `04_gold_rules/abt_v4.md` |
| v5 | + Recarga (M1/M3/M6) | 311 | `05_abt_v5_docs/abt_v5.md` |
| **v6** | + Pagamento + Atraso | **614** | `06_abt_v6_docs/abt_v6.md` |

### Janelas Temporais

| Sufixo | Significado | Uso |
|--------|-------------|-----|
| `_m1` | Último 1 mês | Features recentes |
| `_m3` | Últimos 3 meses | Tendência curto prazo |
| `_m6` | Últimos 6 meses | Comportamento histórico |

### Regras Anti-Vazamento

| Coluna | Papel | Regra |
|--------|-------|-------|
| `fpd_int` | TARGET | NUNCA usar como feature |
| `flag_instalacao_int` | Decisão | NUNCA usar como feature |

**Importante:** Treinar apenas em `flag_instalacao_int=1` (onde FPD é observado)

---

## Principais Features por Bloco

### Score (Bureau)
- `score_01` - Score principal de bureau
- `score_02` - Score secundário de bureau

### Telco (68 variáveis anônimas)
- `var_26_adj` até `var_93_adj` - Variáveis de comportamento telco

### Cadastro
- `idade_anos` - Idade do cliente
- `cep_3_digitos` - Proxy geográfico

### Recarga (Indicadores de Estresse Financeiro)
- `freq_sos_m1` - Frequência de SOS (empréstimo)
- `ticket_medio_m1` - Valor médio de recarga
- `coef_variacao_val_m1` - Instabilidade de valores

### Pagamento (Comportamento de Atraso)
- `pct_pagamentos_com_juros_m1` - % pagamentos com juros
- `ratio_juros_pago_m1` - Intensidade de juros

### Atraso (Risco Atual)
- `pct_aging_90_plus_m1` - % inadimplência >90 dias
- `flag_risco_alto_m1` - Flag WO/PDD/Fraude

---

## Troubleshooting Comum

| Problema | Solução |
|----------|---------|
| Coluna vazia (NULL) | Ver `07_troubleshooting/FIX_IDADE_ANOS_EXECUTION_GUIDE.md` |
| Variáveis numéricas erradas | Ver `07_troubleshooting/CADASTRO_NUMERIC_VARS_FIX.md` |
| Taxa de desconto estranha | Ver `07_troubleshooting/DIAGNOSTICO_DESCONTO_RATE.md` |

**Dica:** Se uma coluna existe mas está toda NULL, verifique se há UDFs Python no script Silver. UDFs falham silenciosamente no Databricks.

---

## Arquivo (99_archive/)

Documentação histórica preservada em `99_archive/` para referência. Estes arquivos estão desatualizados mas podem conter contexto útil.

**Arquivos arquivados da raiz:**
- Roadmaps antigos (DATA_ENGINEERING_ROADMAP.md, etc.)
- Análises pontuais (ANALISE_*.md)
- Sumários redundantes (DELIVERY_SUMMARY.md, etc.)

**Arquivos arquivados de ABT:**
- Checklists de entrega
- Índices redundantes
- Sumários de implementação

---

## Manutenção da Documentação

### Ao adicionar nova versão da ABT:
1. Criar `docs/0X_abt_vX_docs/abt_vX.md`
2. Atualizar `docs/04_gold_rules/00_QUICK_START.md`
3. Atualizar este README

### Ao corrigir um problema:
1. Documentar em `docs/07_troubleshooting/`
2. Atualizar `.claude/CLAUDE.md` se relevante

---

**Última Atualização:** 30 Jan 2026 | **Próxima Revisão:** Quando nova versão da ABT for criada
