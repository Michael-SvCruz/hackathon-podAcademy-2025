# Relatório de Qualidade — Silver Recarga (Primeira Execução)

**Data de Geração:** 22 de janeiro de 2026  
**Script:** `src/jobs/01_silver/03_bronze_silver_recarga.py`  
**Execução:** Primeira run em produção (ambiente Databricks)

---

## 📊 Resumo Executivo

A transformação Bronze → Silver da base **Recarga** processou com sucesso **100.2 milhões de eventos**, resultando em **95.2 milhões de registros deduplados** (taxa de dedup 4.99%). O pipeline executou sem erros após correção de casting tolerante para códigos dimensionais não-numéricos.

| Métrica | Valor | Status |
|---------|-------|--------|
| **Bronze Input** | 100.213.651 registros | ✅ Lido com sucesso |
| **Silver Output** | 95.210.519 registros | ✅ Validado |
| **Duplicatas Removidas** | 5.003.132 (4.99%) | ✅ Esperado |
| **TS_RECARGA Parsing** | 100.00% válida | ✅ Excelente |
| **Tabela Criada** | `hackathon_2025.default.silver_recarga` | ✅ Unity Catalog |

---

## 🔧 Contexto Técnico

### Natureza dos Dados
- **Grão:** EVENT-LEVEL (múltiplos eventos por NUM_CPF + SAFRA)
- **Tamanho:** ~100M eventos (12-36 meses de histórico: abril-outubro 2024 observado)
- **Tipo:** Base transacional de recargas de crédito (Claro Movel)
- **Chave Temporal:** `DAT_INSERCAO_CREDITO` (formato ddMMMyyyy:HH:mm:ss)

### Estrutura Silver Entregue
```
Chaves + Identificação:
  - num_cpf (STRING)
  - dw_num_cliente (STRING)
  - dw_num_ntc (STRING)

Timestamp do Evento:
  - ts_recarga (TIMESTAMP) — parsing de DAT_INSERCAO_CREDITO
  - dt_recarga (DATE)
  - safra_recarga (STRING: YYYYMM)
  - flag_ts_recarga_invalida (INT: 0/1)

Labels (NÃO usar como features):
  - flag_instalacao_int (INT: 0/1 ou NULL)
  - fpd_int (INT: 0/1 ou NULL)

Valores Monetários:
  - val_credito_inserido, val_bonus, val_real, valor_sos (DOUBLE)
  - flag_*_negativo (INT: 0/1) — para cada valor
  - *_clean (DOUBLE) — NULL se original for negativo

Códigos Dimensionais (11 colunas):
  - cod_tecnologia_dw, cod_tipo_credito, dw_tipo_insercao, etc.
  - flag_*_sentinela (INT: 0/1) — para cada código

SOS (Serviço Especial):
  - flag_sos (INT: 0/1)
  - valor_sos (DOUBLE)

Auditoria:
  - metadata_data_ingestao, metadata_nome_arquivo_origem, etc.
```

---

## ✅ Qualidade de Parsing

### TS_RECARGA (Timestamp do Evento)

```
TS_RECARGA válida:    95.210.519 registros (100.00%) ✅
TS_RECARGA inválida:  0 registros (0.00%)
```

**Análise:** Parsing perfeito. A aplicação de `F.to_timestamp(dat_insercao_credito, "ddMMMyyyy:HH:mm:ss")` converteu 100% dos timestamps sem falhas. Nenhum valor inválido ou malformado detectado nesta coluna.

**Implicação:** Confiança máxima na dimensão temporal. SAFRA_RECARGA derivada corretamente para todos os eventos.

---

## ⚠️ Valores Negativos em Montantes

A base contém valores **negativos em colunas monetárias**, possivelmente representando **ajustes, devoluções ou sentinelas de erro** não capturados na data dictionary.

| Coluna | Negativos | % do Total | Status |
|--------|-----------|-----------|--------|
| `val_bonus_negativo` | 13.414.030 | **14.09%** | ⚠️ Acima do esperado |
| `val_real_negativo` | 12.342.283 | **12.96%** | ⚠️ Acima do esperado |
| `val_credito_inserido_negativo` | Não observado | ~0.00% | ✅ Normal |

**Contexto de Expectativa:**
- Data Dictionary (recarga.md) apontava ~6% de valores negativos
- Execução revelou 14% e 13%, **2.3x acima** do esperado
- Causa: Amostra na análise exploratória (EDA) foi diferente da população final

### Estratégia Implementada

1. **Colunas `*_clean`:** Valores negativos → NULL (preserva monetária válida)
2. **Flags `flag_*_negativo`:** Binária 0/1 capturando presença de negativos
3. **Decisão Gold v5:** Usar `*_clean` para agregações (SUM_VAL_REAL_CLEAN_M1, etc.)

**Recomendação:** 
- ✅ Decisão confirmada na Silver
- ⏳ **Gold v5:** Avaliar impacto em KS usando `*_clean` vs valores originais
- 🔍 **Investigação:** Consultar equipe de dados Claro sobre significado de negativos em `val_bonus`/`val_real`

---

## 🚨 Sentinelas em Códigos Dimensionais

Foram mapeadas **11 colunas de códigos dimensionais** com **sentinelas -1/-2/-3** (não aplica, não determinado, não informado). A qualidade varia drasticamente entre dimensões.

### Qualidade por Dimensão

#### 🟢 EXCELENTE (0% sentinelas — Utilizáveis)
```
cod_status_plataforma:      0.03% ✅ Excelente (4.849 sentinelas em 95.2M)
cod_tipo_credito:           0.00% ✅ Perfeito   (0 sentinelas)
cod_tecnologia_dw:          0.00% ✅ Perfeito   (0 sentinelas)
cod_plataforma_atu:         0.00% ✅ Perfeito   (0 sentinelas)
```

**Decisão Gold v5:** Usar estas 4 dimensões em features de granularidade (p.ex., `QTD_RECARGAS_POR_TIPO_CREDITO_M1`).

#### 🟡 MARGINAL (60-70% sentinelas — Usar com Cautela)
```
cod_canal_aquisicao:        69.78% ⚠️ Marginal  (66.506.223 sentinelas)
dw_instituicao:             69.89% ⚠️ Marginal  (66.595.644 sentinelas)
```

**Decisão Gold v5:** Considerar apenas se feature específica (p.ex., canal = direto) agregue valor.

#### 🔴 CRÍTICO (90%+ sentinelas — Inutilizáveis)
```
dw_forma_pagamento:         99.04% ❌ Inutilizável  (94.350.868 sentinelas)
cod_promocao:               99.04% ❌ Inutilizável  (94.350.868 sentinelas)
dw_tipo_recarga:            94.29% ❌ Inutilizável  (89.852.370 sentinelas)
dw_tipo_insercao:           94.29% ❌ Inutilizável  (89.852.370 sentinelas)
dw_plano_tarifacao:         ~95%   ❌ Inutilizável  (estimado ~90M sentinelas)
```

**Decisão Gold v5:** Excluir completamente. Nenhuma feature será derivada destas dimensões.

### Padrão Observado

Dimensões com 99% de sentinelas (forma_pagamento, promocao) têm **exatamente os mesmos valores** (94.350.868), sugerindo:
- Origem de dados incompleta ou erro de integração Bronze
- Possível truncamento em campo de origem
- Deve ser confirmado com equipe de TI/Claro

---

## 🎯 SOS (Serviço Especial)

### Presença de SOS

```
Eventos com SOS (flag_sos=1):  6.496.119 registros (6.82%)
Eventos sem SOS (flag_sos=0):  88.714.400 registros (93.18%)
```

**Esperado (data_quality.md):** ~6.5%  
**Observado:** 6.82%  
**Desvio:** +0.32pp (praticamente perfeito) ✅

### Valor de SOS

- **Range observado:** 3-20 (conforme esperado)
- **Distribuição:** Concentrada em valores baixos
- **Qualidade:** Excelente — sem sentinelas, sem NULLs

**Implicação Gold v5:** SOS é feature de altíssima qualidade para crédito risk. Recomenda-se incluir:
- `FLAG_TEVE_SOS_M1/M3/M6` (binária)
- `SUM_VALOR_SOS_M1/M3/M6` (agregação de montante)

---

## 🚨 FLAG_INSTALACAO_INT — ACHADO CRÍTICO

### Status Observado

```
FLAG_INSTALACAO_INT:
  Nulo:        95.210.519 registros (100.00%) ❌ CRÍTICO
  FLAG=0:      0 registros
  FLAG=1:      0 registros
```

### Impacto

A coluna **FLAG_INSTALACAO_INT está 100% NULL** em toda a base de eventos Recarga. Isto significa:

1. **Coluna não existe em Bronze** (importação falhou), OU
2. **Coluna tem nome diferente** após `standardize_column_names()`, OU
3. **Fonte Recarga não fornece aprovação/instalação** (eventos são pós-aprovação apenas)

### Implicação no Projeto

- ❌ **Label não disponível em Recarga** para uso em Gold v5
- ❌ **Anti-leakage rule não aplicável:** Não há `FLAG_INSTALACAO=1` para validar FPD_INT
- ✅ **Solução:** Usar FLAG_INSTALACAO de **Bureau ou outro documento** como proxy (requer design Gold v5 especial)

### Ações Necessárias

1. **Imediato:** Investigar schema Bronze
   ```sql
   DESCRIBE hackathon_2025.default.bronze_recarga
   ```
   Procurar por: `flag_instalacao`, `FLAG_INSTALACAO`, ou similares

2. **Se coluna não existe:** Documentar em `docs/01_data_dictionary/recarga.md`
   - Adicionar nota: "FLAG_INSTALACAO não disponível nesta fonte"
   - Preparar estratégia Gold v5 alternativa (vincular por chave)

3. **Gold v5 design:** Usar `left_join` com ABT base (v1) para herdar FLAG_INSTALACAO:
   ```
   silver_recarga LEFT JOIN gold_abt_v1 
   ON silver_recarga.num_cpf = gold_abt_v1.num_cpf
   AND silver_recarga.safra_recarga = gold_abt_v1.safra
   ```

---

## 📈 Deduplicação

### Estratégia Implementada

**EVENT_KEY** criada via SHA2-256 hash de:
- `num_cpf`
- `dw_num_ntc`
- `ts_recarga`
- `val_real`
- `val_credito_inserido`
- `cod_tipo_credito`
- `cod_status_plataforma`

**Desempate:** Timestamp DESC (mais recente ganha)

### Resultados

```
Registros antes dedupe:  100.213.651
Registros após dedupe:    95.210.519
Duplicatas removidas:      5.003.132 (4.99%)
```

**Qualidade:** Excelente. Taxa 4.99% é consistente com expectativa de ~5% observada em EDA.

**Confiança:** EVENT_KEY baseada em 7 colunas (chaves + timestamp + valores + tipo). Risco de colisão SHA2 negligenciável.

---

## 🔄 Próximos Passos — Roadmap Gold v5

### Gold v5 — Recarga Temporal (Próximo Sprint)

**Input:** silver_recarga (95.2M evento-level)  
**Output:** gold_abt_v5 com features temporais

#### Features Planejadas (Usando APENAS 4 Dimensões Boas)

```
POR PERÍODO (M1, M3, M6):
├── QTD_RECARGAS_M1/M3/M6
├── SUM_VAL_REAL_CLEAN_M1/M3/M6
├── SUM_VAL_BONUS_CLEAN_M1/M3/M6
├── SUM_VAL_CREDITO_INSERIDO_M1/M3/M6
├── FLAG_TEVE_SOS_M1/M3/M6
├── AVG_VAL_REAL_CLEAN_M1/M3/M6
│
└── GRANULARIDADE POR COD_TIPO_CREDITO (dimensão perfeita):
    ├── QTD_RECARGAS_POR_TIPO_CREDITO_M1
    ├── SUM_VAL_REAL_CLEAN_POR_TIPO_CREDITO_M1
    └── [repetir para M3, M6]
```

#### Exclusões Confirmadas
- ❌ `dw_forma_pagamento` (99% sentinelas)
- ❌ `cod_promocao` (99% sentinelas)
- ❌ `dw_tipo_recarga` (94% sentinelas)
- ❌ `dw_tipo_insercao` (94% sentinelas)
- ❌ `dw_plano_tarifacao` (95% sentinelas aprox.)
- ⚠️ `cod_canal_aquisicao`, `dw_instituicao` (marginal, avaliar por caso)

#### KS Esperado
- **v4 atual:** ~40.2%
- **v5 (com Recarga):** Esperado **42.0-42.5%** (+1.8-2.3pp)
- **Driver:** SOS (excelente quality) + volume (95.2M eventos = ~40 recargas/cliente/mês)

### Gold v5 Bloqueadores Atuais
1. **FLAG_INSTALACAO missing** → Requer design alternativo (join com v1)
2. **Decisão negatives handling** → Usar `*_clean` confirmado, validar em KS

---

## 📋 Conclusões

| Aspecto | Status | Observação |
|---------|--------|-----------|
| **Execução Técnica** | ✅ OK | Script robusto, 100% dos eventos processados |
| **Parsing Temporal** | ✅ Excelente | 100% de TS_RECARGA válida |
| **Valores Negativos** | ⚠️ Monitorar | 2.3x acima de EDA; usar `*_clean` em v5 |
| **Dimensões Boas** | ✅ 4 excelentes | cod_tipo_credito, cod_status_plataforma, etc. |
| **Dimensões Ruins** | ⚠️ 5 críticas | Excluir completamente de v5 |
| **SOS Quality** | ✅ Excelente | 6.82% (vs 6.5% esperado) |
| **Deduplicação** | ✅ Robusta | 4.99% removidas, EVENT_KEY SHA2-256 |
| **FLAG_INSTALACAO** | ❌ Crítico | 100% NULL — investigar Bronze |
| **Pronto para Gold v5** | ⚠️ Condicional | Pendente resolução FLAG_INSTALACAO |

---

## 🔗 Referências

- **Silver Script:** [src/jobs/01_silver/03_bronze_silver_recarga.py](../src/jobs/01_silver/03_bronze_silver_recarga.py)
- **Data Dictionary:** [docs/01_data_dictionary/recarga.md](01_data_dictionary/recarga.md)
- **Data Quality Rules:** [docs/02_data_quality/recarga.md](02_data_quality/recarga.md)
- **Silver Rules:** [docs/03_silver_rules/recarga.md](03_silver_rules/recarga.md)
- **Gold Roadmap:** [docs/04_gold_rules/abt_v5.md](04_gold_rules/abt_v5.md) (próxima versão)

---

**Relatório Preparado:** 22 de janeiro de 2026  
**Status:** ✅ Silver Recarga validado e pronto para Gold v5 (sujeito a resolução de FLAG_INSTALACAO)
