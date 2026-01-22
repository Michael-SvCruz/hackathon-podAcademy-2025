# 🚀 GOLD v1 - QUICK START

## O que foi criado?

```
src/jobs/02_gold/
├── __init__.py
├── 00_gold_abt_builder.py           ← Script principal (RODAR ESTE!)
└── validators/
    ├── __init__.py
    └── validate_abt.py              ← Validações automáticas

docs/04_gold_rules/
└── abt_v1.md                        ← Especificação completa
```

---

## Como rodar?

### Option 1: Databricks Notebook (Desenvolvimento)
```python
%run /Workspace/src/jobs/02_gold/00_gold_abt_builder.py
```

### Option 2: Spark Submit (Produção)
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/00_gold_abt_builder.py \
  --silver_path "/Volumes/hackathon_2025/default/silver/bureau_full_silver_delta/" \
  --output_path "/Volumes/hackathon_2025/default/gold/abt_v1_delta/"
```

### Option 3: Modo Interativo (DEV)
```python
# Usa paths padrão automaticamente
python src/jobs/02_gold/00_gold_abt_builder.py
```

---

## O que acontece quando roda?

1. **Lê Silver Bureau** (spin com scores e labels)
2. **Seleciona colunas** para ABT v1 (score_01 como feature)
3. **Valida 6 gates** automaticamente
   - Unicidade ✓
   - FPD observado SÓ em FLAG_INSTALACAO=1 ✓
   - Sem NULLs em chaves ✓
   - Ambas classes presentes ✓
   - Score_01 com cobertura > 90% ✓
4. **Escreve Delta Lake** em Gold
5. **Registra tabela UC:** `hackathon_2025.default.gold_abt_v1`
6. **Exibe relatório** com distribuições

---

## Saídas Esperadas

```
Tabela: gold_abt_v1
Registros: ~1.2M (exemplo)

FLAG_INSTALACAO distribution:
  FLAG=0: 500K (42%)  ← Reprovados
  FLAG=1: 700K (58%)  ← Aprovados

FPD distribution (SÓ em FLAG=1):
  FPD=0: 600K (86%)   ← Bom pagador
  FPD=1: 100K (14%)   ← Risco

SCORE_01_ADJ: 100% de cobertura (após remover sentinela 0)
```

---

## Gates de Validação (Automáticos)

| Gate | O que verifica | Falha se |
|------|---|---|
| 1 | Unicidade 1:1 NUM_CPF+SAFRA | Há duplicatas |
| 2 | FPD nulo onde FLAG=0 | FPD não nulo com FLAG=0 |
| 3 | Chaves sem NULL | Há chave nula |
| 4 | FLAG tem 0 e 1 | Falta algum valor |
| 5 | FPD tem 0 e 1 | Falta classe |
| 6 | Score_01 > 90% | Cobertura < 90% |

Se algum gate falhar, o script para com erro descritivo. ⚠️

---

## Próximas Etapas

Após v1 estar OK:

1. **Testar modelo** com SCORE_01
   - Calcular KS no OOT (fev/mar)
   - Esperar ≈ 33,1

2. **Criar ABT v2**
   - Adiciona SCORE_02
   - Compara ΔKS

3. **Criar ABT v3**
   - Adiciona Telco features
   - Continua incremental...

---

## Importante: Anti-Leakage

⚠️ **NUNCA fazer:**
- Usar FPD como feature (é o target!)
- Usar FLAG_INSTALACAO como feature (é leakage!)

✅ **FAZER:**
- Treinar em FLAG_INSTALACAO=1 (onde FPD observado)
- Usar SÓ SCORE_01 como feature em v1
- Adicionar novos blocos em v2+ (incremental)

---

## Debug

Se der erro, cheque:

```python
# 1) Silver Bureau existe?
spark.table("silver_bureau").count()

# 2) Score_01 tem dados?
spark.table("silver_bureau").select("score_01_adj").describe().show()

# 3) FPD está onde FLAG=1?
spark.table("silver_bureau").filter("flag_instalacao_int=0").select("fpd_int").describe().show()
```

---

## Documentação Completa

Veja `docs/04_gold_rules/abt_v1.md` para:
- Schema completo
- Definições de cada coluna
- Detalhes de cada gate
- Roadmap v1→v6
- Referências

---

# 🚀 GOLD v5 - QUICK START

## O que foi criado?

```
src/jobs/02_gold/
├── 04_gold_abt_v5_builder.py       ← Script principal (RODAR ESTE!)
└── validators/
    └── validate_abt.py             ← Inclui validate_abt_v5 (10 gates)

docs/04_gold_rules/
└── abt_v5.md                       ← Especificação completa

docs/
└── 05_recarga_silver_quality_report.md   ← Quality insights
```

## Pré-requisitos

✅ **Gold ABT v4** já construído  
✅ **Silver Recarga** já construído e validado (95.2M eventos)  
✅ **Recarga Silver quality** conhecido (4 dimensões boas, 14% negativos esperados)

## Como rodar?

### Option 1: Databricks Notebook (Desenvolvimento)
```python
%run /Workspace/src/jobs/02_gold/04_gold_abt_v5_builder.py
```

### Option 2: Spark Submit (Produção)
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/04_gold_abt_v5_builder.py \
  --gold_v4_path "/Volumes/hackathon_2025/default/gold/abt_v4_delta/" \
  --silver_recarga_path "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/" \
  --output_path "/Volumes/hackathon_2025/default/gold/abt_v5_delta/"
```

### Option 3: Modo Interativo (DEV)
```python
# Usa paths padrão automaticamente
python src/jobs/02_gold/04_gold_abt_v5_builder.py
```

## O que acontece quando roda?

1. **Lê Gold ABT v4** (spine: 3.79M registros cliente-mês)
2. **Lê Silver Recarga** (eventos: 95.2M registros evento-level)
3. **Agrega Recarga** por cliente-mês com temporal windows:
   - M1: Último 1 mês
   - M3: Últimos 3 meses
   - M6: Últimos 6 meses
4. **Calcula features** (18 novas):
   - qtd_recargas_m1/m3/m6
   - sum_val_real_clean_m1/m3/m6
   - sum_val_bonus_clean_m1/m3/m6
   - sum_val_credito_inserido_clean_m1/m3/m6
   - avg_val_real_clean_m1/m3/m6
   - flag_teve_sos_m1/m3/m6
5. **Valida 10 gates** automaticamente (8 herdados de v4 + 2 novos)
6. **Escreve Delta Lake** em Gold v5
7. **Registra tabela UC:** `hackathon_2025.default.gold_abt_v5`
8. **Exibe relatório** com cobertura por bloco

## Saídas Esperadas

```
Tabela: gold_abt_v5
Registros: ~3.79M (mesmo de v4, mantém 1:1)
Colunas: ~195 (v4 columns + 18 Recarga features)

Coverage by block:
  Score_01:   98.18%
  Score_02:   99.95%
  Telco:      20.51%
  Cadastro:   30-40%
  Recarga:    35-45% (clientes com qtd_recargas_m1 > 0)

Recarga aggregates (mean):
  QTD_RECARGAS_M1:         8-12 eventos/cliente
  SUM_VAL_REAL_CLEAN_M1:   150-300 BRL/cliente
  SUM_VAL_BONUS_CLEAN_M1:  10-30 BRL/cliente

FPD distribution (inherited from v4):
  FPD=0: 86% (good payers)
  FPD=1: 14% (risk cases)
```

## Gates de Validação (Automáticos)

| Gate | O que verifica | Novo em v5 |
|------|---|---|
| 1-8 | Unicidade, FPD anti-leak, Keys, Flags, Score coverage, Telco, Cadastro | ❌ Herdado |
| 9 | **Recarga cobertura ≥ 5%** (clientes com qtd_recargas_m1 > 0) | ✅ NEW |
| 10 | **QTD_RECARGAS_M1 sanidade** (no NaNs, no Infs, min≥0) | ✅ NEW |

Se algum gate falhar, o script para com erro descritivo. ⚠️

## Decisões Técnicas Documentadas

### Dimensões Recarga Utilizadas
✅ **BOAS** (0% sentinelas):
- cod_tipo_credito
- cod_status_plataforma
- cod_tecnologia_dw
- cod_plataforma_atu

❌ **EXCLUÍDAS** (90%+ sentinelas):
- dw_forma_pagamento, cod_promocao, dw_tipo_recarga, dw_tipo_insercao, dw_plano_tarifacao

### Tratamento de Negativos
- Silver já criou colunas `*_clean` (NULL se < 0)
- Gold v5 agrega usando `*_clean` + coalesce com 0
- Flags de negativos preservados em Silver para auditoria

### Anti-Leakage
✅ Temporal separation garantida: agregações usam lookback windows **antes** de DT_SAFRA  
✅ FPD_INT e FLAG_INSTALACAO_INT permanecem labels (audit-only)

## Próximas Etapas

Após v5 estar OK:

1. **Validar KS** no OOT
   - Esperado: 42.0-42.5 (vs. v4 de ~40.2%)
   - Delta esperado: +1.8-2.3pp

2. **Build Silver Pagamento e Atraso** (próximas bases)

3. **Build Gold v6** (Pagamento + Atraso features)
   - Target KS final: 45.0%

## Documentação Completa

Veja `docs/04_gold_rules/abt_v5.md` para:
- Schema completo (195 colunas)
- Definições de cada feature de Recarga
- Detalhes de temporal windows (M1/M3/M6)
- Detalhes de cada novo gate (9 e 10)
- Data quality expectations
- Roadmap completo v1→v6
- Referências
