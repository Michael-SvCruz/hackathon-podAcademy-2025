## ✅ ABT v5 — Implementação Concluída

**Data:** 22 de janeiro de 2026  
**Status:** 🟢 Pronto para execução  
**Tempo estimado de build:** 12-15 minutos

---

## 📦 O que foi criado

### Arquivos Implementados

| Arquivo | Linhas | Descrição | Status |
|---------|--------|-----------|--------|
| `src/jobs/02_gold/04_gold_abt_v5_builder.py` | 480+ | Script principal Gold v5 | ✅ Completo |
| `src/jobs/02_gold/validators/validate_abt.py` | +200 | Função validate_abt_v5 (10 gates) | ✅ Adicionado |
| `docs/04_gold_rules/abt_v5.md` | 500+ | Especificação técnica completa | ✅ Criado |
| `docs/04_gold_rules/00_QUICK_START.md` | +150 | Guia de execução (atualizado) | ✅ Atualizado |
| `docs/05_recarga_silver_quality_report.md` | 400+ | Quality insights Recarga | ✅ Anterior |

---

## 🎯 Features Implementadas (18 novas)

Para cada período temporal (M1 = 1 mês, M3 = 3 meses, M6 = 6 meses):

```
6 agregações × 3 períodos = 18 features

├── qtd_recargas_m1/m3/m6              (COUNT eventos)
├── sum_val_real_clean_m1/m3/m6        (SUM crédito real, sem negativos)
├── sum_val_bonus_clean_m1/m3/m6       (SUM bônus, sem negativos)
├── sum_val_credito_inserido_clean_m1/m3/m6  (SUM crédito inserido)
├── avg_val_real_clean_m1/m3/m6        (AVG crédito real)
└── flag_teve_sos_m1/m3/m6             (Indicador SOS, binária 0/1)
```

---

## ⚙️ Arquitetura Técnica

### Pipeline (8 passos)
```
1. Lê Gold ABT v4          (spine: 3.79M)
   ↓
2. Lê Silver Recarga       (eventos: 95.2M)
   ↓
3. Agrupa por M1/M3/M6     (temporal windows)
   ↓
4. LEFT JOIN na chave      (num_cpf + safra)
   ↓
5. Preenche NULLs com 0    (clientes sem recarga)
   ↓
6. Atualiza gold_metadata
   ↓
7. Valida 10 gates         (8 herdados + 2 novos)
   ↓
8. Escreve Delta + UC table
```

### Decisões de Design

**✅ Dimensões UTILIZADAS (0% sentinelas):**
- cod_tipo_credito
- cod_status_plataforma
- cod_tecnologia_dw
- cod_plataforma_atu

**❌ Dimensões EXCLUÍDAS (90%+ sentinelas):**
- dw_forma_pagamento, cod_promocao, dw_tipo_recarga, dw_tipo_insercao, dw_plano_tarifacao

**✅ Tratamento de Negativos:**
- Usando colunas `*_clean` da Silver (já filtradas)
- SUM + COALESCE(0) para evitar NULLs

**✅ Anti-leakage:**
- Temporal windows lookback (passado relativo a DT_SAFRA)
- FPD_INT e FLAG_INSTALACAO_INT permanecem labels

---

## 📊 Métricas Esperadas

### Input/Output
| Métrica | Valor |
|---------|-------|
| Registros input (v4) | 3.79M |
| Registros eventos (Recarga) | 95.2M |
| Registros output (v5) | 3.79M (1:1 mantido) |
| Colunas novas | 18 (Recarga temporal) |
| Colunas totais v5 | ~195 |

### Coverage
| Bloco | Cobertura |
|-------|-----------|
| Score_01 | 98.18% |
| Score_02 | 99.95% |
| Telco | 20.51% |
| Cadastro | 30-40% |
| Recarga | **35-45%** (novo) |

### Agregados Recarga (Médias Esperadas)
```
QTD_RECARGAS_M1:        8-12 eventos/cliente
SUM_VAL_REAL_CLEAN_M1:  150-300 BRL
AVG_VAL_REAL_CLEAN_M1:  20-30 BRL

QTD_RECARGAS_M3:        25-40 eventos/cliente
SUM_VAL_REAL_CLEAN_M3:  500-1000 BRL

QTD_RECARGAS_M6:        60-100 eventos/cliente
SUM_VAL_REAL_CLEAN_M6:  1200-2500 BRL
```

### KS esperado
```
v4 baseline:  40.2%
v5 target:    42.0-42.5%
Delta:        +1.8-2.3pp
```

---

## 🔐 Validações (10 Gates)

### Herdados de v4 (8)
- ✅ Gate 1: Unicidade (1:1 NUM_CPF+SAFRA)
- ✅ Gate 2: FPD anti-leakage
- ✅ Gate 3: Chaves sem NULL
- ✅ Gate 4: FLAG_INSTALACAO distribuição
- ✅ Gate 5: Score_01 cobertura ≥90%
- ✅ Gate 6: Score_02 cobertura ≥40%
- ✅ Gate 7: Telco cobertura ≥20%
- ✅ Gate 8: Cadastro cobertura ≥20%

### Novos em v5 (2)
- **Gate 9:** Recarga cobertura ≥5% (qtd_recargas_m1 > 0)
- **Gate 10:** QTD_RECARGAS_M1 sanidade (sem NaNs, Infs)

**Taxa esperada de pass:** 100% (10/10 gates)

---

## 🚀 Como Rodar

### Databricks (Recomendado)
```python
%run /Workspace/src/jobs/02_gold/04_gold_abt_v5_builder.py
```

### Spark Submit
```bash
spark-submit \
  --py-files src/ \
  src/jobs/02_gold/04_gold_abt_v5_builder.py \
  --gold_v4_path "/Volumes/hackathon_2025/default/gold/abt_v4_delta/" \
  --silver_recarga_path "/Volumes/hackathon_2025/default/silver/recarga_silver_delta/" \
  --output_path "/Volumes/hackathon_2025/default/gold/abt_v5_delta/"
```

### Local Python
```bash
python src/jobs/02_gold/04_gold_abt_v5_builder.py
```

---

## ✔️ Checklist de Validação

Após execução bem-sucedida, verificar:

- [ ] Script completou sem erros
- [ ] Mensagem final: "✓ Gold ABT v5 concluído com sucesso!"
- [ ] Tabela criada: `hackathon_2025.default.gold_abt_v5`
- [ ] Registros: ~3.79M
- [ ] Todos 10 gates mostraram "✓ PASS"
- [ ] Coverage Recarga: 35-45%
- [ ] QTD_RECARGAS_M1 mean: 8-12 (razoável)

---

## 📚 Documentação

| Doc | Conteúdo |
|-----|----------|
| `abt_v5.md` | Spec técnica (14 seções) |
| `00_QUICK_START.md` | Como rodar |
| `05_recarga_silver_quality_report.md` | Quality insights |
| `ABT_V5_IMPLEMENTATION_SUMMARY.md` | Este doc (detalhado) |

---

## 🎓 O que aprendemos da Recarga

### Positivos ✅
- **SOS quality perfeita:** 6.82% vs 6.5% esperado
- **Parsing 100%:** Nenhum timestamp inválido
- **Deduplicação robusta:** 4.99% removidas (esperado)
- **4 dimensões boas:** Sem sentinelas (0% cada)

### Atenção ⚠️
- **14% negativos:** 2.3x acima de EDA (usar `*_clean`)
- **5 dimensões ruins:** 90%+ sentinelas (excluídas)
- **FLAG_INSTALACAO missing:** 100% NULL (investigar Bronze)

---

## 🔄 Próximas Etapas

1. **Executar v5**
2. **Medir KS** (esperado 42.0-42.5%)
3. **Se KS OK:** Prosseguir para v6
   - Build Silver Pagamento
   - Build Silver Atraso
   - Build Gold v6
4. **Target final:** KS 45.0%

---

**Status:** 🟢 Implementação Completa — Pronto para Execução  
**Data:** 22 janeiro 2026  
**Próximo:** Executar script e validar KS
