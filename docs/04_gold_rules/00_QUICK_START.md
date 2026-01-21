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
