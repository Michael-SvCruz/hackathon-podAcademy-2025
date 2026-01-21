# ANÁLISE ESTRATÉGICA - PRÓXIMA ETAPA: GOLD v1

## 📊 ACHADOS CRÍTICOS DA ANÁLISE

### 1. Arquitetura Incremental com Bureau + Score_01 como Base

**INSIGHT PRINCIPAL:**
O projeto começa com **Score_01 como feature**, não com Telco!

```
Incremental Obrigatório (conforme overview.md):
1. ✅ Scores (Score_01 → Score_02)     ← COMEÇA AQUI
2. Telco
3. Cadastro
4. Recarga
5. Pagamento
6. Atraso
```

**Implicação para Gold v1:**
```
gold_abt_v1 = bureau_full (spine) + SCORE_01 como feature
           ↓ (após teste KS)
gold_abt_v2 = gold_abt_v1 + SCORE_02
           ↓ (após teste KS)
gold_abt_v3 = gold_abt_v2 + Telco features
... e assim por diante
```

---

### 2. Definição Clara de Labels (Anti-Leakage)

**DOIS LABELS DISTINTOS (conforme target_definition.md):**

| Label | Uso | Regra Crítica | Fonte |
|-------|-----|---------------|-------|
| **FPD** (0/1) | Target de risco (modelo principal) | Treinar SÓ em FLAG_INSTALACAO=1 | bureau_full |
| **FLAG_INSTALACAO** (0/1) | Análise de impacto (swap-in/out) | NÃO usar como feature | bureau_full |

**REGRA FUNDAMENTAL:**
```python
# Gold ABT deve incluir AMBOS como labels/auditoria, MAS
# Modelo treina APENAS em FLAG_INSTALACAO=1 (onde FPD está observado)

features = [score_01, score_02, var_telco_26, ...]  # Sem FPD, sem FLAG_INSTALACAO
labels = {fpd_int, flag_instalacao_int}             # Ambos para auditoria/swaps
```

---

### 3. Spin Oficial é Bureau_Full

**Características do spine:**
- ✅ 1:1 por NUM_CPF + SAFRA (confirmado)
- ✅ Inclui reprovados (FLAG_INSTALACAO=0)
- ✅ FPD observado SÓ quando FLAG_INSTALACAO=1
- ✅ SCORE_01 e SCORE_02 disponíveis

**Implicação:**
- Gold SEMPRE faz LEFT JOIN com silver_bureau_full como base
- Outras bases (telco, cadastro, etc) vêm como LEFT JOIN ADICIONAL

---

### 4. Benchmark de Referência

**KS = 33,1** no OOT (fev/mar) — isso é o baseline que o modelo incremental deve bater/melhorar

---

## 🏗️ PROPOSTA DE IMPLEMENTAÇÃO: GOLD v1

### Estrutura de Diretórios

```
src/jobs/02_gold/
├── 00_gold_abt_builder.py           ← Script principal (orquestra joins)
├── conftest_gold.py                 ← Config paths, versões
├── validators/
│   ├── __init__.py
│   └── validate_abt.py              ← Checks pós-build
└── docs/
    └── 04_gold_rules/
        └── abt_v1.md                ← Specs do gold v1
```

### Lógica Principal (00_gold_abt_builder.py)

```python
def build_gold_abt_v1(spark, gold_version="v1"):
    """
    Constrói ABT v1:
    - Spine: silver_bureau_full
    - Features: SCORE_01 apenas
    - Labels: FPD_INT, FLAG_INSTALACAO_INT (auditoria)
    """
    
    # 1) Ler spine
    df_bureau = spark.table("silver_bureau")
    
    # 2) Seleção de colunas (spine + scores)
    df_gold = df_bureau.select(
        # Chaves
        "num_cpf",
        "safra",
        "dt_safra",
        
        # LABELS (NÃO FEATURES)
        "flag_instalacao_int",
        "fpd_int",
        
        # FEATURES v1
        "score_01_adj",      # Score_01 com sentinela 0 tratada
        "flag_score01_missing",
        
        # Metadados
        "prod",
        "flag_mig2",
        
        # Auditoria
        "metadata_data_transformacao",
        "metadata_versao_regra"
    )
    
    # 3) Adicionar metadados de gold
    df_gold = df_gold \
        .withColumn("gold_version", F.lit("gold_abt_v1")) \
        .withColumn("gold_build_date", F.current_timestamp()) \
        .withColumn("gold_has_telco_features", F.lit(0))  # v1: sem telco
    
    # 4) Validações
    validate_abt(df_gold)
    
    return df_gold

def validate_abt(df):
    """
    Validações obrigatórias (conforme target_definition.md):
    1. Unicidade NUM_CPF + SAFRA
    2. FPD observado SÓ em FLAG_INSTALACAO=1
    3. Sem NULL em chaves
    4. Contagem de records
    """
    print(">>> [Validate] Checando gates de qualidade...")
    
    # Gate 1: Unicidade
    total = df.count()
    unique_key = df.select("num_cpf", "safra").distinct().count()
    assert total == unique_key, f"ERRO: {total} != {unique_key}"
    
    # Gate 2: FPD observado SÓ em FLAG_INSTALACAO=1
    fpd_where_flag0 = df.filter(
        (F.col("flag_instalacao_int") == 0) & 
        (F.col("fpd_int").isNotNull())
    ).count()
    assert fpd_where_flag0 == 0, f"ERRO: {fpd_where_flag0} FPD observados em FLAG_INSTALACAO=0"
    
    # Gate 3: Distribuição de labels
    dist_flag = df.groupBy("flag_instalacao_int").count().toPandas()
    dist_fpd = df.filter(F.col("fpd_int").isNotNull()).groupBy("fpd_int").count().toPandas()
    
    print(f"  ✓ Total: {total}")
    print(f"  ✓ Unique keys: {unique_key}")
    print(f"  ✓ FLAG_INSTALACAO distribution:\n{dist_flag}")
    print(f"  ✓ FPD distribution (FLAG_INSTALACAO=1):\n{dist_fpd}")
```

### Dataset Final (Gold v1)

```
Colunas:
├── CHAVES
│   ├── num_cpf
│   ├── safra
│   └── dt_safra
│
├── LABELS (para auditoria/impacto)
│   ├── flag_instalacao_int         ← Decisão atual (0/1)
│   └── fpd_int                     ← Target risco (0/1)
│
├── FEATURES v1
│   ├── score_01_adj               ← Feature principal
│   └── flag_score01_missing       ← Flag de sentinela
│
├── METADADOS
│   ├── prod
│   ├── flag_mig2
│   ├── metadata_data_transformacao
│   ├── metadata_versao_regra
│   ├── gold_version               ← "gold_abt_v1"
│   ├── gold_build_date
│   └── gold_has_telco_features    ← False para v1
│
└── QUALIDADE
    └── [registros SÓ com FLAG_INSTALACAO=1 OU com FPD_INT não nulo]
```

---

## 📋 CHECKLIST ANTES DE RODAR GOLD v1

- [ ] Silver Bureau está pronto e testado
- [ ] SCORE_01 foi tratado (sentinela 0 → NULL com flag)
- [ ] Confirmar que FPD está observado SÓ em FLAG_INSTALACAO=1
- [ ] Documentar definição de "treino em FLAG_INSTALACAO=1"
- [ ] Validações automáticas no script
- [ ] Teste KS com SCORE_01 apenas como feature

---

## 🎯 PRÓXIMOS PASSOS APÓS GOLD v1

1. **Teste modelo com SCORE_01:**
   - Baseline para comparação incremental
   - KS esperado: próximo de 33,1

2. **Gold v2:**
   - Adiciona SCORE_02
   - Compara: ΔKS = KS_v2 - KS_v1

3. **Gold v3+:**
   - Adiciona Telco, Cadastro, etc
   - Incremento de KS por bloco

---

## 📝 NOTA IMPORTANTE

**Por quê começar com Score_01?**
- Score_01 JÁ EXISTE no bureau_full (não precisa de uma camada Silver adicional)
- É a baseline histórica do banco
- Permite controle incremental claro: Score_01 → Score_02 → Features externas

**Telco virá depois de validar Score_01 e Score_02!**
