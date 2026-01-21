# Gold Rules — ABT v3 (Score_01 + Score_02 + Telco Incremental)

## 1) Objetivo

Definir a especificação da terceira versão da **Analytical Base Table (ABT v3)**, que serve como:
- **Incremental vs v2** = adiciona 68 variáveis Telco via LEFT JOIN no Bureau
- **Baseline para v4** = pronto para enriquecimento com features Cadastro
- **Referência de KS incremental** = medir ΔKS = KS_v3 - KS_v2

---

## 2) Roadmap Incremental (Atualizado)

Conforme `target_definition.md`, o projeto segue avaliação incremental de KS:

| Versão | Features | Propósito | KS esperado | Status |
|--------|----------|-----------|------------|--------|
| **v1** | Score_01 | Baseline histórico | ≈ 33,1 (OOT) | ✅ Completo |
| **v2** | + Score_02 | ΔKS = KS_v2 - v1 | ?? | ✅ Completo |
| **v3** | + Telco (var_26-93) | ΔKS = KS_v3 - v2 | ?? | ⏳ Novo |
| v4 | + Cadastro | ΔKS = KS_v4 - v3 | ?? |  |
| v5 | + Recarga | ΔKS = KS_v5 - v4 | ?? |  |
| v6 | + Pagamento + Atraso | ΔKS = KS_v6 - v5 | ?? |  |

---

## 3) Definições Críticas (conforme target_definition.md)

### 3.1) Evento Âncora
- Unidade: **cliente-mês** (`NUM_CPF + SAFRA`)
- Data referência: primeiro dia do mês (`DT_SAFRA`)
- Grão esperado: **1:1 por NUM_CPF + SAFRA**

### 3.2) Labels (NÃO SÃO FEATURES)

#### `FPD_INT` (0/1) — Target de Risco
- **Definição:** Primeiro Pagamento Devido (indicador de risco/inadimplência)
- **Observabilidade:** Observado SÓ quando `FLAG_INSTALACAO_INT=1`
- **Uso em treino:** Usar SÓ registros onde `FLAG_INSTALACAO_INT=1`
- **Regra crítica:** NUNCA pode ser feature (risco de leakage)

#### `FLAG_INSTALACAO_INT` (0/1) — Decisão Histórica
- **Definição:** Aprovado/contratado (1) vs Reprovado (0)
- **Uso:** Análise de impacto e swap-in/swap-out (não para treino de risco)
- **Regra crítica:** NUNCA pode ser feature (é a decisão que estamos tentando replicar/melhorar)

### 3.3) Features v1 (Mantidas)

#### `SCORE_01_ADJ` (double)
- **Descrição:** Score de crédito ajustado do bureau_full
- **Tratamento:** Sentinela 0 convertida para NULL (com flag)
- **Range esperado:** 1–778 (após remoção de 0)
- **Tipo:** Contínua

#### `FLAG_SCORE01_MISSING` (0/1)
- **Descrição:** Indica missing/sentinela em SCORE_01 (NULL ou 0)
- **Uso:** Feature binária capturando "presença vs ausência"
- **Valor:** 1 = missing, 0 = válido

### 3.4) Features v2 (Mantidas)

#### `SCORE_02_ADJ` (double)
- **Descrição:** Score de crédito secundário do bureau_full
- **Tratamento:** Sentinela 0 convertida para NULL (com flag)
- **Range esperado:** 1–926 (após remoção de 0)
- **Tipo:** Contínua
- **Cobertura esperada:** 40–50% (complementar a Score_01)

#### `FLAG_SCORE02_MISSING` (0/1)
- **Descrição:** Indica missing/sentinela em SCORE_02 (NULL ou 0)
- **Uso:** Feature binária capturando "presença vs ausência"
- **Valor:** 1 = missing, 0 = válido

### 3.5) Features v3 (NOVAS) — Telco

#### `VAR_26_ADJ` a `VAR_93_ADJ` (double, 68 variáveis)
- **Descrição:** Variáveis anonimizadas do bloco Telco
- **Tratamento:** Sentinela 304 convertida para NULL (com flag correspondente)
- **Range esperado:** Varia por variável (após remoção de 304)
- **Tipo:** Contínua/Mista
- **Cobertura esperada:** > 50% agregado (complementar a scores)
- **Propósito:** Enriquecimento com sinais Telco para incremento de ΔKS

#### `FLAG_VAR_26_MISSING` a `FLAG_VAR_93_MISSING` (0/1, 68 flags)
- **Descrição:** Indica missing/sentinela em VAR_* (NULL ou 304)
- **Uso:** Flags binárias capturando "presença vs ausência" para cada var
- **Valor:** 1 = missing (304 ou NULL original), 0 = válido
- **Benefício:** Permitem usar padrões de missingness como features

### 3.6) Metadados (Iguais a v1-v2)

```python
# Metadados de origem (da Silver)
metadata_data_ingestao          # Quando foi ingerido da Landing
metadata_nome_arquivo_origem    # Nome do arquivo original
metadata_sistema_origem         # Sistema que gerou os dados
metadata_data_transformacao     # Quando foi transformado para Silver
metadata_versao_regra           # Versão das regras de transformação

# Metadados de Gold
gold_version                    # "gold_abt_v3"
gold_build_date                 # Data/hora da construção
gold_feature_blocks             # "score_01,score_02,telco"
```

---

## 4) Schema Completo (v3)

Ordem das colunas na saída:

| # | Coluna | Tipo | Descrição | Observações |
|----|--------|------|-----------|-------------|
| 1 | `num_cpf` | string | Chave 1: CPF do cliente | Obrigatório, não nulo |
| 2 | `safra` | string | Chave 2: Coorte (YYYYMM) | Obrigatório, não nulo |
| 3 | `dt_safra` | date | Data referência (1º dia mês) | Derivada de SAFRA |
| 4 | `flag_instalacao_int` | int | Decisão de aprovação (0/1) | Label (não feature) |
| 5 | `fpd_int` | int | Target de risco (0/1) | Label, observado SÓ em flag=1 |
| 6 | `score_01_adj` | double | Score Bureau principal | v1, sentinela 0→NULL |
| 7 | `flag_score01_missing` | int | Flag missing Score_01 | v1, 1=missing, 0=válido |
| 8 | `score_02_adj` | double | Score Bureau complementar | v2, sentinela 0→NULL |
| 9 | `flag_score02_missing` | int | Flag missing Score_02 | v2, 1=missing, 0=válido |
| 10–77 | `var_26_adj`...`var_93_adj` | double | Features Telco anonimizadas | v3, sentinela 304→NULL |
| 78–145 | `flag_var_26_missing`...`flag_var_93_missing` | int | Flags Telco missing | v3, 1=missing, 0=válido |
| 146 | `prod` | string | Tipo/família de produto | Metadata Bureau |
| 147 | `flag_mig2` | string | Segmentação/jornada | Metadata Bureau |
| 148 | `metadata_data_ingestao` | timestamp | Timestamp ingestão | Auditoria |
| 149 | `metadata_nome_arquivo_origem` | string | Nome arquivo landing | Auditoria |
| 150 | `metadata_sistema_origem` | string | Sistema origem | Auditoria |
| 151 | `metadata_data_transformacao` | timestamp | Timestamp transformação | Auditoria |
| 152 | `metadata_versao_regra` | string | Versão regra transformação | Auditoria |
| 153 | `gold_version` | string | "gold_abt_v3" | Identificação |
| 154 | `gold_build_date` | timestamp | Data construção Gold | Rastreabilidade |
| 155 | `gold_feature_blocks` | string | "score_01,score_02,telco" | Feature blocks presentes |

**Total de colunas: 155**
**Total de features: 138** (2 scores + 68 var Telco + 68 flags missing)

---

## 5) Diferenças vs v2

| Aspecto | v2 | v3 | Delta |
|---------|----|----|-------|
| **Total colunas** | 23 | 155 | +132 (68 var Telco + 68 flags) |
| **Total features** | 4 | 138 | +134 |
| **Fonte 1: Bureau** | Spine (chaves + scores) | Spine (chaves + scores) | Mantida |
| **Fonte 2: Telco** | Não | LEFT JOIN (68 var) | NOVO |
| **Grão esperado** | 1:1 NUM_CPF+SAFRA | 1:1 NUM_CPF+SAFRA | Mantida (join type LEFT) |
| **Join strategy** | N/A (puro Bureau) | Bureau LEFT JOIN Telco | NOVO |
| **Cobertura Telco** | N/A | >50% | NOVO |
| **Feature blocks** | "score_01,score_02" | "score_01,score_02,telco" | Atualizado |
| **Gold KS esperado** | ?? | ?? | Medir ΔKS |

---

## 6) Tratamento de Sentinelas (v3)

### Sentinela 304 em Telco (var_26–var_93)
Valor **304** representa "não informado/não aplicável" em variáveis Telco.

**Tratamento:**
1. Converte 304 → NULL (não usar como valor válido)
2. Cria flag `flag_var_*_missing = 1` para registrar presença de 304
3. Exemplo:
   ```
   var_26_dbl = 304       →  var_26_adj = NULL, flag_var_26_missing = 1
   var_26_dbl = NULL      →  var_26_adj = NULL, flag_var_26_missing = 1
   var_26_dbl = 45.5      →  var_26_adj = 45.5, flag_var_26_missing = 0
   ```

---

## 7) Validação (8 Gates)

### Gate 1: Unicidade (1:1 NUM_CPF + SAFRA)
```
Assert: COUNT(*) == COUNT(DISTINCT NUM_CPF + SAFRA)
```
Garante nenhuma duplicação no join.

### Gate 2: Anti-Leakage FPD
```
Assert: COUNT(FPD NOT NULL WHERE FLAG_INSTALACAO=0) == 0
```
FPD observado somente quando aprovado.

### Gate 3: Integridade de Chaves
```
Assert: COUNT(NULL in [NUM_CPF, SAFRA]) == 0
```
Nenhum NULL nas chaves.

### Gate 4: Distribuição FLAG_INSTALACAO
```
Assert: {0, 1} ⊆ FLAG_INSTALACAO (ambos valores presentes)
```
Dataset inclui tanto aprovados quanto reprovados.

### Gate 5: Distribuição FPD
```
Assert: {0, 1} ⊆ FPD (quando observado, ambos valores presentes)
```
Dataset inclui tanto inadimplentes quanto adimplentes.

### Gate 6: Score_01 Cobertura
```
Assert: COUNT(SCORE_01_ADJ NOT NULL) / COUNT(*) >= 90%
```
Mantém critério v1.

### Gate 7: Score_02 Cobertura
```
Assert: COUNT(SCORE_02_ADJ NOT NULL) / COUNT(*) >= 50%
```
Mantém critério v2.

### Gate 8: Telco Cobertura (NOVO)
```
Assert: (COUNT(Telco cells NOT NULL) / COUNT(Telco cells total)) >= 50%
```
Mínimo 50% de células válidas no bloco Telco.
Cálculo: (total_cells - null_cells) / total_cells, onde total_cells = 68 vars × COUNT(*).

---

## 8) Exemplo de Uso (Modelagem)

### Python/PySpark
```python
spark = SparkSession.builder.appName("v3_modeling").getOrCreate()
abt_v3 = spark.read.format("delta").load("/Volumes/hackathon_2025/default/gold/abt_v3_delta/")

# Filtro anti-leakage (treinar apenas em FLAG_INSTALACAO=1)
train = abt_v3.filter(F.col("flag_instalacao_int") == 1)

# Features para o modelo
feature_cols = [
    "score_01_adj", "flag_score01_missing",
    "score_02_adj", "flag_score02_missing",
    "var_26_adj", "flag_var_26_missing",
    # ... até var_93_adj, flag_var_93_missing
]

# Target
target_col = "fpd_int"

# Train/val com stratified split
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_assembled = assembler.transform(train).select("features", target_col)

# Treinar modelo (LogisticRegression, RandomForest, XGBoost, etc.)
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol=target_col)
model = lr.fit(train_assembled)

# Avaliar KS incremental
# KS_v3 vs KS_v2 para quantificar ΔKS do bloco Telco
```

---

## 9) Checklist de Implementação (v3)

- [x] Script `02_gold_abt_v3_builder.py` criado e testado
- [x] Validator `validate_abt_v3()` com 8 gates implementado
- [x] Documentação `abt_v3.md` completa
- [ ] Teste em Databricks com dados reais
- [ ] Medição de KS_v3 e cálculo de ΔKS = KS_v3 - KS_v2
- [ ] Validação de padrões em var_26–var_93 (EDA Telco)
- [ ] Atualização de README com status ✅

---

## 10) Próximas Etapas (v4+)

### v4: Cadastro
- Enriquecimento com Silver Cadastro (edad, CEP, var_02–var_25)
- LEFT JOIN em NUM_CPF + SAFRA
- Gate 9: Cobertura Cadastro > 50%

### v5: Recarga
- Agregação de eventos Recarga (agregação temporal)
- Precisa definir janela temporal (ex: últimos 12 meses)
- LEFT JOIN em NUM_CPF + SAFRA

### v6: Pagamento + Atraso
- Agregação de eventos Pagamento
- Target Atraso como feature (dias em atraso)
- Conclusão do roadmap incremental

---

## Referências

- [target_definition.md](../target_definition.md) — Definições de evento âncora, labels, anti-leakage
- [abt_v1.md](abt_v1.md) — Especificação v1 (baseline)
- [abt_v2.md](abt_v2.md) — Especificação v2 (Score_02)
- [01_bronze_silver_telco.py](../../src/jobs/01_silver/01_bronze_silver_telco.py) — Fonte Telco Silver
- [00_gold_abt_builder.py](../../src/jobs/02_gold/00_gold_abt_builder.py) — Builder v1
- [01_gold_abt_v2_builder.py](../../src/jobs/02_gold/01_gold_abt_v2_builder.py) — Builder v2
- [02_gold_abt_v3_builder.py](../../src/jobs/02_gold/02_gold_abt_v3_builder.py) — Builder v3 (este script)
- [validate_abt.py](../../src/jobs/02_gold/validators/validate_abt.py) — Validators (v1, v2, v3)
