# Gold Rules — ABT v2 (Score_01 + Score_02 Incremental)

## 1) Objetivo

Definir a especificação da segunda versão da **Analytical Base Table (ABT v2)**, que serve como:
- **Incremental vs v1** = adiciona Score_02 como feature complementar
- **Baseline para v3** = pronto para enriquecimento com features Telco
- **Referência de KS incremental** = medir ΔKS = KS_v2 - KS_v1

---

## 2) Roadmap Incremental (Atualizado)

Conforme `target_definition.md`, o projeto segue avaliação incremental de KS:

| Versão | Features | Propósito | KS esperado | Status |
|--------|----------|-----------|------------|--------|
| **v1** | Score_01 | Baseline histórico | ≈ 33,1 (OOT) | ✅ Completo |
| **v2** | + Score_02 | ΔKS = KS_v2 - v1 | ?? | ⏳ Novo |
| v3 | + Telco (var_26-93) | ΔKS = KS_v3 - v2 | ?? |  |
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

### 3.4) Features v2 (NOVAS)

#### `SCORE_02_ADJ` (double)
- **Descrição:** Score de crédito secundário do bureau_full
- **Tratamento:** Sentinela 0 convertida para NULL (com flag)
- **Range esperado:** 1–926 (após remoção de 0)
- **Tipo:** Contínua
- **Cobertura esperada:** 40–50% (complementar a Score_01)
- **Propósito:** Incrementar poder preditivo quando disponível

#### `FLAG_SCORE02_MISSING` (0/1)
- **Descrição:** Indica missing/sentinela em SCORE_02 (NULL ou 0)
- **Uso:** Feature binária capturando "presença vs ausência"
- **Valor:** 1 = missing, 0 = válido

### 3.5) Metadados (Iguais a v1)

```python
# Metadados de origem (da Silver)
metadata_data_ingestao          # Quando foi ingerido da Landing
metadata_nome_arquivo_origem    # Nome do arquivo original
metadata_sistema_origem         # Sistema de origem (HACKATHON_LANDING, etc)
metadata_data_transformacao     # Quando foi feita a Silver
metadata_versao_regra           # Versão da regra Silver (silver_bureau_full_v1)

# Metadados de Gold
gold_version                    # "gold_abt_v2"
gold_build_date                 # Timestamp de build
gold_feature_blocks             # "score_01,score_02" (qual bloco está em v2)
```

---

## 4) Schema do ABT v2

| Coluna | Tipo | Categoria | Descrição |
|--------|------|-----------|-----------|
| num_cpf | string | Chave | CPF do cliente |
| safra | string | Chave | YYYYMM (período referência) |
| dt_safra | date | Chave | Primeiro dia do mês da safra |
| flag_instalacao_int | int | Label | Decisão observada (0/1) |
| fpd_int | int | Label | Target risco (0/1) |
| score_01_adj | double | Feature | Score 1 ajustado (sentinela→NULL) |
| flag_score01_missing | int | Feature | Flag de missing em score_01 |
| **score_02_adj** | **double** | **Feature** | **Score 2 ajustado (sentinela→NULL) [NOVO]** |
| **flag_score02_missing** | **int** | **Feature** | **Flag de missing em score_02 [NOVO]** |
| prod | string | Metadado | Tipo de produto |
| flag_mig2 | string | Metadado | Flag de migração |
| metadata_data_ingestao | timestamp | Auditoria | Data ingestão |
| metadata_nome_arquivo_origem | string | Auditoria | Arquivo origem |
| metadata_sistema_origem | string | Auditoria | Sistema origem |
| metadata_data_transformacao | timestamp | Auditoria | Data transformação Silver |
| metadata_versao_regra | string | Auditoria | Versão regra Silver |
| gold_version | string | Gold | Versão gold (gold_abt_v2) |
| gold_build_date | timestamp | Gold | Data build gold |
| gold_feature_blocks | string | Gold | Blocos presentes (score_01,score_02) |

---

## 5) Diferenças entre v1 e v2

| Aspecto | ABT v1 | ABT v2 |
|--------|--------|--------|
| **Features numéricas** | 1 score (Score_01) | 2 scores (Score_01 + Score_02) |
| **Flags de missing** | 1 flag (score_01) | 2 flags (score_01 + score_02) |
| **Cobertura Score_01** | ~92% | ~92% (mantida) |
| **Cobertura Score_02** | N/A | ~40-50% |
| **Grão** | 1:1 NUM_CPF + SAFRA | 1:1 NUM_CPF + SAFRA (mantido) |
| **gold_feature_blocks** | "score_01" | "score_01,score_02" |
| **KS esperado** | 33.1 (benchmark) | ?? (a ser medido) |
| **ΔKS vs v1** | Baseline | ΔKS = KS_v2 - 33.1 |

---

## 6) Validações (Gates) implementadas

### Gate 1: Unicidade
- Sem duplicatas: `COUNT(*) == COUNT(DISTINCT NUM_CPF + SAFRA)`

### Gate 2: Anti-leakage de FPD
- FPD SÓ observado em FLAG_INSTALACAO=1
- Garantir: `COUNT(FPD_INT NOT NULL WHERE FLAG_INSTALACAO=0) == 0`

### Gate 3: Integridade de chaves
- Sem NULLs em `NUM_CPF`, `SAFRA`, `DT_SAFRA`

### Gate 4: Distribuição de FLAG_INSTALACAO
- Ambos valores (0,1) presentes
- Mínimo de ambos para swap-in/out analysis

### Gate 5: Distribuição de FPD (quando observado)
- Ambos valores (0,1) presentes em FLAG_INSTALACAO=1
- Para treinar modelo de risco

### Gate 6: Cobertura de Score_01
- Mínimo 90% de SCORE_01_ADJ válido
- Garante baseline robusto

### Gate 7: Cobertura de Score_02 (NOVO EM V2)
- Mínimo 50% de SCORE_02_ADJ válido
- Mais leniente que v1 (complementar)

---

## 7) Tratamento de Sentinelas (igual a v1)

### Score_01
- Valor 0 é sentinela (não informado)
- Convertido para NULL em SCORE_01_ADJ
- Flag preserva: FLAG_SCORE01_MISSING = 1

### Score_02 (NOVO)
- Valor 0 é sentinela (não informado)
- Convertido para NULL em SCORE_02_ADJ
- Flag preserva: FLAG_SCORE02_MISSING = 1

---

## 8) Exemplo de uso em modelagem

```python
# Treinar modelo com v2 vs v1

# v1: usar apenas Score_01
X_v1 = abt_v1[["score_01_adj", "flag_score01_missing"]]
y = abt_v1[abt_v1["flag_instalacao_int"] == 1]["fpd_int"]

# v2: usar ambos scores
X_v2 = abt_v2[["score_01_adj", "flag_score01_missing", "score_02_adj", "flag_score02_missing"]]
y = abt_v2[abt_v2["flag_instalacao_int"] == 1]["fpd_int"]

# Medir ΔKS
ks_v1 = compute_ks(X_v1, y)
ks_v2 = compute_ks(X_v2, y)
delta_ks = ks_v2 - ks_v1
```

---

## 9) Próximos passos

1. ✅ Implementar `01_gold_abt_v2_builder.py`
2. ✅ Implementar `validate_abt_v2()` com Gate 7
3. ⏳ Treinar modelo e medir KS em v2 OOT
4. ⏳ Criar `02_gold_abt_v3_builder.py` (+ Telco)
5. ⏳ Documentar `abt_v3.md`

---

## 10) Checklist pré-produção

- [ ] ABT v2 gerada sem erros de validação
- [ ] KS em OOT (fev/mar) mensurado
- [ ] ΔKS vs v1 documentado
- [ ] Matriz de confusão gerada
- [ ] Análise de swap-in/out executada
- [ ] Próxima versão (v3 com Telco) planejada

---

**Status:** ✅ Implementação v2 completa  
**Data:** 2026-01-21  
**Versão:** gold_abt_v2
