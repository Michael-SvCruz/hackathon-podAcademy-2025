# Gold Rules — ABT v1 (Score_01 Baseline)

## 1) Objetivo

Definir a especificação da primeira versão da **Analytical Base Table (ABT v1)**, que serve como:
- **Baseline** para avaliar incremento de features
- **Source** para treinamento do modelo de risco de Score_01
- **Referência** para comparação com versões posteriores (v2, v3, ...)

---

## 2) Roadmap Incremental

Conforme `target_definition.md`, o projeto segue avaliação incremental de KS:

| Versão | Features | Propósito | KS esperado |
|--------|----------|-----------|------------|
| **v1** | Score_01 | Baseline histórico | ≈ 33,1 (OOT) |
| v2 | + Score_02 | ΔKS = KS_v2 - v1 | ?? |
| v3 | + Telco (var_26-93) | ΔKS = KS_v3 - v2 | ?? |
| v4 | + Cadastro | ΔKS = KS_v4 - v3 | ?? |
| v5 | + Recarga | ΔKS = KS_v5 - v4 | ?? |
| v6 | + Pagamento + Atraso | ΔKS = KS_v6 - v5 | ?? |

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

### 3.3) Features v1

#### `SCORE_01_ADJ` (double)
- **Descrição:** Score de crédito ajustado do bureau_full
- **Tratamento:** Sentinela 0 convertida para NULL (com flag)
- **Range esperado:** 1–778 (após remoção de 0)
- **Tipo:** Contínua

#### `FLAG_SCORE01_MISSING` (0/1)
- **Descrição:** Indica missing/sentinela em SCORE_01 (NULL ou 0)
- **Uso:** Feature binária capturando "presença vs ausência"
- **Valor:** 1 = missing, 0 = válido

### 3.4) Metadados

```python
# Metadados de origem (da Silver)
metadata_data_ingestao          # Quando foi ingerido da Landing
metadata_nome_arquivo_origem    # Nome do arquivo original
metadata_sistema_origem         # Sistema de origem (HACKATHON_LANDING, etc)
metadata_data_transformacao     # Quando foi feita a Silver
metadata_versao_regra           # Versão da regra Silver (silver_bureau_full_v1)

# Metadados de Gold
gold_version                    # "gold_abt_v1"
gold_build_date                 # Timestamp de build
gold_feature_blocks             # "score_01" (qual bloco está em v1)
```

---

## 4) Schema do ABT v1

| Coluna | Tipo | Categoria | Descrição |
|--------|------|-----------|-----------|
| num_cpf | string | Chave | CPF do cliente |
| safra | string | Chave | YYYYMM (período referência) |
| dt_safra | date | Chave | Primeiro dia do mês da safra |
| flag_instalacao_int | int | Label | Decisão observada (0/1) |
| fpd_int | int | Label | Target risco (0/1) |
| score_01_adj | double | Feature | Score 1 ajustado (sentinela→NULL) |
| flag_score01_missing | int | Feature | Flag de missing em score_01 |
| prod | string | Metadado | Tipo de produto |
| flag_mig2 | string | Metadado | Flag de migração |
| metadata_data_ingestao | timestamp | Auditoria | Data ingestão |
| metadata_nome_arquivo_origem | string | Auditoria | Arquivo origem |
| metadata_sistema_origem | string | Auditoria | Sistema origem |
| metadata_data_transformacao | timestamp | Auditoria | Data transformação Silver |
| metadata_versao_regra | string | Auditoria | Versão regra Silver |
| gold_version | string | Gold | Versão gold (gold_abt_v1) |
| gold_build_date | timestamp | Gold | Data build gold |
| gold_feature_blocks | string | Gold | Blocos presentes (score_01) |

---

## 5) Validações Obrigatórias (Gates)

### Gate 1: Unicidade
```
Condição: COUNT(*) = COUNT(DISTINCT NUM_CPF, SAFRA)
Falha: Levanta erro se detectadas duplicatas
```

### Gate 2: FPD observado SÓ em FLAG_INSTALACAO=1
```
Condição: Onde FLAG_INSTALACAO=0, FPD deve ser NULL
Falha: Levanta erro se houver FPD não nulo com FLAG=0
```

### Gate 3: Sem NULLs nas chaves
```
Condição: num_cpf e safra nunca NULL
Falha: Levanta erro se houver chave nula
```

### Gate 4: FLAG_INSTALACAO com ambos valores
```
Condição: Deve haver registros com FLAG=0 E FLAG=1
Falha: Levanta erro se falta algum valor
Motivo: Necessário para análise de impacto (swaps)
```

### Gate 5: FPD com ambos valores
```
Condição: FPD deve ter positivos (1) E negativos (0)
Falha: Levanta erro se falta classe
Motivo: Necessário para treinar modelo balanceado
```

### Gate 6: Cobertura mínima de SCORE_01
```
Condição: SCORE_01_ADJ ≥ 90% de preenchimento
Falha: Levanta erro se < 90%
Motivo: Feature principal para baseline
```

---

## 6) Checklist Antes de Usar em Produção

- [ ] Todos os 6 gates passaram
- [ ] Distribuição de FLAG_INSTALACAO parece razoável
- [ ] Distribuição de FPD balanceada
- [ ] Score_01 tem cobertura > 90%
- [ ] Documentação atualizada com métricas
- [ ] Modelo treinado em FLAG_INSTALACAO=1 (onde FPD observado)
- [ ] KS baseline calculado no OOT (fev/mar)

---

## 7) Observações Importantes

### Anti-Leakage
- FPD está **apenas** onde FLAG_INSTALACAO=1 (por design dos dados)
- Isso significa: **treino usa SÓ FLAG_INSTALACAO=1**
- Teste/OOT também usa FLAG_INSTALACAO=1 (para consistência)
- Análise de swaps usa universo completo (ambos FLAG=0/1)

### Score_01 como Baseline
- Score_01 é a score **histórica** do banco (já existente)
- v1 estabelece KS baseline ≈ 33,1
- v2+ adiciona Score_02 (outra score do banco) e features novas

### Por que começar com Score_01?
- Não precisa de ETL adicional (já está no bureau)
- Permite controle incremental claro
- Referência histórica para comparar melhorias
- Simples + direto = fácil de debugar

---

## 8) Próximas Versões (v2+)

Após v1 estar em produção:

**ABT v2:**
- Adiciona `SCORE_02_ADJ` + `FLAG_SCORE02_MISSING`
- Compara: ΔKS = KS_v2 - KS_v1
- Documenta ganho incremental

**ABT v3:**
- Adiciona telco features (var_26_adj, var_27_adj, ..., var_93_adj)
- Adiciona telco missing flags (flag_var_26_missing, ...)
- Compara: ΔKS = KS_v3 - KS_v2
- Continua incremental...

---

## 9) Referências

- `target_definition.md` - Definição temporal e labels
- `docs/03_silver_rules/bureau_full.md` - Regras Silver Bureau
- `src/jobs/02_gold/00_gold_abt_builder.py` - Script build
- `src/jobs/02_gold/validators/validate_abt.py` - Validações
