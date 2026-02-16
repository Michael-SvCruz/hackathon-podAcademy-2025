# Preparação da Tabela Analítica de Modelagem (ABT)

## 1. Visão Geral

A ABT foi construída utilizando **Medallion Architecture** com versionamento incremental:

```
Landing → Bronze → Silver → Gold (Features) → ABT
```

| Princípio | Descrição |
|-----------|-----------|
| Versionamento Incremental | Cada versão adiciona um bloco de features |
| Anti-Leakage | Features comportamentais usam apenas dados anteriores à SAFRA |
| Validação Automática | Gates de qualidade executados a cada build |

---

## 2. Fontes de Dados

| Fonte | Registros | Descrição | Granularidade |
|-------|-----------|-----------|---------------|
| **Bureau** | 3.79M | Scores de crédito e variáveis target | CPF + SAFRA |
| **Telco** | 3.79M | Comportamento telefônico (var_26 a var_93) | CPF + SAFRA |
| **Cadastro** | 3.79M | Dados demográficos | CPF + SAFRA |
| **Recarga** | 95.2M | Histórico de recargas e SOS | CPF + MÊS |
| **Pagamento** | 21.8M | Histórico de pagamentos | CPF + MÊS |
| **Atraso** | 31.6M | Faturas em aberto (aging) | CPF + MÊS |

---

## 3. Estratégia de Integração

**Spine:** Bureau (população completa com target)

**Chave:** `NUM_CPF + SAFRA`

**JOIN:** LEFT JOIN para preservar todos os registros da spine

```python
df_abt = df_spine.join(df_features, on=["num_cpf", "safra"], how="left")
```

---

## 4. Transformações

### 4.1 Bronze → Silver

| Transformação | Descrição |
|---------------|-----------|
| Type Casting | Strings → tipos apropriados |
| Padronização | Colunas em snake_case |
| Deduplicação | Remoção de duplicatas |
| Sentinelas | Valores especiais (0, 304) → NULL + flag |

### 4.2 Silver → Gold (Feature Engineering)

**Recarga:** Indicadores de estresse financeiro (SOS), padrões temporais, ticket médio

**Pagamento:** Volume de pagamentos, indicadores de negociação (descontos), juros pagos

**Atraso:** Exposição atual, distribuição de aging, flags de risco (WO, PDD, Fraude)

### 4.3 Janelas Temporais

```python
# Anti-leakage: SAFRA_FEATURE < SAFRA_ABT
TEMPORAL_WINDOWS = {"m1": 1, "m3": 3, "m6": 6}
```

**Agregações:**

| Prefixo | Agregação |
|---------|-----------|
| `qtd_*`, `sum_*` | SUM |
| `flag_*` | MAX |
| `pct_*`, `ratio_*` | AVG |

---

## 5. Evolução da ABT

| Versão | Fontes Adicionadas | Colunas |
|--------|-------------------|---------|
| v1 | Bureau (Score_01) | 15 |
| v2 | + Score_02 | 20 |
| v3 | + Telco | 95 |
| v4 | + Cadastro | 185 |
| v5 | + Recarga | 311 |
| **v6** | + Pagamento + Atraso | **614** |

---

## 6. Tratamento de Valores Ausentes

```python
# Contagens/somas: preencher com 0
df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# Criar flag de ausência
df = df.withColumn(f"flag_sem_recarga_{janela}",
    F.when(F.col(f"qtd_meses_dados_rec_{janela}") == 0, 1).otherwise(0))
```

---

## 7. Validações (Gates)

| Gate | Validação |
|------|-----------|
| 1 | Unicidade: 1 registro por CPF + SAFRA |
| 2 | Sem NULLs em chaves |
| 3 | Anti-leakage: FPD só quando FLAG_INSTALACAO=1 |
| 4 | Cobertura Score > 90% |
| 5 | Target ∈ {0, 1} |

---

## 8. ABT Final

| Característica | Valor |
|----------------|-------|
| Tabela | `hackathon_2025.default.gold_abt_v6_v2` |
| Registros | 3.795.310 |
| Colunas | 614 |
| Granularidade | NUM_CPF + SAFRA (1:1) |
| Target | `fpd_int` |
