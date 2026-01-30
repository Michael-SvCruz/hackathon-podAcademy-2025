# Gold Recarga Features v2 — Documentação Técnica

## Visão Geral

O script `gold_recarga_features_v2.py` é uma versão unificada e aprimorada do processamento de dados de Recarga, combinando as melhores práticas de:

- `03_bronze_silver_recarga.py` (tipagem, sentinelas, deduplicação)
- `tratamento_recarga_v1.py` (ajuste SOS, enriquecimento, métricas temporais)

Adiciona **60+ features comportamentais** relevantes para modelagem de risco de crédito.

---

## Arquitetura

```
SILVER (recarga_silver_delta)
     │ Event-level: ~95M registros
     │ Grão: 1 linha por evento de recarga
     │
     ▼
┌────────────────────────────────────────────────────────────────┐
│             GOLD RECARGA FEATURES V2 (este script)             │
│                                                                │
│  1. Preparação e tipagem                                       │
│  2. Classificação de tipo de transação                         │
│  3. Ajuste de valores (SOS, Bônus)                            │
│  4. Métricas temporais (tempo entre recargas)                 │
│  5. Agregação mensal com features comportamentais             │
│  6. Features derivadas avançadas                              │
│                                                                │
└────────────────────────────────────────────────────────────────┘
     │
     │ Grão: 1 linha por NUM_CPF + SAFRA_RECARGA
     ▼
GOLD (recarga_features_v2_delta)
     │
     │ LEFT JOIN por (NUM_CPF, SAFRA)
     │ Filtro: SAFRA_RECARGA < SAFRA (anti-leakage)
     ▼
ABT v5+ (abt_v5_delta)
```

---

## Regras de Negócio

### 1. SOS (Serviço de Empréstimo de Crédito)

Conforme explicação de Fernando (Claro) na reunião de 07/01/2026:

> "SOS é um empréstimo ou antecipação de recarga, geralmente no valor de R$5, que é pago na próxima recarga tradicional do cliente. O valor devido é debitado dessa próxima recarga."

**Tratamento implementado:**
- SOS é adiantamento (R$3-20, geralmente R$5)
- Descontado da próxima recarga tradicional
- **SOS e bônus NÃO contam como "dinheiro real"**
- Exemplo: Recarga R$20 + SOS R$5 pendente = R$20 dinheiro real, não R$25

**Importância para risco:** Frequência de SOS é **indicador de estresse financeiro**.

### 2. Ajuste de Valores

```python
# Etapa 1: Ajuste por SOS
val_real_ajustado_sos = CASE
    WHEN FLAG_SOS=1 AND VALOR_SOS = VAL_CREDITO_INSERIDO THEN -VALOR_SOS
    WHEN FLAG_SOS=1 AND VALOR_SOS != VAL_CREDITO_INSERIDO THEN VAL_CREDITO_INSERIDO - VALOR_SOS
    ELSE VAL_REAL

# Etapa 2: Ajuste por Bônus
val_real_ajustado_final = CASE
    WHEN tipo_transacao IN ('COMBO_PAGO_BONUS', 'BONUS_PURO') THEN val_real_ajustado_sos - VAL_BONUS
    ELSE val_real_ajustado_sos
```

### 3. Classificação de Transações

| Tipo | Condição | Interpretação |
|------|----------|---------------|
| `PAGO_PURO` | credito > 0 AND bonus = 0 | Recarga tradicional |
| `BONUS_PURO` | credito = 0 AND bonus > 0 | Apenas bônus (promoção) |
| `COMBO_PAGO_BONUS` | credito > 0 AND bonus > 0 | Recarga com bônus |
| `ZERO_TOTAL` | credito = 0 AND bonus = 0 AND real = 0 | Transação vazia |
| `VALOR_NEGATIVO` | real < 0 | Estorno/ajuste |

### 4. Sentinelas

| Código | Significado |
|--------|-------------|
| `-1` | Não se aplica |
| `-2` | Não determinado |
| `-3` | Não informado |

**Tratamento:** Flags `FLAG_*_SENTINELA` criadas na Silver.

---

## Features Geradas

### Categorias de Features (60+ variáveis)

#### 1. Volume de Recargas
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `qtd_recargas_mes` | Quantidade total de eventos | Atividade geral |
| `qtd_recargas_validas_mes` | Excluindo zeros | Atividade efetiva |
| `qtd_telefones_distintos_mes` | Linhas distintas | Diversidade de uso |

#### 2. Valores Monetários
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `sum_val_credito_mes` | Soma de crédito inserido | Volume financeiro |
| `sum_val_real_ajustado_mes` | Após ajuste SOS/bônus | Volume real |
| `sum_val_bonus_mes` | Soma de bônus | Dependência de promoções |
| `avg_val_real_mes` | Ticket médio | Capacidade de pagamento |
| `min_val_real_mes` | Menor recarga | Comportamento mínimo |
| `max_val_real_mes` | Maior recarga | Capacidade máxima |
| `std_val_real_mes` | Desvio padrão | Consistência |

#### 3. Comportamento SOS (Estresse Financeiro)
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `qtd_sos_mes` | Quantidade de eventos SOS | Frequência de necessidade |
| `sum_valor_sos_mes` | Soma de valores SOS | Volume de empréstimos |
| `pct_sos_sobre_credito_mes` | % do crédito que foi SOS | **Alto = estresse** |
| `flag_teve_sos_mes` | Binário: usou SOS? | Indicador de necessidade |
| `freq_sos_mes` | qtd_sos / qtd_recargas | **Alto = alto risco** |

#### 4. Métricas Temporais (Padrão de Recarga)
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `dias_medio_entre_recargas_mes` | Regularidade | Previsibilidade |
| `dias_min_entre_recargas_mes` | Menor intervalo | Urgência |
| `dias_max_entre_recargas_mes` | Maior intervalo | Inatividade |
| `std_dias_entre_recargas_mes` | Consistência | Estabilidade |

#### 5. Padrões de Valor (Consistência Financeira)
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `coef_variacao_val_mes` | CV = std/média | Estabilidade financeira |
| `ratio_max_min_val_mes` | max/min | Amplitude comportamental |
| `ratio_bonus_credito_mes` | % bônus | Dependência de promoções |
| `ticket_medio_mes` | Média por recarga | Capacidade típica |

#### 6. Padrões de Frequência
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `recargas_por_semana_mes` | Média semanal | Intensidade de uso |
| `pct_semanas_com_recarga_mes` | % semanas ativas | Consistência |
| `qtd_semanas_com_recarga_mes` | Semanas distintas | Atividade temporal |

#### 7. Padrões de Horário/Dia
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `qtd_recargas_madrugada_mes` | 00h-05h | Comportamento atípico |
| `qtd_recargas_manha_mes` | 06h-11h | Padrão matutino |
| `qtd_recargas_tarde_mes` | 12h-17h | Padrão vespertino |
| `qtd_recargas_noite_mes` | 18h-23h | Padrão noturno |
| `qtd_recargas_fim_semana_mes` | Sábado/Domingo | Padrão de lazer |
| `pct_recargas_fim_semana_mes` | % no fim de semana | Perfil comportamental |
| `pct_recargas_madrugada_mes` | % na madrugada | **Alerta se alto** |

#### 8. Tipos de Transação
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `qtd_pago_puro_mes` | Recargas tradicionais | Comportamento padrão |
| `qtd_bonus_puro_mes` | Apenas bônus | Dependência de promoções |
| `qtd_combo_mes` | Recarga + bônus | Busca de vantagens |
| `qtd_valor_negativo_mes` | Estornos/ajustes | Problemas potenciais |

#### 9. Diversidade
| Feature | Descrição | Relevância para Risco |
|---------|-----------|----------------------|
| `qtd_tipos_credito_distintos_mes` | Tipos de crédito | Diversidade de comportamento |
| `qtd_status_plataforma_distintos_mes` | Status distintos | Consistência operacional |

#### 10. Flags de Cobertura
| Feature | Descrição | Uso |
|---------|-----------|-----|
| `flag_sem_recarga_mes` | Cliente sem recarga | Missing indicator |
| `flag_baixa_atividade_mes` | < 2 recargas | Alerta de inatividade |

---

## Uso

### Execução

```bash
# Modo padrão (caminhos default)
python src/jobs/02_gold/gold_recarga_features_v2.py

# Com caminhos customizados
python src/jobs/02_gold/gold_recarga_features_v2.py \
    --input_path /Volumes/.../silver/recarga_silver_delta/ \
    --output_path /Volumes/.../gold/recarga_features_v2_delta/

# Databricks Notebook
%run /Workspace/src/jobs/02_gold/gold_recarga_features_v2.py
```

### Parâmetros

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `--input_path` | `/Volumes/hackathon_2025/default/silver/recarga_silver_delta/` | Silver Recarga |
| `--output_path` | `/Volumes/hackathon_2025/default/gold/recarga_features_v2_delta/` | Output Gold |
| `--format` | `delta` | Formato de leitura/escrita |
| `--skip_save` | False | Pular salvamento (debug) |

### Output

```
Grão: NUM_CPF + SAFRA_RECARGA (1:1)
Particionamento: SAFRA_RECARGA
Formato: Delta Lake
Tabela UC: hackathon_2025.default.gold_recarga_features_v2
```

---

## Integração com ABT v5

### Join com Spine

```python
# Leitura
df_spine = spark.read.delta(gold_abt_v4_path)
df_recarga = spark.read.delta(gold_recarga_features_v2_path)

# Join com filtro anti-leakage
# SAFRA_RECARGA deve ser ANTERIOR à SAFRA do spine
df_abt_v5 = df_spine.join(
    df_recarga.filter(F.col("safra_recarga") < F.col("safra")),
    on=["num_cpf"],
    how="left"
)
```

### Janelas Temporais (M1/M3/M6)

Para criar features por janela temporal:

```python
# M1: último mês antes da safra
df_m1 = df_recarga.filter(
    F.months_between(F.col("dt_safra"), F.col("dt_recarga_safra")) <= 1
)

# M3: últimos 3 meses
df_m3 = df_recarga.filter(
    F.months_between(F.col("dt_safra"), F.col("dt_recarga_safra")) <= 3
)

# M6: últimos 6 meses
df_m6 = df_recarga.filter(
    F.months_between(F.col("dt_safra"), F.col("dt_recarga_safra")) <= 6
)
```

---

## Validações

### Gates Recomendados para ABT v5

| Gate | Descrição | Critério |
|------|-----------|----------|
| 9 | Cobertura Recarga | > 5% dos registros com qtd_recargas > 0 |
| 10 | SOS distribution | pct_sos_sobre_credito médio < 50% |
| 11 | Valores sensatos | sum_val_real_ajustado > 0 para maioria |

---

## Notas Importantes

### Anti-Leakage
- **SAFRA_RECARGA < SAFRA** (sempre usar dados do passado)
- FPD e FLAG_INSTALACAO **NÃO** são features (labels apenas)
- Janelas temporais garantem lookback correto

### Tratamento de Nulls
- Clientes sem recarga: features = 0 ou NULL (depende da feature)
- Flag `flag_sem_recarga_mes` indica ausência de dados

### Performance
- Particionamento por `SAFRA_RECARGA` otimiza queries temporais
- Compressão típica: ~95M eventos → ~X milhões de registros cliente-mês

---

## Changelog

| Versão | Data | Alterações |
|--------|------|------------|
| v2.0 | 2026-01-29 | Versão unificada com 60+ features comportamentais |

---

## Referências

- [target_definition.md](../target_definition.md) — Definição de labels e anti-leakage
- [recarga.md](../01_data_dictionary/recarga.md) — Dicionário de dados Recarga
- [03_bronze_silver_recarga.py](../../src/jobs/01_silver/03_bronze_silver_recarga.py) — Silver layer
- [tratamento_recarga_v1.py](../../src/jobs/02_gold/tratamento_recarga_v1.py) — Tratamento original
- Reunião 07/01/2026 — Explicação de SOS por Fernando (Claro)
