# Script 04: Bronze → Silver Pagamento

**Arquivo:** `src/jobs/01_silver/04_bronze_silver_pagamento.py`
**Ordem no Pipeline:** 5º (após Recarga)
**Função:** Processar histórico de pagamentos de faturas

---

## Visão Geral

A base Pagamento contém **registros de pagamento de faturas**. O principal desafio é o **versionamento**: uma mesma fatura pode ter múltiplas versões (atualizações de status).

**Grão:** 1 linha por **PAGAMENTO** (evento-level)
**Volume:** ~21.8 milhões de registros

**Particularidades:**
- Versionamento: ~8.163 chaves com 2 versões cada
- Juros negativos são contábeis (não erro)
- ~28% de DAT_STATUS_PAGAMENTO missing
- Derivação de VAL_JUROS_POS e VAL_JUROS_NEG_ABS

---

## Diferença: Versionamento vs Duplicata

| Conceito | Recarga | Pagamento |
|----------|---------|-----------|
| Problema | Duplicatas reais | Versões da mesma fatura |
| Causa | Ingestão duplicada | Atualização de status |
| Solução | Hash event_key | Manter versão mais recente |
| Critério | TS_RECARGA DESC | TS_STATUS_FATURA DESC |

**Exemplo de versionamento:**
```
Fatura 12345:
  v1: 01/Jan - Status "ABERTA" - R$100
  v2: 15/Jan - Status "PAGA"   - R$100  ← Manter esta
```

---

## Código Completo Explicado

### Bloco 1: Configuração (Linhas 43-54)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/pagamento_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/pagamento_silver_delta/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_pagamento_v1"

# Sentinelas observadas
SENTINELAS = ['-1', '-2', '-3']
# =============================================================================
```

**Por que SILVER_VERSION como constante?**
Facilita rastreabilidade: cada registro sabe qual versão do script o processou.

---

### Bloco 2: Função build_silver_pagamento (Linhas 56-182)

#### 2.1: Parse de Datas (Linhas 70-80)

```python
def build_silver_pagamento(df_bronze):
    print(">>> [Transform] Construindo Silver Pagamento...")

    # Step 1: Parse de datas
    print("    → Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_status_fatura",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FATURA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_pagamento",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_PAGAMENTO")), "ddMMMyyyy:HH:mm:ss")
    )
```

**Por que `F.upper()` antes do parse?**

```python
F.to_timestamp(F.upper(F.col("DAT_STATUS_FATURA")), "ddMMMyyyy:HH:mm:ss")
```

O formato `MMM` (mês abreviado) é case-sensitive:
- `15JAN2024` ✓ Funciona
- `15jan2024` ❌ Pode falhar

`F.upper()` normaliza para maiúsculas, garantindo parse consistente.

#### 2.2: Derivação de SAFRA_PAGAMENTO (Linhas 82-89)

```python
    # Step 2: Derivar SAFRA_PAGAMENTO
    print("    → Step 2: Derivando SAFRA_PAGAMENTO...")
    df = df.withColumn(
        "safra_pagamento",
        F.date_format(F.to_date(F.col("ts_status_fatura")), "yyyyMM")
    )
```

**Por que usar TS_STATUS_FATURA e não TS_STATUS_PAGAMENTO?**
- `TS_STATUS_FATURA`: Sempre preenchido (100%)
- `TS_STATUS_PAGAMENTO`: ~28% missing

A safra deve ser derivada de uma coluna confiável.

#### 2.3: Casting de Valores Monetários (Linhas 91-114)

```python
    # Step 3: Cast de valores monetários (double)
    print("    → Step 3: Casting valores monetários...")
    monetary_cols = [
        "VAL_PAGAMENTO_FATURA",
        "VAL_PAGAMENTO_ITEM",
        "VAL_ATUAL_PAGAMENTO",
        "VAL_ORIGINAL_PAGAMENTO",
        "VAL_PAGAMENTO_CREDITO",
        "VAL_DESCONTO_ITEM",
        "VAL_JUROS_MULTAS_ITEM",
        "VAL_MULTA_EQUIP_ITEM",
        "VAL_MULTA_EQUIP_TOTAL",
        "VAL_MULTA_FID_ITEM",
        "VAL_BAIXA_ATIVIDADE"
    ]

    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(
                col.lower(),
                to_double_safe(col)
            )
```

**Por que `col.lower()`?**
Padroniza nomes para snake_case (convenção do projeto).

#### 2.4: Flags e Tratamento de Juros (Linhas 116-143) ⭐

```python
    # Step 4: Flags de sentinelas e condições especiais
    print("    → Step 4: Criando flags...")

    # Flag: DAT_STATUS_PAGAMENTO missing
    df = df.withColumn(
        "flag_ts_status_pagamento_missing",
        F.when(F.col("ts_status_pagamento").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # Flag: VAL_JUROS_MULTAS_ITEM negativo
    df = df.withColumn(
        "flag_juros_neg",
        F.when(
            F.col("val_juros_multas_item").isNotNull() & (F.col("val_juros_multas_item") < 0),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # Criar VAL_JUROS_POS e VAL_JUROS_NEG_ABS para Gold
    df = df.withColumn(
        "val_juros_pos",
        F.greatest(F.col("val_juros_multas_item"), F.lit(0))
    ).withColumn(
        "val_juros_neg_abs",
        F.abs(F.least(F.col("val_juros_multas_item"), F.lit(0)))
    )
```

**O que significam juros negativos?**

| Valor | Significado | Exemplo |
|-------|-------------|---------|
| +10.00 | Juros cobrados | Cliente pagou atrasado |
| -5.00 | Juros estornados | Negociação/acordo |
| 0.00 | Sem juros | Pagou em dia |

**Por que separar em VAL_JUROS_POS e VAL_JUROS_NEG_ABS?**

```python
# Exemplo: val_juros_multas_item = -5.00

val_juros_pos = F.greatest(-5.00, 0) = 0.00      # Parte positiva
val_juros_neg_abs = F.abs(F.least(-5.00, 0)) = 5.00  # Parte negativa (absoluto)
```

Na Gold, isso permite:
- `SUM(val_juros_pos)` → Total de juros cobrados
- `SUM(val_juros_neg_abs)` → Total de juros estornados

**Explicação das funções:**

```python
F.greatest(col, 0)  # Retorna o maior entre col e 0
                    # Se col < 0, retorna 0
                    # Se col > 0, retorna col

F.least(col, 0)     # Retorna o menor entre col e 0
                    # Se col < 0, retorna col
                    # Se col > 0, retorna 0

F.abs(...)          # Valor absoluto
```

#### 2.5: Deduplicação por Versionamento (Linhas 145-162) ⭐

```python
    # Step 5: Deduplicação por versionamento
    print("    → Step 5: Deduplicando por versionamento...")

    # Criar DEDUP_KEY
    df = df.withColumn(
        "dedup_key",
        F.concat_ws("#", F.col("NUM_CPF"), F.col("CONTRATO"), F.col("SEQ_FATURA"),
                    F.col("NUM_SUB_SEQ_FATURA"), F.col("NUM_CREDITO_SEQ"))
    )

    # Aplicar row_number para deduplicação
    window_spec = Window.partitionBy("dedup_key").orderBy(F.col("ts_status_fatura").desc())
    df = df.withColumn("rn", F.row_number().over(window_spec))

    # Manter apenas rn = 1
    df_dedup = df.filter(F.col("rn") == 1).drop("rn", "dedup_key")
```

**Explicação da chave de deduplicação:**

```python
F.concat_ws("#", F.col("NUM_CPF"), F.col("CONTRATO"), F.col("SEQ_FATURA"),
            F.col("NUM_SUB_SEQ_FATURA"), F.col("NUM_CREDITO_SEQ"))
```

| Coluna | Significado |
|--------|-------------|
| NUM_CPF | Cliente |
| CONTRATO | Contrato do cliente |
| SEQ_FATURA | Número da fatura |
| NUM_SUB_SEQ_FATURA | Sub-sequência (parcelas) |
| NUM_CREDITO_SEQ | Sequência do crédito |

**Juntas formam a chave única de um pagamento.**

**Por que separador "#"?**
Mesmo motivo que "||" na Recarga: evitar colisões.

**Por que ordenar por TS_STATUS_FATURA DESC?**
Manter a versão **mais recente** (última atualização de status).

#### 2.6: Padronização de Nomes (Linhas 164-168)

```python
    # Step 6: Padronizar nomes de coluna (snake_case)
    print("    → Step 6: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df_dedup)
```

**Por que padronizar DEPOIS da deduplicação?**
- A deduplicação usa nomes originais (NUM_CPF, CONTRATO)
- Padronizar antes quebraria as referências

---

### Bloco 3: Quality Gates (Linhas 233-251)

```python
    # 3. VALIDAÇÕES SIMPLES (GATES)
    print(">>> [Validate] Executando gates de qualidade...")

    # Gate 1: TS_STATUS_FATURA parseado
    invalidos_ts = df_silver.filter(F.col("ts_status_fatura").isNull()).count()
    print(f"    Gate 1 - TS_STATUS_FATURA inválidos: {invalidos_ts}")

    # Gate 2: NUM_CPF não nulo
    nulos_cpf = df_silver.filter(F.col("num_cpf").isNull()).count()
    print(f"    Gate 2 - NUM_CPF nulos: {nulos_cpf}")

    # Gate 3: Monitoramento VAL_JUROS_NEG
    juros_neg_count = df_silver.filter(F.col("flag_juros_neg") == 1).count()
    juros_neg_pct = (juros_neg_count / count_silver) * 100
    print(f"    Gate 3 - VAL_JUROS_MULTAS_ITEM < 0: {juros_neg_count:,} ({juros_neg_pct:.2f}%)")

    # Gate 4: Monitoramento DAT_STATUS_PAGAMENTO missing
    missing_status_pag = df_silver.filter(F.col("flag_ts_status_pagamento_missing") == 1).count()
    missing_pct = (missing_status_pag / count_silver) * 100
    print(f"    Gate 4 - DAT_STATUS_PAGAMENTO missing: {missing_status_pag:,} ({missing_pct:.2f}%)")
```

**Gates específicos do Pagamento:**

| Gate | Métrica | Esperado | Ação |
|------|---------|----------|------|
| 1 | TS_STATUS_FATURA null | 0 | Sempre preenchido |
| 2 | NUM_CPF null | 0 | Chave obrigatória |
| 3 | Juros negativos | ~X% | Normal (estornos) |
| 4 | Status pagamento missing | ~28% | Esperado |

---

### Bloco 4: Relatório Final (Linhas 280-291)

```python
    # 5. RELATÓRIO FINAL
    print("\n" + "="*80)
    print("RELATÓRIO FINAL — Silver Pagamento")
    print("="*80)
    print(f"  Registros Bronze (entrada): {count_bronze:,}")
    print(f"  Registros Silver (saída):   {count_silver:,}")
    print(f"  Retenção: {(count_silver/count_bronze)*100:.2f}%")
    print(f"  Linhas removidas (versionamento): {linhas_removidas:,}")
    print(f"  Colunas originais: {len(df_bronze.columns)}")
    print(f"  Colunas após transformação: {len(df_silver.columns)}")
    print(f"  Próximo passo: Gold (05_gold_abt_v6_builder.py)")
    print("="*80 + "\n")
```

**Por que relatório formatado?**
- Facilita debug visual
- Documenta execução
- Confirma sucesso do pipeline

---

## Diagrama de Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  pagamento      │
│  (~21.8M)       │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ build_silver_pagamento()│
│                         │
│ Step 1: Parse datas     │  ← F.upper() + to_timestamp
│ Step 2: SAFRA_PAGAMENTO │
│ Step 3: Cast monetários │
│ Step 4: Flags           │  ← juros_neg, status_missing
│         + VAL_JUROS_POS │  ← F.greatest
│         + VAL_JUROS_NEG │  ← F.least + F.abs
│ Step 5: Dedup versão    │  ← row_number by dedup_key
│ Step 6: snake_case      │
│ Step 7: Metadados       │
└────────┬────────────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  pagamento      │
│  (~21.8M - 8k)  │  ← Versões removidas
└─────────────────┘
```

---

## Colunas de Saída

| Categoria | Colunas Principais | Quantidade |
|-----------|-------------------|------------|
| Chaves | num_cpf, contrato, seq_fatura, ... | 5+ |
| Timestamps | ts_status_fatura, ts_status_pagamento, safra_pagamento | 3 |
| Valores | val_pagamento_*, val_desconto_*, val_juros_* | ~11 |
| Juros separados | val_juros_pos, val_juros_neg_abs | 2 |
| Flags | flag_ts_status_pagamento_missing, flag_juros_neg | 2 |
| Auditoria | metadata_* | 5 |
| **Total** | | **~50** |

---

## Features para Gold

Na camada Gold, essas colunas serão agregadas por cliente-mês:

| Feature Gold | Origem Silver | Agregação |
|--------------|---------------|-----------|
| qtd_pagamentos_m1 | COUNT(*) | Contagem |
| sum_val_pago_m1 | val_pagamento_fatura | SUM |
| sum_juros_pos_m1 | val_juros_pos | SUM |
| pct_com_desconto_m1 | val_desconto_item > 0 | AVG |
| flag_teve_juros_m1 | val_juros_pos > 0 | MAX |

---

## Comparativo: Recarga vs Pagamento

| Aspecto | Recarga | Pagamento |
|---------|---------|-----------|
| Volume | ~100M | ~21.8M |
| Problema | Duplicatas | Versionamento |
| Dedup key | Hash SHA-256 | Concat natural |
| Ordenação | TS_RECARGA DESC | TS_STATUS_FATURA DESC |
| Valores negativos | Estorno de recarga | Estorno de juros |
| Missing crítico | N/A | DAT_STATUS_PAGAMENTO (~28%) |

---

## Checklist de Validação

- [x] `F.upper()` antes de parse de timestamp
- [x] SAFRA_PAGAMENTO derivada de coluna sempre preenchida
- [x] Valores monetários tipados com `to_double_safe`
- [x] Flag de status pagamento missing
- [x] VAL_JUROS separado em POS e NEG_ABS
- [x] Deduplicação por versionamento (não hash)
- [x] Chave de dedup com separador "#"
- [x] Ordenação por TS_STATUS_FATURA DESC
- [x] `standardize_column_names` APÓS deduplicação
- [x] Relatório formatado com métricas
- [x] Gates de qualidade (4 checks)
