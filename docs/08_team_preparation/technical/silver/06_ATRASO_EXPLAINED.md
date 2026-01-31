# Script 05: Bronze → Silver Atraso

**Arquivo:** `src/jobs/01_silver/05_bronze_silver_atraso.py`
**Ordem no Pipeline:** 6º (último da Silver)
**Função:** Processar faturas em aberto e aging (snapshot mensal)

---

## Visão Geral

A base Atraso contém **snapshot mensal** do estado de faturas em aberto. Diferente de Recarga e Pagamento, não é transacional — é uma "fotografia" do estado no primeiro dia de cada mês.

**Grão:** 1 linha por **FATURA/ITEM** (múltiplos por cliente)
**Volume:** ~31.6 milhões de registros

**Particularidades:**
- **Snapshot mensal:** DAT_REFERENCIA sempre dia 01
- **Anti-leakage garantido:** Dados refletem estado NA DATA do snapshot
- Sem deduplicação agressiva (pode apagar sinal real)
- Sentinelas em várias colunas categóricas
- ~3.4% de DAT_STATUS_FAT missing

---

## Conceito: Snapshot vs Transacional

| Aspecto | Transacional (Recarga/Pagamento) | Snapshot (Atraso) |
|---------|----------------------------------|-------------------|
| Natureza | Evento que aconteceu | Estado em um momento |
| Timestamp | Momento do evento | Dia 01 do mês (fixo) |
| Exemplo | "Recarga de R$20 às 14:30" | "Fatura X tinha R$100 em aberto em 01/Jan" |
| Dedup | Remover duplicatas | Cuidado! Pode ser sinal real |

**Por que snapshot é seguro para anti-leakage?**
Os dados refletem o estado **no momento da fotografia**, não eventos futuros.

---

## Código Completo Explicado

### Bloco 1: Configuração (Linhas 44-64)

```python
# =============================================================================
# CONFIGURAÇÃO PADRÃO
# =============================================================================
DEFAULT_INPUT_PATH = "/Volumes/hackathon_2025/default/bronze/atraso_delta/"
DEFAULT_OUTPUT_PATH = "/Volumes/hackathon_2025/default/silver/atraso_silver_delta/"
DEFAULT_FORMAT = "delta"
SILVER_VERSION = "silver_atraso_v1"

# Sentinelas observadas
SENTINELAS = ['-1', '-2', '-3']

# Colunas com potencial sentinela
COLS_WITH_SENTINELA = {
    "IND_WO": "flag_ind_wo_sentinela",
    "IND_PDD": "flag_ind_pdd_sentinela",
    "IND_PCCR": "flag_ind_pccr_sentinela",
    "DW_TIPO_CLIENTE_CONTA": "flag_dw_tipo_cliente_sentinela",
    "COD_PLATAFORMA": "flag_cod_plataforma_sentinela",
    "DW_FAIXA_TEMPO_BASE": "flag_faixa_tempo_base_sentinela",
    "DW_FAIXA_AGING_PROX_FECH": "flag_faixa_aging_prox_fech_sentinela"
}
# =============================================================================
```

**Por que dicionário para sentinelas?**

```python
COLS_WITH_SENTINELA = {
    "IND_WO": "flag_ind_wo_sentinela",
    ...
}
```

Mapeia coluna original → nome da flag. Mais legível que lista.

**O que significam essas colunas?**

| Coluna | Significado | Negócio |
|--------|-------------|---------|
| IND_WO | Write-Off | Dívida baixada como prejuízo |
| IND_PDD | PDD | Provisão para Devedores Duvidosos |
| IND_PCCR | PCCR | Pagamento de Crédito em Cobrança/Recuperação |
| DW_TIPO_CLIENTE_CONTA | Tipo de cliente | Pessoa física/jurídica |
| COD_PLATAFORMA | Plataforma | Sistema de origem |
| DW_FAIXA_TEMPO_BASE | Faixa tempo | Tempo de relacionamento |
| DW_FAIXA_AGING_PROX_FECH | Aging | Envelhecimento da dívida |

---

### Bloco 2: Função build_silver_atraso (Linhas 68-172)

#### 2.1: Parse de Datas (Linhas 80-95)

```python
def build_silver_atraso(df_bronze):
    print(">>> [Transform] Construindo Silver Atraso...")

    # Step 1: Parse de datas
    print("    → Step 1: Parseando datas...")
    df = df_bronze.withColumn(
        "ts_referencia",
        F.to_timestamp(F.upper(F.col("DAT_REFERENCIA")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_vencimento",
        F.to_timestamp(F.upper(F.col("DAT_VENCIMENTO_FAT")), "ddMMMyyyy:HH:mm:ss")
    ).withColumn(
        "ts_status_fat",
        F.to_timestamp(F.upper(F.col("DAT_STATUS_FAT")), "ddMMMyyyy:HH:mm:ss")
    )
```

**Três timestamps importantes:**

| Coluna | Significado | Uso |
|--------|-------------|-----|
| ts_referencia | Data do snapshot | Sempre dia 01, derivar SAFRA |
| ts_vencimento | Vencimento da fatura | Calcular aging |
| ts_status_fat | Última atualização | Pode ter missing (~3.4%) |

#### 2.2: Derivação de SAFRA_ATRASO (Linhas 97-104)

```python
    # Step 2: Derivar SAFRA_ATRASO
    print("    → Step 2: Derivando SAFRA_ATRASO...")
    df = df.withColumn(
        "safra_atraso",
        F.date_format(F.to_date(F.col("ts_referencia")), "yyyyMM")
    )
```

**Por que TS_REFERENCIA para SAFRA?**
- É a data do snapshot (sempre válida)
- Sempre dia 01 (consistente)
- Representa "quando esta fotografia foi tirada"

#### 2.3: Casting de Valores Monetários (Linhas 106-130)

```python
    # Step 3: Cast de valores monetários (double)
    print("    → Step 3: Casting valores monetários...")
    monetary_cols = [
        "VAL_FAT_LIQUIDO",
        "VAL_FAT_BRUTO",
        "VAL_FAT_CREDITO",
        "VAL_FAT_AJUSTE",
        "VAL_FAT_BRUTO_BC",
        "VAL_FAT_PAGAMENTO_BRUTO",
        "VAL_FAT_ABERTO",
        "VAL_FAT_ABERTO_LIQ",
        "VAL_MULTA_JUROS",
        "VAL_MULTA_CANCELAMENTO",
        "VAL_PARC_APARELHO_LIQ",
        "VAL_FAT_LIQ_JM_MC"
    ]

    for col in monetary_cols:
        if col in df.columns:
            df = df.withColumn(
                col.lower(),
                to_double_safe(col)
            )
```

**Colunas mais importantes para risco:**

| Coluna | Significado | Importância |
|--------|-------------|-------------|
| val_fat_aberto | Valor em aberto | Exposição atual |
| val_fat_aberto_liq | Aberto líquido | Exposição real |
| val_multa_juros | Multas e juros | Indica atraso |

#### 2.4: Flags de Sentinelas (Linhas 132-152) ⭐

```python
    # Step 4: Flags de sentinelas e missing
    print("    → Step 4: Criando flags de sentinelas...")

    # Flag: DAT_STATUS_FAT missing
    df = df.withColumn(
        "flag_status_fat_missing",
        F.when(F.col("ts_status_fat").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    # Flags para colunas com sentinelas
    for col_orig, flag_name in COLS_WITH_SENTINELA.items():
        if col_orig in df.columns:
            df = df.withColumn(
                flag_name,
                F.when(
                    F.col(col_orig.lower()).isin(SENTINELAS),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
```

**Explicação do loop:**

```python
for col_orig, flag_name in COLS_WITH_SENTINELA.items():
```

Itera sobre o dicionário:
- `col_orig` = "IND_WO"
- `flag_name` = "flag_ind_wo_sentinela"

```python
    F.col(col_orig.lower()).isin(SENTINELAS)
```

- `col_orig.lower()` = "ind_wo" (padronizado)
- `.isin(SENTINELAS)` = verifica se é '-1', '-2', ou '-3'

**Por que SENTINELAS são strings?**

```python
SENTINELAS = ['-1', '-2', '-3']  # Strings, não inteiros
```

Algumas colunas são categóricas (texto). Comparar como string é mais seguro.

#### 2.5: Padronização e Metadados (Linhas 154-170)

```python
    # Step 5: Padronizar nomes de coluna (snake_case)
    print("    → Step 5: Padronizando nomes de colunas...")
    df_silver = standardize_column_names(df)

    # Step 6: Metadados da Silver
    print("    → Step 6: Adicionando metadados...")
    df_silver = df_silver.withColumn(
        "metadata_data_transformacao",
        F.current_timestamp()
    ).withColumn(
        "metadata_versao_regra",
        F.lit(SILVER_VERSION)
    )

    return df_silver
```

**Diferença dos outros scripts:**
- **NÃO há deduplicação** (passo 5 pulado)
- Metadados adicionados direto

---

### Bloco 3: Por Que Não Há Deduplicação?

```python
# OUTROS SCRIPTS:
df_silver_dedup = dedupe_by_key(df_silver)  # Existe

# ATRASO:
# Sem função de dedup!
```

**Motivo:** Atraso é snapshot mensal. Cada linha representa uma fatura/item diferente no momento da fotografia. "Duplicatas" podem ser:
- Múltiplas faturas do mesmo cliente
- Múltiplos itens da mesma fatura
- Faturas de meses diferentes

**Remover "duplicatas" poderia apagar informação real.**

**Validação no main():**

```python
    # Auditoria (sem dedup agressiva, espera-se retenção ~100%)
    retenção_pct = (count_silver / count_bronze) * 100
    print(f">>> [Auditoria] Retenção: {retenção_pct:.2f}%")
```

Se retenção < 100%, algo está errado (não deveria perder linhas).

---

### Bloco 4: Quality Gates (Linhas 222-241)

```python
    # 3. VALIDAÇÕES SIMPLES (GATES)
    print(">>> [Validate] Executando gates de qualidade...")

    # Gate 1: TS_REFERENCIA parseado (sempre válido)
    invalidos_ref = df_silver.filter(F.col("ts_referencia").isNull()).count()
    print(f"    Gate 1 - TS_REFERENCIA inválidos: {invalidos_ref}")

    # Gate 2: NUM_CPF não nulo
    nulos_cpf = df_silver.filter(F.col("num_cpf").isNull()).count()
    print(f"    Gate 2 - NUM_CPF nulos: {nulos_cpf}")

    # Gate 3: Monitoramento DAT_STATUS_FAT missing
    missing_status = df_silver.filter(F.col("flag_status_fat_missing") == 1).count()
    missing_pct = (missing_status / count_silver) * 100
    print(f"    Gate 3 - DAT_STATUS_FAT missing: {missing_status:,} ({missing_pct:.2f}%)")

    # Gate 4: Monitoramento de sentinelas
    if "flag_cod_plataforma_sentinela" in df_silver.columns:
        sentinela_count = df_silver.filter(F.col("flag_cod_plataforma_sentinela") == 1).count()
        sentinela_pct = (sentinela_count / count_silver) * 100
        print(f"    Gate 4 - COD_PLATAFORMA com sentinela: {sentinela_count:,} ({sentinela_pct:.2f}%)")
```

**Gates específicos do Atraso:**

| Gate | Métrica | Esperado |
|------|---------|----------|
| 1 | TS_REFERENCIA null | 0 (sempre preenchido) |
| 2 | NUM_CPF null | 0 (chave obrigatória) |
| 3 | Status FAT missing | ~3.4% |
| 4 | Sentinelas | Varia por coluna |

---

### Bloco 5: Relatório Final (Linhas 268-281)

```python
    # 5. RELATÓRIO FINAL
    print("\n" + "="*80)
    print("RELATÓRIO FINAL — Silver Atraso/Faturamento")
    print("="*80)
    print(f"  Registros Bronze (entrada): {count_bronze:,}")
    print(f"  Registros Silver (saída):   {count_silver:,}")
    print(f"  Retenção: {retenção_pct:.2f}%")
    print(f"  Colunas originais: {len(df_bronze.columns)}")
    print(f"  Colunas após transformação: {len(df_silver.columns)}")
    print(f"  Caminho Delta: {args.output_path}")
    print(f"  Próximo passo: Gold (05_gold_abt_v6_builder.py)")
    print(f"  Nota: Snapshot mensal (sempre dia 01) — anti-leakage garantido")
    print("="*80 + "\n")
```

**Destaque:** Nota explícita sobre anti-leakage.

---

## Diagrama de Fluxo

```
┌─────────────────┐
│  BRONZE         │
│  atraso         │
│  (~31.6M)       │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ build_silver_atraso()   │
│                         │
│ Step 1: Parse datas     │  ← 3 timestamps
│ Step 2: SAFRA_ATRASO    │  ← De ts_referencia
│ Step 3: Cast monetários │  ← 12 colunas
│ Step 4: Flags sentinela │  ← 7 colunas
│ Step 5: snake_case      │
│ Step 6: Metadados       │
│                         │
│ ⚠️ SEM DEDUP ⚠️         │  ← Diferente!
└────────┬────────────────┘
         │
         ▼
┌─────────────────┐
│  SILVER         │
│  atraso         │
│  (~31.6M)       │  ← 100% retenção
└─────────────────┘
```

---

## Colunas de Saída

| Categoria | Colunas Principais | Quantidade |
|-----------|-------------------|------------|
| Chaves | num_cpf, contrato, seq_fatura, ... | 5+ |
| Timestamps | ts_referencia, ts_vencimento, ts_status_fat, safra_atraso | 4 |
| Valores | val_fat_*, val_multa_* | ~12 |
| Indicadores | ind_wo, ind_pdd, ind_pccr, ... | 5+ |
| Flags sentinela | flag_*_sentinela | 7 |
| Flag missing | flag_status_fat_missing | 1 |
| Auditoria | metadata_* | 5 |
| **Total** | | **~58** |

---

## Features para Gold

Na camada Gold, essas colunas serão agregadas por cliente-mês:

| Feature Gold | Origem Silver | Agregação |
|--------------|---------------|-----------|
| qtd_faturas_abertas_m1 | COUNT(*) where val_fat_aberto > 0 | Contagem |
| sum_val_aberto_m1 | val_fat_aberto | SUM |
| pct_aging_90_plus_m1 | Faixas de aging | % |
| flag_teve_wo_m1 | ind_wo = 'S' | MAX |
| flag_teve_pdd_m1 | ind_pdd = 'S' | MAX |
| max_dias_atraso_m1 | ts_referencia - ts_vencimento | MAX |

---

## Indicadores de Risco

### IND_WO (Write-Off)

```
IND_WO = 'S' → Dívida foi baixada como prejuízo
             → Cliente já deu calote antes
             → ALTO RISCO
```

### IND_PDD (Provisão para Devedores Duvidosos)

```
IND_PDD = 'S' → Empresa provisionou perda
              → Cliente considerado de alto risco
              → ALTO RISCO
```

### IND_PCCR (Pagamento de Crédito em Cobrança/Recuperação)

```
IND_PCCR = 'S' → Cliente está em processo de recuperação
               → Já foi cobrado/negativado
               → RISCO MODERADO (está tentando pagar)
```

---

## Anti-Leakage: Por Que Atraso é Seguro

### Problema de Leakage (Outros Contextos)

```
❌ ERRADO: Usar dados de Mar/2024 para prever decisão de Jan/2024
           (dados do futuro contaminam previsão)
```

### Por Que Atraso é Diferente

```
✓ CORRETO: Snapshot de Jan/2024 reflete estado EM Jan/2024
           Não há informação do futuro
           Só usamos snapshots ANTERIORES à decisão
```

**Regra na Gold:**
```python
# Filtrar apenas snapshots anteriores à safra do cliente
df_atraso_filtrado = df_atraso.filter(
    F.col("safra_atraso") < F.col("safra")  # Snapshot < Decisão
)
```

---

## Comparativo: Todos os Scripts Silver

| Aspecto | Bureau/Telco/Cadastro | Recarga | Pagamento | Atraso |
|---------|----------------------|---------|-----------|--------|
| Natureza | Cliente-mês | Evento | Evento | Snapshot |
| Volume | ~3.8M | ~100M | ~21.8M | ~31.6M |
| Dedup | row_number CPF+SAFRA | Hash event_key | Versionamento | **NÃO TEM** |
| Sentinela | 0 ou 304 | -1/-2/-3 | -1/-2/-3 | -1/-2/-3 |
| Missing crítico | idade (fixado) | N/A | ~28% status | ~3.4% status |
| Anti-leakage | N/A | safra_recarga < safra | safra_pag < safra | Garantido (snapshot) |

---

## Checklist de Validação

- [x] Três timestamps parseados (referencia, vencimento, status)
- [x] SAFRA_ATRASO derivada de ts_referencia
- [x] 12 colunas monetárias tipadas
- [x] 7 flags de sentinela via dicionário
- [x] Flag de status_fat missing
- [x] **NÃO há deduplicação** (intencional)
- [x] Retenção esperada: 100%
- [x] Nota de anti-leakage no relatório
- [x] Gates: ts_referencia, num_cpf, status missing, sentinelas
- [x] Metadados de transformação
