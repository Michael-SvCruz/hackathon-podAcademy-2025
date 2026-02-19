# Comparativo de Versões — Silver Recarga

> Documento de referência para escolha da versão de produção do pipeline Bronze → Silver Recarga.
> Gerado em: 2026-02-19

---

## Contexto

O script `silver_recarga.py` passou por múltiplas iterações de otimização. Este documento compara as variantes disponíveis em `opc_standby/` com o script principal, detalhando arquitetura, I/O, prós, contras e recomendações de uso.

**Dados de referência:**

| Item | Valor |
|------|-------|
| Volume Bronze | ~100M registros / ~4 GB (Parquet comprimido) |
| Volume Silver | ~100M registros / ~3-4 GB (Delta tipado) |
| Duplicatas estimadas | ~320k (~0,3% do total) |
| Executors configurados | 2-8 × 4 OCPU |
| Target arquivo de saída | ~128 MB por arquivo |

---

## Mapa de Versões

```
scripts/
├── silver_recarga.py                   ← PRINCIPAL (baseline atual)
└── opc_standby/
    ├── silver_recarga_opt_x.py         ← Opção X: cache + count embutido no agg
    ├── silver_recarga_opt_y.py         ← Opção Y: sem cache, quality na Silver escrita
    └── silver_recarga_with_dedup.py    ← With-Dedup: com deduplicação por EVENT_KEY
```

---

## Fluxo de Execução por Versão

```
PRINCIPAL (baseline)
  Bronze(OCI) → standardize → build_silver → cache()
             → count()               [action 1 — materializa cache]
             → coalesce(N).write()   [action 2 — lê cache]
             → agg().collect()       [action 3 — lê cache]
  I/O OCI: ~4GB leitura Bronze + cache scans internos

──────────────────────────────────────────────────────────────

OPT-X (cache + count no agg)
  Bronze(OCI) → standardize → build_silver → cache()
             → coalesce(28).write()           [action 1 — materializa cache]
             → agg(count(*) + métricas)       [action 2 — lê cache]
  I/O OCI: ~4GB leitura Bronze + 2 cache scans (vs 3 no principal)

──────────────────────────────────────────────────────────────

OPT-Y (sem cache, quality na Silver escrita)
  Bronze(OCI) → standardize → build_silver
             → coalesce(28).write()           [action 1 — lê Bronze 1x do OCI]
  Silver(OCI) → agg(count(*) + métricas)      [action 2 — lê Silver 1x do OCI]
  I/O OCI: ~4GB Bronze + ~3GB Silver = ~7GB total

──────────────────────────────────────────────────────────────

WITH-DEDUP (com deduplicação por EVENT_KEY)
  Bronze(OCI) → standardize → build_silver
             → xxhash64(event_key_cols)
             → dropDuplicates([event_key])    [SHUFFLE: ~20GB I/O distribuído]
             → cache()
             → count()               [action 1 — materializa cache]
             → coalesce(N).write()   [action 2 — lê cache]
             → agg().collect()       [action 3 — lê cache]
  I/O OCI: ~4GB Bronze + ~20GB shuffle + cache scans
```

---

## Tabela Comparativa

| Critério | Principal | Opt-X | Opt-Y | With-Dedup |
|----------|-----------|-------|-------|------------|
| **Actions Spark** | 3 | 2 | 2 | 3 |
| **Shuffle** | Não | Não | Não | Sim (~20GB) |
| **Cache** | Sim | Sim | Não | Sim |
| **Deduplicação** | Não | Não | Não | Sim |
| **Coalesce** | Dinâmico | Fixo (28) | Fixo (28) | Dinâmico |
| **I/O OCI estimado** | ~4GB + cache | ~4GB + cache | ~7GB | ~24GB |
| **Quality valida arquivo real** | Não | Não | **Sim** | Não |
| **Pressão de memória** | Média | Média | **Zero** | Alta |
| **Resultado esperado (duração)** | ~18m (medido) | ~17-18m | ~15-20m¹ | ~25-35m |
| **Registros Silver** | ~100M (com dupl.) | ~100M (com dupl.) | ~100M (com dupl.) | ~99,7M (sem dupl.) |

¹ *Opt-Y depende da velocidade de rede OCI vs memória dos executors — pode variar.*

---

## Análise Detalhada

### Principal — `silver_recarga.py`

**Arquitetura:** Cache + 3 actions (count → write → agg)

**Prós:**
- ✅ Testado e validado em produção (18m25s)
- ✅ Coalesce dinâmico adapta-se automaticamente se o volume crescer
- ✅ Cache protege contra releitura do OCI nas 3 actions
- ✅ Comportamento previsível e bem documentado

**Contras:**
- ❌ 3 actions = 3 leituras do cache (1 desnecessária: o `count()`)
- ❌ Quality valida dados em memória, não o arquivo físico gravado
- ❌ Cache ocupa ~15-20GB de memória dos executors

**Quando usar:**
> Versão padrão de produção. Balanceia performance, simplicidade e observabilidade.

---

### Opção X — `silver_recarga_opt_x.py`

**Arquitetura:** Cache + 2 actions (write → agg+count)

**Diferença chave:** Remove o `count()` separado e emite `F.count("*")` dentro do `agg()` já existente. Coalesce fixo em 28 (calculado com base no volume conhecido).

**Prós:**
- ✅ 1 action a menos que o principal (elimina 1 scan de cache de 100M registros)
- ✅ Ganho estimado: 30-60 segundos
- ✅ Todas as métricas preservadas (count_out vem do agg)
- ✅ Código mais simples — sem lógica de estimativa de tamanho

**Contras:**
- ❌ Coalesce fixo: se o volume crescer significativamente (ex: 200M registros), os arquivos ficarão maiores que o target de 128MB sem aviso
- ❌ Quality ainda valida memória, não o arquivo físico
- ❌ Cache ainda ocupa memória dos executors

**Quando usar:**
> Substituta direta do principal quando se quer ganho marginal de performance sem mudar a lógica. Boa escolha se o volume de dados for estável.

---

### Opção Y — `silver_recarga_opt_y.py`

**Arquitetura:** Sem cache, write direto + quality na Silver escrita

**Diferença chave:** Remove completamente o `cache()`. A escrita lê Bronze uma única vez (pipeline map puro, sem shuffle). O quality check lê a Silver **já gravada no Object Storage**, não uma cópia em memória.

**Prós:**
- ✅ **Zero pressão de memória** nos executors — importante para free tier (quota reduzida de OCPU)
- ✅ **Quality valida o arquivo real no bucket** — detecta falhas silenciosas de escrita que os outros não detectariam
- ✅ 2 actions totais (como Opt-X)
- ✅ Coalesce fixo simples
- ✅ A Silver comprimida (~3GB) pode ser mais rápida de ler que um cache de ~15GB descomprimido

**Contras:**
- ❌ **+3GB de I/O de rede** do OCI para o quality check (leitura da Silver escrita)
- ❌ Se a rede OCI for mais lenta que a memória dos executors, pode ser mais lento que Opt-X
- ❌ Se a escrita falhar parcialmente, o quality check pode gerar métricas incorretas (embora Delta garanta atomicidade)
- ❌ Dois acessos ao Object Storage em vez de um — aumenta latência total

**Quando usar:**
> Quando a disponibilidade de memória for limitada (free tier, poucos executors) **ou** quando houver requisito de auditoria/governança que exija que o quality check valide o dado físico gravado, não o estado em memória.

---

### With-Dedup — `silver_recarga_with_dedup.py`

**Arquitetura:** Deduplicação por EVENT_KEY (xxhash64 + dropDuplicates) + cache + 3 actions

**Diferença chave:** Adiciona a etapa de deduplicação entre o transform e o cache. O `xxhash64` gera um hash de 64 bits (vs SHA-256 de 256 bits da versão original — 5x mais rápido), e o `dropDuplicates` substitui o `Window + row_number` (elimina sort, O(n) vs O(n log n)).

> **Nota:** `KryoSerializer` foi removido — incompatível com OCI Resource Principal. O Kryo altera o class loader da JVM, quebrando a desserialização Jackson usada pelo `X509FederationClient` para renovar tokens IAM (`BmcException -1, MismatchedInputException NO_CREATORS`).

**Prós:**
- ✅ **Silver garantidamente deduplicada** — cada evento aparece exatamente 1 vez
- ✅ Necessário se a Silver for consultada diretamente (sem passar pelo Gold `groupBy`)
- ✅ Correto para pipelines de governança que exigem unicidade event-level
- ✅ `xxhash64` mais eficiente que SHA-256 original
- ✅ `dropDuplicates` mais eficiente que `Window + row_number` original

**Contras:**
- ❌ **Shuffle de ~20GB** para remover apenas ~320k registros (0,3% do total)
  - O `dropDuplicates` redistribui **todos** os 100M registros entre os executors
  - Dados descomprimidos em formato de shuffle (~15-20GB) vs Parquet (~4GB)
- ❌ Duração estimada: **~25-35 minutos** (vs ~18min do principal)
- ❌ Alta pressão de memória: shuffle + cache simultâneos
- ❌ Impacto no modelo final é desprezível: o Gold agrega com `groupBy(NUM_CPF, SAFRA)`, absorvendo as duplicatas nas somas/contagens com erro máximo de ~0,3%

**Quando usar:**
> Somente se houver requisito **explícito** de que a Silver seja deduplicada — por exemplo, consultas analíticas diretas na Silver, relatórios de unicidade de eventos, ou auditoria regulatória. Para o pipeline Gold → ABT, o principal é suficiente.

---

## Diagrama de Decisão

```
Precisa de deduplicação garantida na Silver?
    │
    ├── SIM → silver_recarga_with_dedup.py
    │          (aceitar ~25-35min e ~20GB de shuffle)
    │
    └── NÃO ──────────────────────────────────────────┐
                                                       │
                              Memória dos executors é limitada?
                              (free tier, poucos OCPU)
                                  │
                    ┌─────────────┴──────────────┐
                   SIM                           NÃO
                    │                             │
        silver_recarga_opt_y.py      Volume de dados é estável
        (sem cache, quality no OCI)  e conhecido (~100M registros)?
                                          │
                              ┌───────────┴───────────┐
                             SIM                      NÃO
                              │                        │
               silver_recarga_opt_x.py    silver_recarga.py
               (cache + agg com count)    (coalesce dinâmico)
```

---

## Histórico de Performance (OCI Data Flow)

| Versão | Data | Duração | Data Read | Data Written | Observação |
|--------|------|---------|-----------|--------------|------------|
| Original (Databricks) | — | ~55 min | — | — | Baseline antes da migração |
| silver_recarga.py v1 | 17/02/2026 | ~55 min | 36 GB | 4 GB | Sem otimizações (202 arquivos) |
| silver_recarga.py v2 | 17/02/2026 | 25m 42s | 36 GB | 4 GB | +cache +coalesce +agg unificado |
| silver_recarga.py v3 | 18/02/2026 | 25m 36s | 33 GB | 4 GB | +xxhash64 +dropDuplicates +sem regex |
| **silver_recarga.py v4** | **18/02/2026** | **18m 25s** | **36 GB** | **3 GB** | **Dedup removida (principal atual)** |
| opt_x | 18/02/2026 | 17m 04s | 25 GB | 3 GB | Cache + 2 actions |
| 🏆 **opt_y** | **18/02/2026** | **10m 32s** | **3 GB** | **3 GB** | **Sem cache — VENCEDOR** |
| with_dedup | 18/02/2026 | 32m | 36 GB | 4 GB | Com dedup, sem Kryo |

---

## Recomendação Final — Pós Benchmark (18/02/2026)

**Vencedor absoluto: `silver_recarga_opt_y.py`** — 10m32s, apenas 3GB lidos.

| Posição | Script | Duração | Data Read | Indicado para |
|---------|--------|---------|-----------|---------------|
| 🥇 1º | opt_y | 10m 32s | 3 GB | **Produção — novo principal** |
| 🥈 2º | opt_x | 17m 04s | 25 GB | Fallback se OCI lento |
| 🥉 3º | principal v4 | 18m 25s | 36 GB | Referência anterior |
| 4º | with_dedup | 32m | 36 GB | Somente se dedup for requisito |

**Por que Opt-Y ganhou?**
O cache de 100M registros (~15-20GB de Java objects descomprimidos) gera pressão de GC
e scans lentos. Para pipeline map puro (sem shuffle), reler a Silver já escrita (~3GB Delta
comprimido via OCI) é mais rápido que varrer o cache em memória.

**Próximo passo:** promover `opt_y` para `silver_recarga.py` (principal) e mover a versão
anterior para `opc_standby/`.

---

*Documento gerado automaticamente por análise do pipeline. Atualizar a coluna "A testar" após execução dos benchmarks.*
