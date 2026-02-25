# Comparativo de Versões — Silver Recarga

> Documento de referência para escolha da versão de produção do pipeline Bronze → Silver Recarga.
> Atualizado em: 2026-02-20 (benchmark Opt-Z concluído)

---

## Contexto

O script `silver_recarga.py` passou por múltiplas iterações de otimização. Este documento compara as variantes disponíveis em `opc_standby/` com o script principal, detalhando arquitetura, I/O, prós, contras e recomendações de uso.

**Dados de referência:**

| Item | Valor |
|------|-------|
| Volume Bronze | ~100M registros / ~4 GB (Parquet comprimido) |
| Volume Silver | ~100M registros / ~3-4 GB (Delta tipado) |
| Duplicatas estimadas | ~320k (~0,3% do total) |
| Executors configurados | 2-3 × 4 OCPU |
| Target arquivo de saída | ~128 MB por arquivo |

---

## Mapa de Versões

```
scripts/
├── silver_recarga.py                      ← PRINCIPAL (opt_z promovido 20/02/2026)
└── opc_standby/
    ├── silver_recarga_opt_y.py            ← Opção Y: sem cache, coalesce fixo (28)
    ├── silver_recarga_opt_z.py            ← Opção Z: sem cache, coalesce dinâmico ← cópia do principal
    ├── silver_recarga_opt_x.py            ← Opção X: cache + count embutido no agg
    ├── silver_recarga_v4_cache.py         ← V4: cache + coalesce dinâmico + 3 actions
    └── silver_recarga_with_dedup.py       ← With-Dedup: com deduplicação por EVENT_KEY
```

---

## Fluxo de Execução por Versão

```
PRINCIPAL / OPT-Z (atual — sem cache + coalesce dinâmico)
  Bronze(OCI) → standardize → build_silver
             → count()                     [action 1 — lê Bronze 1x para contar]
             → coalesce(N).write()         [action 2 — lê Bronze 1x para gravar]
  Silver(OCI) → agg(count(*) + métricas)  [action 3 — lê Silver 1x para quality]
  I/O OCI: ~4GB Bronze (count) + ~4GB Bronze (write) + ~3GB Silver = ~11GB

──────────────────────────────────────────────────────────────

OPT-Y (sem cache, coalesce fixo 28)
  Bronze(OCI) → standardize → build_silver
             → coalesce(28).write()           [action 1 — lê Bronze 1x do OCI]
  Silver(OCI) → agg(count(*) + métricas)      [action 2 — lê Silver 1x do OCI]
  I/O OCI: ~4GB Bronze + ~3GB Silver = ~7GB total

──────────────────────────────────────────────────────────────

OPT-X (cache + count no agg)
  Bronze(OCI) → standardize → build_silver → cache()
             → coalesce(28).write()           [action 1 — materializa cache]
             → agg(count(*) + métricas)       [action 2 — lê cache]
  I/O OCI: ~4GB leitura Bronze + 2 cache scans (vs 3 no principal)

──────────────────────────────────────────────────────────────

V4-CACHE (principal anterior — cache + coalesce dinâmico)
  Bronze(OCI) → standardize → build_silver → cache()
             → count()               [action 1 — materializa cache (~15-20GB JVM)]
             → coalesce(N).write()   [action 2 — lê cache]
             → agg().collect()       [action 3 — lê cache]
  I/O OCI: ~4GB leitura Bronze + cache scans internos

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

| Critério | **Principal (Opt-Z)** | Opt-Y | Opt-X | V4-Cache | With-Dedup |
|----------|-----------------------|-------|-------|----------|------------|
| **Actions Spark** | 3 | 2 | 2 | 3 | 3 |
| **Shuffle** | Não | Não | Não | Não | Sim (~20GB) |
| **Cache** | Não | Não | Sim | Sim | Sim |
| **Deduplicação** | Não | Não | Não | Não | Sim |
| **Coalesce** | **Dinâmico** | Fixo (28) | Fixo (28) | Dinâmico | Dinâmico |
| **I/O OCI estimado** | ~11GB | ~7GB | ~4GB + cache | ~4GB + cache | ~24GB |
| **Quality valida arquivo real** | **Sim** | **Sim** | Não | Não | Não |
| **Pressão de memória** | **Zero** | **Zero** | Média | Média | Alta |
| **Duração medida** | **9m 44s** | 10m 32s | 17m 04s | 18m 25s | 32m |
| **Registros Silver** | ~100M (com dupl.) | ~100M | ~100M | ~100M | ~99,7M (sem dupl.) |

---

## Análise Detalhada

### Principal (Opt-Z) — `silver_recarga.py` ← ATUAL

**Arquitetura:** Sem cache + coalesce dinâmico + 3 actions

**Diferença chave vs Opt-Y:** Adiciona `count()` pré-write para calcular `num_output_files` dinamicamente. Pipeline sem cache: Bronze é lido 2x do OCI (count + write). Quality lê Silver já escrita.

**Prós:**
- ✅ **Coalesce dinâmico** — adapta-se automaticamente se o volume crescer (ex: 200M registros)
- ✅ **Zero pressão de memória** — sem cache nos executors
- ✅ **Quality valida o arquivo real no bucket** — detecta falhas silenciosas de escrita
- ✅ **9m44s** — mais rápido que todos os outros (com 3 executors)
- ✅ Sem risco de arquivos maiores que 128MB sem aviso

**Contras:**
- ❌ 1 leitura extra do Bronze vs Opt-Y (~4GB adicional de I/O de rede)
- ❌ Se o volume for estável (~100M), Opt-Y tem menos I/O

**Quando usar:**
> **Versão de produção atual.** Melhor escolha quando o volume pode crescer ou variar entre execuções.

---

### Opção Y — `silver_recarga_opt_y.py`

**Arquitetura:** Sem cache, write direto + quality na Silver escrita, coalesce fixo (28)

**Prós:**
- ✅ **Zero pressão de memória**
- ✅ **Quality valida o arquivo real no bucket**
- ✅ Apenas 2 actions (menos que o principal)
- ✅ I/O mínimo (~7GB vs ~11GB do principal)

**Contras:**
- ❌ **Coalesce fixo em 28** — se o volume dobrar, arquivos ficarão >256MB sem aviso
- ❌ Requer ajuste manual de `NUM_OUTPUT_FILES` ao escalar

**Quando usar:**
> Quando o volume for comprovadamente estável (~100M registros) e se quiser minimizar I/O.

---

### Opção X — `silver_recarga_opt_x.py`

**Arquitetura:** Cache + 2 actions (write materializa cache, agg+count)

**Diferença chave:** Remove o `count()` separado e emite `F.count("*")` dentro do `agg()` já existente. Coalesce fixo em 28.

**Prós:**
- ✅ 1 action a menos que V4-Cache
- ✅ Código simples

**Contras:**
- ❌ Cache ocupa ~15-20GB de memória dos executors
- ❌ Quality valida memória, não o arquivo físico
- ❌ Coalesce fixo

**Quando usar:**
> Fallback se o OCI Object Storage estiver com latência elevada (cache elimina releituras de rede).

---

### V4-Cache — `silver_recarga_v4_cache.py`

**Arquitetura:** Cache + coalesce dinâmico + 3 actions (count → write → agg)

**Benchmark:** 18m25s, 36GB lidos — principal anterior antes das otimizações sem cache.

**Quando usar:**
> Apenas como referência histórica. Substituída pelo principal atual.

---

### With-Dedup — `silver_recarga_with_dedup.py`

**Arquitetura:** Deduplicação por EVENT_KEY (xxhash64 + dropDuplicates) + cache + 3 actions

> **Nota:** `KryoSerializer` foi removido — incompatível com OCI Resource Principal. O Kryo altera o class loader da JVM, quebrando a desserialização Jackson usada pelo `X509FederationClient` para renovar tokens IAM (`BmcException -1, MismatchedInputException NO_CREATORS`).

**Prós:**
- ✅ **Silver garantidamente deduplicada** — cada evento aparece exatamente 1 vez
- ✅ Necessário se a Silver for consultada diretamente (sem passar pelo Gold `groupBy`)

**Contras:**
- ❌ **Shuffle de ~20GB** para remover apenas ~320k registros (0,3% do total)
- ❌ Duração: **~32 minutos** (vs ~10min do principal)
- ❌ Alta pressão de memória: shuffle + cache simultâneos

**Quando usar:**
> Somente se houver requisito **explícito** de que a Silver seja deduplicada — consultas analíticas diretas na Silver, relatórios de unicidade, ou auditoria regulatória.

---

## Diagrama de Decisão

```
Precisa de deduplicação garantida na Silver?
    │
    ├── SIM → silver_recarga_with_dedup.py
    │          (aceitar ~32min e ~20GB de shuffle)
    │
    └── NÃO ───────────────────────────────────────┐
                                                    │
                         Volume de dados pode crescer
                         ou é variável entre execuções?
                                 │
                    ┌────────────┴────────────┐
                   SIM                       NÃO
                    │                         │
        silver_recarga.py          I/O mínimo é crítico?
        (PRINCIPAL — opt_z)              │
        coalesce dinâmico     ┌──────────┴──────────┐
                             SIM                    NÃO
                              │                      │
              silver_recarga_opt_y.py    silver_recarga_opt_x.py
              (coalesce fixo, 2 actions) (cache + 2 actions)
```

---

## Histórico de Performance (OCI Data Flow)

| Versão | Data | Duração | Data Read | Data Written | Observação |
|--------|------|---------|-----------|--------------|------------|
| Original (Databricks) | — | ~55 min | — | — | Baseline antes da migração |
| silver_recarga.py v1 | 17/02/2026 | ~55 min | 36 GB | 4 GB | Sem otimizações (202 arquivos) |
| silver_recarga.py v2 | 17/02/2026 | 25m 42s | 36 GB | 4 GB | +cache +coalesce +agg unificado |
| silver_recarga.py v3 | 18/02/2026 | 25m 36s | 33 GB | 4 GB | +xxhash64 +dropDuplicates +sem regex |
| silver_recarga.py v4 (V4-Cache) | 18/02/2026 | 18m 25s | 36 GB | 3 GB | Dedup removida |
| opt_x | 18/02/2026 | 17m 04s | 25 GB | 3 GB | Cache + 2 actions |
| opt_y | 18/02/2026 | 10m 32s | 3 GB | 3 GB | Sem cache, coalesce fixo |
| with_dedup | 18/02/2026 | 32m | 36 GB | 4 GB | Com dedup, sem Kryo |
| 🏆 **opt_z (PRINCIPAL)** | **20/02/2026** | **9m 44s** | **3 GB** | **3 GB** | **Sem cache, coalesce dinâmico — VENCEDOR** |

---

## Recomendação Final — Pós Benchmark (20/02/2026)

**Vencedor absoluto: `silver_recarga.py` (opt_z)** — 9m44s, apenas 3GB lidos, coalesce dinâmico.

| Posição | Script | Duração | Data Read | Coalesce | Indicado para |
|---------|--------|---------|-----------|----------|---------------|
| 🥇 1º | **principal (opt_z)** | **9m 44s** | **3 GB** | Dinâmico | **Produção — volume variável** |
| 🥈 2º | opt_y | 10m 32s | 3 GB | Fixo (28) | Volume estável comprovado |
| 🥉 3º | opt_x | 17m 04s | 25 GB | Fixo (28) | Fallback se OCI com latência |
| 4º | v4_cache | 18m 25s | 36 GB | Dinâmico | Referência histórica |
| 5º | with_dedup | 32m | 36 GB | Dinâmico | Somente se dedup for requisito |

**Por que Opt-Z/Principal ganhou?**
Combina os dois melhores atributos:
1. **Sem cache** → zero pressão de GC (herança do Opt-Y: 10m32s vs 18m25s do V4)
2. **Coalesce dinâmico** → auto-adapta ao volume sem reconfiguração manual
3. Com 3 executors (vs 2 do Opt-Y), o `count()` extra é absorvido pelo paralelismo adicional

---

*Atualizado em 20/02/2026 após benchmark do opt_z (9m44s, 3 executors).*
