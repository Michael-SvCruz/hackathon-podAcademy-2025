# Guia de Artefatos do Modelo — Bucket `hackathon-2025-models`

Documentação completa dos artefatos gerados pelo script de scoring (`modelo_qualificacao.py`) no bucket OCI Object Storage `hackathon-2025-models`.

---

## Índice

1. [Visão Geral](#1-visão-geral)
   - 1.1 [Conceito de "Run"](#conceito-de-run)
   - 1.2 [Retreino Mensal — É Necessário?](#retreino-mensal--é-necessário)
2. [Estrutura do Bucket](#2-estrutura-do-bucket)
3. [Pasta `pkl/` — Modelo Serializado](#3-pasta-pkl--modelo-serializado)
4. [Pasta `resultados_modelo/` — Predições OOT](#4-pasta-resultados_modelo--predições-oot)
5. [Pasta `metricas/` — Métricas e Features](#5-pasta-metricas--métricas-e-features)
6. [Como Extrair Insights](#6-como-extrair-insights)
   - 6.1 [Análise de Métricas entre Runs](#61-análise-de-métricas-entre-runs)
   - 6.2 [Análise de Predições (Swap-in/Swap-out)](#62-análise-de-predições-swap-inswap-out)
   - 6.3 [Análise de Importância de Features](#63-análise-de-importância-de-features)
   - 6.4 [Distribuição de Score por Decil](#64-distribuição-de-score-por-decil)
   - 6.5 [Comparação de Features entre Versões](#65-comparação-de-features-entre-versões)
7. [Tirando o Melhor Proveito dos Artefatos](#7-tirando-o-melhor-proveito-dos-artefatos)
   - 7.1 [Para a Apresentação do Hackathon](#71-para-a-apresentação-do-hackathon)
   - 7.2 [Para Operação em Produção](#72-para-operação-em-produção)
   - 7.3 [Para Auditoria e Governança](#73-para-auditoria-e-governança)
8. [Comandos Úteis](#8-comandos-úteis)
9. [Boas Práticas](#9-boas-práticas)
10. [Como Testar o Modelo — Entendendo Treino vs OOT](#10-como-testar-o-modelo--entendendo-treino-vs-oot)

---

## 1. Visão Geral

Cada execução do script `modelo_qualificacao.py` gera **três tipos de artefatos** no bucket `hackathon-2025-models`. Juntos, eles formam um registro completo e auditável de cada run do modelo:

```
Execução do Modelo
  │
  ├── pkl/modelo_fpd.pkl                            → O modelo em si (LightGBM)
  ├── resultados_modelo/predicoes_oot_{ts}.parquet   → Predições sobre dados OOT
  └── metricas/
        ├── metricas_{ts}.json                       → Métricas de performance
        └── features_{ts}.txt                        → Features utilizadas
```

**Convenção de nomes:** `{ts}` = timestamp no formato `YYYYMMDD_HHMM` (ex: `20260305_1721`).

### Conceito de "Run"

**Run = cada execução do script `modelo_qualificacao.py`**, independente de treinar ou não.

| Tipo de Run | PKL existe? | Treina modelo? | Gera métricas? | Gera predições? |
|-------------|-------------|----------------|----------------|-----------------|
| **Com treino** | Não | Sim — treina e salva PKL | Sim | Sim |
| **Com PKL existente** | Sim | Não — carrega PKL e pula treino | Sim | Sim |

Todo run gera novos arquivos em `metricas/` e `resultados_modelo/` (com timestamp único). A diferença é apenas se o modelo foi treinado do zero ou carregado do PKL existente.

**Para forçar um novo treino:** deletar `pkl/modelo_fpd.pkl` antes de executar o pipeline. O script detecta a ausência do PKL e treina automaticamente.

### Retreino Mensal — É Necessário?

No nosso pipeline, a cada mês novos dados chegam na landing zone (novas safras de bureau, telco, recarga, pagamento, atraso) e o ETL (Bronze → Silver → Gold → ABT) é reprocessado, gerando uma ABT v6 atualizada. A pergunta natural é: **preciso retreinar o modelo a cada incremento mensal de dados?**

#### Resposta curta: Não necessariamente todo mês, mas sim periodicamente.

#### Por que NÃO precisa retreinar todo mês

1. **O modelo aprende padrões, não dados específicos.** O LightGBM treinado com safras out/nov/dez 2024 aprendeu relações entre features (score de bureau, comportamento de recarga, histórico de pagamento) e a probabilidade de FPD. Essas relações tendem a ser estáveis no curto prazo — um cliente com score baixo, muitas recargas SOS e atraso em faturas continua sendo de alto risco mês a mês.

2. **Re-scoring com modelo existente é o cenário padrão em produção.** A cada mês, o pipeline ETL gera novas features para novos clientes (nova safra). O modelo existente (PKL) é carregado e aplica os padrões aprendidos sobre esses novos dados, gerando scores de risco. Isso é como um médico usando o mesmo protocolo de diagnóstico para pacientes novos — o protocolo não muda a cada consulta.

3. **Estabilidade é desejável.** Retreinar todo mês introduz variabilidade: os hiperparâmetros, features selecionadas e pesos podem mudar, gerando scores diferentes para o mesmo perfil de cliente. Em crédito, essa instabilidade dificulta a calibração de políticas (pontos de corte, limites de aprovação).

#### Quando SIM precisa retreinar

| Sinal de alerta | Como detectar | Ação |
|-----------------|---------------|------|
| **Queda de KS no OOT** | `ks_oot` no JSON de métricas cai > 2 p.p. em relação ao treino original | Retreinar com safras mais recentes |
| **Data drift** | Distribuição das features muda significativamente (ex: ticket médio de recarga dobra) | Investigar causa + retreinar |
| **Mudança de política** | Claro altera critérios de instalação, planos ou público-alvo | Retreinar com dados pós-mudança |
| **Novas features disponíveis** | Pipeline incorpora nova fonte de dados (ex: dados de uso de app) | Retreinar para capturar novo sinal |
| **Degradação temporal** | KS OOT cai gradativamente mês a mês (concept drift) | Retreinar com janela de treino mais recente |

#### Ciclo recomendado para produção

```
Mês 1-3:  Re-scoring mensal (usar PKL existente)
          └── Monitorar ks_oot no JSON de métricas a cada run

Mês 3-4:  Avaliar necessidade de retreino
          └── Comparar ks_oot atual vs ks_oot do treino original
          └── Se gap > 2 p.p.: retreinar
          └── Se estável: continuar re-scoring

Mês 6:    Retreino obrigatório (boas práticas de governança)
          └── Deletar PKL → pipeline treina com safras recentes
          └── Comparar métricas novo vs antigo
          └── Se melhor: manter novo PKL
          └── Se pior: restaurar PKL anterior (rollback)
```

#### Como funciona no nosso pipeline

```
Fluxo mensal (sem retreino):
  ETL (Bronze→ABT) → Carrega PKL existente → Score novos dados → Salva predições + métricas
                      ↑ modelo treinado                           ↑ monitoramento
                      em out/nov/dez 2024                         ks_oot estável?

Fluxo com retreino (quando necessário):
  1. Deletar pkl/modelo_fpd.pkl do bucket
  2. Executar pipeline ETL + Modelo
  3. Script detecta PKL ausente → treina com dados atualizados
  4. Novo PKL salvo → próximos runs usam esse modelo
  5. Comparar métricas novo vs anterior
```

#### Resumo prático

| Pergunta | Resposta |
|----------|----------|
| "Preciso deletar o PKL todo mês?" | **Não.** Só quando for retreinar |
| "O pipeline funciona sem retreino?" | **Sim.** Carrega o PKL e gera scores com o modelo existente |
| "Quando devo retreinar?" | Quando o KS OOT cair > 2 p.p. ou a cada 6 meses (governança) |
| "Como retreino?" | Deletar o PKL + rodar o pipeline. O script treina automaticamente |
| "E se o novo modelo for pior?" | Restaurar o PKL anterior (backup) |

---

## 2. Estrutura do Bucket

```
hackathon-2025-models/
│
├── pkl/
│   └── modelo_fpd.pkl                    # Modelo LightGBM serializado (pickle)
│
├── resultados_modelo/
│   ├── predicoes_oot_20260305_1721.parquet   # Predições run 1
│   ├── predicoes_oot_20260306_0830.parquet   # Predições run 2
│   └── ...                                   # Histórico de predições
│
└── metricas/
    ├── metricas_20260305_1721.json           # Métricas run 1
    ├── features_20260305_1721.txt            # Features run 1
    ├── metricas_20260306_0830.json           # Métricas run 2
    ├── features_20260306_0830.txt            # Features run 2
    └── ...                                   # Histórico de métricas
```

---

## 3. Pasta `pkl/` — Modelo Serializado

### O que é

O arquivo `modelo_fpd.pkl` contém o modelo LightGBM treinado, serializado via `pickle`. É o artefato central — sem ele, não há scoring.

### Conteúdo técnico

| Campo | Valor |
|-------|-------|
| **Formato** | Python pickle (protocolo padrão) |
| **Objeto** | `lightgbm.Booster` |
| **Tamanho aprox.** | 5-15 MB |
| **Hiperparâmetros** | `num_leaves=31, max_depth=6, lr=0.05, feature_fraction=0.8` |
| **Early stopping** | `stopping_rounds=50`, até 1000 iterações |
| **Melhor iteração** | ~900 (varia por run) |

### Comportamento

- **Primeiro run:** O modelo é treinado do zero e salvo como `pkl/modelo_fpd.pkl`.
- **Runs subsequentes:** O script **tenta carregar** o PKL existente. Se encontrar, pula o treino e vai direto para scoring.
- **Retreinar:** Para forçar um novo treino, basta deletar o PKL do bucket antes de rodar o pipeline.

### Como carregar localmente

```python
import pickle
import lightgbm as lgb

# Via OCI CLI
# Executar no Local/WSL:
# oci os object get -bn hackathon-2025-models --name pkl/modelo_fpd.pkl --file modelo_fpd.pkl

with open("modelo_fpd.pkl", "rb") as f:
    model = pickle.loads(f.read())

# Inspecionar
print(f"Iterações: {model.best_iteration}")
print(f"Features: {model.num_feature()}")
print(f"Feature names: {model.feature_name()[:10]}")  # top 10
```

### Importância das features (extraída do PKL)

```python
import pandas as pd

importance = pd.DataFrame({
    "feature": model.feature_name(),
    "importance": model.feature_importance(importance_type="gain"),
}).sort_values("importance", ascending=False)

print(importance.head(20))

# Salvar como CSV para a apresentação
importance.to_csv("feature_importance.csv", index=False)
```

---

## 4. Pasta `resultados_modelo/` — Predições OOT

### O que é

Cada run gera um arquivo `.parquet` com as predições do modelo sobre os dados **Out-of-Time** (safras fev/mar 2025). É o resultado "prático" do modelo — o que seria enviado para o cliente em produção.

### Schema do parquet

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `num_cpf` | int32 | Identificador do cliente (chave) |
| `safra` | string | Mês da operação (`202502` ou `202503`) |
| `fpd_int` | int32 | Target real (0 = bom, 1 = mau) — só existe no OOT para validação |
| `score_fpd` | float32 | Probabilidade de FPD predita pelo modelo (0.0 a 1.0) |
| `decil` | int32 | Decil de risco (1 = menor risco, 10 = maior risco) |

### Registros esperados

| Safra | Registros aprox. | Descrição |
|-------|------------------|-----------|
| 202502 | ~100K | Fevereiro 2025 (OOT) |
| 202503 | ~105K | Março 2025 (OOT) |
| **Total** | **~205K** | Apenas `flag_instalacao_int=1` |

### Como carregar localmente

```python
import pandas as pd

# Via OCI CLI
# Executar no Local/WSL:
# oci os object get -bn hackathon-2025-models \
#   --name resultados_modelo/predicoes_oot_20260305_1721.parquet \
#   --file predicoes_oot.parquet

df = pd.read_parquet("predicoes_oot.parquet")
print(df.head())
print(f"\nRegistros: {len(df):,}")
print(f"Score médio: {df['score_fpd'].mean():.4f}")
print(f"Taxa FPD real: {df['fpd_int'].mean()*100:.2f}%")
```

---

## 5. Pasta `metricas/` — Métricas e Features

### 5.1 Arquivo `metricas_{ts}.json`

Registro completo de performance de cada run. Exemplo:

```json
{
  "timestamp": "20260305_1721",
  "n_features": 261,
  "iv_threshold": 0.01,
  "sample_fraction": 1.0,
  "n_train": 330056,
  "n_oot": 205462,
  "ks_train": 0.3812,
  "ks_oot": 0.3439,
  "auc_train": 0.7654,
  "auc_oot": 0.7327,
  "gini_train": 0.5308,
  "gini_oot": 0.4654,
  "benchmark": 0.331,
  "gap_benchmark": 0.0129,
  "best_iteration": 900,
  "top_10_features": [
    "score_01",
    "score_02",
    "var_26",
    "..."
  ]
}
```

#### Dicionário de campos

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `timestamp` | string | Data/hora da execução |
| `n_features` | int | Quantidade de features utilizadas (IV >= 0.01) |
| `iv_threshold` | float | Limiar de Information Value usado na seleção |
| `sample_fraction` | float | Fração de amostragem (1.0 = dados completos) |
| `n_train` | int | Registros de treino (safras out/nov/dez 2024) |
| `n_oot` | int | Registros OOT (safras fev/mar 2025) |
| `ks_train` | float | KS no treino (capacidade de separação) |
| `ks_oot` | float | **KS no OOT — métrica principal** |
| `auc_train` | float | AUC no treino |
| `auc_oot` | float | AUC no OOT |
| `gini_train` | float | Coeficiente de Gini no treino (2*AUC - 1) |
| `gini_oot` | float | Coeficiente de Gini no OOT |
| `benchmark` | float | KS benchmark fornecido pela Claro (33.10%) |
| `gap_benchmark` | float | Diferença KS_OOT - benchmark (positivo = acima) |
| `best_iteration` | int | Número de árvores usado (early stopping) |
| `top_10_features` | list | Top 10 features por Information Value |

### 5.2 Arquivo `features_{ts}.txt`

Lista completa das features selecionadas (IV >= 0.01), uma por linha. Exemplo:

```
score_01
score_02
var_26
var_27
freq_sos_m1
pct_sos_sobre_credito_m1
ticket_medio_m3
...
```

**Uso principal:** Documentar exatamente quais variáveis o modelo utilizou naquela execução. Essencial para reprodutibilidade e auditoria.

---

## 6. Como Extrair Insights

### 6.1 Análise de Métricas entre Runs

Compare a evolução do modelo ao longo do tempo carregando múltiplos JSONs de métricas:

```python
import json
import pandas as pd

# Supondo que você baixou vários JSONs de metricas/
# Executar no Local/WSL:
# oci os object list -bn hackathon-2025-models --prefix metricas/metricas_ \
#   --query 'data[*].name' --output table

metricas_files = [
    "metricas_20260305_1721.json",
    "metricas_20260306_0830.json",
    # ... adicionar mais conforme novos runs
]

runs = []
for f in metricas_files:
    with open(f) as fp:
        runs.append(json.load(fp))

df_runs = pd.DataFrame(runs)
print(df_runs[["timestamp", "ks_oot", "auc_oot", "n_features", "gap_benchmark"]])
```

**Perguntas que você responde:**
- O KS está estável entre runs ou tem variação?
- Quantas features são selecionadas em cada run?
- O gap vs benchmark se mantém positivo?
- O `best_iteration` mudou? (indica mudança nos dados)

### 6.2 Análise de Predições (Swap-in/Swap-out)

A análise de swap mede o **impacto prático** do modelo: quantos clientes mudariam de decisão (aprovar/reprovar) ao trocar o modelo antigo pelo novo.

```python
import pandas as pd

df = pd.read_parquet("predicoes_oot.parquet")

# --- Tabela de performance por decil ---
tabela_decil = df.groupby("decil").agg(
    qtd=("num_cpf", "count"),
    taxa_fpd=("fpd_int", "mean"),
    score_min=("score_fpd", "min"),
    score_max=("score_fpd", "max"),
    score_medio=("score_fpd", "mean"),
).round(4)

tabela_decil["taxa_fpd_pct"] = (tabela_decil["taxa_fpd"] * 100).round(2)
tabela_decil["pct_populacao"] = (tabela_decil["qtd"] / len(df) * 100).round(1)
print(tabela_decil)
```

**Insight esperado:** Os decis mais altos (9, 10) devem concentrar a maior taxa de FPD. Se o decil 10 tem ~60-70% de taxa FPD enquanto o decil 1 tem ~5-10%, o modelo tem boa capacidade de discriminação.

### Matriz de confusão por ponto de corte

```python
# Simular diferentes pontos de corte para decisão de crédito
for corte in [0.3, 0.4, 0.5]:
    df["decisao"] = (df["score_fpd"] >= corte).astype(int)  # 1 = reprovar

    tp = ((df["decisao"] == 1) & (df["fpd_int"] == 1)).sum()  # Reprovou e era mau
    fp = ((df["decisao"] == 1) & (df["fpd_int"] == 0)).sum()  # Reprovou e era bom
    tn = ((df["decisao"] == 0) & (df["fpd_int"] == 0)).sum()  # Aprovou e era bom
    fn = ((df["decisao"] == 0) & (df["fpd_int"] == 1)).sum()  # Aprovou e era mau

    reprovados = tp + fp
    aprovados = tn + fn
    taxa_aprovacao = aprovados / len(df) * 100
    taxa_fpd_aprovados = fn / aprovados * 100 if aprovados > 0 else 0

    print(f"\nCorte = {corte:.1f}")
    print(f"  Aprovados: {aprovados:,} ({taxa_aprovacao:.1f}%)")
    print(f"  Reprovados: {reprovados:,} ({100-taxa_aprovacao:.1f}%)")
    print(f"  Taxa FPD entre aprovados: {taxa_fpd_aprovados:.2f}%")
    print(f"  Maus capturados: {tp:,} de {tp+fn:,} ({tp/(tp+fn)*100:.1f}%)")
```

**Pergunta chave:** "Se usarmos o score do modelo com corte em 0.4, qual seria a taxa de inadimplência entre os aprovados?"

### 6.3 Análise de Importância de Features

Combine o PKL (importance) com o arquivo de features (.txt) para entender o que impulsiona o modelo:

```python
import pickle
import pandas as pd

# Carregar modelo
with open("modelo_fpd.pkl", "rb") as f:
    model = pickle.loads(f.read())

# Importância por gain (quanto cada feature contribui para reduzir o erro)
importance = pd.DataFrame({
    "feature": model.feature_name(),
    "gain": model.feature_importance(importance_type="gain"),
    "split": model.feature_importance(importance_type="split"),
})

# Classificar por bloco
def classificar(col):
    col_lower = col.lower()
    if "score_01" in col_lower: return "Score_01"
    elif "score_02" in col_lower: return "Score_02"
    elif col_lower.startswith("var_"): return "Telco"
    elif any(x in col_lower for x in ["recarga", "sos", "bonus", "ticket"]): return "Recarga"
    elif any(x in col_lower for x in ["pagamento", "pag_", "juros", "pago"]): return "Pagamento"
    elif any(x in col_lower for x in ["atraso", "atr_", "aging", "wo"]): return "Atraso"
    else: return "Outros"

importance["bloco"] = importance["feature"].apply(classificar)

# Top 20 features
print("=== TOP 20 FEATURES (gain) ===")
print(importance.sort_values("gain", ascending=False).head(20).to_string(index=False))

# Importância agregada por bloco
print("\n=== IMPORTÂNCIA POR BLOCO ===")
bloco_imp = importance.groupby("bloco").agg(
    n_features=("feature", "count"),
    gain_total=("gain", "sum"),
    gain_medio=("gain", "mean"),
).sort_values("gain_total", ascending=False)
bloco_imp["pct_gain"] = (bloco_imp["gain_total"] / bloco_imp["gain_total"].sum() * 100).round(1)
print(bloco_imp)
```

**Insight para apresentação:** Mostrar que apesar dos Scores de bureau (Score_01, Score_02) terem o maior IV individual, as features comportamentais (Recarga, Pagamento, Atraso) contribuem significativamente em conjunto (+5 p.p. de KS).

### 6.4 Distribuição de Score por Decil

Gerar gráfico de barras para a apresentação:

```python
import matplotlib.pyplot as plt

df = pd.read_parquet("predicoes_oot.parquet")

decil_stats = df.groupby("decil").agg(
    taxa_fpd=("fpd_int", "mean"),
    qtd=("num_cpf", "count"),
).reset_index()

fig, ax1 = plt.subplots(figsize=(10, 6))

# Barras: taxa FPD por decil
bars = ax1.bar(decil_stats["decil"], decil_stats["taxa_fpd"] * 100,
               color="#2196F3", alpha=0.8, label="Taxa FPD (%)")
ax1.set_xlabel("Decil de Risco", fontsize=12)
ax1.set_ylabel("Taxa FPD (%)", fontsize=12, color="#2196F3")
ax1.set_xticks(range(1, 11))

# Linha: quantidade por decil
ax2 = ax1.twinx()
ax2.plot(decil_stats["decil"], decil_stats["qtd"], "r--o", label="Qtd clientes")
ax2.set_ylabel("Quantidade de Clientes", fontsize=12, color="red")

plt.title("Modelo FPD — Taxa de Inadimplência por Decil (OOT)", fontsize=14)
fig.tight_layout()
plt.savefig("grafico_decil_fpd.png", dpi=150)
plt.show()
```

### 6.5 Comparação de Features entre Versões

Compare quais features entraram/saíram entre dois runs:

```python
# Carregar listas de features de dois runs diferentes
with open("features_20260305_1721.txt") as f:
    features_run1 = set(f.read().strip().split("\n"))

with open("features_20260306_0830.txt") as f:
    features_run2 = set(f.read().strip().split("\n"))

novas = features_run2 - features_run1
removidas = features_run1 - features_run2
comuns = features_run1 & features_run2

print(f"Features no Run 1: {len(features_run1)}")
print(f"Features no Run 2: {len(features_run2)}")
print(f"Em comum: {len(comuns)}")
print(f"Novas no Run 2: {len(novas)} → {sorted(novas)[:10]}")
print(f"Removidas no Run 2: {len(removidas)} → {sorted(removidas)[:10]}")
```

**Uso:** Se o número de features mudou significativamente entre runs, investigue o motivo — pode indicar mudança na distribuição dos dados (data drift) ou problema na geração de features.

---

## 7. Tirando o Melhor Proveito dos Artefatos

### 7.1 Para a Apresentação do Hackathon

| Artefato | O que extrair | Slide sugerido |
|----------|---------------|----------------|
| `metricas_.json` | KS OOT = 34.39%, gap = +1.29 p.p. | **Resultado final vs benchmark** |
| `predicoes_oot.parquet` | Tabela de decil com taxa FPD | **Capacidade de discriminação** |
| `modelo_fpd.pkl` | Feature importance por bloco | **Contribuição incremental por bloco de dados** |
| `features_.txt` | 261 features selecionadas de 614 | **Processo de seleção (IV >= 0.01)** |

**Narrativa sugerida para a apresentação:**

1. "Partimos de 614 features e selecionamos 261 com IV >= 0.01"
2. "O modelo atinge KS de 34.39% no OOT, superando o benchmark de 33.10% em +1.29 p.p."
3. "No decil 10, a taxa de FPD é X%, enquanto no decil 1 é Y% — o modelo discrimina bem"
4. "Features comportamentais (recarga, pagamento, atraso) contribuem com +5 p.p. de KS coletivamente"

### 7.2 Para Operação em Produção

| Cenário | Como usar os artefatos |
|---------|----------------------|
| **Scoring mensal** | Pipeline ETL gera nova ABT → modelo carrega PKL → gera predições → salva em `resultados_modelo/` |
| **Monitoramento de performance** | Comparar `ks_oot` entre JSONs de métricas. Se cair > 2 p.p., investigar data drift |
| **Retreino** | Deletar `pkl/modelo_fpd.pkl` → próximo run treina do zero com dados atualizados |
| **Rollback** | Se o novo modelo piora, restaurar PKL anterior de um backup |
| **Decisão de crédito** | Ler `predicoes_oot.parquet`, aplicar ponto de corte definido pelo negócio, gerar lista de aprovados/reprovados |

### 7.3 Para Auditoria e Governança

Os artefatos respondem às perguntas de auditoria:

| Pergunta do auditor | Onde encontrar a resposta |
|---------------------|--------------------------|
| "Qual modelo está em produção?" | `pkl/modelo_fpd.pkl` (data de modificação no Object Storage) |
| "Quais variáveis o modelo usa?" | `metricas/features_{ts}.txt` |
| "Qual a performance atual?" | `metricas/metricas_{ts}.json` → campo `ks_oot` |
| "O modelo usa dados proibidos?" | Verificar `features_{ts}.txt` — não deve conter `fpd_int` nem `flag_instalacao_int` |
| "Quando foi a última execução?" | `timestamp` no JSON de métricas ou data do parquet mais recente em `resultados_modelo/` |
| "Quantos clientes foram avaliados?" | `n_oot` no JSON de métricas |

---

## 8. Comandos Úteis

### Listar artefatos no bucket — Executar no Local/WSL

```bash
# Listar todos os artefatos
oci os object list -bn hackathon-2025-models --query 'data[*].{name:name,size:size}' --output table

# Listar apenas métricas
oci os object list -bn hackathon-2025-models --prefix metricas/ --query 'data[*].name' --output table

# Listar apenas predições
oci os object list -bn hackathon-2025-models --prefix resultados_modelo/ --query 'data[*].{name:name,size:size}' --output table
```

### Baixar artefatos — Executar no Local/WSL

```bash
# Baixar modelo PKL
oci os object get -bn hackathon-2025-models --name pkl/modelo_fpd.pkl --file modelo_fpd.pkl

# Baixar métricas mais recente (substituir timestamp)
oci os object get -bn hackathon-2025-models --name metricas/metricas_20260305_1721.json --file metricas.json

# Baixar predições
oci os object get -bn hackathon-2025-models --name resultados_modelo/predicoes_oot_20260305_1721.parquet --file predicoes_oot.parquet

# Baixar lista de features
oci os object get -bn hackathon-2025-models --name metricas/features_20260305_1721.txt --file features.txt
```

### Inspecionar métricas rapidamente — Executar no Local/WSL

```bash
# Ver métricas direto no terminal (sem baixar arquivo)
oci os object get -bn hackathon-2025-models \
  --name metricas/metricas_20260305_1721.json \
  --file /dev/stdout 2>/dev/null | python3 -m json.tool
```

### Deletar PKL para forçar retreino — Executar no Local/WSL

```bash
# CUIDADO: isso fará o próximo run treinar o modelo do zero
oci os object delete -bn hackathon-2025-models --name pkl/modelo_fpd.pkl --force
```

---

## 9. Boas Práticas

| Prática | Motivo |
|---------|--------|
| **Nunca deletar arquivos de `metricas/`** | São o histórico de auditoria do modelo. Ocupam poucos KB |
| **Manter pelo menos 3 versões em `resultados_modelo/`** | Permite comparação entre runs e rollback de decisões |
| **Verificar `gap_benchmark` a cada run** | Se ficar negativo, o modelo precisa de retreino ou revisão de features |
| **Comparar `n_features` entre runs** | Mudança abrupta indica possível data drift ou problema no pipeline |
| **Documentar o ponto de corte escolhido** | O modelo gera scores contínuos (0-1); a decisão de crédito depende de um corte definido pelo negócio |
| **Backup do PKL antes de retreino** | Copiar `pkl/modelo_fpd.pkl` para `pkl/modelo_fpd_backup_{data}.pkl` antes de deletar |
| **Verificar que `fpd_int` e `flag_instalacao_int` NÃO estão em `features_{ts}.txt`** | Anti-leakage: essas colunas nunca devem ser usadas como features |

---

## 10. Como Testar o Modelo — Entendendo Treino vs OOT

### "Posso colocar um CPF qualquer e ver se o modelo aprova?"

Não diretamente. O modelo **não funciona com um CPF isolado** — ele precisa de **614 features** associadas àquele CPF para gerar um score. Um CPF sozinho é apenas um número de identificação, sem informação preditiva.

Para o modelo calcular o risco de um cliente, esse CPF precisa ter **dados nas bases da Claro**:

| Bloco de dados | Exemplos de features | De onde vem |
|----------------|---------------------|-------------|
| **Bureau** | Score_01, Score_02 | Serasa/Boa Vista (consulta externa) |
| **Telco** | var_26 a var_93 (68 variáveis) | Sistemas internos Claro (uso de dados, voz, SMS) |
| **Cadastro** | idade, região, UF | Base cadastral Claro |
| **Recarga** | freq_sos, ticket_medio, dias entre recargas | Histórico de recargas (pré-pago) |
| **Pagamento** | qtd_pagamentos, juros, desconto | Histórico de faturas |
| **Atraso** | faturas abertas, aging 90+ dias | Inadimplência passada |

Sem esses dados, não há features → não há score.

### Então como testamos o modelo?

Usando CPFs que **já existem na ABT v6** mas que o modelo **nunca viu durante o treino**. Isso é o que chamamos de **OOT (Out-of-Time)** — validação temporal.

#### Split temporal do nosso pipeline

```
Safras na ABT v6 (apenas flag_instalacao=1):

  Out/2024   Nov/2024   Dez/2024   │   Fev/2025   Mar/2025
  ────────────────────────────────  │  ──────────────────────
          TREINO (~330K CPFs)       │     OOT (~205K CPFs)
     Modelo aprende com esses       │  Modelo NUNCA viu esses
                                    │  → são o "teste real"
                                    │
                            Barreira temporal
                         (dados futuros para o modelo)
```

| Conjunto | Safras | CPFs aprox. | Papel |
|----------|--------|-------------|-------|
| **Treino** | 202410, 202411, 202412 | ~330.056 | O modelo **aprende** padrões com esses dados |
| **OOT** | 202502, 202503 | ~205.462 | O modelo é **testado** com esses dados — nunca vistos no treino |

#### O que torna o OOT um teste válido?

1. **São CPFs reais** — clientes que efetivamente migraram para pós-pago em fev/mar 2025
2. **Nunca foram vistos no treino** — o modelo não "decorou" esses clientes
3. **Já têm resultado real** — sabemos se deram default (`fpd_int=1`) ou não (`fpd_int=0`)
4. **São do futuro** — foram gerados **depois** dos dados de treino (split temporal, não aleatório)

O modelo aplica o score sobre esses ~205K CPFs e comparamos com o resultado real. Dessa comparação sai o **KS OOT = 34.39%** — a métrica que valida o modelo.

#### O que o arquivo `predicoes_oot.parquet` contém

São exatamente esses ~205K CPFs com:

| Coluna | Exemplo | Descrição |
|--------|---------|-----------|
| `num_cpf` | 12345678901 | CPF do cliente (real, da base Claro) |
| `safra` | 202502 | Mês em que migrou para pós-pago |
| `fpd_int` | 0 ou 1 | **Resultado real** — deu default ou não |
| `score_fpd` | 0.1842 | **Score do modelo** — probabilidade de FPD (0 a 1) |
| `decil` | 3 | Decil de risco (1=menor risco, 10=maior risco) |

### Como inspecionar CPFs específicos do OOT

```python
import pandas as pd

# Baixar o arquivo primeiro:
# Executar no Local/WSL:
# oci os object get -bn hackathon-2025-models \
#   --name resultados_modelo/predicoes_oot_20260305_1721.parquet \
#   --file predicoes_oot.parquet

df = pd.read_parquet("predicoes_oot.parquet")

# --- Exemplos de clientes de ALTO RISCO (decil 10) ---
print("=== ALTO RISCO (decil 10) ===")
alto_risco = df[df["decil"] == 10].head(10)
print(alto_risco.to_string(index=False))
print(f"Taxa FPD real no decil 10: {df[df['decil']==10]['fpd_int'].mean()*100:.1f}%")

# --- Exemplos de clientes de BAIXO RISCO (decil 1) ---
print("\n=== BAIXO RISCO (decil 1) ===")
baixo_risco = df[df["decil"] == 1].head(10)
print(baixo_risco.to_string(index=False))
print(f"Taxa FPD real no decil 1: {df[df['decil']==1]['fpd_int'].mean()*100:.1f}%")

# --- Consultar um CPF específico ---
cpf = 12345678901  # substituir por um CPF real do OOT
resultado = df[df["num_cpf"] == cpf]
if len(resultado) > 0:
    r = resultado.iloc[0]
    risco = "ALTO" if r["decil"] >= 7 else "MÉDIO" if r["decil"] >= 4 else "BAIXO"
    print(f"\nCPF {cpf}:")
    print(f"  Score: {r['score_fpd']:.4f}")
    print(f"  Decil: {r['decil']} ({risco} RISCO)")
    print(f"  FPD real: {'SIM — deu default' if r['fpd_int']==1 else 'NÃO — pagou em dia'}")
else:
    print(f"\nCPF {cpf} não está no OOT (não migrou em fev/mar 2025)")
```

### Tabela de performance por decil

Para ver a capacidade de discriminação do modelo:

```python
# Tabela completa de performance por decil
tabela = df.groupby("decil").agg(
    qtd_clientes=("num_cpf", "count"),
    qtd_fpd=("fpd_int", "sum"),
    taxa_fpd=("fpd_int", "mean"),
    score_min=("score_fpd", "min"),
    score_max=("score_fpd", "max"),
).round(4)

tabela["taxa_fpd_pct"] = (tabela["taxa_fpd"] * 100).round(2).astype(str) + "%"

print("=== PERFORMANCE POR DECIL (OOT) ===")
print(tabela.to_string())
```

**Resultado esperado:** A taxa de FPD deve crescer progressivamente do decil 1 ao decil 10. Exemplo:

```
Decil 1:  ~5%  FPD  ← Clientes bons (modelo recomenda aprovar)
Decil 5:  ~15% FPD  ← Risco médio
Decil 10: ~60% FPD  ← Clientes maus (modelo recomenda recusar)
```

Se o decil 10 concentra a maioria dos defaults, o modelo está discriminando bem — é isso que o KS de 34.39% reflete.

### Por que split temporal e não aleatório?

| Tipo de split | O que faz | Problema |
|---------------|-----------|----------|
| **Aleatório** | Mistura CPFs de todas as safras no treino e teste | Modelo pode capturar padrões sazonais "de graça" — infla o KS |
| **Temporal (nosso)** | Treina com passado, testa com futuro | Simula produção real — o modelo só vê dados do passado |

Em produção, quando um novo cliente pedir migração para pós-pago em **abril 2025**, o modelo terá sido treinado com dados de **out-dez 2024**. O split temporal simula exatamente esse cenário. Por isso o KS OOT é a métrica que realmente importa para a apresentação — ele mede a performance em condições reais.

### Resumo

| Pergunta | Resposta |
|----------|----------|
| "Posso testar com meu CPF?" | Não — seu CPF não tem as 614 features nas bases da Claro |
| "Então como testo o modelo?" | Com os ~205K CPFs do OOT que já existem na ABT v6 |
| "Onde vejo os resultados?" | `resultados_modelo/predicoes_oot_{ts}.parquet` |
| "O modelo 'decorou' esses CPFs?" | Não — são de safras futuras, nunca vistos no treino |
| "O que significa decil 10?" | Maior risco de FPD — ~60% desses clientes deram default |
| "Em produção, como funciona?" | Pipeline ETL monta features do CPF → modelo aplica score → regra de negócio decide aprovação |
