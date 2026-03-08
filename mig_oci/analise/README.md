# Análise e Gráficos para Apresentação

Pasta com scripts de geração de gráficos e dados para a apresentação final do Hackathon PodAcademy 2025.

## Estrutura

```
analise/
├── README.md                          # Este arquivo
├── ANALISE_GRAFICOS.md                # Documentação detalhada (dados, metodologia, interpretação)
├── ks_incremental_charts.py           # Gráficos 01-05: KS incremental + modelo
├── comparacao_databricks_oci.py       # Gráficos 06-10: Databricks vs OCI
└── output/                            # Artefatos gerados
    ├── 01_ks_incremental_lgbm.png     # KS por bloco (barras + linha)
    ├── 02_logistica_vs_lgbm.png       # Logística vs LightGBM
    ├── 03_waterfall_contribuicao.png  # Contribuição por bloco (waterfall)
    ├── 04_resumo_final_benchmark.png  # Resultado final vs benchmark
    ├── 05_features_por_bloco.png      # Features por bloco (donut)
    ├── 06_ks_databricks_vs_oci.png    # KS lado a lado (Databricks vs OCI)
    ├── 07_metricas_comparacao.png     # Métricas: KS, AUC, GINI, Features
    ├── 08_custos_databricks_vs_oci.png# Custos mensais (stacked bars)
    ├── 09_ganhos_migracao.png         # 4 cards: Performance, Custo, IaC, Airflow
    ├── 10_timeline_migracao.png       # Timeline 8 fases (~30 dias)
    ├── dados_apresentacao.json        # Dados KS incremental (JSON)
    ├── dados_apresentacao.txt         # Dados KS incremental (TXT formatado)
    ├── dados_comparacao_databricks_oci.json  # Dados comparação (JSON)
    └── dados_comparacao_databricks_oci.txt   # Dados comparação (TXT formatado)
```

## Como Executar

```bash
# Local (WSL/Linux com Python 3 + matplotlib)
cd mig_oci/analise

# Gerar gráficos 01-05 (KS incremental + modelo)
python3 ks_incremental_charts.py

# Gerar gráficos 06-10 (comparação Databricks vs OCI)
python3 comparacao_databricks_oci.py
```

**Dependências:** `matplotlib`, `numpy` (padrão em ambientes científicos)

```bash
pip install matplotlib numpy
```

## Fonte dos Dados

| Dado | Fonte | Tipo |
|------|-------|------|
| KS Incremental OCI VM | `metricas/ks_incremental_20260307_1850.json` | Exato |
| KS Databricks | Notebook `20260202 - Modelo Final FPD.ipynb` | Score_01 e Final exatos; intermediários aproximados |
| Logistic Regression | `docs/08_team_preparation/technical/modeling/` | Exato |
| Benchmark Claro | Coordenação (2026-01-08) | Definido |
| Custos OCI | 30 dias de operação real | Real |
| Custos Databricks | Estimativa para uso equivalente | Estimado |

## Resultado Principal

| Ambiente | KS OOT | vs Benchmark | Custo/mês |
|----------|--------|--------------|-----------|
| Databricks | 33.94% | +0.84 p.p. | ~$1,450 |
| **OCI VM** | **34.39%** | **+1.29 p.p.** | **~$350** |

**Ganho da migração:** +0.45 p.p. no KS com -76% de custo.
