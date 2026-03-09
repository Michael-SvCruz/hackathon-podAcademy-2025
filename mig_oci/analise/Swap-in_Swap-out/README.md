# Análise Swap-in / Swap-out

Análise do impacto prático de substituir o modelo atual (Score_01 = bureau score) pelo novo modelo LightGBM na mesma população OOT.

## Conceito

```
                          MODELO NOVO (LightGBM)
                       Aprova         Rejeita
                   ┌──────────────┬──────────────┐
MODELO      Aprova │  A — Acordo  │ B — Swap-out │
ANTIGO             │ (sem mudança)│ (rejeitar     │
(Score_01)         │              │  maus pagadores)
                   ├──────────────┼──────────────┤
            Rejeita│ C — Swap-in  │  D — Acordo  │
                   │ (recuperar   │ (sem mudança) │
                   │  bons clientes)              │
                   └──────────────┴──────────────┘
```

- **Swap-out (B):** Clientes que o modelo antigo APROVARIA mas o novo REJEITA → captura maus pagadores
- **Swap-in (C):** Clientes que o modelo antigo REJEITARIA mas o novo APROVA → recupera bons clientes

## Estrutura

```
Swap-in_Swap-out/
├── README.md                  # Este arquivo
├── ANALISE_SWAP.md            # Documentação detalhada dos 5 gráficos
├── swap_analysis_oci.py       # Script de análise (executa na VM OCI)
├── swap_charts.py             # Geração de gráficos (executa LOCAL, dados embutidos)
└── output/
    ├── swap_analysis.json     # Resultados brutos (copiar da VM)
    ├── 11_decil_comparativo.png
    ├── 12_matriz_swap.png
    ├── 13_fpd_por_taxa.png
    ├── 14_swap_volume.png
    ├── 15_resumo_ganho.png
    ├── dados_swap_analysis.json
    └── dados_swap_analysis.txt
```

## Como Executar

### Passo 1: Rodar análise na VM OCI

```bash
# Deploy do script para VM Modelo (executar LOCAL)
scp -i ~/.ssh/airflow_vm -o ProxyJump=opc@<AIRFLOW_IP> \
  mig_oci/analise/Swap-in_Swap-out/swap_analysis_oci.py \
  opc@<MODELO_IP>:/opt/modelo-fpd/swap_analysis_oci.py

# Executar na VM Modelo (via jump host)
ssh -i ~/.ssh/airflow_vm opc@<AIRFLOW_IP> \
  "ssh -i /opt/airflow-fpd/config/modelo_vm_key opc@<MODELO_IP> \
   'nohup python3.11 -u /opt/modelo-fpd/swap_analysis_oci.py > /tmp/swap_analysis.log 2>&1 &'"

# Monitorar (via jump host)
ssh -i ~/.ssh/airflow_vm opc@<AIRFLOW_IP> \
  "ssh -i /opt/airflow-fpd/config/modelo_vm_key opc@<MODELO_IP> \
   'tail -20 /tmp/swap_analysis.log'"
```

### Passo 2: Copiar resultados JSON

```bash
# Copiar JSON da VM para LOCAL (via jump host)
scp -i ~/.ssh/airflow_vm -o ProxyJump=opc@<AIRFLOW_IP> \
  opc@<MODELO_IP>:/tmp/swap_analysis.log /tmp/swap_output.log

# Extrair JSON do log (entre os marcadores)
sed -n '/\[JSON_RESULT_START\]/,/\[JSON_RESULT_END\]/p' /tmp/swap_output.log \
  | grep -v JSON_RESULT > mig_oci/analise/Swap-in_Swap-out/output/swap_analysis.json
```

Ou baixar direto do bucket OCI:
```bash
oci os object get \
  --namespace <NAMESPACE> \
  --bucket-name hackathon-2025-models \
  --name metricas/swap_analysis_<TIMESTAMP>.json \
  --file mig_oci/analise/Swap-in_Swap-out/output/swap_analysis.json
```

### Passo 3: Gerar gráficos (LOCAL)

```bash
cd mig_oci/analise/Swap-in_Swap-out
python3 swap_charts.py
```

## Gráficos Gerados

| # | Arquivo | O que mostra |
|---|---------|-------------|
| 11 | `11_decil_comparativo.png` | Taxa FPD por decil: Score_01 vs LightGBM (barras agrupadas) |
| 12 | `12_matriz_swap.png` | Matriz de swap 2×2 com volumes e taxa FPD por célula (80%) |
| 13 | `13_fpd_por_taxa.png` | FPD entre aprovados para cada taxa de aprovação (70-90%) |
| 14 | `14_swap_volume.png` | Volume de clientes em swap-in e swap-out por taxa |
| 15 | `15_resumo_ganho.png` | Redução relativa de FPD (barras horizontais com antes→depois) |

**Documentação detalhada:** [`ANALISE_SWAP.md`](ANALISE_SWAP.md) — interpretação, metodologia e sugestão de ordem para apresentação.

## Resultados Principais

| Métrica | Score_01 | LightGBM | Ganho |
|---------|---------|----------|-------|
| KS OOT | 26.71% | 34.42% | +7.71 p.p. |
| Separação D1/D10 | 5.5x | 10.3x | ~2x melhor |
| FPD aprovados (80%) | 17.12% | 15.26% | -10.8% |
| Swap-out FPD (80%) | — | 41.8% | Captura maus |
| Swap-in FPD (80%) | — | 23.6% | Recupera bons |

## Dados Necessários

| Dado | Bucket | Coluna | Descrição |
|------|--------|--------|-----------|
| Score_01 | gold-layer/abt_v6_v2/ | `score_01_adj` | Bureau score (2-771), proxy do modelo atual |
| score_fpd | models/resultados_modelo/ | `score_fpd` | Probabilidade FPD do LightGBM (0.0-1.0) |
| fpd_int | ambos | `fpd_int` | Target real (0=bom, 1=mau) |

**Nota:** A coluna é `score_01_adj` (com sufixo `_adj` do tratamento de sentinelas no ABT v6).

**Lógica dos scores:**
- **Score_01:** Score ALTO = cliente BOM (aprovamos os de score alto)
- **score_fpd:** Score ALTO = cliente MAU (aprovamos os de score baixo)
