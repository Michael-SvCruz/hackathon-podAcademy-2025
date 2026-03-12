# Resumo Arquitetura — Texto para Slide

> Usar como speaker notes ou caixa de texto ao lado do diagrama de arquitetura.

---

## Arquitetura de Dados — Oracle Cloud Infrastructure

A solucao foi implementada integralmente na OCI, seguindo a **Arquitetura Medallion** (Landing → Bronze → Silver → Gold) com infraestrutura gerenciada via **Terraform** (~58 recursos provisionados como codigo).

**Ingestao e Processamento:**
6 fontes de dados são processadas por **21 aplicacoes Spark** no OCI Data Flow, organizadas em 4 grupos paralelos — cada grupo usa familias de shapes diferentes para evitar competicao de quota do **free tier**. O pipeline completo rodou inicialmente em ~2h, mas após ajustes dos shapes e configurações spark, **o pipeline agora roda em ~1h20**.

**Armazenamento:**
7 buckets no Object Storage segmentados por camada (landing, bronze, silver, gold, models, pipeline-ops, tfstate). Formato Delta Lake com particionamento por safra.

**Orquestracao:**
Airflow 2.8 em Docker Compose numa VM Start/Stop. A DAG principal encadeia os 21 jobs Spark e dispara automaticamente o scoring do modelo via `TriggerDagRunOperator`.

**Modelo de Scoring:**
VM dedicada (E5.Flex, 2 OCPUs, 32 GB) na subnet privada — ligada sob demanda pelo Airflow, executa o LightGBM (pandas), salva predicoes no Object Storage e desliga. KS OOT = 34.39%.

**Seguranca:**
Rede isolada (VCN com 3 subnets: publica, privada-app, privada-dados). Autenticacao via Instance Principal (Dynamic Groups) — sem chaves estaticas. Compartments segmentam IAM por papel.

**Custo operacional: ~R$105/mes** 

---

## Numeros-Chave (para destacar no slide)

| Metrica | Valor |
|---------|-------|
| Recursos OCI | ~58 (todos via Terraform) |
| Aplicacoes Spark | 21 (Data Flow) |
| Fontes de dados | 6 |
| Buckets | 7 |
| Pipeline completo | ~1h20 |
| Volume armazenado | ~40 GB |
| Custo mensal | ~R$105 |
| KS OOT | 34.39% (+1.29 p.p.) |

---

## Pontos Para Enfatizar na Fala

1. **Infraestrutura como Codigo** — 100% Terraform, reproducivel, versionado
2. **Paralelismo inteligente** — 6 familias de shapes diferentes para rodar 6 jobs simultaneos sem conflito de quota
3. **Start/Stop** — VMs ligam apenas quando necessario, custo zero quando paradas
4. **Seguranca** — Instance Principal (sem senhas), subnets privadas, IAM por compartment
5. **Custo** — $105
