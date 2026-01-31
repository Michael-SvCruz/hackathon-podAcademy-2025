# Contexto do Projeto

## O Problema de Negócio

A Claro Telecom enfrenta um desafio crítico: quando um cliente migra de operadora ou contrata uma nova linha, a empresa investe recursos significativos (habilitação, equipamentos, infraestrutura). Se o cliente não paga a primeira fatura (**First Payment Default - FPD**), a empresa absorve o prejuízo.

### Situação Atual

- **Decisão baseada em política:** Regras fixas usando scores de bureau
- **Problema:** Clientes bons são rejeitados desnecessariamente (perda de receita) e clientes ruins são aprovados (perda por inadimplência)
- **Oportunidade:** Usar dados comportamentais para decisões mais inteligentes

### O Que Queremos Resolver

| Situação | Problema | Solução |
|----------|----------|---------|
| Bom cliente rejeitado | Perda de receita potencial | **Swap-in:** Aprovar com modelo |
| Mau cliente aprovado | Prejuízo por inadimplência | **Swap-out:** Rejeitar com modelo |

## Por Que Este Projeto?

### Contexto do Hackathon

O Hackathon PodAcademy 2025 é uma competição entre equipes de cientistas de dados para resolver problemas reais de negócio. A Claro forneceu dados anonimizados e a Power of Data coordena o evento.

### Stakeholders Principais

| Stakeholder | Papel | Interesse Principal |
|-------------|-------|---------------------|
| **Claro (Fernando Parahyba)** | Cliente/Patrocinador | Modelo que reduza inadimplência |
| **Power of Data (Allan Basilio)** | Coordenador | Sucesso do hackathon |
| **Equipe PodAcademy** | Desenvolvedores | Entrega de qualidade |

### O Que Está em Jogo

- **Para a Claro:** Modelo pode ser implementado em produção
- **Para a equipe:** Certificação, visibilidade, experiência prática
- **Benchmark a bater:** KS = 33.1 (discriminação atual da política)

## Linha do Tempo do Projeto

```
Dez/2025          Jan/2026              Fev/2026           Mar/2026
    │                 │                    │                  │
    ▼                 ▼                    ▼                  ▼
 KICKOFF          DATA ENG            MODELING           DEFESA
 Apresentação     Bronze→Gold         Train/Test/OOT     Qualificação
 inicial          ABT v1-v6           Feature Selection  + Final Oracle
                  COMPLETO ✓          KS Incremental
```

### Marcos Alcançados

| Data | Marco | Status |
|------|-------|--------|
| Dez/2025 | Kickoff e apresentação inicial | ✓ Completo |
| 07/Jan/2026 | Reunião de dúvidas técnicas com Claro | ✓ Completo |
| 08/Jan/2026 | Mensagem de coordenação com diretrizes | ✓ Completo |
| 15/Jan/2026 | Checkpoint de progresso | ✓ Completo |
| 26/Jan/2026 | Fix do Cadastro (UDF → F.to_date) | ✓ Completo |
| 30/Jan/2026 | ABT v6 final com 614 colunas | ✓ Completo |
| Fev/2026 | Modelagem e análise incremental | Em andamento |
| Fev/2026 | Defesa de qualificação (Databricks) | Próximo |
| Mar/2026 | Defesa final (Oracle Cloud) | Planejado |

## Arquitetura de Dados (Visão de Negócio)

### Medalha de Qualidade

Usamos a **Arquitetura Medallion** (Bronze → Silver → Gold), padrão de mercado para data lakes:

```
LANDING           BRONZE              SILVER              GOLD
(Dados brutos)    (+ metadados)       (tipado, validado)  (ABT para modelo)
    │                 │                   │                   │
    │                 │                   │                   │
Parquet          + data_ingestao      + tipo correto      + features
do cliente       + nome_arquivo       + deduplicação      + agregações
                 + hash_linha         + validações        + janelas M1/M3/M6
```

### Por Que Esta Arquitetura?

| Camada | Benefício de Negócio |
|--------|---------------------|
| **Bronze** | Rastreabilidade: sabemos de onde veio cada dado |
| **Silver** | Qualidade: dados limpos e consistentes |
| **Gold** | Valor: features prontas para modelagem |

### Fontes de Dados

| Fonte | O Que Contém | Por Que Importa |
|-------|--------------|-----------------|
| **Bureau (Score_01, Score_02)** | Scores de crédito tradicionais | Baseline de risco |
| **Telco (68 variáveis)** | Uso de telefonia | Comportamento digital |
| **Cadastro** | Dados demográficos | Perfil do cliente |
| **Recarga** | Histórico de recargas | Stress financeiro (SOS) |
| **Pagamento** | Faturas pagas | Comportamento de pagamento |
| **Atraso** | Faturas em aberto | Situação atual de dívida |

## O Target: First Payment Default (FPD)

### Definição

**FPD = 1** quando o cliente foi aprovado (FLAG_INSTALACAO=1) e NÃO pagou a primeira fatura.

### Por Que FPD e Não Outra Métrica?

| Alternativa | Por Que Não Usar |
|-------------|------------------|
| Inadimplência geral | Muito longa para observar (12+ meses) |
| Score de bureau | Não específico para telecom |
| Churn | Não é risco de crédito |

**FPD é ideal porque:**
1. É observado rapidamente (30-60 dias)
2. É altamente preditivo de comportamento futuro
3. É acionável no momento da decisão

### Quem Pode Ter FPD?

```
                    ┌─── FLAG=1 (Aprovado + Contratou)
                    │       │
                    │       ├─── FPD=0 (Pagou 1ª fatura)
UNIVERSO ───────────┤       │
                    │       └─── FPD=1 (Não pagou 1ª fatura)
                    │
                    └─── FLAG=0 (Rejeitado OU não contratou)
                            │
                            └─── FPD = ? (Não observável!)
```

**Regra de Ouro:** Só treinamos onde FPD é observado (FLAG=1).

## Próximos Capítulos

- [02_DECISOES_ESTRATEGICAS.md](02_DECISOES_ESTRATEGICAS.md) - Por que cada decisão foi tomada
- [03_REGRAS_NEGOCIO.md](03_REGRAS_NEGOCIO.md) - Regras críticas de anti-leakage
