# Preparação da Equipe - Visão de Negócios

Este diretório contém documentação para preparar a equipe nas apresentações do Hackathon PodAcademy 2025, com foco nos **aspectos de negócio** do projeto de modelagem de risco de crédito para a Claro Telecom.

## Objetivo

Garantir que todos os membros da equipe (de júnior a sênior) compreendam:
- **Por que** tomamos cada decisão
- **Quais alternativas** existiam e por que não foram escolhidas
- **Qual o benefício** de cada escolha para o negócio
- **O que falta fazer** até a entrega final

## Ordem de Leitura Recomendada

| # | Documento | Tempo | Público |
|---|-----------|-------|---------|
| 1 | [01_CONTEXTO_PROJETO.md](01_CONTEXTO_PROJETO.md) | 10 min | Todos |
| 2 | [02_DECISOES_ESTRATEGICAS.md](02_DECISOES_ESTRATEGICAS.md) | 15 min | Todos |
| 3 | [03_REGRAS_NEGOCIO.md](03_REGRAS_NEGOCIO.md) | 10 min | Todos |
| 4 | [04_EVOLUCAO_ABT.md](04_EVOLUCAO_ABT.md) | 15 min | Data Scientists |
| 5 | [05_METRICAS_SUCESSO.md](05_METRICAS_SUCESSO.md) | 10 min | Todos |
| 6 | [06_ROADMAP_FUTURO.md](06_ROADMAP_FUTURO.md) | 10 min | Todos |

**Tempo total estimado:** 1h de leitura focada

## Resumo Executivo (2 minutos)

**Projeto:** Modelo preditivo de First Payment Default (FPD) para decisões de elegibilidade de clientes Claro Telecom.

**Meta:** Superar o benchmark de KS = 33.1 no conjunto OOT (Fev/Mar 2024).

**Status Atual:** Engenharia de dados COMPLETA (614 features, 3.79M registros). Fase de modelagem iniciando.

**Próximos Passos:** Modelagem incremental mostrando ganho de KS por bloco de dados (Score → Telco → Cadastro → Recarga → Pagamento/Atraso).

## Perguntas Frequentes para a Apresentação

### "Por que usar FPD como target?"
FPD é um indicador antecedente: se o cliente não paga a primeira fatura, há alta probabilidade de inadimplência futura. É acionável porque podemos prever antes mesmo do relacionamento começar.

### "Por que não treinar com clientes reprovados?"
Clientes reprovados nunca contrataram, então não sabemos se pagariam ou não. Usar "reprovado = bom" seria um viés grave. Treinamos apenas onde observamos o outcome real.

### "Qual a diferença deste modelo para a política atual?"
A política atual usa regras fixas (scores de corte). Nosso modelo usa comportamento passado (recarga, pagamento, atraso) para decisões mais granulares, permitindo aprovar bons clientes rejeitados e rejeitar maus clientes aprovados.

### "Por que mostrar KS incremental por fonte?"
Para justificar o ROI de cada fonte de dados. Se Recarga adiciona +2 KS mas custa R$50k/ano para integrar, vale a pena. Se adiciona +0.1 KS, talvez não.

---

**Última atualização:** 30/Jan/2026
**Responsável:** Equipe de Data Engineering
