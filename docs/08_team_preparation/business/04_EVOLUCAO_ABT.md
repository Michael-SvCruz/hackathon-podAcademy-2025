# Evolução da ABT (Analytical Base Table)

Este documento explica **por que** cada versão da ABT foi criada e **qual valor de negócio** cada uma adiciona.

## Visão Geral das Versões

```
ABT v1 ──► ABT v2 ──► ABT v3 ──► ABT v4 ──► ABT v5 ──► ABT v6
   │          │          │          │          │          │
Score_01   +Score_02  +Telco    +Cadastro  +Recarga   +Pagamento
                                                      +Atraso

   10 cols    15 cols    ~100 cols  ~185 cols  ~311 cols  ~614 cols
```

## ABT v1: Baseline Bureau

### O Que Contém
- `score_01`: Score de crédito tradicional (bureau)
- Variáveis de identificação (num_cpf, safra)
- Target (fpd_int) e labels (flag_instalacao_int)

### Por Que Existe
Estabelece o **baseline mínimo**. Se não conseguirmos bater o KS do Score_01 sozinho, qualquer adição de features é inútil.

### Valor de Negócio
- Representa a política atual (decisão baseada em score de corte)
- Benchmark: KS base para comparação

### Limitações
- Bureau não vê comportamento telecom
- Score genérico, não específico para o problema

---

## ABT v2: Score Aprimorado

### O Que Adiciona
- `score_02`: Score de crédito aprimorado/alternativo

### Por Que Foi Adicionado
Testar se um segundo score de bureau adiciona informação incremental.

### Valor de Negócio
- Se Score_02 melhora KS significativamente, justifica custo de contratação
- Se não melhora, economiza dinheiro não contratando

### Pergunta de Negócio que Responde
> "Vale a pena pagar por mais de um score de bureau?"

---

## ABT v3: Dados de Uso Telco

### O Que Adiciona
- 68 variáveis anônimas de uso de telefonia (`var_26` a `var_93`)
- Exemplos: minutos de uso, dados consumidos, padrões de ligação

### Por Que Foi Adicionado
Hipótese: comportamento de uso de telefone revela perfil financeiro.

### Valor de Negócio
- Dados internos da Claro (custo zero de aquisição)
- Se preditivo, vantagem competitiva exclusiva
- Concorrentes não têm acesso a esses dados

### Insight de Negócio
```
Cliente com alto consumo de dados + baixo voice = digital native
Cliente com muito voice + pouco dados = perfil tradicional

Perfis diferentes podem ter riscos diferentes.
```

### Limitações
- Variáveis anônimas (var_XX) dificultam interpretação
- Cobertura de ~35% (nem todos têm histórico Telco)

---

## ABT v4: Dados Demográficos (Cadastro)

### O Que Adiciona
- 33 variáveis demográficas
- Idade, região (CEP), estado (UF)
- Características do cadastro

### Por Que Foi Adicionado
Hipótese: perfil demográfico correlaciona com risco.

### Valor de Negócio
- Segmentação de clientes (jovem vs. maduro, urbano vs. rural)
- Políticas diferenciadas por região
- Entendimento do público-alvo

### Problema Encontrado e Resolvido
```
ANTES (Dez/2025):
- idade_anos = 0% cobertura (tudo NULL)
- Causa: Python UDF falhando silenciosamente

DEPOIS (Jan/2026):
- idade_anos = 99.57% cobertura
- Solução: trocar UDF por F.to_date() nativo
```

**Lição aprendida:** Nunca usar Python UDFs em Databricks. Sempre preferir funções nativas do Spark.

---

## ABT v5: Comportamento de Recarga

### O Que Adiciona
- 60+ features de comportamento de recarga
- Janelas M1, M3, M6 (1, 3, 6 meses)

### Features Principais

| Feature | O Que Mede | Por Que Importa |
|---------|------------|-----------------|
| `freq_sos_m1` | Frequência de SOS (empréstimo) | Stress financeiro |
| `pct_sos_sobre_credito` | % do crédito vindo de SOS | Dependência de empréstimo |
| `ticket_medio_m1` | Valor médio de recarga | Capacidade de pagamento |
| `coef_variacao_val` | Variabilidade dos valores | Instabilidade financeira |
| `dias_max_entre_recargas` | Maior gap sem recarregar | Inatividade/dificuldade |
| `pct_recargas_madrugada` | % de recargas de madrugada | Padrão comportamental |

### Por Que Foi Adicionado
**Insight chave do Fernando (Claro):**
> "SOS é indicador de stress financeiro. Cliente que usa muito SOS está constantemente sem dinheiro."

### Valor de Negócio
- Captura comportamento **antes** da contratação
- SOS é preditor forte de inadimplência
- Dados exclusivos da Claro (vantagem competitiva)

### Pipeline de Dados
```
Silver Recarga (95M eventos)
        │
        ▼
Gold Recarga Features (32.9M cliente-mês)
        │
        ▼
ABT v5 (3.79M registros, 311 colunas)
```

---

## ABT v6: Histórico de Pagamento e Atraso

### O Que Adiciona

**Pagamento (50+ features):**
- Histórico de faturas pagas
- Comportamento de negociação (descontos)
- Juros pagos (indicador de atrasos passados)

**Atraso (60+ features):**
- Faturas em aberto
- Aging (0-30, 30-60, 60-90, 90+ dias)
- Flags de risco (write-off, fraude, PDD)

### Features Principais

| Feature | O Que Mede | Por Que Importa |
|---------|------------|-----------------|
| `pct_pagamentos_com_juros_m1` | % faturas com juros | Atrasos passados |
| `ratio_desconto_pago` | Desconto/Valor pago | Perfil negociador |
| `pct_aging_90_plus_m1` | % dívida >90 dias | Severidade da inadimplência |
| `sum_val_aberto_m1` | Total em aberto | Exposição atual |
| `flag_teve_wo` | Teve write-off? | Inadimplência grave anterior |
| `flag_teve_fraude` | Teve fraude? | Risco de fraude |

### Por Que Foi Adicionado
Hipótese: comportamento de pagamento passado é o melhor preditor de pagamento futuro.

### Valor de Negócio
- **Pagamento:** Cliente que sempre paga com juros = sempre atrasa
- **Atraso:** Cliente com 90+ dias em aberto = alto risco
- **Flags de risco:** Write-off ou fraude anterior = red flag

### Cobertura
| Bloco | Cobertura M1 | Observação |
|-------|--------------|------------|
| Pagamento | 16.13% | Só quem tem histórico de pagamento |
| Atraso | 21.79% | Só quem tem/teve faturas em aberto |

**Nota:** Cobertura baixa não significa features ruins. Significa que são disponíveis para um subconjunto de clientes com relacionamento prévio.

---

## Resumo da Evolução

| Versão | Colunas | Adição | Hipótese de Negócio |
|--------|---------|--------|---------------------|
| v1 | 10 | Score_01 | Bureau tradicional é baseline |
| v2 | 15 | Score_02 | Segundo score agrega valor? |
| v3 | ~100 | Telco (68 vars) | Uso de telefone revela risco? |
| v4 | ~185 | Cadastro (33 vars) | Demografia correlaciona com risco? |
| v5 | ~311 | Recarga (60+ vars) | SOS indica stress financeiro? |
| v6 | ~614 | Pagamento + Atraso | Comportamento passado prevê futuro? |

---

## Ganho Esperado de KS

Com base em experiência de mercado e hipóteses de negócio:

| Transição | Ganho Esperado | Confiança |
|-----------|----------------|-----------|
| v1 → v2 | +0.5 a +1.5 | Média |
| v2 → v3 | +0.5 a +2.0 | Baixa (variáveis anônimas) |
| v3 → v4 | +0.3 a +1.0 | Média |
| v4 → v5 | +1.0 a +2.5 | **Alta (SOS é forte)** |
| v5 → v6 | +1.5 a +3.0 | **Alta (comportamento passado)** |

**Meta:** KS final > 33.1 (benchmark)

---

## Próximos Passos

1. **Modelagem incremental:** Medir KS real por versão
2. **Feature selection:** Reduzir 614 → 50-100 features mais importantes
3. **Interpretação:** SHAP analysis para explicar decisões
4. **Documentação:** Atualizar este documento com resultados reais
