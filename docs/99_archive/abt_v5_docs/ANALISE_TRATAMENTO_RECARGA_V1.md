# Análise do Processamento de Recarga v1
**Data**: 28 de janeiro de 2026  
**Script**: `src/jobs/02_gold/tratamento_recarga_v1.py`  
**Volume**: 100.2M registros transacionais → 32.8M linhas agregadas

---

## 📊 Resumo Executivo

O processamento de recarga foi executado com sucesso, gerando uma visão mensal consolidada com 25 features. O script apresenta **excelente qualidade técnica** mas identifica **3 anomalias críticas** que precisam ser endereçadas antes da integração ao ABT_v5.

---

## ✅ Pontos Positivos

### 1. Volume e Cobertura
- **100.2 milhões** de transações processadas
- **32.8 milhões** de linhas agregadas (1:3 ratio, aceitável)
- **30 colunas** enriquecidas com sucesso

### 2. Taxa de Enriquecimento (Joins)
| Dimensão | Taxa | Status |
|----------|------|--------|
| Forma Pagamento | 100.00% | ✅ Perfeito |
| Status Plataforma | 100.00% | ✅ Perfeito |
| Tipo Crédito | 100.00% | ✅ Perfeito |
| Tipo Inserção | 96.74% | ✅ Excelente |
| Tipo Recarga | 97.85% | ✅ Excelente |
| **Instituição** | **28.66%** | ⚠️ **CRÍTICO** |

### 3. Features Técnicas
✅ Timestamp completo com parsing correto (HHmmss)  
✅ Flags de classificação (FLAG_TIPO_VALOR) bem definidas  
✅ Métricas temporais (dias sem recarga) calculadas corretamente via Window Function  
✅ Agregação mensal sem perda de informação  
✅ Validações inline com relatórios informativos  

---

## 🚨 Alertas Críticos

### 1. ⚠️ INSTITUIÇÃO: 71.34% SEM MATCH

**Evidência**:
```
Arquivo: BI_DIM_INSTITUICAO
Join em: 'DW_INSTITUICAO'
Match rate: 28.66% (28,718,457/100,213,651)
⚠️ Atenção: 71.34% dos registros não tiveram match!
```

**Análise**:
- 71.3 milhões de registros ficaram sem `DSC_TIPO_INSTITUICAO`
- Causa provável: Valores NULL ou inválidos em `DW_INSTITUICAO` da base principal
- Left join deixou NULLs nesses registros

**Impacto no ABT_v5**:
- Feature faltando para 71% dos dados
- Impossível usar como variável preditora sem tratamento

**Ações Recomendadas**:
```markdown
1. Investigar NULL em DW_INSTITUICAO:
   - Quantos valores NULL?
   - Qual é a distribuição temporal?
   - Afeta todos os CPFs ou específicos?

2. Criar FLAG_INSTITUICAO_MISSING=1 para preservar sinal

3. Validar se dimensão está incompleta ou dados estão mal formatados

4. Adicionar Gate 11 (novo para v5):
   - FLAG_INSTITUICAO_MISSING não deve exceder 50%
```

---

### 2. 🔴 ANOMALIA NO AJUSTE DE BÔNUS: RESULTADO NEGATIVO

**Evidência**:
```
IMPACTO DO AJUSTE DE BÔNUS
╔═════════════════════════════════════════════════════════════════╗
║ total_antes_ajuste_bonus    │ 1.010806350733111E12   (R$ 1.01T) ║
║ total_apos_ajuste_bonus     │ -1.9902252047821074E9  (R$ -1.99B)║
║ total_bonus_retirado        │ 1.0100911078590281E12  (R$ 1.01T) ║
╚═════════════════════════════════════════════════════════════════╝
```

**O Problema**:
```
VAL_REAL_AJUSTADO_FINAL = VAL_REAL_AJUSTADO - VAL_BONUS
                        = 1.010T - 1.010T = -1.99B ❌ NEGATIVO
```

**Por Que Isso Acontece?**

A lógica atual remove o bônus PARA TODOS os tipos:
```python
F.when(
    F.col('FLAG_TIPO_VALOR').isin(['COMBO_PAGO_BONUS', 'BONUS_PURO']),
    F.col('VAL_REAL_AJUSTADO') - F.col('VAL_BONUS')  # Subtrai mesmo se maior
).otherwise(...)
```

**Caso Concreto - Top Cliente**:
```
CPF: 9NN87WYZT7Y (2024-03)
┌─────────────────────────────────────┐
│ VAL_CREDITO_MES:          50,040.00 │
│ VAL_BONUS_MES:            34,300.39 │
│ VAL_REAL_AJUSTADO:    ~50,040 + adj │
│ VAL_REAL_AJUSTADO_FINAL:      36.00 │  ← Praticamente zero!
└─────────────────────────────────────┘
```

**Evidência na Validação**:
```
FLAG_TIPO_VALOR = BONUS_PURO
┌─────────────────────────────────────────┐
│ VAL_CREDITO_INSERIDO:         0.0       │
│ VAL_BONUS:              8,200.0         │
│ VAL_REAL_AJUSTADO:      8,200.0         │
│ VAL_REAL_AJUSTADO_FINAL:    0.0         │  ← Bônus totalmente retirado
└─────────────────────────────────────────┘
```

**Questões de Negócio** 🤔:
1. **Por que remover o bônus?** Bônus NÃO é receita real?
2. **Faz sentido ficar negativo?** -1.99B em agregado é aceitável?
3. **Deveria ser flag separada?** Em vez de subtrair, marcar FLAG_BONUS_ALTO?
4. **Para ABT_v5, usar qual métrica?**
   - `VAL_CREDITO_MES` (receita bruta, sem ajuste)?
   - `VAL_REAL_MES` (original, sem ajuste)?
   - `VAL_REAL_AJUSTADO_MES` (sem retirada de bônus)?

**Recomendação Temporária**:
```python
# Em vez de subtrair bônus, criar separado:
.withColumn(
    'FLAG_BONUS_PRESENTE',
    F.when(F.col('VAL_BONUS_MES') > 0, 1).otherwise(0)
)
.withColumn(
    'PERC_BONUS_SOBRE_CREDITO',
    F.when(
        F.col('VAL_CREDITO_MES') > 0,
        F.round((F.col('VAL_BONUS_MES') / F.col('VAL_CREDITO_MES')) * 100, 2)
    ).otherwise(0)
)
# Usar VAL_CREDITO_MES para modelo (receita bruta)
```

---

### 3. 🤔 TRANSAÇÕES COM VALOR ZERO: 14.1% DO VOLUME

**Evidência**:
```
DISTRIBUIÇÃO DE TIPOS DE TRANSAÇÃO
┌──────────────────────────┐
│ FLAG_TIPO_VALOR │    qtd │
├──────────────────────────┤
│ BONUS_PURO       │ 37.3M  │
│ PAGO_PURO        │ 34.2M  │
│ ZERO_TOTAL       │ 14.2M  │ ← 14.1% DOS REGISTROS!
│ VALOR_NEGATIVO   │ 12.4M  │
│ COMBO            │  1.1M  │
│ COMPONENTE_NEG   │  1.1M  │
└──────────────────────────┘
```

**O que é ZERO_TOTAL?**
```python
FLAG_TIPO_VALOR = 'ZERO_TOTAL' quando:
  VAL_CREDITO_INSERIDO = 0
  VAL_BONUS = 0
  VAL_REAL = 0
```

**Perguntas Críticas**:
1. **Por que 14.2M transações têm valor zero?**
   - Tentativas de compra bloqueadas?
   - Transações canceladas?
   - Dados corruptos?
   - Reembolsos duplos (0 = crédito - crédito)?

2. **Qual é a distribuição temporal?**
   - Aumentou no tempo?
   - Correlado com algum evento?

3. **Impacto no agregado**:
   - Essas 14.2M "não são recargas válidas" (correto)
   - Mas precisam ser investigadas para qualidade de dados

**Ação Recomendada**:
```markdown
Gate 12 (novo para v5):
- ZERO_TOTAL não deve exceder 15% do volume mensal
- Se exceder → alerta de qualidade de dados
- Documentar razão em ANALISE_QUALIDADE_DADOS.md
```

---

### 4. 🔴 VALORES NEGATIVOS: 12.4M REEMBOLSOS/DEVOLUÇÕES

**Evidência**:
```
VALOR_NEGATIVO: 12,392,854 (12.4% dos dados)
Definição: VAL_REAL < 0
Status: Contados como FLAG_RECARGA_VALIDA = 1 ✅ Correto
```

**Impacto no Agregado Mensal**:
```
2025-01:
  receita_total:        R$ 44.4B
  valor_real_liquido:   R$ 12.8B  ← 71.3% DE REDUÇÃO!
  
Motivo: Reembolsos reduzem receita em 71%
```

**Reembolsos Mensais**:
| Mês | Total SOS | % da Receita |
|-----|-----------|-------------|
| 2023-10 | -R$357.3B | 100%+ |
| 2024-04 | +R$19.8B | Positivo |
| 2024-10 | +R$30.6B | Positivo |
| 2025-01 | +R$12.8B | 29% |

**Observação**:
- Há meses onde valor_real_liquido é NEGATIVO (2023-10 a 2024-03)
- Há meses onde é POSITIVO (2024-04 em diante)
- Não está claro se a lógica de ajuste está correta

**Recomendação**:
```markdown
1. Criar FLAG_REEMBOLSO separado:
   .withColumn('FLAG_REEMBOLSO', F.when(F.col('VAL_REAL') < 0, 1).otherwise(0))

2. Usar para ABT_v5:
   - VAL_CREDITO_MES (receita bruta, sem reembolsos)
   - QTD_REEMBOLSOS (volume de devoluções)
   - PERC_REEMBOLSO (% do volume)
   - FLAG_CLIENTE_REEMBOLSISTA (alto risco?)

3. Investigar:
   - Por que 2023 tinha -357B em reembolsos?
   - Correlacionado com FLAG_SOS ou colunas específicas?
   - Afeta score de risco?
```

---

## 📈 Distribuição Detalhada

### Tipos de Transação
```
BONUS_PURO           37.3M (37.2%)  → Bônus sem crédito pago
PAGO_PURO            34.2M (34.1%)  → Crédito sem bônus
ZERO_TOTAL           14.2M (14.1%)  → Sem valor (ALERTA!)
VALOR_NEGATIVO       12.4M (12.4%)  → Reembolsos/devoluções
COMBO_PAGO_BONUS      1.1M (1.1%)   → Crédito + bônus
COMPONENTE_NEGATIVO   1.1M (1.1%)   → Componente negativo
────────────────────────────────────
TOTAL              100.2M (100%)
```

### Cobertura de Válidos
```
Total de transações:      100.2M
FLAG_RECARGA_VALIDA=1:     85.8M (85.6%)  ✅ Bom
FLAG_RECARGA_VALIDA=0:     14.2M (14.1%)  ⚠️  ZERO_TOTAL
```

### Crescimento de Clientes
```
Período      | Clientes | Variação
─────────────┼──────────┼──────────
Out/2023     | 1.43M    | -
Nov/2023     | 1.45M    | +1.3%
...
Jan/2025     | 2.30M    | +58.7% (10 meses)
Feb/2025     | 2.33M    | +1.0%
Mar/2025     | 2.33M    | +0.1% (estagnação?)
```

---

## 🎯 Características Extraídas (25 Features)

### Nível Transacional (recarga)
| Feature | Tipo | Descrição |
|---------|------|-----------|
| DATA_INSERCAO | Date | Data da recarga (parsed) |
| TIMESTAMP_COMPLETO | Timestamp | Data + hora completa |
| VAL_CREDITO_INSERIDO | Double | Valor de crédito pago |
| VAL_BONUS | Double | Valor de bônus |
| VAL_REAL | Double | Valor real (crédito - reembolso) |
| VAL_REAL_AJUSTADO | Double | Valor ajustado com SOS |
| VAL_REAL_AJUSTADO_FINAL | Double | Valor ajustado - bônus |
| FLAG_TIPO_VALOR | String | Classificação (PAGO_PURO, BONUS_PURO, etc.) |
| FLAG_RECARGA_VALIDA | Int | 1=válida, 0=ZERO_TOTAL |
| HORAS_SEM_RECARGA | Double | Horas desde última recarga válida |
| DIAS_SEM_RECARGA | Double | Dias desde última recarga válida |

### Nível Agregado Mensal (recarga_mensal)
| Feature | Tipo | Descrição |
|---------|------|-----------|
| NUM_CPF | String | Chave cliente |
| ANO_MES | String | Chave temporal (yyyy-MM) |
| QTD_TELEFONES | Long | Quantidade de celulares distintos |
| QTD_RECARGAS_VALIDAS | Long | Transações não-zero |
| QTD_TRANSACOES_TOTAL | Long | Total inc. zeros |
| QTD_SOS | Long | Quantidade de reembolsos |
| VALOR_SOS_MES | Double | Valor total reembolsos |
| VAL_CREDITO_MES | Double | Receita bruta |
| VAL_BONUS_MES | Double | Bônus distribuído |
| VAL_REAL_MES | Double | Valor real (original) |
| VAL_REAL_AJUSTADO_MES | Double | Valor com ajuste SOS |
| VAL_REAL_AJUSTADO_FINAL_MES | Double | Valor final (com bônus retirado) ⚠️ |
| HORAS_MIN/MAX/MEDIO_SEM_RECARGA | Double | Min/Max/Média em horas |
| DIAS_MIN/MAX/MEDIO_SEM_RECARGA | Double | Min/Max/Média em dias |
| DATA_PRIMEIRA_RECARGA | Date | Primeiro evento do mês |
| DATA_ULTIMA_RECARGA | Date | Último evento do mês |
| TICKET_MEDIO | Double | VAL_CREDITO_MES / QTD_RECARGAS_VALIDAS |
| FLAG_TEM_SOS_MES | Int | 1=houve reembolso |
| PERC_SOS_VALOR | Double | (VALOR_SOS_MES / VAL_CREDITO_MES) * 100 |
| VALOR_LIQUIDO_MES | Double | VAL_CREDITO_MES - VALOR_SOS_MES |
| PERC_TRANSACOES_VALIDAS | Double | (QTD_RECARGAS_VALIDAS / QTD_TRANSACOES_TOTAL) * 100 |

---

## 📊 Insights Interessantes

### 1. Padrão de Cliente Top
```
CPF: 9NN87WYZT7Y
─────────────────────────────────
Período analisado: Out/2023 - Mar/2025
Valor mensal típico: R$ 50K
Telefones: 2-3 números
Recargas/mês: 6-13
Ticket médio: R$ 7K-12K
Padrão: Altamente consistente, cliente de valor
```

### 2. Tendência de Crescimento
```
Out/2023:  1.43M clientes
Jun/2024:  1.71M clientes  (+20%)
Jan/2025:  2.30M clientes  (+35% vs Jun)
Mar/2025:  2.33M clientes  (+1% em 2 meses)  ← Estagnação?
```

**Hipótese**: Saturação de mercado em Mar/2025?

### 3. Sazonalidade de SOS
```
Out-Dez/2023: ~350-400K SOS/mês (10-15% da receita)
Jan-Mar/2025: ~250-320K SOS/mês (8-10% da receita)

Interpretação: Reembolsos diminuem proporcionalmente
```

### 4. Receita Mensal Crescente
```
Out/2023: R$ 38.5B
Mar/2025: R$ 39.3B
Crescimento: +2% em 17 meses (baixo)

Valor real líquido:
Out/2023: -R$ 357.3B (anômalo!)
Mar/2025: +R$ 14.4B (normalizado)
```

---

## 🔧 Recomendações Para ABT_v5

### 🔴 CRÍTICAS (Bloqueia Integração)

| # | Ação | Motivo | Esforço |
|---|------|--------|---------|
| **1** | Investigar INSTITUICAO 71% missing | Impossível usar feature | Alto |
| **2** | Revisar lógica VAL_REAL_AJUSTADO_FINAL | Resultado negativo não faz sentido | Médio |
| **3** | Criar FLAG_REEMBOLSO separada | Precisamos rastrear devoluções | Baixo |

**Ação Imediata**:
```python
# Em tratamento_recarga_v2.py
# NÃO usar VAL_REAL_AJUSTADO_FINAL_MES no ABT
# Substituir por:
- VAL_CREDITO_MES (receita bruta)
- FLAG_REEMBOLSO (1=teve devolução)
- PERC_REEMBOLSO (% volume reembolsado)
```

### 🟡 MÉDIAS (Validação Essencial)

| # | Ação | Motivo | Esforço |
|---|------|--------|---------|
| **4** | Gate 11: INSTITUICAO_MISSING ≤ 50% | Alerta qualidade | Baixo |
| **5** | Gate 12: ZERO_TOTAL ≤ 15% mensal | Anomalia de dados | Baixo |
| **6** | Investigar 2023 com receita negativa | Falta de lógica | Alto |

### 🟢 BOAS (Nice-to-Have)

| # | Ação | Motivo | Esforço |
|---|------|--------|---------|
| **7** | Adicionar FLAG_CLIENTE_REEMBOLSISTA | Segmentação | Baixo |
| **8** | Agregar por semana (além de mês) | Análise temporal | Médio |
| **9** | Detectar clientes SOS-dependentes | Comportamento | Médio |

---

## 📋 Próximos Passos

### Imediato (Hoje)
- [ ] Revisar anomalias com squad de dados
- [ ] Definir métrica correta para receita (bruta vs ajustada)
- [ ] Investigar INSTITUICAO missing

### Curto Prazo (Esta Semana)
- [ ] Criar tratamento_recarga_v2.py com correções
- [ ] Adicionar Gates 11-12
- [ ] Criar FLAG_REEMBOLSO e separar features

### Integração ABT_v5
- [ ] Usar `VAL_CREDITO_MES` como feature principal
- [ ] Adicionar `FLAG_REEMBOLSO`, `PERC_REEMBOLSO`
- [ ] Documentar escolhas em [abt_v5.md](abt_v5.md)
- [ ] Validar correlação com `FPD_INT` (target)

---

## 📎 Referências

- **Script**: [src/jobs/02_gold/tratamento_recarga_v1.py](../../src/jobs/02_gold/tratamento_recarga_v1.py)
- **Notebook (Original)**: [src/jobs/20260126 - Tratamento Recarga.ipynb](../../src/jobs/20260126%20-%20Tratamento%20Recarga.ipynb)
- **Notebook (Corrigido)**: [src/jobs/20260127 - Tratamento Recarga CORRIGIDO.ipynb](../../src/jobs/20260127%20-%20Tratamento%20Recarga%20CORRIGIDO.ipynb)
- **ABT v5 Spec**: [docs/05_abt_v5_docs/abt_v5.md](abt_v5.md)
- **Data Dictionary - Recarga**: [docs/01_data_dictionary/recarga.md](../01_data_dictionary/recarga.md)

---

**Última atualização**: 28 de janeiro de 2026 às 14:00  
**Status**: ⚠️ Pendente revisão de anomalias críticas
