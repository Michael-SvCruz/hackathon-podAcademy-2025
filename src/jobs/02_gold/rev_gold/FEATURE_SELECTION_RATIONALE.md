# 📋 Rationale: Seleção de Features v1 rev_gold

**Documento:** Justificativa técnica e negócio para escolha das 20 features  
**Data:** 27 de janeiro de 2026  
**Versão:** 1.0

---

## 🎯 Contexto Estratégico

### Problema
- Bronze Atraso: ~50 colunas
- Bronze Pagamento: ~75 colunas
- **Total potencial:** 125 colunas
- **Selecionadas para v1:** 20 features
- **Pergunta:** Por que não tudo?

### Resposta: Curse of Dimensionality + Eficiência

A engenharia de features eficaz não é sobre **incluir tudo**, mas **incluir apenas o que discrimina o target**. Razões técnicas:

1. **Overfitting:** 125 features com amostra modesta = modelo memoriza ruído
2. **Colinearidade:** Bronze tem muitas variáveis redundantes (ex: 3 versões de "valor da fatura")
3. **Interpretabilidade:** 20 features > 125 features para entender importância
4. **Velocidade:** Menos features = treino + validação mais rápido
5. **Estabilidade:** Menos variáveis = coeficientes mais estáveis entre datasets

### Estratégia de Validação
Este **não é definitivo**. Validaremos com KS:
- **Se KS ≥ 40%:** seleção está certa ✅
- **Se KS < 38%:** expandir para 30-40 features na iteração seguinte

---

## 🔍 ATRASO: 12 Features Selecionadas + Rationale

### ✅ INCLUÍDAS (Por quê):

#### 1. **ATRASO_DIAS_MAX** → `DW_FAIXA_AGING_FATURA`
```
Bronze tem: DAT_VENCIMENTO_FAT, DAT_STATUS_FAT, múltiplas datas
Agregamos para: DIAS_ATRASO_MAX (quantidade de dias em atraso)
```
**Rationale:**
- **Sinal direto de risco:** quanto mais dias em atraso, maior probabilidade de não pagar
- **Simplicidade:** 1 número ordinal (0-30d, 30-60d, 60-90d, 90+d) é interpretável
- **Reunião citou:** "comportamento de pagamento é crucial" → dias em atraso = comportamento básico
- **Esperado:** Correlação positiva forte com FPD

**O que NÃO levamos da Bronze:**
- `DAT_CRIACAO_FAT, DAT_VENCIMENTO_FAT, DAT_ORIGINAL_VCTO_FAT, DAT_ALTERACAO_VCTO_FAT` (muitas datas)
  - **Por quê:** Todas essas datas são INPUT para calcular `dias_atraso`
  - Manter todas = colinearidade. Mantemos apenas a síntese: faixa aging

---

#### 2. **ATRASO_QTD** → contagem de faturas em atraso
```
Bronze tem: NUM_FATURA_HASH (transacional - 1 linha por fatura)
Agregamos para: contagem de quantas faturas estão atrasadas
```
**Rationale:**
- **Frequência de atrasos:** 1 fatura atrasada ≠ 5 faturas
- **Padrão crônico:** cliente que atrasa múltiplas faturas = padrão = mais risco
- **Novo vs. Estabelecido:** novo cliente com 1 fatura atrasada < cliente com 5

**O que NÃO levamos:**
- `NUM_FATURA_HASH` individual (centenas de valores únicos por cliente)
  - **Por quê:** Detalhe demais. O que importa é a *contagem*, não a fatura específica

---

#### 3. **ATRASO_VALOR_MAX, ATRASO_VALOR_TOTAL, ATRASO_VALOR_MEDIO**
```
Bronze tem: VAL_FAT_LIQUIDO, VAL_FAT_BRUTO, VAL_FAT_ABERTO, VAL_FAT_AJUSTE, VAL_PARC_APARELHO_LIQ (5+ versões)
Agregamos para: MAX, TOTAL, MEDIO (3 estatísticas)
```
**Rationale:**
- **Magnitude importa:** R$ 10 em atraso ≠ R$ 1000
- **3 perspectivas complementares:**
  - MAX = pior caso (cliente deixou evoluir?)
  - TOTAL = volume total em risco
  - MEDIO = tamanho típico (padrão de dificuldade)
- **Conferência de resultado:** TOTAL ≈ MAX × QTD (validação cruzada)

**O que NÃO levamos:**
- `VAL_FAT_LIQUIDO, VAL_FAT_BRUTO, VAL_FAT_BRUTO_BC` (3 versões do mesmo valor)
  - **Por quê:** Alta colinearidade. Diferem apenas por descontos/ajustes, que já capturamos em `ATRASO_VALOR_MEDIO`
- `VAL_FAT_CREDITO, VAL_FAT_AJUSTE` (ajustes específicos)
  - **Por quê:** Detalhe operacional, não comportamental. Já está embutido em VALOR_TOTAL

---

#### 4. **ATRASO_FLAG_ATUAL** → indicador: "há atraso AGORA?"
```
Bronze tem: IND_STATUS_FAT, IND_WO, IND_PDD (informações de status)
Agregamos para: FLAG simples (0/1) = há atraso neste snapshot?
```
**Rationale:**
- **Decisão imediata:** para aprovar crédito agora, a pergunta é "este cliente está em atraso HOJE?"
- **Binária, interpretável:** 0 = limpo, 1 = em atraso
- **Reunião citou:** decisão de crédito é **instantânea** no ponto de venda

**O que NÃO levamos:**
- `DAT_STATUS_FAT` (data específica do status)
  - **Por quê:** Flag 0/1 é mais preditivo que a data. A data é INPUT para saber "há atraso?"
- Múltiplas status (`IND_STATUS_FAT = A, C, P, etc`)
  - **Por quê:** Agrupamos todos os status ≠ "pago" em 1 = "em atraso"

---

#### 5. **ATRASO_FLAG_WRITE_OFF** → indicador de conta baixada
```
Bronze tem: IND_WO (valores como W, R, -1)
Agregamos para: FLAG binária (0/1)
```
**Rationale:**
- **Sinal extremo:** write-off = empresa desistiu de cobrar = **perda total já ocorreu**
- **Predictivo máximo:** qualquer cliente com write-off NO PASSADO = risco altíssimo de reincidência
- **Reunião enfatizou:** "comportamento de pagamento" inclui histórico de default

**O que NÃO levamos:**
- `IND_WO` original com valores W/R/-1
  - **Por quê:** Melhor binarizar (W=1, outro=0) do que deixar categórico (3 valores)
  - Binária = mais fácil para modelo, menos colinearidade

---

#### 6. **ATRASO_FLAG_PDD** → Possibly Defaulted
```
Bronze tem: IND_PDD (valores S, N, -1)
Agregamos para: FLAG binária (0/1)
```
**Rationale:**
- **Score interno de risco:** PDD é síntese de múltiplos critérios internos da Claro
- **Black-box reutilizado:** já que existe, aproveita
- **Complementa write-off:** write-off é histórico; PDD é prognóstico

**O que NÃO levamos:**
- `IND_PDD` original com valores S/N/-1
  - **Por quê:** Mesmo raciocínio: binarizar é mais eficiente
- `IND_PCCR` (PCCR = "payment currency clarification recent"?)
  - **Por quê:** Conceito menos claro; PDD já captura risco de default

---

#### 7. **ATRASO_FLAG_ACA** → Ação de cobrança judicial
```
Bronze tem: IND_ACA (valores S, A, N)
Agregamos para: FLAG binária (0/1)
```
**Rationale:**
- **Escalação:** atraso → cobrança ativa → ação judicial → write-off (sequência de risco)
- **Sinal de esforço:** se a Claro acionou cobrança, algo errado aconteceu
- **Reincidência:** cliente em ACA hoje = histórico de pagamento ruim

**O que NÃO levamos:**
- Detalhe do tipo de cobrança (judicial, administrativa, etc)
  - **Por quê:** Fato binário (houve ou não) é mais preditivo que tipo

---

#### 8. **ATRASO_FAIXA_TEMPO_BASE** → Tempo como cliente
```
Bronze tem: DW_FAIXA_TEMPO_BASE (faixas como 0-3m, 3-6m, 6-12m, 12m+)
Agregamos para: como está
```
**Rationale:**
- **Efeito de novidade:** clientes NOVOS têm risco naturalmente maior (falta de histórico)
- **Sinal demográfico:** diferencia recém-chegados de estabelecidos
- **Reunião citou:** "informações internas de comportamento permitem aproveitar melhor"

**O que NÃO levamos:**
- `DAT_ATIVACAO_CONTA_CLI` (data exata)
  - **Por quê:** Faixa (categórica ordenada) é mais robusta que data absoluta
  - Data absoluta = 10000+ valores únicos; faixa = 4 valores

---

#### 9. **ATRASO_QTD_FATURAS_TOTAL** → Total de faturas no período
```
Bronze tem: NUM_FATURA_HASH (múltiplas linhas = múltiplas faturas)
Agregamos para: contagem de quantas faturas geradas
```
**Rationale:**
- **Contexto de atividade:** cliente que gera 5 faturas/mês ≠ cliente que gera 1
- **Normaliza outras métricas:** permite calcular **TAXA** de atrasos (atraso_qtd / total)
- **Sinal de uso:** produto (telco) gera faturas; mais faturas = mais engajado

**O que NÃO levamos:**
- Detalhe por tipo de fatura (`DW_TIPO_FATURAMENTO`)
  - **Por quê:** O que importa é o total, não os tipos individuais
  - Se tipos são importantes, aparecerão em v3 (Cadastro) ou v4 (Telco)

---

#### 10-12. **FLAGS DE MISSING** (ATRASO_FLAG_*_MISSING)
```
Bronze tem: Sentinelas como -1, -2, -3 para "não informado"
Agregamos para: 3 flags binárias = "faltava dado?"
```
**Rationale:**
- **Captura padrão de dados:** novo cliente pode ter dados incompletos
- **Não descarta:** em vez de descartar linhas com -1, criamos flag
- **Sinal em si:** "falta dado" pode ser sinal de risco (cliente novo sem histórico)

**O que NÃO levamos:**
- Os valores sentinela originais (-1, -2, -3)
  - **Por quê:** Binarizar (tem ou não tem) é mais simples que manter categórico com -1/-2/-3

---

### ❌ NÃO INCLUÍDAS (Por quê deixamos de lado):

#### Datas (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `DAT_CRIACAO_FAT` | Calculável a partir de VENCIMENTO; já capturado em faixa aging |
| `DAT_VENCIMENTO_FAT` | INPUT para calcular dias_atraso; síntese já em FAIXA_AGING |
| `DAT_ORIGINAL_VCTO_FAT` | Detalhamento demais; faixa captura essência |
| `DAT_ALTERACAO_VCTO_FAT` | Operacional, não comportamental |
| `DAT_STATUS_FAT` | INPUT para saber "há atraso?"; síntese é FLAG_ATUAL |
| `DAT_CANCELAMENTO_FAT` | Apenas para canceladas (minoria); coberto em FLAG_ATUAL |
| `DAT_CRIACAO_DW` | Metadata do DW, não de comportamento |
| `DAT_CRIACAO_REGISTRO_TRANS`, `DAT_ALTERACAO_REGISTRO_TRANS` | Auditoria, não preditiva |

**Custo:** 8 datas = 8 colunas  
**Benefício de incluir:** ~0% (tudo já está sintetizado em faixas + flags)  
**Decisão:** ❌ Excluir

---

#### Valores monetários alternativos (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `VAL_FAT_BRUTO` vs `VAL_FAT_LIQUIDO` | Colineares entre si; diferença = descontos |
| `VAL_FAT_BRUTO_BC` | Variação do bruto; redundante |
| `VAL_FAT_CREDITO` | Ajuste específico; já embutido em TOTAL |
| `VAL_FAT_AJUSTE` | Ajuste operacional; não preditivo |
| `VAL_FAT_PAGAMENTO_BRUTO` | Não é atraso, é pagamento (vai em outra feature) |
| `VAL_MULTA_CANCELAMENTO` | Raro; coberto por MULTA_JUROS |
| `VAL_PARC_APARELHO_LIQ` | Detalhe de produto; para v4 (Telco) |
| `VAL_FAT_LIQ_JM_MC` | Cálculo intermediário; redundante |

**Custo:** 8 variáveis  
**Benefício de incluir:** ~5% (detalhe operacional, não comportamental)  
**Decisão:** ❌ Excluir (podem voltar em v2+ se KS estagna)

---

#### Identificadores e Sequências (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `NUM_FATURA_HASH` | Identificador único; não é feature |
| `NUM_ENT_SEQ_FATURA` | Identificador alternativo; já temos NUM_CPF+SAFRA |
| `CONTRATO` | Identificador; não preditivo para risco de cliente |
| `DW_TIPO_FATURAMENTO` | Categoria de tipo; para v4 (Telco) |
| `DW_TIPO_CLIENTE_CONTA` | Segmentação; para v3 (Cadastro) |
| `COD_PLATAFORMA` | Categoria de plataforma; para v4 |

**Custo:** 6 campos  
**Benefício de incluir:** ~0%  
**Decisão:** ❌ Excluir (metadados, não features)

---

#### Indicadores menos preditivos (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `IND_PRIMEIRA_FAT` | Sinal já capturado em FAIXA_TEMPO_BASE (novos clientes) |
| `IND_FRAUDE` | Provavelmente raro; problema separado de inadimplência |
| `IND_ISENCAO_COB_FAT` | Muito específico; operacional |

**Custo:** 3 campos  
**Benefício de incluir:** ~2%  
**Decisão:** ❌ Excluir (sinal redundante ou muito específico)

---

---

## 🔍 PAGAMENTO: 8 Features Selecionadas + Rationale

### ✅ INCLUÍDAS (Por quê):

#### 1. **PAGTO_QTD** → contagem de pagamentos realizados
```
Bronze tem: SEQ_ENTIDADE_PAGAMENTO (transacional - 1 linha por pagamento)
Agregamos para: contagem de quantos pagamentos
```
**Rationale:**
- **Sinal direto de responsabilidade:** cliente que paga múltiplas vezes ≠ cliente que paga 1x
- **Frequência:** mais frequência = mais engajado
- **Complementa atraso:** se ATRASO_QTD=5 e PAGTO_QTD=5, cliente tenta pagar (regulariza)
- **Reunião:** "comportamento de pagamento"

**O que NÃO levamos:**
- `SEQ_ENTIDADE_PAGAMENTO` individual (centenas por cliente)
  - **Por quê:** Detalhe demais. Contagem é síntese suficiente

---

#### 2. **PAGTO_VALOR_TOTAL, PAGTO_VALOR_MEDIO**
```
Bronze tem: VAL_ORIGINAL_PAGAMENTO, VAL_ATUAL_PAGAMENTO, VAL_PAGAMENTO_FATURA, VAL_PAGAMENTO_ITEM (4+ variações)
Agregamos para: TOTAL, MEDIO (2 estatísticas)
```
**Rationale:**
- **Compromisso financeiro:** quanto cliente paga em VALOR (não apenas frequência)
- **TOTAL:** volume absoluto de pagamentos
- **MEDIO:** tamanho típico (indica padrão de dificuldade - paga pouco ou normal?)
- **Complementa ATRASO_VALOR_*:** regulariza métrica (atraso de R$ 1000 vs pagamento de R$ 500 = indicador de dificuldade)

**O que NÃO levamos:**
- `VAL_ORIGINAL_PAGAMENTO` vs `VAL_ATUAL_PAGAMENTO` (duas versões)
  - **Por quê:** Diferença = desconto/abono; já capturamos em DESCONTO_TOTAL. Usar ORIGINAL é suficiente.
- `VAL_PAGAMENTO_FATURA` vs `VAL_PAGAMENTO_ITEM` (nível de granularidade)
  - **Por quê:** Agregamos em TOTAL (tudo junto); detalhe não importa para cliente-mês
- `VAL_DESCONTO_ITEM` individual
  - **Por quê:** Sintetizada em DESCONTO_TOTAL

---

#### 3. **PAGTO_VALOR_MINIMO, PAGTO_VALOR_MAXIMO**
```
Bronze tem: VAL_*_PAGAMENTO em múltiplas linhas
Agregamos para: MIN, MAX (extremos)
```
**Rationale:**
- **MIN:** menor pagamento → indicador de dificuldade (cliente paga "quanto consegue")
- **MAX:** maior pagamento → indicador de capacidade (cliente conseguiu pagar muito 1x)
- **Volatilidade:** spread (MAX-MIN) = inconsistência no comportamento = risco?
- **Complementa MEDIO:** se MEDIO=R$500 mas MIN=R$10 e MAX=R$5000 = comportamento muito volátil

**O que NÃO levamos:**
- Percentis (P25, P75, etc)
  - **Por quê:** MIN/MAX captura suficiente; mais detalhes = overfitting

---

#### 4. **PAGTO_FREQ_DIAS** → dias médios entre pagamentos
```
Bronze tem: DAT_STATUS_PAGAMENTO, DAT_CRIACAO_PAGAMENTO, DAT_ATUALIZACAO_PAGAMENTO
Agregamos para: média de dias entre 2 pagamentos consecutivos
```
**Rationale:**
- **Regularidade:** cliente que paga a cada 30 dias é mais confiável que cliente que paga aleatoriamente
- **Organização:** frequência baixa = cliente organizado; alta = cliente desorganizado/paga quando pode
- **Reunião:** "comportamento de pagamento é crucial" inclui **timing**

**O que NÃO levamos:**
- Data absoluta de pagamentos
  - **Por quê:** Frequência é mais preditiva que data
- Desvio padrão de frequência
  - **Por quê:** Para v1, média é suficiente; desvio pode vir em v6 (Enhanced)

---

#### 5. **PAGTO_DIAS_DESDE_ULTIMO** → dias do último pagamento até snapshot
```
Bronze tem: DAT_STATUS_PAGAMENTO (data do pagamento mais recente)
Agregamos para: dias transcorridos = hoje - último_pagamento
```
**Rationale:**
- **Sinal de inadimplência iminente:** cliente que não paga há 60+ dias = risco crescente
- **Temporal:** diferente de histórico (PAGTO_QTD); isto é STATUS ATUAL
- **Complementa ATRASO_FLAG_ATUAL:** DIAS_DESDE = "quanto tempo?"; FLAG = "há?"

**O que NÃO levamos:**
- Data absoluta
  - **Por quê:** Dias transcorridos é mais preditivo (invariante a quando foi rodado)

---

#### 6. **PAGTO_FLAG_PENDENTE** → há pagamento pendente?
```
Bronze tem: IND_STATUS_PAGAMENTO (valores P, R, C, B, null)
Agregamos para: FLAG binária (1=há pendente, 0=tudo certo)
```
**Rationale:**
- **Status imediato:** pagamento pode estar em processamento (P), realizado (R), cancelado (C), etc
- **Risco:** P (pendente) = cliente iniciou mas não finalizou = risco de não pagar
- **Complementa PAGTO_QTD:** quantidade só diz quantas tentou; pendente diz "começou a última?"

**O que NÃO levamos:**
- Status categórico detalhado (P/R/C/B individual)
  - **Por quê:** Binarizar (pendente vs não) é suficiente; detalhes são meta-dados

---

#### 7. **PAGTO_FLAG_JUROS** → houve incidência de juros?
```
Bronze tem: VAL_JUROS_MULTAS_ITEM, VAL_MULTA_EQUIP_ITEM, VAL_MULTA_EQUIP_TOTAL, VAL_MULTA_FID_ITEM
Agregamos para: FLAG binária (1=houve juros, 0=não)
```
**Rationale:**
- **Histórico de atraso:** juros = consequência de atraso prévio
- **Reincidência:** cliente com juros cobrados = cliente que JÁ atrasou = risco
- **Complementa ATRASO_VALOR_MULTA_JUROS:** aquela tem valor histórico; isto é FLAG (ocorrência)

**O que NÃO levamos:**
- `VAL_MULTA_EQUIP_ITEM` vs `VAL_MULTA_FID_ITEM` (tipos específicos)
  - **Por quê:** Fato é: houve juros? Tipo é detalhe operacional
- Histórico de juros (cada ocorrência)
  - **Por quê:** Agregação em FLAG é suficiente

---

#### 8. **PAGTO_JUROS_TOTAL** → total de juros incididos
```
Bronze tem: VAL_JUROS_MULTAS_ITEM, VAL_MULTA_EQUIP_TOTAL, etc (valores dispersos)
Agregamos para: soma total de juros
```
**Rationale:**
- **Magnitude de atraso:** R$ 10 em juros ≠ R$ 500
- **Complementa FLAG_JUROS:** flag diz "ocorreu"; TOTAL diz "quanto custou?"
- **Sinal de severidade:** cliente que acumulou R$ 500 em juros = atraso crônico e grave

**O que NÃO levamos:**
- Detalhes por tipo (juros vs multa de equipamento vs multa fidelidade)
  - **Por quê:** Apenas JUROS é comportamental; multas são específicas de produto (v4)

---

#### 9. **PAGTO_DESCONTO_TOTAL** → total de descontos/abonos
```
Bronze tem: VAL_DESCONTO_ITEM (múltiplas linhas de desconto)
Agregamos para: soma total
```
**Rationale:**
- **Negociação:** cliente que recebe desconto = cliente em dificuldade (renegociou)
- **Sinal de risco:** muitos descontos = muitas renegociações = cliente com problema crônico
- **Contexto:** desconto alto em relação a pagto = cliente paga menos do que devia

**O que NÃO levamos:**
- Quantidade de descontos (frequência)
  - **Por quê:** TOTAL captura; frequência é detalhe

---

#### 10. **PAGTO_METODO_PREDOMINANTE** → qual método paga mais?
```
Bronze tem: COD_METODO_PAGAMENTO (valores como débito automático, boleto, etc)
Agregamos para: mode (método mais comum)
```
**Rationale:**
- **Compromisso:** débito automático = se paga, é automático (mais confiável)
- **Sinal comportamental:** cliente que usa boleto = cliente que precisa lembrar/ativo
- **Variável categórica:** diferentes métodos têm diferentes taxas de inadimplência

**O que NÃO levamos:**
- Todos os métodos individuais (débito, boleto, etc)
  - **Por quê:** Predominante captura essência; detalhes podem vir em v4
- `COD_FORMA_PAGAMENTO` (alternativa)
  - **Por quê:** Redundante com método; apenas um

---

#### 11. **FLAGS DE MISSING** (PAGTO_FLAG_*_MISSING)
```
Bronze tem: Sentinelas para dados incompletos
Agregamos para: flags binárias
```
**Rationale:**
- Mesmo que em Atraso: captura padrão de dados sem descartar
- Novo cliente pode ter histórico de pagamento incompleto

**O que NÃO levamos:**
- Valores sentinela originais

---

### ❌ NÃO INCLUÍDAS (Por quê deixamos de lado):

#### Datas (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `DAT_STATUS_FATURA` | INPUT para derivar SAFRA; já capturado em DIAS_DESDE |
| `DAT_STATUS_PAGAMENTO` | INPUT para calcular frequência; síntese em FREQ_DIAS |
| `DAT_CRIACAO_PAGAMENTO` | Operacional, não comportamental |
| `DAT_ATUALIZACAO_PAGAMENTO` | Operacional |
| `DAT_CRIACAO_DW` | Metadata do DW |
| `DAT_CRIACAO_CREDITO`, `DAT_ATUALIZACAO_CREDITO`, `DAT_CRIACAO_ATIVIDADE`, etc | Nível de detalhe demasiado; agregados em TOTAL |

**Custo:** 10+ datas  
**Benefício de incluir:** ~0% (tudo sintetizado em estatísticas)  
**Decisão:** ❌ Excluir

---

#### Valores monetários alternativos (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `VAL_PAGAMENTO_FATURA` vs `VAL_PAGAMENTO_ITEM` | Nível de detalhe; agregamos em TOTAL |
| `VAL_ORIGINAL_PAGAMENTO` vs `VAL_ATUAL_PAGAMENTO` | Colineares; diferença = desconto (já capturado) |
| `VAL_PAGAMENTO_CREDITO` | Alocação de crédito; não é pagamento |
| `VAL_BAIXA_ATIVIDADE` | Atividade, não pagamento |
| `VAL_MULTA_EQUIP_ITEM`, `VAL_MULTA_EQUIP_TOTAL`, `VAL_MULTA_FID_ITEM` | Específicas de produto (v4); capturado em FLAG_JUROS |

**Custo:** 8+ variáveis  
**Benefício de incluir:** ~3% (detalhe operacional)  
**Decisão:** ❌ Excluir (podem voltar em v2+)

---

#### Identificadores e Sequências (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `CONTRATO` | Identificador, não feature |
| `DW_NUM_CLIENTE` | Alternativa para NUM_CPF; redundante |
| `SEQ_FATURA`, `NUM_SUB_SEQ_FATURA`, `NUM_CREDITO_SEQ` | Identificadores; não preditivos |
| `SEQ_ENTIDADE_PAGAMENTO`, `SEQ_ENTIDADE_ATIVIDADE`, `SEQ_ENTIDADE_CREDITO` | Identificadores transacionais |
| `SEQ_PAGAMENTO_CREDITO`, `SEQ_FATURA_CREDITO` | Chaves de detalhe |

**Custo:** 10+ campos  
**Benefício de incluir:** ~0%  
**Decisão:** ❌ Excluir (metadados)

---

#### Categorias de tipo/classe (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `DW_TIPO_PAGAMENTO`, `COD_TIPO_PAGAMENTO` | Tipo de pagamento; para v4 (Telco) |
| `DW_TIPO_FATURA`, `COD_TIPO_FATURA` | Tipo de fatura; para v4 |
| `IND_TIPO_CREDITO` | Tipo de crédito; operacional |
| `DW_FORMA_PAGAMENTO` | Alternativa a COD_METODO; redundante |
| `COD_ALOCACAO_CREDITO`, `COD_DESALOCACAO_CREDITO` | Operacional |

**Custo:** 8+ campos  
**Benefício de incluir:** ~2% (detalhe operacional/produto)  
**Decisão:** ❌ Excluir (podem voltar em v4)

---

#### Status e Indicadores menos preditivos (não levamos)
| Campo Bronze | Razão da exclusão |
|---|---|
| `IND_STATUS_PAGAMENTO` (detalhado) | Binarizado em FLAG_PENDENTE; suficiente |
| `IND_STATUS_FATURA` (detalhado) | Binarizado em PAGTO_FLAG_PENDENTE |
| `DSC_PAGAMENTO` | Descrição textual; não preditiva |
| `DSC_NOME_BANCO_PAGAMENTO` | Banco pode ir em v4 se relevante |

**Custo:** 4 campos  
**Benefício de incluir:** ~0%  
**Decisão:** ❌ Excluir (metadados, não features)

---

---

## 📊 Sumário Executivo da Seleção

| Dimensão | Bronze | Selecionado | Taxa | Rationale |
|----------|--------|-------------|------|-----------|
| **Atraso** | ~50 cols | 12 features | 24% | Mantém sinais de risco (dias, valores, flags); remove redundância de datas |
| **Pagamento** | ~75 cols | 8 features | 11% | Mantém sinais de comportamento (freq, valor, status); remove detalhe operacional |
| **TOTAL** | ~125 cols | 20 features | 16% | Elimina colinearidade; preserva discriminação |

---

## 🔬 Teste Científico Proposto

### Hipótese
**20 features bem-selecionadas > 125 features (colineares)**

### Métrica
- KS v1_rev_gold esperado: **40-42%**
- KS v1_original baseline: **33.1%**
- Δ esperado: **+7-9pp** (40-50% melhoria)

### Validação
```
Se KS ≥ 40%:
  ✅ Seleção foi estratégica
  → Prosseguir com v2-v6
  
Se KS < 38%:
  ⚠️ Seleção foi restritiva
  → Expandir para 30-40 features em iteração seguinte
  → Investigar quais features faltam
```

---

## 🎯 Decisão Futura (Post-KS)

### Se expandirmos, prioridade é:

#### De Atraso:
1. `VAL_FAT_ABERTO_LIQ` (valor líquido em aberto)
2. Detalhes por `DW_TIPO_FATURAMENTO` (tipo de fatura)

#### De Pagamento:
1. `COD_TIPO_PAGAMENTO` (tipo de pagamento detalhado)
2. `DW_FORMA_PAGAMENTO` (forma de pagamento)
3. Percentis de distribuição de valores (P25, P75)

#### Cruzadas:
1. Ratio: `ATRASO_VALOR_TOTAL / PAGTO_VALOR_TOTAL` (dívida vs capacidade)
2. Ratio: `PAGTO_QTD / ATRASO_QTD` (regularizações vs problemas)

---

## 📖 Conclusão

Esta seleção de **20 features** representa um **balanço estratégico** entre:
- ✅ **Capturar sinais comportamentais** (o que Fernando enfatizou)
- ✅ **Evitar colinearidade** (garantir estabilidade do modelo)
- ✅ **Manter interpretabilidade** (entender contribuição de cada feature)
- ✅ **Permitir escalabilidade** (v2-v6 podem adicionar mais sem duplicação)

**O teste (KS) dirá se foi suficiente. Caso não, expandimos metodicamente.**

---

**Documento finalizado:** 27 de janeiro de 2026  
**Status:** Justificativa técnica completa para aprovação do time
