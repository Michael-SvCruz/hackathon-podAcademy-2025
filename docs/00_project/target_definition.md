# Target Definition — Evento Âncora, Definição Temporal e Labels (v1 — com Bureau Full)

## 1) 🎯 Objetivo
Formalizar o “relógio” do dataset e os targets/labels usados no projeto, garantindo:
- consistência de treino e avaliação
- prevenção de leakage
- capacidade de fazer análise de impacto (aprovação/reprovação e swaps)

---

## 2) 🧷 Evento âncora (referência do dataset)
**Definição operacional (v1):**
- Unidade de análise do projeto: **cliente-mês** (`NUM_CPF + SAFRA`)
- Evento âncora (na prática): **processo de decisão/contratação associado à safra** do registro

Como a base principal é mensal (`SAFRA`), o projeto usa:
- `DT_SAFRA = to_date(concat(SAFRA,'01'),'yyyyMMdd')` como data_ref operacional

> Observação: caso exista uma data real de “decisão/contratação” em outra fonte, esse documento pode evoluir para v2 com evento âncora mais preciso.

---

## 3) ⏱ Definição temporal (anti-leakage)
Para cada linha (`NUM_CPF`, `SAFRA`):

### 3.1) Features (X)
- devem refletir informação **conhecida até a data_ref** (safra)
- para bases event-level (ex.: recarga/pagamento):
  - derivar `SAFRA_EVENTO` do timestamp do evento
  - agregar por `NUM_CPF + SAFRA_EVENTO`
  - garantir lookback coerente (M1/M3/M6), quando aplicável

### 3.2) Label/Target (Y)
- deve representar um resultado **após** o evento âncora
- e nunca pode ser utilizado como feature

---

## 4) Labels do projeto (duas camadas: risco e impacto)

## 4.1) Label de risco (modelo principal)
### `FPD` (0/1) — target de risco
- **Uso:** target do modelo de risco (inadimplência/atraso no primeiro pagamento, conforme discussão do projeto)
- **Fonte de verdade:** `base_score_bureau_movel_full` (spine v2)
- **Regra observada nos dados (evidência):**
  - quando `FLAG_INSTALACAO=1`: `FPD` está observado (não nulo)
  - quando `FLAG_INSTALACAO=0`: `FPD` é nulo (não observado)

**Implicação de treino:**
- Treinar e avaliar risco usando somente:
  - `FLAG_INSTALACAO = 1` (equivalente a `FPD is not null`)

**Regra crítica:**
- `FPD` não entra como feature.

---

## 4.2) Label de decisão (para impacto e swaps)
### `FLAG_INSTALACAO` (0/1) — decisão/histórico da política atual
- **Uso:** label para medir **aprovação/reprovação** observada na política atual
- **Fonte de verdade:** `base_score_bureau_movel_full`

**Interpretação operacional:**
- `FLAG_INSTALACAO = 1` → aprovado/contratado
- `FLAG_INSTALACAO = 0` → reprovado (não contratado)

**Regra crítica:**
- `FLAG_INSTALACAO` não deve ser feature do modelo de risco (alto risco de leakage/proxy de decisão).

---

## 5) Avaliação (como os dois labels se conectam)

### 5.1) Performance do modelo (risco)
Avaliar em `FLAG_INSTALACAO=1`:
- KS (incluindo OOT fev/mar)
- curva e métricas adicionais se necessário

### 5.2) Impacto em aprovação/reprovação (swap-in/out)
Avaliar no universo completo da base full:
- baseline (política atual): `FLAG_INSTALACAO`
- proposta (política via modelo): decisão por cutoff no score/predição

Definições:
- **Swap-in:** casos com `FLAG_INSTALACAO=0` que o modelo aprovaria
- **Swap-out:** casos com `FLAG_INSTALACAO=1` que o modelo reprovaria

> Observação: como `FPD` não está observado em `FLAG_INSTALACAO=0`, a análise de impacto deve focar no efeito sobre aprovações/reprovações e, quando possível, em estimativas de risco esperado no grupo swap-in.

---

## 6) Checklist (gates) antes de treinar/avaliar
1. Confirmar grão 1:1 por `NUM_CPF + SAFRA` na ABT
2. Garantir que `FPD` é usado apenas como label
3. Garantir que `FLAG_INSTALACAO` é usado apenas para impacto/swaps (não feature)
4. Controlar janelas temporais para bases event-level (recarga/pagamento)