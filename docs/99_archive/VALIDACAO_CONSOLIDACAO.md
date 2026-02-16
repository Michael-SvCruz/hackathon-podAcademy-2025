# 🔧 Consolidação de Arquivo de Validação ABT

**Data:** 26 de janeiro de 2026  
**Status:** ✅ Concluído

---

## 📋 Problema Identificado

Existiam **2 arquivos de validação duplicados e incompletos**:
1. `src/jobs/02_gold/validators/validate_abt.py` (1004 linhas) — tinha v1, v2, v3, v4, v5
2. `src/utils/validate_abt.py` (884 linhas) — tinha v1, v2, v3, v6, v6_1

**Consequências:**
- Risco de erro humano (qual arquivo editar?)
- Inconsistência (funções v4, v5 em um arquivo, v6, v6_1 em outro)
- Confusão na manutenção

---

## ✅ Solução Implementada

### 1️⃣ Consolidação de Funções
Todas as funções de validação foram **consolidadas em UM ÚNICO arquivo**:

**Arquivo central:** `src/utils/validate_abt.py`

**Funções disponíveis:**
```python
def validate_abt_v1(df_abt, count_silver)
def validate_abt_v2(df_abt, count_silver)
def validate_abt_v3(df_abt, count_silver)
def validate_abt_v4(df_abt)                    # ← Adicionado (antes em validators/)
def validate_abt_v5(df_abt, count_v4)          # ← Adicionado (antes em validators/)
def validate_abt_v6(df_abt, count_abt_v5)
def validate_abt_v6_1(df_abt, count_abt_v6)
```

**Total:** 7 funções de validação, todas em um único arquivo

### 2️⃣ Limpeza de Duplicação
**Arquivo deletado:**
- ❌ `src/jobs/02_gold/validators/validate_abt.py` (DELETADO)

**Justificativa:**
- Todos os scripts abt_v* já importavam de `src.utils.validate_abt`
- O arquivo em `validators/` era redundante e incompleto
- Centralizar em `utils/` garante que seja a "fonte de verdade"

### 3️⃣ Verificação de Imports
Todos os scripts abt_v* já **importavam corretamente** de `src.utils.validate_abt`:

| Script | Import | Status |
|--------|--------|--------|
| `00_gold_abt_builder.py` | `from src.utils.validate_abt import validate_abt_v1` | ✅ OK |
| `01_gold_abt_v2_builder.py` | `from src.utils.validate_abt import validate_abt_v2` | ✅ OK |
| `02_gold_abt_v3_builder.py` | `from src.utils.validate_abt import validate_abt_v3` | ✅ OK |
| `03_gold_abt_v4_builder.py` | `from src.utils.validate_abt import validate_abt_v4` | ✅ OK |
| `04_gold_abt_v5_builder.py` | `from src.utils.validate_abt import validate_abt_v5` | ✅ OK |
| `05_gold_abt_v6_builder.py` | `from src.utils.validate_abt import validate_abt_v6` | ✅ OK |
| `06_gold_abt_v6_1_builder.py` | `from src.utils.validate_abt import validate_abt_v6_1` | ✅ OK |

### 4️⃣ Verificação de Funções Inline
✅ **Confirmado:** Nenhum script abt_v* possui funções de validate inline

---

## 📊 Resumo das Alterações

| Operação | Detalhes | Status |
|----------|----------|--------|
| **Adicionar v4, v5** | Copiadas de `validators/` para `utils/` | ✅ Concluído |
| **Consolidação** | Todas as funções agora em um arquivo | ✅ Concluído |
| **Deletar duplicação** | Arquivo `validators/validate_abt.py` removido | ✅ Concluído |
| **Verificar imports** | Todos apontam para `src.utils.validate_abt` | ✅ OK |
| **Verificar inline** | Nenhuma função validate inline | ✅ OK |

---

## 🎯 Benefícios

1. ✅ **Fonte de verdade única** — Um arquivo para todas as validações
2. ✅ **Menor risco de erro** — Nenhuma duplicação confusa
3. ✅ **Manutenção centralizada** — Todos os gates em um lugar
4. ✅ **Imports consistentes** — Todos os scripts usam mesma origem
5. ✅ **Sem funções inline** — Separação clara entre builders e validadores

---

## 📝 Guia de Uso

### Para adicionar nova versão (ex: v7)

1. **Adicione função em** `src/utils/validate_abt.py`:
   ```python
   def validate_abt_v7(df_abt, count_abt_v6):
       """... docstring ..."""
       # Gates aqui
       return {"passed": all_passed, "gates": gates_result}
   ```

2. **No seu builder** `0X_gold_abt_v7_builder.py`:
   ```python
   from src.utils.validate_abt import validate_abt_v7
   
   # Dentro do main():
   validate_abt_v7(df_abt_v7, count_abt_v6)
   ```

3. **NUNCA** adicione função validate inline no builder

### Para editar uma validação existente

**Sempre edite em:** `src/utils/validate_abt.py`

**Nunca crie arquivos paralelos** em `validators/` ou outros diretórios

---

## ✨ Conclusão

A consolidação garante que **todas as validações de ABT estejam em um único arquivo**, eliminando confusão e risco de erro humano. Todos os scripts abt_v* já referenciam corretamente este arquivo centralizado.

**Próximas ações:**
- ✅ Executar Silver e Gold builders com confiança
- ✅ Saber que validações estão centralizadas em `src/utils/validate_abt.py`
- ✅ Adicionar novas validações sempre neste arquivo
