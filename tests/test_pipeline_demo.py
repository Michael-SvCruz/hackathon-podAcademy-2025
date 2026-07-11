"""
================================================================================
DEMO: Validacao do pipeline de features com dados sinteticos
================================================================================
Este script demonstra que o codigo de engenharia de dados funciona corretamente
usando dados sinteticos (sem precisar do Databricks).

Execucao:
    python tests/test_pipeline_demo.py

Ou via Docker:
    docker run --rm hackathon-podacademy python tests/test_pipeline_demo.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
from datetime import date

# ── SparkSession em modo local (sem cluster) ─────────────────────────────────
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("HackathonDemo")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*70)
print("  DEMO — Hackathon PodAcademy 2025 | Validacao com dados sinteticos")
print("="*70)

# ── 1. Dados sinteticos de recarga ────────────────────────────────────────────
schema_recarga = StructType([
    StructField("num_cpf",             StringType(),  True),
    StructField("dt_recarga",          DateType(),    True),
    StructField("val_credito_inserido", DoubleType(), True),
    StructField("val_bonus",           DoubleType(),  True),
    StructField("valor_sos",           DoubleType(),  True),
    StructField("safra_recarga",       StringType(),  True),
])

dados_recarga = [
    # CPF_001: recarga regular, sem stress
    ("CPF_001", date(2024,1,5),  20.0, 0.0,  0.0,  "202401"),
    ("CPF_001", date(2024,1,15), 30.0, 5.0,  0.0,  "202401"),
    ("CPF_001", date(2024,1,25), 20.0, 0.0,  0.0,  "202401"),
    # CPF_002: usa SOS com frequencia (estresse financeiro)
    ("CPF_002", date(2024,1,3),  10.0, 0.0,  5.0,  "202401"),
    ("CPF_002", date(2024,1,10), 10.0, 0.0,  5.0,  "202401"),
    ("CPF_002", date(2024,1,20), 10.0, 0.0,  5.0,  "202401"),
    ("CPF_002", date(2024,1,28), 10.0, 0.0,  5.0,  "202401"),
    # CPF_003: poucas recargas, valores altos
    ("CPF_003", date(2024,1,2),  100.0, 10.0, 0.0, "202401"),
    ("CPF_003", date(2024,1,20), 100.0, 10.0, 0.0, "202401"),
]

df_recarga = spark.createDataFrame(dados_recarga, schema=schema_recarga)

# ── 2. Feature engineering (logica extraida do pipeline real) ─────────────────
SAFRA_REFERENCIA = "202402"  # Safra de avaliacao (features sao do mes anterior)

df_features = (
    df_recarga
    # Filtro anti-leakage: apenas dados ANTERIORES a safra de referencia
    .filter(F.col("safra_recarga") < SAFRA_REFERENCIA)
    # Valor real ajustado: exclui SOS e bonus (regra de negocio)
    .withColumn(
        "val_real",
        F.col("val_credito_inserido") - F.col("valor_sos") - F.col("val_bonus")
    )
    # Flag SOS
    .withColumn("flag_sos", F.when(F.col("valor_sos") > 0, 1).otherwise(0))
    # Agregar por CPF (janela M1 = ultimo mes)
    .groupBy("num_cpf")
    .agg(
        F.count("*").alias("qtd_recargas_m1"),
        F.sum("val_real").alias("sum_val_real_m1"),
        F.avg("val_real").alias("avg_val_real_m1"),
        F.min("val_real").alias("min_val_real_m1"),
        F.max("val_real").alias("max_val_real_m1"),
        F.stddev("val_real").alias("std_val_real_m1"),
        F.sum("flag_sos").alias("qtd_sos_m1"),
        F.sum("valor_sos").alias("sum_valor_sos_m1"),
    )
    # Calcular features derivadas
    .withColumn(
        "freq_sos_m1",
        F.round(F.col("qtd_sos_m1") / F.col("qtd_recargas_m1"), 3)
    )
    .withColumn(
        "coef_variacao_val_m1",
        F.round(F.col("std_val_real_m1") / F.col("avg_val_real_m1"), 3)
    )
    .withColumn(
        "flag_teve_sos_m1",
        F.when(F.col("qtd_sos_m1") > 0, 1).otherwise(0)
    )
    .withColumn(
        "ticket_medio_m1",
        F.round(F.col("avg_val_real_m1"), 2)
    )
)

# ── 3. Exibir resultados ──────────────────────────────────────────────────────
print(f"\n📊 Features geradas para safra de referencia: {SAFRA_REFERENCIA}")
print(f"   (usando dados de recargas anteriores a {SAFRA_REFERENCIA})\n")

df_features.select(
    "num_cpf",
    "qtd_recargas_m1",
    "sum_val_real_m1",
    "ticket_medio_m1",
    "qtd_sos_m1",
    "freq_sos_m1",
    "coef_variacao_val_m1",
    "flag_teve_sos_m1",
).show(truncate=False)

# ── 4. Validacoes automaticas ─────────────────────────────────────────────────
print("🔍 Validando regras de negocio...\n")

resultados = df_features.collect()
resultados_dict = {r["num_cpf"]: r for r in resultados}

# Regra 1: CPF_002 deve ter frequencia SOS alta (usou 4x SOS em 4 recargas)
assert resultados_dict["CPF_002"]["freq_sos_m1"] == 1.0, \
    "ERRO: CPF_002 deveria ter freq_sos_m1 = 1.0"
print("  ✅ PASSOU — CPF_002: freq_sos_m1 = 1.0 (100%% das recargas tinham SOS)")

# Regra 2: CPF_001 nao deve ter SOS
assert resultados_dict["CPF_001"]["flag_teve_sos_m1"] == 0, \
    "ERRO: CPF_001 nao deveria ter SOS"
print("  ✅ PASSOU — CPF_001: flag_teve_sos_m1 = 0 (sem SOS)")

# Regra 3: CPF_003 deve ter menor quantidade de recargas
assert resultados_dict["CPF_003"]["qtd_recargas_m1"] < resultados_dict["CPF_001"]["qtd_recargas_m1"], \
    "ERRO: CPF_003 deveria ter menos recargas que CPF_001"
print("  ✅ PASSOU — CPF_003: menos recargas que CPF_001 (confirmado)")

# Regra 4: Contagem total de CPFs processados
total = df_features.count()
assert total == 3, f"ERRO: Esperava 3 CPFs, got {total}"
print(f"  ✅ PASSOU — Total de CPFs processados: {total}")

print("\n" + "="*70)
print("  ✅ TODOS OS TESTES PASSARAM!")
print(f"  PySpark versao: {spark.version}")
print("="*70 + "\n")

spark.stop()
