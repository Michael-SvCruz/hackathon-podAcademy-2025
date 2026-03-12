"""
compactar_gold.py — Compactacao de small files na Gold Layer
=============================================================
Executa OPTIMIZE (compactacao) + VACUUM nas tabelas Delta da Gold Layer,
consolidando small files em arquivos de ~64MB (features) ou ~64MB (ABTs).

Uso no OCI Data Flow:
    spark-submit compactar_gold.py

Uso local (debug):
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 compactar_gold.py

O que faz:
    1. Gold Features (recarga, pagamento, atraso):
       - Le a tabela particionada
       - Reescreve cada particao com coalesce para atingir ~64-128MB/arquivo
       - VACUUM para remover orfaos

    2. ABTs (v3, v4, v5, v6):
       - Le a tabela
       - Reescreve com coalesce para atingir ~64MB/arquivo
       - VACUUM para remover orfaos

    3. ABTs v1 e v2 nao sao compactadas (2-3 arquivos, irrelevante)

NAO modifica os scripts originais em scripts/.
=============================================================
"""

import sys
import time
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURACAO
# ============================================================================

# Namespace OCI — sera preenchido pelo argumento ou detectado
DEFAULT_NAMESPACE = "<<NAMESPACE>>"

# Target file size em bytes
TARGET_FILE_SIZE_64MB = 64 * 1024 * 1024    # 64 MB
TARGET_FILE_SIZE_128MB = 128 * 1024 * 1024  # 128 MB

# Tabelas Gold Features (particionadas por safra_*)
GOLD_FEATURES = [
    {
        "name": "gold_recarga_features",
        "path_suffix": "recarga_features_v2",
        "partition_col": "safra_recarga",
        "target_file_size": TARGET_FILE_SIZE_128MB,
    },
    {
        "name": "gold_pagamento_features",
        "path_suffix": "pagamento_features_v2",
        "partition_col": "safra_pagamento",
        "target_file_size": TARGET_FILE_SIZE_128MB,
    },
    {
        "name": "gold_atraso_features",
        "path_suffix": "atraso_features_v2",
        "partition_col": "safra_atraso",
        "target_file_size": TARGET_FILE_SIZE_128MB,
    },
]

# Tabelas ABT (sem particionamento)
ABTS = [
    {"name": "abt_v3", "path_suffix": "abt_v3", "target_file_size": TARGET_FILE_SIZE_64MB},
    {"name": "abt_v4", "path_suffix": "abt_v4", "target_file_size": TARGET_FILE_SIZE_64MB},
    {"name": "abt_v5", "path_suffix": "abt_v5", "target_file_size": TARGET_FILE_SIZE_64MB},
    {"name": "abt_v6", "path_suffix": "abt_v6_v2", "target_file_size": TARGET_FILE_SIZE_64MB},
]


# ============================================================================
# FUNCOES UTILITARIAS
# ============================================================================

def get_spark():
    """Cria SparkSession com suporte a Delta Lake."""
    builder = SparkSession.builder \
        .appName("compactar_gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalogExtension") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    return builder.getOrCreate()


def estimar_num_arquivos(spark, path, target_size_bytes):
    """Estima quantos arquivos sao necessarios para atingir o target size."""
    try:
        # Ler tamanho total via Hadoop FileSystem
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs_class = sc._jvm.org.apache.hadoop.fs.FileSystem
        uri = sc._jvm.java.net.URI(path)
        fs = fs_class.get(uri, hadoop_conf)
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)

        # Somar tamanho de todos os .parquet
        total_bytes = 0
        file_count = 0
        iterator = fs.listFiles(hadoop_path, True)  # recursivo
        while iterator.hasNext():
            file_status = iterator.next()
            name = file_status.getPath().getName()
            if name.endswith(".parquet") and not name.startswith("_"):
                total_bytes += file_status.getLen()
                file_count += 1

        if total_bytes == 0:
            return 1, 0, 0

        num_arquivos = max(1, int(total_bytes / target_size_bytes))
        return num_arquivos, file_count, total_bytes

    except Exception as e:
        print(f"    [AVISO] Nao foi possivel estimar tamanho via HDFS: {e}")
        return 4, 0, 0  # fallback conservador


def compactar_tabela_particionada(spark, table_config, base_path):
    """Compacta tabela particionada (Gold Features)."""
    name = table_config["name"]
    path = f"{base_path}/{table_config['path_suffix']}/"
    partition_col = table_config["partition_col"]
    target_size = table_config["target_file_size"]

    print(f"\n{'='*60}")
    print(f"  COMPACTANDO: {name} (particionada por {partition_col})")
    print(f"  Path: {path}")
    print(f"  Target: {target_size // (1024*1024)} MB/arquivo")
    print(f"{'='*60}")

    t0 = time.time()

    try:
        # Ler tabela atual
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"  Registros: {count:,}")

        # Descobrir particoes existentes
        partitions = df.select(partition_col).distinct().collect()
        n_partitions = len(partitions)
        print(f"  Particoes ({partition_col}): {n_partitions}")

        # Estimar arquivos por particao
        # Total de dados / n_partitions / target_size = arquivos por particao
        _, old_files, total_bytes = estimar_num_arquivos(spark, path, target_size)
        bytes_per_partition = total_bytes / max(n_partitions, 1)
        files_per_partition = max(1, int(bytes_per_partition / target_size))

        # Total de arquivos novos
        total_new_files = files_per_partition * n_partitions
        print(f"  Arquivos atuais: {old_files}")
        print(f"  Tamanho total: {total_bytes / (1024*1024):.1f} MB")
        print(f"  Arquivos por particao (alvo): {files_per_partition}")
        print(f"  Total arquivos novos (estimado): {total_new_files}")

        # Reparticionar: N particoes de safra * files_per_partition
        # Usamos repartition para distribuir por safra + coalesce interno
        print(f"  Reescrevendo com repartition({n_partitions * files_per_partition}, '{partition_col}')...")

        df.repartition(n_partitions * files_per_partition, partition_col) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(partition_col) \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(path)

        print(f"  Escrita concluida.")

        # VACUUM
        print(f"  Executando VACUUM...")
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.vacuum(retentionHours=0)
        print(f"  VACUUM concluido.")

        # Verificar resultado
        _, new_files, new_bytes = estimar_num_arquivos(spark, path, target_size)
        elapsed = time.time() - t0
        print(f"  Resultado: {old_files} -> {new_files} arquivos")
        print(f"  Tamanho: {new_bytes / (1024*1024):.1f} MB")
        print(f"  Tempo: {elapsed:.0f}s")
        return True

    except Exception as e:
        print(f"  [ERRO] Falha ao compactar {name}: {e}")
        return False


def compactar_tabela_flat(spark, table_config, base_path):
    """Compacta tabela nao-particionada (ABT)."""
    name = table_config["name"]
    path = f"{base_path}/{table_config['path_suffix']}/"
    target_size = table_config["target_file_size"]

    print(f"\n{'='*60}")
    print(f"  COMPACTANDO: {name} (sem particao)")
    print(f"  Path: {path}")
    print(f"  Target: {target_size // (1024*1024)} MB/arquivo")
    print(f"{'='*60}")

    t0 = time.time()

    try:
        # Estimar numero ideal de arquivos
        num_files, old_files, total_bytes = estimar_num_arquivos(spark, path, target_size)
        print(f"  Arquivos atuais: {old_files}")
        print(f"  Tamanho total: {total_bytes / (1024*1024):.1f} MB")
        print(f"  Arquivos alvo: {num_files}")

        if old_files <= num_files + 2:
            print(f"  Ja esta otimizado ({old_files} arquivos). Pulando.")
            return True

        # Ler, coalesce, reescrever
        df = spark.read.format("delta").load(path)
        count = df.count()
        print(f"  Registros: {count:,}")
        print(f"  Reescrevendo com coalesce({num_files})...")

        df.coalesce(num_files) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(path)

        print(f"  Escrita concluida.")

        # VACUUM
        print(f"  Executando VACUUM...")
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, path)
        delta_table.vacuum(retentionHours=0)
        print(f"  VACUUM concluido.")

        # Verificar resultado
        _, new_files, new_bytes = estimar_num_arquivos(spark, path, target_size)
        elapsed = time.time() - t0
        print(f"  Resultado: {old_files} -> {new_files} arquivos")
        print(f"  Tamanho: {new_bytes / (1024*1024):.1f} MB")
        print(f"  Tempo: {elapsed:.0f}s")
        return True

    except Exception as e:
        print(f"  [ERRO] Falha ao compactar {name}: {e}")
        return False


# ============================================================================
# MAIN
# ============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Compactar small files na Gold Layer")
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="Namespace do Object Storage OCI"
    )
    parser.add_argument(
        "--gold_bucket",
        default="hackathon-2025-gold-layer",
        help="Nome do bucket Gold"
    )
    parser.add_argument(
        "--only",
        default=None,
        help="Compactar apenas uma tabela (ex: abt_v6, gold_recarga_features)"
    )
    parser.add_argument(
        "--skip_features",
        action="store_true",
        help="Pular compactacao das Gold Features (recarga, pagamento, atraso)"
    )
    parser.add_argument(
        "--skip_abts",
        action="store_true",
        help="Pular compactacao dos ABTs (v3-v6)"
    )

    args = parser.parse_args()

    base_path = f"oci://{args.gold_bucket}@{args.namespace}"
    print(f"\n>>> Compactacao Gold Layer")
    print(f">>> Base path: {base_path}")
    print(f">>> Namespace: {args.namespace}")

    spark = get_spark()

    results = {}

    # Gold Features (particionadas)
    if not args.skip_features:
        for table in GOLD_FEATURES:
            if args.only and args.only != table["name"]:
                continue
            ok = compactar_tabela_particionada(spark, table, base_path)
            results[table["name"]] = ok
    else:
        print("\n>>> Pulando Gold Features (--skip_features)")

    # ABTs (flat)
    if not args.skip_abts:
        for table in ABTS:
            if args.only and args.only != table["name"]:
                continue
            ok = compactar_tabela_flat(spark, table, base_path)
            results[table["name"]] = ok
    else:
        print("\n>>> Pulando ABTs (--skip_abts)")

    # Resumo
    print(f"\n{'='*60}")
    print(f"  RESUMO DA COMPACTACAO")
    print(f"{'='*60}")
    for name, ok in results.items():
        status = "OK" if ok else "FALHOU"
        print(f"  {name:40s} [{status}]")

    n_ok = sum(1 for v in results.values() if v)
    n_total = len(results)
    print(f"\n  {n_ok}/{n_total} tabelas compactadas com sucesso.")

    spark.stop()

    if n_ok < n_total:
        sys.exit(1)


if __name__ == "__main__":
    main()
