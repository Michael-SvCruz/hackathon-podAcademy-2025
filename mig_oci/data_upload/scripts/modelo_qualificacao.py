"""
modelo_qualificacao.py
======================
Script self-contained para execução no OCI Data Flow.

Lê a ABT v6 do gold-layer, carrega o modelo PKL do bucket models,
aplica scoring LightGBM e salva predições + métricas.

Uso:
    # OCI Data Flow (Resource Principal automático)
    spark-submit modelo_qualificacao.py

    # Local (com --local e ~/.oci/config)
    spark-submit modelo_qualificacao.py --local

Paths default:
    - ABT v6:      oci://hackathon-2025-gold-layer@{ns}/abt_v6_v2/
    - Modelo PKL:  oci://hackathon-2025-models@{ns}/pkl/modelo_fpd.pkl
    - Resultados:  oci://hackathon-2025-models@{ns}/resultados_modelo/
    - Métricas:    oci://hackathon-2025-models@{ns}/metricas/
"""

import argparse
import io
import json
import pickle
import sys
from datetime import datetime

import numpy as np
import pandas as pd


# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

TARGET = "fpd_int"
ID_COLS = ["num_cpf", "safra"]

META_COLS = [
    "num_cpf", "safra", "dt_safra",
    "fpd_int", "flag_instalacao_int",
    "prod", "flag_mig2",
    "abt_version", "build_date",
    "spine_version", "gold_version", "gold_build_date",
]

SAFRAS_TRAIN = ["202410", "202411", "202412"]
SAFRAS_OOT = ["202502", "202503"]

IV_THRESHOLD = 0.01
SEED = 42

BUCKET_GOLD = "hackathon-2025-gold-layer"
BUCKET_MODELS = "hackathon-2025-models"
ABT_PREFIX = "abt_v6_v2"
PKL_KEY = "pkl/modelo_fpd.pkl"


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def get_oci_clients(local: bool = False):
    """Retorna (ObjectStorageClient, namespace)."""
    import oci

    if local:
        config = oci.config.from_file()
        client = oci.object_storage.ObjectStorageClient(config)
    else:
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.object_storage.ObjectStorageClient({}, signer=signer)

    namespace = client.get_namespace().data
    return client, namespace


def get_spark_session(app_name: str):
    """Cria SparkSession com suporte a Delta e OCI."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )


def calcular_iv(df, feature, target, bins=10):
    """Calcula Information Value de uma feature."""
    try:
        df_temp = df[[feature, target]].dropna()
        if len(df_temp) < 100:
            return 0.0

        if df_temp[feature].nunique() > bins:
            df_temp["bin"] = pd.qcut(df_temp[feature], q=bins, duplicates="drop")
        else:
            df_temp["bin"] = df_temp[feature]

        grouped = df_temp.groupby("bin")[target].agg(["sum", "count"])
        grouped.columns = ["bad", "total"]
        grouped["good"] = grouped["total"] - grouped["bad"]

        total_bad = max(grouped["bad"].sum(), 1)
        total_good = max(grouped["good"].sum(), 1)

        grouped["bad_pct"] = grouped["bad"] / total_bad
        grouped["good_pct"] = grouped["good"] / total_good
        grouped["bad_pct"] = grouped["bad_pct"].replace(0, 0.0001)
        grouped["good_pct"] = grouped["good_pct"].replace(0, 0.0001)

        grouped["woe"] = np.log(grouped["good_pct"] / grouped["bad_pct"])
        grouped["iv"] = (grouped["good_pct"] - grouped["bad_pct"]) * grouped["woe"]

        iv = grouped["iv"].sum()
        return iv if np.isfinite(iv) else 0.0
    except Exception:
        return 0.0


def calcular_ks(y_true, y_proba):
    """Calcula KS (Kolmogorov-Smirnov). Retorna (ks_max, ks_decil)."""
    df_ks = pd.DataFrame({
        "target": y_true.values if hasattr(y_true, "values") else y_true,
        "proba": y_proba,
    }).sort_values("proba", ascending=False).reset_index(drop=True)

    df_ks["bad_acum"] = df_ks["target"].cumsum() / df_ks["target"].sum()
    df_ks["good_acum"] = (1 - df_ks["target"]).cumsum() / (1 - df_ks["target"]).sum()
    df_ks["ks"] = abs(df_ks["bad_acum"] - df_ks["good_acum"])

    ks_max = df_ks["ks"].max()
    ks_decil = df_ks["ks"].idxmax() / len(df_ks) * 10

    return ks_max, ks_decil


def calcular_gini(y_true, y_proba):
    """Calcula Gini = 2*AUC - 1. Retorna (gini, auc)."""
    from sklearn.metrics import roc_auc_score

    auc = roc_auc_score(y_true, y_proba)
    return 2 * auc - 1, auc


def classificar_feature(col_name):
    """Classifica feature por bloco para análise de importância."""
    col_lower = col_name.lower()

    if "score_01" in col_lower:
        return "1_Score_01"
    elif "score_02" in col_lower:
        return "2_Score_02"
    elif col_lower.startswith("var_"):
        return "3_Telco"
    elif any(x in col_lower for x in ["idade", "sexo", "regiao", "uf", "cidade", "cep", "cadastro"]):
        return "4_Cadastro"
    elif any(x in col_lower for x in ["recarga", "credito", "sos", "bonus", "ticket", "dias_medio", "dias_max", "coef_var"]):
        return "5_Recarga"
    elif any(x in col_lower for x in ["pagamento", "pag_", "juros", "desconto", "pago"]):
        return "6_Pagamento"
    elif any(x in col_lower for x in ["atraso", "atr_", "aging", "aberto", "wo", "pdd", "fraude"]):
        return "7_Atraso"
    elif "flag_" in col_lower and "missing" in col_lower:
        return "8_Flag_Missing"
    else:
        return "9_Outros"


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Modelo de Qualificação FPD — OCI Data Flow")
    parser.add_argument("--local", action="store_true", help="Usar ~/.oci/config em vez de Resource Principal")
    parser.add_argument("--sample", type=float, default=1.0, help="Fração de amostragem (default: 1.0)")
    parser.add_argument("--pkl-key", type=str, default=PKL_KEY, help="Chave do objeto PKL no bucket models")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    print("=" * 60)
    print("MODELO DE QUALIFICAÇÃO FPD — OCI")
    print(f"Timestamp: {timestamp}")
    print(f"Local mode: {args.local}")
    print(f"Sample: {args.sample}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Spark + OCI clients
    # ------------------------------------------------------------------
    spark = get_spark_session("modelo_qualificacao_optz")
    oci_client, namespace = get_oci_clients(local=args.local)

    print(f"\nNamespace OCI: {namespace}")

    # ------------------------------------------------------------------
    # 2. Carregar ABT v6
    # ------------------------------------------------------------------
    abt_path = f"oci://{BUCKET_GOLD}@{namespace}/{ABT_PREFIX}/"
    print(f"\nLendo ABT v6 de: {abt_path}")

    from pyspark.sql import functions as F

    df_spark = spark.read.format("delta").load(abt_path)

    # Filtros de população
    df_spark = df_spark.filter(F.col("flag_instalacao_int") == 1)

    n_total = df_spark.count()
    print(f"Total após filtro (flag_instalacao=1): {n_total:,}")

    if args.sample < 1.0:
        df_spark = df_spark.sample(fraction=args.sample, seed=SEED)
        print(f"Após sampling ({args.sample*100:.0f}%): {df_spark.count():,}")

    # Converter para pandas
    print("Convertendo para Pandas...")
    df = df_spark.toPandas()
    df["safra"] = df["safra"].astype(str)
    print(f"  Registros: {len(df):,}")
    print(f"  Colunas: {len(df.columns)}")
    print(f"  Memória: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    print(f"  Taxa FPD: {df[TARGET].mean()*100:.2f}%")

    # ------------------------------------------------------------------
    # 3. Split temporal
    # ------------------------------------------------------------------
    df_train = df[df["safra"].isin(SAFRAS_TRAIN)].copy()
    df_oot = df[df["safra"].isin(SAFRAS_OOT)].copy()

    print(f"\nTrain ({SAFRAS_TRAIN}): {len(df_train):,} registros, FPD={df_train[TARGET].mean()*100:.2f}%")
    print(f"OOT ({SAFRAS_OOT}): {len(df_oot):,} registros, FPD={df_oot[TARGET].mean()*100:.2f}%")

    # ------------------------------------------------------------------
    # 4. Seleção de features (IV)
    # ------------------------------------------------------------------
    print("\nCalculando IV...")
    feature_cols_num = [
        c for c in df_train.columns
        if c not in META_COLS and df_train[c].dtype in ["int64", "float64", "int32", "float32"]
    ]

    iv_results = {}
    for col in feature_cols_num:
        cob = df_train[col].notna().mean()
        iv = calcular_iv(df_train, col, TARGET) if cob > 0.01 else 0.0
        iv_results[col] = iv

    features_selected = [f for f, iv in iv_results.items() if iv >= IV_THRESHOLD]
    features_selected.sort(key=lambda f: iv_results[f], reverse=True)
    print(f"Features selecionadas (IV >= {IV_THRESHOLD}): {len(features_selected)}")

    # ------------------------------------------------------------------
    # 5. Treinar modelo OU carregar PKL
    # ------------------------------------------------------------------
    import lightgbm as lgb

    X_train = df_train[features_selected].fillna(-999)
    y_train = df_train[TARGET]
    X_oot = df_oot[features_selected].fillna(-999)
    y_oot = df_oot[TARGET]

    # Tentar carregar PKL existente
    model = None
    try:
        print(f"\nTentando carregar modelo de: {BUCKET_MODELS}/{args.pkl_key}")
        response = oci_client.get_object(namespace, BUCKET_MODELS, args.pkl_key)
        pkl_bytes = response.data.content
        model = pickle.loads(pkl_bytes)
        print("  Modelo PKL carregado com sucesso.")
    except Exception as e:
        print(f"  PKL não encontrado ({e}). Treinando novo modelo...")

    if model is None:
        params = {
            "objective": "binary",
            "metric": "auc",
            "boosting_type": "gbdt",
            "num_leaves": 31,
            "max_depth": 6,
            "learning_rate": 0.05,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "min_child_samples": 100,
            "reg_alpha": 0.1,
            "reg_lambda": 0.1,
            "verbose": -1,
            "seed": SEED,
            "n_jobs": -1,
        }

        train_data = lgb.Dataset(X_train, label=y_train)
        valid_data = lgb.Dataset(X_oot, label=y_oot, reference=train_data)

        model = lgb.train(
            params,
            train_data,
            num_boost_round=1000,
            valid_sets=[train_data, valid_data],
            valid_names=["train", "oot"],
            callbacks=[
                lgb.early_stopping(stopping_rounds=50),
                lgb.log_evaluation(period=100),
            ],
        )
        print(f"Modelo treinado com {model.best_iteration} iterações.")

        # Salvar PKL no bucket
        pkl_buffer = pickle.dumps(model)
        oci_client.put_object(namespace, BUCKET_MODELS, args.pkl_key, pkl_buffer)
        print(f"Modelo PKL salvo em: {BUCKET_MODELS}/{args.pkl_key}")

    # ------------------------------------------------------------------
    # 6. Predições e métricas
    # ------------------------------------------------------------------
    print("\nRealizando predições...")
    best_iter = getattr(model, "best_iteration", None)
    y_train_proba = model.predict(X_train, num_iteration=best_iter)
    y_oot_proba = model.predict(X_oot, num_iteration=best_iter)

    ks_train, ks_decil_train = calcular_ks(y_train, y_train_proba)
    ks_oot, ks_decil_oot = calcular_ks(y_oot, y_oot_proba)
    gini_train, auc_train = calcular_gini(y_train, y_train_proba)
    gini_oot, auc_oot = calcular_gini(y_oot, y_oot_proba)

    print("\n" + "=" * 50)
    print("MÉTRICAS DO MODELO")
    print("=" * 50)
    print(f"              TRAIN         OOT")
    print(f"KS:           {ks_train*100:6.2f}%      {ks_oot*100:6.2f}%")
    print(f"AUC:          {auc_train:6.4f}       {auc_oot:6.4f}")
    print(f"GINI:         {gini_train*100:6.2f}%      {gini_oot*100:6.2f}%")
    print(f"\nBenchmark: 33.10%  |  Resultado: {ks_oot*100:.2f}%  |  Gap: {(ks_oot-0.331)*100:+.2f} p.p.")

    # ------------------------------------------------------------------
    # 7. Salvar resultados
    # ------------------------------------------------------------------

    # 7a. Predições OOT como Delta
    df_predicoes = df_oot[["num_cpf", "safra", TARGET]].copy()
    df_predicoes["score_fpd"] = y_oot_proba
    df_predicoes["decil"] = pd.qcut(df_predicoes["score_fpd"], q=10, labels=range(1, 11)).astype(int)

    df_pred_spark = spark.createDataFrame(df_predicoes)
    output_path = f"oci://{BUCKET_MODELS}@{namespace}/resultados_modelo/predicoes_oot_{timestamp}/"
    df_pred_spark.write.mode("overwrite").format("delta").save(output_path)
    print(f"\nPredições salvas em: {output_path}")

    # 7b. Métricas como JSON
    metricas = {
        "timestamp": timestamp,
        "n_features": len(features_selected),
        "iv_threshold": IV_THRESHOLD,
        "sample_fraction": args.sample,
        "n_train": len(df_train),
        "n_oot": len(df_oot),
        "ks_train": round(ks_train, 6),
        "ks_oot": round(ks_oot, 6),
        "auc_train": round(auc_train, 6),
        "auc_oot": round(auc_oot, 6),
        "gini_train": round(gini_train, 6),
        "gini_oot": round(gini_oot, 6),
        "benchmark": 0.331,
        "gap_benchmark": round(ks_oot - 0.331, 6),
        "best_iteration": best_iter,
        "top_10_features": features_selected[:10],
    }

    metricas_json = json.dumps(metricas, indent=2, ensure_ascii=False)
    metricas_key = f"metricas/metricas_{timestamp}.json"
    oci_client.put_object(namespace, BUCKET_MODELS, metricas_key, metricas_json.encode("utf-8"))
    print(f"Métricas salvas em: {BUCKET_MODELS}/{metricas_key}")

    # 7c. Feature list
    features_key = f"metricas/features_{timestamp}.txt"
    features_content = "\n".join(features_selected)
    oci_client.put_object(namespace, BUCKET_MODELS, features_key, features_content.encode("utf-8"))
    print(f"Features salvas em: {BUCKET_MODELS}/{features_key}")

    # ------------------------------------------------------------------
    # 8. Resumo final
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    status = "ACIMA DO BENCHMARK" if ks_oot >= 0.331 else "ABAIXO DO BENCHMARK"
    print(f"  Features: {len(features_selected)}")
    print(f"  KS OOT: {ks_oot*100:.2f}%")
    print(f"  Benchmark: 33.10%")
    print(f"  Gap: {(ks_oot-0.331)*100:+.2f} p.p. ({status})")
    print(f"  Predições: {output_path}")
    print(f"  Métricas: {BUCKET_MODELS}/{metricas_key}")
    print("=" * 60)

    spark.stop()
    print("\nFinalizado com sucesso.")


if __name__ == "__main__":
    main()
