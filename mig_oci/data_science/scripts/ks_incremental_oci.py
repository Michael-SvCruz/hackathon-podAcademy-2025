#!/usr/bin/env -S python3.11 -u
"""
ks_incremental_oci.py
=====================
Treina LightGBM para cada ABT (v1→v6) e calcula KS OOT incremental.

Objetivo: obter valores EXATOS de KS por bloco de features, todos no
mesmo ambiente (OCI VM), para a apresentação final na banca.

Cada ABT é lida do bucket gold-layer, filtrada (flag_instalacao=1,
safras válidas), e um modelo LightGBM independente é treinado.

Resultado salvo em JSON no bucket models: metricas/ks_incremental_<timestamp>.json

Autenticação:
    - VM OCI: Instance Principal (automático via Dynamic Group)
    - Local/dev: ~/.oci/config (flag --local)

Uso:
    # Na VM OCI (Instance Principal)
    python3.11 /opt/modelo-fpd/ks_incremental_oci.py

    # Local (debug)
    python3.11 ks_incremental_oci.py --local
"""

import argparse
import gc
import io
import json
import sys
from datetime import datetime

import numpy as np
import pandas as pd


# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

TARGET = "fpd_int"
ID_COLS = ["num_cpf", "safra"]

# Colunas que NUNCA devem ser features (presentes em todas as ABTs)
META_COLS_BASE = [
    "num_cpf", "safra", "dt_safra",
    "fpd_int", "flag_instalacao_int",
    "prod", "flag_mig2",
]

# Colunas de metadados adicionais (podem variar por versão)
META_COLS_EXTRA = [
    "abt_version", "build_date",
    "spine_version", "gold_version", "gold_build_date",
]

META_COLS = META_COLS_BASE + META_COLS_EXTRA

SAFRAS_TRAIN = ["202410", "202411", "202412"]
SAFRAS_OOT = ["202502", "202503"]

IV_THRESHOLD = 0.01
SEED = 42

BUCKET_GOLD = "hackathon-2025-gold-layer"
BUCKET_MODELS = "hackathon-2025-models"

# ABTs em ordem incremental (ordem obrigatória de apresentação)
ABTS = [
    {"version": "v1", "prefix": "abt_v1/",    "label": "Score_01",              "block": "Score_01"},
    {"version": "v2", "prefix": "abt_v2/",    "label": "+ Score_02",            "block": "Score_02"},
    {"version": "v3", "prefix": "abt_v3/",    "label": "+ Telco",              "block": "Telco"},
    {"version": "v4", "prefix": "abt_v4/",    "label": "+ Cadastro",           "block": "Cadastro"},
    {"version": "v5", "prefix": "abt_v5/",    "label": "+ Recarga",            "block": "Recarga"},
    {"version": "v6", "prefix": "abt_v6_v2/", "label": "+ Pagamento + Atraso", "block": "Pagamento+Atraso"},
]


# ============================================================================
# FUNÇÕES — OCI
# ============================================================================

def get_oci_clients(local: bool = False):
    """Retorna (ObjectStorageClient, namespace)."""
    import oci

    if local:
        config = oci.config.from_file()
        client = oci.object_storage.ObjectStorageClient(config)
    else:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.object_storage.ObjectStorageClient({}, signer=signer)

    namespace = client.get_namespace().data
    return client, namespace


def list_parquet_objects(client, namespace: str, bucket: str, prefix: str) -> list:
    """Lista todos os objetos .parquet em um prefix do bucket."""
    objects = []
    next_start = None

    while True:
        kwargs = {
            "namespace_name": namespace,
            "bucket_name": bucket,
            "prefix": prefix,
            "fields": "name,size",
        }
        if next_start:
            kwargs["start"] = next_start

        response = client.list_objects(**kwargs)

        for obj in response.data.objects:
            if obj.name.endswith(".parquet"):
                objects.append(obj.name)

        next_start = response.data.next_start_with
        if not next_start:
            break

    return objects


def read_parquet_from_oci(
    client, namespace: str, bucket: str, prefix: str,
    row_filter=None,
    dedup_cols=None,
) -> pd.DataFrame:
    """Lê arquivos .parquet via OCI SDK com filtro e dedup incremental.

    Mesmo padrão do modelo_qualificacao.py — dedup a cada 40 arquivos
    para evitar OOM por parquets duplicados de re-execução Delta.
    """
    parquet_keys = list_parquet_objects(client, namespace, bucket, prefix)

    if not parquet_keys:
        raise FileNotFoundError(
            f"Nenhum arquivo .parquet encontrado em {bucket}/{prefix}"
        )

    print(f"    Encontrados {len(parquet_keys)} arquivos .parquet", flush=True)

    DEDUP_INTERVAL = 40
    dfs = []
    rows_total = 0
    rows_kept = 0

    for i, key in enumerate(parquet_keys):
        response = client.get_object(namespace, bucket, key)
        buf = io.BytesIO(response.data.content)
        df_part = pd.read_parquet(buf)
        rows_total += len(df_part)

        if row_filter is not None:
            df_part = row_filter(df_part)

        # Downcast para economizar memória
        float64_cols = df_part.select_dtypes(include=["float64"]).columns
        if len(float64_cols) > 0:
            df_part[float64_cols] = df_part[float64_cols].astype(np.float32)
        int64_cols = df_part.select_dtypes(include=["int64"]).columns
        if len(int64_cols) > 0:
            df_part[int64_cols] = df_part[int64_cols].astype(np.int32)

        rows_kept += len(df_part)
        dfs.append(df_part)

        # Dedup incremental
        if dedup_cols and (i + 1) % DEDUP_INTERVAL == 0 and len(dfs) > 1:
            combined = pd.concat(dfs, ignore_index=True)
            n_before = len(combined)
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
            rows_kept -= (n_before - len(combined))
            dfs = [combined]
            gc.collect()

        if (i + 1) % 20 == 0 or (i + 1) == len(parquet_keys):
            print(f"      Lidos {i + 1}/{len(parquet_keys)} ({rows_kept:,} registros)", flush=True)

    result = pd.concat(dfs, ignore_index=True)
    del dfs

    if dedup_cols:
        n_before = len(result)
        result = result.drop_duplicates(subset=dedup_cols, keep="first")
        n_removed = n_before - len(result)
        if n_removed > 0:
            print(f"      Dedup: {n_removed:,} duplicatas removidas", flush=True)

    return result


# ============================================================================
# FUNÇÕES — Métricas
# ============================================================================

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


# ============================================================================
# TREINAR MODELO PARA UMA ABT
# ============================================================================

def treinar_e_avaliar(df, abt_info):
    """Treina LightGBM para uma ABT e retorna métricas.

    Retorna dict com KS, AUC, GINI, n_features, etc.
    """
    import lightgbm as lgb

    version = abt_info["version"]
    label = abt_info["label"]

    df["safra"] = df["safra"].astype(str)

    # Split temporal
    df_train = df[df["safra"].isin(SAFRAS_TRAIN)].copy()
    df_oot = df[df["safra"].isin(SAFRAS_OOT)].copy()

    print(f"    Train: {len(df_train):,} | OOT: {len(df_oot):,}", flush=True)

    if len(df_train) < 1000 or len(df_oot) < 1000:
        print(f"    AVISO: dados insuficientes para {version}, pulando.", flush=True)
        return None

    # Seleção de features por IV
    # Inclui META_COLS conhecidas + qualquer coluna não-numérica
    feature_cols = [
        c for c in df_train.columns
        if c not in META_COLS
        and c not in ID_COLS
        and df_train[c].dtype in ["int64", "float64", "int32", "float32"]
    ]

    iv_results = {}
    for col in feature_cols:
        cob = df_train[col].notna().mean()
        iv = calcular_iv(df_train, col, TARGET) if cob > 0.01 else 0.0
        iv_results[col] = iv

    features_selected = [f for f, iv in iv_results.items() if iv >= IV_THRESHOLD]
    features_selected.sort(key=lambda f: iv_results[f], reverse=True)

    print(f"    Features candidatas: {len(feature_cols)} | Selecionadas (IV>={IV_THRESHOLD}): {len(features_selected)}", flush=True)

    if len(features_selected) < 1:
        print(f"    AVISO: nenhuma feature com IV >= {IV_THRESHOLD} para {version}", flush=True)
        return None

    X_train = df_train[features_selected].fillna(-999)
    y_train = df_train[TARGET]
    X_oot = df_oot[features_selected].fillna(-999)
    y_oot = df_oot[TARGET]

    # Treinar LightGBM (mesmos hiperparâmetros do modelo final)
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
            lgb.log_evaluation(period=200),
        ],
    )

    best_iter = getattr(model, "best_iteration", None)
    y_train_proba = model.predict(X_train, num_iteration=best_iter)
    y_oot_proba = model.predict(X_oot, num_iteration=best_iter)

    ks_train, _ = calcular_ks(y_train, y_train_proba)
    ks_oot, ks_decil = calcular_ks(y_oot, y_oot_proba)
    gini_train, auc_train = calcular_gini(y_train, y_train_proba)
    gini_oot, auc_oot = calcular_gini(y_oot, y_oot_proba)

    result = {
        "version": version,
        "label": label,
        "block": abt_info["block"],
        "prefix": abt_info["prefix"],
        "n_features_total": len(feature_cols),
        "n_features_selected": len(features_selected),
        "n_train": len(df_train),
        "n_oot": len(df_oot),
        "best_iteration": best_iter,
        "ks_train": float(round(ks_train, 6)),
        "ks_oot": float(round(ks_oot, 6)),
        "auc_train": float(round(auc_train, 6)),
        "auc_oot": float(round(auc_oot, 6)),
        "gini_train": float(round(gini_train, 6)),
        "gini_oot": float(round(gini_oot, 6)),
        "top_5_features": features_selected[:5],
    }

    print(f"    >>> KS OOT = {ks_oot*100:.2f}% | AUC = {auc_oot:.4f} | Features = {len(features_selected)} | Iterações = {best_iter}", flush=True)

    # Liberar memória
    del model, X_train, X_oot, y_train, y_oot, df_train, df_oot
    del train_data, valid_data
    gc.collect()

    return result


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="KS Incremental por ABT — OCI VM")
    parser.add_argument("--local", action="store_true", help="Usar ~/.oci/config")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    print("=" * 70, flush=True)
    print("KS INCREMENTAL POR ABT — OCI VM (LightGBM)", flush=True)
    print(f"Timestamp: {timestamp}", flush=True)
    print(f"Local mode: {args.local}", flush=True)
    print(f"ABTs: {[a['version'] for a in ABTS]}", flush=True)
    print(f"Train safras: {SAFRAS_TRAIN} | OOT safras: {SAFRAS_OOT}", flush=True)
    print(f"IV threshold: {IV_THRESHOLD} | Seed: {SEED}", flush=True)
    print("=" * 70, flush=True)

    oci_client, namespace = get_oci_clients(local=args.local)
    print(f"\nNamespace OCI: {namespace}", flush=True)

    valid_safras = set(SAFRAS_TRAIN + SAFRAS_OOT)

    results = []

    for i, abt in enumerate(ABTS):
        print(f"\n{'='*70}", flush=True)
        print(f"[{i+1}/{len(ABTS)}] ABT {abt['version']} — {abt['label']}", flush=True)
        print(f"{'='*70}", flush=True)

        print(f"  Lendo: {BUCKET_GOLD}/{abt['prefix']}", flush=True)

        try:
            df = read_parquet_from_oci(
                oci_client, namespace, BUCKET_GOLD, abt["prefix"],
                row_filter=lambda chunk: chunk[
                    (chunk["flag_instalacao_int"] == 1) &
                    (chunk["safra"].astype(str).isin(valid_safras))
                ],
                dedup_cols=ID_COLS,
            )

            print(f"  Registros: {len(df):,} | Colunas: {len(df.columns)}", flush=True)
            mem_mb = df.memory_usage(deep=True).sum() / 1024**2
            print(f"  Memória: {mem_mb:.0f} MB", flush=True)

            result = treinar_e_avaliar(df, abt)
            if result:
                results.append(result)

            del df
            gc.collect()

        except Exception as e:
            print(f"  ERRO ao processar {abt['version']}: {e}", flush=True)
            results.append({
                "version": abt["version"],
                "label": abt["label"],
                "block": abt["block"],
                "error": str(e),
            })

    # ------------------------------------------------------------------
    # Resumo e salvamento
    # ------------------------------------------------------------------
    print("\n" + "=" * 70, flush=True)
    print("RESUMO — KS INCREMENTAL OCI VM", flush=True)
    print("=" * 70, flush=True)
    print(f"{'ABT':<6} {'Bloco':<25} {'Features':<10} {'KS OOT':>10} {'Delta':>10}", flush=True)
    print("-" * 70, flush=True)

    prev_ks = 0
    for r in results:
        if "error" in r:
            print(f"{r['version']:<6} {r['label']:<25} {'ERRO':<10} {'—':>10} {'—':>10}", flush=True)
            continue

        ks = r["ks_oot"] * 100
        delta = ks - prev_ks
        delta_str = "baseline" if prev_ks == 0 else f"+{delta:.2f}"

        print(f"{r['version']:<6} {r['label']:<25} {r['n_features_selected']:<10} {ks:>9.2f}% {delta_str:>10}", flush=True)
        prev_ks = ks

    print("-" * 70, flush=True)
    print(f"Benchmark Claro: 33.10%", flush=True)
    if results and "error" not in results[-1]:
        final_ks = results[-1]["ks_oot"] * 100
        gap = final_ks - 33.10
        status = "ACIMA" if gap >= 0 else "ABAIXO"
        print(f"Resultado Final: {final_ks:.2f}% ({status} do benchmark por {abs(gap):.2f} p.p.)", flush=True)

    # Salvar JSON no bucket models
    output = {
        "timestamp": timestamp,
        "environment": "oci_vm",
        "iv_threshold": IV_THRESHOLD,
        "seed": SEED,
        "safras_train": SAFRAS_TRAIN,
        "safras_oot": SAFRAS_OOT,
        "benchmark": 33.10,
        "results": results,
    }

    output_json = json.dumps(output, indent=2, ensure_ascii=False)
    output_key = f"metricas/ks_incremental_{timestamp}.json"

    try:
        oci_client.put_object(
            namespace, BUCKET_MODELS, output_key,
            output_json.encode("utf-8"),
        )
        print(f"\nResultados salvos em: {BUCKET_MODELS}/{output_key}", flush=True)
    except Exception as e:
        print(f"\nAVISO: não foi possível salvar no bucket ({e})", flush=True)
        # Salvar localmente como fallback
        local_path = f"/tmp/ks_incremental_{timestamp}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(f"Salvo localmente: {local_path}", flush=True)

    # Imprimir JSON para captura nos logs do Airflow/SSH
    print(f"\n>>> [JSON_RESULT_START]", flush=True)
    print(output_json, flush=True)
    print(f">>> [JSON_RESULT_END]", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("CONCLUÍDO", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main()
