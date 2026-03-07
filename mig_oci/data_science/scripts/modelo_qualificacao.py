#!/usr/bin/env -S python3.11 -u
"""
modelo_qualificacao.py
======================
Script single-node para scoring LightGBM — executa em VM dedicada OCI.

Lê a ABT v6 (parquet) do bucket gold-layer via OCI SDK,
carrega/treina modelo LightGBM e salva predições + métricas.

Autenticação:
    - VM OCI: Instance Principal (automático via Dynamic Group)
    - Local/dev: ~/.oci/config (flag --local)

Uso:
    # Na VM OCI (Instance Principal)
    python3.11 /opt/modelo-fpd/modelo_qualificacao.py

    # Local (debug)
    python3.11 modelo_qualificacao.py --local

Paths default:
    - ABT v6:      bucket hackathon-2025-gold-layer / abt_v6_v2/
    - Modelo PKL:  bucket hackathon-2025-models / pkl/modelo_fpd.pkl
    - Resultados:  bucket hackathon-2025-models / resultados_modelo/
    - Métricas:    bucket hackathon-2025-models / metricas/
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
# FUNÇÕES AUXILIARES — OCI Object Storage
# ============================================================================

def get_oci_clients(local: bool = False):
    """Retorna (ObjectStorageClient, namespace).

    VM OCI usa InstancePrincipalsSecurityTokenSigner (não Resource Principal).
    Resource Principal é exclusivo do Data Flow / Functions.
    """
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
    """Lê arquivos .parquet via OCI SDK, filtra por chunk e concatena.

    Filtrar durante a leitura é essencial para caber na memória da VM (32 GB):
    - ABT v6 completa pode chegar a ~24 GB em pandas (com duplicatas de re-execução)
    - Com flag_instalacao=1 + safras válidas + dedup = ~16 GB (~4.3M registros)

    O pipeline ABT escreve parquet em modo append — re-execuções criam arquivos
    duplicados. Com dedup_cols ativado, o dedup é feito incrementalmente a cada
    40 arquivos para evitar OOM (160 arquivos × 4.3M registros = 32+ GB sem dedup).

    Args:
        row_filter: função que recebe DataFrame e retorna DataFrame filtrado.
                    Aplicada a cada chunk ANTES de acumular.
        dedup_cols: lista de colunas para dedup incremental (ex: ["num_cpf", "safra"]).
                    Se None, não faz dedup (comportamento original).
    """
    parquet_keys = list_parquet_objects(client, namespace, bucket, prefix)

    if not parquet_keys:
        raise FileNotFoundError(
            f"Nenhum arquivo .parquet encontrado em {bucket}/{prefix}"
        )

    print(f"  Encontrados {len(parquet_keys)} arquivos .parquet", flush=True)

    # Intervalo de dedup incremental: a cada 40 arquivos (tamanho de 1 batch ABT)
    DEDUP_INTERVAL = 40

    dfs = []
    rows_total = 0
    rows_kept = 0
    rows_deduped = 0
    for i, key in enumerate(parquet_keys):
        response = client.get_object(namespace, bucket, key)
        buf = io.BytesIO(response.data.content)
        df_part = pd.read_parquet(buf)
        rows_total += len(df_part)

        # Filtrar antes de acumular (economia de memória)
        if row_filter is not None:
            df_part = row_filter(df_part)

        # Downcast float64→float32 e int64→int32 para caber na memória
        # LightGBM usa float32 internamente, não há perda de precisão
        float64_cols = df_part.select_dtypes(include=["float64"]).columns
        if len(float64_cols) > 0:
            df_part[float64_cols] = df_part[float64_cols].astype(np.float32)
        int64_cols = df_part.select_dtypes(include=["int64"]).columns
        if len(int64_cols) > 0:
            df_part[int64_cols] = df_part[int64_cols].astype(np.int32)

        rows_kept += len(df_part)
        dfs.append(df_part)

        # Dedup incremental: concat + dedup a cada DEDUP_INTERVAL arquivos
        # Isso limita o pico de memória a ~2x os dados únicos
        if dedup_cols and (i + 1) % DEDUP_INTERVAL == 0 and len(dfs) > 1:
            combined = pd.concat(dfs, ignore_index=True)
            n_before = len(combined)
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
            n_removed = n_before - len(combined)
            rows_deduped += n_removed
            rows_kept -= n_removed
            dfs = [combined]
            import gc; gc.collect()
            print(f"    [dedup parcial] removidos {n_removed:,} duplicatas, "
                  f"acumulado: {len(combined):,} registros únicos", flush=True)

        if (i + 1) % 10 == 0 or (i + 1) == len(parquet_keys):
            print(f"    Lidos {i + 1}/{len(parquet_keys)} arquivos "
                  f"({rows_kept:,} de {rows_total:,} registros mantidos)", flush=True)

    result = pd.concat(dfs, ignore_index=True)
    del dfs

    # Dedup final (captura duplicatas do último batch incompleto)
    if dedup_cols:
        n_before = len(result)
        result = result.drop_duplicates(subset=dedup_cols, keep="first")
        n_removed = n_before - len(result)
        rows_deduped += n_removed
        if rows_deduped > 0:
            import gc; gc.collect()
            print(f"    [dedup total] {rows_deduped:,} duplicatas removidas no total", flush=True)

    return result


def save_parquet_to_oci(client, namespace: str, bucket: str, key: str, df: pd.DataFrame):
    """Salva DataFrame como parquet no Object Storage."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    client.put_object(namespace, bucket, key, buf.getvalue())


# ============================================================================
# FUNÇÕES AUXILIARES — Métricas
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
    parser = argparse.ArgumentParser(description="Modelo de Qualificação FPD — VM OCI (pandas + LightGBM)")
    parser.add_argument("--local", action="store_true", help="Usar ~/.oci/config em vez de Instance Principal")
    parser.add_argument("--sample", type=float, default=1.0, help="Fração de amostragem (default: 1.0)")
    parser.add_argument("--pkl-key", type=str, default=PKL_KEY, help="Chave do objeto PKL no bucket models")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    print("=" * 60, flush=True)
    print("MODELO DE QUALIFICAÇÃO FPD — VM OCI (pandas + LightGBM)", flush=True)
    print(f"Timestamp: {timestamp}", flush=True)
    print(f"Local mode: {args.local}", flush=True)
    print(f"Sample: {args.sample}", flush=True)
    print("=" * 60, flush=True)

    # ------------------------------------------------------------------
    # 1. OCI clients (sem Spark)
    # ------------------------------------------------------------------
    oci_client, namespace = get_oci_clients(local=args.local)
    print(f"\nNamespace OCI: {namespace}", flush=True)

    # ------------------------------------------------------------------
    # 2. Carregar ABT v6 (parquet via OCI SDK — filtro durante leitura)
    # ------------------------------------------------------------------
    # Filtra flag_instalacao_int=1 E safras válidas em cada chunk.
    # Sem filtro: 3.7M+ × 614 cols ≈ 18-24 GB (com duplicatas de re-execução).
    # Com filtro + dedup: ~4.3M registros ≈ 16 GB → cabe em 32 GB.
    valid_safras = set(SAFRAS_TRAIN + SAFRAS_OOT)
    print(f"\nLendo ABT v6 de: {BUCKET_GOLD}/{ABT_PREFIX}/", flush=True)
    print(f"  Filtros: flag_instalacao=1, safras={sorted(valid_safras)}", flush=True)
    df = read_parquet_from_oci(
        oci_client, namespace, BUCKET_GOLD, f"{ABT_PREFIX}/",
        row_filter=lambda chunk: chunk[
            (chunk["flag_instalacao_int"] == 1) &
            (chunk["safra"].astype(str).isin(valid_safras))
        ],
        dedup_cols=ID_COLS,  # dedup incremental a cada 40 arquivos (evita OOM)
    )

    df["safra"] = df["safra"].astype(str)

    print(f"Total (flag_instalacao=1): {len(df):,}", flush=True)
    print(f"  Colunas: {len(df.columns)}", flush=True)
    print(f"  Memória: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB", flush=True)
    print(f"  Taxa FPD: {df[TARGET].mean()*100:.2f}%", flush=True)

    if args.sample < 1.0:
        df = df.sample(frac=args.sample, random_state=SEED)
        print(f"Após sampling ({args.sample*100:.0f}%): {len(df):,}")

    # ------------------------------------------------------------------
    # 3. Split temporal
    # ------------------------------------------------------------------
    df_train = df[df["safra"].isin(SAFRAS_TRAIN)].copy()
    df_oot = df[df["safra"].isin(SAFRAS_OOT)].copy()
    del df  # liberar ~8 GB (não é mais necessário após o split)

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
    y_train = df_train[TARGET].copy()
    X_oot = df_oot[features_selected].fillna(-999)
    y_oot = df_oot[TARGET].copy()

    # Guardar metadados antes de liberar os DataFrames completos
    n_train = len(df_train)
    n_oot = len(df_oot)
    df_oot_meta = df_oot[["num_cpf", "safra", TARGET]].copy()
    del df_train, df_oot  # liberar ~8.2 GB (614 cols → apenas 261 cols em X)
    import gc; gc.collect()
    print(f"  Memória após liberar DataFrames: X_train={X_train.memory_usage(deep=True).sum()/1024**2:.0f} MB, X_oot={X_oot.memory_usage(deep=True).sum()/1024**2:.0f} MB", flush=True)

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
    # 7. Salvar resultados (parquet + JSON via OCI SDK)
    # ------------------------------------------------------------------

    # 7a. Predições OOT como parquet
    df_predicoes = df_oot_meta.copy()
    df_predicoes["score_fpd"] = y_oot_proba
    df_predicoes["decil"] = pd.qcut(df_predicoes["score_fpd"], q=10, labels=range(1, 11)).astype(int)

    pred_key = f"resultados_modelo/predicoes_oot_{timestamp}.parquet"
    save_parquet_to_oci(oci_client, namespace, BUCKET_MODELS, pred_key, df_predicoes)
    print(f"\nPredições salvas em: {BUCKET_MODELS}/{pred_key}")

    # 7b. Métricas como JSON
    metricas = {
        "timestamp": timestamp,
        "n_features": len(features_selected),
        "iv_threshold": IV_THRESHOLD,
        "sample_fraction": args.sample,
        "n_train": n_train,
        "n_oot": n_oot,
        "ks_train": float(round(ks_train, 6)),
        "ks_oot": float(round(ks_oot, 6)),
        "auc_train": float(round(auc_train, 6)),
        "auc_oot": float(round(auc_oot, 6)),
        "gini_train": float(round(gini_train, 6)),
        "gini_oot": float(round(gini_oot, 6)),
        "benchmark": 0.331,
        "gap_benchmark": float(round(ks_oot - 0.331, 6)),
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
    print(f"  Predições: {BUCKET_MODELS}/{pred_key}")
    print(f"  Métricas: {BUCKET_MODELS}/{metricas_key}")
    print("=" * 60)

    print("\nFinalizado com sucesso.")


if __name__ == "__main__":
    main()
