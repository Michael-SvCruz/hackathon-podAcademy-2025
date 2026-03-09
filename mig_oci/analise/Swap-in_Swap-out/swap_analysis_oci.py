#!/usr/bin/env -S python3.11 -u
"""
swap_analysis_oci.py
====================
Análise de Swap-in / Swap-out — executa na VM OCI ou local.

Compara o modelo antigo (Score_01 = bureau score) com o modelo novo
(LightGBM score_fpd) na mesma população OOT (safras 202502, 202503).

Fluxo:
  1. Lê ABT v6 OOT (score_01_adj, fpd_int, flag_instalacao_int)
  2. Lê predições do modelo (score_fpd, decil)
  3. Merge por (num_cpf, safra)
  4. Calcula:
     - Tabela de performance por decil (ambos modelos)
     - Matriz de swap para múltiplas taxas de aprovação
     - KS de ambos os scores
  5. Salva JSON com resultados no bucket (e imprime no stdout)

Uso:
    # Na VM OCI (Instance Principal)
    python3.11 swap_analysis_oci.py

    # Local (debug)
    python3.11 swap_analysis_oci.py --local
"""

import argparse
import gc
import io
import json
import os
import sys
import traceback
from datetime import datetime

import numpy as np
import pandas as pd


# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

TARGET = "fpd_int"
ID_COLS = ["num_cpf", "safra"]
SAFRAS_OOT = ["202502", "202503"]

BUCKET_GOLD = "hackathon-2025-gold-layer"
BUCKET_MODELS = "hackathon-2025-models"
ABT_PREFIX = "abt_v6_v2"

# Taxas de aprovação para simular (70%, 75%, 80%, 85%, 90%)
APPROVAL_RATES = [0.70, 0.75, 0.80, 0.85, 0.90]


# ============================================================================
# OCI HELPERS
# ============================================================================

def get_oci_clients(local: bool = False):
    import oci
    if local:
        config = oci.config.from_file()
        client = oci.object_storage.ObjectStorageClient(config)
    else:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    namespace = client.get_namespace().data
    return client, namespace


def list_parquet_objects(client, namespace, bucket, prefix, delta_aware=False):
    """Lista parquets no bucket. Se delta_aware=True, filtra apenas arquivos ativos no Delta Lake."""
    objects = []
    next_start = None
    while True:
        kwargs = {"namespace_name": namespace, "bucket_name": bucket,
                  "prefix": prefix, "fields": "name,size"}
        if next_start:
            kwargs["start"] = next_start
        response = client.list_objects(**kwargs)
        for obj in response.data.objects:
            if obj.name.endswith(".parquet") and "/_delta_log/" not in obj.name:
                objects.append(obj.name)
        next_start = response.data.next_start_with
        if not next_start:
            break

    if delta_aware and objects:
        # Tentar ler o Delta log para filtrar apenas arquivos ativos
        active_files = _get_delta_active_files(client, namespace, bucket, prefix)
        if active_files is not None:
            # Filtrar apenas arquivos ativos
            filtered = [o for o in objects if any(o.endswith(af) or af in o for af in active_files)]
            if filtered:
                print(f"  Delta-aware: {len(filtered)} ativos de {len(objects)} totais", flush=True)
                return filtered
            else:
                print(f"  Delta-aware: filtro não matchou, usando todos {len(objects)}", flush=True)

    return objects


def _get_delta_active_files(client, namespace, bucket, prefix):
    """Lê o Delta transaction log para identificar arquivos ativos."""
    try:
        delta_prefix = prefix.rstrip("/") + "/_delta_log/"
        log_objects = []
        next_start = None
        while True:
            kwargs = {"namespace_name": namespace, "bucket_name": bucket,
                      "prefix": delta_prefix, "fields": "name,size"}
            if next_start:
                kwargs["start"] = next_start
            response = client.list_objects(**kwargs)
            for obj in response.data.objects:
                if obj.name.endswith(".json"):
                    log_objects.append(obj.name)
            next_start = response.data.next_start_with
            if not next_start:
                break

        if not log_objects:
            return None

        # Ler o log mais recente (maior número = mais recente)
        log_objects.sort()
        active_files = set()
        removed_files = set()

        for log_name in log_objects:
            response = client.get_object(namespace, bucket, log_name)
            for line in response.data.content.decode("utf-8").strip().split("\n"):
                try:
                    entry = json.loads(line)
                    if "add" in entry and entry["add"].get("path"):
                        path = entry["add"]["path"]
                        active_files.add(path)
                        removed_files.discard(path)
                    if "remove" in entry and entry["remove"].get("path"):
                        path = entry["remove"]["path"]
                        removed_files.add(path)
                        active_files.discard(path)
                except json.JSONDecodeError:
                    continue

        if active_files:
            print(f"  Delta log: {len(active_files)} add, {len(removed_files)} remove", flush=True)
            return active_files
        return None

    except Exception as e:
        print(f"  Delta log leitura falhou ({e}), usando todos os arquivos", flush=True)
        return None


def _get_mem_mb():
    """Retorna uso de memória RSS em MB."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 0


def read_parquet_chunks(client, namespace, bucket, prefix, columns=None,
                        row_filter=None, dedup_cols=None, delta_aware=False):
    """Lê parquets do OCI com filtro, dedup incremental, downcast e gc."""
    keys = list_parquet_objects(client, namespace, bucket, prefix, delta_aware=delta_aware)
    if not keys:
        raise FileNotFoundError(f"Nenhum .parquet em {bucket}/{prefix}")

    print(f"  {len(keys)} arquivos .parquet para ler", flush=True)
    print(f"  Colunas solicitadas: {columns}", flush=True)
    print(f"  Memória inicial: {_get_mem_mb()} MB", flush=True)

    # Diagnóstico: ler schema do primeiro arquivo para validar colunas
    first_key = keys[0]
    try:
        resp0 = client.get_object(namespace, bucket, first_key)
        buf0 = io.BytesIO(resp0.data.content)
        df0 = pd.read_parquet(buf0)
        available_cols = list(df0.columns)
        print(f"  Colunas disponíveis ({len(available_cols)}): {available_cols[:20]}...", flush=True)
        if columns:
            missing = [c for c in columns if c not in available_cols]
            if missing:
                print(f"  WARN: Colunas não encontradas: {missing}", flush=True)
                # Tentar case-insensitive match
                col_map = {c.lower(): c for c in available_cols}
                resolved = []
                for c in columns:
                    if c in available_cols:
                        resolved.append(c)
                    elif c.lower() in col_map:
                        print(f"    Mapeando: {c} → {col_map[c.lower()]}", flush=True)
                        resolved.append(col_map[c.lower()])
                    else:
                        print(f"    ERRO: {c} não existe no parquet (nem case-insensitive)", flush=True)
                columns = resolved if resolved else None
                print(f"  Colunas resolvidas: {columns}", flush=True)
        # Mostrar amostra dos dados com filtro
        if row_filter is not None:
            sample = df0.head(5)
            if "safra" in df0.columns:
                print(f"  safra dtype={df0['safra'].dtype}, valores únicos: {df0['safra'].unique()[:10]}", flush=True)
            if "flag_instalacao_int" in df0.columns:
                print(f"  flag_instalacao_int distribuição: {df0['flag_instalacao_int'].value_counts().to_dict()}", flush=True)
            filtered0 = row_filter(df0)
            print(f"  Diagnóstico filtro: {len(df0)} → {len(filtered0)} registros no 1º arquivo", flush=True)
        del df0, buf0, resp0
        gc.collect()
    except Exception as e:
        print(f"  WARN: diagnóstico falhou: {e}", flush=True)

    DEDUP_INTERVAL = 20  # Dedup mais frequente para liberar memória
    dfs = []
    for i, key in enumerate(keys):
        try:
            response = client.get_object(namespace, bucket, key)
            buf = io.BytesIO(response.data.content)
            df_part = pd.read_parquet(buf, columns=columns)
            del buf, response
        except Exception as e:
            print(f"    WARN: erro ao ler {key}: {e} (arquivo {i+1}/{len(keys)})", flush=True)
            continue

        if row_filter is not None:
            n_before = len(df_part)
            df_part = row_filter(df_part)
            # Se filtrou tudo, pular
            if len(df_part) == 0:
                del df_part
                continue

        # Downcast
        for col in df_part.select_dtypes(include=["float64"]).columns:
            df_part[col] = df_part[col].astype(np.float32)
        for col in df_part.select_dtypes(include=["int64"]).columns:
            df_part[col] = df_part[col].astype(np.int32)

        dfs.append(df_part)

        if dedup_cols and (i + 1) % DEDUP_INTERVAL == 0 and len(dfs) > 1:
            combined = pd.concat(dfs, ignore_index=True)
            combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
            dfs = [combined]
            del combined
            gc.collect()

        if (i + 1) % 10 == 0 or (i + 1) == len(keys):
            total = sum(len(d) for d in dfs)
            mem = _get_mem_mb()
            print(f"    Lidos {i+1}/{len(keys)} ({total:,} registros, {mem} MB RAM)", flush=True)

    if not dfs:
        raise ValueError(f"Nenhum registro encontrado após filtros em {bucket}/{prefix}")

    result = pd.concat(dfs, ignore_index=True)
    del dfs
    gc.collect()
    if dedup_cols:
        before = len(result)
        result = result.drop_duplicates(subset=dedup_cols, keep="first")
        if before != len(result):
            print(f"  Dedup final: {before:,} → {len(result):,}", flush=True)
    print(f"  Memória após carga: {_get_mem_mb()} MB", flush=True)
    return result


# ============================================================================
# MÉTRICAS
# ============================================================================

def calcular_ks(y_true, y_score, higher_is_bad=True):
    """Calcula KS. Se higher_is_bad=True, score alto = risco alto."""
    df = pd.DataFrame({"target": y_true, "score": y_score})
    if higher_is_bad:
        df = df.sort_values("score", ascending=False)
    else:
        df = df.sort_values("score", ascending=True)  # score baixo = risco alto
    df = df.reset_index(drop=True)
    df["bad_acum"] = df["target"].cumsum() / max(df["target"].sum(), 1)
    df["good_acum"] = (1 - df["target"]).cumsum() / max((1 - df["target"]).sum(), 1)
    df["ks"] = abs(df["bad_acum"] - df["good_acum"])
    return float(df["ks"].max())


def tabela_decil(df, score_col, target_col, n_quantiles=10):
    """Gera tabela de performance por decil."""
    df = df.copy()
    df["decil"] = pd.qcut(df[score_col], q=n_quantiles, labels=range(1, n_quantiles + 1),
                           duplicates="drop").astype(int)

    tab = df.groupby("decil").agg(
        qtd=(target_col, "count"),
        n_fpd=(target_col, "sum"),
        taxa_fpd=(target_col, "mean"),
        score_min=(score_col, "min"),
        score_max=(score_col, "max"),
        score_medio=(score_col, "mean"),
    ).reset_index()

    tab["pct_pop"] = (tab["qtd"] / tab["qtd"].sum() * 100).round(1)
    tab["taxa_fpd_pct"] = (tab["taxa_fpd"] * 100).round(2)
    tab["fpd_acum"] = (tab["n_fpd"].cumsum() / tab["n_fpd"].sum() * 100).round(1)

    return tab


# ============================================================================
# SWAP ANALYSIS
# ============================================================================

def swap_matrix(df, score_old, score_new, target, approval_rate):
    """Calcula matriz de swap para uma taxa de aprovação fixa.

    Lógica:
    - Score_01 (bureau): score BAIXO = risco ALTO → rejeitar os menores scores
      (aprovamos quem tem score_01_adj ALTO)
    - score_fpd (LightGBM): score ALTO = risco ALTO → rejeitar os maiores scores
      (aprovamos quem tem score_fpd BAIXO)

    Para mesma taxa de aprovação, definimos thresholds em cada score
    e comparamos as decisões.
    """
    n = len(df)
    n_approve = int(n * approval_rate)

    # Score_01 (bureau): aprovamos os top N por score (maior = melhor)
    threshold_old = df[score_old].quantile(1 - approval_rate)
    decision_old = (df[score_old] >= threshold_old).astype(int)  # 1 = aprovado

    # score_fpd (LightGBM): aprovamos os bottom N por score (menor = melhor)
    threshold_new = df[score_new].quantile(approval_rate)
    decision_new = (df[score_new] <= threshold_new).astype(int)  # 1 = aprovado

    y = df[target]

    # Matriz de swap
    #                    MODELO NOVO
    #                 Aprova    Rejeita
    # MODELO    Aprova   A(acordo)  B(swap-out)
    # ANTIGO    Rejeita  C(swap-in) D(acordo)

    a_mask = (decision_old == 1) & (decision_new == 1)  # Acordo: ambos aprovam
    b_mask = (decision_old == 1) & (decision_new == 0)  # Swap-out: antigo aprova, novo rejeita
    c_mask = (decision_old == 0) & (decision_new == 1)  # Swap-in: antigo rejeita, novo aprova
    d_mask = (decision_old == 0) & (decision_new == 0)  # Acordo: ambos rejeitam

    def cell_stats(mask):
        n_cell = int(mask.sum())
        n_fpd = int(y[mask].sum()) if n_cell > 0 else 0
        taxa_fpd = float(y[mask].mean()) if n_cell > 0 else 0.0
        return {"n": n_cell, "n_fpd": n_fpd, "taxa_fpd": round(taxa_fpd, 6)}

    # FPD entre aprovados (cada modelo)
    approved_old_fpd = float(y[decision_old == 1].mean()) if (decision_old == 1).sum() > 0 else 0
    approved_new_fpd = float(y[decision_new == 1].mean()) if (decision_new == 1).sum() > 0 else 0

    return {
        "approval_rate": approval_rate,
        "n_total": n,
        "n_approved_old": int((decision_old == 1).sum()),
        "n_approved_new": int((decision_new == 1).sum()),
        "threshold_old": float(round(threshold_old, 4)),
        "threshold_new": float(round(threshold_new, 6)),
        "A_acordo_aprovam": cell_stats(a_mask),
        "B_swap_out": cell_stats(b_mask),
        "C_swap_in": cell_stats(c_mask),
        "D_acordo_rejeitam": cell_stats(d_mask),
        "fpd_aprovados_modelo_antigo": round(approved_old_fpd, 6),
        "fpd_aprovados_modelo_novo": round(approved_new_fpd, 6),
        "reducao_fpd_pct": round((approved_old_fpd - approved_new_fpd) / max(approved_old_fpd, 0.0001) * 100, 2),
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Swap-in/Swap-out Analysis — OCI VM")
    parser.add_argument("--local", action="store_true", help="Usar ~/.oci/config")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    print("=" * 60, flush=True)
    print("SWAP-IN / SWAP-OUT ANALYSIS", flush=True)
    print(f"Timestamp: {timestamp}", flush=True)
    print("=" * 60, flush=True)

    # ------------------------------------------------------------------
    # 1. OCI clients
    # ------------------------------------------------------------------
    oci_client, namespace = get_oci_clients(local=args.local)
    print(f"Namespace: {namespace}", flush=True)

    # ------------------------------------------------------------------
    # 2. Carregar ABT v6 (apenas colunas necessárias)
    # ------------------------------------------------------------------
    print("\n--- Carregando ABT v6 OOT (score_01_adj) ---", flush=True)
    abt_cols = ["num_cpf", "safra", "score_01_adj", "fpd_int", "flag_instalacao_int"]
    valid_safras = set(SAFRAS_OOT)

    df_abt = read_parquet_chunks(
        oci_client, namespace, BUCKET_GOLD, f"{ABT_PREFIX}/",
        columns=abt_cols,
        row_filter=lambda chunk: chunk[
            (chunk["flag_instalacao_int"] == 1) &
            (chunk["safra"].astype(str).isin(valid_safras))
        ],
        dedup_cols=ID_COLS,
        delta_aware=True,  # Filtrar apenas arquivos ativos do Delta Lake
    )
    df_abt["safra"] = df_abt["safra"].astype(str)
    print(f"  ABT OOT: {len(df_abt):,} registros", flush=True)
    print(f"  Score_01 cobertura: {df_abt['score_01_adj'].notna().mean()*100:.1f}%", flush=True)

    # ------------------------------------------------------------------
    # 3. Carregar predições do modelo
    # ------------------------------------------------------------------
    print("\n--- Carregando predições do modelo ---", flush=True)
    pred_keys = list_parquet_objects(oci_client, namespace, BUCKET_MODELS, "resultados_modelo/")
    if not pred_keys:
        print("ERRO: Nenhuma predição encontrada em resultados_modelo/", flush=True)
        sys.exit(1)

    # Pegar o mais recente
    pred_key = sorted(pred_keys)[-1]
    print(f"  Usando: {pred_key}", flush=True)
    response = oci_client.get_object(namespace, BUCKET_MODELS, pred_key)
    df_pred = pd.read_parquet(io.BytesIO(response.data.content))
    df_pred["safra"] = df_pred["safra"].astype(str)
    print(f"  Predições: {len(df_pred):,} registros", flush=True)

    # ------------------------------------------------------------------
    # 4. Merge ABT + Predições
    # ------------------------------------------------------------------
    print("\n--- Merge ABT + Predições ---", flush=True)
    # Precisamos de num_cpf como mesmo tipo
    df_abt["num_cpf"] = df_abt["num_cpf"].astype(str)
    df_pred["num_cpf"] = df_pred["num_cpf"].astype(str)

    df = df_pred.merge(
        df_abt[["num_cpf", "safra", "score_01_adj"]],
        on=ID_COLS,
        how="inner",
    )
    print(f"  Merge: {len(df):,} registros ({len(df)/len(df_pred)*100:.1f}% match)", flush=True)

    # Remover registros sem score_01_adj
    df_valid = df[df["score_01_adj"].notna() & (df["score_01_adj"] > 0)].copy()
    print(f"  Com score_01_adj válido (>0): {len(df_valid):,} ({len(df_valid)/len(df)*100:.1f}%)", flush=True)

    # ------------------------------------------------------------------
    # 5. KS de ambos os scores
    # ------------------------------------------------------------------
    print("\n--- KS dos Scores ---", flush=True)
    ks_score01 = calcular_ks(df_valid[TARGET], df_valid["score_01_adj"], higher_is_bad=False)
    ks_lgbm = calcular_ks(df_valid[TARGET], df_valid["score_fpd"], higher_is_bad=True)
    print(f"  KS Score_01 (bureau): {ks_score01*100:.2f}%", flush=True)
    print(f"  KS LightGBM (score_fpd): {ks_lgbm*100:.2f}%", flush=True)

    # ------------------------------------------------------------------
    # 6. Tabelas de decil
    # ------------------------------------------------------------------
    print("\n--- Tabela de Decil (LightGBM) ---", flush=True)
    tab_lgbm = tabela_decil(df_valid, "score_fpd", TARGET)
    print(tab_lgbm[["decil", "qtd", "taxa_fpd_pct", "fpd_acum"]].to_string(index=False), flush=True)

    print("\n--- Tabela de Decil (Score_01) ---", flush=True)
    tab_score01 = tabela_decil(df_valid, "score_01_adj", TARGET)
    print(tab_score01[["decil", "qtd", "taxa_fpd_pct", "fpd_acum"]].to_string(index=False), flush=True)

    # ------------------------------------------------------------------
    # 7. Swap analysis para múltiplas taxas de aprovação
    # ------------------------------------------------------------------
    print("\n--- Swap Analysis ---", flush=True)
    swap_results = []
    for rate in APPROVAL_RATES:
        result = swap_matrix(df_valid, "score_01_adj", "score_fpd", TARGET, rate)
        swap_results.append(result)

        b = result["B_swap_out"]
        c = result["C_swap_in"]
        print(f"\n  Taxa Aprovação = {rate*100:.0f}%", flush=True)
        print(f"    Swap-out (antigo aprova, novo rejeita): {b['n']:,} "
              f"(FPD={b['taxa_fpd']*100:.1f}%)", flush=True)
        print(f"    Swap-in  (antigo rejeita, novo aprova): {c['n']:,} "
              f"(FPD={c['taxa_fpd']*100:.1f}%)", flush=True)
        print(f"    FPD aprovados: antigo={result['fpd_aprovados_modelo_antigo']*100:.2f}% "
              f"→ novo={result['fpd_aprovados_modelo_novo']*100:.2f}% "
              f"(redução: {result['reducao_fpd_pct']:.1f}%)", flush=True)

    # ------------------------------------------------------------------
    # 8. Salvar resultados
    # ------------------------------------------------------------------
    output = {
        "timestamp": timestamp,
        "n_oot": len(df_valid),
        "safras": SAFRAS_OOT,
        "ks_score01": round(ks_score01, 6),
        "ks_lgbm": round(ks_lgbm, 6),
        "decil_lgbm": tab_lgbm.to_dict(orient="records"),
        "decil_score01": tab_score01.to_dict(orient="records"),
        "swap_analysis": swap_results,
    }

    # Converter numpy types para JSON
    output_json = json.dumps(output, indent=2, ensure_ascii=False, default=str)

    # Salvar no bucket
    result_key = f"metricas/swap_analysis_{timestamp}.json"
    oci_client.put_object(namespace, BUCKET_MODELS, result_key, output_json.encode("utf-8"))
    print(f"\nResultados salvos em: {BUCKET_MODELS}/{result_key}", flush=True)

    # Imprimir JSON entre marcadores (para captura via SSH)
    print("\n[JSON_RESULT_START]", flush=True)
    print(output_json, flush=True)
    print("[JSON_RESULT_END]", flush=True)

    print("\n" + "=" * 60, flush=True)
    print("SWAP ANALYSIS CONCLUÍDO", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n{'='*60}", flush=True)
        print(f"ERRO FATAL: {e}", flush=True)
        print(f"{'='*60}", flush=True)
        traceback.print_exc()
        sys.exit(1)
