#!/usr/bin/env python3
"""
ks_incremental_charts.py
========================
Gera gráficos de KS incremental por bloco de features para a apresentação final.

Gráficos gerados:
  1. KS Incremental por Bloco (barras + linha) — LightGBM OCI VM
  2. Comparação Logística vs LightGBM
  3. Contribuição por Bloco (waterfall)
  4. Resumo Final com Benchmark
  5. Features por Bloco (donut)

Uso:
    python3 ks_incremental_charts.py
    # Gera PNGs na pasta output/

Dados:
    - LightGBM per-block: valores EXATOS da execução na OCI VM (2026-03-07)
      Fonte: metricas/ks_incremental_20260307_1850.json
    - Logistic Regression: valores exatos de docs/08_team_preparation/
    - Benchmark Claro: 33.10%
"""

import os
import json
import matplotlib
matplotlib.use("Agg")  # Backend sem GUI (funciona em WSL/servidor)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ============================================================================
# DADOS
# ============================================================================

# Benchmark da Claro (dado pela coordenação)
BENCHMARK = 33.10

# --- LightGBM (OCI VM) — KS OOT por bloco incremental ---
# Fonte: metricas/ks_incremental_20260307_1850.json (execução real na VM OCI)
# TODOS os valores são EXATOS — treinamento independente por ABT
LGBM_OCI_VM_INCREMENTAL = {
    "labels": [
        "Score_01",
        "+ Score_02",
        "+ Telco",
        "+ Cadastro",
        "+ Recarga",
        "+ Pag. + Atraso",
    ],
    "ks_oot": [26.67, 31.25, 31.51, 31.70, 33.95, 34.39],
    "abt_version": ["v1", "v2", "v3", "v4", "v5", "v6"],
    "n_features": [1, 2, 89, 95, 160, 261],
}

# --- LightGBM (OCI VM) — métricas do modelo final (v6) ---
LGBM_OCI_VM = {
    "ks_oot": 34.39,
    "auc_oot": 0.7327,
    "gini_oot": 46.54,
    "n_features": 261,
    "best_iteration": 900,
}

# --- Logistic Regression (Statsmodels) — KS OOT por ABT version ---
# Fonte: docs/08_team_preparation/technical/modeling/ANALISE_NOTEBOOKS_MODELAGEM.md
LOGISTIC = {
    "labels": ["V1\nScore_01", "V2\n+Score_02", "V3\n+Telco", "V4\n+Cadastro", "V5\n+Recarga", "Final"],
    "ks_oot": [24.00, 28.75, 28.76, 28.93, 30.87, 30.73],
}

# --- Dados para PowerPoint (tabela exportável) ---
POWERPOINT_DATA = {
    "incremental_lgbm": {
        "Bloco": ["Score_01", "+ Score_02", "+ Telco", "+ Cadastro", "+ Recarga", "+ Pagamento + Atraso"],
        "ABT": ["v1", "v2", "v3", "v4", "v5", "v6"],
        "Features": [1, 2, 89, 95, 160, 261],
        "KS OOT (%)": [26.67, 31.25, 31.51, 31.70, 33.95, 34.39],
        "Delta (p.p.)": ["baseline", "+4.58", "+0.26", "+0.19", "+2.25", "+0.44"],
    },
    "comparacao_modelos": {
        "Modelo": ["Logistic Regression", "LightGBM (OCI VM)", "Benchmark Claro"],
        "KS OOT (%)": [30.73, 34.39, 33.10],
        "vs Benchmark (p.p.)": [-2.37, +1.29, 0.00],
    },
}

# ============================================================================
# CORES E ESTILO
# ============================================================================

# Paleta Oracle/OCI inspirada
COLORS = {
    "primary": "#C74634",      # Oracle Red
    "secondary": "#312D2A",    # Dark Brown
    "accent": "#00758F",       # Teal
    "gold": "#D4A843",         # Gold
    "green": "#2D8C3C",        # Success green
    "light_gray": "#F5F5F5",
    "benchmark": "#E63946",    # Red line
    "bars": ["#1A4B8C", "#2166AC", "#4393C3", "#74ADD1", "#ABD9E9", "#C74634"],
    "waterfall_pos": "#2D8C3C",
    "waterfall_base": "#1A4B8C",
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 12,
    "axes.titlesize": 16,
    "axes.labelsize": 13,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
})


def ensure_output_dir():
    out = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(out, exist_ok=True)
    return out


# ============================================================================
# GRÁFICO 1: KS Incremental por Bloco (Barras + Linha + Benchmark)
# ============================================================================

def chart_ks_incremental():
    fig, ax = plt.subplots(figsize=(14, 7))

    labels = LGBM_OCI_VM_INCREMENTAL["labels"]
    ks_values = LGBM_OCI_VM_INCREMENTAL["ks_oot"]
    n_features = LGBM_OCI_VM_INCREMENTAL["n_features"]
    n = len(labels)
    x = np.arange(n)

    # Barras com cores crescentes
    bars = ax.bar(x, ks_values, width=0.6, color=COLORS["bars"], edgecolor="white", linewidth=1.5, zorder=3)

    # Linha conectando os topos
    ax.plot(x, ks_values, color=COLORS["secondary"], marker="o", markersize=8,
            linewidth=2.5, zorder=4, markerfacecolor="white", markeredgewidth=2)

    # Benchmark line
    ax.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2, zorder=2, label=f"Benchmark = {BENCHMARK}%")

    # Valores nas barras
    for i, (bar, val) in enumerate(zip(bars, ks_values)):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{val:.2f}%", ha="center", va="bottom", fontsize=13, fontweight="bold", color=COLORS["secondary"])

    # Delta annotations
    for i in range(1, n):
        delta = ks_values[i] - ks_values[i - 1]
        y_mid = (ks_values[i] + ks_values[i - 1]) / 2
        ax.annotate(f"+{delta:.2f}",
                    xy=(i - 0.5, y_mid), fontsize=10, color=COLORS["green"],
                    fontweight="bold", ha="center", va="center",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="#E8F5E9", edgecolor=COLORS["green"], alpha=0.8))

    # Número de features embaixo de cada label
    for i, nf in enumerate(n_features):
        ax.text(i, 22.8, f"({nf} feat.)", ha="center", va="top", fontsize=9, color="gray", style="italic")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12)
    ax.set_ylabel("KS OOT (%)", fontsize=14)
    ax.set_title("KS Incremental por Bloco de Features — LightGBM (OCI VM)", fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(22, 38)
    ax.legend(loc="upper left", fontsize=12, framealpha=0.9)

    # Nota: todos valores exatos
    ax.text(0.98, 0.02, "Valores exatos — treinamento independente por ABT na OCI VM (2026-03-07)",
            transform=ax.transAxes, fontsize=9, ha="right", va="bottom",
            style="italic", color="gray")

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 2: Comparação Logística vs LightGBM
# ============================================================================

def chart_logistic_vs_lgbm():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), sharey=True)

    # --- Logistic Regression ---
    labels_lr = LOGISTIC["labels"]
    ks_lr = LOGISTIC["ks_oot"]
    x_lr = np.arange(len(labels_lr))

    bars_lr = ax1.bar(x_lr, ks_lr, width=0.55, color="#74ADD1", edgecolor="white", linewidth=1.5)
    ax1.plot(x_lr, ks_lr, color=COLORS["secondary"], marker="o", markersize=7, linewidth=2, markerfacecolor="white", markeredgewidth=2)
    ax1.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2, label=f"Benchmark = {BENCHMARK}%")

    for bar, val in zip(bars_lr, ks_lr):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                 f"{val:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax1.set_xticks(x_lr)
    ax1.set_xticklabels(labels_lr, fontsize=10)
    ax1.set_ylabel("KS OOT (%)", fontsize=14)
    ax1.set_title("Regressão Logística", fontsize=15, fontweight="bold")
    ax1.set_ylim(20, 38)
    ax1.legend(fontsize=10)

    # --- LightGBM ---
    labels_lgbm = LGBM_OCI_VM_INCREMENTAL["labels"]
    ks_lgbm = LGBM_OCI_VM_INCREMENTAL["ks_oot"]
    x_lgbm = np.arange(len(labels_lgbm))

    bars_lgbm = ax2.bar(x_lgbm, ks_lgbm, width=0.55, color=COLORS["bars"], edgecolor="white", linewidth=1.5)
    ax2.plot(x_lgbm, ks_lgbm, color=COLORS["secondary"], marker="o", markersize=7, linewidth=2, markerfacecolor="white", markeredgewidth=2)
    ax2.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2, label=f"Benchmark = {BENCHMARK}%")

    for i, (bar, val) in enumerate(zip(bars_lgbm, ks_lgbm)):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                 f"{val:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax2.set_xticks(x_lgbm)
    ax2.set_xticklabels(labels_lgbm, fontsize=10)
    ax2.set_title("LightGBM", fontsize=15, fontweight="bold")
    ax2.legend(fontsize=10)

    fig.suptitle("Evolução do KS OOT — Logística vs LightGBM", fontsize=17, fontweight="bold", y=1.02)
    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 3: Waterfall — Contribuição por Bloco
# ============================================================================

def chart_waterfall():
    fig, ax = plt.subplots(figsize=(14, 7))

    labels = LGBM_OCI_VM_INCREMENTAL["labels"]
    ks_values = LGBM_OCI_VM_INCREMENTAL["ks_oot"]
    n = len(labels)

    # Calcular deltas
    deltas = [ks_values[0]] + [round(ks_values[i] - ks_values[i - 1], 2) for i in range(1, n)]
    bottoms = [0] + ks_values[:-1]

    x = np.arange(n)

    # Barras base (acumulado anterior) — invisíveis
    ax.bar(x, bottoms, width=0.5, color="none")

    # Barras delta (contribuição) — destaque para os maiores saltos
    colors = [COLORS["waterfall_base"]]
    for d in deltas[1:]:
        colors.append(COLORS["primary"] if d >= 2.0 else COLORS["waterfall_pos"])
    bars = ax.bar(x, deltas, bottom=bottoms, width=0.5, color=colors, edgecolor="white", linewidth=1.5, zorder=3)

    # Benchmark
    ax.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2, zorder=2, label=f"Benchmark = {BENCHMARK}%")

    # Conectores entre barras
    for i in range(n - 1):
        ax.plot([i + 0.25, i + 0.75], [ks_values[i], ks_values[i]], color="gray", linewidth=1, linestyle=":", zorder=2)

    # Valores
    for i, (delta, bottom) in enumerate(zip(deltas, bottoms)):
        # Delta label
        if i == 0:
            label = f"{delta:.2f}%\n(base)"
        else:
            label = f"+{delta:.2f} p.p."

        y_pos = bottom + delta / 2
        ax.text(i, y_pos, label, ha="center", va="center", fontsize=11,
                fontweight="bold", color="white" if delta > 2.0 else COLORS["secondary"],
                zorder=5)

        # Total acumulado no topo
        ax.text(i, bottom + delta + 0.3, f"{ks_values[i]:.2f}%",
                ha="center", va="bottom", fontsize=11, fontweight="bold", color=COLORS["secondary"])

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12)
    ax.set_ylabel("KS OOT (%)", fontsize=14)
    ax.set_title("Contribuição de Cada Bloco de Features — Waterfall KS (OCI VM)", fontsize=16, fontweight="bold", pad=15)
    ax.set_ylim(0, 38)
    ax.legend(loc="upper left", fontsize=12)

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 4: Resumo Final — Comparação com Benchmark
# ============================================================================

def chart_resumo_final():
    fig, ax = plt.subplots(figsize=(12, 7))

    modelos = ["Logística\n(Statsmodels)", "LightGBM\n(OCI VM)"]
    ks_values = [30.73, 34.39]
    colors = ["#74ADD1", COLORS["primary"]]

    x = np.arange(len(modelos))
    bars = ax.bar(x, ks_values, width=0.5, color=colors, edgecolor="white", linewidth=2, zorder=3)

    # Benchmark
    ax.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2.5, zorder=2)
    ax.text(len(modelos) - 0.5, BENCHMARK + 0.2, f"Benchmark = {BENCHMARK}%",
            fontsize=12, color=COLORS["benchmark"], fontweight="bold", ha="right")

    # Valores e gap
    for i, (bar, val) in enumerate(zip(bars, ks_values)):
        gap = val - BENCHMARK
        gap_color = COLORS["green"] if gap >= 0 else "#E63946"
        gap_text = f"+{gap:.2f} p.p." if gap >= 0 else f"{gap:.2f} p.p."

        # KS value
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                f"{val:.2f}%", ha="center", va="bottom", fontsize=16, fontweight="bold", color=COLORS["secondary"])

        # Gap badge
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() - 1.5,
                gap_text, ha="center", va="top", fontsize=13, fontweight="bold", color="white",
                bbox=dict(boxstyle="round,pad=0.3", facecolor=gap_color, edgecolor="none", alpha=0.9))

    ax.set_xticks(x)
    ax.set_xticklabels(modelos, fontsize=13)
    ax.set_ylabel("KS OOT (%)", fontsize=14)
    ax.set_title("Resultado Final — KS OOT vs Benchmark (33.10%)", fontsize=17, fontweight="bold", pad=15)
    ax.set_ylim(26, 37)

    # Annotation box
    textstr = (
        f"Modelo Final (OCI VM):\n"
        f"  KS OOT = {LGBM_OCI_VM['ks_oot']:.2f}%\n"
        f"  AUC    = {LGBM_OCI_VM['auc_oot']:.4f}\n"
        f"  GINI   = {LGBM_OCI_VM['gini_oot']:.2f}%\n"
        f"  Features = {LGBM_OCI_VM['n_features']}\n"
        f"  Iterações = {LGBM_OCI_VM['best_iteration']}"
    )
    props = dict(boxstyle="round,pad=0.8", facecolor=COLORS["light_gray"], edgecolor=COLORS["secondary"], alpha=0.9)
    ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=11,
            verticalalignment="top", bbox=props, fontfamily="monospace")

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 5: Features por Bloco (barras horizontais)
# ============================================================================

def chart_features_por_bloco():
    """Barras horizontais mostrando composição de features do modelo final (261 features, OCI VM).

    Breakdown estimado a partir do KS incremental:
    - Scores: 2 (Score_01 + Score_02, sempre selecionados)
    - Cadastro: ~6 (v4 tem 95; 95 - 89 = 6 novas)
    - Recarga: ~65 (v5 tem 160; 160 - 95 = 65 novas)
    - Telco: ~87 (v3 tem 89 features; 89 - 2 scores = 87)
    - Pag. + Atraso: ~101 (v6 tem 261; 261 - 160 = 101 novas)
    Total: 2 + 87 + 6 + 65 + 101 = 261
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    # Ordenado por quantidade (menor → maior, de baixo para cima)
    blocos = ["Scores", "Cadastro", "Recarga", "Telco", "Pag. + Atraso"]
    sizes = [2, 6, 65, 87, 101]
    total = sum(sizes)
    colors_bar = ["#1A4B8C", "#4393C3", "#74ADD1", "#2166AC", "#C74634"]

    y = np.arange(len(blocos))
    bars = ax.barh(y, sizes, height=0.55, color=colors_bar, edgecolor="white", linewidth=1.5, zorder=3)

    # Valores nas barras
    for bar, val in zip(bars, sizes):
        pct = val / total * 100
        # Label dentro se barra larga, fora se estreita
        if val >= 20:
            ax.text(bar.get_width() - 2, bar.get_y() + bar.get_height() / 2,
                    f"{val}  ({pct:.0f}%)", ha="right", va="center",
                    fontsize=12, fontweight="bold", color="white")
        else:
            ax.text(bar.get_width() + 1.5, bar.get_y() + bar.get_height() / 2,
                    f"{val}  ({pct:.0f}%)", ha="left", va="center",
                    fontsize=12, fontweight="bold", color=COLORS["secondary"])

    ax.set_yticks(y)
    ax.set_yticklabels(blocos, fontsize=13)
    ax.set_xlabel("Quantidade de Features", fontsize=14)
    ax.set_title("Features Selecionadas por Bloco\n(261 features, IV >= 0.01 — OCI VM)",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_xlim(0, 120)
    ax.grid(axis="y", visible=False)

    fig.tight_layout()
    return fig


# ============================================================================
# EXPORTAR DADOS PARA POWERPOINT
# ============================================================================

def export_powerpoint_data(output_dir):
    """Exporta dados tabulares em JSON e TXT para fácil cópia para PowerPoint."""

    # JSON com todos os dados
    filepath_json = os.path.join(output_dir, "dados_apresentacao.json")
    with open(filepath_json, "w", encoding="utf-8") as f:
        json.dump(POWERPOINT_DATA, f, indent=2, ensure_ascii=False)

    # TXT formatado para cópia
    filepath_txt = os.path.join(output_dir, "dados_apresentacao.txt")
    with open(filepath_txt, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("DADOS PARA APRESENTAÇÃO — KS INCREMENTAL\n")
        f.write("=" * 70 + "\n\n")

        f.write("1. KS INCREMENTAL POR BLOCO (LightGBM)\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'Bloco':<25} {'ABT':<6} {'Features':<10} {'KS OOT':<10} {'Delta':<10}\n")
        f.write("-" * 70 + "\n")
        d = POWERPOINT_DATA["incremental_lgbm"]
        for i in range(len(d["Bloco"])):
            f.write(f"{d['Bloco'][i]:<25} {d['ABT'][i]:<6} {d['Features'][i]:<10} {d['KS OOT (%)'][i]:<10.2f} {d['Delta (p.p.)'][i]:<10}\n")
        f.write("\n")

        f.write("2. COMPARAÇÃO DE MODELOS\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'Modelo':<30} {'KS OOT (%)':<15} {'vs Benchmark':<15}\n")
        f.write("-" * 70 + "\n")
        d = POWERPOINT_DATA["comparacao_modelos"]
        for i in range(len(d["Modelo"])):
            gap = d["vs Benchmark (p.p.)"][i]
            gap_str = f"+{gap:.2f}" if gap > 0 else f"{gap:.2f}" if gap < 0 else "—"
            f.write(f"{d['Modelo'][i]:<30} {d['KS OOT (%)'][i]:<15.2f} {gap_str:<15}\n")
        f.write("\n")

        f.write("3. MÉTRICAS FINAIS (OCI VM)\n")
        f.write("-" * 70 + "\n")
        for k, v in LGBM_OCI_VM.items():
            f.write(f"  {k}: {v}\n")
        f.write(f"  benchmark: {BENCHMARK}%\n")
        f.write(f"  gap: +{LGBM_OCI_VM['ks_oot'] - BENCHMARK:.2f} p.p.\n")
        f.write("\n")

        f.write("4. NOTA SOBRE VALORES\n")
        f.write("-" * 70 + "\n")
        f.write("- TODOS os valores são EXATOS — treinamento independente por ABT\n")
        f.write("- Fonte: metricas/ks_incremental_20260307_1850.json (OCI VM)\n")
        f.write("- Mesmos hiperparâmetros LightGBM em todas as ABTs\n")
        f.write("- Benchmark Claro: 33.10% (definido pela coordenação)\n")

    print(f"  Dados exportados: {filepath_json}")
    print(f"  Dados exportados: {filepath_txt}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    output_dir = ensure_output_dir()
    print("=" * 60)
    print("GERANDO GRÁFICOS — KS INCREMENTAL")
    print("=" * 60)

    charts = [
        ("01_ks_incremental_lgbm.png", chart_ks_incremental, "KS Incremental por Bloco"),
        ("02_logistica_vs_lgbm.png", chart_logistic_vs_lgbm, "Logística vs LightGBM"),
        ("03_waterfall_contribuicao.png", chart_waterfall, "Waterfall — Contribuição por Bloco"),
        ("04_resumo_final_benchmark.png", chart_resumo_final, "Resumo Final vs Benchmark"),
        ("05_features_por_bloco.png", chart_features_por_bloco, "Features por Bloco (Donut)"),
    ]

    for filename, chart_func, description in charts:
        print(f"\n  Gerando: {description}...")
        fig = chart_func()
        filepath = os.path.join(output_dir, filename)
        fig.savefig(filepath, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"  Salvo: {filepath}")

    print("\n  Exportando dados para PowerPoint...")
    export_powerpoint_data(output_dir)

    print("\n" + "=" * 60)
    print(f"CONCLUÍDO — {len(charts)} gráficos + dados em: {output_dir}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
