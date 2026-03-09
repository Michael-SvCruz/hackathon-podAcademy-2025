#!/usr/bin/env python3
"""
swap_charts.py
==============
Gera gráficos da análise Swap-in / Swap-out para a apresentação final.

Gráficos gerados:
  11. Taxa FPD por Decil — Score_01 vs LightGBM (barras agrupadas)
  12. Matriz de Swap (heatmap) para taxa de aprovação de 80%
  13. FPD entre Aprovados — comparação por taxa de aprovação (linhas)
  14. Volume de Swap por taxa de aprovação (barras agrupadas)
  15. Redução de FPD — ganho do modelo novo por taxa de aprovação (barras)

Uso:
    python3 swap_charts.py
    # Gera PNGs na pasta output/

Dados:
    - Fonte: swap_analysis_oci.py executado na VM OCI (2026-03-08)
    - JSON: metricas/swap_analysis_20260308_1644.json
    - População OOT: 851,616 clientes (safras 202502, 202503)
    - KS Score_01: 26.71% | KS LightGBM: 34.42%
"""

import os
import json
import matplotlib
matplotlib.use("Agg")  # Backend sem GUI (funciona em WSL/servidor)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


# ============================================================================
# DADOS — extraídos de swap_analysis_20260308_1644.json (execução real na VM OCI)
# ============================================================================

# Metadata
SWAP_META = {
    "timestamp": "20260308_1644",
    "n_oot": 851616,
    "safras": ["202502", "202503"],
    "ks_score01": 26.71,  # %
    "ks_lgbm": 34.42,     # %
}

# Tabela de decil — LightGBM (score_fpd)
# Decil 1 = menor risco (score_fpd baixo), Decil 10 = maior risco (score_fpd alto)
DECIL_LGBM = {
    "decil":        [1,     2,     3,     4,     5,     6,     7,     8,     9,     10],
    "taxa_fpd_pct": [5.11,  8.06, 10.00, 12.29, 15.40, 18.98, 23.28, 28.99, 37.27, 52.86],
    "qtd":          [85162, 85162, 85161, 85162, 85161, 85162, 85161, 85162, 85161, 85162],
    "fpd_acum":     [2.4,   6.2,  10.9,  16.7,  24.0,  32.9,  43.9,  57.5,  75.1,  100.0],
}

# Tabela de decil — Score_01 (bureau)
# Decil 1 = maior risco (score_01 baixo), Decil 10 = menor risco (score_01 alto)
DECIL_SCORE01 = {
    "decil":        [1,     2,     3,     4,     5,     6,     7,     8,     9,     10],
    "taxa_fpd_pct": [40.94, 33.83, 29.07, 24.50, 20.53, 17.23, 14.36, 12.44, 10.51,  7.51],
    "qtd":          [88824, 89107, 80264, 84096, 88659, 83674, 82145, 85917, 84248, 84682],
    "score_min":    [2,     532,   554,   570,   583,   596,   608,   620,   637,   657],
    "score_max":    [531,   553,   569,   582,   595,   607,   619,   636,   656,   771],
}

# Swap Analysis — para 5 taxas de aprovação
# Cada entrada: taxa, acordo_aprovam(A), swap_out(B), swap_in(C), acordo_rejeitam(D)
SWAP_DATA = [
    {
        "taxa": 70, "n_approved_old": 597830, "n_approved_new": 596131,
        "A": {"n": 518086, "fpd": 12.39}, "B": {"n": 79744, "fpd": 34.96},
        "C": {"n": 78045, "fpd": 19.34}, "D": {"n": 175741, "fpd": 41.86},
        "fpd_old": 15.40, "fpd_new": 13.30, "reducao": 13.64,
    },
    {
        "taxa": 75, "n_approved_old": 647999, "n_approved_new": 638712,
        "A": {"n": 568774, "fpd": 13.39}, "B": {"n": 79225, "fpd": 38.16},
        "C": {"n": 69938, "fpd": 21.32}, "D": {"n": 133679, "fpd": 44.48},
        "fpd_old": 16.42, "fpd_new": 14.26, "reducao": 13.16,
    },
    {
        "taxa": 80, "n_approved_old": 681876, "n_approved_new": 681293,
        "A": {"n": 612371, "fpd": 14.33}, "B": {"n": 69505, "fpd": 41.76},
        "C": {"n": 68922, "fpd": 23.60}, "D": {"n": 100818, "fpd": 47.35},
        "fpd_old": 17.12, "fpd_new": 15.26, "reducao": 10.85,
    },
    {
        "taxa": 85, "n_approved_old": 728854, "n_approved_new": 723873,
        "A": {"n": 667669, "fpd": 15.60}, "B": {"n": 61185, "fpd": 45.86},
        "C": {"n": 56204, "fpd": 26.16}, "D": {"n": 66558, "fpd": 50.87},
        "fpd_old": 18.14, "fpd_new": 16.42, "reducao": 9.49,
    },
    {
        "taxa": 90, "n_approved_old": 769410, "n_approved_new": 766454,
        "A": {"n": 722814, "fpd": 17.03}, "B": {"n": 46596, "fpd": 51.01},
        "C": {"n": 43640, "fpd": 29.04}, "D": {"n": 38566, "fpd": 55.09},
        "fpd_old": 19.08, "fpd_new": 17.71, "reducao": 7.20,
    },
]

# Dados para PowerPoint (tabela exportável)
POWERPOINT_DATA = {
    "swap_por_taxa": {
        "Taxa Aprovação": ["70%", "75%", "80%", "85%", "90%"],
        "Swap-out (rejeitar maus)": [79744, 79225, 69505, 61185, 46596],
        "FPD Swap-out": ["34.96%", "38.16%", "41.76%", "45.86%", "51.01%"],
        "Swap-in (aprovar bons)": [78045, 69938, 68922, 56204, 43640],
        "FPD Swap-in": ["19.34%", "21.32%", "23.60%", "26.16%", "29.04%"],
        "FPD Antigo": ["15.40%", "16.42%", "17.12%", "18.14%", "19.08%"],
        "FPD Novo": ["13.30%", "14.26%", "15.26%", "16.42%", "17.71%"],
        "Redução FPD": ["13.64%", "13.16%", "10.85%", "9.49%", "7.20%"],
    },
    "resumo": {
        "KS Score_01 (Bureau)": "26.71%",
        "KS LightGBM (OCI VM)": "34.42%",
        "Ganho KS": "+7.71 p.p.",
        "População OOT": "851,616",
        "Safras OOT": "202502, 202503",
        "Melhor Redução FPD": "13.64% (taxa 70%)",
        "Swap-out médio (FPD)": "42.4% (maus pagadores capturados)",
        "Swap-in médio (FPD)": "23.9% (bons clientes recuperados)",
    },
}


# ============================================================================
# CORES E ESTILO (mesma paleta Oracle/OCI do projeto)
# ============================================================================

COLORS = {
    "primary": "#C74634",      # Oracle Red
    "secondary": "#312D2A",    # Dark Brown
    "blue": "#1A4B8C",         # Deep blue
    "accent": "#00758F",       # Teal
    "green": "#2D8C3C",        # Success green
    "gold": "#D4A843",         # Gold
    "light_gray": "#F5F5F5",
    "red": "#E63946",          # Alert red
    "light_blue": "#74ADD1",   # Light blue
    "cell_green": "#4CAF50",   # Cell accord approve
    "cell_gray": "#757575",    # Cell accord reject
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
# GRÁFICO 11: Taxa FPD por Decil — Score_01 vs LightGBM (barras agrupadas)
# ============================================================================

def chart_decil_comparativo():
    fig, ax = plt.subplots(figsize=(14, 7))

    x = np.arange(10)
    width = 0.35

    fpd_lgbm = DECIL_LGBM["taxa_fpd_pct"]
    fpd_score01 = DECIL_SCORE01["taxa_fpd_pct"]

    bars1 = ax.bar(x - width/2, fpd_score01, width, label="Score_01 (Bureau)",
                   color=COLORS["light_blue"], edgecolor="white", linewidth=1.5, zorder=3)
    bars2 = ax.bar(x + width/2, fpd_lgbm, width, label="LightGBM (score_fpd)",
                   color=COLORS["primary"], edgecolor="white", linewidth=1.5, zorder=3)

    # Linhas conectando topos
    ax.plot(x - width/2, fpd_score01, color=COLORS["blue"], marker="s", markersize=5,
            linewidth=1.5, zorder=4, markerfacecolor="white", markeredgewidth=1.5, alpha=0.7)
    ax.plot(x + width/2, fpd_lgbm, color=COLORS["primary"], marker="o", markersize=5,
            linewidth=1.5, zorder=4, markerfacecolor="white", markeredgewidth=1.5, alpha=0.7)

    # Valores nas barras
    for bar, val in zip(bars1, fpd_score01):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=9,
                color=COLORS["blue"], fontweight="bold")
    for bar, val in zip(bars2, fpd_lgbm):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f"{val:.1f}%", ha="center", va="bottom", fontsize=9,
                color=COLORS["primary"], fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([f"D{i+1}" for i in range(10)], fontsize=12)
    ax.set_xlabel("Decil de Risco (1 = menor risco, 10 = maior risco)", fontsize=13)
    ax.set_ylabel("Taxa FPD (%)", fontsize=14)
    ax.set_title("Taxa de Inadimplência por Decil — Score_01 vs LightGBM",
                 fontsize=16, fontweight="bold", pad=15)
    ax.legend(fontsize=12, loc="upper left")

    # KS annotation box (posicionado no centro-direita, abaixo do título, sem sobrepor barras)
    textstr = (f"KS Score_01: {SWAP_META['ks_score01']:.2f}%\n"
               f"KS LightGBM: {SWAP_META['ks_lgbm']:.2f}%\n"
               f"Ganho: +{SWAP_META['ks_lgbm'] - SWAP_META['ks_score01']:.2f} p.p.")
    ax.text(0.98, 0.55, textstr, transform=ax.transAxes, fontsize=11, ha="right", va="top",
            bbox=dict(boxstyle="round,pad=0.4", facecolor=COLORS["light_gray"],
                      edgecolor=COLORS["secondary"], alpha=0.9),
            fontfamily="monospace")

    # Nota
    ax.text(0.98, 0.02, "OOT: 851,616 clientes — safras 202502/202503 (OCI VM, 2026-03-08)",
            transform=ax.transAxes, fontsize=9, ha="right", va="bottom",
            style="italic", color="black")

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 12: Matriz de Swap (heatmap para 80% de aprovação)
# ============================================================================

def chart_swap_matrix():
    # Usar taxa de 80%
    swap_80 = [s for s in SWAP_DATA if s["taxa"] == 80][0]

    fig, ax = plt.subplots(figsize=(12, 9))
    ax.axis("off")

    a = swap_80["A"]
    b = swap_80["B"]
    c = swap_80["C"]
    d = swap_80["D"]

    # Título
    ax.text(0.5, 0.97, "Matriz de Swap — Taxa de Aprovação = 80%",
            fontsize=18, fontweight="bold", ha="center", va="top", transform=ax.transAxes)

    # Headers
    ax.text(0.5, 0.88, "MODELO NOVO (LightGBM)", fontsize=14, fontweight="bold",
            ha="center", transform=ax.transAxes, color=COLORS["primary"])
    ax.text(0.35, 0.83, "Aprova", fontsize=13, fontweight="bold", ha="center",
            transform=ax.transAxes)
    ax.text(0.65, 0.83, "Rejeita", fontsize=13, fontweight="bold", ha="center",
            transform=ax.transAxes)

    ax.text(0.08, 0.65, "MODELO\nANTIGO\n(Score_01)", fontsize=12, fontweight="bold",
            ha="center", va="center", transform=ax.transAxes, color=COLORS["blue"],
            linespacing=1.5)
    ax.text(0.16, 0.72, "Aprova", fontsize=13, fontweight="bold", ha="center",
            transform=ax.transAxes)
    ax.text(0.16, 0.50, "Rejeita", fontsize=13, fontweight="bold", ha="center",
            transform=ax.transAxes)

    # Draw cell function
    def draw_cell(x, y, w, h, label, sublabel, n, fpd, color, text_color="white"):
        rect = plt.Rectangle((x, y), w, h, transform=ax.transAxes,
                              facecolor=color, edgecolor="white", linewidth=3,
                              clip_on=False, zorder=2)
        ax.add_patch(rect)
        ax.text(x + w/2, y + h*0.78, label, fontsize=15, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, color=text_color, zorder=3)
        ax.text(x + w/2, y + h*0.55, sublabel, fontsize=10,
                ha="center", va="center", transform=ax.transAxes, color=text_color,
                zorder=3, alpha=0.85, style="italic")
        ax.text(x + w/2, y + h*0.35, f"{n:,} clientes", fontsize=13,
                ha="center", va="center", transform=ax.transAxes, color=text_color,
                zorder=3, fontweight="bold")
        ax.text(x + w/2, y + h*0.15, f"FPD: {fpd:.1f}%", fontsize=11,
                ha="center", va="center", transform=ax.transAxes,
                color=text_color, zorder=3)

    w, h = 0.28, 0.22
    # A: Acordo — ambos aprovam (verde)
    draw_cell(0.22, 0.61, w, h, "A — Acordo", "(sem mudança)",
              a["n"], a["fpd"], COLORS["cell_green"])
    # B: Swap-out — antigo aprova, novo rejeita (vermelho)
    draw_cell(0.52, 0.61, w, h, "B — Swap-out", "(rejeitar maus)",
              b["n"], b["fpd"], COLORS["red"])
    # C: Swap-in — antigo rejeita, novo aprova (teal)
    draw_cell(0.22, 0.37, w, h, "C — Swap-in", "(recuperar bons)",
              c["n"], c["fpd"], COLORS["accent"])
    # D: Acordo — ambos rejeitam (cinza)
    draw_cell(0.52, 0.37, w, h, "D — Acordo", "(sem mudança)",
              d["n"], d["fpd"], COLORS["cell_gray"])

    # Resumo
    total_swap = b["n"] + c["n"]
    pct_swap = total_swap / SWAP_META["n_oot"] * 100

    resumo_y = 0.24
    ax.text(0.5, resumo_y, "Impacto da Substituição", fontsize=15, fontweight="bold",
            ha="center", transform=ax.transAxes, color=COLORS["secondary"])

    # Caixa de resumo com fundo
    resumo_box = plt.Rectangle((0.12, 0.02), 0.76, 0.20, transform=ax.transAxes,
                                facecolor=COLORS["light_gray"], edgecolor=COLORS["secondary"],
                                linewidth=1, clip_on=False, zorder=1, alpha=0.5)
    ax.add_patch(resumo_box)

    resumo_lines = [
        (f"Clientes que mudam de decisão: {total_swap:,} ({pct_swap:.1f}% da base)", COLORS["secondary"]),
        (f"Swap-out: {b['n']:,} clientes com FPD {b['fpd']:.1f}% seriam rejeitados", COLORS["red"]),
        (f"Swap-in: {c['n']:,} clientes com FPD {c['fpd']:.1f}% seriam aprovados", COLORS["accent"]),
        (f"FPD entre aprovados: {swap_80['fpd_old']:.2f}% → {swap_80['fpd_new']:.2f}% (redução de {swap_80['reducao']:.1f}%)", COLORS["green"]),
    ]

    for i, (line, color) in enumerate(resumo_lines):
        ax.text(0.5, resumo_y - 0.05 * (i + 1), line, fontsize=11,
                ha="center", transform=ax.transAxes, color=color, fontweight="bold" if i == 3 else "normal")

    return fig


# ============================================================================
# GRÁFICO 13: FPD entre Aprovados — Score_01 vs LightGBM (linhas)
# ============================================================================

def chart_fpd_por_taxa():
    fig, ax = plt.subplots(figsize=(14, 7))

    rates = [s["taxa"] for s in SWAP_DATA]
    fpd_old = [s["fpd_old"] for s in SWAP_DATA]
    fpd_new = [s["fpd_new"] for s in SWAP_DATA]

    # Linhas com marcadores
    ax.plot(rates, fpd_old, marker="s", markersize=12, linewidth=3,
            color=COLORS["light_blue"], label="Score_01 (Bureau)", zorder=3,
            markerfacecolor="white", markeredgewidth=2.5, markeredgecolor=COLORS["blue"])
    ax.plot(rates, fpd_new, marker="o", markersize=12, linewidth=3,
            color=COLORS["primary"], label="LightGBM (score_fpd)", zorder=3,
            markerfacecolor="white", markeredgewidth=2.5, markeredgecolor=COLORS["primary"])

    # Área entre as curvas (ganho)
    ax.fill_between(rates, fpd_old, fpd_new, alpha=0.12, color=COLORS["green"], zorder=2)

    # Valores nos pontos
    for r, fo, fn in zip(rates, fpd_old, fpd_new):
        ax.text(r, fo + 0.25, f"{fo:.2f}%", ha="center", va="bottom", fontsize=11,
                color=COLORS["blue"], fontweight="bold")
        ax.text(r, fn - 0.25, f"{fn:.2f}%", ha="center", va="top", fontsize=11,
                color=COLORS["primary"], fontweight="bold")

    # Annotations de redução com setas
    for s in SWAP_DATA:
        r = s["taxa"]
        mid_y = (s["fpd_old"] + s["fpd_new"]) / 2
        ax.annotate(f"-{s['reducao']:.1f}%",
                    xy=(r + 1.5, mid_y), fontsize=10, fontweight="bold", color=COLORS["green"],
                    ha="left", va="center",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="#E8F5E9",
                              edgecolor=COLORS["green"], alpha=0.85))

    ax.set_xlabel("Taxa de Aprovação (%)", fontsize=14)
    ax.set_ylabel("Taxa FPD entre Aprovados (%)", fontsize=14)
    ax.set_title("Redução de Inadimplência — Score_01 vs LightGBM por Taxa de Aprovação",
                 fontsize=16, fontweight="bold", pad=15)
    ax.legend(fontsize=12, loc="upper left")
    ax.set_xticks(rates)
    ax.set_xticklabels([f"{r}%" for r in rates], fontsize=12)
    ax.set_ylim(12, 21)

    # Nota
    ax.text(0.98, 0.02, "A área verde representa a redução de FPD proporcionada pelo modelo novo",
            transform=ax.transAxes, fontsize=9, ha="right", va="bottom",
            style="italic", color="gray")

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 14: Volume de Swap por Taxa de Aprovação (barras agrupadas)
# ============================================================================

def chart_swap_volume():
    fig, ax = plt.subplots(figsize=(14, 7))

    rates = [s["taxa"] for s in SWAP_DATA]
    swap_out_n = [s["B"]["n"] for s in SWAP_DATA]
    swap_in_n = [s["C"]["n"] for s in SWAP_DATA]
    swap_out_fpd = [s["B"]["fpd"] for s in SWAP_DATA]
    swap_in_fpd = [s["C"]["fpd"] for s in SWAP_DATA]

    x = np.arange(len(rates))
    width = 0.35

    bars1 = ax.bar(x - width/2, swap_out_n, width, label="Swap-out (rejeitar maus)",
                   color=COLORS["red"], edgecolor="white", linewidth=1.5, zorder=3, alpha=0.9)
    bars2 = ax.bar(x + width/2, swap_in_n, width, label="Swap-in (aprovar bons)",
                   color=COLORS["accent"], edgecolor="white", linewidth=1.5, zorder=3, alpha=0.9)

    # Valores + taxa FPD
    for bar, n, fpd in zip(bars1, swap_out_n, swap_out_fpd):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 800,
                f"{n:,}", ha="center", va="bottom",
                fontsize=10, fontweight="bold", color=COLORS["red"])
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() - 3000,
                f"FPD {fpd:.1f}%", ha="center", va="top",
                fontsize=9, color="white", fontweight="bold")
    for bar, n, fpd in zip(bars2, swap_in_n, swap_in_fpd):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 800,
                f"{n:,}", ha="center", va="bottom",
                fontsize=10, fontweight="bold", color=COLORS["accent"])
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() - 3000,
                f"FPD {fpd:.1f}%", ha="center", va="top",
                fontsize=9, color="white", fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([f"{r}%" for r in rates], fontsize=12)
    ax.set_xlabel("Taxa de Aprovação", fontsize=14)
    ax.set_ylabel("Quantidade de Clientes", fontsize=14)
    ax.set_title("Volume de Swap-in e Swap-out por Taxa de Aprovação",
                 fontsize=16, fontweight="bold", pad=15)
    ax.legend(fontsize=12, loc="upper right")
    ax.grid(axis="x", visible=False)

    # Nota explicativa
    ax.text(0.02, 0.02,
            "Swap-out: clientes que o modelo antigo APROVA mas o novo REJEITA → captura maus pagadores\n"
            "Swap-in: clientes que o modelo antigo REJEITA mas o novo APROVA → recupera bons clientes",
            transform=ax.transAxes, fontsize=9, va="bottom", style="italic", color="black")

    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 15: Resumo do Ganho — Redução de FPD (barras horizontais)
# ============================================================================

def chart_resumo_ganho():
    fig, ax = plt.subplots(figsize=(14, 7))

    rates = [s["taxa"] for s in SWAP_DATA]
    reducoes = [s["reducao"] for s in SWAP_DATA]
    fpd_old = [s["fpd_old"] for s in SWAP_DATA]
    fpd_new = [s["fpd_new"] for s in SWAP_DATA]

    y = np.arange(len(rates))
    labels = [f"Taxa {r}%" for r in rates]

    # Barras de redução (maiores no topo)
    colors_bar = [COLORS["green"] if r >= 10 else COLORS["accent"] for r in reducoes]
    bars = ax.barh(y, reducoes, height=0.5, color=colors_bar,
                   edgecolor="white", linewidth=1.5, zorder=3)

    # Valores nas barras
    for bar, red, fo, fn in zip(bars, reducoes, fpd_old, fpd_new):
        # Porcentagem de redução
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                f"-{red:.1f}%", ha="left", va="center",
                fontsize=13, fontweight="bold", color=COLORS["green"])
        # Detalhe dentro da barra
        if bar.get_width() >= 8:
            ax.text(bar.get_width() / 2, bar.get_y() + bar.get_height()/2,
                    f"{fo:.2f}% → {fn:.2f}%", ha="center", va="center",
                    fontsize=11, fontweight="bold", color="white")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=13)
    ax.set_xlabel("Redução Relativa de FPD (%)", fontsize=14)
    ax.set_title("Ganho do Modelo Novo — Redução de FPD entre Aprovados",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_xlim(0, 18)
    ax.grid(axis="y", visible=False)

    # Annotation box com métricas chave
    textstr = (
        f"Score_01 (Bureau):  KS = {SWAP_META['ks_score01']:.2f}%\n"
        f"LightGBM (OCI VM): KS = {SWAP_META['ks_lgbm']:.2f}%\n"
        f"Ganho KS: +{SWAP_META['ks_lgbm'] - SWAP_META['ks_score01']:.2f} p.p.\n"
        f"OOT: {SWAP_META['n_oot']:,} clientes"
    )
    ax.text(0.98, 0.95, textstr, transform=ax.transAxes, fontsize=11, ha="right", va="top",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=COLORS["light_gray"],
                      edgecolor=COLORS["secondary"], alpha=0.9),
            fontfamily="monospace")

    fig.tight_layout()
    return fig


# ============================================================================
# EXPORTAR DADOS PARA POWERPOINT
# ============================================================================

def export_powerpoint_data(output_dir):
    """Exporta dados tabulares em JSON e TXT para fácil cópia para PowerPoint."""

    # JSON
    filepath_json = os.path.join(output_dir, "dados_swap_analysis.json")
    with open(filepath_json, "w", encoding="utf-8") as f:
        json.dump(POWERPOINT_DATA, f, indent=2, ensure_ascii=False)

    # TXT formatado
    filepath_txt = os.path.join(output_dir, "dados_swap_analysis.txt")
    with open(filepath_txt, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("SWAP-IN / SWAP-OUT ANALYSIS — DADOS PARA APRESENTAÇÃO\n")
        f.write("=" * 80 + "\n\n")

        f.write("1. RESUMO\n")
        f.write("-" * 80 + "\n")
        for k, v in POWERPOINT_DATA["resumo"].items():
            f.write(f"  {k:<35} {v}\n")
        f.write("\n")

        f.write("2. SWAP POR TAXA DE APROVAÇÃO\n")
        f.write("-" * 80 + "\n")
        d = POWERPOINT_DATA["swap_por_taxa"]
        header = f"{'Taxa':<8} {'Swap-out':>10} {'FPD S-out':>10} {'Swap-in':>10} {'FPD S-in':>10} {'FPD Ant.':>10} {'FPD Novo':>10} {'Redução':>10}\n"
        f.write(header)
        f.write("-" * 80 + "\n")
        for i in range(len(d["Taxa Aprovação"])):
            f.write(f"{d['Taxa Aprovação'][i]:<8} "
                    f"{d['Swap-out (rejeitar maus)'][i]:>10,} "
                    f"{d['FPD Swap-out'][i]:>10} "
                    f"{d['Swap-in (aprovar bons)'][i]:>10,} "
                    f"{d['FPD Swap-in'][i]:>10} "
                    f"{d['FPD Antigo'][i]:>10} "
                    f"{d['FPD Novo'][i]:>10} "
                    f"{d['Redução FPD'][i]:>10}\n")
        f.write("\n")

        f.write("3. DECIL LGBM (score_fpd)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Decil':<8} {'Qtd':>10} {'FPD (%)':>10} {'FPD Acum':>10}\n")
        f.write("-" * 80 + "\n")
        for i in range(10):
            f.write(f"D{DECIL_LGBM['decil'][i]:<7} "
                    f"{DECIL_LGBM['qtd'][i]:>10,} "
                    f"{DECIL_LGBM['taxa_fpd_pct'][i]:>9.2f}% "
                    f"{DECIL_LGBM['fpd_acum'][i]:>9.1f}%\n")
        f.write("\n")

        f.write("4. DECIL SCORE_01 (bureau)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Decil':<8} {'Qtd':>10} {'FPD (%)':>10} {'Score Min':>10} {'Score Max':>10}\n")
        f.write("-" * 80 + "\n")
        for i in range(10):
            f.write(f"D{DECIL_SCORE01['decil'][i]:<7} "
                    f"{DECIL_SCORE01['qtd'][i]:>10,} "
                    f"{DECIL_SCORE01['taxa_fpd_pct'][i]:>9.2f}% "
                    f"{DECIL_SCORE01['score_min'][i]:>10} "
                    f"{DECIL_SCORE01['score_max'][i]:>10}\n")
        f.write("\n")

        f.write("5. INTERPRETAÇÃO\n")
        f.write("-" * 80 + "\n")
        f.write("- Swap-out = clientes que o modelo antigo APROVARIA mas o novo REJEITA\n")
        f.write("  → São maus pagadores (FPD ~42%) que estávamos aprovando\n")
        f.write("- Swap-in = clientes que o modelo antigo REJEITARIA mas o novo APROVA\n")
        f.write("  → São clientes com FPD menor (~24%) que estávamos rejeitando\n")
        f.write("- Na taxa de 80%, a substituição reduziria a FPD em 10.85%\n")
        f.write("- O LightGBM tem KS 7.71 p.p. superior ao Score_01\n")
        f.write("\n")

        f.write("Fonte: swap_analysis_oci.py (VM OCI, 2026-03-08)\n")

    print(f"  Dados exportados: {filepath_json}")
    print(f"  Dados exportados: {filepath_txt}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    output_dir = ensure_output_dir()
    print("=" * 60)
    print("GERANDO GRÁFICOS — SWAP-IN / SWAP-OUT")
    print("=" * 60)
    print(f"  OOT: {SWAP_META['n_oot']:,} clientes")
    print(f"  KS Score_01: {SWAP_META['ks_score01']:.2f}%")
    print(f"  KS LightGBM: {SWAP_META['ks_lgbm']:.2f}%")

    charts = [
        ("11_decil_comparativo.png", chart_decil_comparativo, "Decil Comparativo (Score_01 vs LightGBM)"),
        ("12_matriz_swap.png", chart_swap_matrix, "Matriz de Swap (80% aprovação)"),
        ("13_fpd_por_taxa.png", chart_fpd_por_taxa, "FPD por Taxa de Aprovação"),
        ("14_swap_volume.png", chart_swap_volume, "Volume de Swap"),
        ("15_resumo_ganho.png", chart_resumo_ganho, "Resumo do Ganho — Redução FPD"),
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
