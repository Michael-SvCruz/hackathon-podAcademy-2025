#!/usr/bin/env python3
"""
comparacao_databricks_oci.py
============================
Gráficos comparando Databricks (antes) vs OCI (depois) da migração.

Gráficos gerados:
  06. KS Incremental lado a lado (Databricks vs OCI VM)
  07. Métricas do modelo final (KS, AUC, GINI, Features)
  08. Custo mensal: Databricks vs OCI
  09. Arquitetura — componentes e ganhos
  10. Timeline da migração (fases)

Uso:
    python3 comparacao_databricks_oci.py
    # Gera PNGs na pasta output/

Dados:
    - Databricks: notebook 20260202 (KS=33.94%, 264 features)
    - OCI VM: metricas/ks_incremental_20260307_1850.json (KS=34.39%, 261 features)
    - Custos: CLAUDE.md + docs/architecture/
"""

import os
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
import numpy as np

# ============================================================================
# DADOS
# ============================================================================

BENCHMARK = 33.10

# KS Incremental — Databricks (notebook 20260202)
# Score_01 e Final são exatos; intermediários são aproximados
DATABRICKS_KS = {
    "labels": ["Score_01", "+ Score_02", "+ Telco", "+ Cadastro", "+ Recarga", "+ Pag+Atraso"],
    "ks_oot": [28.81, 29.80, 30.50, 31.20, 32.10, 33.94],
    "n_features": [2, 4, 72, 105, 179, 264],
}

# KS Incremental — OCI VM (execução 2026-03-07, todos exatos)
OCI_VM_KS = {
    "labels": ["Score_01", "+ Score_02", "+ Telco", "+ Cadastro", "+ Recarga", "+ Pag+Atraso"],
    "ks_oot": [26.67, 31.25, 31.51, 31.70, 33.95, 34.39],
    "n_features": [1, 2, 89, 95, 160, 261],
}

# Métricas finais
DATABRICKS_FINAL = {
    "ks_oot": 33.94, "auc_oot": 0.7327, "gini_oot": 46.54,
    "n_features": 264, "best_iteration": None,  # não documentado
}
OCI_VM_FINAL = {
    "ks_oot": 34.39, "auc_oot": 0.7327, "gini_oot": 46.54,
    "n_features": 261, "best_iteration": 900,
}

# Custos mensais estimados (produção)
CUSTOS = {
    "Databricks": {
        "Plataforma (DBUs)": 800,
        "Compute (clusters)": 400,
        "Storage (DBFS)": 50,
        "Jobs/Workflows": 200,
        "Total": 1450,
    },
    "OCI": {
        "Data Flow (3×21 apps)": 240,
        "Storage (295 GB)": 10,
        "Airflow VM (E3.Flex)": 60,
        "Modelo VM (E5.Flex)": 40,
        "Total": 350,
    },
}

# Componentes da arquitetura
COMPONENTES = {
    "Databricks": ["Unity Catalog", "Notebooks", "Spark Clusters", "Delta Lake", "DBFS", "Workflows"],
    "OCI": ["Data Flow (Spark)", "Object Storage", "Airflow (Docker)", "VM Modelo", "Terraform IaC", "Instance Principal"],
}

# Timeline de migração (fases)
TIMELINE = [
    ("Fase 0-1", "IAM + Setup", 2),
    ("Fase 2-3", "Network + Storage", 3),
    ("Fase 4", "Data Flow (21 apps)", 5),
    ("Fase 5", "Airflow VM", 3),
    ("Fase 6A", "Landing → Bronze", 4),
    ("Fase 6B", "Silver → Gold → ABT", 7),
    ("Fase 7", "Data Science", 2),
    ("Fase 8", "Modelo VM + DAG", 4),
]

# ============================================================================
# CORES E ESTILO
# ============================================================================

COLORS = {
    "databricks": "#FF3621",   # Databricks red/orange
    "oci": "#C74634",          # Oracle Red
    "oci_dark": "#8B2F24",     # Oracle darker
    "green": "#2D8C3C",
    "secondary": "#312D2A",
    "light_gray": "#F5F5F5",
    "benchmark": "#E63946",
    "blue": "#1A4B8C",
    "teal": "#00758F",
    "gold": "#D4A843",
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
# GRÁFICO 06: KS Incremental lado a lado (Databricks vs OCI VM)
# ============================================================================

def chart_ks_side_by_side():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8), sharey=True)

    # --- Databricks ---
    labels = DATABRICKS_KS["labels"]
    ks_db = DATABRICKS_KS["ks_oot"]
    x = np.arange(len(labels))

    bars1 = ax1.bar(x, ks_db, width=0.55, color=COLORS["databricks"], alpha=0.85,
                    edgecolor="white", linewidth=1.5, zorder=3)
    ax1.plot(x, ks_db, color=COLORS["secondary"], marker="o", markersize=7,
             linewidth=2, zorder=4, markerfacecolor="white", markeredgewidth=2)
    ax1.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2,
                label=f"Benchmark = {BENCHMARK}%")

    for bar, val in zip(bars1, ks_db):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.25,
                 f"{val:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=10, rotation=15, ha="right")
    ax1.set_ylabel("KS OOT (%)", fontsize=14)
    ax1.set_title("Databricks\n(KS final = 33.94%)", fontsize=15, fontweight="bold",
                  color=COLORS["databricks"])
    ax1.set_ylim(22, 38)
    ax1.legend(fontsize=10)
    ax1.text(0.5, 0.02, "* Score_01 e Final exatos; intermediários aproximados",
             transform=ax1.transAxes, fontsize=8, ha="center", style="italic", color="gray")

    # --- OCI VM ---
    ks_oci = OCI_VM_KS["ks_oot"]

    bars2 = ax2.bar(x, ks_oci, width=0.55, color=COLORS["oci"], alpha=0.85,
                    edgecolor="white", linewidth=1.5, zorder=3)
    ax2.plot(x, ks_oci, color=COLORS["secondary"], marker="o", markersize=7,
             linewidth=2, zorder=4, markerfacecolor="white", markeredgewidth=2)
    ax2.axhline(y=BENCHMARK, color=COLORS["benchmark"], linestyle="--", linewidth=2,
                label=f"Benchmark = {BENCHMARK}%")

    for bar, val in zip(bars2, ks_oci):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.25,
                 f"{val:.2f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=10, rotation=15, ha="right")
    ax2.set_title("OCI VM\n(KS final = 34.39%)", fontsize=15, fontweight="bold",
                  color=COLORS["oci"])
    ax2.legend(fontsize=10)
    ax2.text(0.5, 0.02, "Todos os valores exatos — treinamento independente por ABT",
             transform=ax2.transAxes, fontsize=8, ha="center", style="italic", color="gray")

    # Seta de ganho entre os dois
    fig.text(0.5, 0.95, "+0.45 p.p.", fontsize=20, fontweight="bold", ha="center",
             va="center", color=COLORS["green"],
             bbox=dict(boxstyle="round,pad=0.4", facecolor="#E8F5E9", edgecolor=COLORS["green"]))

    fig.suptitle("KS Incremental — Databricks vs OCI VM", fontsize=18, fontweight="bold", y=1.02)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return fig


# ============================================================================
# GRÁFICO 07: Métricas do Modelo Final (radar-like grouped bar)
# ============================================================================

def chart_metricas_comparacao():
    fig, axes = plt.subplots(1, 4, figsize=(18, 6))

    metricas = [
        ("KS OOT (%)", DATABRICKS_FINAL["ks_oot"], OCI_VM_FINAL["ks_oot"], 30, 36),
        ("AUC OOT", DATABRICKS_FINAL["auc_oot"], OCI_VM_FINAL["auc_oot"], 0.70, 0.76),
        ("GINI OOT (%)", DATABRICKS_FINAL["gini_oot"], OCI_VM_FINAL["gini_oot"], 40, 50),
        ("Features", DATABRICKS_FINAL["n_features"], OCI_VM_FINAL["n_features"], 240, 280),
    ]

    for ax, (nome, val_db, val_oci, ymin, ymax) in zip(axes, metricas):
        x = [0, 1]
        bars = ax.bar(x, [val_db, val_oci], width=0.5,
                       color=[COLORS["databricks"], COLORS["oci"]],
                       edgecolor="white", linewidth=2, zorder=3)

        # Valores
        for bar, val in zip(bars, [val_db, val_oci]):
            fmt = f"{val:.2f}" if isinstance(val, float) and val < 1 else f"{val:.2f}" if isinstance(val, float) else str(val)
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + (ymax - ymin) * 0.02,
                    fmt, ha="center", va="bottom", fontsize=14, fontweight="bold")

        # Delta
        diff = val_oci - val_db
        if abs(diff) > 0.001:
            sign = "+" if diff > 0 else ""
            fmt_diff = f"{sign}{diff:.2f}" if isinstance(diff, float) and abs(diff) < 10 else f"{sign}{diff:.0f}"
            color_diff = COLORS["green"] if diff > 0 else COLORS["benchmark"]
            ax.text(0.5, ymin + (ymax - ymin) * 0.1, fmt_diff,
                    ha="center", fontsize=12, fontweight="bold", color=color_diff,
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor=color_diff, alpha=0.9))

        ax.set_xticks(x)
        ax.set_xticklabels(["Databricks", "OCI VM"], fontsize=10)
        ax.set_title(nome, fontsize=14, fontweight="bold")
        ax.set_ylim(ymin, ymax)
        ax.grid(axis="x", visible=False)

    fig.suptitle("Comparação de Métricas — Databricks vs OCI VM", fontsize=17, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return fig


# ============================================================================
# GRÁFICO 08: Custo Mensal (Databricks vs OCI)
# ============================================================================

def chart_custos():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7),
                                    gridspec_kw={"width_ratios": [3, 1]})

    # --- Barras empilhadas ---
    db_items = {k: v for k, v in CUSTOS["Databricks"].items() if k != "Total"}
    oci_items = {k: v for k, v in CUSTOS["OCI"].items() if k != "Total"}

    # Databricks (stacked)
    db_labels = list(db_items.keys())
    db_values = list(db_items.values())
    db_colors = ["#FF3621", "#FF6B4A", "#FF9B7A", "#FFB89E"]

    bottom = 0
    for i, (label, val) in enumerate(zip(db_labels, db_values)):
        ax1.bar(0, val, bottom=bottom, width=0.4, color=db_colors[i % len(db_colors)],
                edgecolor="white", linewidth=1.5, label=f"DB: {label}")
        if val >= 100:
            ax1.text(0, bottom + val / 2, f"${val}", ha="center", va="center",
                     fontsize=10, fontweight="bold", color="white")
        bottom += val

    # OCI (stacked)
    oci_labels = list(oci_items.keys())
    oci_values = list(oci_items.values())
    oci_colors = ["#C74634", "#8B2F24", "#D4A843", "#00758F"]

    bottom = 0
    for i, (label, val) in enumerate(zip(oci_labels, oci_values)):
        ax1.bar(1, val, bottom=bottom, width=0.4, color=oci_colors[i % len(oci_colors)],
                edgecolor="white", linewidth=1.5, label=f"OCI: {label}")
        if val >= 30:
            ax1.text(1, bottom + val / 2, f"${val}", ha="center", va="center",
                     fontsize=10, fontweight="bold", color="white")
        bottom += val

    # Totais no topo
    ax1.text(0, CUSTOS["Databricks"]["Total"] + 20, f"${CUSTOS['Databricks']['Total']}/mês",
             ha="center", va="bottom", fontsize=16, fontweight="bold", color=COLORS["databricks"])
    ax1.text(1, CUSTOS["OCI"]["Total"] + 20, f"${CUSTOS['OCI']['Total']}/mês",
             ha="center", va="bottom", fontsize=16, fontweight="bold", color=COLORS["oci"])

    ax1.set_xticks([0, 1])
    ax1.set_xticklabels(["Databricks\n(estimado)", "OCI\n(produção)"], fontsize=13)
    ax1.set_ylabel("Custo Mensal (USD)", fontsize=14)
    ax1.set_title("Custo Mensal Estimado", fontsize=16, fontweight="bold")
    ax1.set_ylim(0, 1700)
    ax1.grid(axis="x", visible=False)

    # Economia
    economia = CUSTOS["Databricks"]["Total"] - CUSTOS["OCI"]["Total"]
    pct = economia / CUSTOS["Databricks"]["Total"] * 100
    ax1.annotate(f"Economia:\n${economia}/mês\n({pct:.0f}%)",
                 xy=(0.5, 700), fontsize=14, fontweight="bold", color=COLORS["green"],
                 ha="center", va="center",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#E8F5E9", edgecolor=COLORS["green"]))

    # --- Legenda detalhada ---
    ax2.axis("off")
    ax2.set_title("Detalhamento", fontsize=14, fontweight="bold")

    # Databricks
    y = 0.95
    ax2.text(0.05, y, "Databricks", fontsize=13, fontweight="bold", color=COLORS["databricks"],
             transform=ax2.transAxes)
    y -= 0.06
    for label, val in db_items.items():
        ax2.text(0.1, y, f"• {label}: ${val}", fontsize=10, transform=ax2.transAxes)
        y -= 0.05
    y -= 0.03

    # OCI
    ax2.text(0.05, y, "OCI (Produção)", fontsize=13, fontweight="bold", color=COLORS["oci"],
             transform=ax2.transAxes)
    y -= 0.06
    for label, val in oci_items.items():
        ax2.text(0.1, y, f"• {label}: ${val}", fontsize=10, transform=ax2.transAxes)
        y -= 0.05
    y -= 0.05

    # Nota
    ax2.text(0.05, y, "Nota: Databricks estimado para\nuso equivalente. OCI inclui\nStart/Stop da VM Modelo.", fontsize=9,
             transform=ax2.transAxes, style="italic", color="gray",
             bbox=dict(boxstyle="round,pad=0.3", facecolor=COLORS["light_gray"], edgecolor="gray", alpha=0.5))

    fig.suptitle("Comparação de Custos — Databricks vs OCI", fontsize=17, fontweight="bold", y=1.01)
    fig.tight_layout()
    return fig


# ============================================================================
# GRÁFICO 09: Ganhos da Migração (resumo visual)
# ============================================================================

def chart_ganhos_migracao():
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis("off")

    # Título
    ax.text(0.5, 0.97, "Ganhos da Migração Databricks → OCI", fontsize=20,
            fontweight="bold", ha="center", va="top", transform=ax.transAxes)

    # Cards de ganho
    cards = [
        {
            "titulo": "Performance",
            "valor": "+0.45 p.p.",
            "detalhe": "KS OOT: 33.94% → 34.39%",
            "subtexto": "261 features (vs 264)\n900 iterações com early stopping",
            "cor": COLORS["green"],
            "icone": "^",
        },
        {
            "titulo": "Custo",
            "valor": "-76%",
            "detalhe": "$1,450 → $350 /mês",
            "subtexto": "VM Start/Stop: ~$40/mês\nObject Storage: $10/mês",
            "cor": COLORS["teal"],
            "icone": "v",
        },
        {
            "titulo": "Infraestrutura",
            "valor": "100% IaC",
            "detalhe": "Terraform + 6 módulos",
            "subtexto": "Reprodutível em ~2h\nInstance Principal (sem chaves)",
            "cor": COLORS["blue"],
            "icone": "s",
        },
        {
            "titulo": "Orquestração",
            "valor": "Airflow",
            "detalhe": "21 Data Flow apps + Modelo",
            "subtexto": "ETL → Modelo encadeado\nDeploy automatizado",
            "cor": COLORS["gold"],
            "icone": "D",
        },
    ]

    card_width = 0.22
    card_height = 0.65
    gap = 0.02
    start_x = 0.5 - (4 * card_width + 3 * gap) / 2

    for i, card in enumerate(cards):
        x = start_x + i * (card_width + gap)
        y = 0.08

        # Card background
        rect = plt.Rectangle((x, y), card_width, card_height, transform=ax.transAxes,
                              facecolor="white", edgecolor=card["cor"], linewidth=3,
                              clip_on=False, zorder=2, joinstyle="round")
        ax.add_patch(rect)

        # Header bar
        header = plt.Rectangle((x, y + card_height - 0.08), card_width, 0.08,
                                transform=ax.transAxes, facecolor=card["cor"],
                                clip_on=False, zorder=3)
        ax.add_patch(header)

        # Título (no header)
        ax.text(x + card_width / 2, y + card_height - 0.04, card["titulo"],
                fontsize=13, fontweight="bold", color="white",
                ha="center", va="center", transform=ax.transAxes, zorder=4)

        # Valor grande
        ax.text(x + card_width / 2, y + card_height - 0.20, card["valor"],
                fontsize=24, fontweight="bold", color=card["cor"],
                ha="center", va="center", transform=ax.transAxes, zorder=3)

        # Detalhe
        ax.text(x + card_width / 2, y + card_height - 0.33, card["detalhe"],
                fontsize=10, color=COLORS["secondary"],
                ha="center", va="center", transform=ax.transAxes, zorder=3)

        # Subtexto
        ax.text(x + card_width / 2, y + card_height - 0.50, card["subtexto"],
                fontsize=9, color="gray", ha="center", va="center",
                transform=ax.transAxes, zorder=3, style="italic", linespacing=1.5)

    # Footer
    ax.text(0.5, 0.02, "Hackathon PodAcademy 2025 — Migração Databricks → Oracle Cloud Infrastructure",
            fontsize=10, ha="center", va="bottom", transform=ax.transAxes, color="gray")

    return fig


# ============================================================================
# GRÁFICO 10: Timeline de Migração
# ============================================================================

def chart_timeline():
    fig, ax = plt.subplots(figsize=(16, 6))

    n = len(TIMELINE)
    total_dias = sum(d for _, _, d in TIMELINE)

    # Posições acumuladas
    colors_tl = ["#1A4B8C", "#2166AC", "#4393C3", "#74ADD1",
                 "#ABD9E9", "#C74634", "#D4A843", "#00758F"]

    cumulative = 0
    for i, (fase, desc, dias) in enumerate(TIMELINE):
        # Barra horizontal (Gantt-like)
        bar = ax.barh(0, dias, left=cumulative, height=0.4,
                       color=colors_tl[i % len(colors_tl)], edgecolor="white", linewidth=2, zorder=3)

        # Label na barra
        cx = cumulative + dias / 2
        if dias >= 3:
            ax.text(cx, 0, f"{fase}\n({dias}d)", ha="center", va="center",
                    fontsize=9, fontweight="bold", color="white", zorder=4)

        # Descrição acima/abaixo alternando
        y_desc = 0.35 if i % 2 == 0 else -0.35
        ax.text(cx, y_desc, desc, ha="center", va="center", fontsize=9,
                rotation=0, color=COLORS["secondary"],
                bbox=dict(boxstyle="round,pad=0.2", facecolor=COLORS["light_gray"],
                          edgecolor="gray", alpha=0.7))

        cumulative += dias

    ax.set_xlim(-0.5, total_dias + 0.5)
    ax.set_ylim(-0.8, 0.8)
    ax.set_xlabel("Dias de Implementação", fontsize=13)
    ax.set_title(f"Timeline da Migração — {n} Fases, ~{total_dias} dias",
                 fontsize=16, fontweight="bold", pad=15)
    ax.set_yticks([])
    ax.grid(axis="y", visible=False)

    # Marcador de conclusão
    ax.annotate("Pipeline\nCompleto!", xy=(total_dias, 0), xytext=(total_dias + 1, 0.4),
                fontsize=12, fontweight="bold", color=COLORS["green"],
                arrowprops=dict(arrowstyle="->", color=COLORS["green"], lw=2),
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#E8F5E9", edgecolor=COLORS["green"]))

    fig.tight_layout()
    return fig


# ============================================================================
# EXPORTAR DADOS COMPARATIVOS
# ============================================================================

def export_comparacao_data(output_dir):
    data = {
        "ks_incremental_databricks": {
            "labels": DATABRICKS_KS["labels"],
            "ks_oot": DATABRICKS_KS["ks_oot"],
            "n_features": DATABRICKS_KS["n_features"],
            "nota": "Score_01 e Final exatos; intermediários aproximados",
        },
        "ks_incremental_oci_vm": {
            "labels": OCI_VM_KS["labels"],
            "ks_oot": OCI_VM_KS["ks_oot"],
            "n_features": OCI_VM_KS["n_features"],
            "nota": "Todos valores exatos — treinamento independente por ABT",
        },
        "metricas_finais": {
            "databricks": DATABRICKS_FINAL,
            "oci_vm": OCI_VM_FINAL,
            "benchmark": BENCHMARK,
        },
        "custos_mensais": CUSTOS,
        "ganho_ks": {
            "databricks": 33.94,
            "oci_vm": 34.39,
            "delta": 0.45,
            "vs_benchmark_databricks": 0.84,
            "vs_benchmark_oci": 1.29,
        },
    }

    filepath = os.path.join(output_dir, "dados_comparacao_databricks_oci.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # TXT formatado
    filepath_txt = os.path.join(output_dir, "dados_comparacao_databricks_oci.txt")
    with open(filepath_txt, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("COMPARAÇÃO DATABRICKS vs OCI VM\n")
        f.write("=" * 70 + "\n\n")

        f.write("1. MÉTRICAS DO MODELO FINAL\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'Métrica':<20} {'Databricks':<15} {'OCI VM':<15} {'Delta':<15}\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'KS OOT (%)':<20} {33.94:<15.2f} {34.39:<15.2f} {'+0.45':<15}\n")
        f.write(f"{'AUC OOT':<20} {0.7327:<15.4f} {0.7327:<15.4f} {'=':<15}\n")
        f.write(f"{'GINI OOT (%)':<20} {46.54:<15.2f} {46.54:<15.2f} {'=':<15}\n")
        f.write(f"{'Features':<20} {264:<15} {261:<15} {'-3':<15}\n")
        f.write(f"{'vs Benchmark':<20} {'+0.84 p.p.':<15} {'+1.29 p.p.':<15} {'+0.45':<15}\n")
        f.write("\n")

        f.write("2. CUSTO MENSAL (PRODUÇÃO)\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'Databricks':<20} $1,450/mês\n")
        f.write(f"{'OCI':<20} $350/mês\n")
        f.write(f"{'Economia':<20} $1,100/mês (-76%)\n")
        f.write("\n")

        f.write("3. GANHOS DA MIGRAÇÃO\n")
        f.write("-" * 70 + "\n")
        f.write("- Performance: +0.45 p.p. (KS 33.94% → 34.39%)\n")
        f.write("- Custo: -76% ($1,450 → $350/mês)\n")
        f.write("- Infraestrutura: 100% IaC (Terraform, 6 módulos)\n")
        f.write("- Orquestração: Airflow (ETL + Modelo encadeado)\n")
        f.write("- Reprodutibilidade: Clone + Terraform + Deploy (~2h)\n")

    print(f"  Dados exportados: {filepath}")
    print(f"  Dados exportados: {filepath_txt}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    output_dir = ensure_output_dir()
    print("=" * 60)
    print("GERANDO GRÁFICOS — COMPARAÇÃO DATABRICKS vs OCI")
    print("=" * 60)

    charts = [
        ("06_ks_databricks_vs_oci.png", chart_ks_side_by_side, "KS Incremental lado a lado"),
        ("07_metricas_comparacao.png", chart_metricas_comparacao, "Métricas do Modelo Final"),
        ("08_custos_databricks_vs_oci.png", chart_custos, "Custos Mensais"),
        ("09_ganhos_migracao.png", chart_ganhos_migracao, "Ganhos da Migração"),
        ("10_timeline_migracao.png", chart_timeline, "Timeline da Migração"),
    ]

    for filename, chart_func, description in charts:
        print(f"\n  Gerando: {description}...")
        fig = chart_func()
        filepath = os.path.join(output_dir, filename)
        fig.savefig(filepath, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        print(f"  Salvo: {filepath}")

    print("\n  Exportando dados comparativos...")
    export_comparacao_data(output_dir)

    print("\n" + "=" * 60)
    print(f"CONCLUÍDO — {len(charts)} gráficos + dados em: {output_dir}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
