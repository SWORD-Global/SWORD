#!/usr/bin/env python
"""Generate report figures for facc correction methodology document.

Reads the per-region summary JSONs and CSV outputs from
``correct_facc_denoise.py`` and produces 5 figures:

  Fig 1: Before/after junction conservation + bifurcation ratios
  Fig 2: Correction type breakdown by region (stacked bar)
  Fig 3: Scalability comparison + per-region bar chart
  Fig 4: Isotonic regression (PAVA) example on a synthetic 1:1 chain
  Fig 5: Willamette basin (7822*) original vs corrected scatter

Usage::

    python scripts/visualization/generate_facc_report_figures.py \\
        --input-dir output/facc_detection \\
        --db data/duckdb/sword_v17c.duckdb \\
        --v17b data/duckdb/sword_v17b.duckdb
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]

# Consistent type ordering and colors
CORRECTION_TYPES = [
    "lateral_propagate",
    "junction_floor",
    "bifurc_channel_no_lateral",
    "bifurc_share",
    "baseline_isotonic",
    "lateral_capped",
    "node_denoise",
]
TYPE_COLORS = {
    "lateral_propagate": "#f0c929",
    "junction_floor": "#4caf50",
    "bifurc_channel_no_lateral": "#2196f3",
    "bifurc_share": "#ff5722",
    "baseline_isotonic": "#9c27b0",
    "lateral_capped": "#e91e63",
    "node_denoise": "#607d8b",
}
TYPE_LABELS = {
    "lateral_propagate": "Lateral propagate",
    "junction_floor": "Junction floor",
    "bifurc_channel_no_lateral": "Bifurc channel (no lateral)",
    "bifurc_share": "Bifurc share",
    "baseline_isotonic": "Baseline isotonic",
    "lateral_capped": "Lateral capped",
    "node_denoise": "Node denoise",
}


def load_summaries(input_dir: Path) -> dict[str, dict]:
    """Load all region summary JSONs."""
    summaries = {}
    for region in REGIONS:
        path = input_dir / f"facc_denoise_v3_summary_{region}.json"
        with open(path) as f:
            summaries[region] = json.load(f)
    return summaries


def load_csvs(input_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all region correction CSVs."""
    csvs = {}
    for region in REGIONS:
        path = input_dir / f"facc_denoise_v3_{region}.csv"
        csvs[region] = pd.read_csv(path)
    return csvs


# ---------------------------------------------------------------------------
# Fig 1: Before/after junction conservation + bifurcation ratios
# ---------------------------------------------------------------------------


def fig1_before_after(
    db_path: str, v17b_path: str, csvs: dict[str, pd.DataFrame], out: Path
) -> None:
    """Junction conservation and bifurcation ratio histograms."""
    # Build topology for all regions, compute junction ratios and bifurc ratios
    junction_ratios_before = []
    junction_ratios_after = []
    bifurc_ratios_before = []
    bifurc_ratios_after = []

    conn_v17c = duckdb.connect(db_path, read_only=True)
    conn_v17b = duckdb.connect(v17b_path, read_only=True)

    for region in REGIONS:
        # Load topology
        topo = conn_v17c.execute(
            "SELECT reach_id, direction, neighbor_reach_id "
            "FROM reach_topology WHERE region = ?",
            [region],
        ).fetchdf()

        # Load v17b facc
        v17b_df = conn_v17b.execute(
            "SELECT reach_id, facc FROM reaches WHERE region = ?",
            [region],
        ).fetchdf()
        v17b_facc = dict(
            zip(v17b_df["reach_id"].astype(int), v17b_df["facc"].astype(float))
        )

        # Build corrected facc lookup from CSV
        csv_df = csvs[region]
        corrected_facc = dict(v17b_facc)  # start from v17b
        for _, row in csv_df.iterrows():
            corrected_facc[int(row["reach_id"])] = float(row["corrected_facc"])

        # Build upstream neighbor map
        up_topo = topo[topo["direction"] == "up"]
        upstream_map: dict[int, list[int]] = {}
        for _, row in up_topo.iterrows():
            rid = int(row["reach_id"])
            nid = int(row["neighbor_reach_id"])
            upstream_map.setdefault(rid, []).append(nid)

        down_topo = topo[topo["direction"] == "down"]
        downstream_map: dict[int, list[int]] = {}
        for _, row in down_topo.iterrows():
            rid = int(row["reach_id"])
            nid = int(row["neighbor_reach_id"])
            downstream_map.setdefault(rid, []).append(nid)

        # Junction conservation ratios (reaches with 2+ upstream)
        for rid, parents in upstream_map.items():
            if len(parents) < 2:
                continue
            sum_up_before = sum(v17b_facc.get(p, 0) for p in parents)
            sum_up_after = sum(corrected_facc.get(p, 0) for p in parents)
            val_before = v17b_facc.get(rid, 0)
            val_after = corrected_facc.get(rid, 0)
            if sum_up_before > 0:
                junction_ratios_before.append(val_before / sum_up_before)
            if sum_up_after > 0:
                junction_ratios_after.append(val_after / sum_up_after)

        # Bifurcation child/parent ratios (reaches with 2+ downstream)
        for rid, children in downstream_map.items():
            if len(children) < 2:
                continue
            parent_before = v17b_facc.get(rid, 0)
            parent_after = corrected_facc.get(rid, 0)
            for child in children:
                if parent_before > 0:
                    bifurc_ratios_before.append(v17b_facc.get(child, 0) / parent_before)
                if parent_after > 0:
                    bifurc_ratios_after.append(
                        corrected_facc.get(child, 0) / parent_after
                    )

    conn_v17c.close()
    conn_v17b.close()

    # Plot
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    # Top row: Junction conservation
    bins_junc = np.linspace(0, 3, 80)
    axes[0, 0].hist(
        junction_ratios_before,
        bins=bins_junc,
        color="#e57373",
        alpha=0.8,
        edgecolor="none",
    )
    axes[0, 0].axvline(1.0, color="k", ls="--", lw=1)
    axes[0, 0].set_title("v17b: Junction facc / sum(upstream)")
    axes[0, 0].set_xlabel("Ratio")
    axes[0, 0].set_ylabel("Count")
    n_below = sum(1 for r in junction_ratios_before if r < 0.999)
    axes[0, 0].text(
        0.95,
        0.95,
        f"{n_below}/{len(junction_ratios_before)} below 1.0\n({100 * n_below / len(junction_ratios_before):.0f}%)",
        transform=axes[0, 0].transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=dict(boxstyle="round", fc="white", alpha=0.8),
    )

    axes[0, 1].hist(
        junction_ratios_after,
        bins=bins_junc,
        color="#81c784",
        alpha=0.8,
        edgecolor="none",
    )
    axes[0, 1].axvline(1.0, color="k", ls="--", lw=1)
    axes[0, 1].set_title("v17c: Junction facc / sum(upstream)")
    axes[0, 1].set_xlabel("Ratio")
    axes[0, 1].set_ylabel("Count")
    n_below_after = sum(1 for r in junction_ratios_after if r < 0.999)
    axes[0, 1].text(
        0.95,
        0.95,
        f"{n_below_after}/{len(junction_ratios_after)} below 1.0\n({100 * n_below_after / max(len(junction_ratios_after), 1):.0f}%)",
        transform=axes[0, 1].transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=dict(boxstyle="round", fc="white", alpha=0.8),
    )

    # Bottom row: Bifurcation child/parent ratio
    bins_bif = np.linspace(0, 2, 60)
    axes[1, 0].hist(
        bifurc_ratios_before,
        bins=bins_bif,
        color="#e57373",
        alpha=0.8,
        edgecolor="none",
    )
    axes[1, 0].axvline(1.0, color="k", ls="--", lw=1)
    axes[1, 0].set_title("v17b: Bifurcation child / parent facc")
    axes[1, 0].set_xlabel("Ratio")
    axes[1, 0].set_ylabel("Count")

    axes[1, 1].hist(
        bifurc_ratios_after, bins=bins_bif, color="#81c784", alpha=0.8, edgecolor="none"
    )
    axes[1, 1].axvline(1.0, color="k", ls="--", lw=1)
    axes[1, 1].set_title("v17c: Bifurcation child / parent facc")
    axes[1, 1].set_xlabel("Ratio")
    axes[1, 1].set_ylabel("Count")
    median_after = np.median(bifurc_ratios_after) if bifurc_ratios_after else 0
    axes[1, 1].text(
        0.95,
        0.95,
        f"Median: {median_after:.2f}",
        transform=axes[1, 1].transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox=dict(boxstyle="round", fc="white", alpha=0.8),
    )

    fig.suptitle(
        "Before/After: Junction Conservation and Bifurcation Ratios", fontsize=13
    )
    fig.tight_layout()
    fig.savefig(out / "report_fig1.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Fig 1 saved: {out / 'report_fig1.png'}")


# ---------------------------------------------------------------------------
# Fig 2: Correction type breakdown by region (stacked bar)
# ---------------------------------------------------------------------------


def fig2_correction_breakdown(summaries: dict[str, dict], out: Path) -> None:
    """Stacked bar chart of correction types per region."""
    fig, ax = plt.subplots(figsize=(10, 6))

    x = np.arange(len(REGIONS))
    width = 0.65
    bottoms = np.zeros(len(REGIONS))

    for ctype in CORRECTION_TYPES:
        counts = []
        for region in REGIONS:
            by_type = summaries[region].get("by_type", {})
            counts.append(by_type.get(ctype, {}).get("count", 0))
        counts_arr = np.array(counts, dtype=float)
        ax.bar(
            x,
            counts_arr,
            width,
            bottom=bottoms,
            label=TYPE_LABELS.get(ctype, ctype),
            color=TYPE_COLORS.get(ctype, "#999999"),
        )
        bottoms += counts_arr

    ax.set_xticks(x)
    ax.set_xticklabels(REGIONS)
    ax.set_ylabel("Corrections")
    ax.set_title("Correction Type Breakdown by Region")
    ax.legend(loc="upper left", fontsize=8, ncol=2)

    # Annotate totals
    for i, region in enumerate(REGIONS):
        total = summaries[region]["corrections"]
        ax.text(i, bottoms[i] + 200, f"{total:,}", ha="center", va="bottom", fontsize=8)

    fig.tight_layout()
    fig.savefig(out / "report_fig2.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Fig 2 saved: {out / 'report_fig2.png'}")


# ---------------------------------------------------------------------------
# Fig 3: Scalability comparison + per-region bar chart
# ---------------------------------------------------------------------------


def fig3_scalability(summaries: dict[str, dict], out: Path) -> None:
    """Complexity comparison and per-region reach/correction counts."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Left: Complexity curves
    n_vals = np.logspace(1, 5, 100)
    m_basin = 50  # avg basin size for integrator
    integrator_ops = n_vals * m_basin**2  # O(N * m^2) total
    pipeline_ops = n_vals  # O(N)

    ax1.loglog(n_vals, integrator_ops, "r-", lw=2, label="Integrator: O(N * m²)")
    ax1.loglog(n_vals, pipeline_ops, "b-", lw=2, label="Biphase: O(N)")
    ax1.axvline(248674, color="gray", ls=":", alpha=0.7)
    ax1.text(248674, 1e3, " 248K\n reaches", fontsize=8, va="bottom")
    ax1.set_xlabel("Total reaches (N)")
    ax1.set_ylabel("Operations")
    ax1.set_title("Computational Complexity")
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)

    # Right: Per-region bars
    reaches = [summaries[r]["total_reaches"] for r in REGIONS]
    corrections = [summaries[r]["corrections"] for r in REGIONS]
    x = np.arange(len(REGIONS))
    w = 0.35
    ax2.bar(x - w / 2, reaches, w, label="Total reaches", color="#90caf9")
    ax2.bar(x + w / 2, corrections, w, label="Corrections", color="#ffab91")
    ax2.set_xticks(x)
    ax2.set_xticklabels(REGIONS)
    ax2.set_ylabel("Count")
    ax2.set_title("Reaches and Corrections by Region")
    ax2.legend(fontsize=9)

    # Annotate correction %
    for i in range(len(REGIONS)):
        pct = 100 * corrections[i] / reaches[i]
        ax2.text(
            x[i] + w / 2,
            corrections[i] + 500,
            f"{pct:.0f}%",
            ha="center",
            va="bottom",
            fontsize=7,
        )

    fig.tight_layout()
    fig.savefig(out / "report_fig3.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Fig 3 saved: {out / 'report_fig3.png'}")


# ---------------------------------------------------------------------------
# Fig 4: PAVA example on synthetic chain
# ---------------------------------------------------------------------------


def fig4_pava_example(out: Path) -> None:
    """Isotonic regression (PAVA) demo on a synthetic 1:1 chain."""
    # Synthetic chain with a violation zone (facc dips mid-chain)
    np.random.seed(42)
    n = 15
    # Increasing baseline with a violation dip from R8-R14
    base = np.array(
        [
            5000,
            5200,
            5500,
            5800,
            6100,
            6500,
            7000,
            7800,
            7200,
            6800,
            6500,
            6200,
            5900,
            5600,
            8200,
        ],
        dtype=float,
    )

    # PAVA: pool adjacent violators (non-decreasing) — stack-based with
    # backward merging, matching the production implementation in
    # src/sword_duckdb/facc_detection/correct_facc_denoise.py
    blocks: list[list] = []  # [sum_val, count, start, end]
    for i in range(n):
        blocks.append([base[i], 1, i, i])
        while len(blocks) > 1:
            curr = blocks[-1]
            prev = blocks[-2]
            if prev[0] / prev[1] <= curr[0] / curr[1]:
                break
            prev[0] += curr[0]
            prev[1] += curr[1]
            prev[3] = curr[3]
            blocks.pop()
    pava = np.empty(n)
    for s, cnt, start, end in blocks:
        pava[start : end + 1] = s / cnt

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(1, n + 1)

    # Shade violation zones
    for i in range(n - 1):
        if base[i + 1] < base[i]:
            ax.axvspan(x[i] - 0.5, x[i + 1] + 0.5, color="#ffcdd2", alpha=0.4)

    ax.plot(
        x,
        base,
        "r-o",
        lw=2,
        markersize=6,
        label="Stage A baseline (with violations)",
        zorder=3,
    )
    ax.plot(
        x, pava, "b-s", lw=2, markersize=6, label="After PAVA (monotonic)", zorder=4
    )

    ax.set_xlabel("Reach position (upstream → downstream)")
    ax.set_ylabel("facc (km²)")
    ax.set_title("Isotonic Regression (PAVA) on a 1:1 Chain")
    ax.legend(fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"R{i}" for i in x])
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out / "report_fig4.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Fig 4 saved: {out / 'report_fig4.png'}")


# ---------------------------------------------------------------------------
# Fig 5: Willamette basin — biphase pipeline vs integrator comparison
# ---------------------------------------------------------------------------


def fig5_willamette(
    db_path: str,
    v17b_path: str,
    csvs: dict[str, pd.DataFrame],
    input_dir: Path,
    out: Path,
) -> None:
    """Compare biphase pipeline and CVXPY integrator on Willamette basin."""
    # --- Load v17b original facc ---
    conn = duckdb.connect(v17b_path, read_only=True)
    wil_df = conn.execute(
        "SELECT reach_id, facc FROM reaches "
        "WHERE region = 'NA' AND CAST(reach_id AS VARCHAR) LIKE '7822%'"
    ).fetchdf()
    conn.close()
    v17b_facc = dict(zip(wil_df["reach_id"].astype(int), wil_df["facc"].astype(float)))

    # --- Load biphase pipeline corrections ---
    biphase = dict(v17b_facc)
    csv_na = csvs["NA"]
    wil_csv = csv_na[csv_na["reach_id"].astype(str).str.startswith("7822")]
    for _, row in wil_csv.iterrows():
        biphase[int(row["reach_id"])] = float(row["corrected_facc"])

    # --- Load integrator (CVXPY) reference ---
    ref_path = input_dir / "willamette_integrator_reference.json"
    with open(ref_path) as f:
        integrator_raw = json.load(f)
    integrator = {int(k): v["integrator"] for k, v in integrator_raw.items()}

    # --- Build aligned arrays sorted by v17b facc ---
    rids = sorted(v17b_facc.keys())
    orig = np.array([v17b_facc[r] for r in rids])
    bi = np.array([biphase[r] for r in rids])
    integ = np.array([integrator[r] for r in rids])

    bi_pct = 100 * (bi - orig) / orig
    integ_pct = 100 * (integ - orig) / orig

    # Sort by original facc (ascending) for the bar chart
    sort_idx = np.argsort(orig)
    rids_sorted = [rids[i] for i in sort_idx]
    orig_sorted = orig[sort_idx]
    bi_pct_sorted = bi_pct[sort_idx]
    integ_pct_sorted = integ_pct[sort_idx]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

    # --- Left: log-log scatter, both methods vs v17b ---
    ax1.scatter(
        orig,
        integ,
        c="#ff9800",
        s=45,
        zorder=3,
        label="Integrator (CVXPY)",
        edgecolors="k",
        linewidths=0.3,
        marker="D",
    )
    ax1.scatter(
        orig,
        bi,
        c="#2196f3",
        s=45,
        zorder=4,
        label="Biphase pipeline",
        edgecolors="k",
        linewidths=0.3,
        marker="o",
    )
    all_vals = np.concatenate([orig, bi, integ])
    lims = [all_vals.min() * 0.7, all_vals.max() * 1.3]
    ax1.plot(lims, lims, "k--", lw=1, alpha=0.4, label="1:1 (no change)")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlim(lims)
    ax1.set_ylim(lims)
    ax1.set_xlabel("v17b facc (km²)")
    ax1.set_ylabel("Corrected facc (km²)")
    ax1.set_title("Original vs Corrected (both methods)")
    ax1.legend(fontsize=8, loc="upper left")
    ax1.grid(True, alpha=0.3)

    # --- Right: paired % change bars, sorted by original facc ---
    y = np.arange(len(rids_sorted))
    bar_h = 0.35
    ax2.barh(
        y + bar_h / 2,
        integ_pct_sorted,
        bar_h,
        color="#ff9800",
        edgecolor="k",
        linewidth=0.3,
        label="Integrator",
    )
    ax2.barh(
        y - bar_h / 2,
        bi_pct_sorted,
        bar_h,
        color="#2196f3",
        edgecolor="k",
        linewidth=0.3,
        label="Biphase",
    )
    ax2.axvline(0, color="k", lw=0.8, alpha=0.5)
    ax2.set_xlabel("% Change from v17b")
    ax2.set_ylabel("Reaches (sorted by original facc →)")
    ax2.set_title("Per-Reach Corrections: Both Methods")
    ax2.set_yticks([])
    ax2.legend(fontsize=8, loc="lower right")
    ax2.grid(True, alpha=0.3, axis="x")

    fig.suptitle("Willamette River Basin (7822*): 55 Reaches", fontsize=13)
    fig.tight_layout()
    fig.savefig(out / "report_fig5.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Fig 5 saved: {out / 'report_fig5.png'}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate facc report figures")
    parser.add_argument(
        "--input-dir", default="output/facc_detection", help="Dir with CSVs and JSONs"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="v17c DuckDB path",
    )
    parser.add_argument(
        "--v17b",
        default="data/duckdb/sword_v17b.duckdb",
        help="v17b DuckDB path",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    out = input_dir / "figures"
    out.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    summaries = load_summaries(input_dir)
    csvs = load_csvs(input_dir)

    print("Generating figures...")
    fig1_before_after(args.db, args.v17b, csvs, out)
    fig2_correction_breakdown(summaries, out)
    fig3_scalability(summaries, out)
    fig4_pava_example(out)
    fig5_willamette(args.db, args.v17b, csvs, input_dir, out)

    print(f"\nAll figures saved to {out}/")


if __name__ == "__main__":
    main()
