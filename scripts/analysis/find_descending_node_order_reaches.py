"""
Identify v17b reaches whose nodes are stored in DESCENDING dist_out order
inside the official v17b NetCDF files.

These are the reaches affected by the "node-array direction" bug fixed in
v17c export commit 4d3aeee.  Any downstream consumer that assumes
"first node in the array == downstream end" will produce SIGN-FLIPPED
slopes for these reaches.

Output:
    outputs/jw_slope_audit/v17b_descending_node_reaches.parquet
    outputs/jw_slope_audit/v17b_descending_node_reaches.csv
    outputs/jw_slope_audit/v17b_descending_node_reaches_summary.txt

Expected total: ~18,552 reaches (~8% globally).
"""

from __future__ import annotations

import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd

NETCDF_DIR = Path("/Users/jakegearon/projects/SWORD/data/netcdf")
OUT_DIR = Path("/Users/jakegearon/projects/SWORD/outputs/jw_slope_audit")
REGIONS = ["af", "as", "eu", "na", "oc", "sa"]


def classify_reach_node_order(nc_path: Path, region: str) -> pd.DataFrame:
    """Return per-reach: order classification + diagnostics."""
    print(f"[{region.upper()}] reading {nc_path.name}...", flush=True)
    ds = nc.Dataset(nc_path)

    node_reach_id = np.asarray(ds.groups["nodes"].variables["reach_id"][:]).astype(
        np.int64
    )
    node_dist_out = np.asarray(ds.groups["nodes"].variables["dist_out"][:]).astype(
        np.float64
    )
    node_id = np.asarray(ds.groups["nodes"].variables["node_id"][:]).astype(np.int64)
    reach_ids = np.asarray(ds.groups["reaches"].variables["reach_id"][:]).astype(
        np.int64
    )
    ds.close()

    # Group node array indices by reach_id, preserving array storage order
    # (this is the order JPL processors will see).
    order_per_reach: dict[int, list[int]] = {}
    for arr_idx, rid in enumerate(node_reach_id):
        order_per_reach.setdefault(int(rid), []).append(arr_idx)

    rows = []
    for rid in reach_ids:
        rid = int(rid)
        idxs = order_per_reach.get(rid)
        if not idxs or len(idxs) < 2:
            # Single-node or missing reach: cannot have a slope sign issue.
            rows.append(
                {
                    "reach_id": rid,
                    "region": region.upper(),
                    "n_nodes": 0 if not idxs else 1,
                    "node_order_class": "single_or_missing",
                    "first_node_id": idxs[0] if idxs else None,
                    "last_node_id": idxs[-1] if idxs else None,
                    "first_dist_out": float(node_dist_out[idxs[0]]) if idxs else np.nan,
                    "last_dist_out": float(node_dist_out[idxs[-1]]) if idxs else np.nan,
                }
            )
            continue

        d = node_dist_out[idxs]
        # Drop fill-valued nodes (rare); only judge from finite values.
        finite = np.isfinite(d) & (d > -1e10)
        if finite.sum() < 2:
            cls = "indeterminate"
        else:
            df = d[finite]
            # Strict monotonic ascending = downstream-first (correct).
            # Strict monotonic descending = upstream-first (sign-flip risk).
            # Mixed = anomalous (extremely rare; flag separately).
            ascending = np.all(np.diff(df) >= 0)
            descending = np.all(np.diff(df) <= 0)
            if descending and not ascending:
                cls = "descending"  # <-- the affected reaches
            elif ascending and not descending:
                cls = "ascending"
            elif ascending and descending:
                cls = "constant"  # all equal — degenerate
            else:
                cls = "mixed"

        rows.append(
            {
                "reach_id": rid,
                "region": region.upper(),
                "n_nodes": len(idxs),
                "node_order_class": cls,
                "first_node_id": int(node_id[idxs[0]]),
                "last_node_id": int(node_id[idxs[-1]]),
                "first_dist_out": float(node_dist_out[idxs[0]]),
                "last_dist_out": float(node_dist_out[idxs[-1]]),
            }
        )

    return pd.DataFrame(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    frames = []
    for region in REGIONS:
        nc_path = NETCDF_DIR / f"{region}_sword_v17b.nc"
        if not nc_path.exists():
            print(f"[{region.upper()}] MISSING: {nc_path}", file=sys.stderr)
            continue
        frames.append(classify_reach_node_order(nc_path, region))

    df = pd.concat(frames, ignore_index=True)

    descending = df[df["node_order_class"] == "descending"].copy()
    ascending = df[df["node_order_class"] == "ascending"].copy()
    mixed = df[df["node_order_class"] == "mixed"].copy()

    # Persist artifacts
    descending_path_pq = OUT_DIR / "v17b_descending_node_reaches.parquet"
    descending_path_csv = OUT_DIR / "v17b_descending_node_reaches.csv"
    full_path = OUT_DIR / "v17b_all_reaches_node_order_class.parquet"
    descending.to_parquet(descending_path_pq, index=False)
    descending[["reach_id", "region"]].to_csv(descending_path_csv, index=False)
    df.to_parquet(full_path, index=False)

    # Per-region summary
    summary_lines = []
    summary_lines.append("v17b reach node-array ordering audit")
    summary_lines.append("=" * 60)
    summary_lines.append(f"Total reaches considered: {len(df):,}")
    summary_lines.append(
        f"  ascending  (downstream-first, correct): {len(ascending):,}"
    )
    summary_lines.append(
        f"  descending (upstream-first,   AFFECTED): {len(descending):,}"
    )
    summary_lines.append(f"  mixed      (non-monotonic):              {len(mixed):,}")
    other = len(df) - len(ascending) - len(descending) - len(mixed)
    summary_lines.append(f"  other (single-node / indeterminate / constant): {other:,}")
    summary_lines.append("")
    summary_lines.append("Affected (descending) per region:")
    for region in REGIONS:
        n_desc = int((descending["region"] == region.upper()).sum())
        n_total = int((df["region"] == region.upper()).sum())
        pct = 100 * n_desc / n_total if n_total else 0.0
        summary_lines.append(
            f"  {region.upper():<3}  {n_desc:>7,} / {n_total:>7,}  ({pct:5.2f}%)"
        )
    summary_lines.append("")
    summary_lines.append("Outputs:")
    summary_lines.append(f"  {descending_path_pq}")
    summary_lines.append(f"  {descending_path_csv}")
    summary_lines.append(f"  {full_path}")

    summary = "\n".join(summary_lines)
    print()
    print(summary)
    (OUT_DIR / "v17b_descending_node_reaches_summary.txt").write_text(summary + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
