"""
Flow direction correction for v17c pipeline.

Detects sections with wrong flow direction (via SWOT WSE slope validation)
and corrects high-confidence cases by flipping topology direction.

Confidence tiers use existing slope quality metrics:
- slope_obs_q bit flags from reach_swot_obs.py
- slope_obs_n_passes, n_obs, wse_obs_mean
"""

import json
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import duckdb
import networkx as nx
import numpy as np
import pandas as pd

from .stages._logging import log


def create_flow_corrections_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create v17c_flow_corrections provenance table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v17c_flow_corrections (
            run_id VARCHAR, region VARCHAR(2), section_id INTEGER,
            iteration INTEGER, tier VARCHAR(6), action VARCHAR(16),
            slope_from_upstream DOUBLE, slope_from_downstream DOUBLE,
            n_reaches_flipped INTEGER, reach_ids_flipped VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def snapshot_topology(conn: duckdb.DuckDBPyConnection, region: str, run_id: str) -> str:
    """Backup reach_topology for a region. Returns backup table name."""
    table_name = f"reach_topology_backup_{region}_{run_id.replace('-', '_')}"
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{table_name}" AS '
        "SELECT * FROM reach_topology WHERE region = ?",
        [region.upper()],
    )
    n = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    log(f"Topology snapshot: {n:,} rows -> {table_name}")
    return table_name


def rollback_flow_corrections(
    conn: duckdb.DuckDBPyConnection, region: str, run_id: str
) -> int:
    """Restore reach_topology from backup. Returns rows restored."""
    table_name = f"reach_topology_backup_{region}_{run_id.replace('-', '_')}"
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    ]
    if table_name not in tables:
        backups = [t for t in tables if t.startswith("reach_topology_backup_")]
        raise ValueError(f"Backup '{table_name}' not found. Available: {backups}")
    conn.execute("DELETE FROM reach_topology WHERE region = ?", [region.upper()])
    conn.execute(f'INSERT INTO reach_topology SELECT * FROM "{table_name}"')
    n = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    log(f"Rollback: restored {n:,} rows from {table_name}")
    return n


def _get_reach_quality(reach_ids: List[int], reaches_df: pd.DataFrame) -> Dict:
    """Gather quality metrics for reaches in a section."""
    cols = ["slope_obs_q", "slope_obs_n_passes", "n_obs", "wse_obs_mean", "lakeflag"]
    avail = [c for c in cols if c in reaches_df.columns]
    subset = reaches_df[reaches_df["reach_id"].isin(reach_ids)]

    n_wse = 0
    slope_q_vals = []
    n_passes_vals = []

    for _, row in subset.iterrows():
        if "wse_obs_mean" in avail and pd.notna(row.get("wse_obs_mean")):
            n_wse += 1
        sq = row.get("slope_obs_q")
        if sq is not None and pd.notna(sq):
            slope_q_vals.append(int(sq))
        np_val = row.get("slope_obs_n_passes")
        if np_val is not None and pd.notna(np_val):
            n_passes_vals.append(int(np_val))

    n_good = sum(1 for q in slope_q_vals if q == 0)
    frac_good = n_good / len(slope_q_vals) if slope_q_vals else 0
    med_passes = float(np.median(n_passes_vals)) if n_passes_vals else 0
    has_extreme = any(q & (4 | 8) for q in slope_q_vals)
    has_lake = False
    if "lakeflag" in avail:
        lf_vals = subset["lakeflag"].dropna()
        has_lake = (lf_vals > 0).any()

    return {
        "n_with_wse": n_wse,
        "frac_good_q": frac_good,
        "median_n_passes": med_passes,
        "has_extreme_flags": has_extreme,
        "has_lake": has_lake,
    }


def _get_facc_confidence(
    G: nx.DiGraph,
    reach_ids: List[int],
    reaches_df: pd.DataFrame,
    upstream_junction: int,
    downstream_junction: int,
) -> Tuple[float, float]:
    """
    Calculate confidence based on iterative FACC re-accumulation.
    Returns (facc_delta_score, facc_snap_error).
    """
    # 1. Map reach data
    r_map = reaches_df.set_index("reach_id").to_dict("index")

    # 2. Estimate local area (facc - sum(upstream_facc))
    local_areas = {}
    for rid in reach_ids + [upstream_junction, downstream_junction]:
        if rid not in r_map:
            continue
        preds = list(G.predecessors(rid))
        up_facc = sum(r_map[p]["facc"] for p in preds if p in r_map)
        local_areas[rid] = max(0, r_map[rid]["facc"] - up_facc)

    # 3. Virtual Flip
    G_test = G.copy()
    edges_to_flip = []
    ids_set = set(reach_ids) | {upstream_junction, downstream_junction}
    for u, v in G.edges():
        if u in ids_set and v in ids_set:
            edges_to_flip.append((u, v))

    for u, v in edges_to_flip:
        G_test.remove_edge(u, v)
        G_test.add_edge(v, u)

    # 4. Re-accumulate
    new_facc = {rid: local_areas.get(rid, 0) for rid in G_test.nodes()}
    try:
        for node in nx.topological_sort(G_test):
            for succ in G_test.successors(node):
                if succ in new_facc:
                    new_facc[succ] += new_facc[node]
    except nx.NetworkXUnfeasible:
        return 0.0, 1.0  # Cycle created

    # 5. Measure "Snap" at new downstream boundary
    # In the flipped graph, the original upstream headwater becomes the new outlet
    new_outlet = reach_ids[0] if reach_ids else None
    if not new_outlet or new_outlet not in G_test:
        return 0.0, 1.0

    dn_neighbors = list(G_test.successors(new_outlet))
    if not dn_neighbors:
        return 0.0, 1.0

    neighbor = dn_neighbors[0]
    if neighbor not in r_map:
        return 0.5, 0.5  # Neutral

    calc_val = new_facc[new_outlet]
    expected_val = r_map[neighbor]["facc"]

    snap_error = abs(calc_val - expected_val) / (expected_val + 1)

    # Confidence: 1.0 if perfect snap, 0.0 if huge mismatch
    facc_conf = max(0, 1.0 - (snap_error / 0.5))

    return facc_conf, snap_error


def score_section_confidence(
    validation_row: Dict,
    G: nx.DiGraph,
    reaches_df: pd.DataFrame,
    reach_ids: List[int],
    min_wse_reaches: int = 2,
) -> Tuple[str, Dict]:
    """
    Score a section into HIGH / MEDIUM / LOW / SKIP confidence tier.

    Uses two independent signals:
    - Slope: SWOT-observed WSE slope (requires actual SWOT observations)
    - FACC snap: whether re-accumulated FACC matches boundary after virtual flip

    NOTE: slope_from_upstream = -slope_from_downstream (same measurement,
    flipped sign). They are NOT independent. We use slope_from_upstream
    as the single slope signal and require SWOT backing via n_obs.
    """
    likely_cause = validation_row.get("likely_cause")
    direction_valid = validation_row.get("direction_valid")
    slope_up = validation_row.get("slope_from_upstream")

    if direction_valid is True or direction_valid is None:
        return "SKIP", {"reason": "valid_or_undetermined"}

    if likely_cause in ("lake_section", "extreme_slope_data_error"):
        return "SKIP", {"reason": f"likely_cause={likely_cause}"}

    # 1. Slope signal — single value, gated by SWOT quality
    metrics = _get_reach_quality(reach_ids, reaches_df)
    slope_wrong = pd.notna(slope_up) and slope_up > 0 and abs(slope_up) > 1e-10

    # SWOT quality gate: need actual observations, not just DEM-derived slopes
    subset = reaches_df[reaches_df["reach_id"].isin(reach_ids)]
    n_obs_col = "n_obs" if "n_obs" in subset.columns else None
    swot_reaches = 0
    if n_obs_col:
        swot_reaches = int((subset[n_obs_col].fillna(0) >= 5).sum())
    has_swot = swot_reaches >= min_wse_reaches
    slope_credible = slope_wrong and has_swot

    # 2. FACC snap signal
    uj = validation_row.get("upstream_junction")
    dj = validation_row.get("downstream_junction")
    facc_conf, snap_error = _get_facc_confidence(G, reach_ids, reaches_df, uj, dj)

    meta = {
        "slope_wrong": slope_wrong,
        "slope_credible": slope_credible,
        "swot_reaches": swot_reaches,
        "has_swot": has_swot,
        "facc_confidence": facc_conf,
        "snap_error": snap_error,
        **metrics,
    }

    # HIGH: SWOT-backed slope evidence AND FACC snap agreement
    if slope_credible and snap_error < 0.2:
        return "HIGH", {**meta, "reason": "swot_slope_and_facc_agreement"}

    # MEDIUM: SWOT-backed slope evidence, weaker FACC snap
    if slope_credible and snap_error < 0.4:
        return "MEDIUM", {**meta, "reason": "swot_slope_moderate_facc"}

    # MEDIUM: strong FACC snap + slope direction is wrong (even without SWOT)
    if slope_wrong and snap_error < 0.1:
        return "MEDIUM", {**meta, "reason": "strong_facc_with_slope_direction"}

    # Everything else: LOW — FACC snap alone is not enough
    return "LOW", {**meta, "reason": "insufficient_independent_evidence"}


def flip_section_topology(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    reach_ids: List[int],
    upstream_junction: int,
    downstream_junction: int,
) -> int:
    """
    Flip direction='up'<->'down' for edges within a section.

    Safely handles neighbor_rank collisions by re-ranking all neighbors
    for affected reaches after the flip.
    """
    section_set = list(set(reach_ids) | {upstream_junction, downstream_junction})
    section_df = pd.DataFrame({"rid": section_set})
    conn.register("_flip_ids", section_df)

    # 1. Extract ALL topology for these reaches
    conn.execute(
        """
        CREATE TEMP TABLE _topo_to_process AS
        SELECT * FROM reach_topology
        WHERE region = ?
          AND reach_id IN (SELECT rid FROM _flip_ids)
    """,
        [region.upper()],
    )

    # 2. Delete the rows we're about to replace
    conn.execute(
        """
        DELETE FROM reach_topology
        WHERE region = ?
          AND reach_id IN (SELECT rid FROM _flip_ids)
    """,
        [region.upper()],
    )

    # 3. Flip direction for internal edges and re-rank everything
    # We use ROW_NUMBER() to ensure contiguous 0-based ranks for each direction
    conn.execute(
        """
        INSERT INTO reach_topology
        SELECT
            reach_id,
            region,
            CASE
                WHEN neighbor_reach_id IN (SELECT rid FROM _flip_ids)
                THEN (CASE WHEN direction = 'up' THEN 'down' ELSE 'up' END)
                ELSE direction
            END as direction,
            CAST(ROW_NUMBER() OVER (
                PARTITION BY reach_id, 
                (CASE 
                    WHEN neighbor_reach_id IN (SELECT rid FROM _flip_ids) 
                    THEN (CASE WHEN direction = 'up' THEN 'down' ELSE 'up' END) 
                    ELSE direction 
                END)
                ORDER BY neighbor_rank
            ) - 1 AS TINYINT) as neighbor_rank,
            neighbor_reach_id,
            topology_suspect,
            topology_approved
        FROM _topo_to_process
    """
    )

    # 4. Count how many internal edges were actually flipped
    result = conn.execute(
        """
        SELECT COUNT(*) FROM _topo_to_process
        WHERE neighbor_reach_id IN (SELECT rid FROM _flip_ids)
    """
    )
    n = result.fetchone()[0]

    conn.execute("DROP TABLE _topo_to_process")
    conn.unregister("_flip_ids")
    return n


def correct_flow_directions(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    G: nx.DiGraph,
    sections_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    reaches_df: pd.DataFrame,
    max_iterations: int = 5,
    run_id: Optional[str] = None,
    rebuild_fn=None,
) -> Dict:
    """
    Iterative flow direction correction loop.

    Scores invalid sections, flips HIGH+MEDIUM, rebuilds graph, re-validates.
    Oscillation guard: sections flipped >=2 times are demoted to LOW.

    rebuild_fn(conn, region) -> (G, sections_df, validation_df) rebuilds
    after topology changes. If None, single-pass mode (no re-validation).
    """
    if run_id is None:
        run_id = uuid4().hex[:12]
    log(f"Flow direction correction: region={region}, run_id={run_id}")

    create_flow_corrections_table(conn)
    snapshot_topology(conn, region, run_id)

    # Cross-run oscillation guard: load flip counts from ALL previous runs
    flip_history: Dict[int, int] = {}
    prev = conn.execute(
        """
        SELECT section_id, COUNT(*) AS n
        FROM v17c_flow_corrections
        WHERE region = ? AND action = 'flip' AND n_reaches_flipped > 0
        GROUP BY section_id
    """,
        [region.upper()],
    ).fetchall()
    for sid, n in prev:
        flip_history[sid] = n
    if flip_history:
        log(f"  Cross-run history: {len(flip_history)} sections with prior flips")

    total_flipped = 0
    manual_review = []
    cur_G, cur_sdf, cur_vdf = G, sections_df, validation_df
    iteration = 0

    for iteration in range(1, max_iterations + 1):
        log(f"  Iteration {iteration}/{max_iterations}")
        if cur_vdf.empty:
            break

        invalid = cur_vdf[cur_vdf["direction_valid"] == False]  # noqa: E712
        if invalid.empty:
            log("  All sections valid, converged!")
            break

        sec_map = {}
        for _, r in cur_sdf.iterrows():
            sec_map[int(r["section_id"])] = {
                "reach_ids": r["reach_ids"],
                "uj": int(r["upstream_junction"]),
                "dj": int(r["downstream_junction"]),
            }

        to_flip = []
        log_rows = []
        for _, vrow in invalid.iterrows():
            sid = int(vrow["section_id"])
            si = sec_map.get(sid)
            if si is None:
                continue

            tier, meta = score_section_confidence(
                vrow.to_dict(), cur_G, reaches_df, si["reach_ids"]
            )
            if flip_history.get(sid, 0) >= 2:
                tier, meta["reason"] = "LOW", "oscillation_guard"

            if tier in ("HIGH", "MEDIUM"):
                to_flip.append((sid, tier, si))
            elif tier == "LOW":
                manual_review.append(
                    {
                        "section_id": sid,
                        "reason": meta.get("reason", ""),
                        "slope_up": vrow.get("slope_from_upstream"),
                        "slope_dn": vrow.get("slope_from_downstream"),
                    }
                )

            action = "flip" if tier in ("HIGH", "MEDIUM") else tier.lower()
            log_rows.append(
                {
                    "run_id": run_id,
                    "region": region.upper(),
                    "section_id": sid,
                    "iteration": iteration,
                    "tier": tier,
                    "action": action,
                    "slope_from_upstream": vrow.get("slope_from_upstream"),
                    "slope_from_downstream": vrow.get("slope_from_downstream"),
                    "n_reaches_flipped": len(si["reach_ids"])
                    if tier in ("HIGH", "MEDIUM")
                    else 0,
                    "reach_ids_flipped": json.dumps(si["reach_ids"])
                    if tier in ("HIGH", "MEDIUM")
                    else "[]",
                }
            )

        _write_log(conn, log_rows)

        if not to_flip:
            log("  No sections to flip, stopping")
            break

        log(f"  Flipping {len(to_flip)} sections")
        for sid, tier, si in to_flip:
            n = flip_section_topology(conn, region, si["reach_ids"], si["uj"], si["dj"])
            # False-outlet guard: check if boundary junctions lost all
            # downstream connections. If so, auto-revert this section.
            false_outlet = _check_false_outlets(
                conn, region, si["reach_ids"], si["uj"], si["dj"]
            )
            if false_outlet:
                log(f"    Section {sid}: false outlet at {false_outlet}, reverting")
                # Re-flip to restore original state
                flip_section_topology(conn, region, si["reach_ids"], si["uj"], si["dj"])
                flip_history[sid] = flip_history.get(sid, 0) + 2
                continue
            flip_history[sid] = flip_history.get(sid, 0) + 1
            total_flipped += 1
            log(f"    Section {sid} ({tier}): {n} rows flipped (#{flip_history[sid]})")

        if rebuild_fn is not None:
            cur_G, cur_sdf, cur_vdf = rebuild_fn(conn, region)
        else:
            break

    log(f"Done: {total_flipped} flipped, {len(manual_review)} manual review")
    return {
        "run_id": run_id,
        "region": region,
        "n_flipped": total_flipped,
        "n_manual_review": len(manual_review),
        "iterations": iteration,
        "manual_review": manual_review,
        "flip_history": flip_history,
    }


def _check_false_outlets(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    reach_ids: List[int],
    upstream_junction: int,
    downstream_junction: int,
) -> Optional[int]:
    """Check if flipping a section created a false outlet at a boundary junction.

    A false outlet is a junction that lost ALL downstream connections (n_rch_down
    went to 0) because its only downstream neighbor was inside the flipped section.

    Returns the reach_id of the false outlet, or None if no problem.
    """
    for jid in (upstream_junction, downstream_junction):
        n_dn = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND region = ? AND direction = 'down'
        """,
            [jid, region.upper()],
        ).fetchone()[0]
        if n_dn == 0:
            # Verify it had downstream neighbors before (not a natural headwater)
            n_up = conn.execute(
                """
                SELECT COUNT(*) FROM reach_topology
                WHERE reach_id = ? AND region = ? AND direction = 'up'
            """,
                [jid, region.upper()],
            ).fetchone()[0]
            if n_up > 0:
                return jid
    return None


def _write_log(conn: duckdb.DuckDBPyConnection, rows: List[Dict]) -> None:
    """Write correction log rows to v17c_flow_corrections."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    conn.register("_corr_log", df)
    conn.execute("""
        INSERT INTO v17c_flow_corrections
            (run_id, region, section_id, iteration, tier, action,
             slope_from_upstream, slope_from_downstream,
             n_reaches_flipped, reach_ids_flipped)
        SELECT run_id, region, section_id, iteration, tier, action,
               slope_from_upstream, slope_from_downstream,
               n_reaches_flipped, reach_ids_flipped
        FROM _corr_log
    """)
    conn.unregister("_corr_log")
