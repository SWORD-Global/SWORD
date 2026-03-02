"""
Section-based flow direction verification and correction.

Multi-signal composite scoring with grouped flipping and global path
verification.  Replaces the naive per-section flip loop that caused
30k disconnections in a previous attempt.

Algorithm
---------
1. Load topology + reaches, build graph, junctions, sections.
2. Score each invalid section with multi-signal composite
   (DEM WSE, SWOT WSE, facc monotonicity, slope validation).
3. Group adjacent HIGH/MEDIUM candidates into flip groups (BFS).
4. Verify each group on a virtual graph copy (DAG, connectivity,
   outlet reachability, facc improvement).
5. Apply approved flips to DuckDB with provenance.
6. Rebuild derived attributes via existing pipeline stages.
"""

import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import duckdb
import networkx as nx
import numpy as np
import pandas as pd

from .flow_direction import (
    create_flow_corrections_table,
    flip_section_topology,
    snapshot_topology,
)
from .stages._logging import log
from .stages.distances import compute_best_headwater_outlet, compute_hydro_distances
from .stages.graph import build_reach_graph, build_section_graph, identify_junctions
from .stages.loading import load_reaches, load_topology
from .stages.mainstem import compute_main_neighbors, compute_mainstem
from .stages.output import save_sections_to_duckdb, save_to_duckdb
from .stages.path_variables import compute_path_variables


# ---------------------------------------------------------------------------
# Node loading
# ---------------------------------------------------------------------------


def _load_nodes_for_scoring(
    conn: duckdb.DuckDBPyConnection, region: str
) -> pd.DataFrame:
    """Load node-level WSE and dist_out for slope computation.

    Only loads the columns needed for scoring — not the full nodes table.
    """
    log(f"Loading node-level WSE data for {region}...")
    df = conn.execute(
        """
        SELECT reach_id, node_id, dist_out, wse, wse_obs_p50
        FROM nodes
        WHERE region = ?
        AND (wse IS NOT NULL OR wse_obs_p50 IS NOT NULL)
        """,
        [region.upper()],
    ).fetchdf()
    log(f"Loaded {len(df):,} nodes with WSE data")
    return df


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------


def _compute_wse_slope(
    G: nx.DiGraph, reach_ids: List[int], col: str = "wse"
) -> Optional[float]:
    """Linear regression of WSE vs cumulative distance along section (reach-level).

    Returns slope (m/m).  Positive = WSE increases downstream = reversed.
    Returns None if < 2 reaches have valid WSE.

    Prefer ``_compute_wse_slope_nodes`` when node data is available.
    """
    points: List[Tuple[float, float]] = []
    cum_dist = 0.0
    for rid in reach_ids:
        if rid not in G.nodes:
            continue
        val = G.nodes[rid].get(col)
        rl = G.nodes[rid].get("reach_length", 0) or 0
        if val is not None and not pd.isna(val):
            points.append((cum_dist + rl / 2, val))
        cum_dist += rl

    if len(points) < 2:
        return None

    x, y = zip(*points)
    try:
        return float(np.polyfit(x, y, 1)[0])
    except Exception:
        return None


def _compute_wse_slope_nodes(
    nodes_df: pd.DataFrame, reach_ids: List[int], col: str = "wse"
) -> Optional[float]:
    """Theil-Sen regression of WSE vs dist_out at node level (~200 m spacing).

    Uses Theil-Sen (median of pairwise slopes) instead of OLS to be robust
    against outlier clusters — e.g. a few anomalous SWOT nodes at one end
    of a flat section can't dominate the slope estimate.

    dist_out decreases downstream, so a negative regression slope (WSE drops
    as dist_out drops) means correct flow.  We negate so that positive = reversed
    (WSE increases in the downstream direction), matching ``_compute_wse_slope``.

    Returns slope (m/m) or None if < 3 nodes have valid WSE.
    """
    import warnings

    from scipy.stats import theilslopes

    rids_set = set(int(r) for r in reach_ids)
    mask = nodes_df["reach_id"].isin(rids_set) & nodes_df[col].notna()
    sub = nodes_df.loc[mask, ["dist_out", col]].dropna()

    if len(sub) < 3:
        return None

    x = sub["dist_out"].values
    y = sub[col].values
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            slope, _, _, _ = theilslopes(y, x)
        # dist_out decreases downstream, so negate: positive = reversed
        return -float(slope)
    except Exception:
        return None


def _compute_facc_fraction_decreasing(
    G: nx.DiGraph, reach_ids: List[int]
) -> Optional[float]:
    """Fraction of consecutive reach pairs where facc decreases downstream.

    High fraction = likely reversed.  Returns None if < 2 valid facc values.
    """
    faccs: List[Optional[float]] = []
    for rid in reach_ids:
        if rid not in G.nodes:
            faccs.append(None)
            continue
        f = G.nodes[rid].get("facc")
        if f is not None and not pd.isna(f) and f > 0:
            faccs.append(float(f))
        else:
            faccs.append(None)

    n_dec = 0
    n_pairs = 0
    for i in range(len(faccs) - 1):
        if faccs[i] is not None and faccs[i + 1] is not None:
            n_pairs += 1
            if faccs[i + 1] < faccs[i]:
                n_dec += 1

    if n_pairs == 0:
        return None
    return n_dec / n_pairs


def _section_path_edges(entry: Dict) -> List[Tuple[int, int]]:
    """Return the ordered consecutive-pair edges for a section.

    The section path is: upstream_junction -> reach_ids[0] -> ... -> reach_ids[-1].
    reach_ids already contains the downstream junction as the last element
    (from build_section_graph), so this covers the full chain.
    """
    uj = entry["upstream_junction"]
    dj = entry["downstream_junction"]
    reach_ids = list(entry["reach_ids"])
    if not reach_ids or reach_ids[-1] != dj:
        raise ValueError(
            f"Section {entry.get('section_id')}: reach_ids[-1]={reach_ids[-1] if reach_ids else '(empty)'} "
            f"!= downstream_junction={dj}. "
            "build_section_graph must include the downstream junction in reach_ids."
        )
    ordered = [uj] + reach_ids
    return [(ordered[i], ordered[i + 1]) for i in range(len(ordered) - 1)]


def _count_facc_violations(G: nx.DiGraph, group: List[Dict]) -> int:
    """Count directed section-path edges where facc decreases.

    Only counts consecutive pairs along the section chain, not arbitrary
    chord edges between section members.  Skips edges where either endpoint
    has missing/invalid facc.
    """
    violations = 0
    for entry in group:
        for u, v in _section_path_edges(entry):
            # Check edge exists in the current graph direction
            if not G.has_edge(u, v):
                # Edge might be reversed; check the other direction
                if G.has_edge(v, u):
                    u, v = v, u
                else:
                    continue
            fu = G.nodes.get(u, {}).get("facc")
            fv = G.nodes.get(v, {}).get("facc")
            if fu is None or fv is None or pd.isna(fu) or pd.isna(fv):
                continue
            if fu <= 0 or fv <= 0:
                continue
            if fv < fu:
                violations += 1
    return violations


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def score_reversal_signals(
    G: nx.DiGraph,
    section: Dict,
    reaches_df: pd.DataFrame,
    validation_row: Dict,
    swot_slope_noise: float = 9e-5,
    dem_slope_noise: float = 1e-4,
    nodes_df: Optional[pd.DataFrame] = None,
) -> Tuple[str, Dict]:
    """Score a section into HIGH / MEDIUM / LOW / SKIP via multi-signal composite.

    When ``nodes_df`` is provided, WSE slopes are computed at ~200 m node
    resolution instead of reach-level aggregates.

    Slopes below instrument noise floors are treated as indeterminate and
    do not vote in either direction.  Noise floors:

    - SWOT: 9 cm/km (9e-5 m/m) — mission spec for slope sensitivity
    - DEM:  10 cm/km (1e-4 m/m) — conservative for MERIT Hydro (~2 m vertical)

    Signals
    -------
    1. DEM WSE regression (``wse`` column, slope > 0 = reversed)
    2. SWOT WSE regression (``wse_obs_p50``)
    3. facc monotonicity (fraction of edges where facc decreases downstream)
    4. Junction slope validation (slope_from_upstream > 0 AND slope_from_downstream < 0)

    Returns (tier, diagnostics).
    """
    direction_valid = validation_row.get("direction_valid")
    likely_cause = validation_row.get("likely_cause")

    if direction_valid is True or direction_valid is None:
        return "SKIP", {"reason": "valid_or_undetermined"}

    if likely_cause in ("lake_section", "extreme_slope_data_error"):
        return "SKIP", {"reason": f"likely_cause={likely_cause}"}

    reach_ids = section["reach_ids"]

    # Single-reach sections are too noisy — require n_reaches >= 2
    if len(reach_ids) < 2:
        return "SKIP", {"reason": "single_reach_section"}

    # Ghost filter — skip if ALL reaches are ghost (type=6)
    n_ghost = sum(1 for rid in reach_ids if G.nodes.get(rid, {}).get("type") == 6)
    if n_ghost == len(reach_ids):
        return "SKIP", {"reason": "all_ghost_reaches"}

    # Junction type filter — skip if either endpoint junction is a ghost (type=6)
    # or unreliable (type=5) stub; these are dangling stubs, not real flow paths
    for jct_key, jct_label in [
        ("upstream_junction", "upstream"),
        ("downstream_junction", "downstream"),
    ]:
        jct = section.get(jct_key)
        if jct is not None:
            jct_type = G.nodes.get(jct, {}).get("type")
            if jct_type in (5, 6):
                return "SKIP", {"reason": f"{jct_label}_junction_type={jct_type}"}

    # All-lake filter — DEM is unreliable on lakes (MERIT Hydro flattens
    # lake surfaces, creating fake WSE jumps between adjacent lake reaches)
    n_lake = sum(
        1 for rid in reach_ids if (G.nodes.get(rid, {}).get("lakeflag", 0) or 0) >= 1
    )
    if n_lake == len(reach_ids):
        return "SKIP", {"reason": "all_lake_reaches"}

    # --- Signal 1: DEM WSE ---
    use_nodes = nodes_df is not None and len(nodes_df) > 0
    if use_nodes:
        dem_slope = _compute_wse_slope_nodes(nodes_df, reach_ids, col="wse")
    else:
        dem_slope = _compute_wse_slope(G, reach_ids, col="wse")

    # --- Signal 2: SWOT WSE ---
    if use_nodes and "wse_obs_p50" in nodes_df.columns:
        swot_slope = _compute_wse_slope_nodes(nodes_df, reach_ids, col="wse_obs_p50")
    else:
        swot_slope = _compute_wse_slope(G, reach_ids, col="wse_obs_p50")

    # --- Signal 3: facc monotonicity ---
    facc_frac = _compute_facc_fraction_decreasing(G, reach_ids)

    # --- Signal 4: junction slope validation ---
    slope_up = validation_row.get("slope_from_upstream")
    slope_dn = validation_row.get("slope_from_downstream")
    # Only call a junction slope "wrong" if the magnitude exceeds noise
    upstream_wrong = pd.notna(slope_up) and slope_up > swot_slope_noise
    downstream_wrong = pd.notna(slope_dn) and slope_dn < -swot_slope_noise
    both_slopes_wrong = upstream_wrong and downstream_wrong

    # --- Tally signals (only above noise floor) ---
    # Track non-junction reversed signals separately so junction slopes
    # can't self-corroborate in tier assignment.
    n_reversed = 0
    n_total = 0
    n_nonjct_reversed = 0

    if dem_slope is not None and abs(dem_slope) > dem_slope_noise:
        n_total += 1
        if dem_slope > 0:
            n_reversed += 1
            n_nonjct_reversed += 1

    if swot_slope is not None and abs(swot_slope) > swot_slope_noise:
        n_total += 1
        if swot_slope > 0:
            n_reversed += 1
            n_nonjct_reversed += 1

    if facc_frac is not None:
        n_total += 1
        if facc_frac > 0.5:
            n_reversed += 1
            n_nonjct_reversed += 1

    # Junction slopes: only count if both sides have magnitude above noise
    up_above_noise = pd.notna(slope_up) and abs(slope_up) > swot_slope_noise
    dn_above_noise = pd.notna(slope_dn) and abs(slope_dn) > swot_slope_noise
    if up_above_noise and dn_above_noise:
        n_total += 1
        if both_slopes_wrong:
            n_reversed += 1

    diagnostics = {
        "dem_slope": dem_slope,
        "swot_slope": swot_slope,
        "facc_frac_decreasing": facc_frac,
        "slope_upstream_wrong": upstream_wrong,
        "slope_downstream_wrong": downstream_wrong,
        "both_slopes_wrong": both_slopes_wrong,
        "n_reversed": n_reversed,
        "n_nonjct_reversed": n_nonjct_reversed,
        "n_total": n_total,
    }

    # --- Tier assignment ---
    if n_reversed >= 3 and n_total >= 3:
        return "HIGH", {**diagnostics, "reason": "multi_signal_agreement"}

    if n_reversed >= 2 and both_slopes_wrong:
        return "HIGH", {**diagnostics, "reason": "slopes_plus_supporting"}

    if n_reversed >= 2:
        return "MEDIUM", {**diagnostics, "reason": "two_signal_agreement"}

    if both_slopes_wrong and n_nonjct_reversed >= 1:
        return "MEDIUM", {**diagnostics, "reason": "slopes_plus_weak_supporting"}

    return "LOW", {**diagnostics, "reason": "weak_or_conflicting"}


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def build_flip_groups(
    candidates: List[Dict],
    sections_df: pd.DataFrame,
) -> List[List[Dict]]:
    """Group adjacent candidate sections by shared junctions (BFS components).

    Returns groups sorted smallest-first for safety.
    """
    if not candidates:
        return []

    # Map section → junctions
    sid_to_junctions: Dict[int, set] = {}
    for c in candidates:
        sid = c["section_id"]
        sid_to_junctions[sid] = {c["upstream_junction"], c["downstream_junction"]}

    # Invert: junction → sections
    junction_to_sids: Dict[int, set] = defaultdict(set)
    for sid, jcts in sid_to_junctions.items():
        for j in jcts:
            junction_to_sids[j].add(sid)

    # Build adjacency graph and find connected components
    adj = nx.Graph()
    for sid in sid_to_junctions:
        adj.add_node(sid)
    for sids in junction_to_sids.values():
        sids_list = list(sids)
        for i in range(len(sids_list)):
            for k in range(i + 1, len(sids_list)):
                adj.add_edge(sids_list[i], sids_list[k])

    cand_map = {c["section_id"]: c for c in candidates}
    groups = []
    for component in nx.connected_components(adj):
        group = [cand_map[sid] for sid in component if sid in cand_map]
        if group:
            groups.append(group)

    groups.sort(key=len)
    return groups


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_flip_group(
    G: nx.DiGraph,
    group: List[Dict],
    sections_df: pd.DataFrame,
    reaches_df: pd.DataFrame,
    max_group_size: int = 30,
) -> Tuple[bool, str, Dict]:
    """Verify a flip group on a virtual graph copy.

    Checks
    ------
    1. Group size within cap.
    2. DAG preserved (no cycles).
    3. Weakly connected component count unchanged.
    4. Every boundary junction still reaches an outlet.
    5. facc violations decrease (or at least don't increase).

    Returns (approved, reason, diagnostics).
    """
    if len(group) > max_group_size:
        return (
            False,
            f"group_too_large ({len(group)} > {max_group_size})",
            {"group_size": len(group)},
        )

    # Pre-flip facc violations
    pre_violations = _count_facc_violations(G, group)

    # Clone graph
    G_test = G.copy()

    # Flip section-path edges for every section in group
    for entry in group:
        for u, v in _section_path_edges(entry):
            if G_test.has_edge(u, v):
                G_test.remove_edge(u, v)
                G_test.add_edge(v, u)
            elif G_test.has_edge(v, u):
                G_test.remove_edge(v, u)
                G_test.add_edge(u, v)

    # Check 1: DAG
    if not nx.is_directed_acyclic_graph(G_test):
        return False, "cycle_introduced", {}

    # Check 2: weakly connected components unchanged
    wcc_before = nx.number_weakly_connected_components(G)
    wcc_after = nx.number_weakly_connected_components(G_test)
    if wcc_after != wcc_before:
        return (
            False,
            f"disconnection ({wcc_before} -> {wcc_after} components)",
            {"wcc_before": wcc_before, "wcc_after": wcc_after},
        )

    # Check 3: boundary junctions reach a *pre-flip* outlet
    # Use pre-flip outlets to prevent accepting newly created interior sinks
    pre_outlets = {n for n in G.nodes() if G.out_degree(n) == 0}
    for entry in group:
        for junction in (entry["upstream_junction"], entry["downstream_junction"]):
            if junction in pre_outlets:
                continue
            reachable = nx.descendants(G_test, junction) | {junction}
            if not reachable & pre_outlets:
                return (
                    False,
                    f"junction {junction} cannot reach outlet",
                    {"junction": junction},
                )

    # Check 4: facc violations should not increase
    post_violations = _count_facc_violations(G_test, group)

    diagnostics = {
        "pre_facc_violations": pre_violations,
        "post_facc_violations": post_violations,
        "wcc_before": wcc_before,
        "wcc_after": wcc_after,
        "n_sections": len(group),
    }

    if post_violations > pre_violations:
        return (
            False,
            f"facc violations increased ({pre_violations} -> {post_violations})",
            diagnostics,
        )

    return True, "passed", diagnostics


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def apply_verified_flips(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    approved_groups: List[List[Dict]],
    run_id: str,
) -> Dict:
    """Batch-write approved flips to DuckDB with provenance.

    1. snapshot_topology() backup
    2. flip_section_topology() per section
    3. Log to v17c_flow_corrections
    """
    create_flow_corrections_table(conn)
    snapshot_topology(conn, region, run_id)

    total_rows_flipped = 0
    log_rows: List[Dict] = []

    conn.execute("BEGIN TRANSACTION")
    try:
        for group in approved_groups:
            for entry in group:
                sid = entry["section_id"]
                reach_ids = entry["reach_ids"]
                uj = entry["upstream_junction"]
                dj = entry["downstream_junction"]
                tier = entry["tier"]

                n = flip_section_topology(conn, region, reach_ids, uj, dj)
                total_rows_flipped += n
                log(f"  Flipped section {sid} ({tier}): {n} topology rows")

                log_rows.append(
                    {
                        "run_id": run_id,
                        "region": region.upper(),
                        "section_id": sid,
                        "iteration": 1,
                        "tier": tier,
                        "action": "flip",
                        "slope_from_upstream": entry.get("diagnostics", {}).get(
                            "slope_upstream_wrong"
                        ),
                        "slope_from_downstream": entry.get("diagnostics", {}).get(
                            "slope_downstream_wrong"
                        ),
                        "n_reaches_flipped": len(reach_ids),
                        "reach_ids_flipped": json.dumps([int(r) for r in reach_ids]),
                    }
                )

        # Write provenance log
        if log_rows:
            df = pd.DataFrame(log_rows)
            conn.register("_fv_log", df)
            conn.execute("""
                INSERT INTO v17c_flow_corrections
                    (run_id, region, section_id, iteration, tier, action,
                     slope_from_upstream, slope_from_downstream,
                     n_reaches_flipped, reach_ids_flipped)
                SELECT run_id, region, section_id, iteration, tier, action,
                       slope_from_upstream, slope_from_downstream,
                       n_reaches_flipped, reach_ids_flipped
                FROM _fv_log
            """)
            conn.unregister("_fv_log")

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n_sections = sum(len(g) for g in approved_groups)
    log(f"Applied {n_sections} section flips ({total_rows_flipped} topology rows)")

    return {
        "run_id": run_id,
        "region": region,
        "n_sections_flipped": n_sections,
        "n_topology_rows_flipped": total_rows_flipped,
        "n_groups": len(approved_groups),
    }


# ---------------------------------------------------------------------------
# Rebuild
# ---------------------------------------------------------------------------


def _update_topology_counts(conn: duckdb.DuckDBPyConnection, region: str) -> None:
    """Recompute n_rch_up, n_rch_down, end_reach from reach_topology."""
    log(f"Recomputing topology counts for {region}...")

    conn.execute("INSTALL spatial; LOAD spatial;")
    rtree_indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%' AND table_name = 'reaches'"
    ).fetchall()
    for idx_name, _, _ in rtree_indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')

    try:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE _topo_counts AS
            SELECT
                r.reach_id,
                r.region,
                COALESCE(up_ct.n, 0) AS new_n_rch_up,
                COALESCE(dn_ct.n, 0) AS new_n_rch_down
            FROM reaches r
            LEFT JOIN (
                SELECT reach_id, region, COUNT(*) AS n
                FROM reach_topology WHERE direction = 'up'
                GROUP BY reach_id, region
            ) up_ct ON r.reach_id = up_ct.reach_id AND r.region = up_ct.region
            LEFT JOIN (
                SELECT reach_id, region, COUNT(*) AS n
                FROM reach_topology WHERE direction = 'down'
                GROUP BY reach_id, region
            ) dn_ct ON r.reach_id = dn_ct.reach_id AND r.region = dn_ct.region
            WHERE r.region = ?
        """,
            [region.upper()],
        )

        conn.execute("""
            UPDATE reaches SET
                n_rch_up = tc.new_n_rch_up,
                n_rch_down = tc.new_n_rch_down,
                end_reach = CASE
                    WHEN tc.new_n_rch_up > 1 OR tc.new_n_rch_down > 1 THEN 3
                    WHEN tc.new_n_rch_up = 0 THEN 1
                    WHEN tc.new_n_rch_down = 0 THEN 2
                    ELSE 0
                END
            FROM _topo_counts tc
            WHERE reaches.reach_id = tc.reach_id
            AND reaches.region = tc.region
        """)

        conn.execute("DROP TABLE IF EXISTS _topo_counts")
    finally:
        for _, _, sql in rtree_indexes:
            conn.execute(sql)

    log(f"Topology counts updated for {region}")


def rebuild_derived_attrs(
    conn: duckdb.DuckDBPyConnection, db_path: str, region: str
) -> None:
    """Rebuild all derived attributes after topology changes.

    Sequence
    --------
    1. Recompute n_rch_up, n_rch_down, end_reach via SQL.
    2. Reload topology/reaches, rebuild graph.
    3. compute_path_variables
    4. compute_hydro_distances
    5. compute_best_headwater_outlet
    6. compute_mainstem
    7. compute_main_neighbors
    8. save_to_duckdb
    9. save_sections_to_duckdb (with re-validated slopes)
    """
    from .v17c_pipeline import compute_junction_slopes

    log(f"Rebuilding derived attributes for {region}...")

    # Step 1: update counts
    _update_topology_counts(conn, region)

    # Step 2: reload and rebuild
    topology_df = load_topology(conn, region)
    reaches_df = load_reaches(conn, region)
    G = build_reach_graph(topology_df, reaches_df)

    if not nx.is_directed_acyclic_graph(G):
        log("WARNING: rebuilt graph contains cycles!")
        try:
            cycle = nx.find_cycle(G)
            log(f"  Example cycle: {cycle[:5]}...")
        except nx.NetworkXNoCycle:
            pass

    junctions = identify_junctions(G)
    _, sections_df = build_section_graph(G, junctions)

    # Steps 3-7: compute all v17c attributes
    path_vars = compute_path_variables(G, sections_df, region=region)
    hydro_dist = compute_hydro_distances(G)
    hw_out = compute_best_headwater_outlet(G)
    is_mainstem = compute_mainstem(G, hw_out)
    main_neighbors = compute_main_neighbors(G)

    # Step 8: save
    save_to_duckdb(
        conn, region, hydro_dist, hw_out, is_mainstem, main_neighbors, path_vars
    )

    # Step 9: recompute and save sections
    validation_df = compute_junction_slopes(G, sections_df, reaches_df)
    save_sections_to_duckdb(conn, region, sections_df, validation_df)

    log(f"Derived attributes rebuilt for {region}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run_flow_verification(
    db_path: str,
    region: str,
    dry_run: bool = True,
    max_group_size: int = 30,
    min_section_size: int = 2,
    skip_rebuild: bool = False,
) -> Dict:
    """Score, group, verify, and optionally apply section-based flow corrections.

    Parameters
    ----------
    db_path
        Path to sword_v17c.duckdb.
    region
        Region code (NA, SA, EU, AF, AS, OC).
    dry_run
        If True, score and report only — no DB writes.
    max_group_size
        Maximum sections per flip group before rejection.
    min_section_size
        Minimum reaches per section to be eligible for correction.
    skip_rebuild
        Skip derived-attribute rebuild after applying (debugging).

    Returns
    -------
    dict with keys: region, run_id, dry_run, n_approved, n_rejected, n_low,
    n_skipped, approved_groups, rejected_groups, low_confidence.
    """
    from .v17c_pipeline import compute_junction_slopes

    run_id = uuid4().hex[:12]
    log(f"Flow verification: region={region}, run_id={run_id}, dry_run={dry_run}")

    conn = duckdb.connect(db_path)
    try:
        return _run_inner(
            conn=conn,
            db_path=db_path,
            region=region,
            run_id=run_id,
            dry_run=dry_run,
            max_group_size=max_group_size,
            min_section_size=min_section_size,
            skip_rebuild=skip_rebuild,
            compute_junction_slopes_fn=compute_junction_slopes,
        )
    finally:
        conn.close()


def _run_inner(
    *,
    conn: duckdb.DuckDBPyConnection,
    db_path: str,
    region: str,
    run_id: str,
    dry_run: bool,
    max_group_size: int,
    min_section_size: int,
    skip_rebuild: bool,
    compute_junction_slopes_fn,
) -> Dict:
    """Inner implementation — separated for testability."""
    # 1. Load data
    topology_df = load_topology(conn, region)
    reaches_df = load_reaches(conn, region)
    nodes_df = _load_nodes_for_scoring(conn, region)

    # 2. Build graph, junctions, sections
    G = build_reach_graph(topology_df, reaches_df)
    junctions = identify_junctions(G)
    _, sections_df = build_section_graph(G, junctions)

    # 3. Compute junction-level slope validation
    # Check for any usable WSE column (prefer SWOT p50, fallback to DEM wse)
    has_wse = False
    for wse_col in ("wse_obs_p50", "wse"):
        if wse_col in reaches_df.columns and reaches_df[wse_col].notna().any():
            has_wse = True
            break
    if not has_wse:
        log(f"No WSE data for {region}, skipping flow verification")
        return {"region": region, "run_id": run_id, "n_candidates": 0}

    validation_df = compute_junction_slopes_fn(G, sections_df, reaches_df)
    if validation_df.empty:
        log(f"No sections with slope validation for {region}")
        return {"region": region, "run_id": run_id, "n_candidates": 0}

    # 4. Score invalid sections
    invalid = validation_df[validation_df["direction_valid"] == False]  # noqa: E712
    log(f"Invalid sections to evaluate: {len(invalid):,}")

    # Build section lookup
    sec_map: Dict[int, Dict] = {}
    for _, row in sections_df.iterrows():
        sec_map[int(row["section_id"])] = row.to_dict()

    candidates: List[Dict] = []
    skipped: List[Dict] = []
    low_confidence: List[Dict] = []

    for _, vrow in invalid.iterrows():
        sid = int(vrow["section_id"])
        sec = sec_map.get(sid)
        if sec is None:
            continue

        # Skip short sections
        if len(sec["reach_ids"]) < min_section_size:
            skipped.append({"section_id": sid, "reason": "below_min_section_size"})
            continue

        tier, diag = score_reversal_signals(
            G, sec, reaches_df, vrow.to_dict(), nodes_df=nodes_df
        )

        entry = {
            "section_id": sid,
            "tier": tier,
            "diagnostics": diag,
            "reach_ids": sec["reach_ids"],
            "upstream_junction": sec["upstream_junction"],
            "downstream_junction": sec["downstream_junction"],
            "distance": sec["distance"],
            "n_reaches": sec["n_reaches"],
        }

        if tier in ("HIGH", "MEDIUM"):
            candidates.append(entry)
        elif tier == "LOW":
            low_confidence.append(entry)
        else:
            skipped.append({"section_id": sid, "reason": diag.get("reason", "skip")})

    log(
        f"Scoring: {len(candidates)} HIGH/MEDIUM, "
        f"{len(low_confidence)} LOW, {len(skipped)} SKIP"
    )

    # 5. Group adjacent candidates
    groups = build_flip_groups(candidates, sections_df)
    log(f"Formed {len(groups)} flip groups from {len(candidates)} candidates")

    # 6. Verify each group
    approved_groups: List[List[Dict]] = []
    rejected_groups: List[Dict] = []

    for i, group in enumerate(groups):
        ok, reason, diag = verify_flip_group(
            G, group, sections_df, reaches_df, max_group_size
        )
        if ok:
            approved_groups.append(group)
            log(f"  Group {i}: APPROVED ({len(group)} sections) - {reason}")
        else:
            rejected_groups.append(
                {"group_idx": i, "n_sections": len(group), "reason": reason, **diag}
            )
            log(f"  Group {i}: REJECTED ({len(group)} sections) - {reason}")

    # 7. Report
    n_approved = sum(len(g) for g in approved_groups)
    n_rejected = sum(r["n_sections"] for r in rejected_groups)

    log(f"\nSummary for {region}:")
    log(f"  Approved: {n_approved} sections in {len(approved_groups)} groups")
    log(f"  Rejected: {n_rejected} sections in {len(rejected_groups)} groups")
    log(f"  Low confidence: {len(low_confidence)} sections (manual review)")
    log(f"  Skipped: {len(skipped)} sections")

    result = {
        "region": region,
        "run_id": run_id,
        "dry_run": dry_run,
        "n_approved": n_approved,
        "n_rejected": n_rejected,
        "n_low": len(low_confidence),
        "n_skipped": len(skipped),
        "approved_groups": approved_groups,
        "rejected_groups": rejected_groups,
        "low_confidence": low_confidence,
    }

    if dry_run:
        log("DRY RUN — no changes applied")
        return result

    # 8. Apply approved flips
    if not approved_groups:
        log("No approved groups to apply")
        return result

    apply_result = apply_verified_flips(conn, region, approved_groups, run_id)
    result.update(apply_result)

    # 9. Rebuild derived attributes
    if not skip_rebuild:
        rebuild_derived_attrs(conn, db_path, region)

    return result
