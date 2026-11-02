#!/usr/bin/env python3
"""
SWORD Topology Investigator
----------------------------

Streamlit GUI to investigate topology issues using SWOT WSE data.
Shows side profile views and map context for edges.

Usage:
    streamlit run topology_investigator.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
import duckdb
import os
from pathlib import Path
from typing import List

# Page config
st.set_page_config(
    page_title="SWORD Topology Investigator", page_icon="🔍", layout="wide"
)

# Planet Basemaps API key for the satellite basemap. Read once at startup so a
# missing key fails before any data is loaded.
PLANET_API_KEY = os.environ.get("PLANET_API_KEY", "").strip()
if not PLANET_API_KEY:
    raise RuntimeError(
        "PLANET_API_KEY is not set. Export it or add it to .env (see .env.example) "
        "before launching the topology investigator."
    )


# Cache data loading
@st.cache_data
def load_swot_data(swot_path: str, region: str) -> pd.DataFrame:
    """Load SWOT WSE data."""
    swot_dir = Path(swot_path)
    reaches_dir = swot_dir / "reaches"

    if not reaches_dir.exists():
        return pd.DataFrame()

    parquet_files = list(reaches_dir.glob("*.parquet"))
    dfs = []
    for pf in parquet_files:
        try:
            df = pd.read_parquet(pf, columns=["reach_id", "wse", "time_str"])
            dfs.append(df)
        except:
            pass

    if not dfs:
        return pd.DataFrame()

    swot_df = pd.concat(dfs, ignore_index=True)

    # Filter by region
    region_prefixes = {"NA": "7", "SA": "6", "EU": "2", "AF": "3", "AS": "4", "OC": "5"}
    prefix = region_prefixes.get(region.upper())
    if prefix:
        swot_df = swot_df[swot_df["reach_id"].astype(str).str.startswith(prefix)]

    # Filter fill values
    swot_df = swot_df[(swot_df["wse"] > -1e9) & (swot_df["wse"] < 1e9)]

    return swot_df


@st.cache_data
def load_swot_summary(swot_path: str, region: str) -> pd.DataFrame:
    """Load and summarize SWOT data."""
    swot_df = load_swot_data(swot_path, region)
    if len(swot_df) == 0:
        return pd.DataFrame()

    summary = (
        swot_df.groupby("reach_id")
        .agg({"wse": ["mean", "std", "count", "min", "max"]})
        .reset_index()
    )
    summary.columns = [
        "reach_id",
        "wse_mean",
        "wse_std",
        "wse_count",
        "wse_min",
        "wse_max",
    ]
    summary["reach_id"] = summary["reach_id"].astype(int)
    return summary


@st.cache_data
def load_reaches(db_path: str, region: str) -> pd.DataFrame:
    """Load reach data from DuckDB."""
    conn = duckdb.connect(db_path, read_only=True)
    query = """
        SELECT r.reach_id, r.river_name, r.reach_length, r.wse as sword_wse,
               r.width, r.facc, r.n_rch_up, r.n_rch_down, r.lakeflag, r.trib_flag,
               AVG(n.x) as x, AVG(n.y) as y
        FROM reaches r
        LEFT JOIN nodes n ON r.reach_id = n.reach_id AND r.region = n.region
        WHERE r.region = ?
        GROUP BY r.reach_id, r.river_name, r.reach_length, r.wse, r.width,
                 r.facc, r.n_rch_up, r.n_rch_down, r.lakeflag, r.trib_flag
    """
    df = conn.execute(query, [region]).fetchdf()
    conn.close()
    return df


@st.cache_data
def load_topology(db_path: str, region: str) -> pd.DataFrame:
    """Load topology from DuckDB."""
    conn = duckdb.connect(db_path, read_only=True)
    query = """
        SELECT reach_id, direction, neighbor_reach_id
        FROM reach_topology
        WHERE region = ?
    """
    df = conn.execute(query, [region]).fetchdf()
    conn.close()
    return df


@st.cache_data
def build_graph(reaches_df: pd.DataFrame, topo_df: pd.DataFrame) -> nx.DiGraph:
    """Build directed graph from data."""
    G = nx.DiGraph()

    for _, row in reaches_df.iterrows():
        G.add_node(row["reach_id"], **row.to_dict())

    down_edges = topo_df[topo_df["direction"] == "down"]
    for _, row in down_edges.iterrows():
        src, dst = row["reach_id"], row["neighbor_reach_id"]
        if src in G.nodes and dst in G.nodes:
            G.add_edge(src, dst)

    return G


def get_flow_path(
    G: nx.DiGraph, start_node: int, direction: str = "downstream", max_length: int = 20
) -> List[int]:
    """Get flow path from a node."""
    path = [start_node]
    current = start_node

    for _ in range(max_length):
        if direction == "downstream":
            neighbors = list(G.successors(current))
        else:
            neighbors = list(G.predecessors(current))

        if not neighbors:
            break
        current = neighbors[0]  # Follow main path
        path.append(current)

    return path


def get_extended_path(
    G: nx.DiGraph, node1: int, node2: int, context: int = 5
) -> List[int]:
    """Get path with upstream and downstream context."""
    # Get upstream of node1
    upstream = get_flow_path(G, node1, "upstream", context)[::-1]

    # Get downstream of node2
    downstream = get_flow_path(G, node2, "downstream", context)

    # Combine (upstream includes node1, downstream starts with node2)
    path = upstream[:-1] + [node1, node2] + downstream[1:]

    return path


def analyze_disagreements(
    reaches_df: pd.DataFrame,
    topo_df: pd.DataFrame,
    swot_summary: pd.DataFrame,
    wse_threshold: float = 2.0,
) -> pd.DataFrame:
    """Find edges where SWOT disagrees with current topology."""
    if len(swot_summary) == 0 or "reach_id" not in swot_summary.columns:
        return pd.DataFrame()

    G = build_graph(reaches_df, topo_df)

    swot_dict = dict(zip(swot_summary["reach_id"], swot_summary["wse_mean"]))
    swot_std = dict(zip(swot_summary["reach_id"], swot_summary["wse_std"]))
    swot_count = dict(zip(swot_summary["reach_id"], swot_summary["wse_count"]))

    results = []
    for u, v in G.edges():
        u_wse = swot_dict.get(u)
        v_wse = swot_dict.get(v)

        if u_wse is None or v_wse is None:
            continue

        u_data = G.nodes[u]
        v_data = G.nodes[v]

        wse_diff = u_wse - v_wse  # positive = u higher = current correct
        swot_agrees = wse_diff > 0

        # facc check
        u_facc = u_data.get("facc", 0) or 0
        v_facc = v_data.get("facc", 0) or 0
        facc_consistent = u_facc < v_facc if u_facc > 0 and v_facc > 0 else None

        results.append(
            {
                "upstream": u,
                "downstream": v,
                "upstream_wse": u_wse,
                "downstream_wse": v_wse,
                "wse_diff": wse_diff,
                "abs_wse_diff": abs(wse_diff),
                "swot_agrees": swot_agrees,
                "upstream_facc": u_facc,
                "downstream_facc": v_facc,
                "facc_consistent": facc_consistent,
                "upstream_std": swot_std.get(u, 0),
                "downstream_std": swot_std.get(v, 0),
                "upstream_count": swot_count.get(u, 0),
                "downstream_count": swot_count.get(v, 0),
                "river_name": u_data.get("river_name", ""),
                "upstream_x": u_data.get("x"),
                "upstream_y": u_data.get("y"),
                "downstream_x": v_data.get("x"),
                "downstream_y": v_data.get("y"),
            }
        )

    df = pd.DataFrame(results)
    return df


# Sidebar
st.sidebar.title("Settings")
db_path = st.sidebar.text_input("Database Path", "data/duckdb/sword_v17b.duckdb")
swot_path = st.sidebar.text_input(
    "SWOT Path", "/Volumes/SWORD_DATA/data/swot/parquet_lake_D"
)
region = st.sidebar.selectbox("Region", ["NA", "SA", "EU", "AF", "AS", "OC"])
wse_threshold = st.sidebar.slider("WSE Threshold (m)", 0.5, 10.0, 2.0, 0.5)

# Decisions section
st.sidebar.write("---")
st.sidebar.subheader("📋 Decisions")

if "decisions" not in st.session_state:
    st.session_state.decisions = {}

decisions = st.session_state.decisions
n_decisions = len(decisions)
n_keep = sum(1 for d in decisions.values() if d.get("action") == "keep")
n_flip = sum(1 for d in decisions.values() if d.get("action") == "flip")
n_skip = sum(1 for d in decisions.values() if d.get("action") == "skip")

st.sidebar.write(f"**Total reviewed:** {n_decisions}")
st.sidebar.write(f"✅ Keep: {n_keep} | 🔄 Flip: {n_flip} | ⏭️ Skip: {n_skip}")

if n_decisions > 0:
    if st.sidebar.button("💾 Export Decisions"):
        # Export to CSV
        export_data = []
        for key, dec in decisions.items():
            if dec.get("action") in ["keep", "flip"]:
                export_data.append(
                    {
                        "upstream": dec.get("upstream"),
                        "downstream": dec.get("downstream"),
                        "action": dec.get("action"),
                    }
                )
        if export_data:
            export_df = pd.DataFrame(export_data)
            export_path = f"/tmp/topology_decisions_{region}.csv"
            export_df.to_csv(export_path, index=False)
            st.sidebar.success(f"Saved to {export_path}")

    if st.sidebar.button("🗑️ Clear All Decisions"):
        st.session_state.decisions = {}
        st.rerun()

# Main content
st.title("🔍 SWORD Topology Investigator")

# Load data
with st.spinner("Loading data..."):
    reaches_df = load_reaches(db_path, region)
    topo_df = load_topology(db_path, region)
    swot_summary = load_swot_summary(swot_path, region)
    G = build_graph(reaches_df, topo_df)

st.success(f"Loaded {len(reaches_df):,} reaches, {len(swot_summary):,} with SWOT data")

# Analyze disagreements
with st.spinner("Analyzing disagreements..."):
    disagree_df = analyze_disagreements(
        reaches_df, topo_df, swot_summary, wse_threshold
    )

# Check if we have data
if len(disagree_df) == 0 or "swot_agrees" not in disagree_df.columns:
    st.error("No SWOT data found for this region. Please check the SWOT data path.")
    st.stop()

# Filter to disagreements
disagreements = disagree_df[disagree_df["swot_agrees"] == False].copy()
strong_disagreements = disagreements[disagreements["abs_wse_diff"] > wse_threshold]

st.subheader("Summary")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Edges with SWOT", len(disagree_df))
col2.metric("SWOT Agrees", len(disagree_df[disagree_df["swot_agrees"]]))
col3.metric("SWOT Disagrees", len(disagreements))
col4.metric(f"Strong (>{wse_threshold}m)", len(strong_disagreements))

# Table of disagreements
st.subheader("Disagreements (SWOT says direction is wrong)")

# Filter options
filter_col1, filter_col2 = st.columns(2)
with filter_col1:
    min_wse = st.number_input("Min |WSE diff| (m)", 0.0, 50.0, wse_threshold)
with filter_col2:
    facc_filter = st.selectbox(
        "facc Support", ["All", "facc also disagrees", "facc agrees"]
    )

filtered = disagreements[disagreements["abs_wse_diff"] >= min_wse]
if facc_filter == "facc also disagrees":
    filtered = filtered[filtered["facc_consistent"] == False]
elif facc_filter == "facc agrees":
    filtered = filtered[filtered["facc_consistent"] == True]

st.write(f"Showing {len(filtered)} edges")

# Display table
display_cols = [
    "upstream",
    "downstream",
    "wse_diff",
    "upstream_facc",
    "downstream_facc",
    "facc_consistent",
    "river_name",
]
st.dataframe(
    filtered[display_cols].sort_values("wse_diff", ascending=True).head(100),
    width="stretch",
)

# Select edge for detailed view
st.subheader("Detailed Investigation")

# Initialize session state
if "flip_direction" not in st.session_state:
    st.session_state.flip_direction = False
if "last_selected_edge" not in st.session_state:
    st.session_state.last_selected_edge = None

if len(filtered) > 0:
    edge_options = [
        f"{row['upstream']} → {row['downstream']} (WSE diff: {row['wse_diff']:.2f}m)"
        for _, row in filtered.head(50).iterrows()
    ]
    selected_edge = st.selectbox("Select edge to investigate", edge_options)

    # Reset flip state when selecting a new edge
    if selected_edge != st.session_state.last_selected_edge:
        st.session_state.flip_direction = False
        st.session_state.last_selected_edge = selected_edge

    if selected_edge:
        # Parse selected edge
        parts = selected_edge.split(" → ")
        upstream_id = int(parts[0])
        downstream_id = int(parts[1].split(" ")[0])

        # Get extended path
        path = get_extended_path(G, upstream_id, downstream_id, context=8)

        # Get SWOT data for path
        swot_dict = dict(zip(swot_summary["reach_id"], swot_summary["wse_mean"]))
        swot_std_dict = dict(zip(swot_summary["reach_id"], swot_summary["wse_std"]))

        path_data = []
        cumulative_dist = 0
        for i, rid in enumerate(path):
            node_data = G.nodes.get(rid, {})
            wse = swot_dict.get(rid)
            wse_std = swot_std_dict.get(rid, 0)

            # Get reach length
            reach_len = node_data.get("reach_length", 1000) or 1000
            if i > 0:
                cumulative_dist += reach_len / 1000  # km

            path_data.append(
                {
                    "reach_id": rid,
                    "position": i,
                    "distance_km": cumulative_dist,
                    "wse": wse,
                    "wse_std": wse_std,
                    "sword_wse": node_data.get("sword_wse"),
                    "facc": node_data.get("facc", 0),
                    "x": node_data.get("x"),
                    "y": node_data.get("y"),
                    "is_selected": rid in [upstream_id, downstream_id],
                }
            )

        path_df = pd.DataFrame(path_data)

        # Create profile plot
        col1, col2 = st.columns([2, 1])

        with col1:
            # Check if direction is flipped for profile
            profile_flipped = st.session_state.get("flip_direction", False)
            direction_label = "(FLIPPED)" if profile_flipped else "(Current Direction)"
            st.write(f"**WSE Profile Along Flow Path** {direction_label}")

            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=("SWOT WSE Profile", "Flow Accumulation"),
            )

            # If flipped, reverse the path for display
            display_df = path_df.copy()
            if profile_flipped:
                display_df = display_df.iloc[::-1].reset_index(drop=True)
                # Recalculate distance
                cumulative_dist = 0
                new_distances = []
                for i, row in display_df.iterrows():
                    if i > 0:
                        reach_len = row.get("reach_length", 1000) or 1000
                        if pd.isna(reach_len):
                            reach_len = 1000
                        cumulative_dist += 1  # simplified: 1 unit per reach
                    new_distances.append(cumulative_dist)
                display_df["distance_km"] = new_distances

            # WSE Profile
            valid_wse = display_df[display_df["wse"].notna()]
            if len(valid_wse) > 0:
                # WSE line
                fig.add_trace(
                    go.Scatter(
                        x=valid_wse["distance_km"],
                        y=valid_wse["wse"],
                        mode="lines+markers",
                        name="SWOT WSE",
                        line=dict(color="blue"),
                        error_y=dict(
                            type="data", array=valid_wse["wse_std"], visible=True
                        ),
                    ),
                    row=1,
                    col=1,
                )

                # Highlight selected edge
                selected_df = display_df[display_df["is_selected"]]
                fig.add_trace(
                    go.Scatter(
                        x=selected_df["distance_km"],
                        y=selected_df["wse"],
                        mode="markers",
                        name="Selected Edge",
                        marker=dict(color="red", size=15, symbol="star"),
                    ),
                    row=1,
                    col=1,
                )

                # Add SWORD WSE for comparison
                sword_valid = display_df[
                    (display_df["sword_wse"].notna()) & (display_df["sword_wse"] > 0)
                ]
                if len(sword_valid) > 0:
                    fig.add_trace(
                        go.Scatter(
                            x=sword_valid["distance_km"],
                            y=sword_valid["sword_wse"],
                            mode="lines+markers",
                            name="SWORD WSE",
                            line=dict(color="gray", dash="dash"),
                            opacity=0.5,
                        ),
                        row=1,
                        col=1,
                    )

            # facc Profile
            fig.add_trace(
                go.Scatter(
                    x=display_df["distance_km"],
                    y=display_df["facc"],
                    mode="lines+markers",
                    name="facc",
                    line=dict(color="green"),
                ),
                row=2,
                col=1,
            )

            # Add simple UPSTREAM/DOWNSTREAM labels at ends
            if len(display_df) > 1:
                max_x = display_df["distance_km"].max()
                valid_wse_vals = display_df["wse"].dropna()
                if len(valid_wse_vals) > 0:
                    max_wse = valid_wse_vals.max()
                else:
                    max_wse = 100

                label_color = "orange" if profile_flipped else "blue"

                # UPSTREAM label on left
                fig.add_annotation(
                    text="UPSTREAM",
                    xref="x",
                    yref="paper",
                    x=0,
                    y=0.95,
                    showarrow=False,
                    font=dict(size=16, color=label_color, family="Arial Black"),
                    row=1,
                    col=1,
                )

                # DOWNSTREAM label on right
                fig.add_annotation(
                    text="DOWNSTREAM",
                    xref="x",
                    yref="paper",
                    x=max_x,
                    y=0.95,
                    showarrow=False,
                    font=dict(size=16, color=label_color, family="Arial Black"),
                    xanchor="right",
                    row=1,
                    col=1,
                )

                # Simple arrow in middle
                fig.add_annotation(
                    text="→→→",
                    xref="x",
                    yref="paper",
                    x=max_x * 0.5,
                    y=0.95,
                    showarrow=False,
                    font=dict(size=20, color=label_color),
                    row=1,
                    col=1,
                )

            fig.update_layout(height=500, showlegend=True)
            fig.update_xaxes(title_text="Distance along flow path (km)", row=2, col=1)
            fig.update_yaxes(title_text="WSE (m)", row=1, col=1)
            fig.update_yaxes(title_text="facc", row=2, col=1)

            st.plotly_chart(fig, width="stretch")

        with col2:
            st.write("**Map View**")

            # Direction toggle
            if "flip_direction" not in st.session_state:
                st.session_state.flip_direction = False

            flip_col1, flip_col2 = st.columns(2)
            with flip_col1:
                current_dir = (
                    "FLIPPED" if st.session_state.flip_direction else "CURRENT"
                )
                st.write(f"Direction: **{current_dir}**")
            with flip_col2:
                if st.button("🔄 Flip Direction"):
                    st.session_state.flip_direction = (
                        not st.session_state.flip_direction
                    )
                    st.rerun()

            # Simple map using plotly
            map_df = path_df[path_df["x"].notna() & path_df["y"].notna()]
            if len(map_df) > 0:
                fig_map = go.Figure()

                # Path line (background)
                fig_map.add_trace(
                    go.Scattermapbox(
                        lon=map_df["x"],
                        lat=map_df["y"],
                        mode="lines+markers",
                        marker=dict(size=6, color="lightblue"),
                        line=dict(width=2, color="lightblue"),
                        name="Flow Path",
                        opacity=0.5,
                    )
                )

                # Add flow direction arrows along the path
                import math

                def create_arrowhead(
                    start_lon, start_lat, end_lon, end_lat, head_length=0.003
                ):
                    """Create arrowhead lines pointing from start to end."""
                    # Calculate angle of the line
                    dx = end_lon - start_lon
                    dy = end_lat - start_lat
                    angle = math.atan2(dy, dx)

                    # Arrowhead angle (30 degrees from line)
                    arrow_angle = math.radians(25)

                    # Calculate arrowhead points
                    left_x = end_lon - head_length * math.cos(angle - arrow_angle)
                    left_y = end_lat - head_length * math.sin(angle - arrow_angle)
                    right_x = end_lon - head_length * math.cos(angle + arrow_angle)
                    right_y = end_lat - head_length * math.sin(angle + arrow_angle)

                    return [(left_x, left_y), (end_lon, end_lat), (right_x, right_y)]

                for i in range(len(map_df) - 1):
                    x1, y1 = map_df.iloc[i]["x"], map_df.iloc[i]["y"]
                    x2, y2 = map_df.iloc[i + 1]["x"], map_df.iloc[i + 1]["y"]
                    rid1 = map_df.iloc[i]["reach_id"]
                    rid2 = map_df.iloc[i + 1]["reach_id"]

                    # Determine if this is the selected edge
                    is_selected_edge = bool(
                        (rid1 == upstream_id and rid2 == downstream_id)
                        or (rid1 == downstream_id and rid2 == upstream_id)
                    )

                    # For selected edge, check if we should flip
                    if is_selected_edge and st.session_state.flip_direction:
                        # Flip: arrow points opposite direction
                        start_x, start_y = x2, y2
                        end_x, end_y = x1, y1
                        arrow_color = "#FF6600"  # Bright orange
                        arrow_name = "FLIPPED Direction"
                        flow_text = f"{rid2} → {rid1}"
                    else:
                        # Normal: arrow points in current topology direction
                        start_x, start_y = x1, y1
                        end_x, end_y = x2, y2
                        # Yellow for unselected (visible on satellite), bright red for selected
                        arrow_color = "#FFFF00" if not is_selected_edge else "#FF0000"
                        arrow_name = (
                            "Current Flow" if not is_selected_edge else "Selected Edge"
                        )
                        flow_text = f"{rid1} → {rid2}"

                    # Line width based on selection - THICKER for visibility
                    line_width = 10 if is_selected_edge else 6

                    # Draw the main arrow line
                    fig_map.add_trace(
                        go.Scattermapbox(
                            lon=[start_x, end_x],
                            lat=[start_y, end_y],
                            mode="lines",
                            line=dict(width=line_width, color=arrow_color),
                            name=arrow_name if is_selected_edge else None,
                            showlegend=is_selected_edge,
                            hoverinfo="text",
                            hovertext=flow_text,
                        )
                    )

                    # Create and draw arrowhead - BIGGER for visibility
                    head_size = 0.008 if is_selected_edge else 0.005
                    arrowhead = create_arrowhead(
                        start_x, start_y, end_x, end_y, head_size
                    )

                    fig_map.add_trace(
                        go.Scattermapbox(
                            lon=[p[0] for p in arrowhead],
                            lat=[p[1] for p in arrowhead],
                            mode="lines",
                            line=dict(width=line_width + 2, color=arrow_color),
                            showlegend=False,
                            hoverinfo="skip",
                            fill="toself",
                            fillcolor=arrow_color,
                        )
                    )

                # Highlight selected nodes
                selected_map = map_df[map_df["is_selected"]]
                fig_map.add_trace(
                    go.Scattermapbox(
                        lon=selected_map["x"],
                        lat=selected_map["y"],
                        mode="markers+text",
                        marker=dict(size=18, color="red", symbol="circle"),
                        text=selected_map["reach_id"].astype(str),
                        textposition="top center",
                        name="Selected Nodes",
                    )
                )

                # Planet satellite basemap
                planet_tile_url = f"https://tiles.planet.com/basemaps/v1/planet-tiles/global_monthly_2024_09_mosaic/gmap/{{z}}/{{x}}/{{y}}.png?api_key={PLANET_API_KEY}"

                fig_map.update_layout(
                    mapbox=dict(
                        style="white-bg",
                        center=dict(lat=map_df["y"].mean(), lon=map_df["x"].mean()),
                        zoom=12,
                        layers=[
                            {
                                "sourcetype": "raster",
                                "source": [planet_tile_url],
                                "below": "traces",
                            }
                        ],
                    ),
                    height=450,
                    margin=dict(l=0, r=0, t=0, b=0),
                    showlegend=True,
                    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
                )

                st.plotly_chart(fig_map, width="stretch", config={"scrollZoom": True})

            # Get edge_row for SWOT suggestion display
            edge_row = filtered[
                (filtered["upstream"] == upstream_id)
                & (filtered["downstream"] == downstream_id)
            ].iloc[0]

            # Show what SWOT suggests
            swot_suggestion = (
                "REVERSE (flip)" if not edge_row["swot_agrees"] else "KEEP current"
            )
            swot_color = "🟠" if not edge_row["swot_agrees"] else "🟢"
            st.write(f"{swot_color} **SWOT suggests:** {swot_suggestion}")

            current_matches_swot = (
                st.session_state.flip_direction and not edge_row["swot_agrees"]
            ) or (not st.session_state.flip_direction and edge_row["swot_agrees"])
            if current_matches_swot:
                st.success("✅ Current view matches SWOT suggestion")
            else:
                st.warning("⚠️ Current view does NOT match SWOT suggestion")

            # Decision buttons
            st.write("---")
            btn_col1, btn_col2, btn_col3 = st.columns(3)

            # Initialize decisions storage
            if "decisions" not in st.session_state:
                st.session_state.decisions = {}

            edge_key = f"{upstream_id}_{downstream_id}"
            current_decision = st.session_state.decisions.get(edge_key)

            with btn_col1:
                if st.button(
                    "✅ Keep Current",
                    type="primary"
                    if not st.session_state.flip_direction
                    else "secondary",
                ):
                    st.session_state.decisions[edge_key] = {
                        "upstream": upstream_id,
                        "downstream": downstream_id,
                        "action": "keep",
                        "flip": False,
                    }
                    st.success("Marked: KEEP current direction")
                    st.rerun()

            with btn_col2:
                if st.button(
                    "🔄 Flip Direction",
                    type="primary" if st.session_state.flip_direction else "secondary",
                ):
                    st.session_state.decisions[edge_key] = {
                        "upstream": downstream_id,  # Swapped
                        "downstream": upstream_id,  # Swapped
                        "action": "flip",
                        "flip": True,
                    }
                    st.success("Marked: FLIP direction")
                    st.rerun()

            with btn_col3:
                if st.button("⏭️ Skip"):
                    st.session_state.decisions[edge_key] = {"action": "skip"}
                    st.info("Skipped")
                    st.rerun()

            # Show current decision
            if current_decision:
                action = current_decision.get("action", "none")
                if action == "keep":
                    st.info("📌 Decision: KEEP current direction")
                elif action == "flip":
                    st.info("📌 Decision: FLIP direction")
                elif action == "skip":
                    st.info("📌 Decision: SKIPPED")

        # Edge details
        st.write("**Edge Details**")

        detail_col1, detail_col2 = st.columns(2)
        with detail_col1:
            st.write(f"**Upstream reach:** {upstream_id}")
            st.write(
                f"  - SWOT WSE: {edge_row['upstream_wse']:.2f} m (±{edge_row['upstream_std']:.2f})"
            )
            st.write(f"  - facc: {edge_row['upstream_facc']:,.0f}")
            st.write(f"  - SWOT obs: {edge_row['upstream_count']:.0f}")

        with detail_col2:
            st.write(f"**Downstream reach:** {downstream_id}")
            st.write(
                f"  - SWOT WSE: {edge_row['downstream_wse']:.2f} m (±{edge_row['downstream_std']:.2f})"
            )
            st.write(f"  - facc: {edge_row['downstream_facc']:,.0f}")
            st.write(f"  - SWOT obs: {edge_row['downstream_count']:.0f}")

        st.write(f"**WSE Difference:** {edge_row['wse_diff']:.2f} m")
        st.write(
            f"**SWOT says:** {'Current direction correct' if edge_row['swot_agrees'] else '⚠️ Direction should be REVERSED'}"
        )
        st.write(
            f"**facc says:** {'Current direction correct' if edge_row['facc_consistent'] else '⚠️ facc also suggests reversal' if edge_row['facc_consistent'] == False else 'Cannot determine'}"
        )

else:
    st.info("No disagreements found with current filters")

# Raw SWOT time series (optional)
st.subheader("Raw SWOT Time Series (Optional)")
show_timeseries = st.checkbox("Load raw SWOT time series for selected reaches")

if show_timeseries and "upstream_id" in dir() and "downstream_id" in dir():
    with st.spinner("Loading SWOT time series..."):
        swot_raw = load_swot_data(swot_path, region)

        # Filter to selected reaches
        selected_reaches = [upstream_id, downstream_id]
        ts_data = swot_raw[swot_raw["reach_id"].astype(int).isin(selected_reaches)]

        if len(ts_data) > 0:
            fig_ts = px.scatter(
                ts_data,
                x="time_str",
                y="wse",
                color=ts_data["reach_id"].astype(str),
                title="SWOT WSE Time Series",
            )
            st.plotly_chart(fig_ts, width="stretch")
        else:
            st.warning("No time series data found for selected reaches")
