#!/usr/bin/env python3
"""
SWORD Topology & FACC Reviewer
==============================
Streamlit UI to review and fix topology/facc issues in SWORD database.

Run with: streamlit run deploy/reviewer/app.py

Issue Types:
- Topology Ratio: upstream facc >> downstream facc
- Monotonicity: facc > downstream facc (should decrease)
- Headwaters: n_rch_up=0 but high facc
- Suspect: Already flagged as facc_quality='suspect'
- Fix History: View and export all logged fixes
- Lake Sandwich: River reaches between lake reaches (C001)
"""

import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
import folium
from folium.plugins import AntPath
from streamlit_folium import st_folium
import re
from datetime import datetime
import json
import os
from pathlib import Path
import requests

# Import lint checks
from lint.checks.classification import (
    check_lake_sandwich,
)
from lint.checks.topology import check_orphan_reaches
from lint.checks.attributes import check_slope_reasonableness

# =============================================================================
# LOCAL PERSISTENCE (JSON file backup for session fixes)
# =============================================================================
FIXES_DIR = Path(os.environ.get("FIXES_DIR", "output/lint_fixes"))
FIXES_DIR.mkdir(parents=True, exist_ok=True)


@st.cache_data(ttl=3600)
def get_planet_quarterly_mosaics(api_key: str):
    """Query Planet API for latest 4 quarterly visual basemap mosaics."""
    if not api_key:
        return []
    try:
        resp = requests.get(
            "https://api.planet.com/basemaps/v1/mosaics",
            params={"name__contains": "global_quarterly", "_page_size": 20},
            auth=(api_key, ""),
            timeout=10,
        )
    except requests.RequestException:
        return []
    if resp.status_code != 200:
        return []
    mosaics = resp.json().get("mosaics", [])
    quarterly = sorted(
        [
            m
            for m in mosaics
            if "quarterly" in m["name"] and "visual" in m["name"].lower()
        ],
        key=lambda m: m["name"],
        reverse=True,
    )[:4]
    return [m["name"] for m in quarterly]


def _add_planet_tile_layer(m):
    """Add Planet quarterly basemap tile layer to a folium map if configured."""
    planet_mosaic = st.session_state.get("planet_mosaic")
    planet_key = os.environ.get("PLANET_API_KEY")
    if planet_mosaic and planet_key:
        folium.TileLayer(
            tiles=(
                f"https://tiles.planet.com/basemaps/v1/planet-tiles/"
                f"{planet_mosaic}/gmap/{{z}}/{{x}}/{{y}}.png?api_key={planet_key}"
            ),
            attr="Planet Labs",
            name=f"Planet ({planet_mosaic})",
            overlay=False,
            control=True,
            show=False,
            max_zoom=18,
        ).add_to(m)


def get_session_file(region: str, check_id: str = "all") -> Path:
    """Get the session fixes file path for a region and check type."""
    return FIXES_DIR / f"lint_session_{region}_{check_id}.json"


def load_session_fixes(region: str, check_id: str = "all") -> dict:
    """Load fixes from local JSON file."""
    session_file = get_session_file(region, check_id)
    if session_file.exists():
        with open(session_file, "r") as f:
            return json.load(f)
    return {"fixes": [], "skips": [], "pending": []}


def save_session_fixes(
    region: str, fixes: list, skips: list, pending: list, check_id: str = "all"
):
    """Save fixes to local JSON file."""
    session_file = get_session_file(region, check_id)
    data = {
        "region": region,
        "check_id": check_id,
        "last_updated": datetime.now().isoformat(),
        "fixes": fixes,
        "skips": skips,
        "pending": pending,
    }
    with open(session_file, "w") as f:
        json.dump(data, f, indent=2, default=str)


def append_fix_to_session(region: str, fix_record: dict, check_id: str = "all"):
    """Append a single fix to the session file."""
    session = load_session_fixes(region, check_id)
    session["fixes"].append(fix_record)
    save_session_fixes(
        region, session["fixes"], session["skips"], session.get("pending", []), check_id
    )


def append_skip_to_session(region: str, skip_record: dict, check_id: str = "all"):
    """Append a single skip to the session file."""
    session = load_session_fixes(region, check_id)
    session["skips"].append(skip_record)
    save_session_fixes(
        region, session["fixes"], session["skips"], session.get("pending", []), check_id
    )


def export_session_csv(region: str, check_id: str = "all") -> str:
    """Export session fixes to CSV format."""
    session = load_session_fixes(region, check_id)
    all_records = []
    for fix in session["fixes"]:
        fix["action"] = "fix"
        all_records.append(fix)
    for skip in session["skips"]:
        skip["action"] = "skip"
        all_records.append(skip)
    if all_records:
        df = pd.DataFrame(all_records)
        return df.to_csv(index=False)
    return ""


# Page config
st.set_page_config(page_title="SWORD Reviewer", page_icon="🌊", layout="wide")


# Database connection
@st.cache_resource
def get_connection():
    conn = duckdb.connect(
        os.environ.get("SWORD_DB_PATH", "data/duckdb/sword_v17c.duckdb")
    )
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except:
        pass
    # Ensure fix log table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS facc_fix_log (
            fix_id INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reach_id BIGINT,
            region VARCHAR,
            fix_type VARCHAR,
            old_facc DOUBLE,
            new_facc DOUBLE,
            old_edit_flag VARCHAR,
            new_edit_flag VARCHAR,
            notes VARCHAR,
            source VARCHAR DEFAULT 'manual'
        )
    """)
    # Ensure lint fix log table exists (for C001 lake sandwich fixes)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lint_fix_log (
            fix_id INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            check_id VARCHAR,
            reach_id BIGINT,
            region VARCHAR,
            action VARCHAR,
            column_changed VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            notes VARCHAR,
            undone BOOLEAN DEFAULT FALSE
        )
    """)
    return conn


conn = get_connection()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def log_fix(conn, reach_id, region, fix_type, old_facc, new_facc, notes=""):
    """Log a fix to the facc_fix_log table."""
    # Get current edit_flag
    old_flag = conn.execute(
        "SELECT edit_flag FROM reaches WHERE reach_id = ? AND region = ?",
        [reach_id, region],
    ).fetchone()
    old_flag = old_flag[0] if old_flag else None

    # Get next fix_id
    max_id = conn.execute(
        "SELECT COALESCE(MAX(fix_id), 0) FROM facc_fix_log"
    ).fetchone()[0]
    new_id = max_id + 1

    conn.execute(
        """
        INSERT INTO facc_fix_log (fix_id, reach_id, region, fix_type, old_facc, new_facc, old_edit_flag, notes, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual')
    """,
        [new_id, reach_id, region, fix_type, old_facc, new_facc, old_flag, notes],
    )


def apply_facc_fix(conn, reach_id, region, new_facc, fix_type, notes=""):
    """Apply a facc fix to both reaches and nodes tables, with logging."""
    # Get old facc
    old_facc = conn.execute(
        "SELECT facc FROM reaches WHERE reach_id = ? AND region = ?", [reach_id, region]
    ).fetchone()
    old_facc = old_facc[0] if old_facc else 0

    # Log the fix
    log_fix(conn, reach_id, region, fix_type, old_facc, new_facc, notes)

    # Update reaches table
    conn.execute(
        """
        UPDATE reaches SET facc = ?, facc_quality = 'manual_fix',
        edit_flag = CASE
            WHEN edit_flag IS NULL OR edit_flag = 'NaN' THEN ?
            ELSE edit_flag || ',' || ?
        END
        WHERE reach_id = ? AND region = ?
    """,
        [new_facc, fix_type, fix_type, reach_id, region],
    )

    # Note: nodes table not present in slim reviewer DB — skip node update

    # COMMIT to ensure persistence
    conn.commit()

    return old_facc


@st.cache_data(ttl=300)
def get_reach_geometry(_conn, reach_id):
    """Get reach centroid as a single point (slim DB has no nodes table)."""
    row = _conn.execute(
        """
        SELECT x, y FROM reaches WHERE reach_id = ?
    """,
        [reach_id],
    ).fetchone()
    if row is None:
        return None
    return [[row[0], row[1]]]


@st.cache_data(ttl=300)
def get_reach_info(_conn, reach_id, region):
    """Get detailed reach info."""
    return _conn.execute(
        """
        SELECT reach_id, facc, width, river_name, lakeflag, n_rch_up, n_rch_down,
               facc_quality, edit_flag, x, y
        FROM reaches WHERE reach_id = ? AND region = ?
    """,
        [reach_id, region],
    ).fetchdf()


@st.cache_data(ttl=60)
def get_upstream_chain(_conn, reach_id, region, max_hops=5):
    """Get upstream reaches for context."""
    chain = []
    current_id = reach_id
    for hop in range(max_hops):
        info = _conn.execute(
            """
            SELECT r.reach_id, r.facc, r.width, r.river_name,
                   t.neighbor_reach_id as up_id
            FROM reaches r
            LEFT JOIN reach_topology t ON r.reach_id = t.reach_id AND r.region = t.region AND t.direction = 'up'
            WHERE r.reach_id = ? AND r.region = ?
            ORDER BY t.neighbor_rank LIMIT 1
        """,
            [current_id, region],
        ).fetchone()
        if not info:
            break
        chain.append(
            {
                "hop": hop,
                "reach_id": info[0],
                "facc": info[1],
                "width": info[2],
                "river_name": info[3],
            }
        )
        if info[4] is None or info[4] <= 0:
            break
        current_id = int(info[4])
    return pd.DataFrame(chain)


@st.cache_data(ttl=60)
def get_downstream_chain(_conn, reach_id, region, max_hops=5):
    """Get downstream reaches for context."""
    chain = []
    current_id = reach_id
    for hop in range(max_hops):
        info = _conn.execute(
            """
            SELECT r.reach_id, r.facc, r.width, r.river_name,
                   t.neighbor_reach_id as dn_id
            FROM reaches r
            LEFT JOIN reach_topology t ON r.reach_id = t.reach_id AND r.region = t.region AND t.direction = 'down'
            WHERE r.reach_id = ? AND r.region = ?
            ORDER BY t.neighbor_rank LIMIT 1
        """,
            [current_id, region],
        ).fetchone()
        if not info:
            break
        chain.append(
            {
                "hop": hop,
                "reach_id": info[0],
                "facc": info[1],
                "width": info[2],
                "river_name": info[3],
            }
        )
        if info[4] is None or info[4] <= 0:
            break
        current_id = int(info[4])
    return pd.DataFrame(chain)


# =============================================================================
# LAKE SANDWICH (C001) HELPER FUNCTIONS
# =============================================================================


def get_neighbors(conn, reach_ids, region, hops=2):
    """Get upstream + downstream neighbor IDs (configurable hops deep)."""
    if not reach_ids:
        return set()

    all_neighbors = set()
    current_ids = set(reach_ids)

    for _ in range(hops):
        if not current_ids:
            break
        placeholders = ",".join(["?" for _ in current_ids])
        result = conn.execute(
            f"""
            SELECT DISTINCT neighbor_reach_id
            FROM reach_topology
            WHERE reach_id IN ({placeholders}) AND region = ?
        """,
            list(current_ids) + [region],
        ).fetchall()

        new_ids = {row[0] for row in result if row[0]}
        all_neighbors.update(new_ids)
        current_ids = new_ids - set(reach_ids) - all_neighbors

    return all_neighbors


def run_c001_check(conn, region, reach_ids=None):
    """Run C001 lake sandwich check on full region or specific reaches + neighbors."""
    result = check_lake_sandwich(conn, region)

    if reach_ids is not None and len(reach_ids) > 0:
        # Filter to specific reaches + their neighbors
        neighbor_ids = get_neighbors(conn, reach_ids, region, hops=2)
        all_ids = set(reach_ids) | neighbor_ids
        if len(result.details) > 0:
            result.details = result.details[result.details["reach_id"].isin(all_ids)]

    return result


def get_neighbor_lakeflags(conn, reach_id, region):
    """Get lakeflag values for upstream and downstream neighbors."""
    up_result = conn.execute(
        """
        SELECT r.lakeflag
        FROM reach_topology t
        JOIN reaches r ON t.neighbor_reach_id = r.reach_id AND t.region = r.region
        WHERE t.reach_id = ? AND t.region = ? AND t.direction = 'up'
    """,
        [reach_id, region],
    ).fetchall()

    dn_result = conn.execute(
        """
        SELECT r.lakeflag
        FROM reach_topology t
        JOIN reaches r ON t.neighbor_reach_id = r.reach_id AND t.region = r.region
        WHERE t.reach_id = ? AND t.region = ? AND t.direction = 'down'
    """,
        [reach_id, region],
    ).fetchall()

    up_flags = [row[0] for row in up_result]
    dn_flags = [row[0] for row in dn_result]

    return up_flags, dn_flags


def get_reach_slope(conn, reach_id, region):
    """Get slope for a reach if available."""
    result = conn.execute(
        """
        SELECT slope FROM reaches WHERE reach_id = ? AND region = ?
    """,
        [reach_id, region],
    ).fetchone()
    return result[0] if result and result[0] is not None else None


def get_reach_facc(conn, reach_id, region):
    """Get facc for a reach."""
    result = conn.execute(
        """
        SELECT facc FROM reaches WHERE reach_id = ? AND region = ?
    """,
        [reach_id, region],
    ).fetchone()
    return result[0] if result and result[0] is not None else 0


def add_topology_connection(conn, reach_id, neighbor_id, direction, region):
    """
    Add a topology connection between two reaches.
    direction: 'up' means neighbor is upstream of reach_id
               'down' means neighbor is downstream of reach_id
    """
    # Check if connection already exists
    existing = conn.execute(
        """
        SELECT 1 FROM reach_topology
        WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ? AND direction = ?
    """,
        [reach_id, neighbor_id, region, direction],
    ).fetchone()

    if existing:
        return False, "Connection already exists"

    # Get next rank for this reach/direction
    max_rank = conn.execute(
        """
        SELECT COALESCE(MAX(neighbor_rank), -1) FROM reach_topology
        WHERE reach_id = ? AND region = ? AND direction = ?
    """,
        [reach_id, region, direction],
    ).fetchone()[0]
    new_rank = max_rank + 1

    # Add the forward connection (reach -> neighbor)
    conn.execute(
        """
        INSERT INTO reach_topology (reach_id, neighbor_reach_id, region, direction, neighbor_rank)
        VALUES (?, ?, ?, ?, ?)
    """,
        [reach_id, neighbor_id, region, direction, new_rank],
    )

    # Add the reverse connection (neighbor -> reach)
    reverse_direction = "down" if direction == "up" else "up"
    reverse_rank = (
        conn.execute(
            """
        SELECT COALESCE(MAX(neighbor_rank), -1) FROM reach_topology
        WHERE reach_id = ? AND region = ? AND direction = ?
    """,
            [neighbor_id, region, reverse_direction],
        ).fetchone()[0]
        + 1
    )

    conn.execute(
        """
        INSERT INTO reach_topology (reach_id, neighbor_reach_id, region, direction, neighbor_rank)
        VALUES (?, ?, ?, ?, ?)
    """,
        [neighbor_id, reach_id, region, reverse_direction, reverse_rank],
    )

    # Update n_rch_up/n_rch_down counts
    if direction == "up":
        conn.execute(
            "UPDATE reaches SET n_rch_up = n_rch_up + 1 WHERE reach_id = ? AND region = ?",
            [reach_id, region],
        )
        conn.execute(
            "UPDATE reaches SET n_rch_down = n_rch_down + 1 WHERE reach_id = ? AND region = ?",
            [neighbor_id, region],
        )
    else:
        conn.execute(
            "UPDATE reaches SET n_rch_down = n_rch_down + 1 WHERE reach_id = ? AND region = ?",
            [reach_id, region],
        )
        conn.execute(
            "UPDATE reaches SET n_rch_up = n_rch_up + 1 WHERE reach_id = ? AND region = ?",
            [neighbor_id, region],
        )

    conn.commit()

    # Log the fix
    log_skip(
        conn,
        reach_id,
        region,
        "TOPO",
        f"Added {direction}stream connection to {neighbor_id}",
    )

    return True, f"Connected {reach_id} ←{direction}→ {neighbor_id}"


def undo_last_topology_fix(conn, region):
    """Undo the most recent topology connection for the region."""
    # Get most recent topology fix (look for 'Added' in notes)
    last = conn.execute(
        """
        SELECT fix_id, reach_id, notes
        FROM lint_fix_log
        WHERE region = ? AND check_id = 'TOPO' AND notes LIKE 'Added %' AND NOT undone
        ORDER BY timestamp DESC LIMIT 1
    """,
        [region],
    ).fetchone()

    if not last:
        return None, "No topology fixes to undo"

    fix_id, reach_id, notes = last

    # Parse the connection info from notes: "Added upstream connection to 12345"
    # or "Added downstream connection to 12345"
    import re

    match = re.search(r"Added (up|down)stream connection to (\d+)", notes)
    if not match:
        return None, f"Could not parse connection from: {notes}"

    direction = match.group(1)
    neighbor_id = int(match.group(2))

    # Remove the forward connection
    conn.execute(
        """
        DELETE FROM reach_topology
        WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ? AND direction = ?
    """,
        [reach_id, neighbor_id, region, direction],
    )

    # Remove the reverse connection
    reverse_direction = "down" if direction == "up" else "up"
    conn.execute(
        """
        DELETE FROM reach_topology
        WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ? AND direction = ?
    """,
        [neighbor_id, reach_id, region, reverse_direction],
    )

    # Update n_rch_up/n_rch_down counts
    if direction == "up":
        conn.execute(
            "UPDATE reaches SET n_rch_up = GREATEST(0, n_rch_up - 1) WHERE reach_id = ? AND region = ?",
            [reach_id, region],
        )
        conn.execute(
            "UPDATE reaches SET n_rch_down = GREATEST(0, n_rch_down - 1) WHERE reach_id = ? AND region = ?",
            [neighbor_id, region],
        )
    else:
        conn.execute(
            "UPDATE reaches SET n_rch_down = GREATEST(0, n_rch_down - 1) WHERE reach_id = ? AND region = ?",
            [reach_id, region],
        )
        conn.execute(
            "UPDATE reaches SET n_rch_up = GREATEST(0, n_rch_up - 1) WHERE reach_id = ? AND region = ?",
            [neighbor_id, region],
        )

    # Mark fix as undone
    conn.execute("UPDATE lint_fix_log SET undone = TRUE WHERE fix_id = ?", [fix_id])

    # Log undo action
    log_skip(
        conn,
        reach_id,
        region,
        "TOPO",
        f"Undid {direction}stream connection to {neighbor_id}",
    )

    conn.commit()
    return reach_id, f"Undid connection {reach_id} ←{direction}→ {neighbor_id}"


def apply_lakeflag_fix(conn, reach_id, region, new_lakeflag):
    """Apply a lakeflag fix to reaches table, with logging and local backup."""
    # Get old lakeflag
    old_lakeflag = conn.execute(
        "SELECT lakeflag FROM reaches WHERE reach_id = ? AND region = ?",
        [reach_id, region],
    ).fetchone()
    old_lakeflag = old_lakeflag[0] if old_lakeflag else None

    # Get next fix_id
    max_id = conn.execute(
        "SELECT COALESCE(MAX(fix_id), 0) FROM lint_fix_log"
    ).fetchone()[0]
    new_id = max_id + 1

    timestamp = datetime.now().isoformat()

    # Log the fix to database
    conn.execute(
        """
        INSERT INTO lint_fix_log (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
        VALUES (?, 'C001', ?, ?, 'fix', 'lakeflag', ?, ?, '')
    """,
        [new_id, reach_id, region, str(old_lakeflag), str(new_lakeflag)],
    )

    # Update reaches table
    conn.execute(
        """
        UPDATE reaches SET lakeflag = ?
        WHERE reach_id = ? AND region = ?
    """,
        [new_lakeflag, reach_id, region],
    )

    # COMMIT to ensure persistence
    conn.commit()

    # Also save to local JSON file as backup
    fix_record = {
        "fix_id": new_id,
        "timestamp": timestamp,
        "check_id": "C001",
        "reach_id": int(reach_id),
        "region": region,
        "column_changed": "lakeflag",
        "old_value": old_lakeflag,
        "new_value": new_lakeflag,
        "undone": False,
    }
    append_fix_to_session(region, fix_record, check_id="C001")

    return old_lakeflag


def log_skip(conn, reach_id, region, check_id, notes):
    """Log a skip action (false positive) with required explanation and local backup."""
    max_id = conn.execute(
        "SELECT COALESCE(MAX(fix_id), 0) FROM lint_fix_log"
    ).fetchone()[0]
    new_id = max_id + 1

    timestamp = datetime.now().isoformat()

    conn.execute(
        """
        INSERT INTO lint_fix_log (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
        VALUES (?, ?, ?, ?, 'skip', NULL, NULL, NULL, ?)
    """,
        [new_id, check_id, reach_id, region, notes],
    )

    # COMMIT to ensure persistence
    conn.commit()

    # Also save to local JSON file as backup
    skip_record = {
        "fix_id": new_id,
        "timestamp": timestamp,
        "check_id": check_id,
        "reach_id": int(reach_id),
        "region": region,
        "notes": notes,
        "undone": False,
    }
    append_skip_to_session(region, skip_record, check_id=check_id)


def undo_last_fix(conn, region):
    """Undo the most recent lakeflag fix for the region."""
    # Get most recent non-undone fix
    last = conn.execute(
        """
        SELECT fix_id, reach_id, old_value
        FROM lint_fix_log
        WHERE region = ? AND action = 'fix' AND NOT undone
        ORDER BY timestamp DESC LIMIT 1
    """,
        [region],
    ).fetchone()

    if not last:
        return None

    fix_id, reach_id, old_value = last

    # Restore old value
    if old_value is not None:
        conn.execute(
            """
            UPDATE reaches SET lakeflag = ?
            WHERE reach_id = ? AND region = ?
        """,
            [int(old_value), reach_id, region],
        )

    # Mark fix as undone
    conn.execute(
        """
        UPDATE lint_fix_log SET undone = TRUE WHERE fix_id = ?
    """,
        [fix_id],
    )

    # Log undo action
    max_id = conn.execute(
        "SELECT COALESCE(MAX(fix_id), 0) FROM lint_fix_log"
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO lint_fix_log (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
        VALUES (?, 'C001', ?, ?, 'undo', 'lakeflag', NULL, ?, ?)
    """,
        [max_id + 1, reach_id, region, old_value, f"Undo of fix_id={fix_id}"],
    )

    # COMMIT to ensure persistence
    conn.commit()

    # Update local JSON - mark the fix as undone
    session = load_session_fixes(region, check_id="C001")
    for fix in session["fixes"]:
        if fix.get("fix_id") == fix_id:
            fix["undone"] = True
            break
    save_session_fixes(
        region,
        session["fixes"],
        session["skips"],
        session.get("pending", []),
        check_id="C001",
    )

    return reach_id


def get_nearby_reaches(
    conn,
    center_lon,
    center_lat,
    radius_deg,
    region,
    exclude_ids=None,
    include_all=False,
    max_reaches=2000,
):
    """Get all reaches within a bounding box.

    If include_all=False, excludes reaches in exclude_ids (for finding unconnected).
    If include_all=True, returns ALL reaches in area with lakeflag for coloring.
    """
    exclude_ids = exclude_ids or []
    exclude_str = ",".join([str(int(r)) for r in exclude_ids]) if exclude_ids else "0"

    if include_all:
        # Return ALL reaches with lakeflag for type coloring and flow direction info
        query = f"""
            SELECT reach_id, x, y, lakeflag, facc, width, n_rch_up, n_rch_down
            FROM reaches
            WHERE region = ?
              AND x BETWEEN ? AND ?
              AND y BETWEEN ? AND ?
            LIMIT {max_reaches}
        """
        return conn.execute(
            query,
            [
                region,
                center_lon - radius_deg,
                center_lon + radius_deg,
                center_lat - radius_deg,
                center_lat + radius_deg,
            ],
        ).fetchdf()
    else:
        # Exclude connected reaches (for finding unconnected only)
        query = f"""
            SELECT reach_id, x, y, lakeflag, facc, width, n_rch_up, n_rch_down
            FROM reaches
            WHERE region = ?
              AND x BETWEEN ? AND ?
              AND y BETWEEN ? AND ?
              AND reach_id NOT IN ({exclude_str})
            LIMIT {max_reaches}
        """
        return conn.execute(
            query,
            [
                region,
                center_lon - radius_deg,
                center_lon + radius_deg,
                center_lat - radius_deg,
                center_lat + radius_deg,
            ],
        ).fetchdf()


def add_flow_line(m, coords, color, weight=3, opacity=0.9, tooltip=None, animate=True):
    """Draw a reach line with optional animated flow direction via AntPath."""
    if len(coords) < 2:
        return
    if animate:
        AntPath(
            coords,
            color=color,
            weight=weight,
            opacity=opacity,
            tooltip=tooltip,
            delay=800,
            dash_array=[10, 20],
            pulse_color="#000000",
        ).add_to(m)
    else:
        folium.PolyLine(
            coords, color=color, weight=weight, opacity=opacity, tooltip=tooltip
        ).add_to(m)


def render_reach_map_satellite(reach_id, region, conn, hops=None, color_by_type=False):
    """Render a map centered on a reach with Esri satellite basemap using folium."""
    geom = get_reach_geometry(conn, reach_id)
    if not geom:
        st.warning("No geometry available")
        return

    # Use sidebar settings
    if hops is None:
        hops = st.session_state.get("map_hops", 25)
    show_all = st.session_state.get("show_all_reaches", True)

    # Get upstream and downstream geometries
    up_chain = get_upstream_chain(conn, reach_id, region, hops)
    dn_chain = get_downstream_chain(conn, reach_id, region, hops)

    all_coords = list(geom)
    connected_ids = {reach_id}

    # Collect upstream geometries
    up_geoms = []
    for i, row in up_chain.iterrows():
        if row["reach_id"] == reach_id:
            continue
        connected_ids.add(row["reach_id"])
        up_geom = get_reach_geometry(conn, int(row["reach_id"]))
        if up_geom:
            up_geoms.append((up_geom, i, row["reach_id"]))
            all_coords.extend(up_geom)

    # Collect downstream geometries
    dn_geoms = []
    for i, row in dn_chain.iterrows():
        if row["reach_id"] == reach_id:
            continue
        connected_ids.add(row["reach_id"])
        dn_geom = get_reach_geometry(conn, int(row["reach_id"]))
        if dn_geom:
            dn_geoms.append((dn_geom, i, row["reach_id"]))
            all_coords.extend(dn_geom)

    # Calculate bounds and center
    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    center_lat = (min(lats) + max(lats)) / 2
    center_lon = (min(lons) + max(lons)) / 2
    extent = max(max(lons) - min(lons), max(lats) - min(lats))

    # Expand extent to show more area
    view_radius = max(extent * 0.75, 0.02)  # At least 0.02 degrees

    zoom = 15 if extent < 0.02 else 14 if extent < 0.05 else 12 if extent < 0.1 else 10

    # Create folium map with Esri satellite tiles
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles=None,  # Start with no tiles
    )

    # Add Esri World Imagery (free satellite tiles) - default & shown
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite",
        overlay=False,
        control=True,
        show=True,
    ).add_to(m)

    # Add CartoDB Dark for contrast option
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        attr="CartoDB",
        name="Dark",
        overlay=False,
        control=True,
        show=False,
    ).add_to(m)

    # Add Planet quarterly basemap if configured
    _add_planet_tile_layer(m)

    # Add layer control to toggle between basemaps
    folium.LayerControl().add_to(m)

    # Get and display ALL nearby reaches with colors by type
    nearby_unconnected = []
    nearby_all_count = 0
    if show_all:
        # Get ALL reaches in area with type info - use sidebar settings
        search_radius = st.session_state.get("map_radius", 3.0)
        max_reaches = st.session_state.get("max_reaches", 10000)
        nearby = get_nearby_reaches(
            conn,
            center_lon,
            center_lat,
            search_radius,
            region,
            list(connected_ids),
            include_all=False,
            max_reaches=max_reaches,
        )
        nearby_all_count = len(nearby)

        # Color map by lakeflag: 0=river(white), 1=lake(cyan), 2=canal(yellow), 3=tidal(magenta)
        type_colors = {0: "#ffffff", 1: "#00ffff", 2: "#ffff00", 3: "#ff00ff"}
        type_names = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}

        # Highlight either manually previewed or clicked reach
        highlight_id = st.session_state.get("highlight_reach") or st.session_state.get(
            "clicked_reach"
        )

        for _, row in nearby.iterrows():
            nearby_geom = get_reach_geometry(conn, int(row["reach_id"]))
            if nearby_geom:
                rid = row["reach_id"]
                lakeflag = int(row["lakeflag"]) if pd.notna(row["lakeflag"]) else 0
                facc = row["facc"] if pd.notna(row["facc"]) else 0
                width = row["width"] if pd.notna(row["width"]) else 0
                nearby_unconnected.append((nearby_geom, rid, lakeflag))

                coords = [[c[1], c[0]] for c in nearby_geom]
                type_name = type_names.get(lakeflag, "?")
                color = type_colors.get(lakeflag, "#ffffff")

                # Highlight selected reach in bright green
                if highlight_id and int(rid) == int(highlight_id):
                    add_flow_line(
                        m,
                        coords,
                        "#00ff00",
                        weight=6,
                        opacity=1.0,
                        tooltip=f"SELECTED: {rid} ({type_name}, facc={facc:,.0f}, w={width:.0f}m)",
                    )
                else:
                    add_flow_line(
                        m,
                        coords,
                        color,
                        weight=3,
                        opacity=0.9,
                        tooltip=f"{type_name}: {rid} (facc={facc:,.0f}, w={width:.0f}m)",
                        animate=False,
                    )

    # Batch lookup lakeflags for connected reaches when coloring by type
    connected_lakeflags = {}
    if color_by_type:
        all_connected = [rid for _, _, rid in up_geoms] + [
            rid for _, _, rid in dn_geoms
        ]
        if all_connected:
            placeholders = ",".join(["?"] * len(all_connected))
            lf_rows = conn.execute(
                f"SELECT reach_id, lakeflag FROM reaches WHERE reach_id IN ({placeholders}) AND region = ?",
                all_connected + [region],
            ).fetchall()
            connected_lakeflags = {
                r[0]: int(r[1]) if r[1] is not None else 0 for r in lf_rows
            }

    type_colors_conn = {0: "#ffffff", 1: "#00ffff", 2: "#ffff00", 3: "#ff00ff"}
    type_names_conn = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}

    # Add upstream reaches
    for up_geom, i, rid in up_geoms:
        coords = [[c[1], c[0]] for c in up_geom]
        opacity = max(0.4, 1.0 - (i / max(hops, 1)) * 0.6)
        if color_by_type:
            lf = connected_lakeflags.get(rid, 0)
            color = type_colors_conn.get(lf, "#ffffff")
            tn = type_names_conn.get(lf, "?")
            tooltip = f"Up {i + 1}: {rid} ({tn})"
        else:
            color = "orange"
            tooltip = f"Upstream {i + 1}: {rid}"
        add_flow_line(m, coords, color, weight=4, opacity=opacity, tooltip=tooltip)

    # Add downstream reaches
    for dn_geom, i, rid in dn_geoms:
        coords = [[c[1], c[0]] for c in dn_geom]
        opacity = max(0.4, 1.0 - (i / max(hops, 1)) * 0.6)
        if color_by_type:
            lf = connected_lakeflags.get(rid, 0)
            color = type_colors_conn.get(lf, "#ffffff")
            tn = type_names_conn.get(lf, "?")
            tooltip = f"Down {i + 1}: {rid} ({tn})"
        else:
            color = "#0066ff"
            tooltip = f"Downstream {i + 1}: {rid}"
        add_flow_line(m, coords, color, weight=4, opacity=opacity, tooltip=tooltip)

    # Add main reach - BRIGHT YELLOW with black outline for visibility
    main_coords = [[c[1], c[0]] for c in geom]
    # Black outline first
    folium.PolyLine(main_coords, color="black", weight=12, opacity=1.0).add_to(m)
    # Bright yellow on top
    add_flow_line(
        m,
        main_coords,
        "#FFFF00",
        weight=8,
        opacity=1.0,
        tooltip=f"SELECTED: {reach_id}",
    )

    # Add pulsing circle marker at center of selected reach
    if len(main_coords) > 0:
        center_idx = len(main_coords) // 2
        center = main_coords[center_idx]
        folium.CircleMarker(
            center,
            radius=15,
            color="black",
            fill=True,
            fill_color="#FFFF00",
            fill_opacity=1.0,
            weight=3,
            tooltip=f"★ SELECTED: {reach_id}",
        ).add_to(m)

    # Fit bounds (expand slightly to show context)
    padding = view_radius * 0.2
    m.fit_bounds(
        [
            [min(lats) - padding, min(lons) - padding],
            [max(lats) + padding, max(lons) + padding],
        ]
    )

    # Save network IDs to session state for topology fixing
    st.session_state.last_network_ids = list(connected_ids)

    # Store nearby reach IDs for click detection
    st.session_state.nearby_reach_ids = (
        [int(rid) for _, rid, _ in nearby_unconnected] if show_all else []
    )

    # Render in streamlit and capture clicks
    map_data = st_folium(
        m, width=None, height=500, returned_objects=["last_object_clicked_tooltip"]
    )

    # Check if user clicked on a reach - extract ID from tooltip
    if map_data and map_data.get("last_object_clicked_tooltip"):
        tooltip = map_data["last_object_clicked_tooltip"]
        # Tooltips look like "River: 12345 (facc=...)" or "Lake: 12345 ..."
        import re

        match = re.search(
            r"(?:River|Lake|Canal|Tidal|Unconnected|SELECTED):\s*(\d+)", tooltip
        )
        if match:
            clicked_id = int(match.group(1))
            # Only set if it's a nearby (unconnected) reach, not the selected or network reaches
            if clicked_id in st.session_state.get("nearby_reach_ids", []):
                st.session_state.clicked_reach = clicked_id

    parts = [
        f"Yellow=Selected | Orange=Up ({len(up_geoms)}) | Blue=Down ({len(dn_geoms)})"
    ]
    if show_all and nearby_unconnected:
        type_counts = {}
        for _, rid, lf in nearby_unconnected:
            type_counts[lf] = type_counts.get(lf, 0) + 1
        type_labels = {
            0: "White=River",
            1: "Cyan=Lake",
            2: "Yellow=Canal",
            3: "Magenta=Tidal",
        }
        for lf_val in (0, 1, 2, 3):
            cnt = type_counts.get(lf_val, 0)
            if cnt > 0:
                parts.append(f"{type_labels[lf_val]} ({cnt})")
        highlight_id = st.session_state.get("highlight_reach")
        if highlight_id:
            parts.append("Green=To Connect")
    st.caption(" | ".join(parts))


def get_lint_fix_history(conn, region=None, limit=100):
    """Get lint fix history from log table."""
    if region:
        return conn.execute(
            """
            SELECT * FROM lint_fix_log WHERE region = ? ORDER BY timestamp DESC LIMIT ?
        """,
            [region, limit],
        ).fetchdf()
    else:
        return conn.execute(
            """
            SELECT * FROM lint_fix_log ORDER BY timestamp DESC LIMIT ?
        """,
            [limit],
        ).fetchdf()


# =============================================================================
# ISSUE QUERIES
# =============================================================================


@st.cache_data(ttl=60)
def get_monotonicity_issues(_conn, region, limit=500):
    """Get reaches where facc > downstream facc."""
    return _conn.execute(
        """
        SELECT r.reach_id, r.facc, r.width, r.river_name, r.x, r.y, r.facc_quality,
               rd.reach_id as dn_reach_id, rd.facc as dn_facc, rd.width as dn_width,
               r.facc - rd.facc as diff
        FROM reaches r
        JOIN reach_topology t ON r.reach_id = t.reach_id AND r.region = t.region
        JOIN reaches rd ON t.neighbor_reach_id = rd.reach_id AND rd.region = ?
        WHERE r.region = ? AND t.direction = 'down'
        AND r.facc > rd.facc * 1.01
        AND (r.facc_quality IS NULL OR r.facc_quality NOT IN ('traced', 'manual_fix'))
        ORDER BY r.facc - rd.facc DESC
        LIMIT ?
    """,
        [region, region, limit],
    ).fetchdf()


@st.cache_data(ttl=60)
def get_headwater_issues(_conn, region, min_facc=1000, limit=500):
    """Get headwaters with high facc."""
    return _conn.execute(
        """
        SELECT reach_id, facc, width, river_name, x, y, lakeflag, facc_quality,
               facc / NULLIF(width, 0) as ratio
        FROM reaches
        WHERE region = ? AND n_rch_up = 0 AND facc > ?
        AND (facc_quality IS NULL OR facc_quality NOT IN ('traced', 'manual_fix'))
        ORDER BY facc DESC
        LIMIT ?
    """,
        [region, min_facc, limit],
    ).fetchdf()


@st.cache_data(ttl=60)
def get_suspect_reaches(_conn, region, limit=500):
    """Get reaches flagged as suspect."""
    return _conn.execute(
        """
        SELECT reach_id, facc, width, river_name, x, y, facc_quality, edit_flag,
               facc / NULLIF(width, 0) as ratio
        FROM reaches
        WHERE region = ? AND facc_quality = 'suspect'
        ORDER BY facc DESC
        LIMIT ?
    """,
        [region, limit],
    ).fetchdf()


@st.cache_data(ttl=60)
def get_ratio_violations(_conn, region, min_ratio=10, limit=500):
    """Get topology ratio violations (original query)."""
    return _conn.execute(
        """
        SELECT
            r1.reach_id as upstream_reach, r1.facc as upstream_facc, r1.width as up_width,
            r1.river_name as upstream_name, r1.x as up_x, r1.y as up_y,
            r2.reach_id as downstream_reach, r2.facc as downstream_facc, r2.width as dn_width,
            r2.river_name as downstream_name, r2.x as dn_x, r2.y as dn_y,
            r1.facc / NULLIF(r2.facc, 0) as ratio,
            t.topology_suspect as is_flagged,
            COALESCE(t.topology_approved, FALSE) as is_approved
        FROM reach_topology t
        JOIN reaches r1 ON t.reach_id = r1.reach_id AND t.region = r1.region
        JOIN reaches r2 ON t.neighbor_reach_id = r2.reach_id AND t.region = r2.region
        WHERE t.direction = 'down' AND t.region = ?
        AND r2.facc > 0 AND r1.facc / r2.facc >= ?
        AND (t.topology_approved = FALSE OR t.topology_approved IS NULL)
        ORDER BY r1.facc / r2.facc DESC
        LIMIT ?
    """,
        [region, min_ratio, limit],
    ).fetchdf()


def get_fix_history(_conn, region=None, limit=100):
    """Get fix history from log table."""
    if region:
        return _conn.execute(
            """
            SELECT * FROM facc_fix_log WHERE region = ? ORDER BY timestamp DESC LIMIT ?
        """,
            [region, limit],
        ).fetchdf()
    else:
        return _conn.execute(
            """
            SELECT * FROM facc_fix_log ORDER BY timestamp DESC LIMIT ?
        """,
            [limit],
        ).fetchdf()


# =============================================================================
# UI COMPONENTS
# =============================================================================


def render_reach_map(reach_id, region, title="Reach Location"):
    """Render a map centered on a reach with upstream/downstream context."""
    geom = get_reach_geometry(conn, reach_id)
    if not geom:
        st.warning("No geometry available")
        return

    # Get upstream and downstream geometries
    up_chain = get_upstream_chain(conn, reach_id, region, 3)
    dn_chain = get_downstream_chain(conn, reach_id, region, 3)

    layers = []
    all_coords = list(geom)

    # Main reach (RED)
    layers.append(
        pdk.Layer(
            "PathLayer",
            data=[{"path": geom, "color": [255, 0, 0]}],
            get_path="path",
            get_color="color",
            width_scale=30,
            width_min_pixels=5,
        )
    )

    # Upstream context (ORANGE gradient)
    for i, row in up_chain.iterrows():
        if row["reach_id"] == reach_id:
            continue
        up_geom = get_reach_geometry(conn, int(row["reach_id"]))
        if up_geom:
            alpha = 255 - (i * 50)
            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=[
                        {
                            "path": up_geom,
                            "color": [255, 165, 0, alpha],
                            "name": f"Up {i}: facc={row['facc']:,.0f}",
                        }
                    ],
                    get_path="path",
                    get_color="color",
                    width_scale=20,
                    width_min_pixels=3,
                    pickable=True,
                )
            )
            all_coords.extend(up_geom)

    # Downstream context (BLUE gradient)
    for i, row in dn_chain.iterrows():
        if row["reach_id"] == reach_id:
            continue
        dn_geom = get_reach_geometry(conn, int(row["reach_id"]))
        if dn_geom:
            alpha = 255 - (i * 50)
            layers.append(
                pdk.Layer(
                    "PathLayer",
                    data=[
                        {
                            "path": dn_geom,
                            "color": [0, 100, 255, alpha],
                            "name": f"Dn {i}: facc={row['facc']:,.0f}",
                        }
                    ],
                    get_path="path",
                    get_color="color",
                    width_scale=20,
                    width_min_pixels=3,
                    pickable=True,
                )
            )
            all_coords.extend(dn_geom)

    # Calculate view
    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    center_lon = (min(lons) + max(lons)) / 2
    center_lat = (min(lats) + max(lats)) / 2
    extent = max(max(lons) - min(lons), max(lats) - min(lats))
    zoom = 12 if extent < 0.1 else 10 if extent < 0.5 else 8 if extent < 1 else 7

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=center_lat, longitude=center_lon, zoom=zoom
            ),
            tooltip={"text": "{name}"},
        )
    )
    st.caption("🔴 Selected | 🟠 Upstream | 🔵 Downstream")


def render_fix_panel(reach_id, region, current_facc, issue_type):
    """Render fix options for a reach."""
    st.markdown("### 🔧 Fix Options")

    # Get context
    up_chain = get_upstream_chain(conn, reach_id, region, 5)
    dn_chain = get_downstream_chain(conn, reach_id, region, 3)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Upstream chain:**")
        if len(up_chain) > 0:
            st.dataframe(up_chain[["hop", "reach_id", "facc", "width"]], height=150)
        else:
            st.info("No upstream")

    with col2:
        st.markdown("**Downstream chain:**")
        if len(dn_chain) > 0:
            st.dataframe(dn_chain[["hop", "reach_id", "facc", "width"]], height=150)
        else:
            st.info("No downstream")

    st.divider()

    # Use issue_type in keys to avoid conflicts across tabs
    key_prefix = f"{issue_type}_{reach_id}"

    # Fix options
    notes = st.text_input("Notes (optional)", key=f"notes_{key_prefix}")

    # Option 1: Set to upstream value
    if len(up_chain) > 1:
        up_facc = up_chain.iloc[1]["facc"] if len(up_chain) > 1 else 0
        if up_facc > 0 and up_facc < current_facc * 0.5:
            st.markdown(f"**Option 1:** Use upstream facc = **{up_facc:,.0f}**")
            if st.button(f"Set to {up_facc:,.0f}", key=f"fix_up_{key_prefix}"):
                old = apply_facc_fix(conn, reach_id, region, up_facc, issue_type, notes)
                st.success(f"Fixed! {old:,.0f} → {up_facc:,.0f}")
                st.cache_data.clear()
                st.rerun()

    # Option 2: Set to downstream value
    if len(dn_chain) > 1:
        dn_facc = dn_chain.iloc[1]["facc"] if len(dn_chain) > 1 else 0
        if dn_facc > 0:
            st.markdown(f"**Option 2:** Match downstream facc = **{dn_facc:,.0f}**")
            if st.button(f"Set to {dn_facc:,.0f}", key=f"fix_dn_{key_prefix}"):
                old = apply_facc_fix(conn, reach_id, region, dn_facc, issue_type, notes)
                st.success(f"Fixed! {old:,.0f} → {dn_facc:,.0f}")
                st.cache_data.clear()
                st.rerun()

    # Option 3: Custom value
    custom_facc = st.number_input(
        "Custom facc value",
        min_value=0.0,
        value=float(current_facc),
        key=f"custom_{key_prefix}",
    )
    if st.button("Apply custom value", key=f"fix_custom_{key_prefix}"):
        old = apply_facc_fix(
            conn, reach_id, region, custom_facc, f"{issue_type}_custom", notes
        )
        st.success(f"Fixed! {old:,.0f} → {custom_facc:,.0f}")
        st.cache_data.clear()
        st.rerun()

    # Option 4: Flag as unfixable
    if st.button("🚩 Flag as unfixable", key=f"flag_{key_prefix}"):
        conn.execute(
            """
            UPDATE reaches SET facc_quality = 'unfixable'
            WHERE reach_id = ? AND region = ?
        """,
            [reach_id, region],
        )
        log_fix(
            conn,
            reach_id,
            region,
            "flagged_unfixable",
            current_facc,
            current_facc,
            notes,
        )
        st.warning("Flagged as unfixable")
        st.cache_data.clear()
        st.rerun()


# =============================================================================
# MAIN APP
# =============================================================================

st.title("🌊 SWORD Topology & FACC Reviewer")

# Sidebar
st.sidebar.header("Settings")
region = st.sidebar.selectbox("Region", ["NA", "SA", "EU", "AF", "AS", "OC"], index=0)

# Summary of saved work
st.sidebar.subheader("💾 Saved to Database")
try:
    # Count unique reaches per check type
    saved_summary = conn.execute(
        """
        SELECT check_id, COUNT(DISTINCT reach_id) as cnt
        FROM lint_fix_log
        WHERE region = ? AND NOT undone
        GROUP BY check_id
    """,
        [region],
    ).fetchdf()

    if len(saved_summary) == 0:
        st.sidebar.info("No saved reviews yet")
    else:
        # Get counts by type
        counts = dict(zip(saved_summary["check_id"], saved_summary["cnt"]))
        hw = int(counts.get("HW", 0))
        topo = int(counts.get("TOPO", 0))
        c001 = int(counts.get("C001", 0))
        c004 = int(counts.get("C004", 0))
        t004 = int(counts.get("T004", 0))
        suspect = int(counts.get("SUSPECT", 0))
        a002 = int(counts.get("A002", 0))

        reviews = hw + c001 + c004 + t004 + suspect + a002
        st.sidebar.success(f"**{reviews} reviews, {topo} fixes**")

        st.sidebar.caption("Reviews by tab:")
        if hw:
            st.sidebar.write(f"  🏔️ Headwaters: {hw}")
        if suspect:
            st.sidebar.write(f"  ⚠️ Suspect: {suspect}")
        if c001:
            st.sidebar.write(f"  🥪 Lake Sandwich: {c001}")
        if c004:
            st.sidebar.write(f"  🏷️ Type Mismatch: {c004}")
        if t004:
            st.sidebar.write(f"  🏝️ Orphans: {t004}")
        if a002:
            st.sidebar.write(f"  📐 Slope: {a002}")

        if topo:
            st.sidebar.caption("Fixes applied:")
            st.sidebar.write(f"  🔗 Topology connections: {topo}")
except Exception as e:
    st.sidebar.warning(f"Could not load summary: {e}")

st.sidebar.divider()

# Beginner mode toggle
beginner_mode = st.sidebar.toggle(
    "Beginner mode",
    value=True,
    help="Show only tabs relevant to QA review. Hides advanced/already-reviewed tabs.",
)

st.sidebar.divider()

# Map settings - hardcoded large defaults
st.session_state.map_hops = 30
st.session_state.map_radius = 4.0  # ~444km
st.session_state.max_reaches = 15000
st.session_state.show_all_reaches = True

# Planet quarterly basemap selector
planet_mosaics = get_planet_quarterly_mosaics(os.environ.get("PLANET_API_KEY", ""))
if planet_mosaics:
    st.sidebar.divider()
    st.sidebar.subheader("Planet Basemap")
    selected_quarter = st.sidebar.selectbox("Quarter", planet_mosaics)
    st.session_state["planet_mosaic"] = selected_quarter

# Tabs for different issue types
# NOTE: Tabs 1-2 (Ratio Violations, Monotonicity) hidden until facc strategy decided
if beginner_mode:
    # Beginner mode: undergrad-relevant tabs first, advanced tabs hidden at end
    tab7, tab9, tab_a002, tab4, tab5, tab3, tab6, tab10 = st.tabs(
        [
            "🏷️ Lakeflag/Type",
            "🏝️ Orphans",
            "📐 Slope",
            "⚠️ Suspect",
            "📜 Fix History",
            "🏔️ Headwaters ⚙️",
            "🥪 Lake Sandwich ⚙️",
            "🔍 Review Fixes ⚙️",
        ]
    )
else:
    tab3, tab4, tab5, tab6, tab7, tab9, tab10, tab_a002 = st.tabs(
        [
            "🏔️ Headwaters",
            "⚠️ Suspect",
            "📜 Fix History",
            "🥪 Lake Sandwich",
            "🏷️ Lakeflag/Type",
            "🏝️ Orphans",
            "🔍 Review Fixes",
            "📐 Slope",
        ]
    )

# =============================================================================
# TAB 1: Ratio Violations (HIDDEN - uncomment when facc strategy decided)
# =============================================================================
# with tab1:
#     st.header("Topology Ratio Violations")
#     st.caption("Reaches where upstream facc >> downstream facc")
#
#     min_ratio = st.slider("Minimum ratio", 2, 100, 10, key="ratio_slider")
#     violations = get_ratio_violations(conn, region, min_ratio)
#
#     st.metric("Total", len(violations))
#
#     if len(violations) > 0:
#         selected_idx = st.selectbox(
#             "Select violation",
#             range(len(violations)),
#             format_func=lambda i: f"#{i+1}: {violations.iloc[i]['upstream_name']} ({violations.iloc[i]['ratio']:.0f}x)",
#             key="ratio_select"
#         )
#
#         v = violations.iloc[selected_idx]
#
#         col1, col2 = st.columns([2, 1])
#
#         with col1:
#             render_reach_map(int(v['upstream_reach']), region, "Upstream Reach")
#
#         with col2:
#             st.markdown(f"**Upstream:** `{v['upstream_reach']}`")
#             st.markdown(f"facc: **{v['upstream_facc']:,.0f}** | width: {v['up_width']:.0f}m")
#             st.markdown(f"**Downstream:** `{v['downstream_reach']}`")
#             st.markdown(f"facc: **{v['downstream_facc']:,.0f}** | width: {v['dn_width']:.0f}m")
#             st.markdown(f"**Ratio: {v['ratio']:.0f}x**")
#
#             st.divider()
#             render_fix_panel(int(v['upstream_reach']), region, v['upstream_facc'], "ratio_violation")

# =============================================================================
# TAB 2: Monotonicity Violations (HIDDEN - uncomment when facc strategy decided)
# =============================================================================
# with tab2:
#     st.header("Monotonicity Violations")
#     st.caption("Reaches where facc > downstream facc (should decrease downstream)")
#
#     mono_issues = get_monotonicity_issues(conn, region)
#
#     col1, col2, col3 = st.columns(3)
#     col1.metric("Total", len(mono_issues))
#     col2.metric("Severe (>100k diff)", len(mono_issues[mono_issues['diff'] > 100000]))
#     col3.metric("Minor (<1k diff)", len(mono_issues[mono_issues['diff'] < 1000]))
#
#     if len(mono_issues) > 0:
#         selected_idx = st.selectbox(
#             "Select issue",
#             range(len(mono_issues)),
#             format_func=lambda i: f"#{i+1}: {mono_issues.iloc[i]['reach_id']} (diff={mono_issues.iloc[i]['diff']:,.0f})",
#             key="mono_select"
#         )
#
#         m = mono_issues.iloc[selected_idx]
#
#         col1, col2 = st.columns([2, 1])
#
#         with col1:
#             render_reach_map(int(m['reach_id']), region)
#
#         with col2:
#             st.markdown(f"**Reach:** `{m['reach_id']}`")
#             st.markdown(f"**facc:** {m['facc']:,.0f} km²")
#             st.markdown(f"**width:** {m['width']:.0f}m")
#             st.markdown(f"**River:** {m['river_name']}")
#             st.divider()
#             st.markdown(f"**Downstream:** `{m['dn_reach_id']}`")
#             st.markdown(f"**dn_facc:** {m['dn_facc']:,.0f} km²")
#             st.markdown(f"**Difference:** {m['diff']:,.0f} km²")
#
#             st.divider()
#             render_fix_panel(int(m['reach_id']), region, m['facc'], "monotonicity")

# =============================================================================
# TAB 3: Headwater Issues
# =============================================================================
with tab3:
    st.header("🏔️ Suspicious Headwaters")

    # Load already-reviewed reaches from database (persists across refreshes)
    reviewed_hw = (
        conn.execute(
            """
        SELECT DISTINCT reach_id FROM lint_fix_log
        WHERE region = ? AND check_id = 'HW' AND NOT undone
    """,
            [region],
        )
        .fetchdf()["reach_id"]
        .tolist()
    )

    # Session state tracks current session additions (merged with DB)
    if "hw_pending" not in st.session_state:
        st.session_state.hw_pending = []

    # Combine DB + session (avoid duplicates)
    all_reviewed = set(reviewed_hw) | set(st.session_state.hw_pending)

    min_facc = st.slider("Minimum facc threshold", 1000, 100000, 5000, key="hw_slider")
    hw_issues = get_headwater_issues(conn, region, min_facc)

    if len(hw_issues) == 0:
        st.success(f"✅ No suspicious headwaters (facc > {min_facc:,} km²)")
    else:
        total = len(hw_issues)
        remaining = len(
            [r for r in hw_issues["reach_id"].tolist() if r not in all_reviewed]
        )
        total_reviewed = len(
            all_reviewed
        )  # Total unique reaches reviewed (saved to DB)
        pct_done = (total_reviewed / total * 100) if total > 0 else 0

        # Progress - big and clear
        st.markdown(
            f"### Progress: **{pct_done:.1f}%** ({total_reviewed} of {total} reviewed)"
        )
        st.progress(total_reviewed / total if total > 0 else 0)

        col1, col2 = st.columns(2)
        col1.metric("⏳ Remaining", remaining)
        col2.metric("💾 Reviewed & Saved", total_reviewed)

        if remaining == 0:
            st.success("🎉 All suspicious headwaters reviewed!")
            if st.button("🔄 Reset reviews", key="reset_hw"):
                st.session_state.hw_pending = []
                st.rerun()
        else:
            available = [
                r for r in hw_issues["reach_id"].tolist() if r not in all_reviewed
            ]
            selected = available[0]
            h = hw_issues[hw_issues["reach_id"] == selected].iloc[0]

            # ===== PROBLEM BOX =====
            st.markdown("---")
            st.subheader(f"🔍 Issue #{total_reviewed + 1}: Reach `{selected}`")

            # Clear problem statement
            st.error(f"""
            **PROBLEM:** This reach has **NO upstream neighbors** but has **HIGH facc ({h["facc"]:,.0f} km²)**

            True headwaters should have LOW facc (small catchment). High facc suggests:
            - Lake outlet (facc inherited from lake)
            - Missing upstream topology
            - Wrong flow direction
            """)

            col1, col2 = st.columns([2, 1])

            with col1:
                render_reach_map_satellite(int(selected), region, conn)

            with col2:
                st.markdown("### 📊 Details")
                st.markdown(f"**FACC:** {h['facc']:,.0f} km²")
                st.markdown(f"**Width:** {h['width']:.0f}m")
                st.markdown(f"**River:** {h['river_name'] or 'Unnamed'}")
                laketype = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}.get(
                    h["lakeflag"], "?"
                )

                # Hint based on type
                if h["lakeflag"] == 1:
                    st.success(f"**Type:** {laketype} → Likely valid lake outlet")
                else:
                    st.warning(f"**Type:** {laketype} → Check if topology is correct")

                st.markdown("---")
                st.markdown("### 🎯 Classification")

                # Action buttons
                if st.button(
                    "✅ Lake Outlet (valid)",
                    key=f"hw_lake_{selected}",
                    type="primary",
                    use_container_width=True,
                ):
                    log_skip(
                        conn, selected, region, "HW", "Lake outlet - valid high facc"
                    )
                    st.session_state.hw_pending.append(selected)
                    st.rerun()

                # Expandable section to FIX topology
                with st.expander("🔗 Fix Missing Upstream", expanded=False):
                    st.markdown("**Select a reach to connect as upstream:**")

                    # Get nearby unconnected reaches - use same radius as map
                    network_ids = st.session_state.get("last_network_ids", [])
                    search_radius = st.session_state.get("map_radius", 3.0)
                    max_reaches = st.session_state.get("max_reaches", 10000)
                    nearby = get_nearby_reaches(
                        conn,
                        h["x"],
                        h["y"],
                        search_radius,
                        region,
                        exclude_ids=network_ids,
                        max_reaches=max_reaches,
                    )

                    if len(nearby) == 0:
                        st.info(
                            "No unconnected reaches found nearby. Try increasing search radius in sidebar."
                        )
                    else:
                        reach_ids = [int(r["reach_id"]) for _, r in nearby.iterrows()]

                        # Check if user clicked a reach on the map
                        clicked = st.session_state.get("clicked_reach")

                        # Clear stale click if it's not in current nearby list
                        if clicked and clicked not in reach_ids:
                            st.session_state.clicked_reach = None
                            clicked = None

                        if clicked:
                            # User clicked on map - use that reach directly
                            neighbor_id = clicked
                            st.success(f"🖱️ **Clicked:** `{neighbor_id}`")
                            if st.button(
                                "❌ Clear selection", key=f"hw_clear_{selected}"
                            ):
                                st.session_state.clicked_reach = None
                                st.rerun()
                        else:
                            # Show dropdown to pick manually
                            st.caption(
                                "👆 Click a reach on the map, or pick from list:"
                            )
                            neighbor_id = st.selectbox(
                                "Nearby reaches:",
                                reach_ids,
                                key=f"hw_connect_{selected}",
                            )

                        # Show info about selected neighbor
                        neighbor_info = conn.execute(
                            """
                            SELECT facc, width, lakeflag, river_name
                            FROM reaches WHERE reach_id = ? AND region = ?
                        """,
                            [neighbor_id, region],
                        ).fetchone()
                        if neighbor_info:
                            n_type = {
                                0: "River",
                                1: "Lake",
                                2: "Canal",
                                3: "Tidal",
                            }.get(neighbor_info[2], "?")
                            st.markdown(
                                f"**{n_type}** | facc={neighbor_info[0]:,.0f} km² | width={neighbor_info[1]:.0f}m"
                            )

                        st.markdown(
                            f"➡️ `{neighbor_id}` (upstream) → `{selected}` (this reach)"
                        )

                        if st.button(
                            "✅ Connect as UPSTREAM",
                            key=f"hw_do_connect_{selected}",
                            type="primary",
                            use_container_width=True,
                        ):
                            success, msg = add_topology_connection(
                                conn, selected, neighbor_id, "up", region
                            )
                            if success:
                                st.success(f"✅ {msg}")
                                st.session_state.hw_pending.append(selected)
                                st.session_state.highlight_reach = None
                                st.session_state.clicked_reach = None
                                st.rerun()
                            else:
                                st.error(f"❌ {msg}")

                if st.button(
                    "↩️ Wrong Flow Direction",
                    key=f"hw_flow_{selected}",
                    use_container_width=True,
                ):
                    log_skip(conn, selected, region, "HW", "Wrong flow direction")
                    st.session_state.hw_pending.append(selected)
                    st.rerun()

                if st.button(
                    "✓ Valid Headwater",
                    key=f"hw_valid_{selected}",
                    use_container_width=True,
                ):
                    log_skip(conn, selected, region, "HW", "Valid headwater")
                    st.session_state.hw_pending.append(selected)
                    st.rerun()

                st.markdown("---")
                # Undo button for topology fixes
                if st.button(
                    "⏪ Undo Last Topology Fix",
                    key=f"hw_undo_{selected}",
                    use_container_width=True,
                ):
                    reach_id, msg = undo_last_topology_fix(conn, region)
                    if reach_id:
                        st.success(f"✅ {msg}")
                        st.rerun()
                    else:
                        st.warning(msg)

# =============================================================================
# TAB 4: Suspect Reaches
# =============================================================================
with tab4:
    st.header("⚠️ Suspect Reaches")

    # Initialize session state
    if "suspect_pending" not in st.session_state:
        st.session_state.suspect_pending = []

    suspect = get_suspect_reaches(conn, region)

    if len(suspect) == 0:
        st.success("✅ No suspect reaches in this region")
    else:
        total = len(suspect)
        done = len(st.session_state.suspect_pending)
        remaining = len(
            [
                r
                for r in suspect["reach_id"].tolist()
                if r not in st.session_state.suspect_pending
            ]
        )

        # Progress
        col1, col2, col3 = st.columns(3)
        col1.metric("Remaining", remaining)
        col2.metric("Reviewed", done)
        col3.metric("Total", total)
        st.progress(done / total if total > 0 else 0)

        if remaining == 0:
            st.success("🎉 All suspect reaches reviewed!")
            if st.button("🔄 Reset reviews", key="reset_suspect"):
                st.session_state.suspect_pending = []
                st.rerun()
        else:
            available = [
                r
                for r in suspect["reach_id"].tolist()
                if r not in st.session_state.suspect_pending
            ]
            selected = available[0]
            s = suspect[suspect["reach_id"] == selected].iloc[0]

            # ===== PROBLEM BOX =====
            st.markdown("---")
            st.subheader(f"🔍 Issue #{done + 1}: Reach `{selected}`")

            st.warning(f"""
            **PROBLEM:** This reach was flagged as `facc_quality='suspect'`

            Automated methods couldn't determine the correct facc value. Manual review needed.
            - **Current FACC:** {s["facc"]:,.0f} km²
            - **Width:** {s["width"]:.0f}m
            """)

            col1, col2 = st.columns([2, 1])

            with col1:
                render_reach_map_satellite(int(selected), region, conn)

            with col2:
                st.markdown(f"**River:** {s['river_name'] or 'Unnamed'}")
                st.markdown(f"**Edit flag:** {s['edit_flag']}")

                st.markdown("---")
                st.markdown("### 🎯 Classification")

                if st.button(
                    "✅ FACC looks correct",
                    key=f"suspect_ok_{selected}",
                    type="primary",
                    use_container_width=True,
                ):
                    log_skip(
                        conn, selected, region, "SUSPECT", "FACC value looks correct"
                    )
                    st.session_state.suspect_pending.append(selected)
                    st.rerun()

                if st.button(
                    "❌ FACC is wrong",
                    key=f"suspect_wrong_{selected}",
                    use_container_width=True,
                ):
                    log_skip(
                        conn, selected, region, "SUSPECT", "FACC value is incorrect"
                    )
                    st.session_state.suspect_pending.append(selected)
                    st.rerun()

                if st.button(
                    "🔍 Needs more investigation",
                    key=f"suspect_inv_{selected}",
                    use_container_width=True,
                ):
                    log_skip(
                        conn, selected, region, "SUSPECT", "Needs more investigation"
                    )
                    st.session_state.suspect_pending.append(selected)
                    st.rerun()

# =============================================================================
# TAB 5: Fix History
# =============================================================================
with tab5:
    st.header("Fix History")
    st.caption("Log of all manual fixes for analysis")

    show_all_regions = st.checkbox("Show all regions", value=False)

    history = get_fix_history(conn, None if show_all_regions else region, 500)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Fixes", len(history))
    if len(history) > 0:
        col2.metric("Fix Types", history["fix_type"].nunique())
        col3.metric(
            "Avg Reduction",
            f"{(history['old_facc'] - history['new_facc']).mean():,.0f}",
        )

    if len(history) > 0:
        # Summary by fix type
        st.subheader("By Fix Type")
        summary = (
            history.groupby("fix_type")
            .agg({"reach_id": "count", "old_facc": "mean", "new_facc": "mean"})
            .rename(
                columns={
                    "reach_id": "count",
                    "old_facc": "avg_old",
                    "new_facc": "avg_new",
                }
            )
        )
        summary["avg_reduction"] = summary["avg_old"] - summary["avg_new"]
        st.dataframe(summary)

        # Full table
        st.subheader("All Fixes")
        st.dataframe(history, height=400)

        # Export
        st.subheader("Export")
        csv = history.to_csv(index=False)
        st.download_button(
            "📥 Download CSV",
            csv,
            f"facc_fixes_{region}_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv",
        )
    else:
        st.info("No fixes logged yet. Make some fixes in the other tabs!")

# =============================================================================
# TAB 6: Lake Sandwich (C001)
# =============================================================================
with tab6:
    st.header("🥪 Lake Sandwich Fixer")

    # Initialize session state
    if "pending_fixes" not in st.session_state:
        st.session_state.pending_fixes = []
    if "last_fix" not in st.session_state:
        st.session_state.last_fix = None
    if "c001_results" not in st.session_state:
        st.session_state.c001_results = None

    # Auto-run C001 check on first load or when region changes
    if (
        st.session_state.c001_results is None
        or st.session_state.get("c001_region") != region
    ):
        with st.spinner("Running lake sandwich check..."):
            st.session_state.c001_results = run_c001_check(conn, region)
            st.session_state.c001_region = region
            session = load_session_fixes(region, "C001")
            st.session_state.pending_fixes = session.get("pending", [])

    # Progress bar at top
    result = st.session_state.c001_results
    if result:
        issues = result.details
        total = len(issues)
        done = len(st.session_state.pending_fixes)
        remaining = (
            len(
                [
                    r
                    for r in issues["reach_id"].tolist()
                    if r not in st.session_state.pending_fixes
                ]
            )
            if total > 0
            else 0
        )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Remaining", remaining)
        col2.metric("Reviewed", done)
        col3.metric("Total", total)
        if st.session_state.last_fix:
            col4.button("↩️ Undo", key="undo_c001", on_click=lambda: None)  # Placeholder

        if total > 0:
            st.progress(
                done / total if total > 0 else 0, text=f"{done}/{total} reviewed"
            )

        if remaining == 0 and total > 0:
            st.success("🎉 All lake sandwiches reviewed!")
            if st.button("🔄 Re-run check to verify", key="rerun_c001"):
                st.session_state.c001_results = None
                st.session_state.pending_fixes = []
                st.rerun()
        elif total == 0:
            st.success("✅ No lake sandwich issues in this region!")
        else:
            # Get next issue
            available = [
                r
                for r in issues["reach_id"].tolist()
                if r not in st.session_state.pending_fixes
            ]
            selected = available[0]  # Auto-select first available
            issue = issues[issues["reach_id"] == selected].iloc[0]

            # Get decision data
            up_flags, dn_flags = get_neighbor_lakeflags(conn, selected, region)
            slope = get_reach_slope(conn, selected, region)
            width = issue["width"] if pd.notna(issue["width"]) else 0

            # ===== PROBLEM BOX =====
            st.markdown("---")
            st.subheader(f"🔍 Issue #{done + 1}: Reach `{selected}`")

            # Visual sandwich diagram
            up_label = (
                "🔵 LAKE"
                if up_flags and 1 in up_flags
                else "〰️ River"
                if up_flags
                else "?"
            )
            dn_label = (
                "🔵 LAKE"
                if dn_flags and 1 in dn_flags
                else "〰️ River"
                if dn_flags
                else "?"
            )
            st.markdown(f"""
            ```
            Upstream:   {up_label}
                          ↓
            THIS REACH: 〰️ River (lakeflag=0)  ← Is this actually a lake?
                          ↓
            Downstream: {dn_label}
            ```
            """)

            # ===== DECISION HELPER =====
            col1, col2 = st.columns([2, 1])

            with col1:
                render_reach_map_satellite(int(selected), region, conn)

            with col2:
                st.markdown("### 📊 Key Indicators")

                # Width indicator
                if width > 500:
                    st.success(f"**Width:** {width:.0f}m → Wide (likely lake)")
                elif width > 200:
                    st.warning(f"**Width:** {width:.0f}m → Medium")
                elif width > 0:
                    st.error(f"**Width:** {width:.0f}m → Narrow (likely river)")
                else:
                    st.info("**Width:** N/A")

                # Slope indicator
                if slope is not None:
                    if slope < 0.0001:
                        st.success(f"**Slope:** {slope:.6f} → Flat (likely lake)")
                    elif slope < 0.001:
                        st.warning(f"**Slope:** {slope:.6f} → Low slope")
                    else:
                        st.error(f"**Slope:** {slope:.6f} → Steep (likely river)")
                else:
                    st.info("**Slope:** N/A")

                # Neighbor indicator
                both_lakes = (up_flags and 1 in up_flags) and (
                    dn_flags and 1 in dn_flags
                )
                if both_lakes:
                    st.success("**Neighbors:** Both lakes → Likely lake")
                else:
                    st.warning("**Neighbors:** Mixed types")

                st.markdown(f"**River name:** {issue['river_name'] or 'Unnamed'}")
                st.markdown(
                    f"**Length:** {issue['reach_length']:.0f}m"
                    if pd.notna(issue["reach_length"])
                    else ""
                )

            # ===== BIG DECISION BUTTONS =====
            st.markdown("---")
            st.markdown("### 🎯 Your Decision")

            btn_col1, btn_col2 = st.columns(2)

            with btn_col1:
                st.markdown("**It's a LAKE** (convert lakeflag to 1)")
                if st.button(
                    "✅ YES, IT'S A LAKE",
                    key=f"fix_{selected}",
                    type="primary",
                    use_container_width=True,
                ):
                    apply_lakeflag_fix(conn, selected, region, 1)
                    st.session_state.pending_fixes.append(selected)
                    st.session_state.last_fix = selected
                    st.cache_data.clear()
                    st.rerun()

            with btn_col2:
                st.markdown("**Keep as RIVER** (no change)")
                skip_reason = st.selectbox(
                    "Why is it a river?",
                    [
                        "Flowing water visible",
                        "Narrow channel",
                        "Dam/weir",
                        "Canal",
                        "Tidal",
                        "Other",
                    ],
                    key=f"skip_reason_{selected}",
                    label_visibility="collapsed",
                )
                if st.button(
                    "❌ NO, IT'S A RIVER",
                    key=f"skip_{selected}",
                    use_container_width=True,
                ):
                    log_skip(conn, selected, region, "C001", skip_reason)
                    st.session_state.pending_fixes.append(selected)
                    st.cache_data.clear()
                    st.rerun()

            # Undo option (small, at bottom)
            if st.session_state.last_fix:
                if st.button(
                    f"↩️ Undo last ({st.session_state.last_fix})", key="undo_last"
                ):
                    undone_id = undo_last_fix(conn, region)
                    if undone_id:
                        st.session_state.pending_fixes = [
                            p for p in st.session_state.pending_fixes if p != undone_id
                        ]
                        st.session_state.last_fix = None
                        st.cache_data.clear()
                        st.rerun()

    # Refresh button at bottom
    st.markdown("---")
    if st.button("🔄 Refresh Check", key="refresh_c001"):
        st.session_state.c001_results = None
        st.session_state.pending_fixes = []
        st.rerun()

# =============================================================================
# TAB 7: Lakeflag/Type Consistency (C004)
# =============================================================================
with tab7:
    st.header("🏷️ Lakeflag/Type Mismatch")

    # Initialize session state for C004
    if "c004_issues" not in st.session_state:
        st.session_state.c004_issues = None
    if "c004_pending" not in st.session_state:
        st.session_state.c004_pending = []

    # Load already-reviewed reaches from database
    reviewed_c004 = (
        conn.execute(
            """
        SELECT DISTINCT reach_id FROM lint_fix_log
        WHERE region = ? AND check_id = 'C004' AND NOT undone
    """,
            [region],
        )
        .fetchdf()["reach_id"]
        .tolist()
    )
    all_reviewed_c004 = set(reviewed_c004) | set(st.session_state.c004_pending)

    # Query individual mismatched reaches directly (the lint check only returns a cross-tab summary)
    if (
        st.session_state.c004_issues is None
        or st.session_state.get("c004_region") != region
    ):
        with st.spinner("Finding lakeflag/type mismatches..."):
            try:
                c004_df = conn.execute(
                    """
                    SELECT reach_id, region, river_name, x, y, lakeflag, type,
                        CASE
                            WHEN lakeflag = 1 AND type = 1 THEN 'lake_labeled_as_river_type'
                            WHEN lakeflag = 0 AND type = 3 THEN 'river_labeled_as_lake_type'
                            WHEN lakeflag = 2 AND type NOT IN (1, 4, 5, 6) THEN 'canal_type_mismatch'
                            WHEN lakeflag = 3 AND type NOT IN (5, 6) THEN 'tidal_type_mismatch'
                            ELSE 'other_mismatch'
                        END as issue_type
                    FROM reaches
                    WHERE region = ?
                        AND lakeflag IS NOT NULL AND type IS NOT NULL
                        AND NOT (
                            (lakeflag = 0 AND type IN (1, 4))
                            OR (lakeflag = 1 AND type IN (3, 4))
                            OR (lakeflag = 2 AND type IN (1, 4))
                            OR type IN (5, 6)
                        )
                    ORDER BY reach_id
                """,
                    [region],
                ).fetchdf()
                st.session_state.c004_issues = c004_df
            except Exception:
                st.session_state.c004_issues = pd.DataFrame()
            st.session_state.c004_region = region

    issues = st.session_state.c004_issues

    # Check if type column exists / has results
    if issues is None or len(issues) == 0:
        st.success("No lakeflag/type mismatches in this region!")
    else:
        total = len(issues)
        remaining = len(
            [r for r in issues["reach_id"].tolist() if r not in all_reviewed_c004]
        )
        done = len(all_reviewed_c004)

        # Progress
        col1, col2, col3 = st.columns(3)
        col1.metric("Remaining", remaining)
        col2.metric("Reviewed", done)
        col3.metric("Total", total)
        st.progress(done / total if total > 0 else 0)

        if remaining == 0:
            st.success("🎉 All mismatches reviewed!")
        else:
            available = [
                r for r in issues["reach_id"].tolist() if r not in all_reviewed_c004
            ]
            selected = available[0]
            issue = issues[issues["reach_id"] == selected].iloc[0]

            lakeflag_map = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}
            type_map = {
                1: "river",
                2: "lake",
                3: "tidal_river",
                4: "artificial",
                5: "unassigned",
                6: "unreliable",
            }
            lf = issue["lakeflag"]
            tp = issue["type"]

            # Problem box
            st.markdown("---")
            st.subheader(f"🔍 Issue #{done + 1}: Reach `{selected}`")
            st.error(f"""
            **Mismatch detected:**
            - **Lakeflag:** {lf} ({lakeflag_map.get(lf, "?")})
            - **Type:** {tp} ({type_map.get(tp, "?")})
            - **Issue:** {issue["issue_type"]}
            """)

            col1, col2 = st.columns([2, 1])
            with col1:
                render_reach_map_satellite(
                    int(selected), region, conn, color_by_type=True
                )

            with col2:
                st.markdown(f"**River:** {issue['river_name'] or 'Unnamed'}")

                st.markdown("---")
                st.markdown("### 🎯 Fix the mismatch")

                # Determine fix options based on issue type
                if "lake" in issue["issue_type"]:
                    if st.button(
                        "✅ Set type=2 (lake)",
                        key=f"c004_fix_{selected}",
                        type="primary",
                        use_container_width=True,
                    ):
                        conn.execute(
                            "UPDATE reaches SET type = 2 WHERE reach_id = ? AND region = ?",
                            [selected, region],
                        )
                        conn.commit()
                        log_skip(conn, selected, region, "C004", "Fixed: type→2")
                        st.session_state.c004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()
                elif "river" in issue["issue_type"]:
                    if st.button(
                        "✅ Set type=1 (river)",
                        key=f"c004_fix_{selected}",
                        type="primary",
                        use_container_width=True,
                    ):
                        conn.execute(
                            "UPDATE reaches SET type = 1 WHERE reach_id = ? AND region = ?",
                            [selected, region],
                        )
                        conn.commit()
                        log_skip(conn, selected, region, "C004", "Fixed: type→1")
                        st.session_state.c004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()
                elif "canal" in issue["issue_type"]:
                    if st.button(
                        "✅ Set type=4 (artificial)",
                        key=f"c004_fix_{selected}",
                        type="primary",
                        use_container_width=True,
                    ):
                        conn.execute(
                            "UPDATE reaches SET type = 4 WHERE reach_id = ? AND region = ?",
                            [selected, region],
                        )
                        conn.commit()
                        log_skip(conn, selected, region, "C004", "Fixed: type→4")
                        st.session_state.c004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()
                elif "tidal" in issue["issue_type"]:
                    if st.button(
                        "✅ Set lakeflag=0 (river)",
                        key=f"c004_fix_lf_{selected}",
                        type="primary",
                        use_container_width=True,
                    ):
                        conn.execute(
                            "UPDATE reaches SET lakeflag = 0 WHERE reach_id = ? AND region = ?",
                            [selected, region],
                        )
                        conn.commit()
                        log_skip(
                            conn,
                            selected,
                            region,
                            "C004",
                            "Fixed: lakeflag→0 (was tidal)",
                        )
                        st.session_state.c004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()
                    if st.button(
                        "✅ Set type=1 (river)",
                        key=f"c004_fix_tp_{selected}",
                        use_container_width=True,
                    ):
                        conn.execute(
                            "UPDATE reaches SET type = 1 WHERE reach_id = ? AND region = ?",
                            [selected, region],
                        )
                        conn.commit()
                        log_skip(
                            conn, selected, region, "C004", "Fixed: type→1 (was tidal)"
                        )
                        st.session_state.c004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()

                if st.button(
                    "⏭️ Skip (correct as-is)",
                    key=f"c004_skip_{selected}",
                    use_container_width=True,
                ):
                    log_skip(conn, selected, region, "C004", "Skipped: correct as-is")
                    st.session_state.c004_pending.append(selected)
                    st.cache_data.clear()
                    st.rerun()

# =============================================================================
# TAB 9: Orphan Reaches (T004)
# =============================================================================
with tab9:
    st.header("🏝️ Orphan Reaches")

    # Initialize session state for T004
    if "t004_results" not in st.session_state:
        st.session_state.t004_results = None
    if "t004_pending" not in st.session_state:
        st.session_state.t004_pending = []

    # Auto-run check
    if (
        st.session_state.t004_results is None
        or st.session_state.get("t004_region") != region
    ):
        with st.spinner("Running T004 check..."):
            st.session_state.t004_results = check_orphan_reaches(conn, region)
            st.session_state.t004_region = region

    result = st.session_state.t004_results
    if result is None or len(result.details) == 0:
        st.success("✅ No orphan reaches in this region! Topology is fully connected.")
    else:
        issues = result.details
        total = len(issues)
        done = len(st.session_state.t004_pending)
        remaining = len(
            [
                r
                for r in issues["reach_id"].tolist()
                if r not in st.session_state.t004_pending
            ]
        )

        # Progress
        col1, col2, col3 = st.columns(3)
        col1.metric("Remaining", remaining)
        col2.metric("Reviewed", done)
        col3.metric("Total Orphans", total)
        st.progress(done / total if total > 0 else 0)

        st.info(
            "💡 **Orphans** are reaches with no upstream AND no downstream neighbors. Most are valid isolated water bodies (ponds, small lakes)."
        )

        if remaining == 0:
            st.success("🎉 All orphans reviewed!")
        else:
            available = [
                r
                for r in issues["reach_id"].tolist()
                if r not in st.session_state.t004_pending
            ]
            selected = available[0]
            issue = issues[issues["reach_id"] == selected].iloc[0]

            # Problem box
            st.markdown("---")
            st.subheader(f"🔍 Orphan #{done + 1}: Reach `{selected}`")

            width = issue["width"] if pd.notna(issue["width"]) else 0
            length = issue["reach_length"] if pd.notna(issue["reach_length"]) else 0

            st.warning(f"""
            **Disconnected reach** (no neighbors):
            - **Length:** {length:.0f}m
            - **Width:** {width:.0f}m
            - **Network ID:** {issue["network"]}
            """)

            col1, col2 = st.columns([2, 1])
            with col1:
                render_reach_map_satellite(int(selected), region, conn)

            with col2:
                st.markdown(f"**River:** {issue['river_name'] or 'Unnamed'}")

                # Size-based hint
                if width > 100 or length > 1000:
                    st.warning("Large feature → May need topology connection")
                else:
                    st.success("Small feature → Likely valid isolated pond")

                st.markdown("---")
                st.markdown("### 🎯 Classification")

                btn_col1, btn_col2 = st.columns(2)
                with btn_col1:
                    if st.button(
                        "✅ Valid Orphan",
                        key=f"t004_valid_{selected}",
                        type="primary",
                        use_container_width=True,
                    ):
                        log_skip(
                            conn,
                            selected,
                            region,
                            "T004",
                            "Valid orphan - isolated water body",
                        )
                        st.session_state.t004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()

                with btn_col2:
                    if st.button(
                        "🔗 Needs Connection",
                        key=f"t004_connect_{selected}",
                        use_container_width=True,
                    ):
                        log_skip(
                            conn, selected, region, "T004", "Needs topology connection"
                        )
                        st.session_state.t004_pending.append(selected)
                        st.cache_data.clear()
                        st.rerun()

# =============================================================================
# TAB 10: Review Fixes (Verify/Undo Topology Connections)
# =============================================================================
with tab10:
    st.header("🔍 Review Topology Fixes")
    st.caption("Verify and undo topology connections you've made")

    # Get all TOPO fixes for this region
    topo_fixes = conn.execute(
        """
        SELECT fix_id, timestamp, reach_id, notes, undone
        FROM lint_fix_log
        WHERE region = ? AND check_id = 'TOPO' AND notes LIKE 'Added%'
        ORDER BY timestamp DESC
    """,
        [region],
    ).fetchdf()

    active_fixes = topo_fixes[~topo_fixes["undone"]]
    undone_fixes = topo_fixes[topo_fixes["undone"]]

    col1, col2 = st.columns(2)
    col1.metric("Active Fixes", len(active_fixes))
    col2.metric("Undone", len(undone_fixes))

    if len(active_fixes) == 0:
        st.info(
            "No active topology fixes to review. Make some connections in the Headwaters tab first."
        )
    else:
        st.success(f"✅ {len(active_fixes)} topology connection(s) to review")

        # Let user select which fix to review
        fix_options = []
        for _, fix in active_fixes.iterrows():
            # Parse the notes to get connected reach
            notes = fix["notes"]
            # Format: "Added upstream connection to 12345" or "Added downstream connection to 12345"
            match = re.search(r"Added (\w+)stream connection to (\d+)", notes)
            if match:
                direction = match.group(1)
                connected_id = match.group(2)
                fix_options.append(
                    {
                        "fix_id": fix["fix_id"],
                        "reach_id": int(fix["reach_id"]),
                        "connected_id": int(connected_id),
                        "direction": direction,
                        "timestamp": fix["timestamp"],
                        "label": f"#{fix['fix_id']}: {fix['reach_id']} ← {connected_id} (upstream)",
                    }
                )

        if fix_options:
            selected_idx = st.selectbox(
                "Select a fix to review",
                range(len(fix_options)),
                format_func=lambda i: fix_options[i]["label"],
                key="review_fix_select",
            )

            selected_fix = fix_options[selected_idx]
            reach_id = selected_fix["reach_id"]
            connected_id = selected_fix["connected_id"]

            st.markdown("---")
            st.subheader(f"Fix #{selected_fix['fix_id']}")

            # Info about the connection
            col1, col2, col3 = st.columns(3)
            col1.markdown(f"**Headwater Reach:**\n`{reach_id}`")
            col2.markdown(f"**Connected Upstream:**\n`{connected_id}`")
            col3.markdown(f"**Made:**\n{str(selected_fix['timestamp'])[:16]}")

            st.info(
                f"🔗 **Connection:** Reach `{connected_id}` flows INTO reach `{reach_id}`"
            )

            # Render map showing both reaches
            st.markdown("#### Map View")

            # Get geometries for both reaches
            main_geom = get_reach_geometry(conn, reach_id)
            connected_geom = get_reach_geometry(conn, connected_id)

            if main_geom and connected_geom:
                # Combine coords for bounds
                all_coords = list(main_geom) + list(connected_geom)
                lons = [c[0] for c in all_coords]
                lats = [c[1] for c in all_coords]
                center_lat = (min(lats) + max(lats)) / 2
                center_lon = (min(lons) + max(lons)) / 2
                extent = max(max(lons) - min(lons), max(lats) - min(lats))
                zoom = 15 if extent < 0.02 else 14 if extent < 0.05 else 12

                m = folium.Map(
                    location=[center_lat, center_lon], zoom_start=zoom, tiles=None
                )
                folium.TileLayer(
                    tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                    attr="Esri",
                    name="Satellite",
                ).add_to(m)
                _add_planet_tile_layer(m)
                folium.LayerControl().add_to(m)

                # Draw connected reach (upstream) in orange
                conn_coords = [[c[1], c[0]] for c in connected_geom]
                folium.PolyLine(
                    conn_coords, color="black", weight=10, opacity=1.0
                ).add_to(m)
                add_flow_line(
                    m,
                    conn_coords,
                    "orange",
                    weight=6,
                    opacity=1.0,
                    tooltip=f"UPSTREAM: {connected_id}",
                )

                # Draw main reach (headwater that was fixed) in yellow
                main_coords = [[c[1], c[0]] for c in main_geom]
                folium.PolyLine(
                    main_coords, color="black", weight=12, opacity=1.0
                ).add_to(m)
                add_flow_line(
                    m,
                    main_coords,
                    "#FFFF00",
                    weight=8,
                    opacity=1.0,
                    tooltip=f"HEADWATER: {reach_id}",
                )

                # Add markers at centers
                if len(conn_coords) > 0:
                    center = conn_coords[len(conn_coords) // 2]
                    folium.CircleMarker(
                        center,
                        radius=12,
                        color="black",
                        fill_color="orange",
                        fill_opacity=1.0,
                        weight=2,
                        tooltip=f"UPSTREAM: {connected_id}",
                    ).add_to(m)

                if len(main_coords) > 0:
                    center = main_coords[len(main_coords) // 2]
                    folium.CircleMarker(
                        center,
                        radius=15,
                        color="black",
                        fill_color="#FFFF00",
                        fill_opacity=1.0,
                        weight=3,
                        tooltip=f"★ HEADWATER: {reach_id}",
                    ).add_to(m)

                # Add connection arrow between reaches
                if len(conn_coords) > 0 and len(main_coords) > 0:
                    # Arrow from end of upstream to start of headwater
                    from_pt = conn_coords[-1]  # End of upstream
                    to_pt = main_coords[0]  # Start of headwater
                    folium.PolyLine(
                        [from_pt, to_pt],
                        color="#00ff00",
                        weight=4,
                        opacity=0.8,
                        dash_array="10,10",
                        tooltip="Connection",
                    ).add_to(m)

                padding = max(extent * 0.3, 0.005)
                m.fit_bounds(
                    [
                        [min(lats) - padding, min(lons) - padding],
                        [max(lats) + padding, max(lons) + padding],
                    ]
                )

                st_folium(
                    m,
                    width=None,
                    height=400,
                    key=f"review_map_{selected_fix['fix_id']}",
                )

            else:
                st.warning("Could not load geometry for one or both reaches")

            # Get reach info
            reach_info = conn.execute(
                """
                SELECT facc, width, reach_length, lakeflag, river_name
                FROM reaches WHERE reach_id = ? AND region = ?
            """,
                [reach_id, region],
            ).fetchone()

            connected_info = conn.execute(
                """
                SELECT facc, width, reach_length, lakeflag, river_name
                FROM reaches WHERE reach_id = ? AND region = ?
            """,
                [connected_id, region],
            ).fetchone()

            st.markdown("#### Reach Details")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**Headwater Reach** `{reach_id}`")
                if reach_info:
                    type_name = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}.get(
                        reach_info[3], "?"
                    )
                    st.markdown(f"- **Type:** {type_name}")
                    st.markdown(
                        f"- **FACC:** {reach_info[0]:,.0f} km²"
                        if reach_info[0]
                        else "- **FACC:** N/A"
                    )
                    st.markdown(
                        f"- **Width:** {reach_info[1]:.0f}m"
                        if reach_info[1]
                        else "- **Width:** N/A"
                    )
                    st.markdown(f"- **Name:** {reach_info[4] or 'Unnamed'}")

            with col2:
                st.markdown(f"**Upstream Reach** `{connected_id}`")
                if connected_info:
                    type_name = {0: "River", 1: "Lake", 2: "Canal", 3: "Tidal"}.get(
                        connected_info[3], "?"
                    )
                    st.markdown(f"- **Type:** {type_name}")
                    st.markdown(
                        f"- **FACC:** {connected_info[0]:,.0f} km²"
                        if connected_info[0]
                        else "- **FACC:** N/A"
                    )
                    st.markdown(
                        f"- **Width:** {connected_info[1]:.0f}m"
                        if connected_info[1]
                        else "- **Width:** N/A"
                    )
                    st.markdown(f"- **Name:** {connected_info[4] or 'Unnamed'}")

            st.markdown("---")
            st.markdown("#### Actions")

            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    "✅ Looks Good",
                    key=f"review_keep_{selected_fix['fix_id']}",
                    type="primary",
                    use_container_width=True,
                ):
                    st.success("Fix verified! Moving to next...")
                    st.rerun()

            with col2:
                if st.button(
                    "❌ Undo This Fix",
                    key=f"review_undo_{selected_fix['fix_id']}",
                    type="secondary",
                    use_container_width=True,
                ):
                    # Call the undo function directly for this specific fix
                    try:
                        # Get fix details
                        direction = selected_fix["direction"]

                        # Remove topology edges
                        if direction == "up":
                            conn.execute(
                                """
                                DELETE FROM reach_topology
                                WHERE reach_id = ? AND neighbor_id = ? AND direction = 'up' AND region = ?
                            """,
                                [reach_id, connected_id, region],
                            )
                            conn.execute(
                                """
                                DELETE FROM reach_topology
                                WHERE reach_id = ? AND neighbor_id = ? AND direction = 'down' AND region = ?
                            """,
                                [connected_id, reach_id, region],
                            )
                            # Update counts
                            conn.execute(
                                "UPDATE reaches SET n_rch_up = GREATEST(0, n_rch_up - 1) WHERE reach_id = ? AND region = ?",
                                [reach_id, region],
                            )
                            conn.execute(
                                "UPDATE reaches SET n_rch_down = GREATEST(0, n_rch_down - 1) WHERE reach_id = ? AND region = ?",
                                [connected_id, region],
                            )
                        else:
                            conn.execute(
                                """
                                DELETE FROM reach_topology
                                WHERE reach_id = ? AND neighbor_id = ? AND direction = 'down' AND region = ?
                            """,
                                [reach_id, connected_id, region],
                            )
                            conn.execute(
                                """
                                DELETE FROM reach_topology
                                WHERE reach_id = ? AND neighbor_id = ? AND direction = 'up' AND region = ?
                            """,
                                [connected_id, reach_id, region],
                            )
                            # Update counts
                            conn.execute(
                                "UPDATE reaches SET n_rch_down = GREATEST(0, n_rch_down - 1) WHERE reach_id = ? AND region = ?",
                                [reach_id, region],
                            )
                            conn.execute(
                                "UPDATE reaches SET n_rch_up = GREATEST(0, n_rch_up - 1) WHERE reach_id = ? AND region = ?",
                                [connected_id, region],
                            )

                        # Mark fix as undone
                        conn.execute(
                            "UPDATE lint_fix_log SET undone = TRUE WHERE fix_id = ?",
                            [selected_fix["fix_id"]],
                        )

                        # Log undo action
                        log_skip(
                            conn,
                            reach_id,
                            region,
                            "TOPO",
                            f"Undid {direction}stream connection to {connected_id}",
                        )

                        conn.commit()
                        st.success(f"✅ Undid connection: {reach_id} ← {connected_id}")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error undoing fix: {e}")

    # Show undone fixes history
    if len(undone_fixes) > 0:
        with st.expander(f"📜 Previously Undone ({len(undone_fixes)})"):
            for _, fix in undone_fixes.iterrows():
                st.text(
                    f"#{fix['fix_id']}: {fix['reach_id']} - {fix['notes']} (undone)"
                )

# =============================================================================
# TAB A002: Slope Reasonableness
# =============================================================================
with tab_a002:
    st.header("📐 Slope Reasonableness (A002)")

    # Initialize session state for A002
    if "a002_results" not in st.session_state:
        st.session_state.a002_results = None
    if "a002_pending" not in st.session_state:
        st.session_state.a002_pending = []

    # Load already-reviewed reaches from database
    reviewed_a002 = (
        conn.execute(
            """
        SELECT DISTINCT reach_id FROM lint_fix_log
        WHERE region = ? AND check_id = 'A002' AND NOT undone
    """,
            [region],
        )
        .fetchdf()["reach_id"]
        .tolist()
    )
    all_reviewed_a002 = set(reviewed_a002) | set(st.session_state.a002_pending)

    # Auto-run check
    if (
        st.session_state.a002_results is None
        or st.session_state.get("a002_region") != region
    ):
        with st.spinner("Running A002 slope check..."):
            st.session_state.a002_results = check_slope_reasonableness(conn, region)
            st.session_state.a002_region = region

    result = st.session_state.a002_results
    if result is None or len(result.details) == 0:
        st.success("No slope issues in this region!")
    else:
        issues = result.details
        total = len(issues)
        remaining = len(
            [r for r in issues["reach_id"].tolist() if r not in all_reviewed_a002]
        )
        total_reviewed = len(all_reviewed_a002)
        pct_done = (total_reviewed / total * 100) if total > 0 else 0

        st.markdown(
            f"### Progress: **{pct_done:.1f}%** ({total_reviewed} of {total} reviewed)"
        )
        st.progress(total_reviewed / total if total > 0 else 0)

        col1, col2 = st.columns(2)
        col1.metric("Remaining", remaining)
        col2.metric("Reviewed & Saved", total_reviewed)

        st.info(
            "**Note:** Slope corrections are complex. This tab is triage-only: classify each issue, don't fix values directly."
        )

        if remaining == 0:
            st.success("All slope issues reviewed!")
            if st.button("Re-run check", key="rerun_a002"):
                st.session_state.a002_results = None
                st.session_state.a002_pending = []
                st.rerun()
        else:
            available = [
                r for r in issues["reach_id"].tolist() if r not in all_reviewed_a002
            ]
            selected = available[0]
            issue = issues[issues["reach_id"] == selected].iloc[0]

            issue_type = issue["issue_type"]
            slope_val = issue["slope"]

            # Problem box
            st.markdown("---")
            st.subheader(f"Issue #{total_reviewed + 1}: Reach `{selected}`")

            if issue_type == "negative":
                st.error(f"""
                **PROBLEM:** This reach has **negative slope ({slope_val:.6f})**

                Negative slope means the river would flow uphill - this is a data error.
                Common cause: DEM artifacts, bridge overpasses, or bad elevation data.
                """)
            else:
                st.error(f"""
                **PROBLEM:** This reach has **extremely high slope ({slope_val:.4f} m/km)**

                Very steep slopes are unusual for rivers. Could be a waterfall,
                dam spillway, or elevation data error.
                """)

            col1, col2 = st.columns([2, 1])

            with col1:
                render_reach_map_satellite(int(selected), region, conn)

            with col2:
                st.markdown("### Details")
                st.markdown(f"**River:** {issue['river_name'] or 'Unnamed'}")
                st.markdown(f"**Slope:** {slope_val:.6f}")
                st.markdown(
                    f"**Reach length:** {issue['reach_length']:.0f}m"
                    if pd.notna(issue["reach_length"])
                    else ""
                )
                st.markdown(f"**Issue type:** {issue_type}")

                st.markdown("---")
                st.markdown("### Classification")

                if st.button(
                    "DEM Artifact (expected)",
                    key=f"a002_artifact_{selected}",
                    type="primary",
                    use_container_width=True,
                ):
                    log_skip(
                        conn,
                        selected,
                        region,
                        "A002",
                        f"DEM artifact - {issue_type} slope",
                    )
                    st.session_state.a002_pending.append(selected)
                    st.cache_data.clear()
                    st.rerun()

                if st.button(
                    "Looks wrong (flag for review)",
                    key=f"a002_wrong_{selected}",
                    use_container_width=True,
                ):
                    log_skip(
                        conn,
                        selected,
                        region,
                        "A002",
                        f"Flagged: slope looks wrong - {issue_type}",
                    )
                    st.session_state.a002_pending.append(selected)
                    st.cache_data.clear()
                    st.rerun()

                if st.button(
                    "Skip (unsure)",
                    key=f"a002_skip_{selected}",
                    use_container_width=True,
                ):
                    log_skip(
                        conn,
                        selected,
                        region,
                        "A002",
                        f"Skipped: unsure about {issue_type} slope",
                    )
                    st.session_state.a002_pending.append(selected)
                    st.cache_data.clear()
                    st.rerun()

    # Refresh
    st.markdown("---")
    if st.button("Refresh Check", key="refresh_a002"):
        st.session_state.a002_results = None
        st.session_state.a002_pending = []
        st.rerun()

# Footer
st.divider()
st.caption(f"SWORD Reviewer | Region: {region} | DB: sword_v17c.duckdb")
