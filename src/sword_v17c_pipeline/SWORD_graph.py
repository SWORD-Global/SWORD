import networkx as nx
import geopandas as gpd
import pandas as pd
import numpy as np
import math
import os

import argparse


import itertools
# --- Imports ---

import pickle

# partial modules
from shapely.geometry import Point


# functionality moduldes
from tqdm import tqdm
from datetime import datetime as dt


# Plotting


def total_distance(coords, lines):
    p = Point(coords[0], coords[1])
    return sum(p.distance(line) for line in lines)


# ?????????????
def sync_node_attribute(G_target, G_source, attr_name):
    attr_dict = {n: d.get(attr_name) for n, d in G.nodes(data=True) if attr_name in d}
    nx.set_node_attributes(G_target, attr_dict, attr_name)


# -----------------------------
# Load data
# -----------------------------
def load_sword_data(
    data_dir: str = "data", continent="oc", include_nodes=False
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Load SWORD V17b reach and node data from geopackage files.

    Args:
        data_dir: Directory containing SWORD data files

    Returns:
        Tuple of (reaches_gdf, nodes_gdf)
    """
    # Load reaches data
    reaches_file = f"{data_dir}{continent}_sword_reaches_v17b.gpkg"
    print(
        f"[{dt.now().strftime('%H:%M:%S')}] Loading reaches from {reaches_file}...",
        flush=True,
    )
    reaches_gdf = gpd.read_file(reaches_file)
    print(
        f"[{dt.now().strftime('%H:%M:%S')}] Reaches loaded: {reaches_gdf.shape}",
        flush=True,
    )

    # Load nodes data
    nodes_file = f"{data_dir}{continent}_sword_nodes_v17b.gpkg"
    if include_nodes:
        print(
            f"[{dt.now().strftime('%H:%M:%S')}] Loading nodes from {nodes_file}...",
            flush=True,
        )
        nodes_gdf = gpd.read_file(nodes_file)
        print(
            f"[{dt.now().strftime('%H:%M:%S')}] Nodes loaded: {nodes_gdf.shape}",
            flush=True,
        )
    else:
        nodes_gdf = 1

    print(f"Reaches data shape ({continent}): {reaches_gdf.shape}")

    return reaches_gdf, nodes_gdf


# -----------------------------
# Change string ids to lists
# -----------------------------
def extract_list_id_from_str(rch_ids):
    """ChanGe str to list. No values are changed to an empty list"""
    if isinstance(rch_ids, list):
        return rch_ids
    rch_ids = str(rch_ids).strip()
    if (rch_ids != "nan") & (rch_ids != "None") & (len(rch_ids) > 0):
        id_list = list(map(float, rch_ids.split(" ")))
        id_list = [int(A) for A in id_list]
    else:
        id_list = []
    return id_list


def get_ids(df):
    # Apply the transformation once per direction, vectorized
    for s in ["up", "dn"]:
        df[f"rch_id_{s}"] = df[f"rch_id_{s}"].apply(extract_list_id_from_str)
    return df


# -----------------------------
# Graph node locations
# -----------------------------
def geometric_median(points, eps=1e-6, max_iter=200):
    """
    Weiszfeld algorithm for geometric median of a set of points (list of (x,y)).
    Returns (x,y).
    """
    pts = np.array(points, dtype=float)
    # start at centroid
    cur = pts.mean(axis=0)
    for _ in range(max_iter):
        diff = pts - cur
        d = np.linalg.norm(diff, axis=1)
        # if any point equals current, return that point
        if np.any(d == 0):
            return tuple(pts[np.argmin(d)])
        w = 1.0 / d
        new = (w[:, None] * pts).sum(axis=0) / w.sum()
        if np.linalg.norm(new - cur) <= eps:
            return tuple(new)
        cur = new
    return tuple(cur)


def find_node_location(
    lines,
    metric="sum",  # 'sum' or 'max' pairwise distance
    rep="geomed",  # 'centroid' or 'geomed' (geometric median)
    max_exact=16,
    large_method="greedy",
):
    """
    Select one endpoint per input LineString so that the chosen endpoints
    are as 'close together' as possible.

    Parameters
    ----------
    lines : list of shapely.geometry.LineString
        Input lines. Each line must have at least 2 coords.
    metric : {'sum', 'max'}
        Objective to minimize:
          'sum' = sum of pairwise distances
          'max' = maximum pairwise distance (minimize diameter)
    rep : {'centroid', 'geomed'}
        How to return representative node location for chosen endpoints:
          'centroid' -> arithmetic mean
          'geomed'   -> geometric median (reduces influence of outliers)
    max_exact : int
        Use exact 2^N search when N <= max_exact. For larger N use heuristic.
    large_method : {'greedy', 'local_search'}
        Heuristic to use for large N.

    Returns
    -------
    result : dict with keys:
        'point' : shapely.geometry.Point  -- representative node location
        'selected_points' : list of shapely.geometry.Point (one per line)
        'selection_indices' : list of 0/1 indicating which endpoint chosen per line
        'objective' : float  -- objective value for chosen combination
    """
    if len(lines) == 0:
        raise ValueError("No lines provided")

    N = len(lines)
    # Gather endpoints: endpoints[i] = [ (x0,y0), (x1,y1) ]
    endpoints = []
    for i, ln in enumerate(lines):
        coords = list(ln.coords)
        if len(coords) < 2:
            raise ValueError(f"Line {i} has fewer than 2 coordinates")
        p0 = coords[0]
        p1 = coords[-1]
        endpoints.append([p0, p1])

    def obj_for_points(arr):  # arr shape (k,2)
        if arr.shape[0] <= 1:
            return 0.0
        diff = arr[:, None, :] - arr[None, :, :]
        dmat = np.linalg.norm(diff, axis=2)
        iu = np.triu_indices(arr.shape[0], k=1)
        pairwise = dmat[iu]
        if metric == "sum":
            return float(pairwise.sum())
        else:  # 'max'
            return float(pairwise.max())

    def rep_point_from_coords(coords_array):
        if rep == "centroid":
            cx, cy = coords_array.mean(axis=0)
            return Point(cx, cy)
        else:
            gm = geometric_median(coords_array)
            return Point(gm[0], gm[1])

    # Exact search if small N
    if N <= max_exact:
        best_val = math.inf
        best_choice = None  # tuple of indices 0/1 length N
        # iterate all 2^N combinations
        for bits in itertools.product((0, 1), repeat=N):
            pts = np.array([endpoints[i][bits[i]] for i in range(N)], dtype=float)
            val = obj_for_points(pts)
            if val < best_val:
                best_val = val
                best_choice = bits
        chosen_pts = [Point(endpoints[i][best_choice[i]]) for i in range(N)]
        coords = np.array([[p.x, p.y] for p in chosen_pts], dtype=float)
        rep_point = rep_point_from_coords(coords)

        return rep_point
        # return {
        #     'point': rep_point,
        #     'selected_points': chosen_pts,
        #     'selection_indices': list(best_choice),
        #     'objective': best_val
        # }

    # Large N -> heuristic
    # Greedy: start by choosing endpoints closest to global centroid of all endpoints,
    # then try flipping each line's choice to reduce objective (local improvement).
    # This is fast and usually finds good clusters.
    all_pts = np.array([p for pair in endpoints for p in pair], dtype=float)
    global_centroid = all_pts.mean(axis=0)

    # initial choice: pick endpoint of each line closer to global centroid
    choice = []
    for pair in endpoints:
        d0 = np.linalg.norm(np.array(pair[0]) - global_centroid)
        d1 = np.linalg.norm(np.array(pair[1]) - global_centroid)
        choice.append(0 if d0 <= d1 else 1)
    choice = list(choice)

    def compute_obj_from_choice(choice):
        pts = np.array([endpoints[i][choice[i]] for i in range(N)], dtype=float)
        return obj_for_points(pts)

    cur_obj = compute_obj_from_choice(choice)

    # local improvement: try flipping each index, accept if improves
    improved = True
    iter_count = 0
    while improved and iter_count < 1000:
        improved = False
        iter_count += 1
        for i in range(N):
            choice[i] = 1 - choice[i]  # flip
            new_obj = compute_obj_from_choice(choice)
            if new_obj < cur_obj - 1e-12:
                cur_obj = new_obj
                improved = True
            else:
                choice[i] = 1 - choice[i]  # revert

    chosen_pts = [Point(endpoints[i][choice[i]]) for i in range(N)]
    coords = np.array([[p.x, p.y] for p in chosen_pts], dtype=float)
    rep_point = rep_point_from_coords(coords)
    # return {
    #     'point': rep_point,
    #     'selected_points': chosen_pts,
    #     'selection_indices': list(choice),
    #     'objective': cur_obj
    # }
    return rep_point


# -----------------------------
# Create network nodes
# -----------------------------
def headOutletLoc(target, reference):
    L1 = df.loc[df["reach_id"] == target, "geometry"].iloc[0]
    L2 = df.loc[df["reach_id"] == reference, "geometry"].iloc[0]

    LP = [Point(L1.coords[0]), Point(L1.coords[-1])]

    # Compute distances from endpoints to line2
    distances = [pt.distance(L2) for pt in LP]

    # Pick the endpoint with max distance
    furthest_endpoint = LP[distances.index(max(distances))]

    # print("Furthest endpoint:", furthest_endpoint, "distance:", max(distances))
    return furthest_endpoint


def add_network_node(G, nodeID, nt, upr, dnr, nc, bp, cycle=False):
    if nodeID not in G.nodes():
        G.add_node(
            nodeID,
            node_type=nt,  # change to junction if needed
            upstream_reach=upr,
            downstream_reach=dnr,
            num_connected_reaches=nc,
            x=bp.x,
            y=bp.y,
            cycle=cycle,
        )
    return G


def create_network_nodes(df, edgesDN):
    # check location of headwater and outlet --> not two lines present
    G = nx.MultiGraph()

    for i, row in df.iterrows():
        r = int(row["reach_id"])

        rdn = edgesDN.loc[edgesDN["reach_id"] == r, "rch_id_dn"].values
        up = [r]
        dn = []
        if isinstance(rdn[0], int):
            for rdi, rd in enumerate(rdn):
                dn.append(rd)

                # N.append(create_network_node_id(f'{rd}-{r}'))
                e = edgesUP.loc[edgesUP["reach_id"] == rd, "rch_id_up"].values
                for E in e:
                    # N.append(create_network_node_id(f'{rd}-{E}'))
                    if E not in up:
                        up.append(E)

        connReaches = up + dn
        sep = "-"
        nodeID = sep.join(map(str, sorted(connReaches)))
        # nodeID =  create_network_node_id(N)
        numCon = len(connReaches)

        if numCon == 1:
            node_type = "Outlet"
        elif numCon > 2:
            node_type = "Junction"
        else:
            node_type = "Connection"

        if len(row["rch_id_up"]) == 0:  # add node for headWater
            nodeID_head = f"{r}"
            node_type_head = "Head_water"
            bp = headOutletLoc(r, dn[0])
            G = add_network_node(G, nodeID_head, node_type_head, [], [r], 1, bp)

        # different lines for outlet and headwate
        if node_type == "Outlet":
            bp = headOutletLoc(r, row["rch_id_up"][0])
        else:
            lines = df.loc[df["reach_id"].isin(connReaches), "geometry"].values
            bp = find_node_location(lines, rep="centroid")
        G = add_network_node(G, nodeID, node_type, up, dn, numCon, bp)
    return G


# -----------------------------
# Create network path_segments
# -----------------------------
def network_sections(DG):
    """
    Identify all continuous directed paths between boundary nodes in a (Multi)DiGraph.
    Boundary nodes are defined as nodes with node_type != 'Connection'.

    Each section runs from one boundary node to another without crossing intermediate boundaries.
    Works with MultiDiGraph and handles parallel edges.

    Returns
    -------
    sections : list of dict
        Each item: {
            'path': [n1, n2, ..., nN],
            'edges': [(u, v, key), ...]
        }
    """
    # Define boundary (start/stop) nodes
    boundary_nodes = {
        n for n, d in DG.nodes(data=True) if d.get("node_type") != "Connection"
    }

    sections = []

    # For every boundary node, start exploring outward
    for start_node in boundary_nodes:
        # Iterate over all outgoing edges (including parallel ones)
        for _, next_node, key in DG.out_edges(start_node, keys=True):
            path_nodes = [start_node]
            path_edges = [(start_node, next_node, key)]
            current = next_node

            # Follow downstream until another boundary node or dead end
            while current not in boundary_nodes:
                path_nodes.append(current)

                successors = list(DG.successors(current))
                if len(successors) == 0:
                    # Reached a dead-end
                    break
                elif len(successors) > 1:
                    # Branching — stop here, each branch will be handled separately
                    break

                # Continue downstream (choose the single successor)
                next_node = successors[0]
                # Get all parallel edges between current → next_node
                for k in DG[current][next_node].keys():
                    path_edges.append((current, next_node, k))
                current = next_node

            # Stop section at boundary or end
            if current in boundary_nodes:
                path_nodes.append(current)

            sections.append({"path": path_nodes, "edges": path_edges})

    for i, s in enumerate(sections):
        for order, si in enumerate(s["edges"]):
            u, v, k = si
            DG[u][v][k]["path_seg"] = i
            DG[u][v][k]["path_seg_pos"] = order
            DG[u][v][k]["path_start_node"] = s["path"][0]
            DG[u][v][k]["path_end_node"] = s["path"][-1]

    return sections


# -----------------------------
# Create network edges
# -----------------------------
def create_edges(G, df, nodes_gdf):
    """
    Directed graph is main network graph
    - Undirected graph is used to indentify cycles in the network
    """

    DG = nx.MultiDiGraph(G)
    dnNodeGr = nodes_gdf.groupby("downstream_reach")
    upNodeGr = nodes_gdf.groupby("upstream_reach")
    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
        r = row["reach_id"]
        # dnNode = nodes_gdf.loc[nodes_gdf['downstream_reach'] == r, 'node_id']
        # upNode = nodes_gdf.loc[nodes_gdf['upstream_reach']   == r, 'node_id']
        dnNode = dnNodeGr.get_group(r)["node_id"]
        upNode = upNodeGr.get_group(r)["node_id"]

        if (dnNode.shape[0] == 0) | (upNode.shape[0] == 0):
            print(
                "Missing network node. Each reach should havea a upstream and downstream network node"
            )
        else:
            edge_attrs = row.to_dict()
            # edge_attrs = {'reach_id':r}
            if "geometry" in edge_attrs:
                del edge_attrs["geometry"]
            # edge_attrs['reach_id'] = reach_id

            G.add_edge(dnNode.iloc[0], upNode.iloc[0], **edge_attrs)
            DG.add_edge(dnNode.iloc[0], upNode.iloc[0], **edge_attrs)
    return G, DG


# -----------------------------
# Turn network into geopackage
# -----------------------------
def create_network_nodes_gdf(
    G, save=True, filename="", explode=False, directory="", continent=""
):
    df_nodes = pd.DataFrame.from_dict(
        dict(G.nodes(data=True)), orient="index"
    ).reset_index()
    df_nodes = df_nodes.rename(columns={"index": "node_id"})

    geometry = [Point(xy) for xy in zip(df_nodes["x"], df_nodes["y"])]
    gdf_nodes = gpd.GeoDataFrame(df_nodes, geometry=geometry)
    gdf_nodes.set_crs(epsg=4326, inplace=True)  # WGS84 lat/lon

    if save:
        assert filename, "To save file, filename cannot be an empty string"

        fileName = directory + f"/output/{filename}.gpkg"
        os.makedirs(os.path.dirname(fileName), exist_ok=True)
        gdf_nodes.to_file(fileName, driver="GPKG")

    if explode:
        gdf_nodes = gdf_nodes.explode("downstream_reach").reset_index(drop=True)
        gdf_nodes = gdf_nodes.explode("upstream_reach").reset_index(drop=True)
    return gdf_nodes


def create_edges_gdf(G, dfGeom, directory, filename="", save=True):

    edges_data = []
    updateFromNetwork = ["up_network_node", "dn_network_node", "parallel_path"]
    for u, v, k, attrs in G.edges(keys=True, data=True):
        edge_dict = {
            updateFromNetwork[0]: u,
            updateFromNetwork[1]: v,
            updateFromNetwork[2]: k,
        }
        # # Add selected edge attributes
        for attr in attrs:
            edge_dict[attr] = attrs.get(attr)
        edges_data.append(edge_dict)

    dfNew = pd.DataFrame(edges_data)
    if not all([elem in dfGeom.columns for elem in ["reach_id", "geometry"]]):
        raise TypeError(
            "Missing reach_id or geometry column. Needed for assigning gemoetries to network"
        )
    dfGeom = dfGeom[["reach_id", "geometry"]]
    dfNew = dfNew.merge(dfGeom, on="reach_id", how="left")
    dfNew = gpd.GeoDataFrame(dfNew, geometry="geometry", crs=dfGeom.crs)

    if save:
        assert filename, "To save file, filename cannot be an empty string"
        fileName = directory + f"/output/{filename}.gpkg"
        os.makedirs(os.path.dirname(fileName), exist_ok=True)
        dfNew.to_file(fileName, driver="GPKG")

    return dfNew


# def create_edges_gdf(G, df, edge_attrs=None, save = False, save_name = '', directory = '', continent = ''):
#     if edge_attrs is None:
#         edge_attrs = ['reach_id']
#     else:
#         if isinstance(edge_attrs, list):
#             edge_attrs.append('reach_id')
#         else:
#             edge_attrs = ['reach_id', edge_attrs]

#     edges_data = []
#     newCols = ['up_network_node', 'dn_network_node', 'parallel_path']
#     for u, v, k, attrs in G.edges(keys=True, data=True):
#         edge_dict = {
#             newCols[0]: u,
#             newCols[1]: v,
#             newCols[2]: k
#         }
#         # Add selected edge attributes
#         for attr in edge_attrs:
#             edge_dict[attr] = attrs.get(attr)
#         edges_data.append(edge_dict)

#     edges_gdf = pd.DataFrame(edges_data)

#     checkCols = edge_attrs + newCols
#     for ea in checkCols:
#         if (ea != 'reach_id') & (ea in df.columns):
#             df.drop(ea, axis = 1, inplace=True)

#     dfEdge    = df.copy().merge(edges_gdf, how = 'left', on = 'reach_id')
#     if save == True:
#         if save_name == '':
#             fileName = directory + f'output/{continent}_network_edges.gpkg'
#         else:
#             fileName = directory + f'output/{continent}_network_edges_{save_name}.gpkg'
#         dfEdge.to_file(fileName, driver = 'GPKG')
#     return dfEdge


# -----------------------------
# Turn network into geopackage
# -----------------------------
def add_network_id(DG, continent=None):
    """Assign subnetwork_id to all nodes/edges based on weakly connected components.

    Parameters
    ----------
    DG : nx.MultiDiGraph
        Directed graph.
    continent : str, optional
        Region code (e.g. 'NA'). When provided, IDs are offset by the
        Pfafstetter continent code so they are globally unique across regions.
    """
    from pfaf_offsets import compute_subnetwork_ids

    node_ids = compute_subnetwork_ids(DG, region=continent)

    # Apply to nodes
    for node, sid in node_ids.items():
        DG.nodes[node]["subnetwork_id"] = sid

    # Apply to edges
    for u, v, key in DG.edges(keys=True):
        DG.edges[u, v, key]["subnetwork_id"] = node_ids.get(u, 0)


# -----------------------------
# Check for network cycles
# -----------------------------
def get_cycles(G: nx.MultiGraph, DG: nx.MultiDiGraph, edges_gdf):
    cycles = nx.cycle_basis(nx.Graph(G))
    singe_cycles = (
        edges_gdf.loc[
            edges_gdf["parallel_path"] > 0, ["up_network_node", "dn_network_node"]
        ]
        .to_numpy()
        .tolist()
    )  # algorithm does not detect cycles from two edges
    cycles = cycles + singe_cycles

    cyclesUnique = np.unique([c for cycle in cycles for c in cycle])
    for cycle in cyclesUnique:
        G.nodes[cycle]["cycle"] = True
        DG.nodes[cycle]["cycle"] = True

    return cycles


# -----------------------------
# Classify junctions
# -----------------------------
def find_junctions(G, allow_wrong_junctions=False):
    """
    Find junction nodes in a MultiDiGraph based on degree criteria.

    Parameters
    ----------
    G : nx.MultiDiGraph (Directed multigraph).

    Returns
    -------
    node_type count per type.
    Inplace update of input G with node_type:
    - Head_water: 0 in, 1 out
    - Outlet: 1 in, 0 out
    - Junction: >1 in, >1 out
    - Connection: 1 in, 1 out
    - Bi_inflow: >2 in, 0 out
    - Bi_outflow: 0 in, >2 out
    - No_connection: 0 in, 0 out
    """
    if not isinstance(G, nx.MultiDiGraph):
        raise TypeError("G must be a MultiDiGraph")

    for n in G.nodes():
        indeg = G.in_degree(n)
        outdeg = G.out_degree(n)

        if (indeg == 0) & (outdeg == 1):  # headwater
            G.nodes[n]["node_type"] = "Head_water"
        elif (indeg == 1) & (outdeg == 0):  # outlet
            G.nodes[n]["node_type"] = "Outlet"
        elif (indeg != 0) & (outdeg >= 2) | (indeg >= 2) & (outdeg != 0):  # junction
            G.nodes[n]["node_type"] = "Junction"
        elif (indeg == 1) & (outdeg == 1):  # Connectection
            G.nodes[n]["node_type"] = "Connection"
        elif (indeg >= 2) & (outdeg == 0):  # local inflow
            G.nodes[n]["node_type"] = "Bi_inflow"
        elif (indeg == 0) & (outdeg >= 2):  # local outflow
            G.nodes[n]["node_type"] = "Bi_outflow"
        else:  # lose canon
            G.nodes[n]["node_type"] = "No_connection"
    nodeTypes = [d.get("node_type") for n, d in G.nodes(data=True)]
    nodeTypes, nodeTypeCount = np.unique(nodeTypes, return_counts=True)

    if not allow_wrong_junctions:
        if len(set(nodeTypes) & set(["N_connection", "Bi_inflow", "Bi_outflow"])) > 0:
            raise TypeError(
                f"Wrong node connections in Graph: {nodeTypes}, {nodeTypeCount}"
            )

    print(*[f"{s}: {i}" for s, i in zip(nodeTypes, nodeTypeCount)], sep=", ")


# -----------------------------
# Classify only lakeflag sections
# -----------------------------
def classify_section_validity(edges_df, var1="type", var2="lakeflag"):
    """
    Returns a Series marking whether each section_id is valid (True/False).
    """

    def valid_section(sub, var1="type", var2="lakeflag"):
        has_var1_good = sub[var1].isin([1, 4, 5]).any()
        has_var2_good = sub[var2].isin([0, 2, 3]).any()
        return has_var1_good and has_var2_good

    return edges_df.groupby("path_seg")[[var1, var2]].apply(valid_section)


def remove_invalid_headwater_sections(G, edges_df, section_validity):
    """
    Removes edges that belong to invalid sections starting at headwaters.
    Returns updated G and edges_df.
    # wrong spur:
    # Ghost type: 72599200216, 81260900276

    # Errors in code!!! wrong edge and node identification!!!!
    # missing spur identification
    """
    invalid_sections = section_validity[~section_validity].index
    to_remove = []
    to_remove_net = []
    for sec in invalid_sections:
        sub_edges = edges_df[edges_df["path_seg"] == sec].sort_values("path_seg_pos")
        start_node = sub_edges.iloc[0]["path_start_node"]

        if G.nodes[start_node]["node_type"] == "Head_water":
            to_remove.extend(
                list(zip(sub_edges["path_start_node"], sub_edges["path_end_node"]))
            )
            to_remove_net.extend(
                sub_edges[["up_network_node", "dn_network_node"]].values.tolist()
            )
    # print(to_remove)
    # Remove from graph and dataframe
    G.remove_edges_from(to_remove_net)
    edges_df = edges_df[
        ~edges_df.apply(
            lambda r: (r["path_start_node"], r["path_end_node"]) in to_remove, axis=1
        )
    ]

    isolated_nodes = list(nx.isolates(G))
    G.remove_nodes_from(isolated_nodes)
    return G, edges_df


def iterative_section_cleaning(G, edges_df, classify_nodes_func, max_iter=10):
    """
    Iteratively remove invalid headwater sections and reclassify node types
    until no further removals are needed.
    """
    for i in range(max_iter):
        section_validity = classify_section_validity(edges_df)
        invalid_sections = section_validity[~section_validity]

        if invalid_sections.empty:
            print(f"✅ No invalid sections remaining after {i} iteration(s).")
            break

        edge_count_before = G.number_of_edges()
        G, edges_df = remove_invalid_headwater_sections(G, edges_df, section_validity)

        # Reclassify node types using your function
        classify_nodes_func(G, True)

        if G.number_of_edges() == edge_count_before:
            print(f"⚠️ No further removals after {i} iteration(s). Stopping.")
            break

    return G, edges_df


def classify_lakeflagged_sections(G, df):
    # Remove lake reaches
    GRL = G.copy()
    dfGL = df.copy()

    GRL, dfGL = iterative_section_cleaning(
        GRL, dfGL, classify_nodes_func=find_junctions
    )
    removed_edges = set(G.edges(keys=True)) - set(GRL.edges(keys=True))
    for u, v, k in removed_edges:
        G[u][v][k]["only_lake_section"] = True


# -----------------------------
# Manuall edits to SWORD
# -----------------------------
def manual_edits(continent, df_input):
    # Normalize continent to uppercase for consistent comparison
    continent = continent.upper()
    if continent == "NA":
        T1 = 83150800071
        newDn = 83150800061
        df_input.loc[df_input["reach_id"] == T1, "rch_id_dn"] = f"{newDn}"
        df_input.loc[df_input["reach_id"] == T1, "rch_id_dn"]

        T2 = 83150800061
        df_input.loc[df_input["reach_id"] == T2, "rch_id_up"] = (
            f"{83150800071} {83150800231}"
        )
        df_input = df_input[df_input["reach_id"] != 83150800221]
    if continent == "SA":
        df_input.loc[df_input["reach_id"] == 62294500751, "rch_id_up"] = (
            f"{62294500761}"
        )
        df_input.loc[df_input["reach_id"] == 62294500761, "rch_id_dn"] = (
            f"{62294500751}"
        )
        df_input.loc[df_input["reach_id"] == 62294500791, "rch_id_dn"] = (
            f"{62294500781}"
        )
    return df_input


# -----------------------------
# main
# -----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--directory", required=True)
    ap.add_argument("--continent", required=True)
    args = ap.parse_args()

    directory = args.directory
    continent = args.continent

    # Add output folder if it does not exist
    folder_path = (
        directory + "/output"
    )  # Define the path to the folder you want to create
    os.makedirs(folder_path, exist_ok=True)

    # Check if data folder exists: Should contain SWORD data and SWOT data
    folder_path = directory + "/data/"
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Folder '{folder_path}' does not exist.")

    df_input, _ = load_sword_data(
        data_dir=directory + "/data/", continent=continent, include_nodes=False
    )

    ################################################################
    # manual SWORD edits

    df_input = manual_edits(continent, df_input)
    df = df_input.copy()

    ################################################################
    # Change str down and upstream reach identification to list with int
    print("Change down and upstream reach identification to list")
    df = get_ids(df)

    ################################################################
    # Create list with a rows of up and downstream reach identification. Multiple connections mean multiple rows
    print("Create exploded dataframes for up and downstream reach connections")
    edgesDN = df.explode("rch_id_dn").reset_index(drop=True)[["reach_id", "rch_id_dn"]]
    edgesUP = df.explode("rch_id_up").reset_index(drop=True)[["reach_id", "rch_id_up"]]

    ################################################################
    # Create network nodes based on downstream connections
    print("Create network nodes")
    G = create_network_nodes(df, edgesDN)

    ################################################################
    # turn nodes into a exploded dataframe
    nodes_gdf = create_network_nodes_gdf(G, save=False, explode=True)
    nodes_gdf.head()

    ################################################################
    # Add edges to network nodes and create directed and undirected graph. All reach information is linked to network edges
    print("Add edges")
    G, DG = create_edges(G, df.copy(), nodes_gdf)

    # Add network information to sword reach info
    dfEdge = create_edges_gdf(G, df.copy(), save=False, directory="")

    ################################################################
    # Add network id to nodes and edges in network
    add_network_id(DG, continent=continent)

    ################################################################
    # Identify cycles in network. Single (two reaches) connection cycles are not identified based on network algorithm. Can only be identified in undirected graph
    print("Identify network cycles")
    cycles = get_cycles(G, DG, dfEdge)

    ################################################################
    # redefine connection types
    find_junctions(DG)

    ################################################################
    # Indentify sections
    sections = network_sections(DG)

    ################################################################
    # Indentify lakeflagged sections
    dfEdge = create_edges_gdf(
        DG,
        dfEdge,
        directory=directory,
        save=False,
    )
    classify_lakeflagged_sections(DG, dfEdge)

    ################################################################
    # turn network into node and edge dataframe
    nodes_gdf = create_network_nodes_gdf(
        DG,
        save=True,
        explode=False,
        directory=directory,
        continent=continent,
        filename=f"{continent}_network_nodes",
    )
    dfEdge = create_edges_gdf(
        DG,
        dfEdge,
        save=True,
        directory=directory,
        filename=f"{continent}_network_edges",
    )

    output_path = directory + f"/output/{continent}_MultiDirected.pkl"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        pickle.dump(DG, f)
