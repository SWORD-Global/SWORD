"""Pfafstetter continent code offsets for globally unique v17c IDs.

Reach IDs encode the continent in their first digit (Pfafstetter level 1).
We use this same digit as a multiplier to create non-overlapping ID bands
for per-region sequential variables (main_path_id, subnetwork_id, path_segs).

    AF reach_ids start with 1  ->  offset 1_000_000
    EU reach_ids start with 2  ->  offset 2_000_000
    AS reach_ids start with 3  ->  offset 3_000_000  (also digit 4)
    OC reach_ids start with 5  ->  offset 5_000_000
    SA reach_ids start with 6  ->  offset 6_000_000
    NA reach_ids start with 7  ->  offset 7_000_000  (also digits 8, 9)

Max per-region count is ~18,634 (path_segs in AS), so 1M bands have ~50x headroom.
"""

# Canonical Pfafstetter continent code per region (minimum first digit).
PFAF_CONTINENT_CODE: dict[str, int] = {
    "AF": 1,
    "EU": 2,
    "AS": 3,
    "OC": 5,
    "SA": 6,
    "NA": 7,
}


def pfaf_offset(region: str) -> int:
    """Return the Pfafstetter-based offset for a region (e.g. 'NA' -> 7_000_000)."""
    key = region.upper()
    if key not in PFAF_CONTINENT_CODE:
        raise ValueError(
            f"Unknown region {region!r}, expected one of {sorted(PFAF_CONTINENT_CODE)}"
        )
    return PFAF_CONTINENT_CODE[key] * 1_000_000


def compute_subnetwork_ids(G, region=None):
    """Compute subnetwork_id for each node via weakly connected components.

    Parameters
    ----------
    G : nx.DiGraph or nx.MultiDiGraph
        Directed graph.
    region : str, optional
        Region code (e.g. 'NA'). When provided, IDs are offset by the
        Pfafstetter continent code for global uniqueness.

    Returns
    -------
    dict
        Mapping of node -> subnetwork_id (int).
    """
    import networkx as nx

    offset = pfaf_offset(region) if region else 0
    result = {}
    for idx, component in enumerate(nx.weakly_connected_components(G), start=1):
        sid = offset + idx
        for node in component:
            result[node] = sid
    return result
