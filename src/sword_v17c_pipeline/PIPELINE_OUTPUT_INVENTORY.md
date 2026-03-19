# v17c Pipeline Output Inventory (Legacy)

> **Note:** This documents outputs from the **old** MILP-based pipeline.
> The new v17c_pipeline.py writes directly to `sword_v17c.duckdb` instead of these files.
> See `README.md` for current pipeline documentation.

**Generated:** 2026-01-27
**Location:** `/Users/jakegearon/projects/sword_v17c/output/`
**Total Size:** ~12.5 GB
**Pipeline Run Date:** 2025-11-20 (old pipeline)

## Summary

| Region | Edges GPKG | Nodes GPKG | SWOT Parquet | Complete |
|--------|------------|------------|--------------|----------|
| AF | 122 MB | 4.9 MB | 529 MB | Yes |
| AS | 488 MB | 23 MB | 3.5 GB | Yes |
| EU | 136 MB | 7.1 MB | 892 MB | Yes |
| NA | 195 MB | 8.8 MB | 1.3 GB | Yes |
| OC | 76 MB | 3.5 MB | 244 MB | Yes |
| SA | 235 MB | 9.6 MB | 891 MB | Yes |
| **Global** | 1.2 GB | 57 MB | - | Yes |

---

## Directory Structure

```
output/
├── {region}_network_edges.gpkg     # Step 1: Initial edge network
├── {region}_network_nodes.gpkg     # Step 1: Initial node network
├── {region}_MultiDirected.pkl      # Step 1: NetworkX MultiDiGraph
├── {region}_slope.pkl              # Step 2: Slope calculations (all)
├── {region}_slope_single.pkl       # Step 2: Slope calculations (single)
├── global_edges.gpkg               # Combined global edges (1.2 GB)
├── global_nodes.gpkg               # Combined global nodes (57 MB)
├── logs/                           # Pipeline execution logs
└── {region}/                       # Per-region refined outputs
    ├── {region}_MultiDirected_refined.pkl
    ├── {region}_network_edges.gpkg
    ├── {region}_network_nodes.gpkg
    ├── {region}_swot_nodes.parquet
    ├── {region}_swot_slopes.csv
    ├── {region}.pkl
    ├── global_solver_report.txt
    ├── r_direction_conflicts.csv
    ├── refine_report.txt
    ├── river_directed.pkl
    ├── river_directed_refined.pkl
    ├── river_edges.csv
    ├── river_edges.gpkg
    ├── river_edges_refined.csv
    ├── river_edges_refined.gpkg
    ├── river_nodes.csv
    ├── river_nodes.gpkg
    ├── river_nodes_refined.csv
    └── river_nodes_refined.gpkg
```

---

## File Types Reference

### Step 1: Graph Construction (`SWORD_graph.py`)

| File | Description | Format |
|------|-------------|--------|
| `{region}_network_edges.gpkg` | Initial edge geometries with reach attributes | GeoPackage |
| `{region}_network_nodes.gpkg` | Initial node geometries at junctions | GeoPackage |
| `{region}_MultiDirected.pkl` | NetworkX MultiDiGraph (bidirectional edges) | Pickle |

### Step 2: Slope Calculation (`SWOT_slopes.py`)

| File | Description | Format |
|------|-------------|--------|
| `{region}_slope.pkl` | All slope calculations per edge | Pickle |
| `{region}_slope_single.pkl` | Single (best) slope per edge | Pickle |

### Step 3-4: Phi Algorithm (`phi_only_global.py`, `phi_r_global_refine.py`)

| File | Description | Format |
|------|-------------|--------|
| `{region}/river_directed.pkl` | Initial flow direction assignment | Pickle |
| `{region}/river_directed_refined.pkl` | Refined flow direction | Pickle |
| `{region}/{region}_MultiDirected_refined.pkl` | Refined NetworkX graph | Pickle |
| `{region}/global_solver_report.txt` | Solver convergence report | Text |
| `{region}/refine_report.txt` | Refinement statistics | Text |
| `{region}/r_direction_conflicts.csv` | Direction conflicts (empty = good) | CSV |

### Step 5: Attribute Assignment (`assign_attribute.py`)

| File | Description | Format |
|------|-------------|--------|
| `{region}/{region}_network_edges.gpkg` | Final edges with all v17c attributes | GeoPackage |
| `{region}/{region}_network_nodes.gpkg` | Final nodes with all v17c attributes | GeoPackage |
| `{region}/river_edges.gpkg` | River-only subset edges | GeoPackage |
| `{region}/river_nodes.gpkg` | River-only subset nodes | GeoPackage |
| `{region}/river_edges_refined.gpkg` | Refined river edges | GeoPackage |
| `{region}/river_nodes_refined.gpkg` | Refined river nodes | GeoPackage |
| `{region}/{region}.pkl` | Final NetworkX graph | Pickle |

### SWOT Data

| File | Description | Format |
|------|-------------|--------|
| `{region}/{region}_swot_nodes.parquet` | SWOT observations per node | Parquet |
| `{region}/{region}_swot_slopes.csv` | SWOT-derived slopes | CSV |

### Global Combined

| File | Size | Description |
|------|------|-------------|
| `global_edges.gpkg` | 1.2 GB | All 6 regions combined edges |
| `global_nodes.gpkg` | 57 MB | All 6 regions combined nodes |

---

## Per-Region File Sizes

### Africa (AF)
| File | Size |
|------|------|
| af_network_edges.gpkg | 119 MB |
| af_network_nodes.gpkg | 3.9 MB |
| af_MultiDirected.pkl | 11 MB |
| af_slope.pkl | 847 KB |
| af/af_network_edges.gpkg | 122 MB |
| af/af_network_nodes.gpkg | 4.9 MB |
| af/af_swot_nodes.parquet | 529 MB |
| af/af_swot_slopes.csv | 674 KB |
| af/refine_report.txt | 8.7 KB |

### Asia (AS)
| File | Size |
|------|------|
| as_network_edges.gpkg | 475 MB |
| as_network_nodes.gpkg | 18 MB |
| as_MultiDirected.pkl | 52 MB |
| as_slope.pkl | 4.9 MB |
| as/as_network_edges.gpkg | 488 MB |
| as/as_network_nodes.gpkg | 23 MB |
| as/as_swot_nodes.parquet | 3.5 GB |
| as/as_swot_slopes.csv | 4.1 MB |
| as/refine_report.txt | 33 KB |

### Europe (EU)
| File | Size |
|------|------|
| eu_network_edges.gpkg | 132 MB |
| eu_network_nodes.gpkg | 5.6 MB |
| eu_MultiDirected.pkl | 16 MB |
| eu_slope.pkl | 1.1 MB |
| eu/eu_network_edges.gpkg | 136 MB |
| eu/eu_network_nodes.gpkg | 7.1 MB |
| eu/eu_swot_nodes.parquet | 892 MB |
| eu/eu_swot_slopes.csv | 896 KB |
| eu/refine_report.txt | 17 KB |

### North America (NA)
| File | Size |
|------|------|
| na_network_edges.gpkg | 190 MB |
| na_network_nodes.gpkg | 7.0 MB |
| na_MultiDirected.pkl | 20 MB |
| na_slope.pkl | 1.7 MB |
| na/na_network_edges.gpkg | 195 MB |
| na/na_network_nodes.gpkg | 8.8 MB |
| na/na_swot_nodes.parquet | 1.3 GB |
| na/na_swot_slopes.csv | 1.3 MB |
| na/refine_report.txt | 22 KB |

### Oceania (OC)
| File | Size |
|------|------|
| oc_network_edges.gpkg | 74 MB |
| oc_network_nodes.gpkg | 2.8 MB |
| oc_MultiDirected.pkl | 7.8 MB |
| oc_slope.pkl | 808 KB |
| oc/oc_network_edges.gpkg | 76 MB |
| oc/oc_network_nodes.gpkg | 3.5 MB |
| oc/oc_swot_nodes.parquet | 244 MB |
| oc/oc_swot_slopes.csv | 548 KB |
| oc/refine_report.txt | 26 KB |

### South America (SA)
| File | Size |
|------|------|
| sa_network_edges.gpkg | 229 MB |
| sa_network_nodes.gpkg | 7.6 MB |
| sa_MultiDirected.pkl | 22 MB |
| sa_slope.pkl | 1.9 MB |
| sa/sa_network_edges.gpkg | 235 MB |
| sa/sa_network_nodes.gpkg | 9.6 MB |
| sa/sa_swot_nodes.parquet | 891 MB |
| sa/sa_swot_slopes.csv | 1.5 MB |
| sa/refine_report.txt | 9.3 KB |

---

## Pipeline Logs

**Final successful run:** `all_continents_2025-11-20_11-35-05.log` (19 MB)

77 log files total from development runs (Nov 17-20, 2025).

---

## New Attributes (v17c)

The pipeline adds these columns to reaches:

| Attribute | Description |
|-----------|-------------|
| best_headwater | Optimal headwater reach ID |
| best_outlet | Optimal outlet reach ID |
| is_mainstem | Boolean: on mainstem path |
| rch_id_up_main | Upstream mainstem reach ID |
| rch_id_dn_main | Downstream mainstem reach ID |
| pathlen_hw | Path length to headwater (reaches) |
| pathlen_out | Path length to outlet (reaches) |
| hydro_dist_out | Hydrological distance to outlet (m) |
| hydro_dist_hw | Hydrological distance to headwater (m) |
| subnetwork_id | Connected component ID |
| main_path_id | Mainstem path identifier |

---

## Verification Status

- [x] All 6 regions processed
- [x] Global combined files generated
- [x] SWOT data integrated
- [x] Direction conflicts: **0** (all `r_direction_conflicts.csv` files are empty)
- [ ] Attributes imported to sword_v17c.duckdb (pending)
- [ ] Validation against v17b (pending)

---

## Input Data

**Source:** `/Users/jakegearon/projects/sword_v17c/data/`

| File | Size | Description |
|------|------|-------------|
| af_sword_reaches_v17b.gpkg | 122 MB | AF reaches input |
| af_sword_nodes_v17b.gpkg | 271 MB | AF nodes input |
| as_sword_reaches_v17b.gpkg | 483 MB | AS reaches input |
| as_sword_nodes_v17b.gpkg | 1.05 GB | AS nodes input |
| eu_sword_reaches_v17b.gpkg | 134 MB | EU reaches input |
| eu_sword_nodes_v17b.gpkg | 284 MB | EU nodes input |
| na_sword_reaches_v17b.gpkg | 193 MB | NA reaches input |
| na_sword_nodes_v17b.gpkg | 420 MB | NA nodes input |
| oc_sword_reaches_v17b.gpkg | 76 MB | OC reaches input |
| oc_sword_nodes_v17b.gpkg | 166 MB | OC nodes input |
| sa_sword_reaches_v17b.gpkg | 233 MB | SA reaches input |
| sa_sword_nodes_v17b.gpkg | 519 MB | SA nodes input |
