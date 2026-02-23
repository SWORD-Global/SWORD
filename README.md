<p align=”center”>
    <img src=”https://github.com/ealtenau/SWORD/blob/main/docs/figures%20/SWORD_Logo.png” width=”300”>
</p>

# SWOT River Database (SWORD)

The **SWO**T **R**iver **D**atabase (**SWORD**) is a global hydrological river network database containing **66.9M centerlines**, **11.1M nodes**, and **248.7K reaches** across 6 continental regions (NA, SA, EU, AF, AS, OC). SWORD defines the nodes and reaches that constitute [SWOT](https://swot.jpl.nasa.gov/) river vector data products.

This repository contains the tooling used to develop, validate, and update SWORD. The database has been re-engineered from its original NetCDF format into [DuckDB](https://duckdb.org/) (~10 GB) for fast analytical queries, with an optional [PostgreSQL/PostGIS](https://postgis.net/) backend for multi-user access and spatial indexing.

## Background

The [Surface Water and Ocean Topography (SWOT) satellite mission](https://swot.jpl.nasa.gov/), launched in December 2022, vastly expands observations of river water surface elevation (WSE), width, and slope [(Biancamaria et al., 2016)](https://link.springer.com/chapter/10.1007/978-3-319-32449-4_6). The SWOT mission provides river vector products in shapefile format for each overpass. To enable multitemporal analysis, SWOT reaches and nodes must be defined a priori so that observations can be consistently assigned. SWORD combines multiple global river- and satellite-related datasets (GRWL, MERIT Hydro, HydroBASINS, GRanD/GROD) to define high-resolution river nodes (200 m spacing) and reaches (~10 km) with attached hydrologic variables and a consistent topological system for global rivers 30 m wide and greater. Development of SWORD is detailed by [Altenau et al. (2021)](https://agupubs.onlinelibrary.wiley.com/doi/abs/10.1029/2021WR030054).

## Current Version: v17b

Before using SWORD, please read the [SWORD Product Description Document](https://drive.google.com/file/d/1_1qmuJhL_Yd6ThW2QE4gW0G1eHH_XAer/view?usp=sharing). For questions, email **sword.riverdb@gmail.com**. SWORD v17b is the official version for SWOT **Version D** [**RiverSP Vector Products**](https://podaac.jpl.nasa.gov/SWOT?tab=datasets-information&sections=about).

### Version History

**Version 17** (October 2024)
- Topological updates to ensure consistency
- Distance-from-outlet recalculation based on shortest paths between outlets and headwaters
- New variables: `path_freq`, `path_order`, `path_segs`, `main_side`, `stream_order`, `end_reach`, `network`
- Improved geometry for reach shapefiles
- Additional channels added for improved network connectivity
- New reach and node IDs reflecting improved topology
- Corrected node lengths to match reach lengths when summed

**Version 17b** (March 2025)
- Type change for 1,662 reaches and associated nodes globally, updating impacted Reach and Node IDs
- Corrections to reach/node lengths and distance-from-outlet for select reaches (<2% globally)

**Version 17c** (in progress)
- Dijkstra-based hydrological distances (`hydro_dist_out`, `hydro_dist_hw`)
- Width-prioritized endpoint selection (`best_headwater`, `best_outlet`)
- Mainstem identification (`is_mainstem_edge`, `main_path_id`)
- Main neighbor selection (`rch_id_up_main`, `rch_id_dn_main`)
- Junction-to-junction sections (`v17c_sections`)
- SWOT observation aggregations (`*_obs_mean/median/std/range`, `n_obs`)
- Flow accumulation quality flags (`facc_quality`)
- Connected component IDs (`subnetwork_id`)

## How to Download

- [**SWORD Explorer**](https://www.swordexplorer.com/) - explore and download the most current version
- [**Zenodo**](https://zenodo.org/records/15299138) - current and previous versions with DOI for citation

## Repository Structure

```
src/
  sword_duckdb/              # Core module
    workflow.py              #   SWORDWorkflow - main entry point
    sword_class.py           #   SWORD class - DuckDB-backed data access
    schema.py                #   Table definitions (DDL)
    reactive.py              #   Auto-recalculation of derived attributes
    reconstruction.py        #   35+ attribute reconstructors
    validation.py            #   Topology and attribute validation
    imagery/                 #   Satellite water detection (NDWI, ML4Floods, OPERA)
    lint/                    #   Lint framework (61 checks, 8 categories)
  sword_v17c_pipeline/       # v17b -> v17c topology enhancement pipeline
  _legacy/                   # Archived pre-DuckDB code

scripts/
  topology/                  # Topology recalculation
  maintenance/               # Database rebuild, import, setup
  analysis/                  # Comparison and analysis
  visualization/             # Visualization and presentations
  sql/                       # SQL utilities

deploy/
  reviewer/                  # Streamlit app for manual QA review

data/
  duckdb/
    sword_v17b.duckdb        # READ-ONLY reference baseline (9.9 GB)
    sword_v17c.duckdb        # Working database (11 GB)
  netcdf/                    # Legacy source files

tests/sword_duckdb/          # Test suite
```

## Getting Started

### Database Access

All programmatic access goes through `SWORDWorkflow`:

```python
from sword_duckdb import SWORDWorkflow

workflow = SWORDWorkflow(user_id=”jake”)
sword = workflow.load(“data/duckdb/sword_v17c.duckdb”, “NA”)

# Modify with provenance tracking
workflow.modify_reach(reach_id, wse=45.5, reason=”field correction”)

# Recalculate derived attributes
workflow.calculate_dist_out()
workflow.recalculate_stream_order()

# Query audit history
workflow.get_history(entity_type=”reach”, entity_id=123)

# Export
workflow.export(formats=[“geopackage”], output_dir=”outputs/”)
workflow.close()
```

### Lint Framework

61 automated checks across topology, attributes, flow accumulation, geometry, classification, v17c variables, flags, and network:

```bash
# Run all checks
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb

# Filter by region or category
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb --region NA
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb --checks T  # topology only

# Output as markdown or JSON
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb --format markdown -o report.md

# List all available checks
python -m src.sword_duckdb.lint.cli --list-checks
```

### v17c Pipeline

Computes v17c attributes from v17b topology:

```bash
# All regions
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all

# Single region
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --region NA
```

### Topology Reviewer

Streamlit app for manual QA review of reaches:

```bash
streamlit run deploy/reviewer/app.py       # Topology reviewer
streamlit run deploy/reviewer/lake_app.py  # Lake classification reviewer
```

## Database Schema

**Core tables:**
| Table | Primary Key | Description |
|-------|-------------|-------------|
| `centerlines` | `(cl_id, region)` | River path points (66.9M rows) |
| `nodes` | `(node_id, region)` | Measurement points at ~200 m intervals (11.1M rows) |
| `reaches` | `reach_id` | River segments between junctions (248.7K rows) |
| `reach_topology` | `(reach_id, direction, neighbor_rank)` | Upstream/downstream neighbors |

**Key reach attributes:**
| Attribute | Description |
|-----------|-------------|
| `dist_out` | Distance to outlet (m), decreases downstream |
| `facc` | Flow accumulation (km^2) from MERIT Hydro |
| `width` | River width (m) from GRWL |
| `wse` | Water surface elevation (m) |
| `slope` | Water surface slope (m/km) |
| `stream_order` | Log scale of `path_freq` |
| `lakeflag` | 0=river, 1=lake, 2=canal, 3=tidal |
| `type` | 1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost |
| `end_reach` | 0=middle, 1=headwater, 2=outlet, 3=junction |
| `network` | Connected component ID |

## Testing

```bash
python -m pytest tests/sword_duckdb/ -v
```

Test database: `tests/sword_duckdb/fixtures/sword_test_minimal.duckdb` (100 reaches, 500 nodes)

## Citations

- **Development publication:** Altenau, E. H., Pavelsky, T. M., Durand, M. T., Yang, X., Frasson, R. P. D. M., & Bendezu, L. (2021). The Surface Water and Ocean Topography (SWOT) Mission River Database (SWORD): A global river network for satellite data products. *Water Resources Research*, 57(7), e2021WR030054.
- **Database DOI:** Elizabeth H. Altenau, Tamlin M. Pavelsky, Michael T. Durand, Xiao Yang, Renato P. d. M. Frasson, & Liam Bendezu. (2025). SWOT River Database (SWORD) (Version v17b) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.15299138

---

![Fig1](https://github.com/ealtenau/SWORD/blob/main/docs/figures%20/global_map_dist_out_legend_basins_rch_numbers.png)
*SWORD reach numbers per continent. Colors display distance from outlet calculated from shortest paths between outlets and headwaters.*

![Fig2](https://github.com/ealtenau/SWORD/blob/main/docs/figures%20/global_map_routing_legend.png)
*Modified lumped routing results of accumulated reaches based on updated topology.*
