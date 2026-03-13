<p align="center">
    <img src="docs/figures /SWORD_Logo.png" width="300">
</p>

# SWOT River Database (SWORD)

The **SWO**T **R**iver **D**atabase (**SWORD**) is a global hydrological river network database containing **248,673 reaches**, **11.1M nodes**, and **66.9M centerlines** across 6 continental regions (NA, SA, EU, AF, AS, OC). SWORD defines the nodes and reaches that constitute [SWOT](https://swot.jpl.nasa.gov/) river vector data products.

This repository contains the tooling for SWORD development, validation, and maintenance. The current production database is managed in [DuckDB](https://duckdb.org/) (~11 GB) for fast analytical performance, with support for [PostgreSQL/PostGIS](https://postgis.net/) backends.

## Current Version: v17c

SWORD v17c is the current stable version, merged to `main`. It maintains the topology of v17b while adding 56 new variables covering SWOT observation statistics, mainstem routing, Dijkstra-based distances, and corrected flow accumulation.

**Key Documentation:**
- [**v17c Release Notes**](docs/v17c_release_notes.md) - Summary of changes and new variables
- [**v17c Variable Reference**](docs/v17c_variable_reference.md) - Detailed variable descriptions and encodings
- [**SWORD Explorer**](https://www.swordexplorer.com/) - Web-based data access and visualization

## Project Structure

```
src/
  sword_duckdb/              # Core module
    workflow.py              #   SWORDWorkflow - main entry point
    sword_class.py           #   DuckDB-backed data access
    schema.py                #   Table definitions (DDL)
    reactive.py              #   Auto-recalculation of derived attributes
    validation.py            #   Topology and attribute validation
    lint/                    #   Lint framework (122 checks, 9 categories)
  sword_v17c_pipeline/       # v17b -> v17c enhancement pipeline

scripts/
  export/                    # NetCDF, GeoPackage, Parquet exporters
  maintenance/               # Database rebuild, import, setup
  topology/                  # Topology recalculation utilities
  analysis/                  # Comparison and diagnostic scripts
  visualization/             # Map and figure generation

deploy/
  reviewer/                  # Streamlit manual QA reviewer

tests/sword_duckdb/          # Test suite
```

## Getting Started

### Database Access

Primary access is through the `SWORDWorkflow` class:

```python
from sword_duckdb import SWORDWorkflow

workflow = SWORDWorkflow(user_id="jake")
sword = workflow.load("data/duckdb/sword_v17c.duckdb", "NA")

# Modify with provenance tracking
workflow.modify_reach(reach_id, wse=45.5, reason="manual correction")

# Recalculate derived attributes
workflow.calculate_dist_out()

# Export
workflow.export(formats=["geopackage"], output_dir="outputs/")
workflow.close()
```

### v17c Enhancement Pipeline

Computes new v17c attributes (mainstems, Dijkstra distances, etc.) from base topology:

```bash
# Process all regions
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all
```

### Lint Framework

122 automated checks across 9 categories (topology, attributes, facc, geometry, classification, v17c variables, flags, and network):

```bash
# Run all checks
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb

# Filter by region or check category (e.g., Topology)
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb --region NA --checks T
```

### Exporting Data

- **NetCDF:** `python scripts/export/export_netcdf.py` (Dedicated exporter for UNC/JPL distribution; ~2.5 min for all regions)
- **Other Formats:** `python scripts/maintenance/export_sword.py` (Supports GeoPackage, GeoParquet, and PostgreSQL)

### Manual QA Reviewer

Streamlit applications for manual verification of topology and classifications:

```bash
streamlit run deploy/reviewer/app.py       # Topology reviewer
streamlit run deploy/reviewer/lake_app.py  # Lake classification reviewer
```

## Testing

```bash
python -m pytest tests/sword_duckdb/ -v
```

## Citations

- **Development publication:** Altenau, E. H., Pavelsky, T. M., Durand, M. T., Yang, X., Frasson, R. P. D. M., & Bendezu, L. (2021). The Surface Water and Ocean Topography (SWOT) Mission River Database (SWORD): A global river network for satellite data products. *Water Resources Research*, 57(7), e2021WR030054.
- **Database DOI:** Elizabeth H. Altenau, Tamlin M. Pavelsky, Michael T. Durand, Xiao Yang, Renato P. d. M. Frasson, & Liam Bendezu. (2025). SWOT River Database (SWORD) (Version v17b) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.15299138

---

![Fig1](docs/figures /global_map_dist_out_legend_basins_rch_numbers.png)
*SWORD reach numbers per continent. Colors display distance from outlet calculated from shortest paths between outlets and headwaters.*
