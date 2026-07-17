<p align="center">
    <img src="docs/figures /SWORD_Logo.png" width="300">
</p>

# SWOT River Database (SWORD)

The **SWO**T **R**iver **D**atabase (**SWORD**) is a global hydrological river network database containing **248,673 reaches**, **11.1M nodes**, and **66.9M centerlines** across 6 continental regions (NA, SA, EU, AF, AS, OC). SWORD defines the nodes and reaches that constitute [SWOT](https://swot.jpl.nasa.gov/) river vector data products.

## Background

The [Surface Water and Ocean Topography (SWOT) satellite mission](https://swot.jpl.nasa.gov/), launched in December 2022, vastly expands observations of river water surface elevation (WSE), width, and slope [(Biancamaria et al., 2016)](https://link.springer.com/chapter/10.1007/978-3-319-32449-4_6). SWOT provides river vector products in shapefile format for each overpass. To enable multitemporal analysis, reaches and nodes must be defined a priori so that observations can be consistently assigned across passes. SWORD combines multiple global river and satellite datasets (GRWL, MERIT Hydro, HydroBASINS, GRanD/GROD) to define river nodes (~200 m spacing) and reaches (~10 km) with attached hydrologic variables and a consistent topology for global rivers 30 m wide and greater. SWORD is described by [Altenau et al. (2021)](https://agupubs.onlinelibrary.wiley.com/doi/abs/10.1029/2021WR030054).

## Current Version: v17c

For detailed variable descriptions, release notes, and downloads, see the [**Zenodo record**](https://doi.org/10.5281/zenodo.21415370). Before using SWORD, please also read the [SWORD Product Description Document](https://drive.google.com/file/d/1_1qmuJhL_Yd6ThW2QE4gW0G1eHH_XAer/view?usp=sharing). For questions, email **james.gearon@unc.edu**.

v17c preserves the topology and reach/node definitions of v17b and adds observation-based and routing variables:

- **SWOT observation statistics** — per-reach and per-node distributions (percentiles, range, MAD) of water surface elevation, width, and slope derived from actual SWOT passes.
- **Mainstem routing** — mainstem identification (`is_mainstem`, `main_path_id`) and main upstream/downstream neighbor selection (`rch_id_up_main`, `rch_id_dn_main`).
- **Hydrological distances** — Dijkstra shortest-path and mainstem distances to outlet (`dist_out_dijkstra`, `hydro_dist_out`) and width-prioritized headwater/outlet endpoints (`best_headwater`, `best_outlet`).
- **Corrected flow accumulation** — denoised `facc` at 95,880 reaches (38.6%) where inherited values were physically inconsistent.

All original v17b variables are retained. See the release notes for the full list.

**Documentation:**
- [**v17c Release Notes**](docs/v17c_release_notes.md) ([PDF](docs/v17c_release_notes.pdf)) — summary of changes and new variables
- [**v17c Variable Reference**](docs/v17c_variable_reference.md) ([PDF](docs/v17c_variable_reference.pdf)) — detailed variable descriptions and encodings

### Version History

**Version 17** (October 2024)
- Topological updates for consistency
- Distance-from-outlet recalculation from shortest paths between outlets and headwaters
- New variables: `path_freq`, `path_order`, `path_segs`, `main_side`, `stream_order`, `end_reach`, `network`
- Improved reach geometry; additional channels for connectivity; new reach and node IDs; corrected node lengths

**Version 17b** (March 2025)
- Type change for 1,662 reaches and associated nodes globally, updating impacted Reach and Node IDs
- Corrections to reach/node lengths and distance-from-outlet for select reaches (<2% globally)

**Version 17c** (2026)
- SWOT observation statistics for WSE, width, and slope
- Mainstem routing and main neighbor selection (`is_mainstem`, `main_path_id`, `rch_id_up_main`, `rch_id_dn_main`)
- Dijkstra and mainstem hydrological distances (`dist_out_dijkstra`, `hydro_dist_out`)
- Width-prioritized endpoints (`best_headwater`, `best_outlet`)
- Corrected flow accumulation (`facc`)

## How to Download

- [**SWORD Explorer**](https://www.swordexplorer.com/) — explore and download the current version interactively
- [**Zenodo**](https://doi.org/10.5281/zenodo.21415370) — versioned archive with a DOI for citation

Available formats: NetCDF, GeoPackage, ESRI Shapefile, GeoParquet, and DuckDB, per continent and as global merged files.

## Citation

Please cite the Zenodo record:

> James H. Gearon, Elizabeth H. Altenau, Tamlin M. Pavelsky, Michael T. Durand, Niek Collot d'Escury, Xiao Yang, Pierre-Olivier Malaterre, Renato P. d. M. Frasson, & Liam Bendezu. (2026). SWOT River Database (SWORD) (Version v17c) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.21415370

Alternatively, or in addition, cite the development publication:

> Altenau, E. H., Pavelsky, T. M., Durand, M. T., Yang, X., Frasson, R. P. D. M., & Bendezu, L. (2021). The Surface Water and Ocean Topography (SWOT) Mission River Database (SWORD): A global river network for satellite data products. *Water Resources Research*, 57(7), e2021WR030054. https://doi.org/10.1029/2021WR030054

Concept DOI (always resolves to the latest version): https://doi.org/10.5281/zenodo.3898569

## Development

This repository holds the code used to develop, validate, and maintain SWORD. See [`docs/`](docs/) for technical documentation.

---

![Fig1](docs/figures /global_map_dist_out_legend_basins_rch_numbers.png)
*SWORD reach numbers per continent. Colors display distance from outlet calculated from shortest paths between outlets and headwaters.*
