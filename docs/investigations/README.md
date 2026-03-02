# Investigation Archive

Artifacts from closed investigation issues. Each subdirectory corresponds to a GitHub issue or topic. Files here are historical records — do not delete.

## Index

### n007/ — Boundary node geolocation gaps (#189, closed)

31 extreme topology gaps (>5km between connected reach endpoints). 28 inherited from v17b, 3 from v17c pipeline. Root causes: lake shoreline gaps, ghost reaches, sparse centerlines, cross-basin links.

| File | Description |
|------|-------------|
| `n007_extreme_31_investigation.md` | Full investigation writeup |
| `n007_extreme_31_diagnostic.csv` | 31 reach pairs with gap metrics, types, categories |
| `n007_extreme_27_unc.gpkg` | GeoPackage of 27 UNC-inherited gaps for QGIS review |

### flow-verification/ — WSE inversions and reversed sections (#195, #198, closed)

Automated flow-direction verification across all 6 regions. Built `flow_verification.py` module scoring reversed sections via DEM WSE, SWOT WSE (Theil-Sen), facc monotonicity, junction slopes. Result: only 20/2,032 sections worth flipping (~1% of 4,816 WSE inversions). Most inversions are MERIT DEM noise, not reversed edges.

| File | Description |
|------|-------------|
| `ALL_FACC_DECREASES.csv` | 5,313 downstream facc decreases (all regions) |
| `REVERSED_VALIDATION_REPORT.csv` | 776 suspected reversed chains with confidence scores |
| `suspected_reversed_reaches.geojson` | GeoJSON of reversal candidates for QGIS |
| `HANDOVER_TOPOLOGY.md` | Session handover from topology healing work |
| `figures_NA/results.json` | NA region flow verification results |

### mass-conservation/ — POM mass conservation checks

Artifacts from Pierre-Olivier Malaterre's mass conservation analysis of SWORD.

| File | Description |
|------|-------------|
| `2026.2.18.pdf` | POM's mass conservation check report |
| `distance_errors.csv` | Distance computation errors flagged by POM |
| `metadata_count_errors.csv` | Metadata count discrepancies flagged by POM |
