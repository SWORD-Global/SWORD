# Accepted SWORD v17c NetCDF export path

All supported entry points call `sword_duckdb.netcdf.export_region`. The old
maintenance and `save_nc()` writers have been removed. The published layout
is required for release files and collaborator candidates alike.

Use `SWORDWorkflow` from Python:

```python
from sword_duckdb import SWORDWorkflow

with SWORDWorkflow(user_id="jake") as workflow:
    workflow.load("data/duckdb/sword_v17c.duckdb", "AF")
    workflow.export(
        formats=["netcdf"],
        output_dir="output/netcdf_candidate",
        v17b_nc_dir="data/netcdf",
    )
```

For a full six-region export, the existing release CLI uses the same writer
and opens the source DuckDB read-only:

```bash
python scripts/export/export_netcdf.py \
    --db data/duckdb/sword_v17c.duckdb \
    --v17b-dir data/netcdf \
    --output-dir output/netcdf_candidate --all
```

The maintenance CLI's `--format netcdf` delegates to this implementation too;
its template argument is `--v17b-nc-dir`. Both CLIs support `--overwrite` for
replacing a previous validated output. `SWORD.save_nc()` also delegates and
requires `v17b_nc_dir`; it no longer writes a partial layout. There is no
option to omit original subgroups.

Each requested region requires its original `{region}_sword_v17b.nc`. A batch
preflights all requested templates before writing anything. Required original
variables and subgroups must be present. The source templates are read-only;
they supply daily ice flags, centerline membership and the original model
subgroups, whose full content is not stored in DuckDB.

Exports go to a temporary file in the destination directory. Before that file
replaces the accepted destination, the writer checks the independent published
schema and exercises setfinder's dimension and variable access. Exceptions
close and remove the partial file and leave any previous accepted file intact.
Files record the schema ID and writer-source SHA-256 in their global metadata.

Validate files produced by any external overlay or candidate-building code
before distributing them:

```bash
python scripts/export/validate_netcdf.py output/netcdf_candidate/*_sword_v17c.nc
```

This reports JSON and returns a nonzero exit status if any file fails. Checks
cover every existing published variable path, named dimension, fixed dimension
size, dtype, fill value and subgroup. New variables are allowed. A previous
candidate's preservation checks cannot substitute for this contract check.

The Zenodo bundle assembler runs this same gate before cloning staged files
and again on the cloned files before creating ZIPs. External overlays therefore
cannot bypass the contract simply by entering the packaging directory.

The frozen contract is `src/sword_duckdb/netcdf_v17c_schema.json`, captured
from the SHA-256-verified November 1 public release, Zenodo record 23870877.
Its variable entries distinguish original v17b fields from v17c additions so
the same contract can preflight original templates and validate final files.
It records the source templates' local `area_fits` dimension declarations
separately from their parent-group declarations in the final published layout.
The schema is independent of the writer's specifications.

CI tests all six regions using small synthetic inputs, all Python/CLI entry
points, the historical layout failures, missing sources, and preservation of
accepted files on errors. It also downloads the unchanged setfinder reader at
commit `d0b30ca7276ebf01137521828152f4632ec0c822`, verifies its SHA-256, and runs its
actual extraction method on all six synthetic exports. Normal exports have no
network dependency on setfinder. This extraction check does not replace a
complete Confluence/Unity run.

## Moving from the October 27 collaborator candidate

Use the six November 1 public NetCDFs together. They contain the updated
drainage-area results and the published NetCDF structure. The October candidate
inherited a different beta layout: renamed orbit/neighbor dimensions, missing
node/reach `cl_ids`, scalar reach ice flags and 1-D centerline membership. The
AF/AS/SA files additionally lacked `area_fits` and `discharge_models`.

Renaming dimensions alone leaves those missing variables and wrong array
shapes unresolved. The public files restore them. The candidate and release
also use different entity ordering; compare data by reach/node ID.

The public release has the four core FACC fields. Its `facc_quality` uses int32
codes 3001–3008 with plain source names in `flag_meanings`; the candidate stored
prefixed strings and five additional diagnostic fields. If those extra fields
are needed, add them to a copy of the canonical layout and validate that new
candidate before distribution.

Tracked in [issue #243](https://github.com/SWORD-Global/SWORD/issues/243).
