# v17c Path Topology Audit (2026-03-03)

## Scope

Audited this variable set on `sword_v17c.duckdb`:

- `best_headwater`
- `best_outlet`
- `rch_id_up_main`
- `rch_id_dn_main`
- `main_path_id`

## Checks Run

Command:

```bash
uv run python -m src.sword_duckdb.lint.cli \
  --db /Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb \
  --checks V007 V008 V010 V012 V013 V014 V015 \
  --format json \
  --output /tmp/v17c_path_topology_audit_expanded.json
```

## Results Summary

| Check | Status | Issues | Notes |
|---|---|---:|---|
| V007 `best_headwater_validity` | FAIL | 80 | `best_headwater` points to non-headwater (`n_rch_up > 0`) |
| V008 `best_outlet_validity` | PASS | 0 | No invalid `best_outlet` found |
| V010 `main_connection_integrity` | PASS | 0 | No FK/topology/self-reference issues for `rch_id_*_main` |
| V012 `main_connection_null_semantics` | FAIL | 12 | Non-headwater reaches with NULL `rch_id_up_main` |
| V013 `main_path_id_global_uniqueness` | FAIL | 500 | Global cross-region ID collisions |
| V014 `main_path_id_region_consistency` | FAIL | 2,324 | `(region, main_path_id)` maps to multiple `(best_headwater, best_outlet)` tuples |
| V015 `tuple_to_main_path_id_uniqueness` | FAIL | 3,134 | `(region, best_headwater, best_outlet)` maps to multiple `main_path_id` values |

## Regional Breakdown

### V007 (`best_headwater_validity`)

| Region | Count |
|---|---:|
| NA | 80 |

### V012 (`main_connection_null_semantics`)

Issue type observed:

- `non_headwater_missing_up_main` in NA only (12 reaches)

### V014 (`main_path_id_region_consistency`)

| Region | Bad `main_path_id` groups |
|---|---:|
| AS | 974 |
| NA | 374 |
| SA | 336 |
| EU | 266 |
| OC | 210 |
| AF | 164 |

### V015 (`tuple_to_main_path_id_uniqueness`)

| Region | Bad tuple groups |
|---|---:|
| AS | 1,234 |
| NA | 607 |
| SA | 474 |
| EU | 390 |
| OC | 221 |
| AF | 208 |

## Interpretation

- `rch_id_up_main` / `rch_id_dn_main` integrity is strong (V010 pass, no structural reference breakage).
- The primary breakage is path grouping consistency (`main_path_id` vs `(best_headwater, best_outlet)`).
- The NA-only V007 and V012 findings suggest localized reroute or partial recomputation artifacts.

## Next Action

1. Recompute `main_path_id` from current `(best_headwater, best_outlet)` for all regions, then re-run `V013/V014/V015`.
2. Repair the 12 NA reaches with NULL `rch_id_up_main` where `n_rch_up > 0`, then re-run `V012`.
3. Triage the NA `best_headwater` chain (80 reaches) to determine whether source assignment or topology update is stale.
