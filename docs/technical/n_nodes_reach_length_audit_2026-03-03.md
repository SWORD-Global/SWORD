# n_nodes + reach_length Audit (2026-03-03)

## Scope

Audited this variable set on `sword_v17c.duckdb`:

- `n_nodes`
- `reach_length`

## Checks Run

Command:

```bash
uv run python -m src.sword_duckdb.lint.cli \
  --db /Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb \
  --checks N008 G001 G002 G003 G005 G022 \
  --format json \
  --output /tmp/n_nodes_reach_length_audit_2026-03-03.json
```

## Results Summary

| Check | Status | Issues | Notes |
|---|---|---:|---|
| N008 `node_count_vs_n_nodes` | PASS | 0 | `reaches.n_nodes` matches actual node counts |
| G001 `reach_length_bounds` | FAIL (INFO) | 329 | `<100m` non-headwater reaches; no `>50km` reaches |
| G002 `node_length_consistency` | PASS | 0 | `SUM(node_length)` matches `reach_length` within 10% |
| G003 `zero_length_reaches` | PASS | 0 | No NULL/zero/negative `reach_length` |
| G005 `reach_length_vs_geom_length` | FAIL (WARNING) | 1,396 | `reach_length` differs from geodesic geometry length by >20% |
| G022 `single_node_reaches` | FAIL (INFO) | 1,996 | Non-ghost/non-dam reaches with `n_nodes=1` |

## Key Findings

1. `n_nodes` integrity is strong:
   - No NULL/zero/negative values
   - Min/median/max: `1 / 51 / 100`
   - N008 mismatch count: `0`

2. `reach_length` attribute integrity is strong:
   - No NULL/nodata/nonpositive values
   - No `>50km` outliers
   - G002: `0` violations

3. `G001` short-reach findings are fully single-node:
   - All 329 `G001` records have `n_nodes=1`
   - Regional counts: `NA 112`, `AS 69`, `EU 63`, `SA 63`, `AF 14`, `OC 8`

4. Main risk is geometry drift, not attribute drift:
   - v17b vs v17c: `n_nodes` unchanged for all reaches; `reach_length` unchanged for all reaches
   - But `G005 >20%` is `0` in v17b and `1,396` in v17c
   - This points to v17c geometry differences relative to stored `reach_length`

## Supporting Diagnostics

- v17c reach count: `248,673`
- `n_nodes=1` total: `25,732` (of these, `1,996` are non-ghost/non-dam)
- `G005 >20%` by region:
  - `AS 473`
  - `EU 307`
  - `NA 252`
  - `SA 219`
  - `AF 91`
  - `OC 54`

## Interpretation

- `n_nodes` and `reach_length` columns themselves are internally consistent and stable versus v17b.
- Current audit signal is concentrated in geometry-to-attribute agreement (`G005`), not count/length column corruption.
- `G001` short-reach volume is dominated by single-node segments and classification context (`end_reach`) rather than raw length data failure.

## Decision

Status for this audit scope is **accepted for v17c**:

1. `n_nodes` and `reach_length` integrity checks (`N008/G002/G003`) are clean.
2. `G005` is documented as geometry-convention drift (endpoint overlap vertices in `reaches.geom`), not length/count corruption.
3. `G001` and `G022` are informational QA flags (short/single-node segments), not blockers for this audit.

## POM Relation

This specific finding is **not a POM request check**. It is from the geometry lint suite (`G*`) and is tracked in the technical audit docs.
