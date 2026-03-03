# v17c River Name Audit (2026-03-03)

## Scope

Audited river name attributes on `sword_v17c.duckdb`:

- `river_name` (original SWORD name)
- `river_name_local` (OSM-derived local name)

## Checks Run

Command:

```bash
uv run python -m src.sword_duckdb.lint.cli \
  --db /Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb \
  --checks T019 T020 V011 A011 A012 A013 A014 \
  --format json \
  --output /Users/jakegearon/.gemini/tmp/audit-next-gemini/river_name_audit.json
```

## Results Summary (Post-Fix)

| Check | Severity | Status | Total Checked | Issues | % | Description |
|---|---|---|---:|---:|---:|---|
| T019 | info | ✅ PASS | 248,673 | 127,401 | 51.2% | Reaches with `river_name='NODATA'` |
| T020 | info | ℹ️ INFO | 121,272 | 197 | 0.16% | Name mismatch with all neighbors |
| V011 | warning | ⚠️ FAIL | 153,109 | 3,945 | 2.58% | `river_name_local` change on 1:1 mainstem link |
| A011 | warning | ✅ PASS | 121,272 | 0 | 0.00% | Non-standard separators (FIXED) |
| A012 | warning | ✅ PASS | 2,191 | 0 | 0.00% | Alphabetical ordering of multi-names |
| A013 | error | ✅ PASS | 248,673 | 0 | 0.00% | ASCII-only characters |
| A014 | warning | ✅ PASS | 248,673 | 0 | 0.00% | Leading/trailing/redundant whitespace (FIXED) |

## Regional Breakdown (Original T019)

| Region | Reaches | % Unnamed |
|---|---:|---:|
| OC | 10,477 | 69.4% |
| SA | 25,328 | 60.1% |
| EU | 17,743 | 57.1% |
| NA | 19,872 | 51.4% |
| AS | 48,465 | 48.4% |
| AF | 5,516 | 25.7% |

## Interpretation

- **Unnamed Reaches (T019):** High percentage of unnamed reaches is expected in many regions, but OC and SA are particularly sparse. AF has significantly better naming coverage in the base SWORD dataset compared to other regions.
- **Local Name Continuity (V011):** The 2.5% failure rate for OSM names on 1:1 links indicates places where the OSM data or the assignment logic has localized discontinuities. Since this flags only 1:1 links (not junctions), these are likely "bad" name transitions where a single physical river segment is split into multiple name attributes in OSM.
- **Quality & Standardisation:** With the implementation of `A011`-`A014` and subsequent database-wide fixes, we now have automated enforcement of SWORD's naming standards (semicolon separators, alphabetical ordering, ASCII-only, trimmed whitespace). 280 reaches in `river_name` and 1,661 in `river_name_local` were corrected.

## Next Actions

1. Triage a sample of V011 violations in AS and EU to see if they are valid sub-reach name changes or errors in the OSM-to-SWORD mapping.
2. Investigate AF naming coverage to see if it's using a different source or if the dataset is just more mature there.
3. Consider if T020 (consensus) should be elevated to WARNING if the goal is zero local name flickering.
