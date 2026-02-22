# Validation Spec: GRADES Discharge Variables

> **Status: REMOVED in v17c.** `h_variance` and `w_variance` were dropped from the reaches table. The remaining 19 GRADES columns were never populated and were never included in DuckDB. This spec is retained for historical reference only.

## Summary

All 21 GRADES-related columns from the original NetCDF schema have been removed or were never imported into DuckDB. The two that were populated (`h_variance`, `w_variance`) were dropped in v17c as they duplicated `wse_var` and `width_var` respectively and were not used by any pipeline code.

## Variables

### Breakpoints (8 columns - ALL EMPTY)

| Variable | Type | Description |
|----------|------|-------------|
| h_break_1 | DOUBLE | Height breakpoint 1 for rating curve |
| h_break_2 | DOUBLE | Height breakpoint 2 |
| h_break_3 | DOUBLE | Height breakpoint 3 |
| h_break_4 | DOUBLE | Height breakpoint 4 |
| w_break_1 | DOUBLE | Width breakpoint 1 |
| w_break_2 | DOUBLE | Width breakpoint 2 |
| w_break_3 | DOUBLE | Width breakpoint 3 |
| w_break_4 | DOUBLE | Width breakpoint 4 |

**Status:** 0% coverage - schema exists but never populated

### Variance/Covariance (5 columns - 2 populated)

| Variable | Type | Coverage | Description |
|----------|------|----------|-------------|
| h_variance | DOUBLE | **100%** | Height (WSE) variance |
| w_variance | DOUBLE | **100%** | Width variance |
| hw_covariance | DOUBLE | 0% | Height-width covariance (EMPTY) |
| h_err_stdev | DOUBLE | 0% | Height error std dev (EMPTY) |
| w_err_stdev | DOUBLE | 0% | Width error std dev (EMPTY) |

### Other (8 columns - ALL EMPTY)

| Variable | Type | Coverage | Description |
|----------|------|----------|-------------|
| h_w_nobs | DOUBLE | 0% | Number of height-width observations |
| med_flow_area | DOUBLE | 0% | Median flow area |
| fit_coeffs_1_1 | DOUBLE | 0% | Rating curve coefficient set 1, param 1 |
| fit_coeffs_1_2 | DOUBLE | 0% | Rating curve coefficient set 1, param 2 |
| fit_coeffs_1_3 | DOUBLE | 0% | Rating curve coefficient set 1, param 3 |
| fit_coeffs_2_1 | DOUBLE | 0% | Rating curve coefficient set 2, param 1 |
| fit_coeffs_2_2 | DOUBLE | 0% | Rating curve coefficient set 2, param 2 |
| fit_coeffs_2_3 | DOUBLE | 0% | Rating curve coefficient set 2, param 3 |

## Source Dataset

**GRADES (Global Reach-level A priori Discharge Estimates)**

Reference: Hagemann et al. (2017)

These columns were intended to store rating curve parameters for converting height/width observations to discharge estimates. The breakpoints define piecewise linear relationships.

## Deep Audit Results (2026-02-02)

### Coverage

| Variable | Non-NULL | Percentage |
|----------|----------|------------|
| h_break_1-4 | 0 | 0.0% |
| w_break_1-4 | 0 | 0.0% |
| h_variance | 248,673 | 100.0% |
| w_variance | 248,673 | 100.0% |
| hw_covariance | 0 | 0.0% |
| h_err_stdev | 0 | 0.0% |
| w_err_stdev | 0 | 0.0% |
| h_w_nobs | 0 | 0.0% |
| med_flow_area | 0 | 0.0% |
| fit_coeffs_* (all 6) | 0 | 0.0% |

### Distribution (Populated Variables Only)

#### h_variance

| Statistic | Value |
|-----------|-------|
| Min | ~0 |
| P25 | 0.00 |
| Median | 0.47 |
| P75 | 8.22 |
| Max | 2,439,757.98 |
| Mean | 318.75 |

**Note:** Extreme max value (2.4M) indicates potential outliers.

#### w_variance

| Statistic | Value |
|-----------|-------|
| Min | ~0 |
| P25 | 264.80 |
| Median | 773.38 |
| P75 | 4,972.99 |
| Max | 1,083,616,079.96 |
| Mean | 434,151.42 |

**WARNING:** Extreme max value (1+ billion) indicates severe outliers. This warrants investigation.

### Anomaly Checks

| Check | Count |
|-------|-------|
| h_variance < 0 | 0 ✅ |
| w_variance < 0 | 0 ✅ |

## Issues Found

### Issue 1: 19 Empty Columns

19 of 21 GRADES-related columns contain no data. These columns exist in the schema but were never populated from the original GRADES dataset.

**Recommendation:**
- Document as intentionally empty (future use) OR
- Remove from schema to reduce confusion

### Issue 2: Extreme w_variance Outliers

Maximum w_variance is 1+ billion, while median is only 773. This extreme skew suggests:
- Data quality issues in source
- Unit conversion errors
- Needs outlier capping or quality flag

**Query to investigate:**
```sql
SELECT reach_id, region, width, w_variance
FROM reaches
WHERE w_variance > 1000000
ORDER BY w_variance DESC
LIMIT 20
```

## Proposed Lint Checks

| ID | Severity | Rule |
|----|----------|------|
| A011 | WARNING | w_variance extreme outliers (>1e6) |
| A012 | INFO | Report empty GRADES column coverage |

## Recommendation

1. **Low priority** - these columns are mostly empty and don't affect core SWORD functionality
2. **Document** why columns are empty (GRADES data not integrated?)
3. **Investigate** w_variance outliers if these values are used downstream
4. **Consider** removing empty columns in v18 schema cleanup
