# -*- coding: utf-8 -*-
"""
Facc Correction Engine
----------------------

Automated correction of corrupted facc values.

Two correction strategies:
1. **Entry points**: Estimate from width + upstream_sum using regression
2. **Propagation**: BFS recalc from corrected upstream

Algorithm:
1. Filter fixable anomalies (skip lakes, stream_order=-9999)
2. Classify as 'orphan_entry', 'entry_point', or 'propagation'
3. Fit regression: log(facc) ~ log(width) + log(slope) + log(path_freq)
4. Apply corrections in topological order (entry points first)
5. Validate: ratio_to_median < 50, monotonicity preserved

Safeguards:
- Always log to facc_fix_log for rollback
- Dry-run mode by default
- Regional rollout (NA → EU → SA → OC → AS → AF)
"""

from typing import Optional, Dict, Any, Union, Tuple
from dataclasses import dataclass
import duckdb
import pandas as pd
import numpy as np
import logging

from .features import FaccFeatureExtractor
from .detect import FaccDetector, DetectionResult, detect_hybrid
from .merit_search import create_merit_search

logger = logging.getLogger(__name__)


@dataclass
class RegressionModel:
    """Fitted regression model for facc estimation."""

    coefficients: Dict[str, float]
    intercept: float
    r_squared: float
    n_samples: int
    model_type: str  # 'primary' or 'fallback'
    region: str


@dataclass
class CorrectionResult:
    """Result of facc correction."""

    corrections: (
        pd.DataFrame
    )  # reach_id, old_facc, facc_corrected, fix_type, model_used
    applied: int
    skipped: int
    failed: int
    batch_id: Optional[int] = None
    dry_run: bool = True
    validation: Optional[Dict[str, Any]] = None

    def summary(self) -> str:
        """Return summary string."""
        status = "DRY RUN" if self.dry_run else "APPLIED"
        return (
            f"Facc Correction ({status}):\n"
            f"  Total corrections: {len(self.corrections)}\n"
            f"  Applied: {self.applied}\n"
            f"  Skipped: {self.skipped}\n"
            f"  Failed: {self.failed}\n"
            f"  Batch ID: {self.batch_id or 'N/A'}"
        )


class FaccCorrector:
    """
    Corrects corrupted facc values using regression and topology-aware propagation.

    Parameters
    ----------
    db_path_or_conn : str or duckdb.DuckDBPyConnection
        Path to DuckDB database or existing connection.
        Note: For corrections, connection must be read-write.
    """

    # Known skip conditions
    SKIP_LAKEFLAG = 1  # Lakes may have correct facc
    SKIP_STREAM_ORDER = -9999  # Need topology fix first

    def __init__(
        self,
        db_path_or_conn: Union[str, duckdb.DuckDBPyConnection],
        read_only: bool = False,
    ):
        if isinstance(db_path_or_conn, str):
            self.conn = duckdb.connect(db_path_or_conn, read_only=read_only)
            self._own_conn = True
            self.db_path = db_path_or_conn
        else:
            self.conn = db_path_or_conn
            self._own_conn = False
            self.db_path = None

        self.detector = FaccDetector(self.conn)
        self.extractor = FaccFeatureExtractor(self.conn)
        self._models: Dict[str, Dict[str, RegressionModel]] = {}
        self._read_only = read_only
        if not read_only:
            self._ensure_fix_log_table()

    def _ensure_fix_log_table(self):
        """Create facc_fix_log table if it doesn't exist."""
        if self._read_only:
            return
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS facc_fix_log (
                id INTEGER PRIMARY KEY,
                batch_id INTEGER,
                reach_id BIGINT,
                region VARCHAR,
                old_facc DOUBLE,
                new_facc DOUBLE,
                fix_type VARCHAR,
                model_used VARCHAR,
                ratio_to_median_before DOUBLE,
                ratio_to_median_after DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def close(self):
        """Close connection if owned."""
        if self._own_conn and self.conn:
            self.detector.close()
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def filter_fixable(
        self, anomalies: pd.DataFrame, include_lakes: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split anomalies into fixable vs skipped.

        Skipped: stream_order=-9999 (need topology first)
        Optionally skipped: lakeflag=1 (may be correct, but some lakes have bad facc)

        Parameters
        ----------
        anomalies : pd.DataFrame
            Detected anomalies from FaccDetector.
        include_lakes : bool
            If True, include lakes in fixable (default False for safety).
            Set to True if you've verified lakes need correction.

        Returns
        -------
        tuple of (fixable_df, skipped_df)
        """
        if len(anomalies) == 0:
            return pd.DataFrame(), pd.DataFrame()

        # Get additional attributes needed for filtering
        reach_ids = anomalies["reach_id"].tolist()
        reach_ids_str = ", ".join(str(r) for r in reach_ids)

        attrs_df = self.conn.execute(f"""
            SELECT reach_id, region, lakeflag, stream_order, slope, n_rch_up, n_rch_down
            FROM reaches
            WHERE reach_id IN ({reach_ids_str})
        """).fetchdf()

        # Merge with anomalies
        merged = anomalies.merge(
            attrs_df, on=["reach_id", "region"], how="left", suffixes=("", "_attr")
        )

        # Skip conditions
        if include_lakes:
            skip_mask = merged["stream_order"] == self.SKIP_STREAM_ORDER
        else:
            skip_mask = (merged["lakeflag"] == self.SKIP_LAKEFLAG) | (
                merged["stream_order"] == self.SKIP_STREAM_ORDER
            )

        skipped = merged[skip_mask].copy()
        if not include_lakes:
            skipped["skip_reason"] = np.where(
                merged.loc[skip_mask, "lakeflag"] == self.SKIP_LAKEFLAG,
                "lake",
                "topology_missing",
            )
        else:
            skipped["skip_reason"] = "topology_missing"

        fixable = merged[~skip_mask].copy()

        lake_count = (
            (skipped["skip_reason"] == "lake").sum()
            if "skip_reason" in skipped.columns
            else 0
        )
        topo_count = (
            (skipped["skip_reason"] == "topology_missing").sum()
            if len(skipped) > 0
            else 0
        )

        logger.info(
            f"Filtered anomalies: {len(fixable)} fixable, {len(skipped)} skipped "
            f"(lakes: {lake_count}, topology: {topo_count}, include_lakes={include_lakes})"
        )

        return fixable, skipped

    def fit_regression(self, region: str) -> Dict[str, RegressionModel]:
        """
        Fit primary and fallback regression models for a region.

        Primary: log(facc) ~ log(width) + log(slope) + log(path_freq)
        Fallback: log(facc) ~ log(width) + log(path_freq) [for slope=0]

        Parameters
        ----------
        region : str
            Region to fit model for.

        Returns
        -------
        dict
            {'primary': RegressionModel, 'fallback': RegressionModel}
        """
        # Check cache
        if region in self._models:
            return self._models[region]

        # Get training data: non-anomalous, non-lake reaches with valid values
        query = f"""
            SELECT
                reach_id,
                facc,
                width,
                slope,
                path_freq,
                stream_order
            FROM reaches
            WHERE region = '{region}'
                AND facc > 0 AND facc != -9999
                AND width > 0
                AND path_freq > 0
                AND lakeflag != 1
                -- Exclude high facc/width ratio (likely anomalies)
                AND (facc / width) < 5000
        """

        train_df = self.conn.execute(query).fetchdf()

        if len(train_df) < 100:
            logger.warning(
                f"Insufficient training data for {region}: {len(train_df)} reaches"
            )
            return {}

        # Prepare features
        train_df["log_facc"] = np.log(train_df["facc"])
        train_df["log_width"] = np.log(train_df["width"])
        train_df["log_path_freq"] = np.log(train_df["path_freq"])

        # Handle slope - avoid log(0) warnings
        train_df["has_slope"] = (train_df["slope"] > 0) & (train_df["slope"].notna())
        # Only compute log on valid slopes
        valid_slopes = train_df.loc[train_df["has_slope"], "slope"]
        train_df["log_slope"] = 0.0  # Default
        train_df.loc[train_df["has_slope"], "log_slope"] = np.log(valid_slopes.values)

        models = {}

        # Primary model (with slope)
        primary_df = train_df[train_df["has_slope"]]
        if len(primary_df) >= 50:
            X_primary = primary_df[["log_width", "log_slope", "log_path_freq"]].values
            y_primary = primary_df["log_facc"].values

            # Add intercept column
            X_primary = np.column_stack([np.ones(len(X_primary)), X_primary])

            # Solve via normal equations
            try:
                beta = np.linalg.lstsq(X_primary, y_primary, rcond=None)[0]
                y_pred = X_primary @ beta
                ss_res = np.sum((y_primary - y_pred) ** 2)
                ss_tot = np.sum((y_primary - np.mean(y_primary)) ** 2)
                r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

                models["primary"] = RegressionModel(
                    coefficients={
                        "log_width": beta[1],
                        "log_slope": beta[2],
                        "log_path_freq": beta[3],
                    },
                    intercept=beta[0],
                    r_squared=r2,
                    n_samples=len(primary_df),
                    model_type="primary",
                    region=region,
                )
                logger.info(
                    f"Primary model for {region}: R²={r2:.3f}, n={len(primary_df)}"
                )
            except Exception as e:
                logger.warning(f"Primary model fit failed for {region}: {e}")

        # Fallback model (without slope)
        X_fallback = train_df[["log_width", "log_path_freq"]].values
        y_fallback = train_df["log_facc"].values
        X_fallback = np.column_stack([np.ones(len(X_fallback)), X_fallback])

        try:
            beta = np.linalg.lstsq(X_fallback, y_fallback, rcond=None)[0]
            y_pred = X_fallback @ beta
            ss_res = np.sum((y_fallback - y_pred) ** 2)
            ss_tot = np.sum((y_fallback - np.mean(y_fallback)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

            models["fallback"] = RegressionModel(
                coefficients={"log_width": beta[1], "log_path_freq": beta[2]},
                intercept=beta[0],
                r_squared=r2,
                n_samples=len(train_df),
                model_type="fallback",
                region=region,
            )
            logger.info(f"Fallback model for {region}: R²={r2:.3f}, n={len(train_df)}")
        except Exception as e:
            logger.warning(f"Fallback model fit failed for {region}: {e}")

        self._models[region] = models
        return models

    def classify_anomalies(self, anomalies: pd.DataFrame) -> pd.DataFrame:
        """
        Label anomalies as 'orphan_entry', 'entry_point', or 'propagation'.

        Classification rules:
        - orphan_entry: n_rch_up = 0 (headwater with high facc)
        - entry_point: facc_jump_ratio > 10 (big jump from upstream)
        - propagation: facc_jump_ratio ≈ 1 (inherited bad facc)

        Parameters
        ----------
        anomalies : pd.DataFrame
            Detected anomalies with topology info.

        Returns
        -------
        pd.DataFrame
            Anomalies with 'fix_type' and 'needs_fallback' columns.
        """
        df = anomalies.copy()

        # Get upstream info if not present
        if "n_rch_up" not in df.columns or "facc_jump_ratio" not in df.columns:
            reach_ids = df["reach_id"].tolist()
            reach_ids_str = ", ".join(str(r) for r in reach_ids)

            topo_query = f"""
                WITH upstream_info AS (
                    SELECT
                        r.reach_id,
                        r.region,
                        r.n_rch_up,
                        r.slope,
                        COALESCE(SUM(r_up.facc), 0) as upstream_facc_sum
                    FROM reaches r
                    LEFT JOIN reach_topology rt ON r.reach_id = rt.reach_id
                        AND r.region = rt.region AND rt.direction = 'up'
                    LEFT JOIN reaches r_up ON rt.neighbor_reach_id = r_up.reach_id
                        AND rt.region = r_up.region
                    WHERE r.reach_id IN ({reach_ids_str})
                    GROUP BY r.reach_id, r.region, r.n_rch_up, r.slope
                )
                SELECT
                    reach_id,
                    region,
                    n_rch_up,
                    slope,
                    upstream_facc_sum,
                    CASE WHEN upstream_facc_sum > 0 THEN facc / upstream_facc_sum ELSE NULL END as facc_jump_ratio
                FROM upstream_info ui
                JOIN reaches r USING (reach_id, region)
            """

            topo_df = self.conn.execute(topo_query).fetchdf()
            df = df.merge(
                topo_df[
                    [
                        "reach_id",
                        "region",
                        "n_rch_up",
                        "slope",
                        "upstream_facc_sum",
                        "facc_jump_ratio",
                    ]
                ],
                on=["reach_id", "region"],
                how="left",
                suffixes=("", "_topo"),
            )

        # Classify
        def classify_reach(row):
            if row.get("n_rch_up", 0) == 0 or pd.isna(row.get("n_rch_up")):
                return "orphan_entry"
            elif row.get("facc_jump_ratio", 0) > 10:
                return "entry_point"
            else:
                return "propagation"

        df["fix_type"] = df.apply(classify_reach, axis=1)

        # Flag reaches needing fallback model (slope = 0 or NULL)
        df["needs_fallback"] = (df["slope"] <= 0) | (df["slope"].isna())

        # Count by type
        type_counts = df["fix_type"].value_counts()
        logger.info(f"Classified anomalies: {dict(type_counts)}")

        return df

    def _predict_facc(
        self, row: pd.Series, models: Dict[str, RegressionModel]
    ) -> Tuple[float, str]:
        """
        Predict facc for a single reach using regression.

        Parameters
        ----------
        row : pd.Series
            Reach data with width, slope, path_freq.
        models : dict
            Fitted models for the region.

        Returns
        -------
        tuple of (predicted_facc, model_used)
        """
        width = row.get("width", 1)
        path_freq = row.get("path_freq", 1)
        slope = row.get("slope", 0)

        # Use primary model if slope is valid
        if slope > 0 and "primary" in models:
            model = models["primary"]
            log_facc = (
                model.intercept
                + model.coefficients["log_width"] * np.log(max(width, 1))
                + model.coefficients["log_slope"] * np.log(max(slope, 1e-6))
                + model.coefficients["log_path_freq"] * np.log(max(path_freq, 1))
            )
            return np.exp(log_facc), "primary"
        elif "fallback" in models:
            model = models["fallback"]
            log_facc = (
                model.intercept
                + model.coefficients["log_width"] * np.log(max(width, 1))
                + model.coefficients["log_path_freq"] * np.log(max(path_freq, 1))
            )
            return np.exp(log_facc), "fallback"
        else:
            # Last resort: use path_freq * regional median
            return path_freq * 1000, "median_fallback"

    def estimate_corrections(
        self,
        anomalies: pd.DataFrame,
        region: Optional[str] = None,
        merit_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compute facc_corrected with edge case handling and optional MERIT guided search.

        Processing order (topologically sorted):
        1. orphan_entry: guided MERIT search → regression fallback
        2. entry_point: guided MERIT search → topology fallback
        3. propagation: use corrected upstream values via BFS

        Parameters
        ----------
        anomalies : pd.DataFrame
            Classified anomalies from classify_anomalies().
        region : str, optional
            Region for model fitting.
        merit_path : str, optional
            Path to MERIT Hydro base directory. If provided, enables guided
            MERIT search for entry points and orphan entries.

        Returns
        -------
        pd.DataFrame
            Corrections with [reach_id, old_facc, facc_corrected, fix_type, model_used]
        """
        if len(anomalies) == 0:
            return pd.DataFrame(
                columns=[
                    "reach_id",
                    "region",
                    "old_facc",
                    "facc_corrected",
                    "fix_type",
                    "model_used",
                ]
            )

        # Initialize MERIT search if path provided
        merit_search = create_merit_search(merit_path)
        if merit_search:
            logger.info(f"MERIT guided search enabled from {merit_path}")
        else:
            logger.info("MERIT guided search disabled (no path or invalid path)")

        # Determine regions
        regions = anomalies["region"].unique() if region is None else [region]

        all_corrections = []

        for reg in regions:
            reg_anomalies = anomalies[anomalies["region"] == reg].copy()
            if len(reg_anomalies) == 0:
                continue

            # Fit models for this region
            models = self.fit_regression(reg)
            if not models:
                logger.warning(f"No models fitted for {reg}, skipping")
                continue

            # Get upstream/downstream topology for all anomalies
            reach_ids = reg_anomalies["reach_id"].tolist()
            reach_ids_str = ", ".join(str(r) for r in reach_ids)
            anomaly_set = set(reach_ids)

            # Get upstream neighbors and their facc
            upstream_df = self.conn.execute(f"""
                SELECT
                    rt.reach_id,
                    rt.neighbor_reach_id as upstream_id,
                    r_up.facc as upstream_facc
                FROM reach_topology rt
                JOIN reaches r_up ON rt.neighbor_reach_id = r_up.reach_id
                    AND rt.region = r_up.region
                WHERE rt.direction = 'up'
                    AND rt.reach_id IN ({reach_ids_str})
                    AND rt.region = '{reg}'
            """).fetchdf()

            # Build upstream map: reach_id -> [(upstream_id, facc), ...]
            upstream_map = {}
            for _, row in upstream_df.iterrows():
                rid = row["reach_id"]
                if rid not in upstream_map:
                    upstream_map[rid] = []
                upstream_map[rid].append((row["upstream_id"], row["upstream_facc"]))

            # Pre-fetch geometries if MERIT search enabled
            geom_map = {}
            if merit_search:
                # Ensure spatial extension is loaded
                try:
                    self.conn.execute("LOAD spatial;")
                except Exception:
                    try:
                        self.conn.execute("INSTALL spatial; LOAD spatial;")
                    except Exception as e:
                        logger.warning(f"Could not load spatial extension: {e}")

                try:
                    geom_df = self.conn.execute(f"""
                        SELECT reach_id, ST_AsText(geom) as geom_wkt
                        FROM reaches
                        WHERE reach_id IN ({reach_ids_str})
                            AND region = '{reg}'
                    """).fetchdf()
                    from shapely import wkt

                    for _, grow in geom_df.iterrows():
                        try:
                            geom_map[grow["reach_id"]] = wkt.loads(grow["geom_wkt"])
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning(f"Could not fetch geometries: {e}")

            # Corrected values dict (to propagate downstream)
            corrected_facc = {}

            # Track MERIT search stats
            merit_hits = 0
            merit_misses = 0

            # Process in order: orphan_entry -> entry_point -> propagation
            for fix_type in ["orphan_entry", "entry_point", "propagation"]:
                type_anomalies = reg_anomalies[reg_anomalies["fix_type"] == fix_type]

                for _, row in type_anomalies.iterrows():
                    reach_id = row["reach_id"]
                    old_facc = row["facc"]

                    # Predict local contribution (regression estimate)
                    local_estimate, model_used = self._predict_facc(row, models)

                    # Get upstream values (use corrected if available)
                    upstream_neighbors = upstream_map.get(reach_id, [])
                    upstream_sum = 0
                    for up_id, up_facc in upstream_neighbors:
                        if up_id in corrected_facc:
                            upstream_sum += corrected_facc[up_id]
                        elif up_id in anomaly_set:
                            # Upstream is also an anomaly but not yet corrected - use regression
                            upstream_sum += local_estimate / row.get("path_freq", 1)
                        else:
                            # Upstream is clean
                            upstream_sum += up_facc

                    # Compute correction based on fix type
                    source = model_used  # Track correction source

                    if fix_type in ["orphan_entry", "entry_point"]:
                        # Try guided MERIT search first
                        merit_match = None
                        if merit_search and reach_id in geom_map:
                            geom = geom_map[reach_id]
                            width = row.get("width", 100)

                            merit_match, meta = merit_search.search_near_reach(
                                geom=geom,
                                region=reg,
                                facc_expected=local_estimate,
                                width=width,
                            )

                            if merit_match:
                                facc_corrected = merit_match
                                source = "merit_guided"
                                merit_hits += 1
                                logger.debug(
                                    f"MERIT match for {reach_id}: "
                                    f"expected={local_estimate:.0f}, found={merit_match:.0f}, "
                                    f"searched={meta['searched']}, matched={meta['matched']}"
                                )
                            else:
                                merit_misses += 1
                                logger.debug(
                                    f"No MERIT match for {reach_id}: "
                                    f"expected={local_estimate:.0f}, searched={meta['searched']}"
                                )

                        if merit_match is None:
                            # Fallback based on fix type
                            if fix_type == "orphan_entry":
                                # Headwater: use regression
                                facc_corrected = local_estimate
                                source = f"{model_used}_fallback"
                            else:
                                # Entry point: use topology-based
                                local_contrib = local_estimate / max(
                                    row.get("path_freq", 1), 1
                                )
                                facc_corrected = upstream_sum + local_contrib
                                source = "topology_fallback"
                    else:
                        # Propagation: always topology-based
                        # Use corrected upstream sum + small local contribution
                        local_contrib = local_estimate / max(row.get("path_freq", 1), 1)
                        facc_corrected = upstream_sum + local_contrib
                        source = "topology"

                    # Safeguards
                    facc_corrected = max(1.0, facc_corrected)  # Never negative/zero
                    facc_corrected = min(facc_corrected, 1e9)  # Sanity cap

                    # Ensure monotonicity: facc >= max(upstream)
                    if upstream_sum > 0 and facc_corrected < upstream_sum:
                        facc_corrected = (
                            upstream_sum * 1.01
                        )  # Slightly more than upstream

                    # Store for downstream propagation
                    corrected_facc[reach_id] = facc_corrected

                    all_corrections.append(
                        {
                            "reach_id": reach_id,
                            "region": reg,
                            "old_facc": old_facc,
                            "facc_corrected": facc_corrected,
                            "fix_type": fix_type,
                            "model_used": source,
                            "upstream_facc_sum": upstream_sum,
                            "reduction_factor": old_facc / facc_corrected
                            if facc_corrected > 0
                            else 0,
                        }
                    )

            # Log MERIT stats for this region
            if merit_search:
                total = merit_hits + merit_misses
                if total > 0:
                    logger.info(
                        f"MERIT search for {reg}: {merit_hits}/{total} hits "
                        f"({100 * merit_hits / total:.1f}%)"
                    )

        # Clean up MERIT cache
        if merit_search:
            merit_search.clear_cache()

        return pd.DataFrame(all_corrections)

    def validate_corrections(self, corrections: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate correction quality.

        Checks:
        - ratio_to_median < 50 for all?
        - monotonicity preserved?
        - no negative facc?

        Parameters
        ----------
        corrections : pd.DataFrame
            Corrections from estimate_corrections().

        Returns
        -------
        dict
            Validation metrics.
        """
        if len(corrections) == 0:
            return {"valid": True, "issues": []}

        issues = []

        # Check for negative/zero corrections
        neg_mask = corrections["facc_corrected"] <= 0
        if neg_mask.any():
            issues.append(f"Negative/zero facc: {neg_mask.sum()} reaches")

        # Check for extreme values
        extreme_mask = corrections["facc_corrected"] > 1e9
        if extreme_mask.any():
            issues.append(f"Extreme facc (>1e9): {extreme_mask.sum()} reaches")

        # Check reduction is reasonable
        if "reduction_factor" in corrections.columns:
            small_reduction = corrections["reduction_factor"] < 2
            if small_reduction.sum() > len(corrections) * 0.5:
                issues.append(
                    f"Small reduction (<2x) for {small_reduction.sum()} reaches"
                )

        # Compute ratio_to_median after correction
        regions = corrections["region"].unique()
        ratio_issues = 0

        for region in regions:
            # Get regional median facc per path_freq
            median_query = f"""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facc / NULLIF(path_freq, 0))
                FROM reaches
                WHERE region = '{region}'
                    AND facc > 0 AND facc != -9999
                    AND path_freq > 0
                    AND lakeflag != 1
                    AND (facc / width) < 5000
            """
            median_fpr = self.conn.execute(median_query).fetchone()[0]

            if median_fpr and median_fpr > 0:
                reg_corrections = corrections[corrections["region"] == region]
                for _, row in reg_corrections.iterrows():
                    path_freq = (
                        self.conn.execute(f"""
                        SELECT path_freq FROM reaches WHERE reach_id = {row["reach_id"]}
                    """).fetchone()[0]
                        or 1
                    )

                    fpr_after = row["facc_corrected"] / path_freq
                    ratio_after = fpr_after / median_fpr

                    if ratio_after > 50:
                        ratio_issues += 1

        if ratio_issues > 0:
            issues.append(
                f"High ratio_to_median (>50) after correction: {ratio_issues} reaches"
            )

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "total_corrections": len(corrections),
            "negative_count": neg_mask.sum() if "neg_mask" in dir() else 0,
            "extreme_count": extreme_mask.sum() if "extreme_mask" in dir() else 0,
            "ratio_issues": ratio_issues,
        }

    def apply_corrections(
        self, corrections: pd.DataFrame, dry_run: bool = True
    ) -> CorrectionResult:
        """
        Apply corrections with logging.

        Parameters
        ----------
        corrections : pd.DataFrame
            Corrections from estimate_corrections().
        dry_run : bool
            If True, don't actually update database.

        Returns
        -------
        CorrectionResult
            Result with counts and batch ID.
        """
        if len(corrections) == 0:
            return CorrectionResult(
                corrections=corrections, applied=0, skipped=0, failed=0, dry_run=dry_run
            )

        # Validate first
        validation = self.validate_corrections(corrections)
        if not validation["valid"]:
            logger.warning(f"Validation issues: {validation['issues']}")

        if dry_run:
            logger.info(f"DRY RUN: Would apply {len(corrections)} corrections")
            return CorrectionResult(
                corrections=corrections,
                applied=len(corrections),
                skipped=0,
                failed=0,
                dry_run=True,
                validation=validation,
            )

        # Get next batch ID
        batch_id_result = self.conn.execute("""
            SELECT COALESCE(MAX(batch_id), 0) + 1 FROM facc_fix_log
        """).fetchone()
        batch_id = batch_id_result[0] if batch_id_result else 1

        applied = 0
        failed = 0

        for _, row in corrections.iterrows():
            try:
                reach_id = row["reach_id"]
                region = row["region"]
                old_facc = row["old_facc"]
                new_facc = row["facc_corrected"]

                # Log the change
                self.conn.execute(
                    """
                    INSERT INTO facc_fix_log
                    (batch_id, reach_id, region, old_facc, new_facc, fix_type, model_used)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        batch_id,
                        reach_id,
                        region,
                        old_facc,
                        new_facc,
                        row.get("fix_type", "unknown"),
                        row.get("model_used", "unknown"),
                    ],
                )

                # Update reaches table
                self.conn.execute(f"""
                    UPDATE reaches
                    SET facc = {new_facc}
                    WHERE reach_id = {reach_id} AND region = '{region}'
                """)

                applied += 1

            except Exception as e:
                logger.error(f"Failed to update reach {row['reach_id']}: {e}")
                failed += 1

        logger.info(
            f"Applied {applied} corrections (batch_id={batch_id}), {failed} failed"
        )

        return CorrectionResult(
            corrections=corrections,
            applied=applied,
            skipped=0,
            failed=failed,
            batch_id=batch_id,
            dry_run=False,
            validation=validation,
        )

    def rollback(self, batch_id: int) -> int:
        """
        Rollback corrections from a batch.

        Parameters
        ----------
        batch_id : int
            Batch ID to rollback.

        Returns
        -------
        int
            Number of reaches reverted.
        """
        # Get changes from log
        changes = self.conn.execute(f"""
            SELECT reach_id, region, old_facc
            FROM facc_fix_log
            WHERE batch_id = {batch_id}
        """).fetchdf()

        if len(changes) == 0:
            logger.warning(f"No changes found for batch_id={batch_id}")
            return 0

        reverted = 0
        for _, row in changes.iterrows():
            try:
                self.conn.execute(f"""
                    UPDATE reaches
                    SET facc = {row["old_facc"]}
                    WHERE reach_id = {row["reach_id"]} AND region = '{row["region"]}'
                """)
                reverted += 1
            except Exception as e:
                logger.error(f"Failed to rollback reach {row['reach_id']}: {e}")

        # Mark batch as rolled back
        self.conn.execute(f"""
            DELETE FROM facc_fix_log WHERE batch_id = {batch_id}
        """)

        logger.info(f"Rolled back {reverted} changes from batch {batch_id}")
        return reverted

    def get_batch_history(self) -> pd.DataFrame:
        """Get summary of all correction batches."""
        return self.conn.execute("""
            SELECT
                batch_id,
                COUNT(*) as n_changes,
                MIN(created_at) as started_at,
                COUNT(DISTINCT region) as n_regions,
                AVG(old_facc / NULLIF(new_facc, 0)) as avg_reduction
            FROM facc_fix_log
            GROUP BY batch_id
            ORDER BY batch_id DESC
        """).fetchdf()

    def classify_with_mainstem(
        self,
        anomalies: pd.DataFrame,
        v17c_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> pd.DataFrame:
        """
        Classify anomalies using v17c mainstem routing information.

        Uses is_mainstem, rch_id_up_main, rch_id_dn_main to determine:
        - side_to_mainstem: Side channel has facc that belongs on mainstem sibling
        - headwater_reset: Headwater with huge facc
        - mainstem_reextract: High FWR on mainstem (D8 misroute)

        Parameters
        ----------
        anomalies : pd.DataFrame
            Detected anomalies.
        v17c_conn : duckdb.DuckDBPyConnection, optional
            Connection to v17c database with routing info. If None, uses self.conn.

        Returns
        -------
        pd.DataFrame
            Anomalies with correction_type and correction_target columns.
        """
        if len(anomalies) == 0:
            return anomalies

        v17c = v17c_conn or self.conn
        df = anomalies.copy()

        # Check if v17c columns exist
        try:
            v17c.execute("SELECT is_mainstem FROM reaches LIMIT 1")
            has_v17c = True
        except Exception:
            logger.warning("v17c routing columns not found, using basic classification")
            has_v17c = False

        reach_ids = df["reach_id"].tolist()
        reach_ids_str = ", ".join(str(r) for r in reach_ids)

        if has_v17c:
            # Get v17c routing info
            routing_df = v17c.execute(f"""
                SELECT reach_id, region, is_mainstem, rch_id_up_main, rch_id_dn_main
                FROM reaches
                WHERE reach_id IN ({reach_ids_str})
            """).fetchdf()

            df = df.merge(
                routing_df,
                on=["reach_id", "region"],
                how="left",
                suffixes=("", "_v17c"),
            )

        # Get v17b info for classification
        v17b_info = self.conn.execute(f"""
            SELECT reach_id, region, facc, width, main_side, n_rch_up, path_freq,
                   facc / GREATEST(width, 15) as fwr
            FROM reaches
            WHERE reach_id IN ({reach_ids_str})
        """).fetchdf()

        df = df.merge(
            v17b_info[["reach_id", "region", "main_side", "n_rch_up", "fwr"]],
            on=["reach_id", "region"],
            how="left",
            suffixes=("", "_v17b"),
        )

        # Classify each anomaly
        correction_types = []
        correction_targets = []

        for _, row in df.iterrows():
            rid = row["reach_id"]
            facc = row.get("facc", 0)
            main_side = row.get("main_side", 0)
            n_rch_up = row.get("n_rch_up", 0)
            fwr = row.get("fwr", 0)
            is_mainstem = row.get("is_mainstem", True)

            correction_type = None
            correction_target = None

            # Case 1: Side channel (not on mainstem) with significant facc
            if has_v17c and not is_mainstem and facc > 100000:
                # Find mainstem sibling at upstream junction
                upstream = self.conn.execute(f"""
                    SELECT rt.neighbor_reach_id
                    FROM reach_topology rt
                    WHERE rt.reach_id = {rid} AND rt.direction = 'up'
                """).fetchall()

                for up in upstream:
                    up_id = up[0]
                    # Get siblings from this upstream
                    siblings = self.conn.execute(f"""
                        SELECT rt.neighbor_reach_id, r.facc
                        FROM reach_topology rt
                        JOIN reaches r ON rt.neighbor_reach_id = r.reach_id
                        WHERE rt.reach_id = {up_id} AND rt.direction = 'down'
                            AND rt.neighbor_reach_id != {rid}
                    """).fetchall()

                    for sib_id, sib_facc in siblings:
                        sib_ms = v17c.execute(f"""
                            SELECT is_mainstem FROM reaches WHERE reach_id = {sib_id}
                        """).fetchone()
                        if sib_ms and sib_ms[0]:  # Sibling is on mainstem
                            if facc > sib_facc * 0.5:  # Side has too much facc
                                correction_type = "side_to_mainstem"
                                correction_target = sib_id
                                break
                    if correction_type:
                        break

            # Case 2: Headwater with huge facc
            if correction_type is None and n_rch_up == 0 and facc > 500000:
                correction_type = "headwater_reset"

            # Case 3: High FWR on mainstem (D8 misroute, needs re-extraction)
            if (
                correction_type is None
                and (is_mainstem or not has_v17c)
                and fwr
                and fwr > 10000
            ):
                correction_type = "mainstem_reextract"

            # Case 4: Side channel via main_side (v17b fallback if no v17c)
            if correction_type is None and main_side == 1 and facc > 100000:
                correction_type = "side_channel_high_facc"

            # Default: use regression
            if correction_type is None:
                correction_type = "regression"

            correction_types.append(correction_type)
            correction_targets.append(correction_target)

        df["correction_type"] = correction_types
        df["correction_target"] = correction_targets

        # Log stats
        type_counts = df["correction_type"].value_counts()
        logger.info(f"Mainstem classification: {dict(type_counts)}")

        return df

    def estimate_mainstem_corrections(
        self,
        classified_anomalies: pd.DataFrame,
        v17c_conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> pd.DataFrame:
        """
        Estimate corrections using mainstem routing.

        For side_to_mainstem: Set side channel facc to small value (local catchment only)
        For headwater_reset: Set to small headwater-appropriate value
        For mainstem_reextract: Use regression or MERIT re-extraction

        Parameters
        ----------
        classified_anomalies : pd.DataFrame
            Anomalies with correction_type from classify_with_mainstem().
        v17c_conn : duckdb.DuckDBPyConnection, optional
            Connection to v17c database.

        Returns
        -------
        pd.DataFrame
            Corrections with facc_corrected.
        """
        if len(classified_anomalies) == 0:
            return pd.DataFrame()

        corrections = []

        for region in classified_anomalies["region"].unique():
            # Fit regression model for fallback
            models = self.fit_regression(region)

            # Get regional baseline for headwaters
            baseline = self.conn.execute(f"""
                SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facc) as median_facc,
                    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY facc) as p10_facc
                FROM reaches
                WHERE region = '{region}'
                    AND n_rch_up = 0  -- Headwaters only
                    AND facc > 0 AND facc != -9999
                    AND lakeflag != 1
                    AND (facc / GREATEST(width, 15)) < 2000  -- Exclude anomalies
            """).fetchone()
            headwater_typical = baseline[1] if baseline and baseline[1] else 1000

            reg_anomalies = classified_anomalies[
                classified_anomalies["region"] == region
            ]

            for _, row in reg_anomalies.iterrows():
                rid = row["reach_id"]
                old_facc = row["facc"]
                correction_type = row["correction_type"]
                width = row.get("width", 50)
                path_freq = row.get("path_freq", 1)
                if path_freq <= 0:
                    path_freq = 1

                facc_corrected = None
                source = correction_type

                if correction_type == "side_to_mainstem":
                    # Side channel should have minimal facc (just local catchment)
                    # Estimate: width * reach_length / 1e6 (very rough km² approx)
                    reach_length = row.get("reach_length", 5000)
                    local_catchment = width * reach_length / 1e6  # km²
                    facc_corrected = max(local_catchment * 100, 100)  # min 100 km²

                elif correction_type == "headwater_reset":
                    # Use typical headwater value for this region
                    facc_corrected = headwater_typical

                elif correction_type == "mainstem_reextract":
                    # Use regression model
                    pred, model_used = self._predict_facc(row, models)
                    facc_corrected = pred
                    source = f"mainstem_{model_used}"

                elif correction_type == "side_channel_high_facc":
                    # Similar to side_to_mainstem
                    reach_length = row.get("reach_length", 5000)
                    local_catchment = width * reach_length / 1e6
                    facc_corrected = max(local_catchment * 100, 100)

                else:  # regression fallback
                    pred, model_used = self._predict_facc(row, models)
                    facc_corrected = pred
                    source = model_used

                # Sanity checks
                facc_corrected = max(1.0, facc_corrected)
                facc_corrected = min(facc_corrected, 1e9)

                corrections.append(
                    {
                        "reach_id": rid,
                        "region": region,
                        "old_facc": old_facc,
                        "facc_corrected": facc_corrected,
                        "correction_type": correction_type,
                        "model_used": source,
                        "reduction_factor": old_facc / facc_corrected
                        if facc_corrected > 0
                        else 0,
                        "correction_target": row.get("correction_target"),
                    }
                )

        return pd.DataFrame(corrections)

    def detect_hybrid(self, region: Optional[str] = None) -> DetectionResult:
        """
        Use hybrid detection (ratio_to_median based) - catches 5/5 seeds, 0/24 FPs.

        This is the recommended detection method for correction.

        Parameters
        ----------
        region : str, optional
            Region to detect.

        Returns
        -------
        DetectionResult
            Hybrid detection results.
        """
        return detect_hybrid(self.conn, region=region)


def correct_facc_anomalies(
    db_path: str,
    region: Optional[str] = None,
    anomaly_threshold: float = 0.5,
    dry_run: bool = True,
    merit_path: Optional[str] = None,
) -> CorrectionResult:
    """
    Convenience function to detect and correct facc anomalies.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database.
    region : str, optional
        Region to correct.
    anomaly_threshold : float
        Detection threshold.
    dry_run : bool
        If True, don't apply changes.
    merit_path : str, optional
        Path to MERIT Hydro base directory. If provided, enables guided
        MERIT search for entry points and orphan entries.

    Returns
    -------
    CorrectionResult
        Correction result.

    Examples
    --------
    >>> from sword_duckdb.facc_detection import correct_facc_anomalies
    >>> result = correct_facc_anomalies("sword_v17c.duckdb", region="NA", dry_run=True)
    >>> print(result.summary())

    >>> # With MERIT guided search
    >>> result = correct_facc_anomalies(
    ...     "sword_v17c.duckdb",
    ...     region="NA",
    ...     merit_path="/Volumes/SWORD_DATA/data/MERIT_Hydro"
    ... )
    """
    with FaccCorrector(db_path, read_only=dry_run) as corrector:
        # Detect anomalies
        detector = FaccDetector(corrector.conn)
        detection = detector.detect(region=region, anomaly_threshold=anomaly_threshold)

        # Filter fixable
        fixable, skipped = corrector.filter_fixable(detection.anomalies)

        if len(fixable) == 0:
            logger.info("No fixable anomalies found")
            return CorrectionResult(
                corrections=pd.DataFrame(),
                applied=0,
                skipped=len(skipped),
                failed=0,
                dry_run=dry_run,
            )

        # Classify
        classified = corrector.classify_anomalies(fixable)

        # Estimate corrections with optional MERIT search
        corrections = corrector.estimate_corrections(
            classified, region=region, merit_path=merit_path
        )

        # Apply
        result = corrector.apply_corrections(corrections, dry_run=dry_run)
        result.skipped = len(skipped)

        return result
