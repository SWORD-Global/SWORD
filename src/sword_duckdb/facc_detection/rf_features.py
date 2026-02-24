# -*- coding: utf-8 -*-
"""
RF Feature Extraction for FACC Anomaly Detection
-------------------------------------------------

Extracts comprehensive feature set (~56 features) for Random Forest classifier.

Feature categories:
1. Core Hydrology (8): facc, width, slope, reach_length, wse, wse_var, max_width, width_var
2. Topology (10): n_rch_up, n_rch_down, stream_order, path_freq, path_order, path_segs,
                  end_reach, main_side, network, trib_flag
3. Classification (5): lakeflag, type, n_chan_max, n_chan_mod, iceflag
4. SWOT Observations (13): n_obs, swot_obs, wse_obs_*, width_obs_*, slope_obs_*
5. v17c Topology (7): hydro_dist_out, hydro_dist_hw, dist_out,
                      pathlen_hw, pathlen_out, is_mainstem_edge, main_path_id
6. Computed (12): ratios, logs, neighbor metrics

Usage:
    from facc_detection.rf_features import RFFeatureExtractor

    extractor = RFFeatureExtractor("sword_v17c.duckdb")
    features = extractor.extract_all()
    features.to_parquet("output/facc_detection/rf_features.parquet")
"""

from typing import Optional, Union, List, Dict, Any
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np


class RFFeatureExtractor:
    """
    Extracts features for RF-based facc anomaly detection.

    Parameters
    ----------
    db_path_or_conn : str or duckdb.DuckDBPyConnection
        Path to DuckDB database or existing connection.
    """

    # Feature column names by category
    CORE_HYDROLOGY = [
        "facc",
        "width",
        "slope",
        "reach_length",
        "wse",
        "wse_var",
        "max_width",
        "width_var",
    ]

    TOPOLOGY = [
        "n_rch_up",
        "n_rch_down",
        "stream_order",
        "path_freq",
        "path_order",
        "path_segs",
        "end_reach",
        "main_side",
        "network",
        "trib_flag",
    ]

    CLASSIFICATION = ["lakeflag", "type", "n_chan_max", "n_chan_mod", "iceflag"]

    SWOT_OBS = [
        "n_obs",
        "swot_obs",
        "wse_obs_mean",
        "wse_obs_median",
        "wse_obs_std",
        "wse_obs_range",
        "width_obs_mean",
        "width_obs_median",
        "width_obs_std",
        "width_obs_range",
        "slope_obs_mean",
        "slope_obs_median",
        "slope_obs_std",
    ]

    V17C_TOPOLOGY = [
        "hydro_dist_out",
        "hydro_dist_hw",
        "dist_out",
        "pathlen_hw",
        "pathlen_out",
        "is_mainstem_edge",
        "main_path_id",
    ]

    def __init__(self, db_path_or_conn: Union[str, duckdb.DuckDBPyConnection]):
        if isinstance(db_path_or_conn, str):
            self.conn = duckdb.connect(db_path_or_conn, read_only=True)
            self._own_conn = True
        else:
            self.conn = db_path_or_conn
            self._own_conn = False

    def close(self):
        """Close connection if owned."""
        if self._own_conn and self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_available_columns(self) -> List[str]:
        """Get list of columns available in reaches table."""
        result = self.conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'reaches'
        """).fetchdf()
        return result["column_name"].tolist()

    def extract_base_features(self, region: Optional[str] = None) -> pd.DataFrame:
        """
        Extract base features directly from reaches table.

        Parameters
        ----------
        region : str, optional
            Region to extract for (e.g., 'NA'). If None, extracts all.

        Returns
        -------
        pd.DataFrame
            Base features for all reaches.
        """
        available = set(self._get_available_columns())

        # Build SELECT clause with available columns
        select_cols = ["reach_id", "region"]

        # Core hydrology
        for col in self.CORE_HYDROLOGY:
            if col in available:
                select_cols.append(col)

        # Topology
        for col in self.TOPOLOGY:
            if col in available:
                select_cols.append(col)

        # Classification
        for col in self.CLASSIFICATION:
            if col in available:
                select_cols.append(col)

        # SWOT observations
        for col in self.SWOT_OBS:
            if col in available:
                select_cols.append(col)

        # v17c topology
        for col in self.V17C_TOPOLOGY:
            if col in available:
                select_cols.append(col)

        select_clause = ", ".join(select_cols)
        where_clause = f"AND region = '{region}'" if region else ""

        query = f"""
        SELECT {select_clause}
        FROM reaches
        WHERE facc IS NOT NULL
            AND facc > 0
            AND facc != -9999
            {where_clause}
        """

        return self.conn.execute(query).fetchdf()

    def extract_neighbor_features(self, region: Optional[str] = None) -> pd.DataFrame:
        """
        Extract neighbor-based features (upstream/downstream metrics).

        Parameters
        ----------
        region : str, optional
            Region to extract for.

        Returns
        -------
        pd.DataFrame
            Neighbor features: upstream_facc_sum, max_upstream_facc, etc.
        """
        where_region = f"AND r.region = '{region}'" if region else ""
        topo_region = f"AND rt.region = '{region}'" if region else ""

        query = f"""
        WITH -- 2-hop upstream info
        hop1_up AS (
            SELECT rt.reach_id, rt.region, rt.neighbor_reach_id as up1_id
            FROM reach_topology rt
            WHERE rt.direction = 'up' {topo_region}
        ),
        hop2_up AS (
            SELECT h1.reach_id, h1.region, rt.neighbor_reach_id as up2_id
            FROM hop1_up h1
            JOIN reach_topology rt ON h1.up1_id = rt.reach_id AND h1.region = rt.region
            WHERE rt.direction = 'up'
        ),
        two_hop_upstream AS (
            SELECT h2.reach_id, h2.region,
                MAX(r.facc) as max_2hop_upstream_facc,
                AVG(r.facc / NULLIF(r.width, 0)) as avg_2hop_upstream_fwr,
                COUNT(DISTINCT h2.up2_id) as n_upstream_2hop
            FROM hop2_up h2
            JOIN reaches r ON h2.up2_id = r.reach_id AND h2.region = r.region
            WHERE r.facc > 0 AND r.facc != -9999
            GROUP BY h2.reach_id, h2.region
        ),
        -- 2-hop downstream info
        hop1_dn AS (
            SELECT rt.reach_id, rt.region, rt.neighbor_reach_id as dn1_id
            FROM reach_topology rt
            WHERE rt.direction = 'down' {topo_region}
        ),
        hop2_dn AS (
            SELECT h1.reach_id, h1.region, rt.neighbor_reach_id as dn2_id
            FROM hop1_dn h1
            JOIN reach_topology rt ON h1.dn1_id = rt.reach_id AND h1.region = rt.region
            WHERE rt.direction = 'down'
        ),
        two_hop_downstream AS (
            SELECT h2.reach_id, h2.region,
                MAX(r.facc) as max_2hop_downstream_facc,
                AVG(r.facc / NULLIF(r.width, 0)) as avg_2hop_downstream_fwr
            FROM hop2_dn h2
            JOIN reaches r ON h2.dn2_id = r.reach_id AND h2.region = r.region
            WHERE r.facc > 0 AND r.facc != -9999
            GROUP BY h2.reach_id, h2.region
        ),
        upstream_info AS (
            SELECT
                rt.reach_id,
                rt.region,
                MAX(r_up.facc) as max_upstream_facc,
                SUM(r_up.facc) as upstream_facc_sum,
                COUNT(*) as n_upstream_actual,
                MAX(r_up.facc / NULLIF(r_up.width, 0)) as max_upstream_fwr,
                AVG(r_up.facc / NULLIF(r_up.width, 0)) as avg_upstream_fwr,
                AVG(r_up.width) as avg_upstream_width
            FROM reach_topology rt
            JOIN reaches r_up ON rt.neighbor_reach_id = r_up.reach_id AND rt.region = r_up.region
            WHERE rt.direction = 'up'
                AND r_up.facc > 0 AND r_up.facc != -9999
                {topo_region}
            GROUP BY rt.reach_id, rt.region
        ),
        downstream_info AS (
            SELECT
                rt.reach_id,
                rt.region,
                MAX(r_dn.facc) as max_downstream_facc,
                MAX(r_dn.width) as max_downstream_width,
                MAX(r_dn.facc / NULLIF(r_dn.width, 0)) as max_downstream_fwr,
                AVG(r_dn.facc / NULLIF(r_dn.width, 0)) as avg_downstream_fwr,
                AVG(r_dn.width) as avg_downstream_width
            FROM reach_topology rt
            JOIN reaches r_dn ON rt.neighbor_reach_id = r_dn.reach_id AND rt.region = r_dn.region
            WHERE rt.direction = 'down'
                AND r_dn.facc > 0 AND r_dn.facc != -9999
                {topo_region}
            GROUP BY rt.reach_id, rt.region
        ),
        -- Network-level FWR statistics for z-score calculation
        network_stats AS (
            SELECT
                network,
                region,
                AVG(facc / NULLIF(width, 0)) as network_avg_fwr,
                STDDEV(facc / NULLIF(width, 0)) as network_std_fwr,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facc / NULLIF(width, 0)) as network_median_fwr
            FROM reaches
            WHERE facc > 0 AND facc != -9999 AND width > 0
            GROUP BY network, region
        )
        SELECT
            r.reach_id,
            r.region,
            COALESCE(ui.max_upstream_facc, 0) as max_upstream_facc,
            COALESCE(ui.upstream_facc_sum, 0) as upstream_facc_sum,
            COALESCE(ui.n_upstream_actual, 0) as n_upstream_actual,
            ui.max_upstream_fwr,
            ui.avg_upstream_fwr,
            ui.avg_upstream_width,
            di.max_downstream_facc,
            di.max_downstream_width,
            di.max_downstream_fwr,
            di.avg_downstream_fwr,
            di.avg_downstream_width,
            -- Computed ratios
            CASE
                WHEN COALESCE(ui.upstream_facc_sum, 0) > 0
                THEN r.facc / ui.upstream_facc_sum
                ELSE NULL
            END as facc_jump_ratio,
            CASE
                WHEN di.max_downstream_fwr IS NOT NULL AND di.max_downstream_fwr > 0
                THEN (r.facc / NULLIF(r.width, 0)) / di.max_downstream_fwr
                ELSE NULL
            END as fwr_drop_ratio,
            CASE
                WHEN (r.facc / NULLIF(r.width, 0)) > 0 AND ui.max_upstream_fwr IS NOT NULL
                THEN ui.max_upstream_fwr / (r.facc / NULLIF(r.width, 0))
                ELSE NULL
            END as upstream_fwr_ratio,
            r.width / NULLIF(di.max_downstream_width, 0) as width_ratio_to_dn,
            -- NEW: FWR consistency with upstream (high = propagation pattern)
            CASE
                WHEN ui.avg_upstream_fwr IS NOT NULL AND ui.avg_upstream_fwr > 0
                THEN (r.facc / NULLIF(r.width, 0)) / ui.avg_upstream_fwr
                ELSE NULL
            END as fwr_upstream_consistency,
            -- NEW: FWR per path_freq (high = too much facc for network position)
            CASE
                WHEN r.path_freq > 0 AND r.path_freq != -9999
                THEN (r.facc / NULLIF(r.width, 0)) / r.path_freq
                ELSE NULL
            END as fwr_per_path_freq,
            -- NEW: Network z-score (how many std devs above network mean)
            CASE
                WHEN ns.network_std_fwr IS NOT NULL AND ns.network_std_fwr > 0
                THEN ((r.facc / NULLIF(r.width, 0)) - ns.network_avg_fwr) / ns.network_std_fwr
                ELSE NULL
            END as fwr_network_zscore,
            -- NEW: FWR ratio to network median
            CASE
                WHEN ns.network_median_fwr IS NOT NULL AND ns.network_median_fwr > 0
                THEN (r.facc / NULLIF(r.width, 0)) / ns.network_median_fwr
                ELSE NULL
            END as fwr_network_ratio,
            -- NEW: Downstream FWR consistency (low drop + high FWR = propagation)
            CASE
                WHEN di.avg_downstream_fwr IS NOT NULL AND di.avg_downstream_fwr > 0
                THEN (r.facc / NULLIF(r.width, 0)) / di.avg_downstream_fwr
                ELSE NULL
            END as fwr_downstream_consistency,
            -- 2-HOP UPSTREAM FEATURES
            tu.max_2hop_upstream_facc,
            tu.avg_2hop_upstream_fwr,
            COALESCE(tu.n_upstream_2hop, 0) as n_upstream_2hop,
            -- Facc ratio to 2-hop upstream (high = anomaly entry point)
            CASE
                WHEN tu.max_2hop_upstream_facc IS NOT NULL AND tu.max_2hop_upstream_facc > 0
                THEN r.facc / tu.max_2hop_upstream_facc
                ELSE NULL
            END as facc_2hop_ratio,
            -- 2-HOP DOWNSTREAM FEATURES
            td.max_2hop_downstream_facc,
            td.avg_2hop_downstream_fwr,
            -- Facc chain direction: 1 if increasing downstream, -1 if decreasing
            CASE
                WHEN tu.max_2hop_upstream_facc IS NOT NULL AND td.max_2hop_downstream_facc IS NOT NULL
                     AND tu.max_2hop_upstream_facc > 0
                THEN SIGN(td.max_2hop_downstream_facc - tu.max_2hop_upstream_facc)
                ELSE NULL
            END as facc_chain_direction
        FROM reaches r
        LEFT JOIN upstream_info ui ON r.reach_id = ui.reach_id AND r.region = ui.region
        LEFT JOIN downstream_info di ON r.reach_id = di.reach_id AND r.region = di.region
        LEFT JOIN network_stats ns ON r.network = ns.network AND r.region = ns.region
        LEFT JOIN two_hop_upstream tu ON r.reach_id = tu.reach_id AND r.region = tu.region
        LEFT JOIN two_hop_downstream td ON r.reach_id = td.reach_id AND r.region = td.region
        WHERE r.facc > 0 AND r.facc != -9999
            {where_region}
        """

        return self.conn.execute(query).fetchdf()

    def extract_regional_stats(self, region: Optional[str] = None) -> pd.DataFrame:
        """
        Extract regional statistics for ratio_to_median calculation.

        Parameters
        ----------
        region : str, optional
            Region to extract for.

        Returns
        -------
        pd.DataFrame
            Regional stats joined to reaches.
        """
        where_region = f"AND r.region = '{region}'" if region else ""

        query = f"""
        WITH regional_stats AS (
            SELECT
                region,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facc / NULLIF(path_freq, 0)) as median_fpr,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY facc / NULLIF(width, 0)) as median_fwr,
                AVG(facc / NULLIF(width, 0)) as avg_fwr
            FROM reaches
            WHERE facc > 0 AND facc != -9999
                AND path_freq > 0
                AND width > 0
                AND lakeflag != 1
                AND (facc / NULLIF(width, 0)) < 5000  -- Exclude known anomalies from baseline
            GROUP BY region
        )
        SELECT
            r.reach_id,
            r.region,
            rs.median_fpr,
            rs.median_fwr,
            rs.avg_fwr,
            (r.facc / NULLIF(r.path_freq, 0)) / NULLIF(rs.median_fpr, 0) as ratio_to_median,
            (r.facc / NULLIF(r.width, 0)) / NULLIF(rs.median_fwr, 0) as fwr_ratio_to_median
        FROM reaches r
        JOIN regional_stats rs ON r.region = rs.region
        WHERE r.facc > 0 AND r.facc != -9999
            {where_region}
        """

        return self.conn.execute(query).fetchdf()

    def compute_derived_features(
        self, df: pd.DataFrame, include_region_encoding: bool = True
    ) -> pd.DataFrame:
        """
        Compute derived features from base features.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with base features.
        include_region_encoding : bool
            If True, add one-hot encoded region columns.

        Returns
        -------
        pd.DataFrame
            DataFrame with additional computed features.
        """
        df = df.copy()

        # Regional one-hot encoding
        if include_region_encoding and "region" in df.columns:
            region_dummies = pd.get_dummies(df["region"], prefix="region")
            # Ensure all regions are present even if not in data
            for r in ["NA", "SA", "EU", "AF", "AS", "OC"]:
                col = f"region_{r}"
                if col not in region_dummies.columns:
                    region_dummies[col] = 0
            df = pd.concat([df, region_dummies], axis=1)

        # Facc-width ratio (FWR)
        df["facc_width_ratio"] = df["facc"] / df["width"].replace(0, np.nan)

        # Facc per reach (using path_freq as proxy for reach count)
        df["facc_per_reach"] = np.where(
            df["path_freq"] > 0, df["facc"] / df["path_freq"], np.nan
        )

        # Log transforms
        df["log_facc"] = np.log1p(df["facc"].fillna(0))
        df["log_width"] = np.log1p(df["width"].fillna(0))
        df["log_path_freq"] = np.log1p(df["path_freq"].fillna(0).clip(lower=0))

        # Binary indicators
        df["is_headwater"] = (df["end_reach"] == 1).astype(int)
        df["is_outlet"] = (df["end_reach"] == 2).astype(int)
        df["is_junction"] = (df["end_reach"] == 3).astype(int)
        df["is_side_channel"] = (df["main_side"] == 1).astype(int)
        df["is_lake"] = (df["lakeflag"] == 1).astype(int)

        # SWOT observation indicator
        if "n_obs" in df.columns:
            df["has_swot_obs"] = (df["n_obs"].fillna(0) > 0).astype(int)
        else:
            df["has_swot_obs"] = 0

        # Mainstem indicator
        if "is_mainstem_edge" in df.columns:
            df["is_mainstem"] = df["is_mainstem_edge"].fillna(0).astype(int)
        else:
            df["is_mainstem"] = 0

        # Path_freq validity indicator (some have -9999)
        if "path_freq" in df.columns:
            df["valid_path_freq"] = (
                (df["path_freq"].fillna(0) > 0) & (df["path_freq"].fillna(0) != -9999)
            ).astype(int)
        else:
            df["valid_path_freq"] = 1

        # NEW PROPAGATION DETECTION FEATURES

        # Side channel with high FWR (common propagation pattern)
        if "main_side" in df.columns and "facc_width_ratio" in df.columns:
            df["side_channel_high_fwr"] = (
                (df["main_side"] == 1) & (df["facc_width_ratio"] > 500)
            ).astype(int)

        # Invalid path_freq with high FWR (disconnected but high facc)
        if "path_freq" in df.columns and "facc_width_ratio" in df.columns:
            df["invalid_pf_high_fwr"] = (
                (
                    (df["path_freq"].fillna(0) <= 0)
                    | (df["path_freq"].fillna(0) == -9999)
                )
                & (df["facc_width_ratio"] > 500)
            ).astype(int)

        # Low path_freq with high facc (propagation near headwater)
        if "path_freq" in df.columns and "facc" in df.columns:
            df["low_pf_high_facc"] = (
                (df["path_freq"].fillna(0) > 0)
                & (df["path_freq"].fillna(0) <= 5)
                & (df["facc"] > 100000)
            ).astype(int)

        # FWR propagation indicator: high FWR + consistent with upstream
        if (
            "facc_width_ratio" in df.columns
            and "fwr_upstream_consistency" in df.columns
        ):
            df["fwr_propagation"] = (
                (df["facc_width_ratio"] > 1000)
                & (df["fwr_upstream_consistency"].fillna(0) > 0.5)
                & (df["fwr_upstream_consistency"].fillna(0) < 2.0)
            ).astype(int)

        # High absolute FWR (catches cases where relative metrics fail)
        if "facc_width_ratio" in df.columns:
            df["extreme_fwr_1000"] = (df["facc_width_ratio"] > 1000).astype(int)
            df["extreme_fwr_2000"] = (df["facc_width_ratio"] > 2000).astype(int)
            df["extreme_fwr_5000"] = (df["facc_width_ratio"] > 5000).astype(int)

        return df

    def extract_all(
        self, region: Optional[str] = None, include_geometry: bool = False
    ) -> pd.DataFrame:
        """
        Extract all features for RF training.

        Parameters
        ----------
        region : str, optional
            Region to extract for. If None, extracts all regions.
        include_geometry : bool
            If True, include geometry for GeoJSON export.

        Returns
        -------
        pd.DataFrame
            Complete feature set for all reaches.
        """
        # Extract base features
        base_df = self.extract_base_features(region)
        print(f"Base features: {len(base_df)} reaches, {len(base_df.columns)} columns")

        # Extract neighbor features
        neighbor_df = self.extract_neighbor_features(region)
        print(f"Neighbor features: {len(neighbor_df.columns)} columns")

        # Extract regional stats
        regional_df = self.extract_regional_stats(region)
        print(f"Regional stats: {len(regional_df.columns)} columns")

        # Merge all features
        features = base_df.merge(
            neighbor_df.drop(columns=["region"], errors="ignore"),
            on="reach_id",
            how="left",
        )

        features = features.merge(
            regional_df.drop(columns=["region"], errors="ignore"),
            on="reach_id",
            how="left",
        )

        # Compute derived features
        features = self.compute_derived_features(features)
        print(f"After derived features: {len(features.columns)} columns")

        # Handle missing SWOT values (impute with 0, indicator already added)
        swot_cols = [
            c for c in features.columns if "obs" in c.lower() and c != "has_swot_obs"
        ]
        for col in swot_cols:
            features[col] = features[col].fillna(0)

        # Get geometry if requested
        if include_geometry:
            geom_df = self._get_geometry(features["reach_id"].tolist(), region)
            features = features.merge(geom_df, on="reach_id", how="left")

        return features

    def _get_geometry(
        self, reach_ids: List[int], region: Optional[str] = None
    ) -> pd.DataFrame:
        """Get geometry for reaches (for GeoJSON export)."""
        self.conn.execute("INSTALL spatial; LOAD spatial;")

        reach_ids_str = ", ".join(str(r) for r in reach_ids)
        where_region = f"AND region = '{region}'" if region else ""

        query = f"""
        SELECT reach_id, ST_AsGeoJSON(geom) as geometry
        FROM reaches
        WHERE reach_id IN ({reach_ids_str})
            {where_region}
        """

        return self.conn.execute(query).fetchdf()

    def get_feature_info(self) -> Dict[str, Any]:
        """
        Get information about available features.

        Returns
        -------
        dict
            Feature metadata including names, categories, and descriptions.
        """
        available = set(self._get_available_columns())

        info = {
            "categories": {
                "core_hydrology": [c for c in self.CORE_HYDROLOGY if c in available],
                "topology": [c for c in self.TOPOLOGY if c in available],
                "classification": [c for c in self.CLASSIFICATION if c in available],
                "swot_obs": [c for c in self.SWOT_OBS if c in available],
                "v17c_topology": [c for c in self.V17C_TOPOLOGY if c in available],
            },
            "neighbor_computed": [
                "max_upstream_facc",
                "upstream_facc_sum",
                "n_upstream_actual",
                "max_upstream_fwr",
                "avg_upstream_width",
                "max_downstream_facc",
                "max_downstream_width",
                "max_downstream_fwr",
                "facc_jump_ratio",
                "fwr_drop_ratio",
                "upstream_fwr_ratio",
                "width_ratio_to_dn",
                # 2-hop features
                "max_2hop_upstream_facc",
                "avg_2hop_upstream_fwr",
                "n_upstream_2hop",
                "facc_2hop_ratio",
                "max_2hop_downstream_facc",
                "avg_2hop_downstream_fwr",
                "facc_chain_direction",
            ],
            "derived": [
                "facc_width_ratio",
                "facc_per_reach",
                "log_facc",
                "log_width",
                "log_path_freq",
                "is_headwater",
                "is_outlet",
                "is_junction",
                "is_side_channel",
                "is_lake",
                "has_swot_obs",
                "is_mainstem",
                "valid_path_freq",
                "ratio_to_median",
                "fwr_ratio_to_median",
                # Regional encoding
                "region_NA",
                "region_SA",
                "region_EU",
                "region_AF",
                "region_AS",
                "region_OC",
            ],
        }

        total = sum(len(v) for v in info["categories"].values())
        total += len(info["neighbor_computed"])
        total += len(info["derived"])
        info["total_features"] = total

        return info


def extract_rf_features(
    db_path: str, region: Optional[str] = None, output_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Convenience function to extract RF features.

    Parameters
    ----------
    db_path : str
        Path to DuckDB database.
    region : str, optional
        Region to extract for.
    output_path : str, optional
        Path to save parquet file. If None, returns DataFrame only.

    Returns
    -------
    pd.DataFrame
        Complete feature set.

    Examples
    --------
    >>> features = extract_rf_features(
    ...     "data/duckdb/sword_v17c.duckdb",
    ...     output_path="output/facc_detection/rf_features.parquet"
    ... )
    """
    with RFFeatureExtractor(db_path) as extractor:
        features = extractor.extract_all(region=region)

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            features.to_parquet(output_path, index=False)
            print(f"Saved {len(features)} rows to {output_path}")

        return features


def load_anomaly_labels(
    anomaly_geojson: str, false_positives: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Load anomaly labels from GeoJSON file.

    Parameters
    ----------
    anomaly_geojson : str
        Path to all_anomalies.geojson from detection.
    false_positives : list of int, optional
        Known false positive reach IDs to exclude.

    Returns
    -------
    pd.DataFrame
        DataFrame with reach_id and label (1=anomaly, 0=clean).
    """
    import json

    with open(anomaly_geojson) as f:
        data = json.load(f)

    # Extract reach IDs from GeoJSON
    reach_ids = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        if "reach_id" in props:
            reach_ids.append(int(props["reach_id"]))

    # Remove false positives
    if false_positives:
        reach_ids = [r for r in reach_ids if r not in false_positives]

    return pd.DataFrame({"reach_id": reach_ids, "is_anomaly": 1})


def create_labeled_dataset(
    features: pd.DataFrame, anomaly_labels: pd.DataFrame
) -> pd.DataFrame:
    """
    Create labeled dataset for RF training.

    Parameters
    ----------
    features : pd.DataFrame
        Full feature set from extract_rf_features().
    anomaly_labels : pd.DataFrame
        Anomaly labels from load_anomaly_labels().

    Returns
    -------
    pd.DataFrame
        Features with is_anomaly label column.
    """
    # Merge labels
    labeled = features.merge(
        anomaly_labels[["reach_id", "is_anomaly"]], on="reach_id", how="left"
    )

    # Non-anomalies get label 0
    labeled["is_anomaly"] = labeled["is_anomaly"].fillna(0).astype(int)

    return labeled


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract RF features for facc anomaly detection"
    )
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--region", help="Region to extract (e.g., NA)")
    parser.add_argument(
        "--output",
        default="output/facc_detection/rf_features.parquet",
        help="Output parquet path",
    )
    parser.add_argument(
        "--info", action="store_true", help="Print feature info and exit"
    )

    args = parser.parse_args()

    with RFFeatureExtractor(args.db) as extractor:
        if args.info:
            info = extractor.get_feature_info()
            print("\nFeature Categories:")
            for cat, cols in info["categories"].items():
                print(f"  {cat}: {len(cols)} features")
                for col in cols:
                    print(f"    - {col}")
            print(f"\nNeighbor computed: {len(info['neighbor_computed'])} features")
            print(f"Derived: {len(info['derived'])} features")
            print(f"\nTotal: ~{info['total_features']} features")
        else:
            features = extract_rf_features(args.db, args.region, args.output)
            print(
                f"\nExtracted {len(features)} reaches with {len(features.columns)} features"
            )
