# -*- coding: utf-8 -*-
"""
SWORD DuckDB Class
==================

DuckDB-backed SWORD class - drop-in replacement for the original NetCDF-based class.
Provides the same interface for backward compatibility with existing pipelines.

Example Usage:
    from sword_duckdb import SWORD

    # Load from DuckDB
    sword = SWORD('data/duckdb/sword_v17b.duckdb', 'NA')

    # Access data (same interface as original)
    print(sword.reaches.wse[:5])
    print(sword.nodes.facc[:5])
    print(sword.centerlines.x[:5])

    # Topology arrays
    print(sword.reaches.rch_id_up[:, :5])
"""

from __future__ import annotations

import gc
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Union, Optional, List, Dict, Any, Generator
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from .reactive import SWORDReactive

from .column_order import NODES_COLUMN_ORDER, REACHES_COLUMN_ORDER
from .sword_db import SWORDDatabase
from .views import CenterlinesView, NodesView, ReachesView

logger = logging.getLogger(__name__)


class SWORD:
    """
    DuckDB-backed SWORD class.

    Drop-in replacement for the original NetCDF-based SWORD class.
    Loads data from DuckDB and provides numpy-array-style attribute access.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB database file.
    region : str
        Two-letter region code (NA, SA, EU, AF, OC, AS).
    version : str, optional
        SWORD version. Default is 'v17b'.

    Attributes
    ----------
    region : str
        The region code.
    version : str
        The SWORD version.
    centerlines : CenterlinesView
        Centerline data with numpy-array access.
    nodes : NodesView
        Node data with numpy-array access.
    reaches : ReachesView
        Reach data with numpy-array access.
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        region: str,
        version: str = "v17b",
        spatial: bool = True,
    ):
        self.region = region.upper()
        self.version = version
        self._db_path = Path(db_path)
        # Always load spatial for write operations (needed for RTREE indexes)
        self._db = SWORDDatabase(db_path, read_only=False, spatial=spatial)
        self._reactive = None  # Optional reactive system reference

        # Load data
        self._load_data()

    def _load_data(self) -> None:
        """Load all data for the region from DuckDB."""
        # Load main DataFrames
        self._centerlines_df = self._db.query(
            "SELECT * FROM centerlines WHERE region = ? ORDER BY cl_id", [self.region]
        )
        self._nodes_df = self._db.query(
            "SELECT * FROM nodes WHERE region = ? ORDER BY node_id", [self.region]
        )
        self._reaches_df = self._db.query(
            "SELECT * FROM reaches WHERE region = ? ORDER BY reach_id", [self.region]
        )

        # Reconstruct multi-dimensional arrays
        cl_reach_id, cl_node_id = self._reconstruct_centerline_neighbors()
        rch_id_up = self._reconstruct_reach_topology("up")
        rch_id_down = self._reconstruct_reach_topology("down")
        orbits = self._reconstruct_orbits()
        iceflag = self._reconstruct_iceflag()

        # Create view objects with db and region for write support
        # Also pass reactive reference if configured
        self._centerlines = CenterlinesView(
            self._centerlines_df,
            cl_reach_id,
            cl_node_id,
            db=self._db,
            region=self.region,
            reactive=self._reactive,
        )
        self._nodes = NodesView(
            self._nodes_df, db=self._db, region=self.region, reactive=self._reactive
        )
        self._reaches = ReachesView(
            self._reaches_df,
            rch_id_up,
            rch_id_down,
            orbits,
            iceflag,
            db=self._db,
            region=self.region,
            reactive=self._reactive,
        )

    def _reconstruct_centerline_neighbors(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Reconstruct [4,N] reach_id and node_id arrays from centerlines + neighbors.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            (reach_id[4,N], node_id[4,N]) arrays
        """
        n = len(self._centerlines_df)
        reach_id = np.zeros((4, n), dtype=np.int64)
        node_id = np.zeros((4, n), dtype=np.int64)

        # Row 0: primary from main table
        reach_id[0, :] = self._centerlines_df["reach_id"].values
        node_id[0, :] = self._centerlines_df["node_id"].values

        # Rows 1-3: from centerline_neighbors table
        neighbors_df = self._db.query(
            """
            SELECT cl_id, neighbor_rank, reach_id, node_id
            FROM centerline_neighbors
            WHERE region = ?
            ORDER BY cl_id, neighbor_rank
        """,
            [self.region],
        )

        if len(neighbors_df) > 0:
            # Build lookup from cl_id to array index
            cl_ids = self._centerlines_df["cl_id"].values
            cl_idx = {cid: i for i, cid in enumerate(cl_ids)}

            for _, row in neighbors_df.iterrows():
                idx = cl_idx.get(row["cl_id"])
                if idx is not None:
                    rank = row["neighbor_rank"]
                    if 1 <= rank <= 3:
                        reach_id[rank, idx] = row["reach_id"] if row["reach_id"] else 0
                        node_id[rank, idx] = row["node_id"] if row["node_id"] else 0

        return reach_id, node_id

    def _reconstruct_reach_topology(self, direction: str) -> np.ndarray:
        """
        Reconstruct [4,N] topology array from reach_topology table.

        Parameters
        ----------
        direction : str
            'up' for upstream, 'down' for downstream.

        Returns
        -------
        np.ndarray
            [4,N] array of neighbor reach IDs.
        """
        n = len(self._reaches_df)
        arr = np.zeros((4, n), dtype=np.int64)

        # Build lookup from reach_id to array index
        reach_ids = self._reaches_df["reach_id"].values
        reach_idx = {rid: i for i, rid in enumerate(reach_ids)}

        # Query topology
        topo_df = self._db.query(
            """
            SELECT reach_id, neighbor_rank, neighbor_reach_id
            FROM reach_topology
            WHERE direction = ? AND region = ?
        """,
            [direction, self.region],
        )

        for _, row in topo_df.iterrows():
            idx = reach_idx.get(row["reach_id"])
            if idx is not None:
                rank = row["neighbor_rank"]
                if 0 <= rank <= 3:
                    arr[rank, idx] = row["neighbor_reach_id"]

        return arr

    def _reconstruct_orbits(self) -> np.ndarray:
        """
        Reconstruct [75,N] SWOT orbits array from reach_swot_orbits table.

        Returns
        -------
        np.ndarray
            [75,N] array of orbit IDs.
        """
        n = len(self._reaches_df)
        arr = np.zeros((75, n), dtype=np.int64)

        reach_ids = self._reaches_df["reach_id"].values
        reach_idx = {rid: i for i, rid in enumerate(reach_ids)}

        orbits_df = self._db.query(
            """
            SELECT reach_id, orbit_rank, orbit_id
            FROM reach_swot_orbits
            WHERE region = ?
        """,
            [self.region],
        )

        for _, row in orbits_df.iterrows():
            idx = reach_idx.get(row["reach_id"])
            if idx is not None:
                rank = row["orbit_rank"]
                if 0 <= rank < 75:
                    arr[rank, idx] = row["orbit_id"]

        return arr

    def _reconstruct_iceflag(self) -> np.ndarray:
        """
        Reconstruct [366,N] ice flag array from reach_ice_flags table.

        Returns
        -------
        np.ndarray
            [366,N] array of daily ice flags.
        """
        n = len(self._reaches_df)
        arr = np.zeros((366, n), dtype=np.int32)

        reach_ids = self._reaches_df["reach_id"].values
        reach_idx = {rid: i for i, rid in enumerate(reach_ids)}

        # Ice flags may be large, query in batches if needed
        ice_df = self._db.query(
            """
            SELECT reach_id, julian_day, iceflag
            FROM reach_ice_flags
            WHERE reach_id IN (SELECT reach_id FROM reaches WHERE region = ?)
        """,
            [self.region],
        )

        for _, row in ice_df.iterrows():
            idx = reach_idx.get(row["reach_id"])
            if idx is not None:
                day = row["julian_day"] - 1  # Convert 1-366 to 0-365
                if 0 <= day < 366:
                    arr[day, idx] = row["iceflag"]

        return arr

    def set_reactive(self, reactive) -> None:
        """
        Set the reactive system for automatic change tracking.

        When a reactive system is configured, modifications to WritableArray
        properties (e.g., sword.reaches.dist_out[0] = value) will automatically
        call mark_dirty() on the reactive system, enabling cascading recalculation
        of dependent attributes.

        Parameters
        ----------
        reactive : SWORDReactive
            The reactive system instance to use for change tracking.

        Example
        -------
        >>> from sword_duckdb import SWORD, SWORDReactive
        >>> sword = SWORD('data/duckdb/sword_v17b.duckdb', 'NA')
        >>> reactive = SWORDReactive(sword)
        >>> sword.set_reactive(reactive)
        >>> sword.reaches.dist_out[0] = 1234.5  # Auto-calls mark_dirty()
        >>> reactive.recalculate()  # Recalculates all dependent attributes
        """
        self._reactive = reactive
        # Update views with the reactive reference
        self._centerlines._reactive = reactive
        self._nodes._reactive = reactive
        self._reaches._reactive = reactive

    @property
    def reactive(self) -> Optional["SWORDReactive"]:
        """Get the reactive system instance (if configured)."""
        return self._reactive

    @contextmanager
    def batch_modify(
        self, auto_commit: bool = True
    ) -> Generator[Optional["SWORDReactive"], None, None]:
        """
        Context manager for batch modifications with deferred recalculation.

        Within this context, modifications are tracked but reactive
        recalculation is deferred until the context exits (if auto_commit=True)
        or until commit() is explicitly called.

        Parameters
        ----------
        auto_commit : bool, optional
            If True (default), automatically run recalculation on exit.
            If False, changes are tracked but recalculation must be triggered
            manually via reactive.recalculate().

        Yields
        ------
        SWORDReactive or None
            The reactive system instance (if configured), allowing direct
            access to dirty_set and other reactive methods.

        Example
        -------
        >>> with sword.batch_modify():
        ...     sword.reaches.wse[0] = 100.0
        ...     sword.reaches.wse[1] = 101.0
        ...     sword.reaches.slope[0] = 0.001
        ... # Recalculation runs once at exit

        >>> # Or without auto-commit:
        >>> with sword.batch_modify(auto_commit=False) as reactive:
        ...     sword.reaches.wse[0] = 100.0
        ...     # Check what's dirty:
        ...     print(reactive.dirty_set.dirty_attributes)
        ... # No recalculation - call reactive.recalculate() manually
        """
        if self._reactive is None:
            # No reactive system - just yield None
            yield None
            return

        try:
            yield self._reactive
        finally:
            if auto_commit and len(self._reactive._dirty_attrs) > 0:
                self._reactive.recalculate()

    @property
    def centerlines(self) -> CenterlinesView:
        """Centerline data with numpy-array access."""
        return self._centerlines

    @property
    def nodes(self) -> NodesView:
        """Node data with numpy-array access."""
        return self._nodes

    @property
    def reaches(self) -> ReachesView:
        """Reach data with numpy-array access."""
        return self._reaches

    @property
    def db(self) -> SWORDDatabase:
        """Get the underlying database connection."""
        return self._db

    @property
    def paths(self) -> dict:
        """
        Backward-compatible paths dictionary for file I/O operations.

        Returns the same directory structure as the original SWORD class,
        derived from the DuckDB database path.

        Returns
        -------
        dict
            Dictionary containing all SWORD file paths and filenames.
        """
        # Derive main_dir from db_path: data/duckdb/sword.duckdb -> project root
        main_dir = str(self._db_path.parent.parent.parent)
        region = self.region
        version = self.version

        paths = {}

        # Directory paths
        paths["shp_dir"] = (
            f"{main_dir}/data/outputs/Reaches_Nodes/{version}/shp/{region}/"
        )
        paths["gpkg_dir"] = f"{main_dir}/data/outputs/Reaches_Nodes/{version}/gpkg/"
        paths["nc_dir"] = f"{main_dir}/data/outputs/Reaches_Nodes/{version}/netcdf/"
        paths["geom_dir"] = (
            f"{main_dir}/data/outputs/Reaches_Nodes/{version}/reach_geometry/"
        )
        paths["update_dir"] = f"{main_dir}/data/update_requests/{version}/{region}/"
        paths["topo_dir"] = f"{main_dir}/data/outputs/Topology/{version}/{region}/"
        paths["version_dir"] = f"{main_dir}/data/outputs/Version_Differences/{version}/"
        paths["pts_gpkg_dir"] = (
            f"{main_dir}/data/outputs/Reaches_Nodes/{version}/gpkg_30m/{region}/"
        )

        # Filenames
        paths["nc_fn"] = f"{region.lower()}_sword_{version}.nc"
        paths["gpkg_rch_fn"] = f"{region.lower()}_sword_reaches_{version}.gpkg"
        paths["gpkg_node_fn"] = f"{region.lower()}_sword_nodes_{version}.gpkg"
        paths["shp_rch_fn"] = f"{region.lower()}_sword_reaches_hbXX_{version}.shp"
        paths["shp_node_fn"] = f"{region.lower()}_sword_nodes_hbXX_{version}.shp"
        paths["geom_fn"] = f"{region.lower()}_sword_{version}_connectivity.nc"

        return paths

    def copy(self) -> None:
        """
        Create a backup of the current state.

        For DuckDB, this creates a checkpoint and stores a backup timestamp.
        The database file can be copied externally if needed.
        """
        current_datetime = datetime.now()
        backup_note = (
            f"Backup created at {current_datetime.strftime('%Y-%m-%d_%H-%M-%S')}"
        )

        # Create a checkpoint to ensure data is flushed
        self._db.execute("CHECKPOINT")

        # Record backup in versions table
        self._db.execute(
            """
            INSERT OR REPLACE INTO sword_versions (version, notes)
            VALUES (?, ?)
        """,
            [f"backup_{current_datetime.strftime('%Y%m%d_%H%M%S')}", backup_note],
        )

        logger.info(f"Checkpoint created: {backup_note}")

    def delete_data(self, rm_rch: Union[List[int], np.ndarray]) -> None:
        """
        Delete reaches and associated data across all dimensions.

        This method performs a cascade deletion of:
        - Reaches (main table)
        - Centerlines associated with those reaches
        - Centerline neighbors for those centerlines
        - Nodes associated with those reaches
        - Reach topology entries (both as source and as neighbor)
        - SWOT orbit data for those reaches
        - Ice flag data for those reaches

        After deletion, topology counts (n_rch_up, n_rch_down) are updated
        for any reaches that were neighbors of the deleted reaches.

        Parameters
        ----------
        rm_rch : list or np.ndarray
            Reach IDs to delete.

        Notes
        -----
        - Creates a database backup timestamp before deletion
        - Uses transaction semantics (all-or-nothing)
        - Automatically updates topology counts for remaining reaches
        """
        if len(rm_rch) == 0:
            return

        rm_rch = np.array(rm_rch)

        # Disable GC during database operations to avoid DuckDB segfaults
        gc_was_enabled = gc.isenabled()
        gc.disable()

        # Start transaction
        conn = self._db.connect()
        conn.execute("BEGIN TRANSACTION")

        try:
            # Convert to list for SQL IN clause
            rm_list = rm_rch.tolist()

            # IMPORTANT: Get centerline IDs BEFORE deleting centerlines
            # This is needed for cleaning up centerline_neighbors
            cl_ids_result = conn.execute(
                f"""
                SELECT cl_id FROM centerlines
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            ).fetchall()
            cl_ids_to_delete = [row[0] for row in cl_ids_result]

            # Delete centerline_neighbors FIRST (before centerlines are deleted)
            if cl_ids_to_delete:
                conn.execute(
                    f"""
                    DELETE FROM centerline_neighbors
                    WHERE cl_id IN ({",".join("?" * len(cl_ids_to_delete))})
                    AND region = ?
                """,
                    cl_ids_to_delete + [self.region],
                )

            # Delete from centerlines
            conn.execute(
                f"""
                DELETE FROM centerlines
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Delete from nodes
            conn.execute(
                f"""
                DELETE FROM nodes
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Delete from reach_topology (both as source and neighbor)
            conn.execute(
                f"""
                DELETE FROM reach_topology
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Update topology: remove deleted reaches from neighbor lists
            conn.execute(
                f"""
                DELETE FROM reach_topology
                WHERE neighbor_reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Delete from reach_swot_orbits
            conn.execute(
                f"""
                DELETE FROM reach_swot_orbits
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Delete from reach_ice_flags
            conn.execute(
                f"""
                DELETE FROM reach_ice_flags
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
            """,
                rm_list,
            )

            # Delete from reaches (main table)
            conn.execute(
                f"""
                DELETE FROM reaches
                WHERE reach_id IN ({",".join("?" * len(rm_list))})
                AND region = ?
            """,
                rm_list + [self.region],
            )

            # Update n_rch_up and n_rch_down counts for affected reaches
            conn.execute(
                """
                UPDATE reaches
                SET n_rch_up = (
                    SELECT COUNT(*) FROM reach_topology t
                    WHERE t.reach_id = reaches.reach_id
                    AND t.region = reaches.region
                    AND t.direction = 'up'
                )
                WHERE region = ?
            """,
                [self.region],
            )

            conn.execute(
                """
                UPDATE reaches
                SET n_rch_down = (
                    SELECT COUNT(*) FROM reach_topology t
                    WHERE t.reach_id = reaches.reach_id
                    AND t.region = reaches.region
                    AND t.direction = 'down'
                )
                WHERE region = ?
            """,
                [self.region],
            )

            conn.execute("COMMIT")

            # Reload data
            self._load_data()

            logger.info(f"Deleted {len(rm_rch)} reaches and associated data")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to delete data: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def delete_rchs(self, rm_rch: Union[List[int], np.ndarray]) -> None:
        """
        Delete reaches only (without cascading to centerlines/nodes).

        Parameters
        ----------
        rm_rch : list or numpy.ndarray
            List of reach IDs to delete.
        """
        import gc

        rm_rch = np.array(rm_rch)
        if len(rm_rch) == 0:
            return

        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()
        try:
            conn.execute("BEGIN TRANSACTION")

            for rid in rm_rch:
                rid = int(rid)
                # Delete from normalized tables
                conn.execute(
                    "DELETE FROM reach_topology WHERE reach_id = ? AND region = ?",
                    [rid, self.region],
                )
                conn.execute(
                    "DELETE FROM reach_swot_orbits WHERE reach_id = ? AND region = ?",
                    [rid, self.region],
                )
                conn.execute("DELETE FROM reach_ice_flags WHERE reach_id = ?", [rid])
                # Delete the reach
                conn.execute(
                    "DELETE FROM reaches WHERE reach_id = ? AND region = ?",
                    [rid, self.region],
                )

            conn.execute("COMMIT")
            self._load_data()
            logger.info(f"Deleted {len(rm_rch)} reaches")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to delete reaches: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def delete_nodes(self, node_ids: Union[List[int], np.ndarray]) -> None:
        """
        Delete nodes by node ID.

        Parameters
        ----------
        node_ids : list or numpy.ndarray
            List of node IDs to delete.
        """
        import gc

        node_ids = np.array(node_ids)
        if len(node_ids) == 0:
            return

        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()
        try:
            conn.execute("BEGIN TRANSACTION")

            for nid in node_ids:
                conn.execute(
                    "DELETE FROM nodes WHERE node_id = ? AND region = ?",
                    [int(nid), self.region],
                )

            conn.execute("COMMIT")
            self._load_data()
            logger.info(f"Deleted {len(node_ids)} nodes")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to delete nodes: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def append_nodes(self, subnodes) -> None:
        """
        Append nodes to the database.

        Parameters
        ----------
        subnodes : object
            Object containing node data with attributes matching NodesView.
        """
        import gc

        if len(subnodes.id) == 0:
            return

        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()
        try:
            conn.execute("BEGIN TRANSACTION")
            self._insert_nodes(conn, subnodes)
            conn.execute("COMMIT")
            self._load_data()
            logger.info(f"Appended {len(subnodes.id)} nodes")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to append nodes: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def save_vectors(
        self, export: str = "All", output_dir: Optional[str] = None
    ) -> None:
        """
        Save SWORD data to vector formats (GeoPackage and/or Shapefile).

        Parameters
        ----------
        export : str
            'All' - writes both reach and node files.
            'nodes' - writes node files only.
            'reaches' - writes reach files only.
        output_dir : str, optional
            Output directory. If not provided, uses current directory.
        """
        try:
            import geopandas as gpd  # noqa: F401
            from shapely.geometry import Point, LineString  # noqa: F401
        except ImportError:
            raise ImportError("geopandas and shapely are required for save_vectors()")

        if output_dir is None:
            output_dir = Path(".")
        else:
            output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if export in ("All", "reaches"):
            logger.info("Creating reach geometries...")
            self._save_reach_vectors(output_dir)

        if export in ("All", "nodes"):
            logger.info("Creating node geometries...")
            self._save_node_vectors(output_dir)

    def _save_reach_vectors(self, output_dir: Path) -> None:
        """Save reach data to GeoPackage and Shapefile."""
        import geopandas as gpd
        from shapely.geometry import LineString

        # Build reach geometries from centerlines
        reach_geoms = {}
        for rid in self.reaches.id:
            cl_idx = np.where(self.centerlines.reach_id[0, :] == rid)[0]
            if len(cl_idx) > 1:
                # Sort by cl_id
                sorted_idx = cl_idx[np.argsort(self.centerlines.cl_id[cl_idx])]
                coords = list(
                    zip(self.centerlines.x[sorted_idx], self.centerlines.y[sorted_idx])
                )
                if len(coords) >= 2:
                    reach_geoms[rid] = LineString(coords)
                else:
                    reach_geoms[rid] = None
            else:
                reach_geoms[rid] = None

        # Build GeoDataFrame
        data = {
            "reach_id": self.reaches.id,
            "x": self.reaches.x,
            "y": self.reaches.y,
            "reach_len": self.reaches.len,
            "wse": self.reaches.wse,
            "width": self.reaches.wth,
            "slope": self.reaches.slope,
            "n_nodes": self.reaches.n_nodes,
            "facc": self.reaches.facc,
            "dist_out": self.reaches.dist_out,
            "river_name": self.reaches.river_name,
        }
        gdf = gpd.GeoDataFrame(
            data,
            geometry=[reach_geoms.get(rid) for rid in self.reaches.id],
            crs="EPSG:4326",
        )
        # Remove rows with null geometry
        gdf = gdf[gdf.geometry.notna()]

        # Save
        gpkg_path = output_dir / f"sword_{self.region}_reaches.gpkg"
        shp_path = output_dir / f"sword_{self.region}_reaches.shp"
        gdf.to_file(gpkg_path, driver="GPKG")
        gdf.to_file(shp_path, driver="ESRI Shapefile")
        logger.info(f"Saved reaches to {gpkg_path} and {shp_path}")

    def _save_node_vectors(self, output_dir: Path) -> None:
        """Save node data to GeoPackage and Shapefile."""
        import geopandas as gpd
        from shapely.geometry import Point

        # Build GeoDataFrame
        data = {
            "node_id": self.nodes.id,
            "x": self.nodes.x,
            "y": self.nodes.y,
            "reach_id": self.nodes.reach_id,
            "node_len": self.nodes.len,
            "wse": self.nodes.wse,
            "width": self.nodes.wth,
            "facc": self.nodes.facc,
            "dist_out": self.nodes.dist_out,
            "river_name": self.nodes.river_name,
        }
        geometry = [Point(x, y) for x, y in zip(self.nodes.x, self.nodes.y)]
        gdf = gpd.GeoDataFrame(data, geometry=geometry, crs="EPSG:4326")

        # Save
        gpkg_path = output_dir / f"sword_{self.region}_nodes.gpkg"
        shp_path = output_dir / f"sword_{self.region}_nodes.shp"
        gdf.to_file(gpkg_path, driver="GPKG")
        gdf.to_file(shp_path, driver="ESRI Shapefile")
        logger.info(f"Saved nodes to {gpkg_path} and {shp_path}")

    def save_nc(self, output_path: Optional[str] = None) -> None:
        """
        Export data to NetCDF format for backward compatibility.

        Parameters
        ----------
        output_path : str, optional
            Output file path. If not provided, uses default naming.
        """
        try:
            import netCDF4 as nc
        except ImportError:
            raise ImportError(
                "netCDF4 is required for save_nc(). Install with: pip install netCDF4"
            )

        if output_path is None:
            output_path = f"sword_{self.region}_{self.version}.nc"

        # Create NetCDF file
        with nc.Dataset(output_path, "w", format="NETCDF4") as ds:
            # Global attributes
            ds.title = f"SWORD {self.version} - {self.region}"
            ds.created = datetime.now().isoformat()
            ds.source = "Exported from DuckDB"

            # Centerlines group
            cl_grp = ds.createGroup("centerlines")
            n_cl = len(self._centerlines)
            cl_grp.createDimension("num_centerlines", n_cl)
            cl_grp.createDimension("num_neighbors", 4)

            cl_grp.createVariable("cl_id", "i8", ("num_centerlines",))[:] = (
                self._centerlines.cl_id
            )
            cl_grp.createVariable("x", "f8", ("num_centerlines",))[:] = (
                self._centerlines.x
            )
            cl_grp.createVariable("y", "f8", ("num_centerlines",))[:] = (
                self._centerlines.y
            )
            cl_grp.createVariable(
                "reach_id", "i8", ("num_neighbors", "num_centerlines")
            )[:] = self._centerlines.reach_id
            cl_grp.createVariable(
                "node_id", "i8", ("num_neighbors", "num_centerlines")
            )[:] = self._centerlines.node_id

            # Nodes group
            nd_grp = ds.createGroup("nodes")
            n_nd = len(self._nodes)
            nd_grp.createDimension("num_nodes", n_nd)
            nd_grp.createDimension("num_cl_bounds", 2)

            nd_grp.createVariable("node_id", "i8", ("num_nodes",))[:] = self._nodes.id
            nd_grp.createVariable("x", "f8", ("num_nodes",))[:] = self._nodes.x
            nd_grp.createVariable("y", "f8", ("num_nodes",))[:] = self._nodes.y
            nd_grp.createVariable("reach_id", "i8", ("num_nodes",))[:] = (
                self._nodes.reach_id
            )
            nd_grp.createVariable("cl_id", "i8", ("num_cl_bounds", "num_nodes"))[:] = (
                self._nodes.cl_id
            )
            nd_grp.createVariable("len", "f8", ("num_nodes",))[:] = self._nodes.len
            nd_grp.createVariable("wse", "f8", ("num_nodes",))[:] = self._nodes.wse
            nd_grp.createVariable("wse_var", "f8", ("num_nodes",))[:] = (
                self._nodes.wse_var
            )
            nd_grp.createVariable("wth", "f8", ("num_nodes",))[:] = self._nodes.wth
            nd_grp.createVariable("wth_var", "f8", ("num_nodes",))[:] = (
                self._nodes.wth_var
            )
            nd_grp.createVariable("max_wth", "f8", ("num_nodes",))[:] = (
                self._nodes.max_wth
            )
            nd_grp.createVariable("facc", "f8", ("num_nodes",))[:] = self._nodes.facc
            nd_grp.createVariable("dist_out", "f8", ("num_nodes",))[:] = (
                self._nodes.dist_out
            )
            nd_grp.createVariable("grod", "i4", ("num_nodes",))[:] = self._nodes.grod
            nd_grp.createVariable("lakeflag", "i4", ("num_nodes",))[:] = (
                self._nodes.lakeflag
            )
            nd_grp.createVariable("strm_order", "i4", ("num_nodes",))[:] = (
                self._nodes.strm_order
            )
            nd_grp.createVariable("end_rch", "i4", ("num_nodes",))[:] = (
                self._nodes.end_rch
            )

            # Reaches group
            rch_grp = ds.createGroup("reaches")
            n_rch = len(self._reaches)
            rch_grp.createDimension("num_reaches", n_rch)
            rch_grp.createDimension("num_rch_neighbors", 4)
            rch_grp.createDimension("num_cl_bounds", 2)
            rch_grp.createDimension("num_orbits", 75)
            rch_grp.createDimension("num_days", 366)

            rch_grp.createVariable("reach_id", "i8", ("num_reaches",))[:] = (
                self._reaches.id
            )
            rch_grp.createVariable("x", "f8", ("num_reaches",))[:] = self._reaches.x
            rch_grp.createVariable("y", "f8", ("num_reaches",))[:] = self._reaches.y
            rch_grp.createVariable("cl_id", "i8", ("num_cl_bounds", "num_reaches"))[
                :
            ] = self._reaches.cl_id
            rch_grp.createVariable("len", "f8", ("num_reaches",))[:] = self._reaches.len
            rch_grp.createVariable("wse", "f8", ("num_reaches",))[:] = self._reaches.wse
            rch_grp.createVariable("wth", "f8", ("num_reaches",))[:] = self._reaches.wth
            rch_grp.createVariable("slope", "f8", ("num_reaches",))[:] = (
                self._reaches.slope
            )
            rch_grp.createVariable("facc", "f8", ("num_reaches",))[:] = (
                self._reaches.facc
            )
            rch_grp.createVariable("dist_out", "f8", ("num_reaches",))[:] = (
                self._reaches.dist_out
            )
            rch_grp.createVariable("n_rch_up", "i4", ("num_reaches",))[:] = (
                self._reaches.n_rch_up
            )
            rch_grp.createVariable("n_rch_down", "i4", ("num_reaches",))[:] = (
                self._reaches.n_rch_down
            )
            rch_grp.createVariable(
                "rch_id_up", "i8", ("num_rch_neighbors", "num_reaches")
            )[:] = self._reaches.rch_id_up
            rch_grp.createVariable(
                "rch_id_down", "i8", ("num_rch_neighbors", "num_reaches")
            )[:] = self._reaches.rch_id_down
            rch_grp.createVariable("orbits", "i8", ("num_orbits", "num_reaches"))[:] = (
                self._reaches.orbits
            )
            rch_grp.createVariable("iceflag", "i4", ("num_days", "num_reaches"))[:] = (
                self._reaches.iceflag
            )
            rch_grp.createVariable("strm_order", "i4", ("num_reaches",))[:] = (
                self._reaches.strm_order
            )
            rch_grp.createVariable("end_rch", "i4", ("num_reaches",))[:] = (
                self._reaches.end_rch
            )

        logger.info(f"Exported to {output_path}")

    def validate_reach_id(self, reach_id: int) -> bool:
        """
        Validate that a reach ID follows the SWORD format: CBBBBBRRRRT.

        Parameters
        ----------
        reach_id : int
            The reach ID to validate.

        Returns
        -------
        bool
            True if valid, False otherwise.

        Notes
        -----
        Format: CBBBBBRRRRT (11 digits)
        - C: Continent code (1-9)
        - BBBBB: Basin code (5 digits)
        - RRRR: Reach number within basin (4 digits)
        - T: Type flag (1=river, 2=lake, 3=lake on river, 4=dam, 5=delta, 6=ghost)
        """
        rid_str = str(reach_id)
        if len(rid_str) != 11:
            return False

        # Check continent code (must be 1-9)
        if not rid_str[0].isdigit() or rid_str[0] == "0":
            return False

        # Check basin code (5 digits, can include leading zeros)
        if not rid_str[1:6].isdigit():
            return False

        # Check reach number (4 digits)
        if not rid_str[6:10].isdigit():
            return False

        # Check type flag (1-6)
        type_flag = rid_str[-1]
        if type_flag not in "123456":
            return False

        return True

    def validate_node_id(self, node_id: int) -> bool:
        """
        Validate that a node ID follows the SWORD format: CBBBBBRRRRNNNT.

        Parameters
        ----------
        node_id : int
            The node ID to validate.

        Returns
        -------
        bool
            True if valid, False otherwise.

        Notes
        -----
        Format: CBBBBBRRRRNNNT (14 digits)
        - C: Continent code (1-9)
        - BBBBB: Basin code (5 digits)
        - RRRR: Reach number within basin (4 digits)
        - NNN: Node number within reach (3 digits)
        - T: Type flag (1=river, 2=lake, 3=lake on river, 4=dam, 5=delta, 6=ghost)
        """
        nid_str = str(node_id)
        if len(nid_str) != 14:
            return False

        # Check continent code (must be 1-9)
        if not nid_str[0].isdigit() or nid_str[0] == "0":
            return False

        # Check basin code (5 digits)
        if not nid_str[1:6].isdigit():
            return False

        # Check reach number (4 digits)
        if not nid_str[6:10].isdigit():
            return False

        # Check node number (3 digits)
        if not nid_str[10:13].isdigit():
            return False

        # Check type flag (1-6)
        type_flag = nid_str[-1]
        if type_flag not in "123456":
            return False

        return True

    def append_data(
        self, subcls, subnodes, subreaches, validate_ids: bool = True
    ) -> None:
        """
        Append new centerlines, nodes, and reaches to the database.

        Parameters
        ----------
        subcls : object
            Object containing centerlines data with attributes:
            cl_id, lon/x, lat/y, reach_id[4,N], node_id[4,N]
        subnodes : object
            Object containing nodes data with attributes matching NodesView.
        subreaches : object
            Object containing reaches data with attributes matching ReachesView.
        validate_ids : bool, optional
            If True (default), validate that reach and node IDs follow SWORD format.
            Set to False to skip validation (e.g., for migration operations).

        Raises
        ------
        ValueError
            If validate_ids is True and any IDs fail format validation.
        RuntimeError
            If the database operation fails.

        Notes
        -----
        ID Formats:
        - Reach ID: CBBBBBRRRRT (11 digits)
          C=continent, BBBBB=basin, RRRR=reach num, T=type
        - Node ID: CBBBBBRRRRNNNT (14 digits)
          Same prefix as reach, NNN=node number, T=type
        - Type flags: 1=river, 2=lake, 3=lake-on-river, 4=dam, 5=delta, 6=ghost
        """
        import gc

        if len(subcls.cl_id) == 0 and len(subnodes.id) == 0 and len(subreaches.id) == 0:
            return

        # Validate IDs if requested
        if validate_ids:
            # Validate reach IDs
            if len(subreaches.id) > 0:
                invalid_reaches = []
                for rid in subreaches.id:
                    if not self.validate_reach_id(int(rid)):
                        invalid_reaches.append(rid)
                if invalid_reaches:
                    raise ValueError(
                        f"Invalid reach ID format. Expected CBBBBBRRRRT (11 digits). "
                        f"Invalid IDs: {invalid_reaches[:5]}..."
                        if len(invalid_reaches) > 5
                        else f"Invalid reach ID format. Expected CBBBBBRRRRT (11 digits). "
                        f"Invalid IDs: {invalid_reaches}"
                    )

            # Validate node IDs
            if len(subnodes.id) > 0:
                invalid_nodes = []
                for nid in subnodes.id:
                    if not self.validate_node_id(int(nid)):
                        invalid_nodes.append(nid)
                if invalid_nodes:
                    raise ValueError(
                        f"Invalid node ID format. Expected CBBBBBRRRRNNNT (14 digits). "
                        f"Invalid IDs: {invalid_nodes[:5]}..."
                        if len(invalid_nodes) > 5
                        else f"Invalid node ID format. Expected CBBBBBRRRRNNNT (14 digits). "
                        f"Invalid IDs: {invalid_nodes}"
                    )

            # Check for duplicate IDs with existing data
            if len(subreaches.id) > 0:
                existing_reaches = set(self.reaches.id)
                duplicates = [
                    rid for rid in subreaches.id if int(rid) in existing_reaches
                ]
                if duplicates:
                    raise ValueError(
                        f"Duplicate reach IDs found. These already exist: {duplicates[:5]}..."
                        if len(duplicates) > 5
                        else f"Duplicate reach IDs found. These already exist: {duplicates}"
                    )

            if len(subnodes.id) > 0:
                existing_nodes = set(self.nodes.id)
                duplicates = [nid for nid in subnodes.id if int(nid) in existing_nodes]
                if duplicates:
                    raise ValueError(
                        f"Duplicate node IDs found. These already exist: {duplicates[:5]}..."
                        if len(duplicates) > 5
                        else f"Duplicate node IDs found. These already exist: {duplicates}"
                    )

        conn = self._db.connect()
        conn.execute("BEGIN TRANSACTION")

        # Disable gc during bulk insert to avoid DuckDB segfaults
        gc_was_enabled = gc.isenabled()
        gc.disable()

        try:
            # Insert centerlines
            if len(subcls.cl_id) > 0:
                self._insert_centerlines(conn, subcls)

            # Insert nodes
            if len(subnodes.id) > 0:
                self._insert_nodes(conn, subnodes)

            # Insert reaches (includes topology, orbits, ice flags)
            if len(subreaches.id) > 0:
                self._insert_reaches(conn, subreaches)

            conn.execute("COMMIT")

            # Reload data to refresh views
            self._load_data()

            logger.info(
                f"Appended {len(subcls.cl_id)} centerlines, "
                f"{len(subnodes.id)} nodes, {len(subreaches.id)} reaches"
            )

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to append data: {e}") from e
        finally:
            # Re-enable gc if it was enabled before
            if gc_was_enabled:
                gc.enable()

    def _to_python(self, val) -> Any:
        """Convert numpy types to Python types for DuckDB."""
        if hasattr(val, "item"):
            return val.item()
        return val

    def _insert_centerlines(self, conn, subcls) -> None:
        """Insert centerlines and neighbors into database."""
        n = len(subcls.cl_id)

        # Handle both lon/lat and x/y attribute names
        x_vals = getattr(subcls, "x", None)
        if x_vals is None:
            x_vals = getattr(subcls, "lon", None)
        y_vals = getattr(subcls, "y", None)
        if y_vals is None:
            y_vals = getattr(subcls, "lat", None)

        # Build rows for insertion
        rows = []
        for i in range(n):
            rows.append(
                (
                    int(subcls.cl_id[i]),
                    self.region,
                    float(x_vals[i]),
                    float(y_vals[i]),
                    int(subcls.reach_id[0, i]),
                    int(subcls.node_id[0, i]),
                    self.version,
                )
            )

        # Insert main centerlines using executemany
        conn.executemany(
            """
            INSERT INTO centerlines (cl_id, region, x, y, reach_id, node_id, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            rows,
        )

        # Insert neighbors (rows 1-3 of reach_id/node_id arrays)
        neighbor_rows = []
        for i in range(n):
            for rank in range(1, 4):
                rid = (
                    int(subcls.reach_id[rank, i])
                    if subcls.reach_id.shape[0] > rank
                    else 0
                )
                nid = (
                    int(subcls.node_id[rank, i])
                    if subcls.node_id.shape[0] > rank
                    else 0
                )
                if rid != 0 or nid != 0:  # Only store non-zero neighbors
                    neighbor_rows.append(
                        (
                            int(subcls.cl_id[i]),
                            self.region,
                            rank,
                            rid,
                            nid,
                        )
                    )

        if neighbor_rows:
            conn.executemany(
                """
                INSERT INTO centerline_neighbors (cl_id, region, neighbor_rank, reach_id, node_id)
                VALUES (?, ?, ?, ?, ?)
            """,
                neighbor_rows,
            )

        # Update geometry only for newly inserted rows (skip if spatial extension unavailable)
        try:
            cl_ids = [int(cid) for cid in subcls.cl_id]
            placeholders = ", ".join(["?"] * len(cl_ids))
            conn.execute(
                f"""
                UPDATE centerlines
                SET geom = ST_Point(x, y)
                WHERE region = ? AND cl_id IN ({placeholders}) AND geom IS NULL
            """,
                [self.region] + cl_ids,
            )
        except Exception:
            pass  # Spatial extension may not be available

    def _insert_nodes(self, conn, subnodes) -> None:
        """Insert nodes into database."""
        import pandas as pd

        n = len(subnodes.id)

        # Build cl_id bounds from [2,N] array
        cl_id_min = subnodes.cl_id[0, :] if subnodes.cl_id.ndim == 2 else subnodes.cl_id
        cl_id_max = subnodes.cl_id[1, :] if subnodes.cl_id.ndim == 2 else subnodes.cl_id

        # Helper to safely get attribute with fallback
        def get_attr(obj, names, default=None):
            """Get attribute by trying multiple names."""
            if isinstance(names, str):
                names = [names]
            for name in names:
                val = getattr(obj, name, None)
                if val is not None:
                    return val
            if default is not None:
                return [default] * n
            return [None] * n

        nodes_data = {
            "node_id": subnodes.id,
            "region": [self.region] * n,
            "x": subnodes.x,
            "y": subnodes.y,
            "cl_id_min": cl_id_min,
            "cl_id_max": cl_id_max,
            "reach_id": subnodes.reach_id,
            "node_length": get_attr(subnodes, "len", 0),
            "wse": get_attr(subnodes, "wse", 0),
            "wse_var": get_attr(subnodes, "wse_var", 0),
            "width": get_attr(subnodes, ["wth", "width"], 0),
            "width_var": get_attr(subnodes, ["wth_var", "width_var"], 0),
            "max_width": get_attr(subnodes, ["max_wth", "max_width"], 0),
            "facc": get_attr(subnodes, "facc", 0),
            "dist_out": get_attr(subnodes, "dist_out", 0),
            "lakeflag": get_attr(subnodes, "lakeflag", 0),
            "obstr_type": get_attr(subnodes, ["grod", "obstr_type"], 0),
            "grod_id": get_attr(subnodes, ["grod_fid", "grod_id"], 0),
            "hfalls_id": get_attr(subnodes, ["hfalls_fid", "hfalls_id"], 0),
            "n_chan_max": get_attr(subnodes, ["nchan_max", "n_chan_max"], 0),
            "n_chan_mod": get_attr(subnodes, ["nchan_mod", "n_chan_mod"], 0),
            "wth_coef": get_attr(subnodes, "wth_coef", 0),
            "ext_dist_coef": get_attr(subnodes, "ext_dist_coef", 0),
            "meander_length": get_attr(subnodes, ["meand_len", "meander_length"], 0),
            "sinuosity": get_attr(subnodes, "sinuosity", 0),
            "river_name": get_attr(subnodes, "river_name", ""),
            "manual_add": get_attr(subnodes, "manual_add", 0),
            "edit_flag": get_attr(subnodes, "edit_flag", ""),
            "trib_flag": get_attr(subnodes, "trib_flag", 0),
            "path_freq": get_attr(subnodes, "path_freq", 0),
            "path_order": get_attr(subnodes, "path_order", 0),
            "path_segs": get_attr(subnodes, "path_segs", 0),
            "stream_order": get_attr(subnodes, ["strm_order", "stream_order"], 0),
            "main_side": get_attr(subnodes, "main_side", 0),
            "end_reach": get_attr(subnodes, ["end_rch", "end_reach"], 0),
            "network": get_attr(subnodes, "network", 0),
            "add_flag": get_attr(subnodes, "add_flag", 0),
            "version": [self.version] * n,
        }

        nodes_df = pd.DataFrame(nodes_data)

        # Reorder columns to match canonical order (only columns present in df)
        cols = [c for c in NODES_COLUMN_ORDER if c in nodes_df.columns]
        nodes_df = nodes_df[cols]

        # Insert nodes using executemany for reliability
        placeholders = ", ".join(["?"] * len(cols))
        conn.executemany(
            f"""
            INSERT INTO nodes ({", ".join(cols)})
            VALUES ({placeholders})
        """,
            nodes_df.values.tolist(),
        )

        # Update geometry only for newly inserted rows (skip if spatial extension unavailable)
        try:
            node_ids = [int(nid) for nid in subnodes.id]
            placeholders = ", ".join(["?"] * len(node_ids))
            conn.execute(
                f"""
                UPDATE nodes
                SET geom = ST_Point(x, y)
                WHERE region = ? AND node_id IN ({placeholders}) AND geom IS NULL
            """,
                [self.region] + node_ids,
            )
        except Exception:
            pass  # Spatial extension may not be available

    def _insert_reaches(self, conn, subreaches) -> None:
        """Insert reaches and related normalized tables."""
        import pandas as pd

        n = len(subreaches.id)

        # Build cl_id bounds from [2,N] array
        cl_id_min = (
            subreaches.cl_id[0, :] if subreaches.cl_id.ndim == 2 else subreaches.cl_id
        )
        cl_id_max = (
            subreaches.cl_id[1, :] if subreaches.cl_id.ndim == 2 else subreaches.cl_id
        )

        # Helper to safely get attribute with fallback
        def get_attr(obj, names, default=None):
            if isinstance(names, str):
                names = [names]
            for name in names:
                val = getattr(obj, name, None)
                if val is not None:
                    return val
            if default is not None:
                return [default] * n
            return [None] * n

        reaches_data = {
            "reach_id": subreaches.id,
            "region": [self.region] * n,
            "x": subreaches.x,
            "y": subreaches.y,
            "x_min": get_attr(subreaches, "x_min", 0),
            "x_max": get_attr(subreaches, "x_max", 0),
            "y_min": get_attr(subreaches, "y_min", 0),
            "y_max": get_attr(subreaches, "y_max", 0),
            "cl_id_min": cl_id_min,
            "cl_id_max": cl_id_max,
            "reach_length": get_attr(subreaches, "len", 0),
            "n_nodes": get_attr(subreaches, ["rch_n_nodes", "n_nodes"], 0),
            "wse": get_attr(subreaches, "wse", 0),
            "wse_var": get_attr(subreaches, "wse_var", 0),
            "width": get_attr(subreaches, ["wth", "width"], 0),
            "width_var": get_attr(subreaches, ["wth_var", "width_var"], 0),
            "slope": get_attr(subreaches, "slope", 0),
            "max_width": get_attr(subreaches, ["max_wth", "max_width"], 0),
            "facc": get_attr(subreaches, "facc", 0),
            "dist_out": get_attr(subreaches, "dist_out", 0),
            "lakeflag": get_attr(subreaches, "lakeflag", 0),
            "obstr_type": get_attr(subreaches, ["grod", "obstr_type"], 0),
            "grod_id": get_attr(subreaches, ["grod_fid", "grod_id"], 0),
            "hfalls_id": get_attr(subreaches, ["hfalls_fid", "hfalls_id"], 0),
            "n_chan_max": get_attr(subreaches, ["nchan_max", "n_chan_max"], 0),
            "n_chan_mod": get_attr(subreaches, ["nchan_mod", "n_chan_mod"], 0),
            "n_rch_up": get_attr(subreaches, "n_rch_up", 0),
            "n_rch_down": get_attr(subreaches, "n_rch_down", 0),
            "swot_obs": get_attr(subreaches, ["max_obs", "swot_obs"], 0),
            "iceflag": get_attr(subreaches, "iceflag_scalar", 0),  # Scalar version
            "low_slope_flag": get_attr(subreaches, ["low_slope", "low_slope_flag"], 0),
            "river_name": get_attr(subreaches, "river_name", ""),
            "edit_flag": get_attr(subreaches, "edit_flag", ""),
            "trib_flag": get_attr(subreaches, "trib_flag", 0),
            "path_freq": get_attr(subreaches, "path_freq", 0),
            "path_order": get_attr(subreaches, "path_order", 0),
            "path_segs": get_attr(subreaches, "path_segs", 0),
            "stream_order": get_attr(subreaches, ["strm_order", "stream_order"], 0),
            "main_side": get_attr(subreaches, "main_side", 0),
            "end_reach": get_attr(subreaches, ["end_rch", "end_reach"], 0),
            "network": get_attr(subreaches, "network", 0),
            "add_flag": get_attr(subreaches, "add_flag", 0),
            "version": [self.version] * n,
        }

        reaches_df = pd.DataFrame(reaches_data)

        # Reorder columns to match canonical order (only columns present in df)
        cols = [c for c in REACHES_COLUMN_ORDER if c in reaches_df.columns]
        reaches_df = reaches_df[cols]

        # Insert main reaches using executemany for reliability
        placeholders = ", ".join(["?"] * len(cols))
        conn.executemany(
            f"""
            INSERT INTO reaches ({", ".join(cols)})
            VALUES ({placeholders})
        """,
            reaches_df.values.tolist(),
        )

        # Insert reach topology (rch_id_up and rch_id_down [4,N] arrays)
        self._insert_reach_topology(conn, subreaches)

        # Insert SWOT orbits if present
        if hasattr(subreaches, "orbits") and subreaches.orbits is not None:
            self._insert_reach_orbits(conn, subreaches)

        # Insert ice flags if present (daily [366,N] array)
        if hasattr(subreaches, "iceflag") and subreaches.iceflag is not None:
            if subreaches.iceflag.ndim == 2:
                self._insert_reach_ice_flags(conn, subreaches)

    def _insert_reach_topology(self, conn, subreaches) -> None:
        """Insert reach topology (upstream/downstream neighbors)."""
        import pandas as pd

        n = len(subreaches.id)
        topology_data = []

        # Process upstream neighbors
        if hasattr(subreaches, "rch_id_up") and subreaches.rch_id_up is not None:
            for i in range(n):
                for rank in range(4):
                    if rank < subreaches.rch_id_up.shape[0]:
                        neighbor_id = subreaches.rch_id_up[rank, i]
                        if neighbor_id != 0:
                            topology_data.append(
                                {
                                    "reach_id": subreaches.id[i],
                                    "region": self.region,
                                    "direction": "up",
                                    "neighbor_rank": rank,
                                    "neighbor_reach_id": neighbor_id,
                                }
                            )

        # Process downstream neighbors
        if hasattr(subreaches, "rch_id_down") and subreaches.rch_id_down is not None:
            for i in range(n):
                for rank in range(4):
                    if rank < subreaches.rch_id_down.shape[0]:
                        neighbor_id = subreaches.rch_id_down[rank, i]
                        if neighbor_id != 0:
                            topology_data.append(
                                {
                                    "reach_id": subreaches.id[i],
                                    "region": self.region,
                                    "direction": "down",
                                    "neighbor_rank": rank,
                                    "neighbor_reach_id": neighbor_id,
                                }
                            )

        if topology_data:
            topo_df = pd.DataFrame(topology_data)
            cols = [
                "reach_id",
                "region",
                "direction",
                "neighbor_rank",
                "neighbor_reach_id",
            ]
            topo_df = topo_df[cols]
            conn.executemany(
                """
                INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                VALUES (?, ?, ?, ?, ?)
            """,
                topo_df.values.tolist(),
            )

    def _insert_reach_orbits(self, conn, subreaches) -> None:
        """Insert SWOT orbit data."""
        import pandas as pd

        n = len(subreaches.id)
        orbits_data = []

        for i in range(n):
            for rank in range(min(75, subreaches.orbits.shape[0])):
                orbit_id = subreaches.orbits[rank, i]
                if orbit_id != 0:
                    orbits_data.append(
                        {
                            "reach_id": subreaches.id[i],
                            "region": self.region,
                            "orbit_rank": rank,
                            "orbit_id": orbit_id,
                        }
                    )

        if orbits_data:
            orbits_df = pd.DataFrame(orbits_data)
            cols = ["reach_id", "region", "orbit_rank", "orbit_id"]
            orbits_df = orbits_df[cols]
            conn.executemany(
                """
                INSERT INTO reach_swot_orbits (reach_id, region, orbit_rank, orbit_id)
                VALUES (?, ?, ?, ?)
            """,
                orbits_df.values.tolist(),
            )

    def _insert_reach_ice_flags(self, conn, subreaches) -> None:
        """Insert daily ice flag data."""
        import pandas as pd

        n = len(subreaches.id)
        ice_data = []

        for i in range(n):
            for day in range(min(366, subreaches.iceflag.shape[0])):
                flag = subreaches.iceflag[day, i]
                if flag != 0:  # Only store non-zero flags
                    ice_data.append(
                        {
                            "reach_id": subreaches.id[i],
                            "julian_day": day + 1,  # Convert 0-365 to 1-366
                            "iceflag": flag,
                        }
                    )

        if ice_data:
            ice_df = pd.DataFrame(ice_data)
            cols = ["reach_id", "julian_day", "iceflag"]
            ice_df = ice_df[cols]
            conn.executemany(
                """
                INSERT INTO reach_ice_flags (reach_id, julian_day, iceflag)
                VALUES (?, ?, ?)
            """,
                ice_df.values.tolist(),
            )

    def break_reaches(self, reach_id, break_cl_id, verbose=False) -> None:
        """
        Break and create new SWORD reaches at specified centerline locations.

        This method splits existing reaches at specified centerline points,
        creating new reach and node IDs for the resulting segments. It updates
        all related data including centerlines, nodes, reaches, and topology.

        Parameters
        ----------
        reach_id : numpy.ndarray or list
            Reach IDs of SWORD reaches to break.
        break_cl_id : numpy.ndarray or list
            Centerline IDs along the reach indicating where to break.
        verbose : bool, optional
            If True, print progress information. Default is False.

        Notes
        -----
        - New reach IDs are generated based on the basin and existing max reach number
        - New node IDs follow the SWORD ID convention
        - Topology (upstream/downstream neighbors) is automatically updated
        - Edit flag '6' is added to indicate broken reaches
        """
        import gc
        from geopy import distance

        def get_distances(lon, lat):
            """Calculate geodesic distances along coordinate arrays."""
            traces = len(lon) - 1
            distances = np.zeros(traces)
            for i in range(traces):
                start = (lat[i], lon[i])
                finish = (lat[i + 1], lon[i + 1])
                distances[i] = distance.geodesic(start, finish).m
            return np.append(0, distances)

        # Extract level6 basin info and node numbers from centerline node IDs
        cl_level6 = np.array([str(ind)[0:6] for ind in self.centerlines.node_id[0, :]])
        cl_node_num_int = np.array(
            [int(str(ind)[10:13]) for ind in self.centerlines.node_id[0, :]]
        )
        cl_rch_type = np.array([str(ind)[-1] for ind in self.centerlines.node_id[0, :]])

        # Format input
        reach = np.array(reach_id)
        break_id = np.array(break_cl_id)

        # Disable gc during bulk operations
        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()

        try:
            # Loop through unique reaches to break
            unq_rchs = np.unique(reach)
            for r in range(len(unq_rchs)):
                if verbose:
                    logger.debug(
                        f"Processing reach {r}: {unq_rchs[r]} ({r}/{len(unq_rchs) - 1})"
                    )

                # Find centerline points for this reach and sort by cl_id
                cl_r = np.where(self.centerlines.reach_id[0, :] == unq_rchs[r])[0]
                order_ids = np.argsort(self.centerlines.cl_id[cl_r])
                old_dist = self.reaches.dist_out[
                    np.where(self.reaches.id == unq_rchs[r])[0]
                ]
                old_len = self.reaches.len[np.where(self.reaches.id == unq_rchs[r])[0]]
                base_val = old_dist - old_len

                # Find break points
                breaks = break_id[np.where(reach == unq_rchs[r])[0]]
                break_pts = np.array(
                    [
                        np.where(self.centerlines.cl_id[cl_r[order_ids]] == b)[0][0]
                        for b in breaks
                    ]
                )

                # Build boundary array with start, break points, and end
                bounds = np.append(0, break_pts)
                bounds = np.append(bounds, len(cl_r))
                bounds = np.sort(bounds)
                bounds = np.unique(bounds)

                # Create temporary array marking new reach divisions at centerline scale
                new_divs = np.zeros(len(cl_r))
                count = 1
                for b in range(len(bounds) - 1):
                    update_nds = cl_r[order_ids[bounds[b] : bounds[b + 1]]]
                    nds = np.unique(self.centerlines.node_id[0, update_nds])
                    fill = np.where(
                        np.in1d(self.centerlines.node_id[0, cl_r[order_ids]], nds)
                    )[0]
                    if np.max(new_divs[fill]) == 0:
                        new_divs[fill] = count
                        count += 1
                    else:
                        z = np.where(new_divs[fill] == 0)[0]
                        new_divs[fill[z]] = count
                        count += 1

                # Process each new division
                unq_divs = np.unique(new_divs)
                if len(unq_divs) == 1:
                    continue

                for d in range(len(unq_divs)):
                    if d == 0:
                        # First division keeps original reach ID
                        update_ids = cl_r[order_ids[np.where(new_divs == unq_divs[d])]]
                        new_cl_rch_id = self.centerlines.reach_id[0, update_ids]
                        new_cl_node_ids = self.centerlines.node_id[0, update_ids]
                        new_rch_id = np.unique(
                            self.centerlines.reach_id[0, update_ids]
                        )[0]
                    else:
                        # Create new reach ID
                        update_ids = cl_r[order_ids[np.where(new_divs == unq_divs[d])]]
                        old_nodes = np.unique(self.centerlines.node_id[0, update_ids])
                        old_rch = np.unique(self.centerlines.reach_id[0, update_ids])[0]
                        l6_basin = np.where(
                            cl_level6 == np.unique(cl_level6[update_ids])
                        )[0]
                        cl_rch_num_int = np.array(
                            [
                                int(str(ind)[6:10])
                                for ind in self.centerlines.node_id[0, l6_basin]
                            ]
                        )
                        new_rch_num = np.max(cl_rch_num_int) + 1

                        # Format new reach ID with proper zero-padding
                        level6 = str(np.unique(cl_level6[update_ids])[0])
                        rch_type = str(np.unique(cl_rch_type[update_ids])[0])
                        new_rch_id = int(f"{level6}{new_rch_num:04d}{rch_type}")

                        new_cl_rch_id = np.repeat(new_rch_id, len(update_ids))

                        # Create new node IDs
                        new_cl_node_ids = np.zeros(len(update_ids), dtype=np.int64)
                        new_cl_node_nums = (
                            cl_node_num_int[update_ids]
                            - np.min(cl_node_num_int[update_ids])
                            + 1
                        )
                        for n in range(len(new_cl_node_nums)):
                            new_cl_node_ids[n] = int(
                                f"{str(new_rch_id)[:-1]}{new_cl_node_nums[n]:03d}{str(new_rch_id)[-1]}"
                            )

                    # Calculate new geometry for this division
                    x_coords = self.centerlines.x[update_ids]
                    y_coords = self.centerlines.y[update_ids]
                    diff = get_distances(x_coords, y_coords)
                    dist = np.cumsum(diff)

                    new_rch_len = np.max(dist)
                    new_rch_x = np.median(self.centerlines.x[update_ids])
                    new_rch_y = np.median(self.centerlines.y[update_ids])
                    new_rch_x_max = np.max(self.centerlines.x[update_ids])
                    new_rch_x_min = np.min(self.centerlines.x[update_ids])
                    new_rch_y_max = np.max(self.centerlines.y[update_ids])
                    new_rch_y_min = np.min(self.centerlines.y[update_ids])

                    # Calculate node-level attributes
                    unq_nodes = np.unique(new_cl_node_ids)
                    new_node_len = np.zeros(len(unq_nodes))
                    new_node_x = np.zeros(len(unq_nodes))
                    new_node_y = np.zeros(len(unq_nodes))
                    new_node_id = np.zeros(len(unq_nodes), dtype=np.int64)
                    new_node_cl_ids = np.zeros((2, len(unq_nodes)), dtype=np.int64)

                    for n2 in range(len(unq_nodes)):
                        pts = np.where(new_cl_node_ids == unq_nodes[n2])[0]
                        new_node_x[n2] = np.median(self.centerlines.x[update_ids[pts]])
                        new_node_y[n2] = np.median(self.centerlines.y[update_ids[pts]])
                        new_node_len[n2] = (
                            max(np.cumsum(diff[pts])) if len(pts) > 1 else 30
                        )
                        new_node_id[n2] = unq_nodes[n2]
                        new_node_cl_ids[0, n2] = np.min(
                            self.centerlines.cl_id[update_ids[pts]]
                        )
                        new_node_cl_ids[1, n2] = np.max(
                            self.centerlines.cl_id[update_ids[pts]]
                        )

                    # Determine edit flag
                    rch_idx = np.where(
                        self.reaches.id == (new_rch_id if d == 0 else old_rch)
                    )[0]
                    current_edit = (
                        self.reaches.edit_flag[rch_idx][0] if len(rch_idx) > 0 else ""
                    )
                    if current_edit == "NaN" or current_edit == "":
                        edit_val = "6"
                    elif "6" not in current_edit.split(","):
                        edit_val = f"{current_edit},6"
                    else:
                        edit_val = current_edit

                    if new_rch_id in self.reaches.id:
                        # Update existing reach in database
                        self._update_existing_reach_break(
                            conn,
                            new_rch_id,
                            new_node_id,
                            new_node_len,
                            new_node_cl_ids,
                            new_node_x,
                            new_node_y,
                            update_ids,
                            new_rch_x,
                            new_rch_y,
                            new_rch_x_min,
                            new_rch_x_max,
                            new_rch_y_min,
                            new_rch_y_max,
                            new_rch_len,
                            edit_val,
                        )
                    else:
                        # Insert new reach
                        self._insert_new_reach_break(
                            conn,
                            new_rch_id,
                            old_rch,
                            new_cl_rch_id,
                            new_cl_node_ids,
                            update_ids,
                            old_nodes,
                            new_node_id,
                            new_node_len,
                            new_node_cl_ids,
                            new_node_x,
                            new_node_y,
                            new_rch_x,
                            new_rch_y,
                            new_rch_x_min,
                            new_rch_x_max,
                            new_rch_y_min,
                            new_rch_y_max,
                            new_rch_len,
                            edit_val,
                        )

                # Update topology for all new reaches
                self._update_break_topology(conn, cl_r, order_ids, unq_rchs[r])

                # Update distance from outlet
                nrchs = np.unique(self.centerlines.reach_id[0, cl_r[order_ids]])
                self._update_dist_out(conn, nrchs, cl_r, order_ids, base_val)

            # Reload data to refresh views
            self._load_data()

            if verbose:
                logger.info(
                    f"Break reaches complete. Processed {len(unq_rchs)} reaches."
                )

        except Exception as e:
            raise RuntimeError(f"Failed to break reaches: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def _update_existing_reach_break(
        self,
        conn,
        reach_id,
        node_ids,
        node_lens,
        node_cl_ids,
        node_x,
        node_y,
        cl_update_ids,
        rch_x,
        rch_y,
        rch_x_min,
        rch_x_max,
        rch_y_min,
        rch_y_max,
        rch_len,
        edit_val,
    ) -> None:
        """Update an existing reach after break operation."""
        # Update nodes
        for i, nid in enumerate(node_ids):
            conn.execute(
                """
                UPDATE nodes SET
                    node_length = ?,
                    cl_id_min = ?,
                    cl_id_max = ?,
                    x = ?,
                    y = ?,
                    edit_flag = ?
                WHERE node_id = ? AND region = ?
            """,
                [
                    float(node_lens[i]),
                    int(node_cl_ids[0, i]),
                    int(node_cl_ids[1, i]),
                    float(node_x[i]),
                    float(node_y[i]),
                    edit_val,
                    int(nid),
                    self.region,
                ],
            )

        # Update reach
        conn.execute(
            """
            UPDATE reaches SET
                cl_id_min = ?,
                cl_id_max = ?,
                x = ?,
                y = ?,
                x_min = ?,
                x_max = ?,
                y_min = ?,
                y_max = ?,
                reach_length = ?,
                n_nodes = ?,
                edit_flag = ?
            WHERE reach_id = ? AND region = ?
        """,
            [
                int(np.min(self.centerlines.cl_id[cl_update_ids])),
                int(np.max(self.centerlines.cl_id[cl_update_ids])),
                float(rch_x),
                float(rch_y),
                float(rch_x_min),
                float(rch_x_max),
                float(rch_y_min),
                float(rch_y_max),
                float(rch_len),
                len(node_ids),
                edit_val,
                int(reach_id),
                self.region,
            ],
        )

    def _insert_new_reach_break(
        self,
        conn,
        new_rch_id,
        old_rch,
        new_cl_rch_id,
        new_cl_node_ids,
        update_ids,
        old_nodes,
        new_node_id,
        new_node_len,
        new_node_cl_ids,
        new_node_x,
        new_node_y,
        rch_x,
        rch_y,
        rch_x_min,
        rch_x_max,
        rch_y_min,
        rch_y_max,
        rch_len,
        edit_val,
    ) -> None:
        """Insert a new reach created from break operation."""
        # Update centerlines with new reach/node IDs
        for i, cl_idx in enumerate(update_ids):
            conn.execute(
                """
                UPDATE centerlines SET
                    reach_id = ?,
                    node_id = ?
                WHERE cl_id = ? AND region = ?
            """,
                [
                    int(new_cl_rch_id[i])
                    if hasattr(new_cl_rch_id, "__len__")
                    else int(new_cl_rch_id),
                    int(new_cl_node_ids[i]),
                    int(self.centerlines.cl_id[cl_idx]),
                    self.region,
                ],
            )

        # Update nodes with new IDs and attributes
        old_ind = np.where(np.in1d(self.nodes.id, old_nodes))[0]
        for i, old_idx in enumerate(old_ind):
            if i < len(new_node_id):
                conn.execute(
                    """
                    UPDATE nodes SET
                        node_id = ?,
                        node_length = ?,
                        cl_id_min = ?,
                        cl_id_max = ?,
                        x = ?,
                        y = ?,
                        reach_id = ?,
                        edit_flag = ?
                    WHERE node_id = ? AND region = ?
                """,
                    [
                        int(new_node_id[i]),
                        float(new_node_len[i]),
                        int(new_node_cl_ids[0, i]),
                        int(new_node_cl_ids[1, i]),
                        float(new_node_x[i]),
                        float(new_node_y[i]),
                        int(new_rch_id),
                        edit_val,
                        int(self.nodes.id[old_idx]),
                        self.region,
                    ],
                )

        # Get attributes from old reach to copy to new reach
        old_rch_idx = np.where(self.reaches.id == old_rch)[0]
        if len(old_rch_idx) == 0:
            return

        old_rch_idx = old_rch_idx[0]

        # Insert new reach with copied attributes
        conn.execute(
            """
            INSERT INTO reaches (
                reach_id, region, x, y, x_min, x_max, y_min, y_max,
                cl_id_min, cl_id_max, reach_length, n_nodes, wse, wse_var,
                width, width_var, slope, max_width, facc, dist_out, lakeflag,
                obstr_type, grod_id, hfalls_id, n_chan_max, n_chan_mod,
                swot_obs, low_slope_flag, river_name, edit_flag,
                trib_flag, path_freq, path_order, path_segs, stream_order,
                main_side, end_reach, network, add_flag, version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                int(new_rch_id),
                self.region,
                float(rch_x),
                float(rch_y),
                float(rch_x_min),
                float(rch_x_max),
                float(rch_y_min),
                float(rch_y_max),
                int(np.min(self.centerlines.cl_id[update_ids])),
                int(np.max(self.centerlines.cl_id[update_ids])),
                float(rch_len),
                len(new_node_id),
                float(self.reaches.wse[old_rch_idx]),
                float(self.reaches.wse_var[old_rch_idx]),
                float(self.reaches.wth[old_rch_idx]),
                float(self.reaches.wth_var[old_rch_idx]),
                float(self.reaches.slope[old_rch_idx]),
                float(self.reaches.max_wth[old_rch_idx]),
                float(self.reaches.facc[old_rch_idx]),
                float(self.reaches.dist_out[old_rch_idx]),
                int(self.reaches.lakeflag[old_rch_idx]),
                int(self.reaches.grod[old_rch_idx]),
                int(self.reaches.grod_fid[old_rch_idx]),
                int(self.reaches.hfalls_fid[old_rch_idx]),
                int(self.reaches.nchan_max[old_rch_idx]),
                int(self.reaches.nchan_mod[old_rch_idx]),
                int(self.reaches.max_obs[old_rch_idx]),
                int(self.reaches.low_slope[old_rch_idx]),
                str(self.reaches.river_name[old_rch_idx]),
                edit_val,
                int(self.reaches.trib_flag[old_rch_idx]),
                int(self.reaches.path_freq[old_rch_idx]),
                int(self.reaches.path_order[old_rch_idx]),
                int(self.reaches.path_segs[old_rch_idx]),
                int(self.reaches.strm_order[old_rch_idx]),
                int(self.reaches.main_side[old_rch_idx]),
                int(self.reaches.end_rch[old_rch_idx]),
                int(self.reaches.network[old_rch_idx]),
                int(self.reaches.add_flag[old_rch_idx])
                if hasattr(self.reaches, "add_flag")
                and not (
                    self.reaches.add_flag[old_rch_idx] is None
                    or str(self.reaches.add_flag[old_rch_idx]) == "<NA>"
                )
                else 0,
                self.version,
            ],
        )

        # Copy topology from old reach
        self._copy_reach_topology(conn, old_rch, new_rch_id)

        # Copy orbits from old reach
        self._copy_reach_orbits(conn, old_rch, new_rch_id)

        # Copy ice flags from old reach
        self._copy_reach_ice_flags(conn, old_rch, new_rch_id)

    def _copy_reach_topology(self, conn, old_rch_id, new_rch_id) -> None:
        """Copy topology entries from old reach to new reach."""
        # Get existing topology
        result = conn.execute(
            """
            SELECT direction, neighbor_rank, neighbor_reach_id
            FROM reach_topology
            WHERE reach_id = ? AND region = ?
        """,
            [int(old_rch_id), self.region],
        ).fetchall()

        for row in result:
            conn.execute(
                """
                INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                VALUES (?, ?, ?, ?, ?)
            """,
                [int(new_rch_id), self.region, row[0], row[1], row[2]],
            )

    def _copy_reach_orbits(self, conn, old_rch_id, new_rch_id) -> None:
        """Copy orbit entries from old reach to new reach."""
        result = conn.execute(
            """
            SELECT orbit_rank, orbit_id
            FROM reach_swot_orbits
            WHERE reach_id = ? AND region = ?
        """,
            [int(old_rch_id), self.region],
        ).fetchall()

        for row in result:
            conn.execute(
                """
                INSERT INTO reach_swot_orbits (reach_id, region, orbit_rank, orbit_id)
                VALUES (?, ?, ?, ?)
            """,
                [int(new_rch_id), self.region, row[0], row[1]],
            )

    def _copy_reach_ice_flags(self, conn, old_rch_id, new_rch_id) -> None:
        """Copy ice flag entries from old reach to new reach."""
        result = conn.execute(
            """
            SELECT julian_day, iceflag
            FROM reach_ice_flags
            WHERE reach_id = ?
        """,
            [int(old_rch_id)],
        ).fetchall()

        for row in result:
            conn.execute(
                """
                INSERT INTO reach_ice_flags (reach_id, julian_day, iceflag)
                VALUES (?, ?, ?)
            """,
                [int(new_rch_id), row[0], row[1]],
            )

    def _update_break_topology(self, conn, cl_r, order_ids, original_rch) -> None:
        """Update topology after breaking reaches."""
        nrchs = np.unique(self.centerlines.reach_id[0, cl_r[order_ids]])
        max_id = [
            max(
                self.centerlines.cl_id[
                    cl_r[
                        order_ids[
                            np.where(
                                self.centerlines.reach_id[0, cl_r[order_ids]] == n
                            )[0]
                        ]
                    ]
                ]
            )
            for n in nrchs
        ]
        id_sort = np.argsort(max_id)
        nrchs = nrchs[id_sort]

        for idx in range(len(nrchs)):
            pts = np.where(self.centerlines.reach_id[0, cl_r[order_ids]] == nrchs[idx])[
                0
            ]
            binary = np.copy(self.centerlines.reach_id[1:, cl_r[order_ids[pts]]])
            binary[np.where(binary > 0)] = 1
            binary_sum = np.sum(binary, axis=0)
            existing_nghs = np.where(binary_sum > 0)[0]

            if len(existing_nghs) > 0:
                mn = np.where(
                    self.centerlines.cl_id[cl_r[order_ids[pts]]]
                    == min(self.centerlines.cl_id[cl_r[order_ids[pts]]])
                )[0]
                mx = np.where(
                    self.centerlines.cl_id[cl_r[order_ids[pts]]]
                    == max(self.centerlines.cl_id[cl_r[order_ids[pts]]])
                )[0]

                current_rch_id = int(
                    self.centerlines.reach_id[0, cl_r[order_ids[pts[0]]]]
                )

                if mn[0] in existing_nghs and mx[0] not in existing_nghs:
                    # Update upstream neighbor relationship
                    if pts[mx[0]] + 1 < len(order_ids):
                        neighbor_rch_id = int(
                            self.centerlines.reach_id[
                                0, cl_r[order_ids[pts[mx[0]] + 1]]
                            ]
                        )

                        # Update centerline neighbors
                        self._update_centerline_neighbor(
                            conn, cl_r[order_ids[pts[mx[0]]]], neighbor_rch_id
                        )

                        # Update reach topology
                        self._update_reach_topology_entry(
                            conn, current_rch_id, "up", neighbor_rch_id
                        )

                        if idx > 0:
                            self._update_reach_topology_entry(
                                conn, neighbor_rch_id, "down", current_rch_id
                            )

                elif mx[0] in existing_nghs and mn[0] not in existing_nghs:
                    # Update downstream neighbor relationship
                    if pts[mn[0]] > 0:
                        neighbor_rch_id = int(
                            self.centerlines.reach_id[
                                0, cl_r[order_ids[pts[mn[0]] - 1]]
                            ]
                        )

                        self._update_centerline_neighbor(
                            conn, cl_r[order_ids[pts[mn[0]]]], neighbor_rch_id
                        )

                        self._update_reach_topology_entry(
                            conn, current_rch_id, "down", neighbor_rch_id
                        )

                        if idx > 0:
                            self._update_reach_topology_entry(
                                conn, neighbor_rch_id, "up", current_rch_id
                            )

    def _update_centerline_neighbor(self, conn, cl_idx, neighbor_rch_id) -> None:
        """Update centerline neighbor reach ID."""
        cl_id = int(self.centerlines.cl_id[cl_idx])
        # Clear existing neighbors and set new one
        conn.execute(
            """
            DELETE FROM centerline_neighbors
            WHERE cl_id = ? AND region = ?
        """,
            [cl_id, self.region],
        )

        conn.execute(
            """
            INSERT INTO centerline_neighbors (cl_id, region, neighbor_rank, reach_id, node_id)
            VALUES (?, ?, 1, ?, 0)
        """,
            [cl_id, self.region, neighbor_rch_id],
        )

    def _update_reach_topology_entry(
        self, conn, reach_id, direction, neighbor_id
    ) -> None:
        """Update or insert a reach topology entry."""
        # Clear existing entries for this direction
        conn.execute(
            """
            DELETE FROM reach_topology
            WHERE reach_id = ? AND region = ? AND direction = ?
        """,
            [reach_id, self.region, direction],
        )

        # Insert new entry
        conn.execute(
            """
            INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
            VALUES (?, ?, ?, 0, ?)
        """,
            [reach_id, self.region, direction, neighbor_id],
        )

    def _update_dist_out(self, conn, nrchs, cl_r, order_ids, base_val) -> None:
        """Update distance from outlet for broken reaches."""
        # Get reach lengths and calculate cumulative distance
        rch_lens = []
        for rch in nrchs:
            idx = np.where(self.reaches.id == rch)[0]
            if len(idx) > 0:
                rch_lens.append(self.reaches.len[idx[0]])
            else:
                # Calculate from centerlines
                cl_pts = np.where(self.centerlines.reach_id[0, cl_r[order_ids]] == rch)[
                    0
                ]
                if len(cl_pts) > 0:
                    from geopy import distance

                    x = self.centerlines.x[cl_r[order_ids[cl_pts]]]
                    y = self.centerlines.y[cl_r[order_ids[cl_pts]]]
                    dists = []
                    for i in range(len(x) - 1):
                        d = distance.geodesic((y[i], x[i]), (y[i + 1], x[i + 1])).m
                        dists.append(d)
                    rch_lens.append(sum(dists))
                else:
                    rch_lens.append(0)

        rch_cs = np.cumsum(rch_lens) + base_val

        for i, rch in enumerate(nrchs):
            conn.execute(
                """
                UPDATE reaches SET dist_out = ?
                WHERE reach_id = ? AND region = ?
            """,
                [float(rch_cs[i]), int(rch), self.region],
            )

    def merge_reaches(
        self, source_reach_id: int, target_reach_id: int, verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Merge a source reach into a target reach.

        This method combines two adjacent reaches by moving all centerlines and
        nodes from the source reach into the target reach, recalculating attributes,
        updating topology, and deleting the source reach.

        Parameters
        ----------
        source_reach_id : int
            The reach ID to merge (will be deleted after merge)
        target_reach_id : int
            The reach ID to merge into (will be preserved and expanded)
        verbose : bool, optional
            If True, print progress information. Default is False.

        Returns
        -------
        dict
            Results including:
            - 'source_reach': int - The merged (deleted) reach ID
            - 'target_reach': int - The expanded reach ID
            - 'merged_nodes': int - Number of nodes merged
            - 'merged_centerlines': int - Number of centerlines merged
            - 'success': bool - Whether operation succeeded

        Raises
        ------
        ValueError
            If reaches are not adjacent or cannot be merged
        RuntimeError
            If database operation fails

        Notes
        -----
        Algorithm based on legacy aggregate_1node_rchs.py:
        1. Validate source and target are topologically adjacent
        2. Reassign centerlines from source to target
        3. Update node IDs and reach assignments
        4. Recalculate reach attributes using aggregation methods:
           - wse, wth: median
           - wse_var, wth_var: max
           - nchan_max, grod: max
           - nchan_mod, lakeflag: mode
           - slope: linear regression of wse vs dist_out
        5. Update topology (target inherits source's neighbors)
        6. Delete source reach
        """
        import gc
        from statistics import mode as stat_mode

        # Validate inputs
        source_idx = np.where(self.reaches.id == source_reach_id)[0]
        target_idx = np.where(self.reaches.id == target_reach_id)[0]

        if len(source_idx) == 0:
            raise ValueError(f"Source reach {source_reach_id} not found")
        if len(target_idx) == 0:
            raise ValueError(f"Target reach {target_reach_id} not found")

        source_idx = source_idx[0]
        target_idx = target_idx[0]

        # Check if reaches are adjacent
        is_adjacent = self._check_reaches_adjacent(source_reach_id, target_reach_id)
        if not is_adjacent:
            raise ValueError(
                f"Reaches {source_reach_id} and {target_reach_id} are not adjacent"
            )

        if verbose:
            logger.info(f"Merging reach {source_reach_id} into {target_reach_id}")

        # Disable GC during bulk operations
        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()
        conn.execute("BEGIN TRANSACTION")

        try:
            # Get source centerlines and nodes
            source_cl_idx = np.where(
                self.centerlines.reach_id[0, :] == source_reach_id
            )[0]
            source_node_idx = np.where(self.nodes.reach_id == source_reach_id)[0]

            if verbose:
                logger.debug(
                    f"Moving {len(source_cl_idx)} centerlines, {len(source_node_idx)} nodes"
                )

            # Determine merge direction (upstream or downstream)
            merge_direction = self._get_merge_direction(
                source_reach_id, target_reach_id
            )

            # Update centerlines: reassign to target reach
            self._reassign_centerlines_for_merge(
                conn, source_reach_id, target_reach_id, merge_direction
            )

            # Update nodes: reassign to target reach with new node IDs
            self._reassign_nodes_for_merge(
                conn, source_reach_id, target_reach_id, merge_direction
            )

            # Recalculate target reach attributes
            self._recalculate_merged_reach_attributes(conn, target_reach_id, stat_mode)

            # Update topology: target inherits source's neighbors
            self._update_topology_for_merge(
                conn, source_reach_id, target_reach_id, merge_direction
            )

            # Delete source reach and its topology entries
            self._delete_merged_source_reach(conn, source_reach_id)

            conn.execute("COMMIT")

            # Reload data
            self._load_data()

            if verbose:
                logger.info(
                    f"Merge complete. Target reach {target_reach_id} now has "
                    f"{self.reaches.rch_n_nodes[np.where(self.reaches.id == target_reach_id)[0][0]]} nodes"
                )

            return {
                "source_reach": source_reach_id,
                "target_reach": target_reach_id,
                "merged_nodes": len(source_node_idx),
                "merged_centerlines": len(source_cl_idx),
                "success": True,
            }

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to merge reaches: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def _check_reaches_adjacent(self, reach_id_1: int, reach_id_2: int) -> bool:
        """Check if two reaches are topologically adjacent."""
        conn = self._db.connect()

        # Check if reach_id_2 is upstream or downstream of reach_id_1
        result = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ?
        """,
            [int(reach_id_1), int(reach_id_2), self.region],
        ).fetchone()

        if result[0] > 0:
            return True

        # Check the reverse
        result = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ?
        """,
            [int(reach_id_2), int(reach_id_1), self.region],
        ).fetchone()

        return result[0] > 0

    def _get_merge_direction(self, source_reach_id: int, target_reach_id: int) -> str:
        """
        Determine the direction of merge.

        Returns 'downstream' if source is upstream of target,
        'upstream' if source is downstream of target.
        """
        conn = self._db.connect()

        # Check if target is downstream of source
        result = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND neighbor_reach_id = ?
            AND direction = 'down' AND region = ?
        """,
            [int(source_reach_id), int(target_reach_id), self.region],
        ).fetchone()

        if result[0] > 0:
            return "downstream"  # source flows into target

        return "upstream"  # target flows into source

    def _reassign_centerlines_for_merge(
        self, conn, source_reach_id: int, target_reach_id: int, merge_direction: str
    ) -> None:
        """Reassign centerlines from source reach to target reach."""
        # Update reach_id for all centerlines of source reach
        conn.execute(
            """
            UPDATE centerlines
            SET reach_id = ?
            WHERE reach_id = ? AND region = ?
        """,
            [int(target_reach_id), int(source_reach_id), self.region],
        )

        # Update centerline_neighbors: replace source reach references with target
        conn.execute(
            """
            UPDATE centerline_neighbors
            SET reach_id = ?
            WHERE reach_id = ? AND region = ?
        """,
            [int(target_reach_id), int(source_reach_id), self.region],
        )

    def _reassign_nodes_for_merge(
        self, conn, source_reach_id: int, target_reach_id: int, merge_direction: str
    ) -> int:
        """
        Reassign nodes from source reach to target reach.

        Updates node IDs to follow target reach's ID pattern.
        Returns the number of nodes reassigned.
        """
        # Get source nodes
        source_node_idx = np.where(self.nodes.reach_id == source_reach_id)[0]
        if len(source_node_idx) == 0:
            return 0

        # Get target reach's existing max node number
        target_node_idx = np.where(self.nodes.reach_id == target_reach_id)[0]
        if len(target_node_idx) > 0:
            target_node_nums = np.array(
                [int(str(nid)[10:13]) for nid in self.nodes.id[target_node_idx]]
            )
            max_node_num = np.max(target_node_nums)
        else:
            max_node_num = 0

        # Get reach type digit from target reach
        reach_type = str(target_reach_id)[-1]

        # Update each source node
        for i, src_idx in enumerate(source_node_idx):
            old_node_id = int(self.nodes.id[src_idx])

            if merge_direction == "downstream":
                # Source is upstream, so its nodes get higher numbers (added at end)
                new_node_num = max_node_num + i + 1
            else:
                # Source is downstream, need to renumber all nodes
                # For simplicity, just append to end with new numbers
                new_node_num = max_node_num + i + 1

            # Format new node ID: reach_id[:-1] + node_num (3 digits) + type
            new_node_id = int(
                f"{str(target_reach_id)[:-1]}{new_node_num:03d}{reach_type}"
            )

            # Update node record
            conn.execute(
                """
                UPDATE nodes
                SET node_id = ?, reach_id = ?, edit_flag = CASE
                    WHEN edit_flag IS NULL OR edit_flag = 'NaN' OR edit_flag = '' THEN '6'
                    WHEN edit_flag NOT LIKE '%6%' THEN edit_flag || ',6'
                    ELSE edit_flag
                END
                WHERE node_id = ? AND region = ?
            """,
                [new_node_id, int(target_reach_id), old_node_id, self.region],
            )

            # Update centerlines that reference this node
            conn.execute(
                """
                UPDATE centerlines
                SET node_id = ?
                WHERE node_id = ? AND region = ?
            """,
                [new_node_id, old_node_id, self.region],
            )

        return len(source_node_idx)

    def _recalculate_merged_reach_attributes(
        self, conn, target_reach_id: int, stat_mode
    ) -> None:
        """
        Recalculate reach attributes after merge using aggregation methods.

        Aggregation methods (from legacy aggregate_1node_rchs.py):
        - x, y: median of centerlines
        - x_min, x_max, y_min, y_max: min/max of centerlines
        - wse, wth: median of nodes
        - wse_var, wth_var: max of nodes
        - nchan_max, grod, hfalls_id, max_width: max of nodes
        - nchan_mod, lakeflag: mode of nodes
        - slope: linear regression of wse vs dist_out
        - reach_length: sum of node lengths
        - n_nodes: count of nodes
        """
        # Get updated centerlines for target reach
        target_cl_idx = np.where(self.centerlines.reach_id[0, :] == target_reach_id)[0]

        # Recalculate geometry from centerlines
        if len(target_cl_idx) > 0:
            x_coords = self.centerlines.x[target_cl_idx]
            y_coords = self.centerlines.y[target_cl_idx]
            cl_ids = self.centerlines.cl_id[target_cl_idx]

            new_x = float(np.median(x_coords))
            new_y = float(np.median(y_coords))
            new_x_min = float(np.min(x_coords))
            new_x_max = float(np.max(x_coords))
            new_y_min = float(np.min(y_coords))
            new_y_max = float(np.max(y_coords))
            new_cl_id_min = int(np.min(cl_ids))
            new_cl_id_max = int(np.max(cl_ids))

        # Get node-level data from database (after reassignment)
        node_data = conn.execute(
            """
            SELECT node_id, wse, wse_var, width, width_var, n_chan_max, n_chan_mod,
                   obstr_type, grod_id, hfalls_id, lakeflag, max_width, node_length,
                   dist_out
            FROM nodes
            WHERE reach_id = ? AND region = ?
            ORDER BY node_id
        """,
            [int(target_reach_id), self.region],
        ).fetchall()

        if len(node_data) == 0:
            return

        # Extract arrays
        node_ids = np.array([r[0] for r in node_data])
        wse_vals = np.array([r[1] for r in node_data])
        wse_var_vals = np.array([r[2] for r in node_data])
        wth_vals = np.array([r[3] for r in node_data])
        wth_var_vals = np.array([r[4] for r in node_data])
        nchan_max_vals = np.array([r[5] for r in node_data])
        nchan_mod_vals = np.array([r[6] for r in node_data])
        grod_vals = np.array([r[7] for r in node_data])
        grod_fid_vals = np.array([r[8] for r in node_data])
        hfalls_fid_vals = np.array([r[9] for r in node_data])
        lakeflag_vals = np.array([r[10] for r in node_data])
        max_wth_vals = np.array([r[11] for r in node_data])
        node_len_vals = np.array([r[12] for r in node_data])
        dist_out_vals = np.array([r[13] for r in node_data])

        # Calculate aggregated values
        new_wse = float(np.median(wse_vals))
        new_wse_var = float(np.max(wse_var_vals))
        new_wth = float(np.median(wth_vals))
        new_wth_var = float(np.max(wth_var_vals))
        new_nchan_max = int(np.max(nchan_max_vals))
        new_grod = int(np.max(grod_vals))
        new_grod_fid = int(np.max(grod_fid_vals))
        new_hfalls_fid = int(np.max(hfalls_fid_vals))
        new_max_wth = float(np.max(max_wth_vals))
        new_n_nodes = len(node_data)
        new_reach_length = float(np.sum(node_len_vals))
        new_dist_out = float(np.max(dist_out_vals))

        # Mode calculations (with fallback)
        try:
            new_nchan_mod = int(stat_mode(nchan_mod_vals.tolist()))
        except Exception:
            new_nchan_mod = int(nchan_mod_vals[0]) if len(nchan_mod_vals) > 0 else 1

        try:
            new_lakeflag = int(stat_mode(lakeflag_vals.tolist()))
        except Exception:
            new_lakeflag = int(lakeflag_vals[0]) if len(lakeflag_vals) > 0 else 0

        # Slope calculation: linear regression of wse vs dist_out/1000
        if len(node_data) >= 2:
            order_ids = np.argsort(node_ids)
            slope_pts = np.vstack(
                [dist_out_vals[order_ids] / 1000, np.ones(len(order_ids))]
            ).T
            try:
                slope, _ = np.linalg.lstsq(slope_pts, wse_vals[order_ids], rcond=None)[
                    0
                ]
                new_slope = abs(float(slope))
            except Exception:
                new_slope = 0.0
        else:
            new_slope = 0.0

        # Update reach record
        conn.execute(
            """
            UPDATE reaches SET
                x = ?, y = ?, x_min = ?, x_max = ?, y_min = ?, y_max = ?,
                cl_id_min = ?, cl_id_max = ?,
                wse = ?, wse_var = ?, width = ?, width_var = ?,
                slope = ?, max_width = ?, dist_out = ?,
                n_chan_max = ?, n_chan_mod = ?,
                obstr_type = ?, grod_id = ?, hfalls_id = ?,
                lakeflag = ?,
                reach_length = ?, n_nodes = ?,
                edit_flag = CASE
                    WHEN edit_flag IS NULL OR edit_flag = 'NaN' OR edit_flag = '' THEN '6'
                    WHEN edit_flag NOT LIKE '%6%' THEN edit_flag || ',6'
                    ELSE edit_flag
                END
            WHERE reach_id = ? AND region = ?
        """,
            [
                new_x,
                new_y,
                new_x_min,
                new_x_max,
                new_y_min,
                new_y_max,
                new_cl_id_min,
                new_cl_id_max,
                new_wse,
                new_wse_var,
                new_wth,
                new_wth_var,
                new_slope,
                new_max_wth,
                new_dist_out,
                new_nchan_max,
                new_nchan_mod,
                new_grod,
                new_grod_fid,
                new_hfalls_fid,
                new_lakeflag,
                new_reach_length,
                new_n_nodes,
                int(target_reach_id),
                self.region,
            ],
        )

    def _update_topology_for_merge(
        self, conn, source_reach_id: int, target_reach_id: int, merge_direction: str
    ) -> None:
        """
        Update topology after merge.

        Target reach inherits source's neighbors (excluding each other).
        Neighbors that pointed to source now point to target.
        """
        # Get source's neighbors
        source_neighbors = conn.execute(
            """
            SELECT direction, neighbor_rank, neighbor_reach_id
            FROM reach_topology
            WHERE reach_id = ? AND region = ?
        """,
            [int(source_reach_id), self.region],
        ).fetchall()

        for direction, rank, neighbor_id in source_neighbors:
            # Skip the target reach (they were connected to each other)
            if neighbor_id == target_reach_id:
                continue

            # Check if this neighbor relationship already exists for target
            existing = conn.execute(
                """
                SELECT COUNT(*) FROM reach_topology
                WHERE reach_id = ? AND neighbor_reach_id = ?
                AND direction = ? AND region = ?
            """,
                [int(target_reach_id), int(neighbor_id), direction, self.region],
            ).fetchone()

            if existing[0] == 0:
                # Get next available rank for this direction
                max_rank = conn.execute(
                    """
                    SELECT COALESCE(MAX(neighbor_rank), -1) FROM reach_topology
                    WHERE reach_id = ? AND direction = ? AND region = ?
                """,
                    [int(target_reach_id), direction, self.region],
                ).fetchone()[0]
                new_rank = max_rank + 1

                # Add this neighbor to target with new rank
                conn.execute(
                    """
                    INSERT INTO reach_topology
                    (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    [
                        int(target_reach_id),
                        self.region,
                        direction,
                        new_rank,
                        int(neighbor_id),
                    ],
                )

            # Update the neighbor to point to target instead of source
            conn.execute(
                """
                UPDATE reach_topology
                SET neighbor_reach_id = ?
                WHERE neighbor_reach_id = ? AND region = ?
            """,
                [int(target_reach_id), int(source_reach_id), self.region],
            )

        # Remove source's entry pointing to target (and vice versa) from topology
        conn.execute(
            """
            DELETE FROM reach_topology
            WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ?
        """,
            [int(target_reach_id), int(source_reach_id), self.region],
        )

        # Update n_rch_up and n_rch_down for target
        up_count = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND direction = 'up' AND region = ?
        """,
            [int(target_reach_id), self.region],
        ).fetchone()[0]

        down_count = conn.execute(
            """
            SELECT COUNT(*) FROM reach_topology
            WHERE reach_id = ? AND direction = 'down' AND region = ?
        """,
            [int(target_reach_id), self.region],
        ).fetchone()[0]

        conn.execute(
            """
            UPDATE reaches SET n_rch_up = ?, n_rch_down = ?
            WHERE reach_id = ? AND region = ?
        """,
            [up_count, down_count, int(target_reach_id), self.region],
        )

    def _delete_merged_source_reach(self, conn, source_reach_id: int) -> None:
        """Delete the source reach after merge (without cascade since data was moved)."""
        # Delete topology entries for source
        conn.execute(
            """
            DELETE FROM reach_topology
            WHERE reach_id = ? AND region = ?
        """,
            [int(source_reach_id), self.region],
        )

        # Delete orbit data
        conn.execute(
            """
            DELETE FROM reach_swot_orbits
            WHERE reach_id = ? AND region = ?
        """,
            [int(source_reach_id), self.region],
        )

        # Delete ice flag data
        conn.execute(
            """
            DELETE FROM reach_ice_flags
            WHERE reach_id = ?
        """,
            [int(source_reach_id)],
        )

        # Delete the reach record
        conn.execute(
            """
            DELETE FROM reaches
            WHERE reach_id = ? AND region = ?
        """,
            [int(source_reach_id), self.region],
        )

    def check_topo_consistency(
        self, verbose: int = 1, return_details: bool = False
    ) -> dict:
        """
        Check the topological consistency of SWORD data.

        This method validates the integrity of reach topology by running
        several consistency checks based on the legacy check_topo_consistency.py
        script.

        Parameters
        ----------
        verbose : int, optional
            Output verbosity level:
            - 0: Silent (only return results)
            - 1: Show errors only (default)
            - 2: Show errors and warnings
        return_details : bool, optional
            If True, include detailed error information in results.
            Default is False.

        Returns
        -------
        dict
            Dictionary containing:
            - 'passed': bool - True if no errors found
            - 'total_reaches': int - Number of reaches checked
            - 'error_counts': dict - Count of each error type
            - 'warning_counts': dict - Count of each warning type
            - 'reaches_with_issues': list - Reach IDs with problems
            - 'details': list (if return_details=True) - Detailed error messages

        Notes
        -----
        Check types:
        1. Count mismatch: n_rch_up/n_rch_down doesn't match actual neighbor count
        2. Unrequited neighbors: A->B connection not reciprocated by B->A
        3. Ghost reach warning: Ghost reach (type 6) has both up and downstream
        4. Orphan reach warning: Non-ghost reach has no upstream neighbors
        5. Self-reference: Reach references itself as neighbor

        Example
        -------
        >>> results = sword.check_topo_consistency(verbose=1)
        >>> if not results['passed']:
        ...     print(f"Found issues in {len(results['reaches_with_issues'])} reaches")
        """
        error_counts = {
            "type_0_missing_fields": 0,
            "type_1_count_mismatch": 0,
            "type_2_unrequited_neighbor": 0,
            "type_5_self_reference": 0,
        }
        warning_counts = {
            "type_3_ghost_both_neighbors": 0,
            "type_4_no_upstream": 0,
        }
        reaches_with_issues = set()
        details = []

        # Build reach lookup for efficient neighbor checking
        reach_ids = self.reaches.id
        reach_idx = {rid: i for i, rid in enumerate(reach_ids)}

        for idx, reach_id in enumerate(reach_ids):
            rid_str = str(reach_id)
            reach_type = rid_str[-1]

            # Get topology data
            n_rch_up = self.reaches.n_rch_up[idx]
            n_rch_down = self.reaches.n_rch_down[idx]
            rch_id_up = self.reaches.rch_id_up[:, idx]
            rch_id_down = self.reaches.rch_id_down[:, idx]

            # Check 1: Count mismatch
            actual_up = np.count_nonzero(rch_id_up)
            actual_down = np.count_nonzero(rch_id_down)

            if actual_up != n_rch_up:
                error_counts["type_1_count_mismatch"] += 1
                reaches_with_issues.add(reach_id)
                msg = f"Type 1: Reach {rid_str} claims {n_rch_up} upstream, but has {actual_up}"
                if verbose >= 1:
                    logger.warning(msg)
                if return_details:
                    details.append({"type": 1, "reach_id": reach_id, "message": msg})

            if actual_down != n_rch_down:
                error_counts["type_1_count_mismatch"] += 1
                reaches_with_issues.add(reach_id)
                msg = f"Type 1: Reach {rid_str} claims {n_rch_down} downstream, but has {actual_down}"
                if verbose >= 1:
                    logger.warning(msg)
                if return_details:
                    details.append({"type": 1, "reach_id": reach_id, "message": msg})

            # Check 2: Unrequited neighbors - downstream
            for neighbor_id in rch_id_down:
                if neighbor_id == 0:
                    continue
                neighbor_idx = reach_idx.get(neighbor_id)
                if neighbor_idx is None:
                    error_counts["type_2_unrequited_neighbor"] += 1
                    reaches_with_issues.add(reach_id)
                    msg = f"Type 2: Reach {rid_str} references non-existent downstream {neighbor_id}"
                    if verbose >= 1:
                        logger.warning(msg)
                    if return_details:
                        details.append(
                            {"type": 2, "reach_id": reach_id, "message": msg}
                        )
                else:
                    # Check if neighbor has us as upstream
                    neighbor_up = self.reaches.rch_id_up[:, neighbor_idx]
                    if reach_id not in neighbor_up:
                        error_counts["type_2_unrequited_neighbor"] += 1
                        reaches_with_issues.add(reach_id)
                        reaches_with_issues.add(neighbor_id)
                        msg = f"Type 2: {rid_str} -> {neighbor_id} (down) is unrequited"
                        if verbose >= 1:
                            logger.warning(msg)
                        if return_details:
                            details.append(
                                {"type": 2, "reach_id": reach_id, "message": msg}
                            )

            # Check 2: Unrequited neighbors - upstream
            for neighbor_id in rch_id_up:
                if neighbor_id == 0:
                    continue
                neighbor_idx = reach_idx.get(neighbor_id)
                if neighbor_idx is None:
                    error_counts["type_2_unrequited_neighbor"] += 1
                    reaches_with_issues.add(reach_id)
                    msg = f"Type 2: Reach {rid_str} references non-existent upstream {neighbor_id}"
                    if verbose >= 1:
                        logger.warning(msg)
                    if return_details:
                        details.append(
                            {"type": 2, "reach_id": reach_id, "message": msg}
                        )
                else:
                    # Check if neighbor has us as downstream
                    neighbor_down = self.reaches.rch_id_down[:, neighbor_idx]
                    if reach_id not in neighbor_down:
                        error_counts["type_2_unrequited_neighbor"] += 1
                        reaches_with_issues.add(reach_id)
                        reaches_with_issues.add(neighbor_id)
                        msg = f"Type 2: {rid_str} -> {neighbor_id} (up) is unrequited"
                        if verbose >= 1:
                            logger.warning(msg)
                        if return_details:
                            details.append(
                                {"type": 2, "reach_id": reach_id, "message": msg}
                            )

            # Check 3: Ghost reach with both upstream and downstream (warning)
            if reach_type == "6" and n_rch_up > 0 and n_rch_down > 0:
                warning_counts["type_3_ghost_both_neighbors"] += 1
                if verbose >= 2:
                    logger.warning(
                        f"Type 3 Warning: Ghost reach {rid_str} has both up and downstream"
                    )
                if return_details:
                    details.append(
                        {
                            "type": 3,
                            "reach_id": reach_id,
                            "message": f"Ghost reach {rid_str} has both neighbors",
                            "is_warning": True,
                        }
                    )

            # Check 4: Non-ghost with no upstream (warning)
            if n_rch_up == 0 and reach_type != "6":
                warning_counts["type_4_no_upstream"] += 1
                if verbose >= 2:
                    logger.warning(
                        f"Type 4 Warning: Non-ghost reach {rid_str} has no upstream"
                    )
                if return_details:
                    details.append(
                        {
                            "type": 4,
                            "reach_id": reach_id,
                            "message": f"Non-ghost reach {rid_str} has no upstream",
                            "is_warning": True,
                        }
                    )

            # Check 5: Self-reference
            if reach_id in rch_id_up or reach_id in rch_id_down:
                error_counts["type_5_self_reference"] += 1
                reaches_with_issues.add(reach_id)
                msg = f"Type 5: Reach {rid_str} references itself as neighbor"
                if verbose >= 1:
                    logger.warning(msg)
                if return_details:
                    details.append({"type": 5, "reach_id": reach_id, "message": msg})

        # Calculate totals
        total_errors = sum(error_counts.values())
        total_warnings = sum(warning_counts.values())

        results = {
            "passed": total_errors == 0,
            "total_reaches": len(reach_ids),
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "error_counts": error_counts,
            "warning_counts": warning_counts,
            "reaches_with_issues": list(reaches_with_issues),
        }

        if return_details:
            results["details"] = details

        if verbose >= 1:
            logger.info("Topology Check Summary:")
            logger.info(f"  Reaches checked: {len(reach_ids)}")
            logger.info(f"  Total errors: {total_errors}")
            logger.info(f"  Total warnings: {total_warnings}")
            if error_counts["type_1_count_mismatch"]:
                logger.info(
                    f"    Type 1 (count mismatch): {error_counts['type_1_count_mismatch']}"
                )
            if error_counts["type_2_unrequited_neighbor"]:
                logger.info(
                    f"    Type 2 (unrequited): {error_counts['type_2_unrequited_neighbor']}"
                )
            if error_counts["type_5_self_reference"]:
                logger.info(
                    f"    Type 5 (self-ref): {error_counts['type_5_self_reference']}"
                )

        return results

    def check_node_lengths(
        self, verbose: int = 1, long_threshold: float = 1000.0, warn_zero: bool = True
    ) -> dict:
        """
        Check for abnormal node lengths.

        Parameters
        ----------
        verbose : int, optional
            Output verbosity level (0=silent, 1=errors, 2=all). Default is 1.
        long_threshold : float, optional
            Length threshold (meters) above which nodes are flagged. Default is 1000m.
        warn_zero : bool, optional
            If True, flag nodes with zero length. Default is True.

        Returns
        -------
        dict
            Dictionary containing:
            - 'passed': bool - True if no issues found
            - 'long_nodes': list - Node IDs exceeding threshold
            - 'zero_length_nodes': list - Node IDs with zero length
            - 'affected_reaches': list - Reaches containing problem nodes

        Notes
        -----
        Target node length is 200m. Nodes over 1000m or with zero length
        typically indicate processing issues.
        """
        long_nodes = []
        zero_nodes = []
        affected_reaches = set()

        for idx, node_id in enumerate(self.nodes.id):
            node_len = self.nodes.len[idx]
            reach_id = self.nodes.reach_id[idx]

            if node_len > long_threshold:
                long_nodes.append(node_id)
                affected_reaches.add(reach_id)
                if verbose >= 1:
                    logger.warning(
                        f"Long node: {node_id} in reach {reach_id} ({node_len:.1f}m)"
                    )

            if warn_zero and node_len == 0:
                zero_nodes.append(node_id)
                affected_reaches.add(reach_id)
                if verbose >= 2:
                    logger.warning(f"Zero length node: {node_id} in reach {reach_id}")

        results = {
            "passed": len(long_nodes) == 0 and (not warn_zero or len(zero_nodes) == 0),
            "total_nodes": len(self.nodes.id),
            "long_nodes": long_nodes,
            "zero_length_nodes": zero_nodes,
            "affected_reaches": list(affected_reaches),
        }

        if verbose >= 1:
            logger.info("Node Length Check Summary:")
            logger.info(f"  Nodes checked: {len(self.nodes.id)}")
            logger.info(f"  Long nodes (>{long_threshold}m): {len(long_nodes)}")
            logger.info(f"  Zero length nodes: {len(zero_nodes)}")

        return results

    def calculate_dist_out_from_topology(
        self, update_nodes: bool = True, verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Calculate distance from outlet (dist_out) using topology BFS traversal.

        This method implements the legacy dist_out_from_topo.py algorithm exactly.
        It traverses the river network from outlets upstream, computing cumulative
        reach lengths as distance from the nearest outlet.

        Parameters
        ----------
        update_nodes : bool, optional
            If True (default), also update node-level dist_out values after
            computing reach-level values.
        verbose : bool, optional
            If True (default), print progress information.

        Returns
        -------
        dict
            Operation results including:
            - 'success': bool - Whether operation completed
            - 'reaches_updated': int - Number of reaches with updated dist_out
            - 'nodes_updated': int - Number of nodes with updated dist_out
            - 'outlets_found': int - Number of outlet reaches processed
            - 'loops': int - Number of BFS iterations
            - 'unfilled_reaches': list - Reach IDs that couldn't be computed

        Notes
        -----
        Algorithm (from legacy dist_out_from_topo.py):
        1. Initialize dist_out = -9999 for all reaches
        2. Find all outlets (reaches with n_rch_down == 0)
        3. Start BFS from first outlet:
           - For outlets: dist_out = reach_length
           - For non-outlets: dist_out = reach_length + max(downstream dist_out)
        4. Handle multi-channel cases with flag system
        5. When upstream exhausted, move to next unfilled outlet
        6. Continue until all reaches processed
        7. Update node dist_out: cumsum(node_lengths) + base_val

        Multi-Downstream Handling:
        When a reach has multiple downstream neighbors, uses max(downstream dist_out)
        to ensure distance is calculated along the longest path to outlet.
        """
        from itertools import chain

        if verbose:
            logger.info("Calculating dist_out from Topology")

        # Initialize arrays following legacy code exactly
        dist_out = np.repeat(-9999.0, len(self.reaches.id))
        flag = np.zeros(len(self.reaches.id))

        # Find all outlets (reaches with no downstream neighbors)
        outlets = self.reaches.id[np.where(self.reaches.n_rch_down == 0)[0]]
        outlets_found = len(outlets)

        if len(outlets) == 0:
            if verbose:
                logger.warning("No outlet reaches found (n_rch_down == 0)")
            return {
                "success": False,
                "reaches_updated": 0,
                "nodes_updated": 0,
                "outlets_found": 0,
                "loops": 0,
                "unfilled_reaches": list(self.reaches.id),
            }

        # Start with first outlet
        start_rchs = np.array([outlets[0]])
        loop = 1
        max_loops = 5 * len(self.reaches.id)

        # Create a lookup for reach IDs to indices for O(1) lookup
        reach_id_map = {rid: i for i, rid in enumerate(self.reaches.id)}

        # BFS traversal - following legacy algorithm exactly
        while len(start_rchs) > 0:
            up_ngh_list = []

            for r in range(len(start_rchs)):
                rch = reach_id_map.get(start_rchs[r])
                if rch is None:
                    continue
                rch_flag = np.max(flag[rch])

                if self.reaches.n_rch_down[rch] == 0:
                    # Outlet reach: dist_out = reach_length
                    dist_out[rch] = self.reaches.len[rch]

                    # Get upstream neighbors
                    up_nghs = self.reaches.rch_id_up[:, rch]
                    up_nghs = up_nghs[up_nghs > 0]

                    # Filter out already-flagged neighbors
                    up_flag = np.array(
                        [
                            np.max(flag[np.where(self.reaches.id == n)[0]])
                            for n in up_nghs
                            if len(np.where(self.reaches.id == n)[0]) > 0
                        ]
                    )
                    if len(up_flag) > 0:
                        up_nghs = up_nghs[: len(up_flag)][up_flag == 0]

                    up_ngh_list.append(up_nghs)
                else:
                    # Non-outlet reach: need downstream dist_out first
                    dn_nghs = self.reaches.rch_id_down[:, rch]
                    dn_nghs = dn_nghs[dn_nghs > 0]

                    # Get downstream distances
                    dn_dist = []
                    for n in dn_nghs:
                        idx = reach_id_map.get(n)
                        if idx is not None:
                            dn_dist.append(dist_out[idx])
                    
                    dn_dist = np.array(dn_dist)

                    if len(dn_dist) == 0:
                        continue

                    if min(dn_dist) == -9999:
                        # Some downstream not yet computed — defer.
                        # Increment flag so we can detect truly stuck reaches
                        # in the stall-recovery loop below.
                        flag[rch] = rch_flag + 1
                    else:
                        # All downstream computed, use max
                        add_val = max(dn_dist)
                        dist_out[rch] = self.reaches.len[rch] + add_val

                        # Get upstream neighbors
                        up_nghs = self.reaches.rch_id_up[:, rch]
                        up_nghs = up_nghs[up_nghs > 0]

                        up_flag = np.array(
                            [
                                np.max(flag[np.where(self.reaches.id == n)[0]])
                                for n in up_nghs
                                if len(np.where(self.reaches.id == n)[0]) > 0
                            ]
                        )
                        if len(up_flag) > 0:
                            up_nghs = up_nghs[: len(up_flag)][up_flag == 0]

                        up_ngh_list.append(up_nghs)

            # Flatten and get unique upstream neighbors for next iteration
            up_ngh_arr = np.array(list(chain.from_iterable(up_ngh_list)))
            start_rchs = np.unique(up_ngh_arr)

            # If no more upstream neighbors, move to next outlet
            if len(start_rchs) == 0:
                # Find unfilled outlets
                unfilled_outlets = self.reaches.id[
                    np.where((self.reaches.n_rch_down == 0) & (dist_out == -9999))[0]
                ]

                if len(unfilled_outlets) > 0:
                    # Process next unfilled outlet
                    start_rchs = np.array([unfilled_outlets[0]])
                elif np.min(dist_out) > -9999:
                    # All done
                    start_rchs = np.array([])
                else:
                    # No unfilled outlets but some reaches still -9999.
                    # These are deferred bifurcation parents whose children
                    # route to different outlets.  Now that all outlets are
                    # processed, some children may have been filled.  Re-queue
                    # any deferred reach that now has all downstream computed.
                    deferred = np.where((flag > 0) & (dist_out == -9999))[0]
                    newly_ready = []
                    for idx in deferred:
                        dn_nghs = self.reaches.rch_id_down[:, idx]
                        dn_nghs = dn_nghs[dn_nghs > 0]
                        dn_d = []
                        for n in dn_nghs:
                            idx_ngh = reach_id_map.get(n)
                            if idx_ngh is not None:
                                dn_d.append(dist_out[idx_ngh])
                        
                        if len(dn_d) > 0 and min(dn_d) > -9999:
                            newly_ready.append(self.reaches.id[idx])
                    
                    if len(newly_ready) > 0:
                        start_rchs = np.array(newly_ready)
                    else:
                        # Truly stuck — some downstream children are
                        # unreachable (disconnected components).  Use max
                        # of available downstream values as fallback.
                        stuck = []
                        for idx in deferred:
                            dn_nghs = self.reaches.rch_id_down[:, idx]
                            dn_nghs = dn_nghs[dn_nghs > 0]
                            dn_d = []
                            for n in dn_nghs:
                                idx_ngh = reach_id_map.get(n)
                                if idx_ngh is not None:
                                    dn_d.append(dist_out[idx_ngh])
                            
                            dn_d = np.array(dn_d)
                            valid = dn_d[dn_d > -9999]
                            if len(valid) > 0:
                                dist_out[idx] = self.reaches.len[idx] + max(valid)
                                stuck.append(self.reaches.id[idx])
                        if len(stuck) > 0:
                            if verbose:
                                logger.warning(
                                    f"{len(stuck)} reaches resolved via "
                                    f"fallback (partial downstream)"
                                )
                            # Re-queue their upstream neighbors
                            start_rchs = np.array(stuck)
                        else:
                            if verbose:
                                logger.warning("No more outlets but still -9999 values")
                            break

            loop += 1
            if loop > max_loops:
                if verbose:
                    logger.warning("Loop limit reached")
                break

        # Count unfilled reaches
        unfilled_idx = np.where(dist_out == -9999)[0]
        unfilled_reaches = list(self.reaches.id[unfilled_idx])

        if verbose:
            logger.info(f"Processed {loop} iterations")
            logger.info(
                f"{len(self.reaches.id) - len(unfilled_reaches)}/{len(self.reaches.id)} reaches computed"
            )
            if len(unfilled_reaches) > 0:
                logger.warning(f"{len(unfilled_reaches)} reaches unfilled")

        # Update reach dist_out in database
        conn = self._db.connect()
        
        valid_idx = np.where(dist_out != -9999)[0]
        if len(valid_idx) > 0:
            df_updates = pd.DataFrame({
                'reach_id': self.reaches.id[valid_idx],
                'dist_out': dist_out[valid_idx]
            })
            
            try:
                conn.register('_reach_updates', df_updates)
                conn.execute(f"""
                    UPDATE reaches 
                    SET dist_out = u.dist_out
                    FROM _reach_updates u
                    WHERE reaches.reach_id = u.reach_id AND reaches.region = '{self.region}'
                """)
                conn.unregister('_reach_updates')
            except Exception as e:
                raise RuntimeError(f"Failed to update reach dist_out: {e}") from e

        # Update nodes if requested
        nodes_updated = 0
        if update_nodes:
            if verbose:
                logger.info("Updating node dist_out values")
            nodes_updated = self._calculate_node_dist_out(dist_out, verbose)

        # Reload data
        self._load_data()

        return {
            "success": len(unfilled_reaches) == 0,
            "reaches_updated": len(self.reaches.id) - len(unfilled_reaches),
            "nodes_updated": nodes_updated,
            "outlets_found": outlets_found,
            "loops": loop,
            "unfilled_reaches": unfilled_reaches,
        }

    def _calculate_node_dist_out(
        self, reach_dist_out: np.ndarray, verbose: bool = False
    ) -> int:
        """
        Calculate node-level dist_out from reach-level values.

        For each reach, node dist_out is the cumulative node length plus
        the base value (reach dist_out minus reach length).

        Parameters
        ----------
        reach_dist_out : np.ndarray
            Array of reach-level dist_out values (indexed same as self.reaches.id)
        verbose : bool, optional
            If True, print progress. Default is False.

        Returns
        -------
        int
            Number of nodes updated

        Notes
        -----
        Algorithm (from legacy dist_out_from_topo.py):
        ```python
        base_val = dist_out[reach] - reach_length
        node_dist_out = cumsum(node_lengths) + base_val
        ```
        """
        nodes_out = np.copy(self.nodes.dist_out)
        updated_count = 0

        for r in range(len(self.reaches.id)):
            if reach_dist_out[r] == -9999:
                continue

            # Find nodes for this reach
            nds = np.where(self.nodes.reach_id == self.reaches.id[r])[0]
            if len(nds) == 0:
                continue

            # Sort nodes by ID ascending (downstream to upstream)
            sort_nodes = np.argsort(self.nodes.id[nds])

            # Distance at the downstream end of the reach:
            dn_val = reach_dist_out[r] - self.reaches.len[r]

            # Assign node dist_out values by interpolating between the
            # downstream end (dn_val) and the upstream end (reach_dist_out).
            # This ensures perfect continuity at boundaries (gap = 0)
            # while respecting the node order.
            if len(nds) > 1:
                node_values = np.linspace(dn_val, reach_dist_out[r], len(nds))
            else:
                # For single-node reaches, use the reach dist_out (upstream end)
                node_values = np.array([reach_dist_out[r]])

            sort_nodes = np.argsort(self.nodes.id[nds])
            nodes_out[nds[sort_nodes]] = node_values
            updated_count += len(nds)

        # Update database
        conn = self._db.connect()
        
        # Filter to nodes that actually changed to reduce data transfer
        changed_mask = nodes_out != self.nodes.dist_out
        if np.any(changed_mask):
            df_node_updates = pd.DataFrame({
                'node_id': self.nodes.id[changed_mask],
                'dist_out': nodes_out[changed_mask]
            })
            
            try:
                conn.register('_node_updates', df_node_updates)
                conn.execute(f"""
                    UPDATE nodes 
                    SET dist_out = u.dist_out
                    FROM _node_updates u
                    WHERE nodes.node_id = u.node_id AND nodes.region = '{self.region}'
                """)
                conn.unregister('_node_updates')
            except Exception as e:
                raise RuntimeError(f"Failed to update node dist_out: {e}") from e

        if verbose:
            logger.info(f"Updated {updated_count} nodes")

        return updated_count

    def create_ghost_reach(
        self, reach_id: int, position: str = "auto", verbose: bool = False
    ) -> dict:
        """
        Create a ghost reach at the headwater or outlet of an existing reach.

        Ghost reaches (type 6) are placeholder reaches used to mark network
        endpoints. This method creates a new ghost reach by extracting the
        first or last node from an existing reach and assigning it to a new
        ghost reach with proper SWORD ID format.

        Parameters
        ----------
        reach_id : int
            The reach ID to split a ghost reach from.
        position : str, optional
            Where to create the ghost reach:
            - 'headwater': Create at upstream end (takes first node)
            - 'outlet': Create at downstream end (takes last node)
            - 'auto': Automatically determine based on topology (default)
              Uses 'headwater' if reach has no upstream neighbors,
              'outlet' if reach has no downstream neighbors.
        verbose : bool, optional
            If True, print progress information. Default is False.

        Returns
        -------
        dict
            Operation results including:
            - 'success': bool - Whether operation succeeded
            - 'original_reach': int - The original reach ID
            - 'ghost_reach_id': int - The new ghost reach ID
            - 'ghost_node_id': int - The new ghost node ID
            - 'position': str - Where the ghost was created ('headwater' or 'outlet')

        Raises
        ------
        ValueError
            If position='auto' but reach has both up and down neighbors,
            or if the reach has only one node and can't be split.

        Notes
        -----
        ID Generation:
        - Ghost Reach ID: CBBBBBRRRR6 (11 digits, type=6)
          where RRRR is max(basin_reach_nums) + 1
        - Ghost Node ID: CBBBBBRRRRNNNG (14 digits, type=6)
          where NNN starts at 001

        Algorithm (from create_missing_ghost_reach.py):
        1. Find the node to extract (first for headwater, last for outlet)
        2. Generate new ghost reach ID (basin + max_rch + 1 + '6')
        3. Generate new ghost node ID (reach_prefix + node_num + '6')
        4. Update centerlines to reference new reach/node
        5. Update or create node record
        6. Create new reach record with copied attributes
        7. Update topology connections

        Example
        -------
        >>> # Create ghost at headwater of a reach with no upstream
        >>> result = sword.create_ghost_reach(72140300041, position='headwater')
        >>> print(f"Created ghost reach: {result['ghost_reach_id']}")
        """
        import gc

        # Validate reach exists
        rch_idx = np.where(self.reaches.id == reach_id)[0]
        if len(rch_idx) == 0:
            raise ValueError(f"Reach {reach_id} not found")
        rch_idx = rch_idx[0]

        # Determine position if auto
        n_up = self.reaches.n_rch_up[rch_idx]
        n_down = self.reaches.n_rch_down[rch_idx]

        if position == "auto":
            if n_up == 0 and n_down == 0:
                # Isolated reach - default to headwater
                position = "headwater"
            elif n_up == 0:
                position = "headwater"
            elif n_down == 0:
                position = "outlet"
            else:
                raise ValueError(
                    f"Reach {reach_id} has both upstream ({n_up}) and "
                    f"downstream ({n_down}) neighbors. Specify position explicitly."
                )

        if position not in ("headwater", "outlet"):
            raise ValueError(
                f"Invalid position: {position}. Use 'headwater', 'outlet', or 'auto'"
            )

        # Get nodes for this reach, sorted by ID
        node_indices = np.where(self.nodes.reach_id == reach_id)[0]
        if len(node_indices) == 0:
            raise ValueError(f"Reach {reach_id} has no nodes")

        node_ids_sorted = np.sort(self.nodes.id[node_indices])

        # Check if reach has enough nodes to split
        if len(node_ids_sorted) < 2:
            raise ValueError(
                f"Reach {reach_id} has only {len(node_ids_sorted)} node(s). "
                f"Cannot create ghost reach (need at least 2 nodes)."
            )

        # Select node to extract
        if position == "headwater":
            ghost_node_old_id = node_ids_sorted[-1]  # Upstream = highest node ID
        else:
            ghost_node_old_id = node_ids_sorted[0]  # Downstream = lowest node ID

        # Get basin info from reach ID
        reach_str = str(reach_id)
        level6_basin = reach_str[0:6]  # CBBBBB

        # Find max reach number in this basin
        cl_level6 = np.array([str(nid)[0:6] for nid in self.centerlines.node_id[0, :]])
        basin_mask = cl_level6 == level6_basin
        cl_rch_nums = np.array(
            [int(str(nid)[6:10]) for nid in self.centerlines.node_id[0, basin_mask]]
        )
        new_rch_num = np.max(cl_rch_nums) + 1 if len(cl_rch_nums) > 0 else 1

        # Generate new ghost reach ID (type 6)
        new_ghost_rch_id = int(f"{level6_basin}{new_rch_num:04d}6")

        # Generate new ghost node ID
        new_ghost_node_id = int(f"{level6_basin}{new_rch_num:04d}0016")

        if verbose:
            logger.info(
                f"Creating ghost reach {new_ghost_rch_id} at {position} of {reach_id}"
            )
            logger.debug(f"Extracting node {ghost_node_old_id} -> {new_ghost_node_id}")

        # Get centerlines for the node being extracted
        cl_indices = np.where(self.centerlines.node_id[0, :] == ghost_node_old_id)[0]
        if len(cl_indices) == 0:
            raise ValueError(f"No centerlines found for node {ghost_node_old_id}")

        # Disable GC during database operations
        gc_was_enabled = gc.isenabled()
        gc.disable()

        conn = self._db.connect()
        conn.execute("BEGIN TRANSACTION")

        try:
            # 1. Update centerlines to reference new reach/node
            for cl_idx in cl_indices:
                cl_id = int(self.centerlines.cl_id[cl_idx])
                conn.execute(
                    """
                    UPDATE centerlines SET
                        reach_id = ?,
                        node_id = ?
                    WHERE cl_id = ? AND region = ?
                """,
                    [new_ghost_rch_id, new_ghost_node_id, cl_id, self.region],
                )

            # Also update neighbor references in centerline_neighbors
            conn.execute(
                """
                UPDATE centerline_neighbors SET reach_id = ?
                WHERE reach_id = ? AND region = ?
            """,
                [new_ghost_rch_id, reach_id, self.region],
            )

            conn.execute(
                """
                UPDATE centerline_neighbors SET node_id = ?
                WHERE node_id = ? AND region = ?
            """,
                [new_ghost_node_id, ghost_node_old_id, self.region],
            )

            # 2. Update node record with new IDs
            conn.execute(
                """
                UPDATE nodes SET
                    node_id = ?,
                    reach_id = ?,
                    edit_flag = CASE
                        WHEN edit_flag IS NULL OR edit_flag = '' OR edit_flag = 'NaN'
                        THEN '6'
                        WHEN edit_flag NOT LIKE '%6%'
                        THEN edit_flag || ',6'
                        ELSE edit_flag
                    END
                WHERE node_id = ? AND region = ?
            """,
                [new_ghost_node_id, new_ghost_rch_id, ghost_node_old_id, self.region],
            )

            # 3. Create new ghost reach record by copying from original
            # Get attributes from original reach
            reach_data = conn.execute(
                """
                SELECT * FROM reaches WHERE reach_id = ? AND region = ?
            """,
                [reach_id, self.region],
            ).fetchone()

            if reach_data is None:
                raise ValueError(f"Could not find reach {reach_id} in database")

            # Get node data for geometry
            node_data = conn.execute(
                """
                SELECT x, y, node_length, wse, wse_var, width, width_var,
                       max_width, facc, dist_out, lakeflag, obstr_type,
                       grod_id, hfalls_id, n_chan_max, n_chan_mod,
                       river_name, trib_flag, stream_order, network
                FROM nodes WHERE node_id = ? AND region = ?
            """,
                [new_ghost_node_id, self.region],
            ).fetchone()

            if node_data is None:
                raise ValueError(f"Could not find updated node {new_ghost_node_id}")

            # Get cl_id bounds for ghost reach
            cl_bounds = conn.execute(
                """
                SELECT MIN(cl_id), MAX(cl_id) FROM centerlines
                WHERE reach_id = ? AND region = ?
            """,
                [new_ghost_rch_id, self.region],
            ).fetchone()

            # Determine end_reach value and topology
            if position == "headwater":
                end_reach = 1
                n_rch_up_ghost = 0
                n_rch_down_ghost = 1
            else:
                end_reach = 2
                n_rch_up_ghost = 1
                n_rch_down_ghost = 0

            # Calculate edit flag
            edit_flag = "6"

            # Insert new ghost reach
            conn.execute(
                """
                INSERT INTO reaches (
                    reach_id, region, x, y, x_min, x_max, y_min, y_max,
                    cl_id_min, cl_id_max, reach_length, n_nodes, wse, wse_var,
                    width, width_var, slope, max_width, facc, dist_out, lakeflag,
                    obstr_type, grod_id, hfalls_id, n_chan_max, n_chan_mod,
                    n_rch_up, n_rch_down, swot_obs, low_slope_flag, river_name,
                    edit_flag, trib_flag, path_freq, path_order, path_segs,
                    stream_order, main_side, end_reach, network, add_flag, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    new_ghost_rch_id,
                    self.region,
                    float(node_data[0]),  # x
                    float(node_data[1]),  # y
                    float(node_data[0]),  # x_min
                    float(node_data[0]),  # x_max
                    float(node_data[1]),  # y_min
                    float(node_data[1]),  # y_max
                    int(cl_bounds[0]) if cl_bounds[0] else 0,  # cl_id_min
                    int(cl_bounds[1]) if cl_bounds[1] else 0,  # cl_id_max
                    float(node_data[2]),  # reach_length = node_length
                    1,  # n_nodes
                    float(node_data[3]),  # wse
                    float(node_data[4]),  # wse_var
                    float(node_data[5]),  # width
                    float(node_data[6]),  # width_var
                    0.0,  # slope - ghost reaches typically have no slope
                    float(node_data[7]),  # max_width
                    float(node_data[8]),  # facc
                    float(node_data[9]),  # dist_out
                    int(node_data[10]),  # lakeflag
                    int(node_data[11]),  # obstr_type
                    int(node_data[12]),  # grod_id
                    int(node_data[13]),  # hfalls_id
                    int(node_data[14]),  # n_chan_max
                    int(node_data[15]),  # n_chan_mod
                    n_rch_up_ghost,
                    n_rch_down_ghost,
                    0,  # swot_obs
                    0,  # low_slope_flag
                    str(node_data[16]) if node_data[16] else "",  # river_name
                    edit_flag,
                    int(node_data[17]) if node_data[17] else 0,  # trib_flag
                    0,  # path_freq
                    0,  # path_order
                    0,  # path_segs
                    int(node_data[18]) if node_data[18] else 0,  # stream_order
                    0,  # main_side
                    end_reach,
                    int(node_data[19]) if node_data[19] else 0,  # network
                    0,  # add_flag
                    self.version,
                ],
            )

            # 4. Update topology for ghost reach
            if position == "headwater":
                # Ghost is upstream of original reach
                conn.execute(
                    """
                    INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                    VALUES (?, ?, 'down', 0, ?)
                """,
                    [new_ghost_rch_id, self.region, reach_id],
                )
            else:
                # Ghost is downstream of original reach
                conn.execute(
                    """
                    INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                    VALUES (?, ?, 'up', 0, ?)
                """,
                    [new_ghost_rch_id, self.region, reach_id],
                )

            # 5. Update original reach's topology to reference ghost
            if position == "headwater":
                # Original reach now has ghost as upstream neighbor
                # First check if there's an existing 'up' entry
                existing = conn.execute(
                    """
                    SELECT COUNT(*) FROM reach_topology
                    WHERE reach_id = ? AND region = ? AND direction = 'up'
                """,
                    [reach_id, self.region],
                ).fetchone()[0]

                if existing == 0:
                    conn.execute(
                        """
                        INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                        VALUES (?, ?, 'up', 0, ?)
                    """,
                        [reach_id, self.region, new_ghost_rch_id],
                    )
                else:
                    # Find next available rank
                    max_rank = conn.execute(
                        """
                        SELECT MAX(neighbor_rank) FROM reach_topology
                        WHERE reach_id = ? AND region = ? AND direction = 'up'
                    """,
                        [reach_id, self.region],
                    ).fetchone()[0]
                    conn.execute(
                        """
                        INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                        VALUES (?, ?, 'up', ?, ?)
                    """,
                        [reach_id, self.region, max_rank + 1, new_ghost_rch_id],
                    )

                # Update n_rch_up for original reach
                conn.execute(
                    """
                    UPDATE reaches SET n_rch_up = n_rch_up + 1, end_reach = 0
                    WHERE reach_id = ? AND region = ?
                """,
                    [reach_id, self.region],
                )
            else:
                # Original reach now has ghost as downstream neighbor
                existing = conn.execute(
                    """
                    SELECT COUNT(*) FROM reach_topology
                    WHERE reach_id = ? AND region = ? AND direction = 'down'
                """,
                    [reach_id, self.region],
                ).fetchone()[0]

                if existing == 0:
                    conn.execute(
                        """
                        INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                        VALUES (?, ?, 'down', 0, ?)
                    """,
                        [reach_id, self.region, new_ghost_rch_id],
                    )
                else:
                    max_rank = conn.execute(
                        """
                        SELECT MAX(neighbor_rank) FROM reach_topology
                        WHERE reach_id = ? AND region = ? AND direction = 'down'
                    """,
                        [reach_id, self.region],
                    ).fetchone()[0]
                    conn.execute(
                        """
                        INSERT INTO reach_topology (reach_id, region, direction, neighbor_rank, neighbor_reach_id)
                        VALUES (?, ?, 'down', ?, ?)
                    """,
                        [reach_id, self.region, max_rank + 1, new_ghost_rch_id],
                    )

                # Update n_rch_down for original reach
                conn.execute(
                    """
                    UPDATE reaches SET n_rch_down = n_rch_down + 1, end_reach = 0
                    WHERE reach_id = ? AND region = ?
                """,
                    [reach_id, self.region],
                )

            # 6. Update original reach attributes
            # Recalculate geometry excluding the extracted node
            remaining_cl = conn.execute(
                """
                SELECT cl_id, x, y FROM centerlines
                WHERE reach_id = ? AND region = ?
                ORDER BY cl_id
            """,
                [reach_id, self.region],
            ).fetchall()

            if len(remaining_cl) > 0:
                x_coords = [r[1] for r in remaining_cl]
                y_coords = [r[2] for r in remaining_cl]
                cl_ids = [r[0] for r in remaining_cl]

                conn.execute(
                    """
                    UPDATE reaches SET
                        x = ?,
                        y = ?,
                        x_min = ?,
                        x_max = ?,
                        y_min = ?,
                        y_max = ?,
                        cl_id_min = ?,
                        cl_id_max = ?,
                        n_nodes = n_nodes - 1,
                        edit_flag = CASE
                            WHEN edit_flag IS NULL OR edit_flag = '' OR edit_flag = 'NaN'
                            THEN '6'
                            WHEN edit_flag NOT LIKE '%6%'
                            THEN edit_flag || ',6'
                            ELSE edit_flag
                        END
                    WHERE reach_id = ? AND region = ?
                """,
                    [
                        float(np.median(x_coords)),
                        float(np.median(y_coords)),
                        float(np.min(x_coords)),
                        float(np.max(x_coords)),
                        float(np.min(y_coords)),
                        float(np.max(y_coords)),
                        int(np.min(cl_ids)),
                        int(np.max(cl_ids)),
                        reach_id,
                        self.region,
                    ],
                )

            # Copy orbits and ice flags from original reach
            self._copy_reach_orbits(conn, reach_id, new_ghost_rch_id)
            self._copy_reach_ice_flags(conn, reach_id, new_ghost_rch_id)

            conn.execute("COMMIT")

            # Reload data to refresh views
            self._load_data()

            if verbose:
                logger.info(f"Successfully created ghost reach {new_ghost_rch_id}")

            return {
                "success": True,
                "original_reach": reach_id,
                "ghost_reach_id": new_ghost_rch_id,
                "ghost_node_id": new_ghost_node_id,
                "position": position,
            }

        except Exception as e:
            conn.execute("ROLLBACK")
            raise RuntimeError(f"Failed to create ghost reach: {e}") from e
        finally:
            if gc_was_enabled:
                gc.enable()

    def find_missing_ghost_reaches(self) -> dict:
        """
        Find reaches that should have ghost reaches but don't.

        Ghost reaches are typically needed at:
        - Headwaters: Non-ghost reaches (type != 6) with no upstream neighbors
        - Outlets: Non-ghost reaches (type != 6) with no downstream neighbors

        Returns
        -------
        dict
            Dictionary containing:
            - 'missing_headwaters': list of reach IDs needing headwater ghosts
            - 'missing_outlets': list of reach IDs needing outlet ghosts
            - 'total_missing': int total count

        Example
        -------
        >>> missing = sword.find_missing_ghost_reaches()
        >>> print(f"Found {missing['total_missing']} reaches needing ghost reaches")
        >>> for rid in missing['missing_headwaters'][:5]:
        ...     sword.create_ghost_reach(rid, position='headwater')
        """
        missing_headwaters = []
        missing_outlets = []

        for idx, reach_id in enumerate(self.reaches.id):
            reach_type = str(reach_id)[-1]

            # Skip existing ghost reaches
            if reach_type == "6":
                continue

            n_up = self.reaches.n_rch_up[idx]
            n_down = self.reaches.n_rch_down[idx]

            if n_up == 0:
                missing_headwaters.append(reach_id)
            if n_down == 0:
                missing_outlets.append(reach_id)

        return {
            "missing_headwaters": missing_headwaters,
            "missing_outlets": missing_outlets,
            "total_missing": len(missing_headwaters) + len(missing_outlets),
        }

    def find_incorrect_ghost_reaches(self) -> dict:
        """
        Find ghost reaches that are incorrectly labeled.

        A ghost reach (type 6) should only have neighbors in ONE direction
        (either upstream OR downstream, not both). Ghost reaches with both
        are likely mislabeled and should be a different type.

        Returns
        -------
        dict
            Dictionary containing:
            - 'incorrect_ghost_reaches': list of dicts with reach_id and suggested_type
            - 'total_incorrect': int

        Example
        -------
        >>> incorrect = sword.find_incorrect_ghost_reaches()
        >>> for item in incorrect['incorrect_ghost_reaches']:
        ...     print(f"Reach {item['reach_id']} should be type {item['suggested_type']}")
        """
        incorrect = []

        for idx, reach_id in enumerate(self.reaches.id):
            reach_type = str(reach_id)[-1]

            # Only check ghost reaches
            if reach_type != "6":
                continue

            n_up = self.reaches.n_rch_up[idx]
            n_down = self.reaches.n_rch_down[idx]

            # Ghost reach should NOT have both up and down neighbors
            if n_up > 0 and n_down > 0:
                # Determine suggested type from neighbors
                rch_id_up = self.reaches.rch_id_up[:, idx]
                rch_id_down = self.reaches.rch_id_down[:, idx]

                neighbor_types = []
                for nid in np.concatenate([rch_id_up, rch_id_down]):
                    if nid > 0:
                        ntype = str(nid)[-1]
                        if ntype != "6":  # Don't use other ghost types
                            neighbor_types.append(int(ntype))

                # Suggest the most common neighbor type
                if neighbor_types:
                    suggested = max(set(neighbor_types), key=neighbor_types.count)
                else:
                    suggested = 1  # Default to river type

                incorrect.append(
                    {
                        "reach_id": reach_id,
                        "suggested_type": suggested,
                        "n_rch_up": n_up,
                        "n_rch_down": n_down,
                    }
                )

        return {
            "incorrect_ghost_reaches": incorrect,
            "total_incorrect": len(incorrect),
        }

    # =========================================================================
    # STREAM ORDER AND PATH SEGMENTS RECALCULATION
    # =========================================================================

    def recalculate_stream_order(
        self,
        update_nodes: bool = True,
        update_reaches: bool = True,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Recalculate stream_order from path_freq using the legacy formula.

        Stream order is calculated as: round(ln(path_freq)) + 1
        This provides a log-based stream ordering where higher values indicate
        larger/more connected streams.

        Algorithm (from stream_order.py):
        1. For points with path_freq > 0: stream_order = round(ln(path_freq)) + 1
        2. For points with path_freq <= 0: stream_order = -9999 (nodata)

        Parameters
        ----------
        update_nodes : bool
            If True, update stream_order in nodes table
        update_reaches : bool
            If True, update stream_order in reaches table (mode of node values)
        verbose : bool
            If True, print progress messages

        Returns
        -------
        dict
            Results including:
            - 'nodes_updated': int - Number of nodes updated
            - 'reaches_updated': int - Number of reaches updated
            - 'nodes_with_valid_path_freq': int - Nodes that had valid path_freq
            - 'nodes_missing_path_freq': int - Nodes with invalid/missing path_freq

        Examples
        --------
        >>> result = sword.recalculate_stream_order()
        >>> print(f"Updated {result['nodes_updated']} nodes")
        """
        import math

        if verbose:
            logger.info("Recalculating stream_order from path_freq...")

        conn = self._db.connect()

        # Disable GC to avoid DuckDB segfaults
        import gc

        gc.disable()

        try:
            # Get all nodes with their path_freq
            nodes_data = conn.execute(
                """
                SELECT node_id, path_freq, stream_order
                FROM nodes
                WHERE region = ?
                ORDER BY node_id
            """,
                [self.region],
            ).fetchall()

            if verbose:
                logger.debug(f"Processing {len(nodes_data)} nodes...")

            # Calculate new stream_order values
            nodes_updated = 0
            nodes_valid = 0
            nodes_missing = 0
            node_updates = []

            for node_id, path_freq, old_stream_order in nodes_data:
                if path_freq is not None and path_freq > 0:
                    # Formula: round(ln(path_freq)) + 1
                    new_stream_order = int(round(math.log(path_freq))) + 1
                    nodes_valid += 1
                else:
                    new_stream_order = -9999
                    nodes_missing += 1

                # Only update if changed
                if new_stream_order != old_stream_order:
                    node_updates.append((new_stream_order, node_id))

            # Batch update nodes
            if update_nodes and node_updates:
                if verbose:
                    logger.debug(
                        f"Updating {len(node_updates)} nodes with new stream_order..."
                    )

                conn.execute("BEGIN TRANSACTION")
                for new_val, node_id in node_updates:
                    conn.execute(
                        """
                        UPDATE nodes
                        SET stream_order = ?
                        WHERE node_id = ? AND region = ?
                    """,
                        [new_val, int(node_id), self.region],
                    )
                conn.execute("COMMIT")
                nodes_updated = len(node_updates)

            # Update reaches with mode of node stream_order values
            reaches_updated = 0
            if update_reaches:
                if verbose:
                    logger.debug("Aggregating stream_order to reaches (mode)...")

                # Get reach stream_order as mode of node values
                reach_data = conn.execute(
                    """
                    SELECT
                        r.reach_id,
                        r.stream_order as old_stream_order,
                        (
                            SELECT n.stream_order
                            FROM nodes n
                            WHERE n.reach_id = r.reach_id
                              AND n.region = r.region
                              AND n.stream_order > 0
                            GROUP BY n.stream_order
                            ORDER BY COUNT(*) DESC, n.stream_order DESC
                            LIMIT 1
                        ) as new_stream_order
                    FROM reaches r
                    WHERE r.region = ?
                """,
                    [self.region],
                ).fetchall()

                reach_updates = []
                for reach_id, old_val, new_val in reach_data:
                    if new_val is None:
                        new_val = -9999
                    if new_val != old_val:
                        reach_updates.append((new_val, reach_id))

                if reach_updates:
                    if verbose:
                        logger.debug(f"Updating {len(reach_updates)} reaches...")

                    conn.execute("BEGIN TRANSACTION")
                    for new_val, reach_id in reach_updates:
                        conn.execute(
                            """
                            UPDATE reaches
                            SET stream_order = ?
                            WHERE reach_id = ? AND region = ?
                        """,
                            [new_val, int(reach_id), self.region],
                        )
                    conn.execute("COMMIT")
                    reaches_updated = len(reach_updates)

            if verbose:
                logger.info(
                    f"Done. Nodes: {nodes_updated} updated, Reaches: {reaches_updated} updated"
                )

            return {
                "nodes_updated": nodes_updated,
                "reaches_updated": reaches_updated,
                "nodes_with_valid_path_freq": nodes_valid,
                "nodes_missing_path_freq": nodes_missing,
            }

        finally:
            gc.enable()
            # Don't close the shared connection - let SWORDDatabase manage it

    def recalculate_path_segs(
        self,
        update_nodes: bool = True,
        update_reaches: bool = True,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Recalculate path_segs (path segments) from path_order and path_freq.

        Path segments are unique IDs assigned to river segments between junctions.
        Each unique combination of (path_order, path_freq) gets a unique segment ID.

        Algorithm (from stream_order.py find_path_segs function):
        1. For each unique path_order value (sorted):
           - Find all points with that path_order
           - For each unique path_freq value within those points:
             - Assign a unique segment ID (incrementing counter)

        Parameters
        ----------
        update_nodes : bool
            If True, update path_segs in nodes table
        update_reaches : bool
            If True, update path_segs in reaches table (mode of node values)
        verbose : bool
            If True, print progress messages

        Returns
        -------
        dict
            Results including:
            - 'nodes_updated': int - Number of nodes updated
            - 'reaches_updated': int - Number of reaches updated
            - 'total_segments': int - Total unique segments created
            - 'nodes_assigned': int - Nodes that were assigned a segment

        Examples
        --------
        >>> result = sword.recalculate_path_segs()
        >>> print(f"Created {result['total_segments']} unique segments")
        """
        if verbose:
            logger.info("Recalculating path_segs from path_order and path_freq...")

        conn = self._db.connect()

        # Disable GC to avoid DuckDB segfaults
        import gc

        gc.disable()

        try:
            # Get all nodes with their path variables
            nodes_data = conn.execute(
                """
                SELECT node_id, path_order, path_freq, path_segs
                FROM nodes
                WHERE region = ?
                ORDER BY node_id
            """,
                [self.region],
            ).fetchall()

            if verbose:
                logger.debug(f"Processing {len(nodes_data)} nodes...")

            # Build arrays for vectorized processing
            node_ids = []
            path_orders = []
            path_freqs = []
            old_path_segs = []

            for node_id, path_order, path_freq, old_seg in nodes_data:
                node_ids.append(node_id)
                path_orders.append(path_order if path_order is not None else 0)
                path_freqs.append(path_freq if path_freq is not None else 0)
                old_path_segs.append(old_seg if old_seg is not None else 0)

            import numpy as np

            node_ids = np.array(node_ids)
            path_orders = np.array(path_orders)
            path_freqs = np.array(path_freqs)
            old_path_segs = np.array(old_path_segs)

            # Calculate path_segs using legacy algorithm
            new_path_segs = np.zeros(len(node_ids), dtype=np.int64)

            # Get unique path_order values > 0
            unq_orders = np.unique(path_orders)
            unq_orders = unq_orders[unq_orders > 0]

            cnt = 1
            nodes_assigned = 0

            for path_ord in unq_orders:
                # Find all nodes with this path_order
                pth_idx = np.where(path_orders == path_ord)[0]

                # Get unique path_freq values within these nodes
                sections = np.unique(path_freqs[pth_idx])

                for section_freq in sections:
                    # Find nodes with this (path_order, path_freq) combination
                    sec_idx = np.where(path_freqs[pth_idx] == section_freq)[0]
                    new_path_segs[pth_idx[sec_idx]] = cnt
                    nodes_assigned += len(sec_idx)
                    cnt += 1

            total_segments = cnt - 1

            if verbose:
                logger.debug(f"Created {total_segments} unique segments")

            # Find nodes that need updating
            changed_mask = new_path_segs != old_path_segs
            node_updates = list(
                zip(new_path_segs[changed_mask], node_ids[changed_mask])
            )

            # Batch update nodes
            nodes_updated = 0
            if update_nodes and node_updates:
                if verbose:
                    logger.debug(
                        f"Updating {len(node_updates)} nodes with new path_segs..."
                    )

                conn.execute("BEGIN TRANSACTION")
                for new_val, node_id in node_updates:
                    conn.execute(
                        """
                        UPDATE nodes
                        SET path_segs = ?
                        WHERE node_id = ? AND region = ?
                    """,
                        [int(new_val), int(node_id), self.region],
                    )
                conn.execute("COMMIT")
                nodes_updated = len(node_updates)

            # Update reaches with mode of node path_segs values
            reaches_updated = 0
            if update_reaches:
                if verbose:
                    logger.debug("Aggregating path_segs to reaches (mode)...")

                # Get reach path_segs as mode of node values
                reach_data = conn.execute(
                    """
                    SELECT
                        r.reach_id,
                        r.path_segs as old_path_segs,
                        (
                            SELECT n.path_segs
                            FROM nodes n
                            WHERE n.reach_id = r.reach_id
                              AND n.region = r.region
                              AND n.path_segs > 0
                            GROUP BY n.path_segs
                            ORDER BY COUNT(*) DESC, n.path_segs DESC
                            LIMIT 1
                        ) as new_path_segs
                    FROM reaches r
                    WHERE r.region = ?
                """,
                    [self.region],
                ).fetchall()

                reach_updates = []
                for reach_id, old_val, new_val in reach_data:
                    if new_val is None:
                        new_val = 0
                    if new_val != old_val:
                        reach_updates.append((new_val, reach_id))

                if reach_updates:
                    if verbose:
                        logger.debug(f"Updating {len(reach_updates)} reaches...")

                    conn.execute("BEGIN TRANSACTION")
                    for new_val, reach_id in reach_updates:
                        conn.execute(
                            """
                            UPDATE reaches
                            SET path_segs = ?
                            WHERE reach_id = ? AND region = ?
                        """,
                            [int(new_val), int(reach_id), self.region],
                        )
                    conn.execute("COMMIT")
                    reaches_updated = len(reach_updates)

            if verbose:
                logger.info(
                    f"Done. Nodes: {nodes_updated} updated, Reaches: {reaches_updated} updated"
                )

            return {
                "nodes_updated": nodes_updated,
                "reaches_updated": reaches_updated,
                "total_segments": total_segments,
                "nodes_assigned": nodes_assigned,
            }

        finally:
            gc.enable()
            # Don't close the shared connection - let SWORDDatabase manage it

    # =========================================================================
    # SINUOSITY RECALCULATION
    # =========================================================================

    def recalculate_sinuosity(
        self,
        reach_ids: Optional[List[int]] = None,
        update_database: bool = True,
        min_reach_len_factor: float = 1.0,
        smoothing_span: int = 5,
        verbose: bool = True,
    ) -> Dict[str, Any]:
        """
        Recalculate sinuosity from centerline geometry using the legacy MATLAB algorithm.

        Sinuosity is calculated by:
        1. Projecting centerline coordinates to UTM for accurate distance measurements
        2. Smoothing coordinates with a moving average to remove pixel-level noise
        3. Detecting inflection points (where river curvature changes direction)
        4. Merging short reaches based on similarity to neighbors
        5. Computing sinuosity as arc_length / straight_line_distance for each bend

        The algorithm is based on the legacy MATLAB code from:
        - SinuosityMinAreaVarMinReach.m
        - MergeShortReachesVarMin.m

        Parameters
        ----------
        reach_ids : list of int, optional
            Specific reaches to recalculate. If None, recalculates all reaches.
        update_database : bool
            If True, update sinuosity values in the database.
        min_reach_len_factor : float
            Multiplier for minimum reach length (based on width). Default 1.0.
        smoothing_span : int
            Number of points for moving average smoothing. Default 5.
        verbose : bool
            If True, print progress messages.

        Returns
        -------
        dict
            Results including:
            - 'reaches_processed': int - Number of reaches processed
            - 'reaches_updated': int - Number of reaches with changed sinuosity
            - 'mean_sinuosity': float - Mean sinuosity across all processed reaches
            - 'reach_sinuosities': dict - {reach_id: sinuosity} for each reach

        Notes
        -----
        Sinuosity values:
        - 1.0 = perfectly straight
        - 1.0-1.5 = nearly straight
        - 1.5-2.0 = sinuous
        - >2.0 = highly sinuous/meandering

        Examples
        --------
        >>> result = sword.recalculate_sinuosity()
        >>> print(f"Mean sinuosity: {result['mean_sinuosity']:.2f}")

        >>> # Recalculate specific reaches
        >>> result = sword.recalculate_sinuosity(reach_ids=[123, 456])
        """
        import numpy as np

        try:
            from pyproj import CRS, Transformer  # noqa: F401
        except ImportError:
            raise ImportError(
                "pyproj is required for sinuosity calculation. Install with: pip install pyproj"
            )

        if verbose:
            logger.info("Recalculating sinuosity from centerline geometry...")

        conn = self._db.connect()

        # Disable GC to avoid DuckDB segfaults
        gc.disable()

        try:
            # Check if sinuosity column exists, create if not
            columns = [
                col[0]
                for col in conn.execute("SELECT * FROM reaches LIMIT 1").description
            ]

            if "sinuosity" not in columns:
                if verbose:
                    logger.debug("Adding sinuosity column to reaches table...")
                conn.execute("ALTER TABLE reaches ADD COLUMN sinuosity DOUBLE")

            # Get reach IDs to process
            if reach_ids is None:
                reach_data = conn.execute(
                    """
                    SELECT reach_id, sinuosity
                    FROM reaches
                    WHERE region = ?
                    ORDER BY reach_id
                """,
                    [self.region],
                ).fetchall()
                reach_ids_to_process = [r[0] for r in reach_data]
                old_sinuosities = {r[0]: r[1] for r in reach_data}
            else:
                reach_ids_to_process = list(reach_ids)
                reach_data = conn.execute(
                    """
                    SELECT reach_id, sinuosity
                    FROM reaches
                    WHERE region = ? AND reach_id IN ({})
                    ORDER BY reach_id
                """.format(",".join("?" * len(reach_ids))),
                    [self.region] + reach_ids_to_process,
                ).fetchall()
                old_sinuosities = {r[0]: r[1] for r in reach_data}

            if verbose:
                logger.debug(f"Processing {len(reach_ids_to_process)} reaches...")

            # Calculate sinuosity for each reach
            reach_sinuosities = {}
            processed = 0
            skipped = 0

            for reach_id in reach_ids_to_process:
                # Get centerlines for this reach (x, y only - width from reaches)
                centerlines = conn.execute(
                    """
                    SELECT x, y
                    FROM centerlines
                    WHERE reach_id = ? AND region = ?
                    ORDER BY cl_id
                """,
                    [reach_id, self.region],
                ).fetchall()

                if len(centerlines) < 3:
                    # Too few points - set sinuosity to 1.0 (straight)
                    reach_sinuosities[reach_id] = 1.0
                    skipped += 1
                    continue

                # Get reach width (use same width for all centerlines in reach)
                reach_width = conn.execute(
                    """
                    SELECT width FROM reaches WHERE reach_id = ? AND region = ?
                """,
                    [reach_id, self.region],
                ).fetchone()
                reach_width = (
                    reach_width[0] if reach_width and reach_width[0] else 100.0
                )

                lons = np.array([c[0] for c in centerlines])
                lats = np.array([c[1] for c in centerlines])
                # Use constant width for all points in reach
                widths = np.full(
                    len(centerlines), reach_width if reach_width > 0 else 100.0
                )

                # Calculate sinuosity using the algorithm
                sinuosity = self._calculate_reach_sinuosity(
                    lats,
                    lons,
                    widths,
                    smoothing_span=smoothing_span,
                    min_reach_len_factor=min_reach_len_factor,
                )

                reach_sinuosities[reach_id] = sinuosity
                processed += 1

                if verbose and processed % 1000 == 0:
                    logger.debug(
                        f"Processed {processed}/{len(reach_ids_to_process)} reaches..."
                    )

            if verbose:
                logger.debug(
                    f"Processed {processed} reaches, skipped {skipped} (too few points)"
                )

            # Update database
            reaches_updated = 0
            if update_database:
                reach_updates = []
                for reach_id, new_sin in reach_sinuosities.items():
                    old_sin = old_sinuosities.get(reach_id)
                    # Compare with tolerance for floating point
                    if old_sin is None or abs(new_sin - old_sin) > 0.001:
                        reach_updates.append((new_sin, reach_id))

                if reach_updates:
                    if verbose:
                        logger.debug(
                            f"Updating {len(reach_updates)} reaches in database..."
                        )

                    conn.execute("BEGIN TRANSACTION")
                    for new_val, reach_id in reach_updates:
                        conn.execute(
                            """
                            UPDATE reaches
                            SET sinuosity = ?
                            WHERE reach_id = ? AND region = ?
                        """,
                            [float(new_val), int(reach_id), self.region],
                        )
                    conn.execute("COMMIT")
                    reaches_updated = len(reach_updates)

            # Calculate statistics
            sinuosity_values = list(reach_sinuosities.values())
            mean_sinuosity = np.mean(sinuosity_values) if sinuosity_values else 0.0

            if verbose:
                logger.info(f"Done. Updated {reaches_updated} reaches.")
                logger.info(f"Mean sinuosity: {mean_sinuosity:.3f}")

            return {
                "reaches_processed": processed,
                "reaches_skipped": skipped,
                "reaches_updated": reaches_updated,
                "mean_sinuosity": float(mean_sinuosity),
                "reach_sinuosities": reach_sinuosities,
            }

        finally:
            gc.enable()
            # Don't close the shared connection - let SWORDDatabase manage it

    def _calculate_reach_sinuosity(
        self,
        lats: np.ndarray,
        lons: np.ndarray,
        widths: np.ndarray,
        smoothing_span: int = 5,
        min_reach_len_factor: float = 1.0,
    ) -> float:
        """
        Calculate sinuosity for a single reach from its centerline coordinates.

        This implements the core algorithm from SinuosityMinAreaVarMinReach.m.

        Parameters
        ----------
        lats : np.ndarray
            Latitude coordinates of centerline points
        lons : np.ndarray
            Longitude coordinates of centerline points
        widths : np.ndarray
            River widths at each point (meters)
        smoothing_span : int
            Moving average window size for smoothing
        min_reach_len_factor : float
            Multiplier for minimum reach length constraint

        Returns
        -------
        float
            Calculated sinuosity (arc_length / straight_line_distance)
        """
        import numpy as np
        from pyproj import CRS, Transformer

        n = len(lats)
        if n < 3:
            return 1.0

        # Determine UTM zone from centroid
        center_lon = np.mean(lons)
        center_lat = np.mean(lats)
        utm_zone = int((center_lon + 180) / 6) + 1
        is_north = center_lat >= 0

        # Create transformer to UTM
        utm_crs = CRS.from_proj4(
            f"+proj=utm +zone={utm_zone} +{'north' if is_north else 'south'} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
        )
        wgs84_crs = CRS.from_epsg(4326)
        transformer = Transformer.from_crs(wgs84_crs, utm_crs, always_xy=True)

        # Project to UTM
        X, Y = transformer.transform(lons, lats)
        X = np.array(X)
        Y = np.array(Y)

        # Apply moving average smoothing to remove pixel-level noise
        X = self._moving_average(X, smoothing_span)
        Y = self._moving_average(Y, smoothing_span)

        # Calculate cumulative distance along centerline
        D = np.zeros(n)
        for i in range(1, n):
            D[i] = D[i - 1] + np.sqrt((X[i] - X[i - 1]) ** 2 + (Y[i] - Y[i - 1]) ** 2)

        total_length = D[-1] - D[0]
        if total_length < 1.0:  # Less than 1 meter
            return 1.0

        # For short reaches, use simple sinuosity
        if n <= 20:
            # Simple sinuosity: total arc length / straight line distance
            straight_dist = np.sqrt((X[-1] - X[0]) ** 2 + (Y[-1] - Y[0]) ** 2)
            if straight_dist < 1.0:
                return 1.0
            return total_length / straight_dist

        # Find inflection points (where curvature changes direction)
        # Using cross product of adjacent segment vectors
        Dx = np.diff(X)
        Dy = np.diff(Y)

        # Cross product: Dx[i-1]*Dy[i] - Dx[i]*Dy[i-1]
        Product = np.zeros(n)
        for i in range(1, n - 1):
            Product[i] = Dx[i - 1] * Dy[i] - Dx[i] * Dy[i - 1]

        # Extend to endpoints
        Product[0] = Product[1] if n > 1 else 0
        Product[-1] = Product[-2] if n > 1 else 0

        # Expand to look at larger-scale curvature (like MATLAB code)
        for count in range(1, n - 1):
            Base = np.sqrt(
                (X[min(count + 1, n - 1)] - X[max(count - 1, 0)]) ** 2
                + (Y[min(count + 1, n - 1)] - Y[max(count - 1, 0)]) ** 2
            )
            if Base > 0:
                height = abs(Product[count] / (2 * Base))
            else:
                height = 0

            width = 4
            while height < 30 and width < 30:
                i1 = max(0, count - width // 2)
                i2 = min(n - 1, count + width // 2)

                x1, y1 = X[i1], Y[i1]
                x2, y2 = X[i2], Y[i2]

                Dxup = X[count] - x1
                Dxdo = x2 - X[count]
                Dyup = Y[count] - y1
                Dydo = y2 - Y[count]

                prod = Dxup * Dydo - Dxdo * Dyup
                Base = np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
                if Base > 0:
                    height = abs(prod / (2 * Base))
                else:
                    height = 0

                Product[count] = prod
                width += 2

        # Find inflection points (sign changes in Product)
        bounds = [0]
        for i in range(1, n - 1):
            if Product[i] * Product[i + 1] < 0:
                bounds.append(i + 1)
        bounds.append(n - 1)

        # Merge short reaches
        bounds = self._merge_short_reaches(
            bounds, D, Product, widths, min_reach_len_factor
        )

        # Remove boundaries where curvature no longer changes sign after merging
        if len(bounds) > 2:
            bounds = self._remove_invalid_boundaries(bounds, Product)

        # Calculate sinuosity for each bend and average
        if len(bounds) < 2:
            straight_dist = np.sqrt((X[-1] - X[0]) ** 2 + (Y[-1] - Y[0]) ** 2)
            if straight_dist < 1.0:
                return 1.0
            return total_length / straight_dist

        # Calculate sinuosity as weighted average of bend sinuosities
        total_arc = 0.0
        total_straight = 0.0

        for i in range(len(bounds) - 1):
            i1, i2 = bounds[i], bounds[i + 1]
            arc_length = D[i2] - D[i1]
            straight_dist = np.sqrt((X[i1] - X[i2]) ** 2 + (Y[i1] - Y[i2]) ** 2)

            if straight_dist > 0:
                total_arc += arc_length
                total_straight += straight_dist

        if total_straight < 1.0:
            return 1.0

        sinuosity = total_arc / total_straight

        # Clamp to reasonable range
        return max(1.0, min(sinuosity, 10.0))

    def _moving_average(self, x: np.ndarray, span: int = 5) -> np.ndarray:
        """
        Apply moving average smoothing.

        Parameters
        ----------
        x : np.ndarray
            Input array
        span : int
            Window size (will be centered)

        Returns
        -------
        np.ndarray
            Smoothed array
        """
        import numpy as np

        if len(x) < span:
            return x

        # Use cumsum for efficient moving average
        pad = span // 2
        x_padded = np.pad(x, (pad, pad), mode="edge")
        cumsum = np.cumsum(x_padded)
        result = (cumsum[span:] - cumsum[:-span]) / span

        # Ensure same length
        if len(result) > len(x):
            result = result[: len(x)]
        elif len(result) < len(x):
            result = np.pad(result, (0, len(x) - len(result)), mode="edge")

        return result

    def _merge_short_reaches(
        self,
        bounds: List[int],
        D: np.ndarray,
        Product: np.ndarray,
        widths: np.ndarray,
        min_factor: float = 1.0,
    ) -> List[int]:
        """
        Merge short reaches by identifying which neighbor is more similar.

        This implements MergeShortReachesVarMin.m algorithm.

        Parameters
        ----------
        bounds : list of int
            Reach boundary indices
        D : np.ndarray
            Cumulative distance array
        Product : np.ndarray
            Cross product (concavity) array
        widths : np.ndarray
            River widths
        min_factor : float
            Multiplier for minimum reach length

        Returns
        -------
        list of int
            Updated boundary indices
        """
        import numpy as np

        bounds = list(bounds)
        if len(bounds) <= 2:
            return bounds

        max_iterations = len(bounds) * 2  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            if len(bounds) <= 2:
                break

            # Calculate reach lengths
            reach_lengths = []
            for i in range(len(bounds) - 1):
                reach_lengths.append(D[bounds[i + 1]] - D[bounds[i]])

            # Calculate minimum lengths based on width
            min_lengths = []
            for i in range(len(bounds) - 1):
                avg_width = np.mean(widths[bounds[i] : bounds[i + 1] + 1])
                total_len = D[-1] - D[0]
                min_len = min(avg_width * min_factor, total_len / 2)
                min_len = max(min_len, 100)  # Minimum 100m
                min_lengths.append(min_len)

            # Find shortest reach
            min_length = min(reach_lengths)
            min_idx = reach_lengths.index(min_length)

            # Check if shortest is long enough
            if min_length >= min_lengths[min_idx]:
                break

            # Merge with similar neighbor
            if min_idx == 0:
                # First reach - merge with second
                bounds.pop(1)
            elif min_idx == len(reach_lengths) - 1:
                # Last reach - merge with previous
                bounds.pop(-2)
            else:
                # Middle reach - merge with more similar neighbor
                curr_concav = np.mean(Product[bounds[min_idx] : bounds[min_idx + 1]])
                up_concav = np.mean(Product[bounds[min_idx - 1] : bounds[min_idx]])
                down_concav = np.mean(
                    Product[bounds[min_idx + 1] : bounds[min_idx + 2]]
                )

                if abs(down_concav - curr_concav) < abs(up_concav - curr_concav):
                    # More similar to downstream - remove boundary between current and downstream
                    bounds.pop(min_idx + 1)
                else:
                    # More similar to upstream - remove boundary between upstream and current
                    bounds.pop(min_idx)

        return bounds

    def _remove_invalid_boundaries(
        self, bounds: List[int], Product: np.ndarray
    ) -> List[int]:
        """
        Remove boundaries where curvature no longer changes sign after merging.

        Parameters
        ----------
        bounds : list of int
            Reach boundary indices
        Product : np.ndarray
            Cross product (concavity) array

        Returns
        -------
        list of int
            Updated boundary indices
        """
        import numpy as np

        if len(bounds) <= 2:
            return bounds

        bounds = list(bounds)

        # Recalculate average curvature for each reach
        avg_products = []
        for i in range(len(bounds) - 1):
            avg_products.append(np.mean(Product[bounds[i] : bounds[i + 1]]))

        # Find boundaries to remove (where adjacent reaches have same-sign curvature)
        to_remove = []
        for i in range(len(avg_products) - 1):
            if avg_products[i] * avg_products[i + 1] > 0:
                to_remove.append(i + 1)  # Index in bounds to remove

        # Remove from end to preserve indices
        for idx in reversed(to_remove):
            if 0 < idx < len(bounds) - 1:  # Don't remove first or last
                bounds.pop(idx)

        return bounds

    def close(self) -> None:
        """Close the database connection."""
        self._db.close()

    def __enter__(self) -> "SWORD":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
