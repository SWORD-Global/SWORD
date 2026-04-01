# -*- coding: utf-8 -*-
"""
SWORD DuckDB View Classes
=========================

Wrapper classes that provide numpy-array-style attribute access to DataFrame columns.
These classes enable backward compatibility with the original SWORD class interface.

The original SWORD class uses attribute access like:
    sword.reaches.wse  -> numpy array
    sword.nodes.facc   -> numpy array

These view classes wrap DataFrames and provide the same interface.
WritableArray enables in-place array modifications to persist to the database.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Any
import numpy as np
import gc

if TYPE_CHECKING:
    import pandas as pd
    from .reactive import SWORDReactive
    from .provenance import ProvenanceLogger


class WritableArray:
    """
    Numpy array wrapper that syncs modifications to DuckDB.

    Intercepts __setitem__ calls and updates both the local numpy array
    and the database. Supports scalar and array index assignments.

    Examples
    --------
    >>> arr = sword.reaches.dist_out  # Returns WritableArray
    >>> arr[0] = 1234.5               # Updates index 0 in DB
    >>> arr[[1,2,3]] = [5,6,7]        # Updates indices 1,2,3 in DB
    """

    def __init__(
        self,
        data: np.ndarray,
        db,
        table: str,
        id_col: str,
        col_name: str,
        db_col_name: str,
        ids: np.ndarray,
        region: str,
        reactive: Optional["SWORDReactive"] = None,
        attr_name: Optional[str] = None,
        provenance_logger: Optional["ProvenanceLogger"] = None,
        operation_id: Optional[int] = None,
    ):
        """
        Initialize WritableArray.

        Parameters
        ----------
        data : np.ndarray
            The underlying numpy array data.
        db : SWORDDatabase
            Database connection for persisting changes.
        table : str
            Database table name (centerlines, nodes, reaches).
        id_col : str
            ID column name in the database (cl_id, node_id, reach_id).
        col_name : str
            Property name (for error messages).
        db_col_name : str
            Actual database column name (may differ from property name).
        ids : np.ndarray
            Array of IDs corresponding to each row (for lookups).
        region : str
            Region code for WHERE clause.
        reactive : SWORDReactive, optional
            Reactive system instance for automatic change tracking.
            When provided, modifications trigger mark_dirty() calls.
        attr_name : str, optional
            Attribute name for the reactive system (e.g., 'reach.dist_out').
            Required if reactive is provided.
        provenance_logger : ProvenanceLogger, optional
            Provenance logger for tracking value changes.
            When provided, modifications are logged for rollback capability.
        operation_id : int, optional
            Active operation ID for provenance logging.
            Required if provenance_logger is provided.
        """
        self._data = data
        self._db = db
        self._table = table
        self._id_col = id_col
        self._col_name = col_name
        self._db_col_name = db_col_name
        self._ids = ids
        self._region = region
        self._reactive = reactive
        self._attr_name = attr_name
        self._provenance_logger = provenance_logger
        self._operation_id = operation_id

    def __getitem__(self, key) -> Any:
        """Get items from underlying array."""
        return self._data[key]

    def __setitem__(self, key, value) -> None:
        """Set items and sync to database."""
        # Capture old values for provenance logging
        old_values = (
            self._data[key].copy()
            if hasattr(self._data[key], "copy")
            else self._data[key]
        )

        # Update local array
        self._data[key] = value

        # Determine affected IDs
        affected_ids = self._ids[key]

        # Disable GC during database operations to avoid DuckDB issues
        gc_was_enabled = gc.isenabled()
        gc.disable()

        try:
            # Use db.execute() for backend-agnostic SQL execution
            # Placeholders (?) are automatically converted for PostgreSQL
            if np.isscalar(affected_ids) or (
                isinstance(affected_ids, np.ndarray) and affected_ids.ndim == 0
            ):
                # Single value update
                id_val = (
                    int(affected_ids)
                    if hasattr(affected_ids, "item")
                    else int(affected_ids)
                )
                val = value.item() if hasattr(value, "item") else value
                old_val = (
                    old_values.item() if hasattr(old_values, "item") else old_values
                )

                self._db.execute(
                    f"""
                    UPDATE {self._table}
                    SET {self._db_col_name} = ?
                    WHERE {self._id_col} = ? AND region = ?
                """,
                    [val, id_val, self._region],
                )

                # Log to provenance system if configured
                if self._provenance_logger and self._operation_id:
                    self._provenance_logger.log_value_change(
                        self._operation_id,
                        self._table,
                        id_val,
                        self._db_col_name,
                        old_val,
                        val,
                    )

                # Hook into reactive system if configured
                if self._reactive and self._attr_name:
                    self._mark_dirty_for_ids([id_val])
            else:
                # Multiple value update
                affected_ids = np.atleast_1d(affected_ids)
                values = np.atleast_1d(value)
                old_vals = np.atleast_1d(old_values)

                # Handle broadcasting if single value assigned to multiple indices
                if len(values) == 1 and len(affected_ids) > 1:
                    values = np.repeat(values, len(affected_ids))

                for i, (id_val, val) in enumerate(zip(affected_ids, values)):
                    id_val = int(id_val) if hasattr(id_val, "item") else int(id_val)
                    val = val.item() if hasattr(val, "item") else val
                    old_val = (
                        old_vals[i].item()
                        if hasattr(old_vals[i], "item")
                        else old_vals[i]
                    )

                    self._db.execute(
                        f"""
                        UPDATE {self._table}
                        SET {self._db_col_name} = ?
                        WHERE {self._id_col} = ? AND region = ?
                    """,
                        [val, id_val, self._region],
                    )

                    # Log to provenance system if configured
                    if self._provenance_logger and self._operation_id:
                        self._provenance_logger.log_value_change(
                            self._operation_id,
                            self._table,
                            id_val,
                            self._db_col_name,
                            old_val,
                            val,
                        )

                # Hook into reactive system if configured
                if self._reactive and self._attr_name:
                    self._mark_dirty_for_ids([int(i) for i in affected_ids])
        finally:
            if gc_was_enabled:
                gc.enable()

    def _mark_dirty_for_ids(self, entity_ids: list) -> None:
        """
        Mark the reactive system as dirty for the given entity IDs.

        Maps table names to the appropriate dirty set parameter.
        """
        if not self._reactive or not self._attr_name:
            return

        # Import ChangeType here to avoid circular imports
        from .reactive import ChangeType

        # Determine change type based on attribute name
        if "geometry" in self._attr_name or self._col_name in ("x", "y"):
            change_type = ChangeType.GEOMETRY
        elif "topology" in self._attr_name:
            change_type = ChangeType.TOPOLOGY
        else:
            change_type = ChangeType.ATTRIBUTE

        # Map table to the appropriate parameter
        if self._table == "reaches":
            self._reactive.mark_dirty(
                self._attr_name, change_type, reach_ids=entity_ids
            )
        elif self._table == "nodes":
            self._reactive.mark_dirty(self._attr_name, change_type, node_ids=entity_ids)
        elif self._table == "centerlines":
            self._reactive.mark_dirty(self._attr_name, change_type, cl_ids=entity_ids)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"WritableArray({self._col_name}, shape={self._data.shape})"

    def __array__(self, dtype=None) -> np.ndarray:
        """Support numpy array conversion."""
        if dtype:
            return self._data.astype(dtype)
        return self._data

    # Forward common numpy array attributes/methods
    @property
    def shape(self) -> tuple:
        return self._data.shape

    @property
    def dtype(self) -> np.dtype:
        return self._data.dtype

    @property
    def ndim(self) -> int:
        return self._data.ndim

    def __iter__(self):
        return iter(self._data)

    def __eq__(self, other) -> np.ndarray:
        return self._data == other

    def __ne__(self, other) -> np.ndarray:
        return self._data != other

    def __lt__(self, other) -> np.ndarray:
        return self._data < other

    def __le__(self, other) -> np.ndarray:
        return self._data <= other

    def __gt__(self, other) -> np.ndarray:
        return self._data > other

    def __ge__(self, other) -> np.ndarray:
        return self._data >= other

    def __add__(self, other) -> np.ndarray:
        return self._data + other

    def __radd__(self, other) -> np.ndarray:
        return other + self._data

    def __sub__(self, other) -> np.ndarray:
        return self._data - other

    def __rsub__(self, other) -> np.ndarray:
        return other - self._data

    def __mul__(self, other) -> np.ndarray:
        return self._data * other

    def __rmul__(self, other) -> np.ndarray:
        return other * self._data

    def __truediv__(self, other) -> np.ndarray:
        return self._data / other

    def __floordiv__(self, other) -> np.ndarray:
        return self._data // other

    def copy(self) -> np.ndarray:
        return self._data.copy()

    def astype(self, dtype) -> np.ndarray:
        return self._data.astype(dtype)


class CenterlinesView:
    """
    Wrapper for centerlines data providing numpy-array-style attribute access.

    Maps original SWORD attribute names to DuckDB column names.
    Handles [4,N] arrays for reach_id and node_id by reconstruction.
    Returns WritableArray for attributes that support modification.

    Original attributes:
        cl_id: [N] centerline IDs
        x: [N] longitude coordinates
        y: [N] latitude coordinates
        reach_id: [4,N] reach IDs (primary + 3 neighbors)
        node_id: [4,N] node IDs (primary + 3 neighbors)
    """

    def __init__(
        self,
        df: "pd.DataFrame",
        reach_id_array: np.ndarray,
        node_id_array: np.ndarray,
        db=None,
        region: str = None,
        reactive: Optional["SWORDReactive"] = None,
    ):
        """
        Initialize CenterlinesView.

        Parameters
        ----------
        df : pd.DataFrame
            Centerlines data from DuckDB query.
        reach_id_array : np.ndarray
            Reconstructed [4,N] reach_id array.
        node_id_array : np.ndarray
            Reconstructed [4,N] node_id array.
        db : SWORDDatabase, optional
            Database connection for write operations.
        region : str, optional
            Region code for write operations.
        reactive : SWORDReactive, optional
            Reactive system instance for automatic change tracking.
        """
        self._df = df
        self._reach_id = reach_id_array
        self._node_id = node_id_array
        self._db = db
        self._region = region
        self._ids = df["cl_id"].values
        self._reactive = reactive

    def _writable(self, col_name: str, db_col_name: str = None) -> WritableArray:
        """Create a WritableArray for the given column."""
        if db_col_name is None:
            db_col_name = col_name
        # Determine reactive attribute name based on column
        attr_name = f"centerline.{col_name}"
        if col_name in ("x", "y"):
            attr_name = "centerline.geometry"  # x,y changes are geometry changes
        return WritableArray(
            self._df[db_col_name].values,
            self._db,
            "centerlines",
            "cl_id",
            col_name,
            db_col_name,
            self._ids,
            self._region,
            reactive=self._reactive,
            attr_name=attr_name,
        )

    @property
    def cl_id(self) -> np.ndarray:
        """Centerline IDs [N] - read only."""
        return self._df["cl_id"].values

    @property
    def x(self) -> WritableArray:
        """Longitude coordinates [N]."""
        return self._writable("x")

    @property
    def y(self) -> WritableArray:
        """Latitude coordinates [N]."""
        return self._writable("y")

    @property
    def reach_id(self) -> np.ndarray:
        """Reach IDs [4,N] - primary + 3 neighbors."""
        return self._reach_id

    @reach_id.setter
    def reach_id(self, value: np.ndarray) -> None:
        """Set reach_id array."""
        self._reach_id = value

    @property
    def node_id(self) -> np.ndarray:
        """Node IDs [4,N] - primary + 3 neighbors."""
        return self._node_id

    @node_id.setter
    def node_id(self, value: np.ndarray) -> None:
        """Set node_id array."""
        self._node_id = value

    def __len__(self) -> int:
        return len(self._df)


class NodesView:
    """
    Wrapper for nodes data providing numpy-array-style attribute access.

    Maps original SWORD attribute names to DuckDB column names.
    Returns WritableArray for attributes that support modification.

    Original -> DuckDB mapping:
        id -> node_id
        len -> node_length
        wth -> width
        wth_var -> width_var
        max_wth -> max_width
        grod -> obstr_type
        grod_fid -> grod_id
        hfalls_fid -> hfalls_id
        nchan_max -> n_chan_max
        nchan_mod -> n_chan_mod
        meand_len -> meander_length
        strm_order -> stream_order
        end_rch -> end_reach
        cl_id[2,N] -> (cl_id_min, cl_id_max)
    """

    def __init__(
        self,
        df: "pd.DataFrame",
        db=None,
        region: str = None,
        reactive: Optional["SWORDReactive"] = None,
    ):
        """
        Initialize NodesView.

        Parameters
        ----------
        df : pd.DataFrame
            Nodes data from DuckDB query.
        db : SWORDDatabase, optional
            Database connection for write operations.
        region : str, optional
            Region code for write operations.
        reactive : SWORDReactive, optional
            Reactive system instance for automatic change tracking.
        """
        self._df = df
        self._db = db
        self._region = region
        self._ids = df["node_id"].values
        self._reactive = reactive
        # Build cl_id [2,N] array from min/max columns
        self._cl_id = np.vstack([df["cl_id_min"].values, df["cl_id_max"].values])

    def _writable(self, col_name: str, db_col_name: str = None) -> WritableArray:
        """Create a WritableArray for the given column."""
        if db_col_name is None:
            db_col_name = col_name
        # Determine reactive attribute name
        attr_name = f"node.{col_name}"
        if col_name in ("x", "y"):
            attr_name = "node.xy"
        elif col_name == "len":
            attr_name = "node.len"
        elif col_name == "dist_out":
            attr_name = "node.dist_out"
        elif col_name == "end_rch":
            attr_name = "node.end_rch"
        elif col_name == "main_side":
            attr_name = "node.main_side"
        return WritableArray(
            self._df[db_col_name].values,
            self._db,
            "nodes",
            "node_id",
            col_name,
            db_col_name,
            self._ids,
            self._region,
            reactive=self._reactive,
            attr_name=attr_name,
        )

    # Direct mappings (same name) - now writable
    @property
    def x(self) -> WritableArray:
        return self._writable("x")

    @property
    def y(self) -> WritableArray:
        return self._writable("y")

    @property
    def wse(self) -> WritableArray:
        return self._writable("wse")

    @property
    def wse_var(self) -> WritableArray:
        return self._writable("wse_var")

    @property
    def facc(self) -> WritableArray:
        return self._writable("facc")

    @property
    def dist_out(self) -> WritableArray:
        return self._writable("dist_out")

    @property
    def lakeflag(self) -> WritableArray:
        return self._writable("lakeflag")

    @property
    def wth_coef(self) -> WritableArray:
        return self._writable("wth_coef")

    @property
    def ext_dist_coef(self) -> WritableArray:
        return self._writable("ext_dist_coef")

    @property
    def sinuosity(self) -> WritableArray:
        return self._writable("sinuosity")

    @property
    def river_name(self) -> WritableArray:
        return self._writable("river_name")

    @property
    def manual_add(self) -> WritableArray:
        return self._writable("manual_add")

    @property
    def edit_flag(self) -> WritableArray:
        return self._writable("edit_flag")

    @property
    def trib_flag(self) -> WritableArray:
        return self._writable("trib_flag")

    @property
    def path_freq(self) -> WritableArray:
        return self._writable("path_freq")

    @property
    def path_order(self) -> WritableArray:
        return self._writable("path_order")

    @property
    def path_segs(self) -> WritableArray:
        return self._writable("path_segs")

    @property
    def main_side(self) -> WritableArray:
        return self._writable("main_side")

    @property
    def network(self) -> WritableArray:
        return self._writable("network")

    @property
    def node_order(self) -> np.ndarray:
        """Node order (1-based position within reach, 1=downstream). Read-only."""
        return self._df["node_order"].values

    @property
    def add_flag(self) -> WritableArray:
        return self._writable("add_flag")

    # Renamed mappings - writable
    @property
    def id(self) -> np.ndarray:
        """Node IDs [N] (DuckDB: node_id) - read only."""
        return self._df["node_id"].values

    @property
    def len(self) -> WritableArray:
        """Node length [N] (DuckDB: node_length)."""
        return self._writable("len", "node_length")

    @property
    def wth(self) -> WritableArray:
        """Width [N] (DuckDB: width)."""
        return self._writable("wth", "width")

    @property
    def wth_var(self) -> WritableArray:
        """Width variance [N] (DuckDB: width_var)."""
        return self._writable("wth_var", "width_var")

    @property
    def max_wth(self) -> WritableArray:
        """Max width [N] (DuckDB: max_width)."""
        return self._writable("max_wth", "max_width")

    @property
    def grod(self) -> WritableArray:
        """Obstruction type [N] (DuckDB: obstr_type)."""
        return self._writable("grod", "obstr_type")

    @property
    def grod_fid(self) -> WritableArray:
        """GROD feature ID [N] (DuckDB: grod_id)."""
        return self._writable("grod_fid", "grod_id")

    @property
    def hfalls_fid(self) -> WritableArray:
        """HydroFALLS feature ID [N] (DuckDB: hfalls_id)."""
        return self._writable("hfalls_fid", "hfalls_id")

    @property
    def nchan_max(self) -> WritableArray:
        """Max channels [N] (DuckDB: n_chan_max)."""
        return self._writable("nchan_max", "n_chan_max")

    @property
    def nchan_mod(self) -> WritableArray:
        """Modal channels [N] (DuckDB: n_chan_mod)."""
        return self._writable("nchan_mod", "n_chan_mod")

    @property
    def meand_len(self) -> WritableArray:
        """Meander length [N] (DuckDB: meander_length)."""
        return self._writable("meand_len", "meander_length")

    @property
    def strm_order(self) -> WritableArray:
        """Stream order [N] (DuckDB: stream_order)."""
        return self._writable("strm_order", "stream_order")

    @property
    def end_rch(self) -> WritableArray:
        """End reach flag [N] (DuckDB: end_reach)."""
        return self._writable("end_rch", "end_reach")

    @property
    def reach_id(self) -> np.ndarray:
        """Parent reach ID [N] - read only."""
        return self._df["reach_id"].values

    # [2,N] array
    @property
    def cl_id(self) -> np.ndarray:
        """Centerline ID range [2,N] - (min, max)."""
        return self._cl_id

    @cl_id.setter
    def cl_id(self, value: np.ndarray) -> None:
        """Set cl_id array."""
        self._cl_id = value

    def __len__(self) -> int:
        return len(self._df)


class ReachesView:
    """
    Wrapper for reaches data providing numpy-array-style attribute access.

    Maps original SWORD attribute names to DuckDB column names.
    Handles multi-dimensional arrays by reconstruction.
    Returns WritableArray for attributes that support modification.

    Original -> DuckDB mapping:
        id -> reach_id
        len -> reach_length
        rch_n_nodes -> n_nodes
        grod -> obstr_type
        grod_fid -> grod_id
        hfalls_fid -> hfalls_id
        nchan_max -> n_chan_max
        nchan_mod -> n_chan_mod
        max_obs -> swot_obs
        low_slope -> low_slope_flag
        strm_order -> stream_order
        end_rch -> end_reach
        wth -> width
        wth_var -> width_var
        max_wth -> max_width
        cl_id[2,N] -> (cl_id_min, cl_id_max)
        rch_id_up[4,N] -> reach_topology table
        rch_id_down[4,N] -> reach_topology table
        orbits[75,N] -> reach_swot_orbits table
        iceflag[366,N] -> reach_ice_flags table
    """

    def __init__(
        self,
        df: "pd.DataFrame",
        rch_id_up: np.ndarray,
        rch_id_down: np.ndarray,
        orbits: np.ndarray = None,
        iceflag: np.ndarray = None,
        db=None,
        region: str = None,
        reactive: Optional["SWORDReactive"] = None,
    ):
        """
        Initialize ReachesView.

        Parameters
        ----------
        df : pd.DataFrame
            Reaches data from DuckDB query.
        rch_id_up : np.ndarray
            Reconstructed [4,N] upstream neighbor array.
        rch_id_down : np.ndarray
            Reconstructed [4,N] downstream neighbor array.
        orbits : np.ndarray, optional
            Reconstructed [75,N] SWOT orbits array.
        iceflag : np.ndarray, optional
            Reconstructed [366,N] ice flag array.
        db : SWORDDatabase, optional
            Database connection for write operations.
        region : str, optional
            Region code for write operations.
        reactive : SWORDReactive, optional
            Reactive system instance for automatic change tracking.
        """
        self._df = df
        self._rch_id_up = rch_id_up
        self._rch_id_down = rch_id_down
        self._orbits = orbits
        self._iceflag = iceflag
        self._db = db
        self._region = region
        self._ids = df["reach_id"].values
        self._reactive = reactive

        # Build cl_id [2,N] array from min/max columns
        self._cl_id = np.vstack([df["cl_id_min"].values, df["cl_id_max"].values])

    def _writable(self, col_name: str, db_col_name: str = None) -> WritableArray:
        """Create a WritableArray for the given column."""
        if db_col_name is None:
            db_col_name = col_name
        # Determine reactive attribute name
        attr_name = f"reach.{col_name}"
        if col_name in ("x", "y", "x_min", "x_max", "y_min", "y_max"):
            attr_name = "reach.bounds"
        elif col_name == "len":
            attr_name = "reach.len"
        elif col_name == "dist_out":
            attr_name = "reach.dist_out"
        elif col_name == "end_rch":
            attr_name = "reach.end_rch"
        elif col_name == "main_side":
            attr_name = "reach.main_side"
        elif col_name in ("n_rch_up", "n_rch_down"):
            attr_name = "reach.topology"
        return WritableArray(
            self._df[db_col_name].values,
            self._db,
            "reaches",
            "reach_id",
            col_name,
            db_col_name,
            self._ids,
            self._region,
            reactive=self._reactive,
            attr_name=attr_name,
        )

    # Direct mappings (same name) - now writable
    @property
    def x(self) -> WritableArray:
        return self._writable("x")

    @property
    def y(self) -> WritableArray:
        return self._writable("y")

    @property
    def x_min(self) -> WritableArray:
        return self._writable("x_min")

    @property
    def x_max(self) -> WritableArray:
        return self._writable("x_max")

    @property
    def y_min(self) -> WritableArray:
        return self._writable("y_min")

    @property
    def y_max(self) -> WritableArray:
        return self._writable("y_max")

    @property
    def wse(self) -> WritableArray:
        return self._writable("wse")

    @property
    def wse_var(self) -> WritableArray:
        return self._writable("wse_var")

    @property
    def slope(self) -> WritableArray:
        return self._writable("slope")

    @property
    def facc(self) -> WritableArray:
        return self._writable("facc")

    @property
    def dist_out(self) -> WritableArray:
        return self._writable("dist_out")

    @property
    def lakeflag(self) -> WritableArray:
        return self._writable("lakeflag")

    @property
    def n_rch_up(self) -> WritableArray:
        return self._writable("n_rch_up")

    @property
    def n_rch_down(self) -> WritableArray:
        return self._writable("n_rch_down")

    @property
    def river_name(self) -> WritableArray:
        return self._writable("river_name")

    @property
    def edit_flag(self) -> WritableArray:
        return self._writable("edit_flag")

    @property
    def trib_flag(self) -> WritableArray:
        return self._writable("trib_flag")

    @property
    def path_freq(self) -> WritableArray:
        return self._writable("path_freq")

    @property
    def path_order(self) -> WritableArray:
        return self._writable("path_order")

    @property
    def path_segs(self) -> WritableArray:
        return self._writable("path_segs")

    @property
    def main_side(self) -> WritableArray:
        return self._writable("main_side")

    @property
    def network(self) -> WritableArray:
        return self._writable("network")

    @property
    def add_flag(self) -> WritableArray:
        return self._writable("add_flag")

    # Renamed mappings - writable
    @property
    def id(self) -> np.ndarray:
        """Reach IDs [N] (DuckDB: reach_id) - read only."""
        return self._df["reach_id"].values

    @property
    def len(self) -> WritableArray:
        """Reach length [N] (DuckDB: reach_length)."""
        return self._writable("len", "reach_length")

    @property
    def rch_n_nodes(self) -> WritableArray:
        """Number of nodes per reach [N] (DuckDB: n_nodes)."""
        return self._writable("rch_n_nodes", "n_nodes")

    @property
    def grod(self) -> WritableArray:
        """Obstruction type [N] (DuckDB: obstr_type)."""
        return self._writable("grod", "obstr_type")

    @property
    def grod_fid(self) -> WritableArray:
        """GROD feature ID [N] (DuckDB: grod_id)."""
        return self._writable("grod_fid", "grod_id")

    @property
    def hfalls_fid(self) -> WritableArray:
        """HydroFALLS feature ID [N] (DuckDB: hfalls_id)."""
        return self._writable("hfalls_fid", "hfalls_id")

    @property
    def nchan_max(self) -> WritableArray:
        """Max channels [N] (DuckDB: n_chan_max)."""
        return self._writable("nchan_max", "n_chan_max")

    @property
    def nchan_mod(self) -> WritableArray:
        """Modal channels [N] (DuckDB: n_chan_mod)."""
        return self._writable("nchan_mod", "n_chan_mod")

    @property
    def max_obs(self) -> WritableArray:
        """Max SWOT observations [N] (DuckDB: swot_obs)."""
        return self._writable("max_obs", "swot_obs")

    @property
    def low_slope(self) -> WritableArray:
        """Low slope flag [N] (DuckDB: low_slope_flag)."""
        return self._writable("low_slope", "low_slope_flag")

    @property
    def strm_order(self) -> WritableArray:
        """Stream order [N] (DuckDB: stream_order)."""
        return self._writable("strm_order", "stream_order")

    @property
    def end_rch(self) -> WritableArray:
        """End reach flag [N] (DuckDB: end_reach)."""
        return self._writable("end_rch", "end_reach")

    @property
    def wth(self) -> WritableArray:
        """Width [N] (DuckDB: width)."""
        return self._writable("wth", "width")

    @property
    def wth_var(self) -> WritableArray:
        """Width variance [N] (DuckDB: width_var)."""
        return self._writable("wth_var", "width_var")

    @property
    def max_wth(self) -> WritableArray:
        """Max width [N] (DuckDB: max_width)."""
        return self._writable("max_wth", "max_width")

    # Multi-dimensional arrays
    @property
    def cl_id(self) -> np.ndarray:
        """Centerline ID range [2,N] - (min, max)."""
        return self._cl_id

    @cl_id.setter
    def cl_id(self, value: np.ndarray) -> None:
        """Set cl_id array."""
        self._cl_id = value

    @property
    def rch_id_up(self) -> np.ndarray:
        """Upstream reach IDs [4,N]."""
        return self._rch_id_up

    @rch_id_up.setter
    def rch_id_up(self, value: np.ndarray) -> None:
        """Set upstream reach IDs array."""
        self._rch_id_up = value

    @property
    def rch_id_down(self) -> np.ndarray:
        """Downstream reach IDs [4,N]."""
        return self._rch_id_down

    @rch_id_down.setter
    def rch_id_down(self, value: np.ndarray) -> None:
        """Set downstream reach IDs array."""
        self._rch_id_down = value

    @property
    def orbits(self) -> np.ndarray:
        """SWOT orbits [75,N]."""
        return self._orbits

    @orbits.setter
    def orbits(self, value: np.ndarray) -> None:
        """Set orbits array."""
        self._orbits = value

    @property
    def iceflag(self) -> np.ndarray:
        """Daily ice flags [366,N]."""
        return self._iceflag

    @iceflag.setter
    def iceflag(self, value: np.ndarray) -> None:
        """Set iceflag array."""
        self._iceflag = value

    def __len__(self) -> int:
        return len(self._df)
