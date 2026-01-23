# -*- coding: utf-8 -*-
"""
STAC Client for Sentinel-2 Imagery
==================================

Wrapper around pystac-client for searching Element 84 Earth Search catalog.
"""

from datetime import datetime, timedelta
from typing import Optional, List, Tuple, TYPE_CHECKING
import logging

from pystac_client import Client
from pystac import ItemCollection, Item

from .config import ImageryConfig, DEFAULT_CONFIG
from .exceptions import STACSearchError, NoImageryFoundError

if TYPE_CHECKING:
    from ..sword_class import SWORD

logger = logging.getLogger(__name__)


class SentinelSTACClient:
    """
    Client for searching Sentinel-2 imagery via STAC API.

    Uses Element 84 Earth Search catalog by default.
    """

    def __init__(self, config: ImageryConfig = None):
        """
        Initialize STAC client.

        Args:
            config: Imagery configuration. Uses DEFAULT_CONFIG if not provided.
        """
        self.config = config or DEFAULT_CONFIG
        self._client = None

    @property
    def client(self) -> Client:
        """Lazy-load STAC client connection."""
        if self._client is None:
            try:
                self._client = Client.open(self.config.stac_url)
                logger.info(f"Connected to STAC catalog: {self.config.stac_url}")
            except Exception as e:
                raise STACSearchError(f"Failed to connect to STAC catalog: {e}")
        return self._client

    def search_by_bbox(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_cloud_cover: Optional[float] = None,
        limit: int = 100,
    ) -> ItemCollection:
        """
        Search for Sentinel-2 items intersecting a bounding box.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_cloud_cover: Maximum cloud cover percentage (0-100)
            limit: Maximum number of items to return

        Returns:
            ItemCollection of matching STAC items
        """
        # Default date range
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start = datetime.now() - timedelta(days=self.config.default_date_range_days)
            start_date = start.strftime("%Y-%m-%d")

        datetime_range = f"{start_date}/{end_date}"

        # Cloud cover filter
        cloud_cover = max_cloud_cover or self.config.max_cloud_cover
        query = {"eo:cloud_cover": {"lt": cloud_cover}}

        try:
            search = self.client.search(
                collections=[self.config.collection],
                bbox=bbox,
                datetime=datetime_range,
                query=query,
                limit=limit,
            )
            items = search.item_collection()
            logger.info(f"Found {len(items)} items for bbox {bbox}")
            return items

        except Exception as e:
            raise STACSearchError(
                f"STAC search failed: {e}",
                bbox=bbox,
                query={"datetime": datetime_range, "cloud_cover": cloud_cover},
            )

    def search_by_reach(
        self,
        sword: "SWORD",
        reach_id: int,
        buffer_m: Optional[float] = None,
        **kwargs,
    ) -> ItemCollection:
        """
        Search for imagery covering a SWORD reach.

        Args:
            sword: SWORD instance with loaded data
            reach_id: Reach ID to search for
            buffer_m: Buffer around reach bbox in meters
            **kwargs: Additional arguments passed to search_by_bbox

        Returns:
            ItemCollection of matching STAC items
        """
        buffer = buffer_m or self.config.buffer_m

        # Get reach index
        reach_idx = (sword.reaches.reach_id == reach_id).argmax()
        if sword.reaches.reach_id[reach_idx] != reach_id:
            raise STACSearchError(f"Reach {reach_id} not found in SWORD database")

        # Get bounding box from reach
        x_min = sword.reaches.x_min[reach_idx]
        x_max = sword.reaches.x_max[reach_idx]
        y_min = sword.reaches.y_min[reach_idx]
        y_max = sword.reaches.y_max[reach_idx]

        # Convert buffer from meters to degrees (approximate)
        lat_center = (y_min + y_max) / 2
        buffer_deg = buffer / 111320  # meters to degrees at equator
        buffer_deg_lon = buffer_deg / max(0.1, abs(cos_deg(lat_center)))

        # Apply buffer
        bbox = (
            x_min - buffer_deg_lon,
            y_min - buffer_deg,
            x_max + buffer_deg_lon,
            y_max + buffer_deg,
        )

        return self.search_by_bbox(bbox, **kwargs)

    def search_by_reaches(
        self,
        sword: "SWORD",
        reach_ids: List[int],
        buffer_m: Optional[float] = None,
        **kwargs,
    ) -> ItemCollection:
        """
        Search for imagery covering multiple SWORD reaches.

        Computes combined bounding box for all reaches.

        Args:
            sword: SWORD instance with loaded data
            reach_ids: List of reach IDs to search for
            buffer_m: Buffer around combined bbox in meters
            **kwargs: Additional arguments passed to search_by_bbox

        Returns:
            ItemCollection of matching STAC items
        """
        buffer = buffer_m or self.config.buffer_m

        # Collect bounds from all reaches
        x_mins, x_maxs, y_mins, y_maxs = [], [], [], []

        for reach_id in reach_ids:
            reach_idx = (sword.reaches.reach_id == reach_id).argmax()
            if sword.reaches.reach_id[reach_idx] != reach_id:
                logger.warning(f"Reach {reach_id} not found, skipping")
                continue

            x_mins.append(sword.reaches.x_min[reach_idx])
            x_maxs.append(sword.reaches.x_max[reach_idx])
            y_mins.append(sword.reaches.y_min[reach_idx])
            y_maxs.append(sword.reaches.y_max[reach_idx])

        if not x_mins:
            raise STACSearchError(f"None of the specified reaches found: {reach_ids}")

        # Combined bbox
        x_min, x_max = min(x_mins), max(x_maxs)
        y_min, y_max = min(y_mins), max(y_maxs)

        # Convert buffer from meters to degrees
        lat_center = (y_min + y_max) / 2
        buffer_deg = buffer / 111320
        buffer_deg_lon = buffer_deg / max(0.1, abs(cos_deg(lat_center)))

        bbox = (
            x_min - buffer_deg_lon,
            y_min - buffer_deg,
            x_max + buffer_deg_lon,
            y_max + buffer_deg,
        )

        return self.search_by_bbox(bbox, **kwargs)

    def get_best_item(
        self,
        items: ItemCollection,
        target_date: Optional[datetime] = None,
        prefer_low_cloud: bool = True,
    ) -> Item:
        """
        Select the best item from a collection.

        Selection priority:
        1. Cloud cover (if prefer_low_cloud)
        2. Closest to target date (or most recent if no target)

        Args:
            items: ItemCollection to select from
            target_date: Preferred acquisition date
            prefer_low_cloud: Prioritize low cloud cover

        Returns:
            Best matching STAC Item

        Raises:
            NoImageryFoundError: If no items in collection
        """
        if len(items) == 0:
            raise NoImageryFoundError("No imagery items to select from")

        # Sort items
        def sort_key(item: Item) -> tuple:
            props = item.properties
            cloud = props.get("eo:cloud_cover", 100)

            # Parse acquisition datetime
            dt_str = props.get("datetime") or item.datetime
            if isinstance(dt_str, str):
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            else:
                dt = dt_str

            # Date score (days from target or now)
            target = target_date or datetime.now(dt.tzinfo)
            date_diff = abs((dt - target).days)

            if prefer_low_cloud:
                return (cloud, date_diff)
            else:
                return (date_diff, cloud)

        sorted_items = sorted(items, key=sort_key)
        best = sorted_items[0]

        logger.info(
            f"Selected item {best.id} "
            f"(cloud: {best.properties.get('eo:cloud_cover', 'N/A')}%, "
            f"date: {best.properties.get('datetime', 'N/A')})"
        )

        return best

    def get_seasonal_items(
        self,
        bbox: Tuple[float, float, float, float],
        year: Optional[int] = None,
        season: str = "summer",
        **kwargs,
    ) -> ItemCollection:
        """
        Search for imagery from a specific season.

        Automatically adjusts season based on hemisphere.

        Args:
            bbox: Bounding box
            year: Year to search (default: current year)
            season: 'summer', 'winter', 'spring', 'fall'
            **kwargs: Additional search arguments

        Returns:
            ItemCollection of seasonal items
        """
        year = year or datetime.now().year
        lat_center = (bbox[1] + bbox[3]) / 2

        # Determine hemisphere and adjust season
        northern = lat_center > 0

        # Season date ranges (Northern Hemisphere)
        seasons = {
            "summer": ("06-01", "09-30"),
            "winter": ("12-01", "02-28"),
            "spring": ("03-01", "05-31"),
            "fall": ("09-01", "11-30"),
        }

        # Flip for Southern Hemisphere
        if not northern:
            flip = {"summer": "winter", "winter": "summer", "spring": "fall", "fall": "spring"}
            season = flip.get(season, season)

        start_mmdd, end_mmdd = seasons.get(season, ("01-01", "12-31"))

        # Handle year wraparound for winter
        if season == "winter" and northern:
            # Dec of previous year to Feb of current year
            start_date = f"{year - 1}-{start_mmdd}"
            end_date = f"{year}-{end_mmdd}"
        else:
            start_date = f"{year}-{start_mmdd}"
            end_date = f"{year}-{end_mmdd}"

        return self.search_by_bbox(bbox, start_date=start_date, end_date=end_date, **kwargs)


def cos_deg(degrees: float) -> float:
    """Cosine of angle in degrees."""
    import math

    return math.cos(math.radians(degrees))
