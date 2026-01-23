# -*- coding: utf-8 -*-
"""
Imagery Pipeline Exceptions
===========================

Custom exceptions for the SWORD satellite imagery inspection pipeline.
"""


class ImageryError(Exception):
    """Base exception for imagery pipeline errors."""

    pass


class STACSearchError(ImageryError):
    """Error during STAC catalog search."""

    def __init__(self, message: str, bbox: tuple = None, query: dict = None):
        self.bbox = bbox
        self.query = query
        super().__init__(message)


class NoImageryFoundError(ImageryError):
    """No imagery found matching search criteria."""

    def __init__(
        self,
        message: str = "No imagery found matching criteria",
        bbox: tuple = None,
        date_range: tuple = None,
        cloud_cover: float = None,
    ):
        self.bbox = bbox
        self.date_range = date_range
        self.cloud_cover = cloud_cover
        super().__init__(message)


class COGReadError(ImageryError):
    """Error reading Cloud Optimized GeoTIFF."""

    def __init__(self, message: str, url: str = None, band: str = None):
        self.url = url
        self.band = band
        super().__init__(message)


class CacheError(ImageryError):
    """Error with imagery cache operations."""

    pass


class NDWIComputationError(ImageryError):
    """Error computing NDWI index."""

    def __init__(self, message: str, green_shape: tuple = None, nir_shape: tuple = None):
        self.green_shape = green_shape
        self.nir_shape = nir_shape
        super().__init__(message)


class BandMismatchError(ImageryError):
    """Mismatch between expected and available bands."""

    def __init__(self, message: str, expected: list = None, available: list = None):
        self.expected = expected
        self.available = available
        super().__init__(message)
