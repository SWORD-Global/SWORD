"""
SWORD Lint Framework - Core Types and Registry

Provides foundational types for the linting framework:
- Severity enum (ERROR, WARNING, INFO)
- Category enum (TOPOLOGY, ATTRIBUTES, GEOMETRY, CLASSIFICATION)
- CheckResult dataclass
- CheckSpec dataclass for check metadata
- Decorator-based check registration
"""

from enum import Enum
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional
import pandas as pd


class Severity(Enum):
    """Severity levels for lint checks."""

    ERROR = "error"  # Must fix before release
    WARNING = "warning"  # Should investigate
    INFO = "info"  # Expected/acceptable


class Category(Enum):
    """Categories for organizing lint checks."""

    TOPOLOGY = "topology"
    ATTRIBUTES = "attributes"
    GEOMETRY = "geometry"
    CLASSIFICATION = "classification"
    V17C = "v17c"  # v17c-specific checks for new attributes
    FLAGS = "flags"  # iceflag, low_slope_flag, edit_flag, swot_obs
    NETWORK = "network"  # main_side, stream_order consistency
    OBSTRUCTION = "obstruction"  # obstr_type, grod_id, dl_grod_id, hfalls_id


@dataclass
class CheckResult:
    """Result of a lint check execution."""

    check_id: str  # e.g., "T001"
    name: str  # e.g., "dist_out_monotonicity"
    severity: Severity
    passed: bool
    total_checked: int
    issues_found: int
    issue_pct: float
    details: pd.DataFrame  # DataFrame of issue rows
    description: str
    threshold: Optional[float] = None
    elapsed_ms: float = 0.0

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"CheckResult({self.check_id} {self.name}: {status}, {self.issues_found}/{self.total_checked} issues)"


@dataclass
class CheckSpec:
    """Specification for a registered lint check."""

    check_id: str
    name: str
    category: Category
    severity: Severity
    description: str
    default_threshold: Optional[float]
    check_fn: Callable

    def __repr__(self) -> str:
        return f"CheckSpec({self.check_id}: {self.name}, {self.category.value}, {self.severity.value})"


# Global registry of checks
_CHECK_REGISTRY: Dict[str, CheckSpec] = {}


def register_check(
    check_id: str,
    category: Category,
    severity: Severity,
    description: str,
    default_threshold: Optional[float] = None,
) -> Callable:
    """
    Decorator to register a lint check function.

    Usage:
        @register_check("T001", Category.TOPOLOGY, Severity.ERROR,
                        "dist_out must decrease downstream")
        def check_dist_out_monotonicity(conn, region=None, threshold=None):
            ...

    Args:
        check_id: Unique ID like "T001", "A002"
        category: Check category (TOPOLOGY, ATTRIBUTES, etc.)
        severity: Default severity level
        description: Human-readable description of what the check validates
        default_threshold: Optional default threshold value

    Returns:
        Decorated function
    """

    def decorator(fn: Callable) -> Callable:
        # Extract name from function name (remove check_ prefix if present)
        name = fn.__name__
        if name.startswith("check_"):
            name = name[6:]

        # Validate check_id uniqueness
        if check_id in _CHECK_REGISTRY:
            raise ValueError(f"Check ID {check_id} already registered")

        # Register the check
        spec = CheckSpec(
            check_id=check_id,
            name=name,
            category=category,
            severity=severity,
            description=description,
            default_threshold=default_threshold,
            check_fn=fn,
        )
        _CHECK_REGISTRY[check_id] = spec

        # Attach metadata to function for introspection
        fn._check_spec = spec
        return fn

    return decorator


def get_registry() -> Dict[str, CheckSpec]:
    """Return a copy of the check registry."""
    return dict(_CHECK_REGISTRY)


def get_check(check_id: str) -> Optional[CheckSpec]:
    """Get a specific check by ID."""
    return _CHECK_REGISTRY.get(check_id)


def get_checks_by_category(category: Category) -> List[CheckSpec]:
    """Get all checks in a category."""
    return [spec for spec in _CHECK_REGISTRY.values() if spec.category == category]


def get_checks_by_severity(severity: Severity) -> List[CheckSpec]:
    """Get all checks with a specific severity."""
    return [spec for spec in _CHECK_REGISTRY.values() if spec.severity == severity]


def list_check_ids() -> List[str]:
    """Return sorted list of all registered check IDs."""
    return sorted(_CHECK_REGISTRY.keys())
