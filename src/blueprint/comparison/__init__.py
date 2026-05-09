"""Plan comparison system with side-by-side analysis."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.comparison.plan_comparator import (
        ComparisonDataStore,
        ComparisonDimension,
        ComparisonReport,
        ComparisonResult,
        ComparisonUseCase,
        Difference,
        DifferenceType,
        MetricComparison,
        PlanComparator,
        ResourceComparison,
        TaskOverlap,
        TimelineComparison,
    )

_MODULE = "blueprint.comparison.plan_comparator"

_EXPORTS = {
    "ComparisonDataStore": _MODULE,
    "ComparisonDimension": _MODULE,
    "ComparisonReport": _MODULE,
    "ComparisonResult": _MODULE,
    "ComparisonUseCase": _MODULE,
    "Difference": _MODULE,
    "DifferenceType": _MODULE,
    "MetricComparison": _MODULE,
    "PlanComparator": _MODULE,
    "ResourceComparison": _MODULE,
    "TaskOverlap": _MODULE,
    "TimelineComparison": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load comparison classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
