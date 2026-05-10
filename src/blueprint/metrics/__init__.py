"""Metrics and velocity tracking for sprint-based planning."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.metrics.sprint_model import (
        BurndownPoint,
        BurnupPoint,
        CapacityForecast,
        Sprint,
        SprintStatus,
        TrendAnalysis,
        VelocityRecord,
        VelocityTrend,
    )
    from blueprint.metrics.velocity_tracker import VelocityTracker

_TRACKER_MODULE = "blueprint.metrics.velocity_tracker"
_MODEL_MODULE = "blueprint.metrics.sprint_model"

_EXPORTS = {
    "VelocityTracker": _TRACKER_MODULE,
    "BurndownPoint": _MODEL_MODULE,
    "BurnupPoint": _MODEL_MODULE,
    "CapacityForecast": _MODEL_MODULE,
    "Sprint": _MODEL_MODULE,
    "SprintStatus": _MODEL_MODULE,
    "TrendAnalysis": _MODEL_MODULE,
    "VelocityRecord": _MODEL_MODULE,
    "VelocityTrend": _MODEL_MODULE,
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
