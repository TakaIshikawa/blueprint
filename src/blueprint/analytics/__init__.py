"""Analytics subpackage for blueprint."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.analytics.scope_creep_detector import (
        ScopeBaseline,
        ScopeChange,
        ScopeCreepDetectorConfig,
        DriftResult,
        ChangeVelocity,
        SprintScopeTrend,
        TaskSnapshot,
        ThresholdAlert,
    )

_EXPORTS = {
    "ScopeBaseline": "blueprint.analytics.scope_creep_detector",
    "ScopeChange": "blueprint.analytics.scope_creep_detector",
    "ScopeCreepDetectorConfig": "blueprint.analytics.scope_creep_detector",
    "DriftResult": "blueprint.analytics.scope_creep_detector",
    "ChangeVelocity": "blueprint.analytics.scope_creep_detector",
    "SprintScopeTrend": "blueprint.analytics.scope_creep_detector",
    "TaskSnapshot": "blueprint.analytics.scope_creep_detector",
    "ThresholdAlert": "blueprint.analytics.scope_creep_detector",
    "capture_baseline": "blueprint.analytics.scope_creep_detector",
    "calculate_drift": "blueprint.analytics.scope_creep_detector",
    "calculate_change_velocity": "blueprint.analytics.scope_creep_detector",
    "detect_changes": "blueprint.analytics.scope_creep_detector",
    "analyze_trends": "blueprint.analytics.scope_creep_detector",
    "generate_scope_change_report": "blueprint.analytics.scope_creep_detector",
}


def __getattr__(name: str) -> Any:
    """Load analytics classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
