"""Portfolio management for aggregating multiple execution plans."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.portfolio.portfolio_manager import (
        PlanData,
        PortfolioManager,
    )
    from blueprint.portfolio.portfolio_model import (
        CrossPlanDependency,
        HealthDashboard,
        HealthRating,
        MilestoneEntry,
        PerformanceTarget,
        PlanReference,
        Portfolio,
        PortfolioGoal,
        PortfolioRisk,
        PortfolioStatus,
        ResourceAllocation,
        RiskLevel,
        RollupMetrics,
    )

_MANAGER_MODULE = "blueprint.portfolio.portfolio_manager"
_MODEL_MODULE = "blueprint.portfolio.portfolio_model"

_EXPORTS = {
    "PlanData": _MANAGER_MODULE,
    "PortfolioManager": _MANAGER_MODULE,
    "CrossPlanDependency": _MODEL_MODULE,
    "HealthDashboard": _MODEL_MODULE,
    "HealthRating": _MODEL_MODULE,
    "MilestoneEntry": _MODEL_MODULE,
    "PerformanceTarget": _MODEL_MODULE,
    "PlanReference": _MODEL_MODULE,
    "Portfolio": _MODEL_MODULE,
    "PortfolioGoal": _MODEL_MODULE,
    "PortfolioRisk": _MODEL_MODULE,
    "PortfolioStatus": _MODEL_MODULE,
    "ResourceAllocation": _MODEL_MODULE,
    "RiskLevel": _MODEL_MODULE,
    "RollupMetrics": _MODEL_MODULE,
}


def __getattr__(name: str) -> Any:
    """Load portfolio classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
