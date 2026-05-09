"""Plan transformation rules engine."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.transformation.plan_transformer import (
        PlanTransformer,
        RuleValidation,
        TransformationResult,
        TransformationType,
        TransformDataStore,
        TransformPreview,
    )
    from blueprint.transformation.rules_engine import (
        ActionType,
        ConditionOperator,
        RuleAction,
        RuleCondition,
        TransformationRule,
    )

_TRANSFORMER_MODULE = "blueprint.transformation.plan_transformer"
_ENGINE_MODULE = "blueprint.transformation.rules_engine"

_EXPORTS = {
    "PlanTransformer": _TRANSFORMER_MODULE,
    "RuleValidation": _TRANSFORMER_MODULE,
    "TransformationResult": _TRANSFORMER_MODULE,
    "TransformationType": _TRANSFORMER_MODULE,
    "TransformDataStore": _TRANSFORMER_MODULE,
    "TransformPreview": _TRANSFORMER_MODULE,
    "ActionType": _ENGINE_MODULE,
    "ConditionOperator": _ENGINE_MODULE,
    "RuleAction": _ENGINE_MODULE,
    "RuleCondition": _ENGINE_MODULE,
    "TransformationRule": _ENGINE_MODULE,
}


def __getattr__(name: str) -> Any:
    """Load transformation classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
