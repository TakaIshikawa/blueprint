"""Templates subpackage for blueprint."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.templates.task_template_engine import TaskTemplateEngine
    from blueprint.templates.template_model import (
        ParameterType,
        TemplateParameter,
        TaskDefinition,
        TaskTemplate,
    )

_EXPORTS = {
    "TaskTemplateEngine": "blueprint.templates.task_template_engine",
    "ParameterType": "blueprint.templates.template_model",
    "TemplateParameter": "blueprint.templates.template_model",
    "TaskDefinition": "blueprint.templates.template_model",
    "TaskTemplate": "blueprint.templates.template_model",
}


def __getattr__(name: str) -> Any:
    """Load template classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
