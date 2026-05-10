"""Data models for task template engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ParameterType(str, Enum):
    """Supported parameter types in templates."""

    STRING = "string"
    NUMBER = "number"
    DATE = "date"
    SELECT = "select"
    LIST = "list"


@dataclass(frozen=True, slots=True)
class TemplateParameter:
    """A parameter definition within a template."""

    name: str
    param_type: ParameterType
    description: str = ""
    required: bool = True
    default: Any = None
    options: tuple[str, ...] = ()  # for SELECT type

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "param_type": self.param_type.value,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "options": list(self.options),
        }


@dataclass(frozen=True, slots=True)
class TaskDefinition:
    """A task to be generated from a template."""

    title_template: str
    description_template: str = ""
    effort: float = 0.0
    tags: tuple[str, ...] = ()
    condition: str | None = None  # parameter name that must be truthy

    def to_dict(self) -> dict[str, Any]:
        return {
            "title_template": self.title_template,
            "description_template": self.description_template,
            "effort": self.effort,
            "tags": list(self.tags),
            "condition": self.condition,
        }


@dataclass(frozen=True, slots=True)
class TaskTemplate:
    """A reusable template for generating tasks."""

    name: str
    description: str = ""
    version: str = "1.0.0"
    parameters: tuple[TemplateParameter, ...] = ()
    task_definitions: tuple[TaskDefinition, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "parameters": [p.to_dict() for p in self.parameters],
            "task_definitions": [t.to_dict() for t in self.task_definitions],
        }

    def parameter_names(self) -> set[str]:
        """Return set of all parameter names."""
        return {p.name for p in self.parameters}


__all__ = [
    "ParameterType",
    "TaskDefinition",
    "TaskTemplate",
    "TemplateParameter",
]
