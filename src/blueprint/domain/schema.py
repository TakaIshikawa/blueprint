"""JSON Schema helpers for Blueprint domain records."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from pydantic import BaseModel

from blueprint.domain.models import (
    ExecutionPlan,
    ExecutionTask,
    ExportRecord,
    ImplementationBrief,
    SourceBrief,
    StatusEvent,
)


DOMAIN_SCHEMA_MODELS: Mapping[str, type[BaseModel]] = MappingProxyType(
    {
        "source-brief": SourceBrief,
        "implementation-brief": ImplementationBrief,
        "execution-plan": ExecutionPlan,
        "execution-task": ExecutionTask,
        "status-event": StatusEvent,
        "export-record": ExportRecord,
    }
)
AVAILABLE_SCHEMA_MODELS = tuple(DOMAIN_SCHEMA_MODELS.keys())


class UnknownSchemaModelError(ValueError):
    """Raised when a requested schema model is not exported."""


def get_model_json_schema(model_name: str) -> dict:
    """Return the JSON Schema for a single exported domain model."""
    try:
        model = DOMAIN_SCHEMA_MODELS[model_name]
    except KeyError as e:
        available = ", ".join((*AVAILABLE_SCHEMA_MODELS, "all"))
        raise UnknownSchemaModelError(
            f"Unknown schema model: {model_name}. Expected one of: {available}"
        ) from e

    return model.model_json_schema(ref_template="#/$defs/{model}")


def get_all_model_json_schemas() -> dict[str, dict]:
    """Return JSON Schemas for every exported domain model keyed by CLI model name."""
    return {model_name: get_model_json_schema(model_name) for model_name in AVAILABLE_SCHEMA_MODELS}
