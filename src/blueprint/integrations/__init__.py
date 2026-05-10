"""Integrations with external ITSM and project management systems."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.integrations.make import MakeIntegration
    from blueprint.integrations.n8n import N8nIntegration
    from blueprint.integrations.servicenow_integration import ServiceNowIntegration
    from blueprint.integrations.zapier import ZapierIntegration

_EXPORTS = {
    "ServiceNowIntegration": "blueprint.integrations.servicenow_integration",
    "ZapierIntegration": "blueprint.integrations.zapier",
    "MakeIntegration": "blueprint.integrations.make",
    "N8nIntegration": "blueprint.integrations.n8n",
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
