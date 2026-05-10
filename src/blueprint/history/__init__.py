"""Plan change history tracking with timeline view."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.history.change_event import ChangeEvent, ChangeType, EntityType
    from blueprint.history.change_tracker import ChangeTracker

_EVENT_MODULE = "blueprint.history.change_event"
_TRACKER_MODULE = "blueprint.history.change_tracker"

_EXPORTS = {
    "ChangeEvent": _EVENT_MODULE,
    "ChangeType": _EVENT_MODULE,
    "EntityType": _EVENT_MODULE,
    "ChangeTracker": _TRACKER_MODULE,
}


def __getattr__(name: str) -> Any:
    """Load history classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
