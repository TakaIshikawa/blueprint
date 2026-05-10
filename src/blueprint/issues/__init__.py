"""Issue and blocker tracking with resolution workflow."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.issues.issue_model import (
        Issue,
        IssueSeverity,
        IssueStatus,
        IssueTransition,
        IssueType,
        SLAConfig,
    )
    from blueprint.issues.issue_tracker import IssueTracker

_TRACKER_MODULE = "blueprint.issues.issue_tracker"
_MODEL_MODULE = "blueprint.issues.issue_model"

_EXPORTS = {
    "IssueTracker": _TRACKER_MODULE,
    "Issue": _MODEL_MODULE,
    "IssueSeverity": _MODEL_MODULE,
    "IssueStatus": _MODEL_MODULE,
    "IssueTransition": _MODEL_MODULE,
    "IssueType": _MODEL_MODULE,
    "SLAConfig": _MODEL_MODULE,
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
