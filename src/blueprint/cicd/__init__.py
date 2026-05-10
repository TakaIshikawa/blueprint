"""CI/CD integrations for build and deployment tracking."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.cicd.github_actions import GitHubActionsIntegration
    from blueprint.cicd.gitlab_ci import GitLabCIIntegration
    from blueprint.cicd.jenkins import JenkinsIntegration

_EXPORTS = {
    "GitHubActionsIntegration": "blueprint.cicd.github_actions",
    "GitLabCIIntegration": "blueprint.cicd.gitlab_ci",
    "JenkinsIntegration": "blueprint.cicd.jenkins",
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
