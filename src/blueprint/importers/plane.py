"""Plane.so API importer for issues and cycles."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Status / priority mapping
# ---------------------------------------------------------------------------

DEFAULT_STATE_MAP: dict[str, str] = {
    "Backlog": "pending",
    "Unstarted": "pending",
    "Started": "in_progress",
    "In Progress": "in_progress",
    "Done": "completed",
    "Cancelled": "cancelled",
}

PRIORITY_MAP: dict[str, str] = {
    "urgent": "critical",
    "high": "high",
    "medium": "medium",
    "low": "low",
    "none": "none",
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


@dataclass
class PlaneClient:
    """Lightweight client for Plane.so API."""

    api_key: str
    base_url: str = "https://api.plane.so/api/v1"

    def _headers(self) -> dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def request_json(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a GET request and return parsed JSON.

        Args:
            endpoint: API endpoint path.
            params: Optional query parameters.

        Returns:
            Parsed JSON response.

        Raises:
            ImportError: If request fails.
        """
        url = f"{self.base_url}/{endpoint}"
        if params:
            url = f"{url}?{urlencode(params)}"

        request = Request(url, headers=self._headers(), method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Plane API request failed with HTTP {exc.code}: {endpoint}"
            ) from exc
        except URLError as exc:
            raise ImportError(f"Plane API request failed: {exc.reason}") from exc

        return json.loads(raw)

    def get_issues(
        self, workspace_slug: str, project_id: str
    ) -> list[dict[str, Any]]:
        """Get issues for a project."""
        result = self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}/issues"
        )
        if isinstance(result, dict):
            return result.get("results", [])
        return result if isinstance(result, list) else []

    def get_issue(
        self, workspace_slug: str, project_id: str, issue_id: str
    ) -> dict[str, Any]:
        """Get a single issue by ID."""
        return self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}/issues/{issue_id}"
        )

    def get_cycles(
        self, workspace_slug: str, project_id: str
    ) -> list[dict[str, Any]]:
        """Get cycles (sprints) for a project."""
        result = self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}/cycles"
        )
        if isinstance(result, dict):
            return result.get("results", [])
        return result if isinstance(result, list) else []

    def get_cycle_issues(
        self, workspace_slug: str, project_id: str, cycle_id: str
    ) -> list[dict[str, Any]]:
        """Get issues assigned to a cycle."""
        result = self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}/cycles/{cycle_id}/cycle-issues"
        )
        if isinstance(result, dict):
            return result.get("results", [])
        return result if isinstance(result, list) else []

    def get_modules(
        self, workspace_slug: str, project_id: str
    ) -> list[dict[str, Any]]:
        """Get modules for a project."""
        result = self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}/modules"
        )
        if isinstance(result, dict):
            return result.get("results", [])
        return result if isinstance(result, list) else []

    def get_project(
        self, workspace_slug: str, project_id: str
    ) -> dict[str, Any]:
        """Get project details."""
        return self.request_json(
            f"workspaces/{workspace_slug}/projects/{project_id}"
        )


# ---------------------------------------------------------------------------
# Importer
# ---------------------------------------------------------------------------


class PlaneImporter(SourceImporter):
    """Import issues and cycles from Plane.so.

    Supports:
    - API key authentication
    - Issue attributes mapped to blueprint tasks
    - Parent-child issue hierarchy
    - Cycles as sprint phases
    - Modules as milestones
    - Labels as tags
    - Issue links/relations as dependencies
    - Incremental sync by updated_at
    """

    def __init__(
        self,
        api_key: str | None = None,
        workspace_slug: str | None = None,
        state_map: dict[str, str] | None = None,
    ):
        self.api_key = api_key or os.environ.get("PLANE_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "API key required: pass api_key or set PLANE_API_KEY env var"
            )
        self.workspace_slug = workspace_slug or os.environ.get("PLANE_WORKSPACE_SLUG", "")
        self.client = PlaneClient(api_key=self.api_key)
        self.state_map = state_map or DEFAULT_STATE_MAP

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Import issues from a Plane project.

        Args:
            source_id: Plane project ID.

        Returns:
            Dictionary representing a SourceBrief with tasks.
        """
        if not self.workspace_slug:
            raise ValueError("workspace_slug is required for import")

        project = self.client.get_project(self.workspace_slug, source_id)
        if not project:
            raise ImportError(f"Plane project not found: {source_id}")

        issues = self.client.get_issues(self.workspace_slug, source_id)
        tasks = [self._map_issue(i) for i in issues]

        return {
            "id": generate_source_brief_id(),
            "source_type": "plane",
            "source_id": source_id,
            "title": project.get("name", f"Plane project {source_id}"),
            "description": project.get("description", ""),
            "tasks": tasks,
            "imported_at": datetime.now().isoformat(),
            "metadata": {
                "plane_workspace": self.workspace_slug,
                "plane_project_id": source_id,
                "issue_count": len(tasks),
            },
        }

    def import_issues(
        self,
        project_id: str,
        updated_since: str | None = None,
    ) -> list[dict[str, Any]]:
        """Import issues with optional incremental sync.

        Args:
            project_id: Plane project ID.
            updated_since: Optional ISO date for incremental sync.

        Returns:
            List of blueprint task dictionaries.
        """
        if not self.workspace_slug:
            raise ValueError("workspace_slug is required")
        issues = self.client.get_issues(self.workspace_slug, project_id)
        if updated_since:
            issues = [
                i for i in issues
                if (i.get("updated_at") or "") >= updated_since
            ]
        return [self._map_issue(i) for i in issues]

    def import_cycles(self, project_id: str) -> list[dict[str, Any]]:
        """Import cycles as sprint phases.

        Args:
            project_id: Plane project ID.

        Returns:
            List of phase dictionaries with dates.
        """
        if not self.workspace_slug:
            raise ValueError("workspace_slug is required")
        cycles = self.client.get_cycles(self.workspace_slug, project_id)
        return [
            {
                "phase_id": str(c.get("id", "")),
                "name": c.get("name", ""),
                "start_date": c.get("start_date"),
                "end_date": c.get("end_date"),
                "status": c.get("status", ""),
            }
            for c in cycles
        ]

    def import_modules(self, project_id: str) -> list[dict[str, Any]]:
        """Import modules as milestones.

        Args:
            project_id: Plane project ID.

        Returns:
            List of milestone dictionaries.
        """
        if not self.workspace_slug:
            raise ValueError("workspace_slug is required")
        modules = self.client.get_modules(self.workspace_slug, project_id)
        return [
            {
                "milestone_id": str(m.get("id", "")),
                "name": m.get("name", ""),
                "description": m.get("description", ""),
                "start_date": m.get("start_date"),
                "target_date": m.get("target_date"),
                "status": m.get("status", ""),
            }
            for m in modules
        ]

    def validate_source(self, source_id: str) -> bool:
        """Check if the project is accessible."""
        if not self.workspace_slug:
            return False
        try:
            project = self.client.get_project(self.workspace_slug, source_id)
            return bool(project and project.get("id"))
        except ImportError:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List available projects in the workspace."""
        if not self.workspace_slug:
            return []
        try:
            result = self.client.request_json(
                f"workspaces/{self.workspace_slug}/projects"
            )
            projects = result if isinstance(result, list) else result.get("results", [])
            return [
                {
                    "id": str(p.get("id", "")),
                    "title": p.get("name", ""),
                    "description": p.get("description", ""),
                }
                for p in projects[:limit]
            ]
        except ImportError:
            return []

    def _map_issue(self, issue: dict[str, Any]) -> dict[str, Any]:
        state_detail = issue.get("state_detail") or {}
        state_name = state_detail.get("name", "")
        assignees = issue.get("assignees") or []
        labels = issue.get("labels") or []
        label_names = [
            lbl.get("name", str(lbl)) if isinstance(lbl, dict) else str(lbl)
            for lbl in labels
        ]
        priority = str(issue.get("priority") or "none").lower()

        return {
            "task_id": str(issue.get("id", "")),
            "title": issue.get("name", ""),
            "description": issue.get("description_stripped") or issue.get("description", ""),
            "status": self.state_map.get(state_name, "pending"),
            "priority": PRIORITY_MAP.get(priority, priority),
            "assignees": assignees,
            "effort": float(issue.get("estimate_point") or 0.0),
            "tags": label_names,
            "target_date": issue.get("target_date"),
            "parent_id": str(issue["parent"]) if issue.get("parent") else None,
            "source": "plane",
        }


__all__ = [
    "DEFAULT_STATE_MAP",
    "PRIORITY_MAP",
    "PlaneClient",
    "PlaneImporter",
]
