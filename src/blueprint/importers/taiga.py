"""Taiga REST API importer for epics and user stories."""

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
# Status mapping
# ---------------------------------------------------------------------------

DEFAULT_STATUS_MAP: dict[str, str] = {
    "New": "pending",
    "Ready": "pending",
    "In progress": "in_progress",
    "Ready for test": "in_progress",
    "Done": "completed",
    "Archived": "completed",
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


@dataclass
class TaigaClient:
    """Lightweight client for Taiga REST API."""

    base_url: str
    auth_token: str

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.auth_token}",
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
        url = f"{self.base_url}/api/v1/{endpoint}"
        if params:
            url = f"{url}?{urlencode(params)}"

        request = Request(url, headers=self._headers(), method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Taiga API request failed with HTTP {exc.code}: {endpoint}"
            ) from exc
        except URLError as exc:
            raise ImportError(f"Taiga API request failed: {exc.reason}") from exc

        return json.loads(raw)

    def authenticate(self, username: str, password: str) -> str:
        """Authenticate with username/password and return auth token.

        Args:
            username: Taiga username.
            password: Taiga password.

        Returns:
            Authentication token string.

        Raises:
            ImportError: If authentication fails.
        """
        url = f"{self.base_url}/api/v1/auth"
        payload = json.dumps({
            "type": "normal",
            "username": username,
            "password": password,
        }).encode()
        request = Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                data = json.loads(response.read())
        except (HTTPError, URLError) as exc:
            raise ImportError(f"Taiga authentication failed: {exc}") from exc

        token = data.get("auth_token", "")
        self.auth_token = token
        return token

    def get_project(self, project_id: str) -> dict[str, Any]:
        """Get project details."""
        return self.request_json(f"projects/{project_id}")

    def get_user_stories(
        self,
        project_id: str,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get user stories from a project."""
        params: dict[str, Any] = {"project": project_id}
        if status:
            params["status__name"] = status
        result = self.request_json("userstories", params)
        return result if isinstance(result, list) else []

    def get_epics(self, project_id: str) -> list[dict[str, Any]]:
        """Get epics for a project."""
        result = self.request_json("epics", {"project": project_id})
        return result if isinstance(result, list) else []

    def get_epic_related_stories(self, epic_id: str) -> list[dict[str, Any]]:
        """Get user stories related to an epic."""
        result = self.request_json(f"epics/{epic_id}/related_userstories")
        return result if isinstance(result, list) else []

    def get_milestones(self, project_id: str) -> list[dict[str, Any]]:
        """Get milestones (sprints) for a project."""
        result = self.request_json("milestones", {"project": project_id})
        return result if isinstance(result, list) else []


# ---------------------------------------------------------------------------
# Importer
# ---------------------------------------------------------------------------


class TaigaImporter(SourceImporter):
    """Import epics and user stories from Taiga.

    Supports:
    - Token authentication and username/password auth
    - User story attributes mapped to blueprint tasks
    - Epic-story hierarchy as task structure
    - Sprint/milestone mapping to blueprint phases
    - Story point estimates for capacity planning
    - Tags preservation
    - Incremental sync by modified_date
    """

    def __init__(
        self,
        base_url: str | None = None,
        auth_token: str | None = None,
        status_map: dict[str, str] | None = None,
    ):
        self.base_url = base_url or os.environ.get("TAIGA_BASE_URL", "https://api.taiga.io")
        self.auth_token = auth_token or os.environ.get("TAIGA_AUTH_TOKEN", "")
        if not self.auth_token:
            raise ValueError(
                "Auth token required: pass auth_token or set TAIGA_AUTH_TOKEN env var"
            )
        self.client = TaigaClient(base_url=self.base_url, auth_token=self.auth_token)
        self.status_map = status_map or DEFAULT_STATUS_MAP

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Import user stories from a Taiga project.

        Args:
            source_id: Taiga project ID.

        Returns:
            Dictionary representing a SourceBrief with tasks.
        """
        project = self.client.get_project(source_id)
        if not project:
            raise ImportError(f"Taiga project not found: {source_id}")

        stories = self.client.get_user_stories(source_id)
        tasks = [self._map_story(s) for s in stories]

        return {
            "id": generate_source_brief_id(),
            "source_type": "taiga",
            "source_id": source_id,
            "title": project.get("name", f"Taiga project {source_id}"),
            "description": project.get("description", ""),
            "tasks": tasks,
            "imported_at": datetime.now().isoformat(),
            "metadata": {
                "taiga_project_id": source_id,
                "story_count": len(tasks),
            },
        }

    def import_stories(
        self,
        project_id: str,
        status_filter: str | None = None,
        modified_since: str | None = None,
    ) -> list[dict[str, Any]]:
        """Import user stories with optional filters.

        Args:
            project_id: Taiga project ID.
            status_filter: Optional status name to filter by.
            modified_since: Optional ISO date string for incremental sync.

        Returns:
            List of blueprint task dictionaries.
        """
        stories = self.client.get_user_stories(project_id, status=status_filter)
        if modified_since:
            stories = [
                s for s in stories
                if s.get("modified_date", "") >= modified_since
            ]
        return [self._map_story(s) for s in stories]

    def import_epic_hierarchy(self, project_id: str) -> list[dict[str, Any]]:
        """Import epics with their related user stories.

        Args:
            project_id: Taiga project ID.

        Returns:
            List of epic task dicts, each with a ``subtasks`` list.
        """
        epics = self.client.get_epics(project_id)
        result: list[dict[str, Any]] = []
        for epic in epics:
            epic_task = self._map_epic(epic)
            related = self.client.get_epic_related_stories(str(epic.get("id", "")))
            epic_task["subtasks"] = [
                self._map_story(rs.get("user_story", rs)) for rs in related
            ]
            result.append(epic_task)
        return result

    def import_milestones(self, project_id: str) -> list[dict[str, Any]]:
        """Import milestones as blueprint phases.

        Args:
            project_id: Taiga project ID.

        Returns:
            List of phase dictionaries with dates.
        """
        milestones = self.client.get_milestones(project_id)
        return [
            {
                "phase_id": str(m.get("id", "")),
                "name": m.get("name", ""),
                "start_date": m.get("estimated_start"),
                "end_date": m.get("estimated_finish"),
                "closed": m.get("closed", False),
            }
            for m in milestones
        ]

    def validate_source(self, source_id: str) -> bool:
        """Check if the project is accessible."""
        try:
            project = self.client.get_project(source_id)
            return bool(project and project.get("id"))
        except ImportError:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List available projects (requires project listing endpoint)."""
        try:
            projects = self.client.request_json("projects")
            if not isinstance(projects, list):
                return []
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

    def _map_story(self, story: dict[str, Any]) -> dict[str, Any]:
        status_name = story.get("status_extra_info", {}).get("name", "")
        assigned = story.get("assigned_to_extra_info") or {}
        tags = [t[0] if isinstance(t, list) else t for t in (story.get("tags") or [])]
        return {
            "task_id": str(story.get("id", "")),
            "title": story.get("subject", ""),
            "description": story.get("description", ""),
            "status": self.status_map.get(status_name, "pending"),
            "assignee": assigned.get("full_name_display") or assigned.get("username"),
            "effort": float(story.get("total_points") or 0.0),
            "tags": tags,
            "due_date": story.get("finish_date"),
            "milestone_id": str(story.get("milestone", "")) if story.get("milestone") else None,
            "source": "taiga",
        }

    def _map_epic(self, epic: dict[str, Any]) -> dict[str, Any]:
        status_name = epic.get("status_extra_info", {}).get("name", "")
        assigned = epic.get("assigned_to_extra_info") or {}
        tags = [t[0] if isinstance(t, list) else t for t in (epic.get("tags") or [])]
        return {
            "task_id": str(epic.get("id", "")),
            "title": epic.get("subject", ""),
            "description": epic.get("description", ""),
            "status": self.status_map.get(status_name, "pending"),
            "assignee": assigned.get("full_name_display") or assigned.get("username"),
            "tags": tags,
            "type": "epic",
            "source": "taiga",
            "subtasks": [],
        }


__all__ = [
    "DEFAULT_STATUS_MAP",
    "TaigaClient",
    "TaigaImporter",
]
