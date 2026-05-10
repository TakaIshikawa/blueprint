"""Wrike API v4 importer for projects and tasks."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


# Status mapping from Wrike to blueprint
DEFAULT_STATUS_MAP: dict[str, str] = {
    "Active": "in_progress",
    "Completed": "completed",
    "Deferred": "pending",
    "Cancelled": "cancelled",
}

# Importance mapping
IMPORTANCE_MAP: dict[str, str] = {
    "High": "high",
    "Normal": "medium",
    "Low": "low",
}


@dataclass
class WrikeClient:
    """Lightweight client for Wrike API v4."""

    api_token: str
    base_url: str = "https://www.wrike.com/api/v4"

    def _headers(self) -> dict[str, str]:
        """Build request headers with authentication."""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def request_json(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """
        Execute a GET request and return parsed JSON.

        Args:
            endpoint: API endpoint path (e.g., "folders", "tasks")
            params: Optional query parameters

        Returns:
            Parsed JSON response

        Raises:
            ImportError: If request fails
        """
        import json
        from urllib.parse import urlencode

        url = f"{self.base_url}/{endpoint}"
        if params:
            url = f"{url}?{urlencode(params)}"

        request = Request(url, headers=self._headers(), method="GET")

        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Wrike API request failed with HTTP {exc.code}: {endpoint}"
            ) from exc
        except URLError as exc:
            raise ImportError(f"Wrike API request failed: {exc.reason}") from exc

        return json.loads(raw)

    def get_folder(self, folder_id: str) -> dict[str, Any]:
        """Get folder details by ID."""
        response = self.request_json(f"folders/{folder_id}")
        return response.get("data", [{}])[0] if response.get("data") else {}

    def get_tasks_in_folder(
        self, folder_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """
        Get tasks from a folder.

        Args:
            folder_id: Wrike folder ID
            limit: Maximum number of tasks to retrieve

        Returns:
            List of task dictionaries
        """
        params = {"descendants": "true", "pageSize": str(limit)}
        response = self.request_json(f"folders/{folder_id}/tasks", params)
        return response.get("data", [])

    def get_task(self, task_id: str) -> dict[str, Any]:
        """Get task details by ID."""
        response = self.request_json(f"tasks/{task_id}")
        return response.get("data", [{}])[0] if response.get("data") else {}

    def get_custom_fields(self) -> list[dict[str, Any]]:
        """Get account custom field definitions."""
        response = self.request_json("customfields")
        return response.get("data", [])

    def get_time_logs(self, task_id: str) -> list[dict[str, Any]]:
        """Get time logs for a task."""
        response = self.request_json(f"tasks/{task_id}/timelogs")
        return response.get("data", [])


class WrikeImporter(SourceImporter):
    """
    Import tasks and projects from Wrike.

    Supports:
    - Task import with all attributes
    - Custom field mapping
    - Dependency and hierarchy import
    - Time tracking data
    - Multi-folder import
    - Incremental sync based on updatedDate
    """

    def __init__(
        self,
        api_token: str | None = None,
        status_map: dict[str, str] | None = None,
        importance_map: dict[str, str] | None = None,
    ):
        """
        Initialize Wrike importer.

        Args:
            api_token: Wrike permanent access token (or set WRIKE_API_TOKEN env var)
            status_map: Custom status mapping (defaults to DEFAULT_STATUS_MAP)
            importance_map: Custom importance mapping (defaults to IMPORTANCE_MAP)
        """
        self.api_token = api_token or os.environ.get("WRIKE_API_TOKEN")
        if not self.api_token:
            raise ValueError(
                "API token required: pass api_token parameter or set WRIKE_API_TOKEN environment variable"
            )

        self.client = WrikeClient(api_token=self.api_token)
        self.status_map = status_map or DEFAULT_STATUS_MAP
        self.importance_map = importance_map or IMPORTANCE_MAP
        self._custom_fields_cache: list[dict[str, Any]] | None = None

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """
        Import tasks from a Wrike folder.

        Args:
            source_id: Wrike folder ID

        Returns:
            Dictionary representing a SourceBrief with tasks

        Raises:
            ImportError: If folder cannot be found or read
        """
        folder = self.client.get_folder(source_id)
        if not folder:
            raise ImportError(f"Wrike folder not found: {source_id}")

        tasks = self.client.get_tasks_in_folder(source_id)
        blueprint_tasks = [self._map_task_to_blueprint(task) for task in tasks]

        return {
            "id": generate_source_brief_id(),
            "source_type": "wrike",
            "source_id": source_id,
            "title": folder.get("title", "Imported from Wrike"),
            "description": folder.get("description", ""),
            "tasks": blueprint_tasks,
            "imported_at": datetime.now().isoformat(),
            "metadata": {
                "wrike_folder_id": source_id,
                "wrike_folder_title": folder.get("title"),
                "task_count": len(blueprint_tasks),
            },
        }

    def import_tasks(
        self,
        folder_ids: list[str],
        include_time_logs: bool = False,
    ) -> dict[str, Any]:
        """
        Import tasks from multiple Wrike folders.

        Args:
            folder_ids: List of Wrike folder IDs to import from
            include_time_logs: Whether to include time tracking data

        Returns:
            Dictionary with tasks from all folders
        """
        all_tasks = []
        folder_metadata = []

        for folder_id in folder_ids:
            folder = self.client.get_folder(folder_id)
            tasks = self.client.get_tasks_in_folder(folder_id)

            for task in tasks:
                blueprint_task = self._map_task_to_blueprint(task)

                # Add time logs if requested
                if include_time_logs:
                    time_logs = self.client.get_time_logs(task["id"])
                    blueprint_task["metadata"]["time_logs"] = [
                        self._map_time_log(log) for log in time_logs
                    ]

                all_tasks.append(blueprint_task)

            folder_metadata.append(
                {
                    "folder_id": folder_id,
                    "folder_title": folder.get("title"),
                    "task_count": len(tasks),
                }
            )

        return {
            "id": generate_source_brief_id(),
            "source_type": "wrike",
            "title": f"Imported from {len(folder_ids)} Wrike folder(s)",
            "tasks": all_tasks,
            "imported_at": datetime.now().isoformat(),
            "metadata": {
                "folders": folder_metadata,
                "total_task_count": len(all_tasks),
            },
        }

    def validate_source(self, source_id: str) -> bool:
        """
        Check if a Wrike folder exists and is accessible.

        Args:
            source_id: Wrike folder ID

        Returns:
            True if folder exists and can be imported
        """
        try:
            folder = self.client.get_folder(source_id)
            return bool(folder)
        except ImportError:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        List available Wrike folders.

        Args:
            limit: Maximum number of folders to return

        Returns:
            List of dictionaries with id, title, and metadata
        """
        # Note: This requires additional permissions and API calls
        # For now, returning empty list as Wrike folder listing
        # requires specific folder hierarchy navigation
        return []

    def sync_incremental(
        self, folder_id: str, since: datetime
    ) -> dict[str, Any]:
        """
        Sync tasks updated since a specific date.

        Args:
            folder_id: Wrike folder ID
            since: Only include tasks updated after this datetime

        Returns:
            Dictionary with updated tasks
        """
        tasks = self.client.get_tasks_in_folder(folder_id)

        # Filter by updatedDate
        updated_tasks = []
        # Ensure since has timezone info
        if since.tzinfo is None:
            from datetime import timezone
            since = since.replace(tzinfo=timezone.utc)

        for task in tasks:
            updated_date_str = task.get("updatedDate")
            if updated_date_str:
                updated_date = datetime.fromisoformat(
                    updated_date_str.replace("Z", "+00:00")
                )
                if updated_date > since:
                    updated_tasks.append(self._map_task_to_blueprint(task))

        return {
            "id": generate_source_brief_id(),
            "source_type": "wrike",
            "source_id": folder_id,
            "title": f"Incremental sync from Wrike (since {since.isoformat()})",
            "tasks": updated_tasks,
            "imported_at": datetime.now().isoformat(),
            "metadata": {
                "sync_since": since.isoformat(),
                "updated_task_count": len(updated_tasks),
            },
        }

    def _map_task_to_blueprint(self, task: dict[str, Any]) -> dict[str, Any]:
        """
        Map a Wrike task to blueprint task format.

        Args:
            task: Wrike task dictionary

        Returns:
            Blueprint task dictionary
        """
        # Extract basic fields
        task_id = task.get("id", "")
        title = task.get("title", "Untitled Task")
        description = task.get("description", "")
        status = task.get("status", "Active")
        importance = task.get("importance", "Normal")

        # Map status
        blueprint_status = self.status_map.get(status, "pending")

        # Map importance to priority
        priority = self.importance_map.get(importance, "medium")

        # Extract dates
        created_date = task.get("createdDate")
        updated_date = task.get("updatedDate")
        start_date = task.get("dates", {}).get("start")
        due_date = task.get("dates", {}).get("due")

        # Extract assignees
        assignees = [
            assignee_id for assignee_id in task.get("responsibleIds", [])
        ]

        # Extract parent/dependencies
        parent_ids = task.get("superParentIds", [])
        dependency_ids = task.get("dependencyIds", [])

        # Extract custom fields
        custom_fields = self._extract_custom_fields(task)

        # Build blueprint task
        blueprint_task = {
            "id": f"wrike-{task_id}",
            "title": title,
            "description": description,
            "status": blueprint_status,
            "priority": priority,
            "created_at": created_date,
            "updated_at": updated_date,
            "start_date": start_date,
            "due_date": due_date,
            "assignees": assignees,
            "depends_on": [f"wrike-{dep_id}" for dep_id in dependency_ids],
            "parent_ids": [f"wrike-{parent_id}" for parent_id in parent_ids],
            "metadata": {
                "wrike_id": task_id,
                "wrike_permalink": task.get("permalink"),
                "wrike_status": status,
                "wrike_importance": importance,
                "custom_fields": custom_fields,
            },
        }

        return blueprint_task

    def _extract_custom_fields(self, task: dict[str, Any]) -> dict[str, Any]:
        """
        Extract and map custom fields from task.

        Args:
            task: Wrike task dictionary

        Returns:
            Dictionary of custom field values
        """
        custom_fields = {}
        custom_field_data = task.get("customFields", [])

        # Get custom field definitions (cached)
        if self._custom_fields_cache is None:
            try:
                self._custom_fields_cache = self.client.get_custom_fields()
            except ImportError:
                self._custom_fields_cache = []

        # Map custom field IDs to names
        field_id_to_name = {
            field["id"]: field.get("title", field["id"])
            for field in self._custom_fields_cache
        }

        for cf in custom_field_data:
            field_id = cf.get("id")
            value = cf.get("value")

            if field_id and value is not None:
                field_name = field_id_to_name.get(field_id, field_id)
                custom_fields[field_name] = value

        return custom_fields

    def _map_time_log(self, time_log: dict[str, Any]) -> dict[str, Any]:
        """
        Map a Wrike time log entry to blueprint format.

        Args:
            time_log: Wrike time log dictionary

        Returns:
            Blueprint time log dictionary
        """
        return {
            "user_id": time_log.get("userId"),
            "hours": time_log.get("hours", 0),
            "tracked_date": time_log.get("trackedDate"),
            "comment": time_log.get("comment", ""),
            "created_at": time_log.get("createdDate"),
        }


__all__ = ["WrikeImporter", "WrikeClient"]
