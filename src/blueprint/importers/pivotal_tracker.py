"""Pivotal Tracker story importer."""

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter


HttpOpen = Callable[..., Any]


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class PivotalTrackerStoryRef:
    """Parsed Pivotal Tracker story reference."""

    project_id: int
    story_id: int

    @property
    def source_id(self) -> str:
        """Return canonical source ID."""
        return f"pivotal-{self.project_id}-{self.story_id}"


def parse_story_ref(source_id: str, default_project_id: int | None = None) -> PivotalTrackerStoryRef:
    """Parse Pivotal Tracker story ID with optional project."""
    source_id = source_id.strip()

    # Strip pivotal- prefix if present
    if source_id.lower().startswith("pivotal-"):
        source_id = source_id[8:]

    # Parse PROJECT-STORY or just STORY
    if "-" in source_id:
        project_part, story_part = source_id.split("-", 1)
        try:
            project_id = int(project_part)
            story_id = int(story_part)
        except ValueError as exc:
            raise ValueError(f"Invalid Pivotal Tracker story reference: {source_id}") from exc
    else:
        if default_project_id is None:
            raise ValueError("Pivotal Tracker story reference requires project ID")
        project_id = default_project_id
        try:
            story_id = int(source_id)
        except ValueError as exc:
            raise ValueError(f"Invalid Pivotal Tracker story ID: {source_id}") from exc

    if project_id < 1 or story_id < 1:
        raise ValueError(f"Invalid Pivotal Tracker story reference: project={project_id}, story={story_id}")

    return PivotalTrackerStoryRef(project_id=project_id, story_id=story_id)


def parse_pivotal_tracker_story_json(
    story: dict[str, Any],
    project_id: int | None = None,
) -> dict[str, Any]:
    """Normalize Pivotal Tracker story JSON into a SourceBrief dictionary."""
    if not isinstance(story, dict):
        raise ValueError("Pivotal Tracker story payload must be a mapping")

    title = _required_string(story, "name")
    story_id = _required_int(story, "id")

    # Extract project_id from story or use provided
    story_project_id = story.get("project_id")
    if story_project_id is not None:
        project_id = story_project_id
    if project_id is None:
        raise ValueError("Pivotal Tracker story must include project_id")

    source_id = f"pivotal-{project_id}-{story_id}"

    now = datetime.utcnow()
    labels = _extract_labels(story.get("labels", []))
    owners = _extract_owner_ids(story.get("owner_ids", []))
    story_type = story.get("story_type", "feature")
    current_state = story.get("current_state")
    estimate = story.get("estimate")
    description = story.get("description") or ""
    blockers = _extract_blockers(story.get("blockers", []))
    tasks = _extract_tasks(story.get("tasks", []))
    comments = story.get("comments", [])
    epic_id = _extract_epic_id(story)

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "pivotal_tracker",
        "summary": _create_summary(story),
        "source_project": "pivotal_tracker",
        "source_entity_type": story_type,
        "source_id": source_id,
        "source_payload": {
            "story": story,
            "normalized": {
                "project_id": project_id,
                "story_id": story_id,
                "title": title,
                "story_type": story_type,
                "current_state": current_state,
                "estimate": estimate,
                "labels": labels,
                "owner_ids": owners,
                "epic_id": epic_id,
                "description": description,
                "blockers": blockers,
                "tasks": tasks,
                "comment_count": len(comments) if isinstance(comments, list) else 0,
            },
        },
        "source_links": {
            "url": story.get("url"),
        },
        "created_at": now,
        "updated_at": now,
    }


class PivotalTrackerImporter(SourceImporter):
    """Import Pivotal Tracker stories through the API v5."""

    def __init__(
        self,
        *,
        token_env: str = "PIVOTAL_TRACKER_API_TOKEN",
        api_base: str = "https://www.pivotaltracker.com/services/v5",
        http_open: HttpOpen = urlopen,
        project_id: int | None = None,
    ):
        """Initialize importer with optional Pivotal Tracker API v5 settings."""
        self.token_env = token_env
        self.api_base = api_base.rstrip("/")
        self.http_open = http_open
        self.project_id = project_id

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a Pivotal Tracker story."""
        story_ref = parse_story_ref(source_id, default_project_id=self.project_id)
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Pivotal Tracker API token in environment variable {self.token_env}")

        url = f"{self.api_base}/projects/{story_ref.project_id}/stories/{story_ref.story_id}"
        request = Request(url, headers={"X-TrackerToken": token})

        try:
            response = self.http_open(request)
            story = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                raise ImportError(
                    f"Pivotal Tracker story not found: project={story_ref.project_id}, story={story_ref.story_id}"
                ) from exc
            raise ImportError(f"Failed to fetch Pivotal Tracker story: {exc}") from exc
        except (URLError, ValueError) as exc:
            raise ImportError(f"Failed to fetch Pivotal Tracker story: {exc}") from exc

        return parse_pivotal_tracker_story_json(story, project_id=story_ref.project_id)

    def validate_source(self, source_id: str) -> bool:
        """Check if a Pivotal Tracker story exists and is accessible."""
        try:
            self.import_from_source(source_id)
            return True
        except ImportError:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List available Pivotal Tracker stories."""
        if self.project_id is None:
            raise ImportError("Project ID must be configured to list stories")

        return self.list_by_project(self.project_id, limit=limit)

    def list_by_project(self, project_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """List stories by project ID."""
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Pivotal Tracker API token in environment variable {self.token_env}")

        params = {"limit": limit}
        query_string = urlencode(params)
        url = f"{self.api_base}/projects/{project_id}/stories?{query_string}"
        request = Request(url, headers={"X-TrackerToken": token})

        try:
            response = self.http_open(request)
            stories = json.loads(response.read().decode("utf-8"))

            return [
                {
                    "id": f"pivotal-{project_id}-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "project_id": project_id,
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "current_state": story.get("current_state"),
                        "estimate": story.get("estimate"),
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(f"Failed to list Pivotal Tracker stories for project {project_id}: {exc}") from exc

    def list_by_filter(self, project_id: int, filter_query: str, limit: int = 50) -> list[dict[str, Any]]:
        """List stories by filter query.

        Filter query examples:
        - 'label:backend'
        - 'state:started'
        - 'type:bug'
        - 'owner:username'
        """
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Pivotal Tracker API token in environment variable {self.token_env}")

        params = {"filter": filter_query, "limit": limit}
        query_string = urlencode(params)
        url = f"{self.api_base}/projects/{project_id}/stories?{query_string}"
        request = Request(url, headers={"X-TrackerToken": token})

        try:
            response = self.http_open(request)
            stories = json.loads(response.read().decode("utf-8"))

            return [
                {
                    "id": f"pivotal-{project_id}-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "project_id": project_id,
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "current_state": story.get("current_state"),
                        "estimate": story.get("estimate"),
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(
                f"Failed to list Pivotal Tracker stories with filter '{filter_query}': {exc}"
            ) from exc


def _required_string(mapping: dict[str, Any], key: str) -> str:
    """Extract a required string field."""
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Missing or empty required field: {key}")
    return value


def _required_int(mapping: dict[str, Any], key: str) -> int:
    """Extract a required integer field."""
    value = mapping.get(key)
    if not isinstance(value, int):
        raise ValueError(f"Missing or invalid required field: {key}")
    return value


def _extract_labels(labels: list[dict[str, Any]]) -> list[str]:
    """Extract label names from Pivotal Tracker labels."""
    if not isinstance(labels, list):
        return []
    return [label.get("name", "") for label in labels if isinstance(label, dict)]


def _extract_owner_ids(owner_ids: list[int]) -> list[int]:
    """Extract owner IDs from Pivotal Tracker owners."""
    if not isinstance(owner_ids, list):
        return []
    return [owner_id for owner_id in owner_ids if isinstance(owner_id, int)]


def _extract_blockers(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract blocker summaries from Pivotal Tracker blockers."""
    if not isinstance(blockers, list):
        return []
    return [
        {
            "description": blocker.get("description", ""),
            "resolved": blocker.get("resolved", False),
        }
        for blocker in blockers
        if isinstance(blocker, dict)
    ]


def _extract_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract task summaries from Pivotal Tracker tasks."""
    if not isinstance(tasks, list):
        return []
    return [
        {
            "description": task.get("description", ""),
            "complete": task.get("complete", False),
        }
        for task in tasks
        if isinstance(task, dict)
    ]


def _extract_epic_id(story: dict[str, Any]) -> int | None:
    """Extract epic ID from story labels or direct epic reference."""
    # Check for direct epic_id field (not standard but some integrations use it)
    if "epic_id" in story:
        epic_id = story["epic_id"]
        if isinstance(epic_id, int):
            return epic_id

    # Pivotal Tracker doesn't have a direct epic_id field in stories
    # Epics are identified through labels with epic prefix
    # This is a simplified extraction
    return None


def _create_summary(story: dict[str, Any]) -> str:
    """Create a summary from a Pivotal Tracker story."""
    parts = []
    story_type = story.get("story_type", "feature")
    current_state = story.get("current_state", "")

    parts.append(f"Pivotal Tracker {story_type}")
    if current_state:
        parts.append(f"({current_state})")

    if description := story.get("description"):
        # Truncate description to first 200 chars
        desc_preview = description[:200].strip()
        if len(description) > 200:
            desc_preview += "..."
        parts.append(desc_preview)

    return " ".join(parts)


__all__ = [
    "PivotalTrackerImporter",
    "PivotalTrackerStoryRef",
    "parse_story_ref",
    "parse_pivotal_tracker_story_json",
    "generate_source_brief_id",
]
