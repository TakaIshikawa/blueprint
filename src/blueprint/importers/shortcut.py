"""Shortcut (formerly Clubhouse) story importer."""

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter


HttpOpen = Callable[..., Any]


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class ShortcutStoryRef:
    """Parsed Shortcut story reference."""

    story_id: int

    @property
    def source_id(self) -> str:
        """Return canonical source ID."""
        return f"shortcut-{self.story_id}"


def parse_story_ref(source_id: str) -> ShortcutStoryRef:
    """Parse Shortcut story ID."""
    source_id = source_id.strip()

    # Strip shortcut- prefix if present
    if source_id.lower().startswith("shortcut-"):
        source_id = source_id[9:]

    try:
        story_id = int(source_id)
    except ValueError as exc:
        raise ValueError(f"Invalid Shortcut story ID: {source_id}") from exc

    if story_id < 1:
        raise ValueError(f"Invalid Shortcut story ID: {story_id}")

    return ShortcutStoryRef(story_id=story_id)


def parse_shortcut_story_json(
    story: dict[str, Any],
) -> dict[str, Any]:
    """Normalize Shortcut story JSON into a SourceBrief dictionary."""
    if not isinstance(story, dict):
        raise ValueError("Shortcut story payload must be a mapping")

    title = _required_string(story, "name")
    story_id = _required_int(story, "id")
    source_id = f"shortcut-{story_id}"

    now = datetime.utcnow()
    labels = _extract_labels(story.get("labels", []))
    owners = _extract_owners(story.get("owners", []))
    story_type = story.get("story_type", "feature")
    workflow_state_id = story.get("workflow_state_id")
    estimate = story.get("estimate")
    epic_id = story.get("epic_id")
    iteration_id = story.get("iteration_id")
    project_id = story.get("project_id")
    description = story.get("description") or ""
    blocked = story.get("blocked", False)
    blocker = story.get("blocker", False)
    tasks = _extract_tasks(story.get("tasks", []))
    comments = _extract_comments(story.get("comments", []))

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "shortcut",
        "summary": _create_summary(story),
        "source_project": "shortcut",
        "source_entity_type": story_type,
        "source_id": source_id,
        "source_payload": {
            "story": story,
            "normalized": {
                "story_id": story_id,
                "title": title,
                "story_type": story_type,
                "state": workflow_state_id,
                "estimate": estimate,
                "labels": labels,
                "owners": owners,
                "epic_id": epic_id,
                "iteration_id": iteration_id,
                "project_id": project_id,
                "description": description,
                "blocked": blocked,
                "blocker": blocker,
                "tasks": tasks,
                "comment_count": len(comments),
            },
        },
        "source_links": {
            "app_url": story.get("app_url"),
            "api_url": f"https://api.app.shortcut.com/api/v3/stories/{story_id}",
        },
        "created_at": now,
        "updated_at": now,
    }


class ShortcutImporter(SourceImporter):
    """Import Shortcut stories through the REST API."""

    def __init__(
        self,
        *,
        token_env: str = "SHORTCUT_API_TOKEN",
        api_base: str = "https://api.app.shortcut.com/api/v3",
        http_open: HttpOpen = urlopen,
        project_id: int | None = None,
        workspace: str | None = None,
    ):
        """Initialize importer with optional Shortcut REST API settings."""
        self.token_env = token_env
        self.api_base = api_base.rstrip("/")
        self.http_open = http_open
        self.project_id = project_id
        self.workspace = workspace

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a Shortcut story."""
        story_ref = parse_story_ref(source_id)
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Shortcut API token in environment variable {self.token_env}")

        url = f"{self.api_base}/stories/{story_ref.story_id}"
        request = Request(url, headers={"Shortcut-Token": token})

        try:
            response = self.http_open(request)
            story = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                raise ImportError(f"Shortcut story not found: {story_ref.story_id}") from exc
            raise ImportError(f"Failed to fetch Shortcut story: {exc}") from exc
        except (URLError, ValueError) as exc:
            raise ImportError(f"Failed to fetch Shortcut story: {exc}") from exc

        return parse_shortcut_story_json(story)

    def validate_source(self, source_id: str) -> bool:
        """Check if a Shortcut story exists and is accessible."""
        try:
            self.import_from_source(source_id)
            return True
        except ImportError:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List available Shortcut stories."""
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Shortcut API token in environment variable {self.token_env}")

        # Use search endpoint with filters
        url = f"{self.api_base}/search/stories"
        params: dict[str, Any] = {"page_size": limit}

        # Apply project filter if configured
        if self.project_id is not None:
            params["project_id"] = self.project_id

        query_string = urlencode(params)
        request = Request(f"{url}?{query_string}", headers={"Shortcut-Token": token})

        try:
            response = self.http_open(request)
            result = json.loads(response.read().decode("utf-8"))
            stories = result.get("data", [])

            return [
                {
                    "id": f"shortcut-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "state": story.get("workflow_state_id"),
                        "epic_id": story.get("epic_id"),
                        "iteration_id": story.get("iteration_id"),
                        "project_id": story.get("project_id"),
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(f"Failed to list Shortcut stories: {exc}") from exc

    def list_by_project(self, project_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """List stories by project ID."""
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Shortcut API token in environment variable {self.token_env}")

        url = f"{self.api_base}/projects/{project_id}/stories"
        request = Request(url, headers={"Shortcut-Token": token})

        try:
            response = self.http_open(request)
            stories = json.loads(response.read().decode("utf-8"))

            return [
                {
                    "id": f"shortcut-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "state": story.get("workflow_state_id"),
                        "epic_id": story.get("epic_id"),
                        "iteration_id": story.get("iteration_id"),
                        "project_id": project_id,
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(f"Failed to list Shortcut stories for project {project_id}: {exc}") from exc

    def list_by_epic(self, epic_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """List stories by epic ID."""
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Shortcut API token in environment variable {self.token_env}")

        url = f"{self.api_base}/epics/{epic_id}/stories"
        request = Request(url, headers={"Shortcut-Token": token})

        try:
            response = self.http_open(request)
            stories = json.loads(response.read().decode("utf-8"))

            return [
                {
                    "id": f"shortcut-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "state": story.get("workflow_state_id"),
                        "epic_id": epic_id,
                        "iteration_id": story.get("iteration_id"),
                        "project_id": story.get("project_id"),
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(f"Failed to list Shortcut stories for epic {epic_id}: {exc}") from exc

    def list_by_iteration(self, iteration_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """List stories by iteration ID."""
        token = os.environ.get(self.token_env)
        if not token:
            raise ImportError(f"Missing Shortcut API token in environment variable {self.token_env}")

        url = f"{self.api_base}/iterations/{iteration_id}/stories"
        request = Request(url, headers={"Shortcut-Token": token})

        try:
            response = self.http_open(request)
            stories = json.loads(response.read().decode("utf-8"))

            return [
                {
                    "id": f"shortcut-{story['id']}",
                    "title": story.get("name", ""),
                    "metadata": {
                        "story_id": story["id"],
                        "story_type": story.get("story_type"),
                        "state": story.get("workflow_state_id"),
                        "epic_id": story.get("epic_id"),
                        "iteration_id": iteration_id,
                        "project_id": story.get("project_id"),
                    },
                }
                for story in stories[:limit]
            ]
        except (HTTPError, URLError, ValueError) as exc:
            raise ImportError(f"Failed to list Shortcut stories for iteration {iteration_id}: {exc}") from exc


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
    """Extract label names from Shortcut labels."""
    if not isinstance(labels, list):
        return []
    return [label.get("name", "") for label in labels if isinstance(label, dict)]


def _extract_owners(owners: list[dict[str, Any]]) -> list[str]:
    """Extract owner IDs from Shortcut owners."""
    if not isinstance(owners, list):
        return []
    return [str(owner.get("id", "")) for owner in owners if isinstance(owner, dict)]


def _extract_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract task summaries from Shortcut tasks."""
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


def _extract_comments(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract comment summaries from Shortcut comments."""
    if not isinstance(comments, list):
        return []
    return [
        {
            "text": comment.get("text", ""),
            "author_id": comment.get("author_id", ""),
        }
        for comment in comments
        if isinstance(comment, dict)
    ]


def _create_summary(story: dict[str, Any]) -> str:
    """Create a summary from a Shortcut story."""
    parts = []
    story_type = story.get("story_type", "feature")
    parts.append(f"Shortcut {story_type}")

    if description := story.get("description"):
        # Truncate description to first 200 chars
        desc_preview = description[:200].strip()
        if len(description) > 200:
            desc_preview += "..."
        parts.append(desc_preview)

    return ": ".join(parts)


__all__ = [
    "ShortcutImporter",
    "ShortcutStoryRef",
    "parse_story_ref",
    "parse_shortcut_story_json",
    "generate_source_brief_id",
]
