"""Graph node JSON importer."""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from blueprint.importers.base import SourceImporter


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


def parse_graph_node_json(
    node: dict[str, Any],
    *,
    file_path: str | None = None,
) -> dict[str, Any]:
    """Normalize exported Graph node JSON into a SourceBrief dictionary."""
    if not isinstance(node, dict):
        raise ValueError("Graph node payload must be a mapping")

    source_id = _required_node_id(node)
    title = _string_field(node, "title") or _string_field(node, "name") or source_id
    body = _string_field(node, "body") or _string_field(node, "content") or ""
    summary = _summary(node, body=body, title=title)
    domain = _string_field(node, "domain")
    tags = _tags(node.get("tags"))
    links = node.get("links")
    properties = node.get("properties") if isinstance(node.get("properties"), dict) else {}

    source_links: dict[str, Any] = {}
    if file_path:
        source_links["file_path"] = str(Path(file_path).expanduser())
    if links is not None:
        source_links["links"] = links

    now = datetime.utcnow()
    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "graph",
        "source_entity_type": "node",
        "source_id": source_id,
        "source_payload": {
            "node": node,
            "normalized": {
                "id": source_id,
                "title": title,
                "summary": summary,
                "body": body,
                "domain": domain,
                "tags": tags,
                "links": links,
                "properties": properties,
            },
        },
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


class GraphImporter(SourceImporter):
    """Import exported Graph nodes from JSON files."""

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read and normalize a Graph node JSON file."""
        path = Path(source_id).expanduser()
        try:
            with path.open(encoding="utf-8") as f:
                payload = json.load(f)
        except FileNotFoundError as exc:
            raise ImportError(f"Graph node JSON file not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise ImportError(f"Graph node JSON file is invalid JSON: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read Graph node JSON file: {path}") from exc

        try:
            return parse_graph_node_json(payload, file_path=str(path))
        except ValueError as exc:
            raise ImportError(str(exc)) from exc

    def validate_source(self, source_id: str) -> bool:
        """Check whether a Graph node JSON file is readable and valid."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """Graph node imports are file-based and do not support discovery."""
        return []


def _required_node_id(node: dict[str, Any]) -> str:
    value = node.get("id")
    if value is None:
        raise ValueError("Graph node field 'id' must be set")

    source_id = str(value).strip()
    if not source_id:
        raise ValueError("Graph node field 'id' must be a non-empty value")
    return source_id


def _string_field(node: dict[str, Any], key: str) -> str | None:
    value = node.get(key)
    if isinstance(value, str):
        value = value.strip()
        if value:
            return value
    return None


def _summary(node: dict[str, Any], *, body: str, title: str) -> str:
    summary = _string_field(node, "summary") or _string_field(node, "description")
    if summary:
        return summary
    if body:
        return body
    return title


def _tags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []
