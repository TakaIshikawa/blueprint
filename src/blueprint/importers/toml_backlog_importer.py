"""TOML backlog importer."""

from __future__ import annotations

import hashlib
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any

from blueprint.importers.base import SourceImporter


BACKLOG_COLLECTIONS = ("briefs", "items")
DEFAULT_SOURCE_PROJECT = "toml-backlog"


class TomlBacklogImporter(SourceImporter):
    """Import backlog records from project-local TOML files."""

    def __init__(self, source_path: str | Path | None = None) -> None:
        self.source_path = Path(source_path).expanduser() if source_path is not None else None

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read a TOML backlog file and return the first normalized SourceBrief."""
        source_briefs = self.import_file(source_id)
        if not source_briefs:
            raise ImportError(f"TOML backlog has no importable records: {source_id}")
        return source_briefs[0]

    def import_file(self, source_id: str | Path) -> list[dict[str, Any]]:
        """Read and normalize all backlog records from a TOML file."""
        path = Path(source_id).expanduser()
        payload = _load_toml(path)
        resolved_path = str(path.resolve())

        source_briefs: list[dict[str, Any]] = []
        for collection, record, index in _iter_records(payload):
            try:
                source_briefs.append(
                    parse_toml_backlog_record(
                        record,
                        collection=collection,
                        index=index,
                        file_path=resolved_path,
                    )
                )
            except ValueError as exc:
                raise ImportError(str(exc)) from exc
        return source_briefs

    def validate_source(self, source_id: str) -> bool:
        """Check whether a TOML backlog file exists, parses, and has valid records."""
        try:
            self.import_file(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List records from the configured TOML backlog file."""
        if limit <= 0 or self.source_path is None:
            return []

        try:
            source_briefs = self.import_file(self.source_path)
        except Exception:
            return []

        available: list[dict[str, Any]] = []
        for source_brief in source_briefs[:limit]:
            payload = source_brief["source_payload"]
            available.append(
                {
                    "id": source_brief["source_id"],
                    "title": source_brief["title"],
                    "metadata": {
                        "domain": source_brief.get("domain"),
                        "source_project": source_brief["source_project"],
                        "source_entity_type": source_brief["source_entity_type"],
                        "file_path": source_brief["source_links"].get("file_path"),
                        "collection": payload["collection"],
                        "index": payload["index"],
                    },
                }
            )
        return available


def parse_toml_backlog_record(
    record: dict[str, Any],
    *,
    collection: str,
    index: int,
    file_path: str | None = None,
) -> dict[str, Any]:
    """Normalize one TOML backlog record into a SourceBrief dictionary."""
    if not isinstance(record, dict):
        raise ValueError(f"TOML {collection}[{index}] record must be a table")

    title = _first_text(record, "title", "name")
    if not title:
        raise ValueError(f"TOML {collection}[{index}] record must include a non-empty title")

    summary = _first_text(record, "summary", "description", "body", "content") or title
    domain = _optional_text(record.get("domain"))
    source_project = _optional_text(record.get("source_project")) or DEFAULT_SOURCE_PROJECT
    source_entity_type = _optional_text(record.get("source_entity_type")) or collection[:-1]
    source_id = (
        _first_text(record, "source_id", "id", "key")
        or f"{collection}-{index}"
    )
    links = _links(record.get("links"))
    tags = _string_list(record.get("tags"))

    source_links: dict[str, Any] = {
        "collection": collection,
        "index": index,
    }
    if file_path:
        source_links["file_path"] = file_path
    if links:
        source_links["links"] = links

    normalized = {
        "title": title,
        "summary": summary,
        "domain": domain,
        "source_project": source_project,
        "source_entity_type": source_entity_type,
        "source_id": source_id,
        "links": links,
        "tags": tags,
    }

    now = datetime.utcnow()
    return {
        "id": _stable_source_brief_id(source_project, source_entity_type, source_id),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": source_project,
        "source_entity_type": source_entity_type,
        "source_id": source_id,
        "source_payload": {
            "record": dict(record),
            "collection": collection,
            "index": index,
            "normalized": normalized,
        },
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as toml_file:
            payload = tomllib.load(toml_file)
    except FileNotFoundError as exc:
        raise ImportError(f"TOML backlog file not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ImportError(f"TOML backlog file is invalid TOML: {path}") from exc
    except OSError as exc:
        raise ImportError(f"Could not read TOML backlog file: {path}") from exc

    if not isinstance(payload, dict):
        raise ImportError(f"TOML backlog root must be a table: {path}")
    return payload


def _iter_records(payload: dict[str, Any]) -> list[tuple[str, dict[str, Any], int]]:
    records: list[tuple[str, dict[str, Any], int]] = []
    for collection in BACKLOG_COLLECTIONS:
        value = payload.get(collection)
        if value is None:
            continue
        if not isinstance(value, list):
            raise ImportError(f"TOML backlog '{collection}' must be an array of tables")
        for index, record in enumerate(value, start=1):
            if not isinstance(record, dict):
                raise ImportError(f"TOML {collection}[{index}] record must be a table")
            records.append((collection, record, index))
    return records


def _stable_source_brief_id(
    source_project: str,
    source_entity_type: str,
    source_id: str,
) -> str:
    digest = hashlib.sha1(
        f"{source_project}:{source_entity_type}:{source_id}".encode("utf-8")
    ).hexdigest()
    return f"sb-toml-{digest[:12]}"


def _first_text(record: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        text = _optional_text(record.get(key))
        if text:
            return text
    return None


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    return None


def _links(value: Any) -> list[Any]:
    if isinstance(value, list):
        return [item for item in value if item]
    if isinstance(value, str):
        return [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
    return []


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
    return []


__all__ = [
    "TomlBacklogImporter",
    "parse_toml_backlog_record",
]
