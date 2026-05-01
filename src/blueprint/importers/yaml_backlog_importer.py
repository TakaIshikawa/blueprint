"""YAML backlog importer."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from blueprint.importers.base import SourceImporter


BACKLOG_COLLECTIONS = ("briefs", "items", "tasks", "backlog")
DEFAULT_SOURCE_PROJECT = "yaml-backlog"


class YamlBacklogImporter(SourceImporter):
    """Import backlog records from YAML files."""

    def __init__(self, source_path: str | Path | None = None) -> None:
        self.source_path = Path(source_path).expanduser() if source_path is not None else None

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read a YAML backlog file and return the first normalized SourceBrief."""
        source_briefs = self.import_file(source_id)
        if not source_briefs:
            raise ImportError(f"YAML backlog has no importable records: {source_id}")
        return source_briefs[0]

    def import_file(self, source_id: str | Path) -> list[dict[str, Any]]:
        """Read and normalize all backlog records from a YAML file."""
        path = Path(source_id).expanduser()
        payload = _load_yaml(path)
        resolved_path = str(path.resolve())

        source_briefs: list[dict[str, Any]] = []
        for collection, record, index in _iter_records(payload):
            try:
                source_briefs.append(
                    parse_yaml_backlog_record(
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
        """Check whether a YAML backlog file exists, parses, and has valid records."""
        try:
            self.import_file(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List records from the configured YAML backlog file."""
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


def parse_yaml_backlog_record(
    record: dict[str, Any],
    *,
    collection: str,
    index: int,
    file_path: str | None = None,
) -> dict[str, Any]:
    """Normalize one YAML backlog record into a SourceBrief dictionary."""
    if not isinstance(record, dict):
        raise ValueError(f"YAML {collection}[{index}] record must be a mapping")

    title = _text(record.get("title"))
    if not title:
        raise ValueError(f"YAML {collection}[{index}] record must include a non-empty title")

    summary = _text(record.get("summary"))
    if not summary:
        raise ValueError(f"YAML {collection}[{index}] record must include a non-empty summary")

    domain = _text(record.get("domain"))
    source_project = _text(record.get("source_project")) or DEFAULT_SOURCE_PROJECT
    source_entity_type = _text(record.get("source_entity_type")) or _entity_type(collection)
    source_id = _first_text(record, "source_id", "id", "key") or f"{collection}-{index}"
    scope = _string_list(record.get("scope"))
    non_goals = _string_list(record.get("non_goals"))
    assumptions = _string_list(record.get("assumptions"))
    risks = _string_list(record.get("risks"))
    acceptance_criteria = _string_list(record.get("acceptance_criteria"))
    labels = _string_list(record.get("labels"))
    owner = _text(record.get("owner"))
    priority = _text(record.get("priority"))
    links = _links(record.get("links"))

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
        "scope": scope,
        "non_goals": non_goals,
        "assumptions": assumptions,
        "risks": risks,
        "acceptance_criteria": acceptance_criteria,
        "labels": labels,
        "owner": owner,
        "priority": priority,
        "links": links,
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


def _load_yaml(path: Path) -> Any:
    try:
        with path.open(encoding="utf-8") as yaml_file:
            payload = yaml.safe_load(yaml_file)
    except FileNotFoundError as exc:
        raise ImportError(f"YAML backlog file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ImportError(f"YAML backlog file is invalid YAML: {path}") from exc
    except OSError as exc:
        raise ImportError(f"Could not read YAML backlog file: {path}") from exc

    if payload is None:
        raise ImportError(f"YAML backlog file is empty: {path}")
    return payload


def _iter_records(payload: Any) -> list[tuple[str, dict[str, Any], int]]:
    if isinstance(payload, list):
        return _records_from_list("records", payload)

    if not isinstance(payload, dict):
        raise ImportError("YAML backlog root must be a mapping or a list of records")

    if _looks_like_record(payload):
        return [("record", payload, 1)]

    records: list[tuple[str, dict[str, Any], int]] = []
    for collection in BACKLOG_COLLECTIONS:
        value = payload.get(collection)
        if value is None:
            continue
        if isinstance(value, dict) and _looks_like_record(value):
            records.append((collection, value, 1))
            continue
        if not isinstance(value, list):
            raise ImportError(f"YAML backlog '{collection}' must be a list of records")
        records.extend(_records_from_list(collection, value))

    if not records:
        raise ImportError(
            "YAML backlog must include a single record or one of: "
            f"{', '.join(BACKLOG_COLLECTIONS)}"
        )
    return records


def _records_from_list(collection: str, value: list[Any]) -> list[tuple[str, dict[str, Any], int]]:
    records: list[tuple[str, dict[str, Any], int]] = []
    for index, record in enumerate(value, start=1):
        if not isinstance(record, dict):
            raise ImportError(f"YAML {collection}[{index}] record must be a mapping")
        records.append((collection, record, index))
    return records


def _looks_like_record(value: dict[str, Any]) -> bool:
    return any(key in value for key in ("title", "summary", "source_id", "id", "key"))


def _stable_source_brief_id(
    source_project: str,
    source_entity_type: str,
    source_id: str,
) -> str:
    digest = hashlib.sha1(
        f"{source_project}:{source_entity_type}:{source_id}".encode("utf-8")
    ).hexdigest()
    return f"sb-yaml-{digest[:12]}"


def _entity_type(collection: str) -> str:
    if collection in {"records", "record"}:
        return "backlog_item"
    return collection[:-1] if collection.endswith("s") else collection


def _first_text(record: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        text = _text(record.get(key))
        if text:
            return text
    return None


def _text(value: Any) -> str | None:
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
    "YamlBacklogImporter",
    "parse_yaml_backlog_record",
]
