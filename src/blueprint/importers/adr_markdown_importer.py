"""Architecture Decision Record markdown importer."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any
import hashlib
import re

import yaml

from blueprint.importers.base import SourceImporter


DEFAULT_SOURCE_PROJECT = "adr"
DEFAULT_SOURCE_ENTITY_TYPE = "architecture_decision_record"
ADR_SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "status": ("status", "state"),
    "context": ("context", "problem", "background", "motivation"),
    "decision": ("decision", "decisions", "chosen option"),
    "consequences": ("consequences", "consequence", "results", "outcomes"),
    "alternatives": ("alternatives", "options", "considered options", "rejected options"),
    "links": ("links", "references", "related", "related decisions"),
}


class AdrMarkdownImporter(SourceImporter):
    """Import ADR markdown files into SourceBrief dictionaries."""

    def __init__(self, source_path: str | Path | None = None) -> None:
        self.source_path = Path(source_path).expanduser() if source_path is not None else None

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read one ADR file or return the first ADR from a directory."""
        source_briefs = self.import_path(source_id)
        if not source_briefs:
            raise ImportError(f"ADR source has no importable markdown files: {source_id}")
        return source_briefs[0]

    def import_path(self, source_id: str | Path) -> list[dict[str, Any]]:
        """Import one ADR markdown file or all direct markdown files in a directory."""
        path = Path(source_id).expanduser()
        if path.is_dir():
            return self.import_directory(path)
        return [self.import_file(path)]

    def import_file(self, source_id: str | Path) -> dict[str, Any]:
        """Read and normalize one ADR markdown file."""
        path = Path(source_id).expanduser()
        if path.suffix.lower() not in {".md", ".markdown"}:
            raise ImportError(f"ADR markdown file must be a .md or .markdown file: {path}")
        try:
            markdown_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ImportError(f"ADR markdown file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read ADR markdown file: {path}") from exc

        try:
            return parse_adr_markdown(markdown_text, file_path=str(path))
        except Exception as exc:
            raise ImportError(str(exc)) from exc

    def import_directory(self, source_id: str | Path) -> list[dict[str, Any]]:
        """Import direct ADR markdown children in deterministic filename order."""
        directory = Path(source_id).expanduser()
        if not directory.exists():
            raise ImportError(f"ADR directory not found: {directory}")
        if not directory.is_dir():
            raise ImportError(f"ADR source is not a directory: {directory}")

        records: list[dict[str, Any]] = []
        for path in sorted(
            directory.iterdir(),
            key=lambda item: (item.name.casefold(), item.name),
        ):
            if path.is_file() and path.suffix.lower() in {".md", ".markdown"}:
                records.append(self.import_file(path))
        return records

    def validate_source(self, source_id: str) -> bool:
        """Check whether the ADR source exists and can be imported."""
        try:
            self.import_path(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List ADRs from the configured source path."""
        if limit <= 0 or self.source_path is None:
            return []
        try:
            source_briefs = self.import_path(self.source_path)
        except Exception:
            return []

        return [
            {
                "id": source_brief["source_id"],
                "title": source_brief["title"],
                "metadata": {
                    "status": source_brief["source_payload"].get("status"),
                    "file_path": source_brief["source_links"].get("file_path"),
                },
            }
            for source_brief in source_briefs[:limit]
        ]


def parse_adr_markdown(markdown_text: str, *, file_path: str) -> dict[str, Any]:
    """Normalize one ADR markdown document into a SourceBrief dictionary."""
    resolved_path = str(Path(file_path).expanduser().resolve())
    frontmatter, body = _parse_frontmatter(markdown_text)
    sections = _parse_sections(body)
    canonical_sections = {
        canonical: _section_text(sections, canonical) or ""
        for canonical in ADR_SECTION_ALIASES
    }

    title = (
        _pick_string(frontmatter, ("title", "name"))
        or _first_h1(body)
        or _title_from_path(resolved_path)
    )
    source_id = _stable_source_id(Path(file_path).name)
    status = _pick_string(frontmatter, ("status", "state")) or _first_text_line(
        canonical_sections["status"]
    )
    context = canonical_sections["context"]
    decision = canonical_sections["decision"]
    consequences = canonical_sections["consequences"]
    alternatives = canonical_sections["alternatives"]
    links_section = canonical_sections["links"]
    source_links = _source_links(frontmatter, resolved_path, links_section)
    summary = (
        _pick_string(frontmatter, ("summary", "description", "overview"))
        or _first_paragraph(decision)
        or _first_paragraph(context)
        or title
    )
    domain = _pick_string(frontmatter, ("domain", "product_domain", "area"))
    now = datetime.utcnow()

    normalized = {
        "title": title,
        "domain": domain,
        "summary": summary,
        "status": status,
        "context": context,
        "decision": decision,
        "consequences": consequences,
        "alternatives": alternatives,
        "links": source_links.get("links", []),
        "source_id": source_id,
        "source_links": source_links,
        "source_metadata": {
            "file_path": resolved_path,
            "frontmatter": frontmatter,
            "section_titles": list(sections.keys()),
        },
    }

    return {
        "id": _stable_source_brief_id(source_id),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": DEFAULT_SOURCE_PROJECT,
        "source_entity_type": DEFAULT_SOURCE_ENTITY_TYPE,
        "source_id": source_id,
        "source_payload": _json_safe(
            {
                "file_path": resolved_path,
                "filename": Path(file_path).name,
                "raw_markdown": markdown_text,
                "frontmatter": frontmatter,
                "body": body,
                "sections": sections,
                "status": status,
                "context": context,
                "decision": decision,
                "consequences": consequences,
                "alternatives": alternatives,
                "links": source_links.get("links", []),
                "normalized": normalized,
            }
        ),
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


def _parse_frontmatter(markdown_text: str) -> tuple[dict[str, Any], str]:
    stripped = markdown_text.lstrip()
    if not stripped.startswith("---\n"):
        return {}, markdown_text.strip()

    closing = stripped.find("\n---\n", 4)
    if closing == -1:
        return {}, markdown_text.strip()

    raw_metadata = stripped[4:closing]
    content = stripped[closing + len("\n---\n") :]
    try:
        metadata = yaml.safe_load(raw_metadata) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid ADR frontmatter: {exc}") from exc
    if not isinstance(metadata, dict):
        metadata = {}
    return _json_safe(metadata), content.strip()


def _parse_sections(content: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current_title: str | None = None
    current_lines: list[str] = []

    for line in content.splitlines():
        heading = _heading_text(line)
        if heading:
            if current_title is not None:
                sections[current_title] = current_lines.copy()
            current_title = _normalize_key(heading)
            current_lines = []
            continue

        if current_title is not None:
            current_lines.append(line.rstrip())

    if current_title is not None:
        sections[current_title] = current_lines.copy()

    return {title: "\n".join(lines).strip() for title, lines in sections.items()}


def _section_text(sections: dict[str, str], canonical: str) -> str | None:
    for alias in ADR_SECTION_ALIASES[canonical]:
        text = sections.get(_normalize_key(alias))
        if text:
            return text
    return None


def _heading_text(line: str) -> str | None:
    match = re.match(r"^\s{0,3}#{1,6}\s+(.*?)\s*$", line)
    if not match:
        return None
    return match.group(1).strip()


def _first_h1(body: str) -> str | None:
    for line in body.splitlines():
        match = re.match(r"^\s{0,3}#\s+(.*?)\s*$", line)
        if match:
            return match.group(1).strip()
    return None


def _first_paragraph(text: str) -> str | None:
    for paragraph in re.split(r"\n\s*\n", text):
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if lines:
            return " ".join(lines)
    return None


def _first_text_line(text: str) -> str | None:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return re.sub(r"^\s*(?:[-*+]|(?:\d+\.))\s+", "", line).strip()
    return None


def _title_from_path(file_path: str) -> str:
    stem = Path(file_path).stem
    cleaned = re.sub(r"^\d+[-_.\s]+", "", stem)
    cleaned = re.sub(r"[_-]+", " ", cleaned).strip()
    return cleaned.title() if cleaned else "Architecture Decision Record"


def _stable_source_id(filename: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", Path(filename).stem.casefold()).strip("-")
    return f"adr/{slug or 'decision'}"


def _stable_source_brief_id(source_id: str) -> str:
    digest = hashlib.sha1(source_id.encode("utf-8")).hexdigest()
    return f"sb-adr-{digest[:12]}"


def _source_links(
    frontmatter: dict[str, Any],
    file_path: str,
    links_section: str,
) -> dict[str, Any]:
    links: dict[str, Any] = {"file_path": file_path}
    raw_source_links = _metadata_value(frontmatter, "source_links") or _metadata_value(
        frontmatter,
        "source links",
    )
    if isinstance(raw_source_links, dict):
        links.update(_json_safe(raw_source_links))

    collected_links = _coerce_list(_metadata_value(frontmatter, "links"))
    collected_links.extend(_extract_markdown_links(links_section))
    if collected_links:
        links["links"] = _dedupe_strings(collected_links)

    for key in ("source", "url", "uri", "link"):
        value = _metadata_value(frontmatter, key)
        if isinstance(value, str) and value.strip():
            links[key] = value.strip()
    return links


def _extract_markdown_links(text: str) -> list[str]:
    links: list[str] = []
    for match in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", text):
        value = match.group(1).strip()
        if value:
            links.append(value)
    for match in re.finditer(r"(?<!\()https?://[^\s>)]+", text):
        links.append(match.group(0).rstrip(".,;"))
    return links


def _pick_string(metadata: dict[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = _metadata_value(metadata, key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return None


def _metadata_value(metadata: dict[str, Any], key: str) -> Any:
    if key in metadata:
        return metadata[key]
    normalized = _normalize_key(key)
    for candidate_key, value in metadata.items():
        if _normalize_key(str(candidate_key)) == normalized:
            return value
    return None


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [line.strip() for line in value.replace(",", "\n").splitlines() if line.strip()]
    return []


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _normalize_key(value: str) -> str:
    return re.sub(r"[\s_]+", " ", value.strip().lower())


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


__all__ = [
    "AdrMarkdownImporter",
    "parse_adr_markdown",
]
