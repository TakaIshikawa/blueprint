"""Obsidian vault note importer."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any
import re
import uuid

try:
    import frontmatter
except ImportError:  # pragma: no cover - dependency is declared, fallback supports sandboxes
    frontmatter = None

from blueprint.importers.base import SourceImporter


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


def parse_obsidian_note(
    markdown_text: str,
    *,
    file_path: str,
    source_id: str | None = None,
    vault_path: str | None = None,
) -> dict[str, Any]:
    """Normalize an Obsidian markdown note into a SourceBrief dictionary."""
    resolved_path = str(Path(file_path).expanduser().resolve())
    frontmatter_data, body = _parse_frontmatter(markdown_text)

    title = (
        _pick_string(frontmatter_data, ("title", "name"))
        or _first_h1(body)
        or _title_from_path(resolved_path)
    )
    summary = (
        _pick_string(frontmatter_data, ("summary", "description", "overview"))
        or _first_non_heading_paragraph(body)
        or title
    )
    domain = _pick_string(frontmatter_data, ("domain", "product_domain", "area"))
    frontmatter_tags = _pick_list(frontmatter_data, ("tags", "tag"))
    body_tags = _extract_tags(body)
    tags = _dedupe_strings([*frontmatter_tags, *body_tags])
    aliases = _pick_list(frontmatter_data, ("aliases", "alias"))
    source_links = _source_links(frontmatter_data, resolved_path)
    wikilinks = _extract_wikilinks(body)
    relative_path = source_id or resolved_path

    now = datetime.utcnow()
    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "obsidian",
        "source_entity_type": "markdown_note",
        "source_id": relative_path,
        "source_payload": _json_safe(
            {
                "file_path": resolved_path,
                "relative_path": relative_path,
                "vault_path": vault_path,
                "frontmatter": frontmatter_data,
                "tags": tags,
                "frontmatter_tags": frontmatter_tags,
                "body_tags": body_tags,
                "aliases": aliases,
                "wikilinks": wikilinks,
                "body": body,
                "raw_markdown": markdown_text,
                "normalized": {
                    "title": title,
                    "domain": domain,
                    "summary": summary,
                    "source_links": source_links,
                },
            }
        ),
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


class ObsidianImporter(SourceImporter):
    """Import Obsidian markdown notes from a vault."""

    def __init__(self, vault_path: str | None = None):
        """Initialize an importer scoped to an optional Obsidian vault path."""
        self.vault_path = Path(vault_path).expanduser().resolve() if vault_path else None

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read and normalize an Obsidian markdown note."""
        path, relative_path = self._resolve_note_path(source_id)
        try:
            markdown_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ImportError(f"Obsidian note not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read Obsidian note: {path}") from exc

        return parse_obsidian_note(
            markdown_text,
            file_path=str(path),
            source_id=relative_path,
            vault_path=str(self.vault_path) if self.vault_path else None,
        )

    def validate_source(self, source_id: str) -> bool:
        """Check whether an Obsidian note exists and can be parsed."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50, query: str | None = None) -> list[dict[str, Any]]:
        """List notes in the configured vault, optionally matching filename or content."""
        if self.vault_path is None:
            raise ImportError("Obsidian vault path is not configured")
        if not self.vault_path.exists() or not self.vault_path.is_dir():
            raise ImportError(f"Obsidian vault path not found: {self.vault_path}")

        normalized_query = query.casefold() if query else None
        matches: list[dict[str, Any]] = []
        for path in sorted(
            self.vault_path.rglob("*.md"),
            key=lambda item: item.relative_to(self.vault_path).as_posix(),
        ):
            if not path.is_file():
                continue

            relative_path = path.relative_to(self.vault_path).as_posix()
            title = _title_from_path(relative_path)
            matched_in: list[str] = []
            text: str | None = None

            if normalized_query:
                if normalized_query in relative_path.casefold():
                    matched_in.append("path")
                try:
                    text = path.read_text(encoding="utf-8")
                except OSError:
                    continue
                if normalized_query in text.casefold():
                    matched_in.append("content")
                if not matched_in:
                    continue

            if text is None:
                try:
                    text = path.read_text(encoding="utf-8")
                except OSError:
                    text = ""
            frontmatter_data, body = _parse_frontmatter(text)
            title = _pick_string(frontmatter_data, ("title", "name")) or _first_h1(body) or title
            summary = (
                _pick_string(frontmatter_data, ("summary", "description", "overview"))
                or _first_non_heading_paragraph(body)
                or title
            )

            matches.append(
                {
                    "id": relative_path,
                    "title": title,
                    "summary": summary,
                    "path": str(path),
                    "relative_path": relative_path,
                    "matched_in": matched_in,
                }
            )
            if len(matches) >= limit:
                break
        return matches

    def _resolve_note_path(self, source_id: str) -> tuple[Path, str]:
        """Resolve a note path and reject configured-vault traversal."""
        raw_path = Path(source_id).expanduser()
        if self.vault_path is None:
            path = raw_path.resolve()
            return path, str(path)

        candidate = raw_path if raw_path.is_absolute() else self.vault_path / raw_path
        resolved = candidate.resolve()
        try:
            relative_path = resolved.relative_to(self.vault_path).as_posix()
        except ValueError as exc:
            raise ImportError(
                f"Obsidian note path is outside configured vault: {source_id}"
            ) from exc
        return resolved, relative_path


def _extract_wikilinks(body: str) -> list[dict[str, Any]]:
    """Extract Obsidian wikilinks from markdown body text."""
    wikilinks: list[dict[str, Any]] = []
    for match in re.finditer(r"(!?)\[\[([^\]]+)\]\]", body):
        raw_target = match.group(2).strip()
        target_part, alias = raw_target.split("|", 1) if "|" in raw_target else (raw_target, None)
        target, heading = target_part.split("#", 1) if "#" in target_part else (target_part, None)
        wikilinks.append(
            {
                "raw": match.group(0),
                "target": target.strip(),
                "heading": heading.strip() if heading else None,
                "alias": alias.strip() if alias else None,
                "embed": bool(match.group(1)),
            }
        )
    return wikilinks


def _extract_tags(body: str) -> list[str]:
    """Extract inline Obsidian tags from markdown body text."""
    tags: list[str] = []
    for line in body.splitlines():
        if re.match(r"^\s{0,3}#{1,6}\s+", line):
            continue
        for match in re.finditer(r"(?<![\w/])#([A-Za-z0-9][A-Za-z0-9_/-]*)", line):
            tag = match.group(1).rstrip("/").strip()
            if tag:
                tags.append(tag)
    return _dedupe_strings(tags)


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _parse_frontmatter(markdown_text: str) -> tuple[dict[str, Any], str]:
    if frontmatter is not None:
        try:
            post = frontmatter.loads(markdown_text)
        except Exception as exc:
            raise ImportError(f"Could not parse Obsidian note frontmatter: {exc}") from exc

        metadata = post.metadata or {}
        if not isinstance(metadata, dict):
            metadata = {}
        return _json_safe(metadata), post.content.strip()

    return _parse_frontmatter_fallback(markdown_text)


def _parse_frontmatter_fallback(markdown_text: str) -> tuple[dict[str, Any], str]:
    stripped = markdown_text.lstrip()
    if not stripped.startswith("---\n"):
        return {}, markdown_text.strip()

    closing = stripped.find("\n---\n", 4)
    if closing == -1:
        return {}, markdown_text.strip()

    raw_metadata = stripped[4:closing]
    content = stripped[closing + len("\n---\n") :]
    metadata: dict[str, Any] = {}
    current_key: str | None = None

    for raw_line in raw_metadata.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if line.startswith(" ") or line.startswith("\t"):
            if current_key is not None:
                value = metadata.get(current_key)
                stripped_item = line.strip()
                nested_key, nested_value = _parse_mapping_item(stripped_item)
                if nested_key and not stripped_item.startswith("- "):
                    if not isinstance(value, dict):
                        value = {}
                        metadata[current_key] = value
                    value[nested_key] = _parse_scalar(nested_value)
                elif isinstance(value, list):
                    if stripped_item.startswith("- "):
                        stripped_item = stripped_item[2:].strip()
                    value.append(_parse_scalar(stripped_item))
                elif isinstance(value, dict):
                    if nested_key:
                        value[nested_key] = _parse_scalar(nested_value)
            continue
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        current_key = key.strip()
        metadata[current_key] = _parse_yaml_value(value.strip())

    return _json_safe(metadata), content.strip()


def _first_h1(body: str) -> str | None:
    for line in body.splitlines():
        match = re.match(r"^\s{0,3}#\s+(.*?)\s*$", line)
        if match:
            return match.group(1).strip()
    return None


def _first_non_heading_paragraph(body: str) -> str | None:
    for paragraph in re.split(r"\n\s*\n", body):
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if not lines:
            continue
        if all(re.match(r"^\s{0,3}#{1,6}\s+", line) for line in lines):
            continue
        non_heading_lines = [line for line in lines if not re.match(r"^\s{0,3}#{1,6}\s+", line)]
        if non_heading_lines:
            return " ".join(non_heading_lines).strip()
    return None


def _title_from_path(file_path: str) -> str:
    stem = Path(file_path).stem
    cleaned = re.sub(r"[_-]+", " ", stem).strip()
    return cleaned.title() if cleaned else "Obsidian Note"


def _pick_string(metadata: dict[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
    return None


def _pick_list(metadata: dict[str, Any], keys: Iterable[str]) -> list[str]:
    for key in keys:
        items = _coerce_list(metadata.get(key))
        if items:
            return items
    return []


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return [stripped]
    return []


def _source_links(metadata: dict[str, Any], file_path: str) -> dict[str, Any]:
    links: dict[str, Any] = {"file_path": file_path}
    raw_links = metadata.get("source_links")
    if isinstance(raw_links, dict):
        links.update(_json_safe(raw_links))

    for key in ("source", "url", "uri", "link"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            links[key] = value.strip()
    return links


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


def _parse_yaml_value(value: str) -> Any:
    if value == "":
        return []
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    return _parse_scalar(value)


def _parse_mapping_item(value: str) -> tuple[str | None, str]:
    if ":" not in value:
        return None, ""
    key, raw_value = value.split(":", 1)
    return key.strip(), raw_value.strip()


def _parse_scalar(value: str) -> Any:
    if value.startswith("- "):
        value = value[2:].strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", "~"}:
        return None
    return value
