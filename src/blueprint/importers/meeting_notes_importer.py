"""Meeting notes markdown and text importer."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any
import hashlib
import re

import yaml

from blueprint.importers.base import SourceImporter


def _normalize_key(value: str) -> str:
    return re.sub(r"[\s_]+", " ", value.strip().lower())


SUPPORTED_EXTENSIONS = {".md", ".markdown", ".txt"}
SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "summary": ("summary", "overview", "context", "notes"),
    "participants": ("attendees", "participants", "present"),
    "date": ("date", "meeting date"),
    "decisions": ("decisions", "decision"),
    "actions": ("action items", "actions", "todos", "to dos", "next steps"),
    "risks": ("risks", "risk", "blockers", "concerns"),
    "unresolved_questions": (
        "open questions",
        "questions",
        "unresolved questions",
        "parking lot",
    ),
}
METADATA_KEYS = {
    "attendees",
    "participants",
    "present",
    "date",
    "meeting date",
    "title",
    "summary",
    "domain",
    "source_id",
    "source id",
}
METADATA_KEY_ALIASES = {_normalize_key(item) for item in METADATA_KEYS}
PLAIN_SECTION_HEADINGS = {
    _normalize_key(alias)
    for section_aliases in SECTION_ALIASES.values()
    for alias in section_aliases
}
ACTION_OWNER_RE = re.compile(r"(?:^|\s)(?:owner|assignee)\s*:\s*([^;,\n]+)", re.IGNORECASE)
ACTION_DUE_RE = re.compile(r"(?:^|\s)due\s*:\s*([^;,\n]+)", re.IGNORECASE)


class MeetingNotesImporter(SourceImporter):
    """Import meeting notes from markdown or plain text files."""

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read and normalize one meeting notes file."""
        path = Path(source_id).expanduser()
        if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            raise ImportError(f"Meeting notes file must be .md, .markdown, or .txt: {path}")
        try:
            notes_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ImportError(f"Meeting notes file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read meeting notes file: {path}") from exc

        try:
            return parse_meeting_notes(notes_text, file_path=str(path))
        except Exception as exc:
            raise ImportError(str(exc)) from exc

    def validate_source(self, source_id: str) -> bool:
        """Check whether a meeting notes file exists and can be imported."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """Meeting notes imports are file-based and do not support discovery."""
        return []


def parse_meeting_notes(notes_text: str, *, file_path: str) -> dict[str, Any]:
    """Normalize meeting notes text into a SourceBrief dictionary."""
    if not notes_text.strip():
        raise ValueError("Meeting notes file is empty")

    resolved_path = str(Path(file_path).expanduser().resolve())
    frontmatter, content = _parse_frontmatter(notes_text)
    sections = _parse_sections(content)
    inline_metadata = _parse_inline_metadata(content)

    title = (
        _pick_string(frontmatter, ("title", "name"))
        or _pick_string(inline_metadata, ("title",))
        or _first_h1(content)
        or _title_from_path(resolved_path)
    )
    domain = _pick_string(frontmatter, ("domain", "product_domain", "area")) or _pick_string(
        inline_metadata,
        ("domain",),
    )
    meeting_date = (
        _pick_string(frontmatter, ("date", "meeting_date", "meeting date"))
        or _pick_string(inline_metadata, ("date", "meeting_date", "meeting date"))
        or _first_text_line(_section_text(sections, "date") or "")
    )

    parsed_participants = _dedupe_strings(
        [
            *_pick_list(inline_metadata, ("attendees", "participants", "present")),
            *_section_list(sections, "participants"),
        ]
    )
    frontmatter_participants = _pick_list(frontmatter, ("attendees", "participants", "present"))
    participants = _dedupe_strings([*frontmatter_participants, *parsed_participants])

    parsed_decisions = _section_list(sections, "decisions")
    frontmatter_decisions = _pick_list(frontmatter, ("decisions", "decision"))
    decisions = _dedupe_strings([*frontmatter_decisions, *parsed_decisions])

    parsed_actions = _parse_actions(_section_lines(sections, "actions"))
    frontmatter_actions = _coerce_action_items(
        _metadata_value(frontmatter, "actions")
        or _metadata_value(frontmatter, "action_items")
        or _metadata_value(frontmatter, "action items")
    )
    action_items = _dedupe_actions([*frontmatter_actions, *parsed_actions])

    risks = _dedupe_strings(
        [
            *_pick_list(frontmatter, ("risks", "risk", "blockers")),
            *_section_list(sections, "risks"),
        ]
    )
    unresolved_questions = _dedupe_strings(
        [
            *_pick_list(
                frontmatter,
                ("open_questions", "open questions", "unresolved_questions", "questions"),
            ),
            *_section_list(sections, "unresolved_questions"),
        ]
    )
    summary = (
        _pick_string(frontmatter, ("summary", "description", "overview"))
        or _pick_string(inline_metadata, ("summary",))
        or _first_paragraph(_section_text(sections, "summary") or "")
        or _first_paragraph(_body_without_title(content))
        or title
    )
    source_id = (
        _pick_string(frontmatter, ("source_id", "source id", "id"))
        or _pick_string(inline_metadata, ("source_id", "source id"))
        or _stable_source_id(resolved_path)
    )
    source_links = _source_links(frontmatter, resolved_path)
    source_metadata = {
        "file_path": resolved_path,
        "filename": Path(file_path).name,
        "frontmatter": frontmatter,
        "inline_metadata": inline_metadata,
        "section_titles": list(sections.keys()),
    }
    normalized = {
        "title": title,
        "domain": domain,
        "summary": summary,
        "date": meeting_date,
        "participants": participants,
        "decisions": decisions,
        "actions": action_items,
        "action_items": action_items,
        "risks": risks,
        "unresolved_questions": unresolved_questions,
        "source_id": source_id,
        "source_links": source_links,
        "source_metadata": source_metadata,
    }

    now = datetime.utcnow()
    return {
        "id": _stable_source_brief_id(source_id),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "meeting_notes",
        "source_entity_type": "meeting_notes",
        "source_id": source_id,
        "source_payload": _json_safe(
            {
                "file_path": resolved_path,
                "filename": Path(file_path).name,
                "raw_text": notes_text,
                "frontmatter": frontmatter,
                "content": content,
                "sections": sections,
                "metadata": source_metadata,
                "date": meeting_date,
                "participants": participants,
                "decisions": decisions,
                "actions": action_items,
                "action_items": action_items,
                "risks": risks,
                "unresolved_questions": unresolved_questions,
                "normalized": normalized,
            }
        ),
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


def _parse_frontmatter(notes_text: str) -> tuple[dict[str, Any], str]:
    stripped = notes_text.lstrip()
    if not stripped.startswith("---\n"):
        return {}, notes_text.strip()

    closing = stripped.find("\n---\n", 4)
    if closing == -1:
        return {}, notes_text.strip()

    raw_metadata = stripped[4:closing]
    content = stripped[closing + len("\n---\n") :].strip()
    try:
        metadata = yaml.safe_load(raw_metadata) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid meeting notes frontmatter: {exc}") from exc
    if not isinstance(metadata, dict):
        return {}, content
    return _json_safe(metadata), content


def _parse_sections(content: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current_title: str | None = None
    current_lines: list[str] = []

    for line in content.splitlines():
        heading = _heading_text(line) or _plain_section_heading(line)
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


def _parse_inline_metadata(content: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for raw_line in content.splitlines():
        stripped = raw_line.strip().lstrip("-* ").replace("**", "")
        if not stripped or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        normalized_key = _normalize_key(key)
        if normalized_key in METADATA_KEY_ALIASES:
            metadata[normalized_key.replace(" ", "_")] = value.strip()
    return metadata


def _section_text(sections: dict[str, str], canonical: str) -> str | None:
    for alias in SECTION_ALIASES[canonical]:
        text = sections.get(_normalize_key(alias))
        if text:
            return text
    return None


def _section_lines(sections: dict[str, str], canonical: str) -> list[str]:
    text = _section_text(sections, canonical) or ""
    return [line for line in text.splitlines() if line.strip()]


def _section_list(sections: dict[str, str], canonical: str) -> list[str]:
    text = _section_text(sections, canonical) or ""
    return _list_items(text)


def _list_items(text: str) -> list[str]:
    items: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cleaned = re.sub(r"^\s*(?:[-*+]|\d+[.)])\s+", "", line)
        cleaned = re.sub(r"^\[[ xX]\]\s+", "", cleaned).strip()
        if cleaned:
            items.append(cleaned)
    return items


def _parse_actions(lines: Iterable[str]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for raw_line in lines:
        text = re.sub(r"^\s*(?:[-*+]|\d+[.)])\s+", "", raw_line.strip())
        text = re.sub(r"^\[[ xX]\]\s+", "", text).strip()
        if not text:
            continue
        owner_match = ACTION_OWNER_RE.search(text)
        due_match = ACTION_DUE_RE.search(text)
        actions.append(
            {
                "text": _strip_action_metadata(text),
                "owner": owner_match.group(1).strip() if owner_match else None,
                "due": due_match.group(1).strip() if due_match else None,
                "raw": raw_line.strip(),
            }
        )
    return actions


def _coerce_action_items(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict):
        return [_action_from_mapping(value)]
    if isinstance(value, (list, tuple, set)):
        actions: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                actions.append(_action_from_mapping(item))
            elif str(item).strip():
                actions.append(
                    {"text": str(item).strip(), "owner": None, "due": None, "raw": str(item)}
                )
        return actions
    if isinstance(value, str):
        return _parse_actions(value.replace(",", "\n").splitlines())
    return []


def _action_from_mapping(value: dict[Any, Any]) -> dict[str, Any]:
    text = _pick_string(value, ("text", "title", "action", "task", "description")) or str(value)
    return {
        "text": text,
        "owner": _pick_string(value, ("owner", "assignee")),
        "due": _pick_string(value, ("due", "due_date", "due date")),
        "raw": _json_safe(value),
    }


def _strip_action_metadata(text: str) -> str:
    cleaned = ACTION_OWNER_RE.sub("", text)
    cleaned = ACTION_DUE_RE.sub("", cleaned)
    return re.sub(r"\s{2,}", " ", cleaned).strip(" ;,")


def _pick_string(metadata: dict[Any, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = _metadata_value(metadata, key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
        elif value is not None and not isinstance(value, (dict, list, tuple, set)):
            text = str(value).strip()
            if text:
                return text
    return None


def _pick_list(metadata: dict[Any, Any], keys: Iterable[str]) -> list[str]:
    for key in keys:
        value = _metadata_value(metadata, key)
        items = _coerce_list(value)
        if items:
            return items
    return []


def _metadata_value(metadata: dict[Any, Any], key: str) -> Any:
    if key in metadata:
        return metadata[key]
    normalized = _normalize_key(key)
    for candidate_key, value in metadata.items():
        if _normalize_key(str(candidate_key)) == normalized:
            return value
    return None


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [line.strip() for line in value.replace(",", "\n").splitlines() if line.strip()]
    return [str(value).strip()] if str(value).strip() else []


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = value.strip()
        key = item.casefold()
        if not item or key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _dedupe_actions(actions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str | None, str | None]] = set()
    result: list[dict[str, Any]] = []
    for action in actions:
        text = str(action.get("text") or "").strip()
        owner = action.get("owner")
        due = action.get("due")
        key = (text.casefold(), str(owner).casefold() if owner else None, str(due) if due else None)
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(
            {
                "text": text,
                "owner": str(owner).strip() if owner else None,
                "due": str(due).strip() if due else None,
                "raw": _json_safe(action.get("raw", text)),
            }
        )
    return result


def _source_links(frontmatter: dict[str, Any], file_path: str) -> dict[str, Any]:
    links: dict[str, Any] = {"file_path": file_path}
    raw_source_links = _metadata_value(frontmatter, "source_links") or _metadata_value(
        frontmatter,
        "source links",
    )
    if isinstance(raw_source_links, dict):
        links.update(_json_safe(raw_source_links))
    for key in ("source", "url", "uri", "link"):
        value = _metadata_value(frontmatter, key)
        if isinstance(value, str) and value.strip():
            links[key] = value.strip()
    return links


def _heading_text(line: str) -> str | None:
    match = re.match(r"^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$", line)
    return match.group(1).strip() if match else None


def _plain_section_heading(line: str) -> str | None:
    stripped = line.strip().rstrip(":").strip()
    normalized = _normalize_key(stripped)
    return stripped if normalized in PLAIN_SECTION_HEADINGS else None


def _first_h1(content: str) -> str | None:
    for line in content.splitlines():
        match = re.match(r"^\s{0,3}#\s+(.*?)\s*#*\s*$", line)
        if match:
            return match.group(1).strip()
    return None


def _first_text_line(text: str) -> str | None:
    for line in text.splitlines():
        stripped = re.sub(r"^\s*(?:[-*+]|\d+[.)])\s+", "", line).strip()
        if stripped:
            return stripped
    return None


def _first_paragraph(text: str) -> str | None:
    for paragraph in re.split(r"\n\s*\n", text.strip()):
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if lines:
            return " ".join(lines)
    return None


def _body_without_title(content: str) -> str:
    return "\n".join(
        line for line in content.splitlines() if not re.match(r"^\s{0,3}#\s+", line)
    ).strip()


def _title_from_path(path: str) -> str:
    cleaned = re.sub(r"[_-]+", " ", Path(path).stem).strip()
    return cleaned.title() if cleaned else "Meeting Notes"


def _stable_source_id(file_path: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", Path(file_path).stem.casefold()).strip("-")
    return f"meeting-notes/{slug or 'notes'}"


def _stable_source_brief_id(source_id: str) -> str:
    digest = hashlib.sha1(source_id.encode("utf-8")).hexdigest()
    return f"sb-meeting-{digest[:12]}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
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
    "MeetingNotesImporter",
    "parse_meeting_notes",
]
