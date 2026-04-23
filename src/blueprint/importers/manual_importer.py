"""Manual markdown brief importer."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any
import re
import uuid

try:
    import frontmatter
except ImportError:  # pragma: no cover - optional dependency fallback
    frontmatter = None

from blueprint.importers.base import SourceImporter


SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "summary": ("summary", "problem statement", "problem", "overview"),
    "mvp_goal": ("mvp goal", "mvp", "goal", "objective", "objectives"),
    "scope": ("scope", "in scope"),
    "non_goals": ("non-goals", "non goals", "out of scope", "exclusions"),
    "assumptions": ("assumptions",),
    "validation_plan": ("validation plan", "validation", "test plan", "verification"),
    "definition_of_done": (
        "definition of done",
        "done",
        "acceptance criteria",
        "success criteria",
    ),
    "product_surface": ("product surface", "surface", "surfaces", "interface", "interfaces"),
}


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


def parse_manual_brief_markdown(markdown_text: str, *, file_path: str) -> dict[str, Any]:
    """Normalize a manual markdown brief into a SourceBrief dictionary."""
    resolved_path = str(Path(file_path).expanduser().resolve())
    metadata, content = _parse_frontmatter(markdown_text)
    sections = _parse_sections(content)

    title = (
        _pick_string(
            metadata,
            ("title", "name"),
        )
        or _section_title(content)
        or _title_from_path(resolved_path)
    )

    summary = (
        _pick_string(
            metadata,
            ("summary", "problem_statement", "problem statement", "problem"),
        )
        or _first_non_empty(
            _section_text(sections, "summary"),
            _section_text(sections, "problem_statement"),
            _section_text(sections, "problem statement"),
            _section_text(sections, "problem"),
            _section_text(sections, "overview"),
            _first_paragraph(content),
        )
        or title
    )

    domain = _pick_string(metadata, ("domain", "product_domain", "area"))
    mvp_goal = (
        _pick_string(metadata, ("mvp_goal", "mvp goal", "goal", "objective"))
        or _first_non_empty(
            _section_text(sections, "mvp_goal"),
            _section_text(sections, "mvp goal"),
            _section_text(sections, "mvp"),
            _section_text(sections, "goal"),
            _section_text(sections, "objective"),
        )
        or summary
    )
    scope = _pick_list(metadata, ("scope",)) or _section_list(sections, "scope")
    non_goals = _pick_list(metadata, ("non_goals", "non goals", "non-goals")) or _section_list(
        sections,
        "non-goals",
    )
    assumptions = _pick_list(metadata, ("assumptions",)) or _section_list(sections, "assumptions")
    validation_plan = (
        _pick_string(
            metadata,
            ("validation_plan", "validation plan", "test_plan", "test plan"),
        )
        or _first_non_empty(
            _section_text(sections, "validation_plan"),
            _section_text(sections, "validation plan"),
            _section_text(sections, "validation"),
            _section_text(sections, "test plan"),
            _section_text(sections, "verification"),
        )
        or "Validate the manual brief with implementation review."
    )
    definition_of_done = _pick_list(
        metadata,
        (
            "definition_of_done",
            "definition of done",
            "done",
            "acceptance_criteria",
            "acceptance criteria",
        ),
    ) or _section_list(sections, "definition of done")
    product_surface = _pick_string(
        metadata,
        ("product_surface", "product surface", "surface", "surfaces", "interface", "interfaces"),
    ) or _first_non_empty(
        _section_text(sections, "product_surface"),
        _section_text(sections, "product surface"),
        _section_text(sections, "surface"),
        _section_text(sections, "surfaces"),
        _section_text(sections, "interface"),
        _section_text(sections, "interfaces"),
    )

    source_metadata = _json_safe(
        {
            "file_path": resolved_path,
            "frontmatter": metadata,
            "section_titles": list(sections.keys()),
        }
    )

    now = datetime.utcnow()
    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "manual",
        "source_entity_type": "markdown_brief",
        "source_id": resolved_path,
        "source_payload": _json_safe(
            {
                "file_path": resolved_path,
                "raw_markdown": markdown_text,
                "frontmatter": metadata,
                "content": content,
                "sections": sections,
                "normalized": {
                    "title": title,
                    "domain": domain,
                    "summary": summary,
                    "mvp_goal": mvp_goal,
                    "scope": scope,
                    "non_goals": non_goals,
                    "assumptions": assumptions,
                    "validation_plan": validation_plan,
                    "definition_of_done": definition_of_done,
                    "product_surface": product_surface,
                    "source_metadata": source_metadata,
                },
            }
        ),
        "source_links": {
            "file_path": resolved_path,
        },
        "created_at": now,
        "updated_at": now,
    }


class ManualBriefImporter(SourceImporter):
    """Import manual design briefs from markdown files."""

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read and normalize a markdown brief."""
        path = Path(source_id).expanduser()
        try:
            markdown_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ImportError(f"Markdown brief file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read markdown brief file: {path}") from exc

        try:
            return parse_manual_brief_markdown(markdown_text, file_path=str(path))
        except Exception as exc:
            raise ImportError(str(exc)) from exc

    def validate_source(self, source_id: str) -> bool:
        """Check whether a markdown brief exists and can be parsed."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """Markdown briefs are file-based and do not support discovery."""
        return []


def _parse_frontmatter(markdown_text: str) -> tuple[dict[str, Any], str]:
    """Parse YAML front matter if present."""
    if frontmatter is None:
        return _parse_frontmatter_fallback(markdown_text)

    try:
        post = frontmatter.loads(markdown_text)
    except Exception:
        return _parse_frontmatter_fallback(markdown_text)

    metadata = post.metadata or {}
    if not isinstance(metadata, dict):
        metadata = {}
    return _json_safe(metadata), post.content.strip()


def _parse_frontmatter_fallback(markdown_text: str) -> tuple[dict[str, Any], str]:
    """Parse a basic YAML front matter block without external dependencies."""
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
                if isinstance(value, list):
                    value.append(_parse_scalar(line.strip()))
                elif value is None:
                    metadata[current_key] = [_parse_scalar(line.strip())]
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        current_key = key.strip()
        metadata[current_key] = _parse_yaml_value(value.strip())

    return _json_safe(metadata), content.strip()


def _parse_sections(content: str) -> dict[str, str]:
    """Parse markdown headings into section bodies."""
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


def _section_title(content: str) -> str | None:
    """Return the first markdown heading if one exists."""
    for line in content.splitlines():
        heading = _heading_text(line)
        if heading:
            return heading.strip()
    return None


def _title_from_path(file_path: str) -> str:
    """Build a human-readable title from a file name."""
    stem = Path(file_path).stem
    cleaned = re.sub(r"[_-]+", " ", stem).strip()
    return cleaned.title() if cleaned else "Manual Brief"


def _heading_text(line: str) -> str | None:
    """Extract heading text from a markdown heading line."""
    match = re.match(r"^\s{0,3}#{1,6}\s+(.*?)\s*$", line)
    if not match:
        return None
    return match.group(1).strip()


def _normalize_key(value: str) -> str:
    """Normalize a label for matching."""
    return re.sub(r"[\s_]+", " ", value.strip().lower())


def _pick_string(metadata: dict[str, Any], keys: Iterable[str]) -> str | None:
    """Read the first non-empty string value from metadata."""
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
    return None


def _pick_list(metadata: dict[str, Any], keys: Iterable[str]) -> list[str]:
    """Read a list-like value from metadata."""
    for key in keys:
        value = metadata.get(key)
        items = _coerce_list(value)
        if items:
            return items
    return []


def _section_text(sections: dict[str, str], key: str) -> str | None:
    """Find a section body by normalized heading."""
    return sections.get(_normalize_key(key))


def _section_list(sections: dict[str, str], key: str) -> list[str]:
    """Return a bullet list or paragraph from a markdown section."""
    text = _section_text(sections, key)
    if not text:
        return []
    items = _extract_bullets(text)
    if items:
        return items
    return [line for line in (text.strip(),) if line]


def _extract_bullets(text: str) -> list[str]:
    """Extract bullet or numbered list items from markdown text."""
    items = []
    for line in text.splitlines():
        match = re.match(r"^\s*(?:[-*+]|(?:\d+\.))\s+(.*\S)\s*$", line)
        if match:
            items.append(match.group(1).strip())
    return items


def _first_paragraph(text: str) -> str | None:
    """Return the first non-empty paragraph from text."""
    paragraphs = [
        paragraph.strip() for paragraph in re.split(r"\n\s*\n", text) if paragraph.strip()
    ]
    for paragraph in paragraphs:
        if not _heading_text(paragraph.splitlines()[0]):
            return " ".join(line.strip() for line in paragraph.splitlines()).strip()
    return None


def _first_non_empty(*values: str | None) -> str | None:
    """Return the first non-empty string value."""
    for value in values:
        if value:
            return value
    return None


def _coerce_list(value: Any) -> list[str]:
    """Convert metadata values into a list of strings."""
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return items
    if isinstance(value, tuple):
        items = [str(item).strip() for item in value if str(item).strip()]
        return items
    if isinstance(value, str):
        value = value.strip()
        if value:
            return [value]
    return []


def _json_safe(value: Any) -> Any:
    """Convert values into JSON-serializable data."""
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _parse_yaml_value(value: str) -> Any:
    """Parse a small subset of YAML scalar/list values for front matter fallback."""
    if value == "":
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part.strip()) for part in inner.split(",")]
    return _parse_scalar(value)


def _parse_scalar(value: str) -> Any:
    """Parse a scalar YAML value conservatively."""
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none", "~"}:
        return None
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            pass
    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except ValueError:
            pass
    return value
