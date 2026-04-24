"""Markdown execution plan importer."""

from __future__ import annotations

import csv
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any

import yaml

try:
    import frontmatter
except ImportError:  # pragma: no cover - dependency is declared, fallback remains useful
    frontmatter = None


PLAN_FRONTMATTER_FIELDS = (
    "id",
    "implementation_brief_id",
    "target_engine",
    "target_repo",
    "project_type",
    "test_strategy",
    "handoff_prompt",
    "status",
    "milestones",
)

TABLE_COLUMN_ALIASES = {
    "id": "id",
    "task_id": "id",
    "task id": "id",
    "title": "title",
    "task": "title",
    "description": "description",
    "desc": "description",
    "acceptance_criteria": "acceptance_criteria",
    "acceptance criteria": "acceptance_criteria",
    "criteria": "acceptance_criteria",
    "acceptance": "acceptance_criteria",
    "depends_on": "depends_on",
    "depends on": "depends_on",
    "dependencies": "depends_on",
    "deps": "depends_on",
    "files/modules": "files_or_modules",
    "files_or_modules": "files_or_modules",
    "files or modules": "files_or_modules",
    "files": "files_or_modules",
    "modules": "files_or_modules",
    "milestone": "milestone",
    "owner_type": "owner_type",
    "owner type": "owner_type",
    "suggested_engine": "suggested_engine",
    "suggested engine": "suggested_engine",
    "engine": "suggested_engine",
    "estimated_complexity": "estimated_complexity",
    "estimated complexity": "estimated_complexity",
    "complexity": "estimated_complexity",
    "status": "status",
}

FIELD_ALIASES = {
    "id": "id",
    "task_id": "id",
    "task id": "id",
    "title": "title",
    "description": "description",
    "acceptance_criteria": "acceptance_criteria",
    "acceptance criteria": "acceptance_criteria",
    "criteria": "acceptance_criteria",
    "depends_on": "depends_on",
    "depends on": "depends_on",
    "dependencies": "depends_on",
    "deps": "depends_on",
    "files/modules": "files_or_modules",
    "files_or_modules": "files_or_modules",
    "files or modules": "files_or_modules",
    "files": "files_or_modules",
    "modules": "files_or_modules",
    "milestone": "milestone",
    "owner_type": "owner_type",
    "owner type": "owner_type",
    "suggested_engine": "suggested_engine",
    "suggested engine": "suggested_engine",
    "engine": "suggested_engine",
    "estimated_complexity": "estimated_complexity",
    "estimated complexity": "estimated_complexity",
    "complexity": "estimated_complexity",
    "status": "status",
}

LIST_FIELDS = {"acceptance_criteria", "depends_on", "files_or_modules"}
OPTIONAL_TASK_FIELDS = {
    "milestone",
    "owner_type",
    "suggested_engine",
    "estimated_complexity",
    "status",
}


class PlanMarkdownImportError(ValueError):
    """Raised when a markdown execution plan cannot be imported."""


@dataclass(frozen=True)
class ParsedPlanMarkdown:
    """Parsed execution plan payload."""

    plan: dict[str, Any]
    tasks: list[dict[str, Any]]


def generate_execution_plan_id() -> str:
    """Generate a local execution plan ID."""
    return f"plan-{uuid.uuid4().hex[:12]}"


def generate_execution_task_id(index: int) -> str:
    """Generate a deterministic fallback task ID within an imported plan."""
    return f"task-{index:03d}"


class PlanMarkdownImporter:
    """Import execution plans from structured markdown files."""

    def import_file(
        self,
        file_path: str | Path,
        *,
        implementation_brief_id: str | None = None,
    ) -> ParsedPlanMarkdown:
        """Read and parse a markdown execution plan file."""
        path = Path(file_path).expanduser()
        try:
            markdown_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise PlanMarkdownImportError(f"Markdown execution plan not found: {path}") from exc
        except OSError as exc:
            raise PlanMarkdownImportError(f"Could not read markdown execution plan: {path}") from exc

        return parse_plan_markdown(
            markdown_text,
            file_path=str(path),
            implementation_brief_id=implementation_brief_id,
        )


def parse_plan_markdown(
    markdown_text: str,
    *,
    file_path: str | None = None,
    implementation_brief_id: str | None = None,
) -> ParsedPlanMarkdown:
    """Parse markdown into an ExecutionPlan dictionary and task dictionaries."""
    metadata, content = _parse_frontmatter(markdown_text)
    resolved_path = str(Path(file_path).expanduser().resolve()) if file_path else None
    plan_brief_id = implementation_brief_id or _optional_string(
        metadata.get("implementation_brief_id")
    )
    if not plan_brief_id:
        raise PlanMarkdownImportError(
            "Missing implementation brief ID. Add implementation_brief_id to "
            "frontmatter or pass --brief IB_ID."
        )

    tasks = _parse_table_tasks(content)
    if not tasks:
        tasks = _parse_heading_tasks(content)
    _validate_tasks(tasks)

    plan = _build_plan(
        metadata,
        content=content,
        file_path=resolved_path,
        implementation_brief_id=plan_brief_id,
        tasks=tasks,
    )
    return ParsedPlanMarkdown(plan=plan, tasks=tasks)


def _build_plan(
    metadata: dict[str, Any],
    *,
    content: str,
    file_path: str | None,
    implementation_brief_id: str,
    tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    plan: dict[str, Any] = {
        "id": _optional_string(metadata.get("id")) or generate_execution_plan_id(),
        "implementation_brief_id": implementation_brief_id,
        "target_engine": _optional_string(metadata.get("target_engine")),
        "target_repo": _optional_string(metadata.get("target_repo")),
        "project_type": _optional_string(metadata.get("project_type")),
        "milestones": _normalize_milestones(metadata.get("milestones"), tasks),
        "test_strategy": _optional_string(metadata.get("test_strategy")),
        "handoff_prompt": _optional_string(metadata.get("handoff_prompt")),
        "status": _optional_string(metadata.get("status")) or "draft",
        "metadata": {
            "importer": "plan_markdown",
            "frontmatter": _json_safe(metadata),
            "task_count": len(tasks),
            "imported_at": datetime.utcnow().isoformat(),
        },
    }
    if file_path:
        plan["metadata"]["file_path"] = file_path
    if content:
        plan["metadata"]["markdown_excerpt"] = content[:2000]

    return {key: value for key, value in plan.items() if value is not None}


def _parse_frontmatter(markdown_text: str) -> tuple[dict[str, Any], str]:
    if frontmatter is not None:
        try:
            post = frontmatter.loads(markdown_text)
            metadata = post.metadata or {}
            return _json_safe(metadata if isinstance(metadata, dict) else {}), post.content.strip()
        except Exception as exc:
            raise PlanMarkdownImportError(f"Invalid YAML frontmatter: {exc}") from exc

    stripped = markdown_text.lstrip()
    if not stripped.startswith("---\n"):
        return {}, markdown_text.strip()
    closing = stripped.find("\n---\n", 4)
    if closing == -1:
        raise PlanMarkdownImportError("Invalid YAML frontmatter: missing closing ---")
    raw_metadata = stripped[4:closing]
    try:
        metadata = yaml.safe_load(raw_metadata) or {}
    except yaml.YAMLError as exc:
        raise PlanMarkdownImportError(f"Invalid YAML frontmatter: {exc}") from exc
    if not isinstance(metadata, dict):
        raise PlanMarkdownImportError("Invalid YAML frontmatter: expected a mapping")
    return _json_safe(metadata), stripped[closing + len("\n---\n") :].strip()


def _parse_table_tasks(content: str) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for table_lines in _iter_markdown_tables(content):
        headers = [_normalize_header(cell) for cell in _split_table_row(table_lines[0])]
        canonical_headers = [TABLE_COLUMN_ALIASES.get(header, header) for header in headers]
        if not {"title", "description", "acceptance_criteria"}.issubset(canonical_headers):
            continue

        for row_number, line in enumerate(table_lines[2:], start=1):
            cells = _split_table_row(line)
            if not any(cell.strip() for cell in cells):
                continue
            row = {
                canonical_headers[index]: cells[index].strip()
                for index in range(min(len(canonical_headers), len(cells)))
                if canonical_headers[index]
            }
            tasks.append(_task_from_fields(row, source=f"table row {row_number}"))
    return tasks


def _iter_markdown_tables(content: str) -> list[list[str]]:
    lines = content.splitlines()
    tables: list[list[str]] = []
    index = 0
    while index < len(lines) - 1:
        if _is_table_row(lines[index]) and _is_separator_row(lines[index + 1]):
            table = [lines[index], lines[index + 1]]
            index += 2
            while index < len(lines) and _is_table_row(lines[index]):
                table.append(lines[index])
                index += 1
            tables.append(table)
            continue
        index += 1
    return tables


def _parse_heading_tasks(content: str) -> list[dict[str, Any]]:
    sections = _heading_sections(content)
    task_sections = [
        section
        for section in sections
        if section["level"] >= 2 and _looks_like_task_section(section["title"], section["body"])
    ]
    tasks: list[dict[str, Any]] = []
    for index, section in enumerate(task_sections, start=1):
        fields = _fields_from_task_section(section["title"], section["body"], index)
        tasks.append(_task_from_fields(fields, source=f"heading '{section['title']}'"))
    return tasks


def _heading_sections(content: str) -> list[dict[str, Any]]:
    matches = list(re.finditer(r"^(#{1,6})\s+(.+?)\s*$", content, flags=re.MULTILINE))
    sections: list[dict[str, Any]] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        sections.append(
            {
                "level": len(match.group(1)),
                "title": match.group(2).strip(),
                "body": content[start:end].strip(),
            }
        )
    return sections


def _looks_like_task_section(title: str, body: str) -> bool:
    normalized_title = title.strip().lower()
    if normalized_title in {"tasks", "execution tasks", "plan"}:
        return False
    if re.match(r"^(task\s+)?[\w.-]+\s*[:\-]\s+.+$", title.strip(), flags=re.IGNORECASE):
        return True
    field_names = {
        FIELD_ALIASES.get(_normalize_header(match.group(1)), "")
        for match in re.finditer(r"^\s*[-*]?\s*([A-Za-z_ /-]+)\s*:", body, flags=re.MULTILINE)
    }
    return bool({"description", "acceptance_criteria"} & field_names)


def _fields_from_task_section(title: str, body: str, index: int) -> dict[str, Any]:
    fields = _extract_field_blocks(body)
    heading_id, heading_title = _parse_task_heading(title)
    fields.setdefault("id", heading_id or generate_execution_task_id(index))
    fields.setdefault("title", heading_title or title.strip())
    if "description" not in fields:
        description = _description_from_body(body)
        if description:
            fields["description"] = description
    return fields


def _parse_task_heading(title: str) -> tuple[str | None, str | None]:
    cleaned = title.strip()
    task_match = re.match(
        r"^(?:task\s+)?(?P<id>[\w.-]+)\s*[:\-]\s*(?P<title>.+)$",
        cleaned,
        flags=re.IGNORECASE,
    )
    if task_match:
        return task_match.group("id").strip(), task_match.group("title").strip()
    if cleaned.lower().startswith("task "):
        return None, cleaned[5:].strip() or None
    return None, cleaned or None


def _extract_field_blocks(body: str) -> dict[str, Any]:
    lines = body.splitlines()
    field_starts: list[tuple[int, str, str]] = []
    for index, line in enumerate(lines):
        match = re.match(r"^\s*(?:[-*]\s*)?([A-Za-z_ /-]+)\s*:\s*(.*)$", line)
        if not match:
            continue
        field = FIELD_ALIASES.get(_normalize_header(match.group(1)))
        if field:
            field_starts.append((index, field, match.group(2).strip()))

    fields: dict[str, Any] = {}
    for offset, (line_index, field, inline_value) in enumerate(field_starts):
        next_index = field_starts[offset + 1][0] if offset + 1 < len(field_starts) else len(lines)
        block_lines = [inline_value] if inline_value else []
        block_lines.extend(lines[line_index + 1 : next_index])
        block_text = "\n".join(block_lines).strip()
        if field in LIST_FIELDS:
            fields[field] = _parse_multi_value(block_text)
        else:
            fields[field] = _plain_text(block_text)
    return {key: value for key, value in fields.items() if value not in (None, "", [])}


def _description_from_body(body: str) -> str | None:
    lines = []
    for line in body.splitlines():
        if re.match(r"^\s*(?:[-*]\s*)?[A-Za-z_ /-]+\s*:", line):
            break
        if line.strip():
            lines.append(line.strip())
    return _plain_text("\n".join(lines))


def _task_from_fields(raw_fields: dict[str, Any], *, source: str) -> dict[str, Any]:
    fields = {str(key): value for key, value in raw_fields.items()}
    task = {
        "id": _optional_string(fields.get("id")),
        "title": _optional_string(fields.get("title")),
        "description": _plain_text(fields.get("description")),
        "acceptance_criteria": _parse_multi_value(fields.get("acceptance_criteria")),
        "depends_on": _parse_multi_value(fields.get("depends_on")),
        "files_or_modules": _parse_multi_value(fields.get("files_or_modules")),
        "metadata": {"import_source": source},
    }
    for field in OPTIONAL_TASK_FIELDS:
        value = _optional_string(fields.get(field))
        if value:
            task[field] = value

    if not task["id"]:
        task["id"] = generate_execution_task_id(abs(hash(source)) % 1000)
    if not task["files_or_modules"]:
        task.pop("files_or_modules")
    return task


def _validate_tasks(tasks: list[dict[str, Any]]) -> None:
    errors: list[str] = []
    if not tasks:
        errors.append(
            "No tasks found. Add task headings or a markdown table with title, "
            "description, and acceptance_criteria columns."
        )

    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        label = task.get("id") or f"task #{index}"
        if not task.get("title"):
            errors.append(f"{label}: missing required title")
        if not task.get("description"):
            errors.append(f"{label}: missing required description")
        if not task.get("acceptance_criteria"):
            errors.append(f"{label}: missing required acceptance criteria")
        if task.get("id") in seen_ids:
            errors.append(f"{label}: duplicate task id")
        seen_ids.add(task.get("id"))

    known_ids = {task["id"] for task in tasks if task.get("id")}
    for task in tasks:
        for dependency_id in task.get("depends_on") or []:
            if dependency_id not in known_ids:
                errors.append(
                    f"{task['id']}: depends_on references unknown task id '{dependency_id}'"
                )

    if errors:
        raise PlanMarkdownImportError("Invalid execution plan markdown:\n- " + "\n- ".join(errors))


def _normalize_milestones(value: Any, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(value, list):
        milestones: list[dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict):
                milestones.append(_json_safe(item))
            elif str(item).strip():
                milestones.append({"name": str(item).strip()})
        return milestones

    milestone_names = []
    for task in tasks:
        milestone = task.get("milestone")
        if milestone and milestone not in milestone_names:
            milestone_names.append(milestone)
    return [{"name": milestone} for milestone in milestone_names]


def _split_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return next(csv.reader(StringIO(stripped), delimiter="|", escapechar="\\"))


def _is_table_row(line: str) -> bool:
    return "|" in line and line.strip().startswith("|")


def _is_separator_row(line: str) -> bool:
    cells = _split_table_row(line)
    return bool(cells) and all(re.match(r"^\s*:?-{3,}:?\s*$", cell) for cell in cells)


def _normalize_header(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower().replace("-", "_"))


def _parse_multi_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    items = []
    for line in text.splitlines():
        stripped = re.sub(r"^\s*(?:[-*]|\d+[.)])\s+", "", line).strip()
        if stripped:
            items.extend(_split_inline_list(stripped))
    return items


def _split_inline_list(value: str) -> list[str]:
    if "," in value or ";" in value:
        return [part.strip() for part in re.split(r"[;,]", value) if part.strip()]
    return [value.strip()] if value.strip() else []


def _plain_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return "\n".join(str(item).strip() for item in value if str(item).strip()) or None
    lines = []
    for line in str(value).splitlines():
        stripped = re.sub(r"^\s*(?:[-*]|\d+[.)])\s+", "", line).strip()
        if stripped:
            lines.append(stripped)
    return "\n".join(lines) or None


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
