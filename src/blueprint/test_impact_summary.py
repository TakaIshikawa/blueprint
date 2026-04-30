"""Summarize likely validation impact across execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


IMPACT_CLASSIFICATIONS = (
    "unit",
    "integration",
    "migration",
    "UI",
    "docs-only",
    "unknown",
)


@dataclass(frozen=True, slots=True)
class TaskTestImpact:
    """Validation impact and command coverage for one execution task."""

    __test__ = False

    task_id: str
    title: str
    classifications: tuple[str, ...]
    recommended_commands: tuple[str, ...] = field(default_factory=tuple)
    has_validation_gap: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "classifications": list(self.classifications),
            "recommended_commands": list(self.recommended_commands),
            "has_validation_gap": self.has_validation_gap,
        }


@dataclass(frozen=True, slots=True)
class TestImpactSummary:
    """Aggregate validation impact summary for an execution plan."""

    __test__ = False

    plan_id: str | None
    tasks: tuple[TaskTestImpact, ...]
    recommended_commands: tuple[str, ...]
    validation_gaps: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "tasks": [task.to_dict() for task in self.tasks],
            "recommended_commands": list(self.recommended_commands),
            "validation_gaps": list(self.validation_gaps),
        }

    def to_markdown(self) -> str:
        """Render a compact deterministic markdown summary."""
        title_parts = ["Test Impact Summary"]
        if self.plan_id:
            title_parts.append(f"for {self.plan_id}")
        lines = [f"# {' '.join(title_parts)}", ""]

        lines.append("## Aggregates")
        lines.append(f"- Tasks: {len(self.tasks)}")
        lines.append("- Impact counts:")
        counts = self.impact_counts()
        for classification in IMPACT_CLASSIFICATIONS:
            lines.append(f"  - {classification}: {counts[classification]}")
        if self.recommended_commands:
            lines.append("- Recommended commands:")
            lines.extend(f"  - `{command}`" for command in self.recommended_commands)
        else:
            lines.append("- Recommended commands: None")
        if self.validation_gaps:
            lines.append("- Validation gaps: " + ", ".join(f"`{gap}`" for gap in self.validation_gaps))
        else:
            lines.append("- Validation gaps: None")
        lines.append("")

        lines.append("## Task Details")
        if not self.tasks:
            lines.append("- No tasks found.")
        for task in self.tasks:
            classifications = ", ".join(task.classifications)
            lines.append(f"- `{task.task_id}` {task.title}: {classifications}")
            if task.recommended_commands:
                commands = ", ".join(f"`{command}`" for command in task.recommended_commands)
                lines.append(f"  - commands: {commands}")
            if task.has_validation_gap:
                lines.append("  - gap: missing task validation command")

        return "\n".join(lines).rstrip() + "\n"

    def impact_counts(self) -> dict[str, int]:
        """Return counts of tasks matching each impact classification."""
        counts = {classification: 0 for classification in IMPACT_CLASSIFICATIONS}
        for task in self.tasks:
            for classification in task.classifications:
                counts[classification] += 1
        return counts


def build_test_impact_summary(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> TestImpactSummary:
    """Build a validation impact summary from an execution plan."""
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))
    plan_commands = _plan_validation_commands(plan)

    task_impacts: list[TaskTestImpact] = []
    aggregate_commands: list[str] = []
    gaps: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title")) or task_id
        task_commands = _task_validation_commands(task)
        commands = _dedupe([*task_commands, *plan_commands])
        has_gap = not commands
        if has_gap:
            gaps.append(task_id)
        aggregate_commands.extend(commands)
        task_impacts.append(
            TaskTestImpact(
                task_id=task_id,
                title=title,
                classifications=tuple(_classifications(task, task_commands)),
                recommended_commands=tuple(commands),
                has_validation_gap=has_gap,
            )
        )

    return TestImpactSummary(
        plan_id=_optional_text(plan.get("id")),
        tasks=tuple(task_impacts),
        recommended_commands=tuple(_dedupe(aggregate_commands)),
        validation_gaps=tuple(_dedupe(gaps)),
    )


def test_impact_summary_to_dict(summary: TestImpactSummary) -> dict[str, Any]:
    """Serialize a test impact summary to a dictionary."""
    return summary.to_dict()


test_impact_summary_to_dict.__test__ = False


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _classifications(task: Mapping[str, Any], task_commands: list[str]) -> list[str]:
    files = _strings(task.get("files_or_modules"))
    context = _task_context(task, task_commands)
    classifications: list[str] = []

    if _is_docs_only_task(files, context):
        classifications.append("docs-only")
    else:
        if _is_migration_task(files, context):
            classifications.append("migration")
        if _is_ui_task(files, context):
            classifications.append("UI")
        if _is_integration_task(files, context):
            classifications.append("integration")
        if _is_unit_task(files, context, task_commands):
            classifications.append("unit")

    if not classifications:
        classifications.append("unknown")
    return [classification for classification in IMPACT_CLASSIFICATIONS if classification in classifications]


def _is_docs_only_task(files: list[str], context: str) -> bool:
    if files and all(_is_docs_path(file_path) for file_path in files):
        return True
    return not files and _has_token(context, {"docs", "documentation", "readme", "runbook"})


def _is_docs_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    return (
        pure_path.suffix in {".md", ".mdx", ".rst", ".txt", ".adoc"}
        or "docs" in pure_path.parts
        or pure_path.name in {"readme", "readme.md", "changelog.md"}
    )


def _is_migration_task(files: list[str], context: str) -> bool:
    if any(_is_migration_path(file_path) for file_path in files):
        return True
    return _has_token(
        context,
        {"alembic", "backfill", "database", "migration", "migrations", "schema", "sql"},
    )


def _is_migration_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    return pure_path.suffix == ".sql" or bool(
        {"alembic", "db", "migrations", "schema"} & set(pure_path.parts)
    )


def _is_ui_task(files: list[str], context: str) -> bool:
    if any(_is_ui_path(file_path) for file_path in files):
        return True
    return _has_token(
        context,
        {
            "accessibility",
            "browser",
            "component",
            "frontend",
            "page",
            "react",
            "screen",
            "storybook",
            "ui",
            "view",
            "vue",
        },
    )


def _is_ui_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    suffixes = {".css", ".scss", ".sass", ".tsx", ".jsx", ".vue", ".svelte"}
    parts = set(pure_path.parts)
    return pure_path.suffix in suffixes or bool(
        {"app", "components", "frontend", "pages", "ui", "views"} & parts
    )


def _is_integration_task(files: list[str], context: str) -> bool:
    if any(_is_integration_path(file_path) for file_path in files):
        return True
    return _has_token(
        context,
        {
            "api",
            "client",
            "connector",
            "e2e",
            "endpoint",
            "external",
            "integration",
            "playwright",
            "provider",
            "route",
            "webhook",
        },
    )


def _is_integration_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    parts = set(pure_path.parts)
    name = pure_path.name
    return bool({"api", "clients", "connectors", "e2e", "integrations", "routes"} & parts) or (
        "integration" in name or "e2e" in name
    )


def _is_unit_task(files: list[str], context: str, task_commands: list[str]) -> bool:
    if any(_is_unit_path(file_path) for file_path in files):
        return True
    if any(
        _has_token(command, {"jest", "pytest", "test", "unittest", "vitest"})
        for command in task_commands
    ):
        return True
    return _has_token(
        context,
        {"function", "helper", "module", "unit", "pytest", "unittest", "vitest", "jest"},
    )


def _is_unit_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    name = pure_path.name
    return (
        name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith(".test.ts")
        or name.endswith(".test.tsx")
        or name.endswith(".spec.ts")
        or name.endswith(".spec.tsx")
        or "tests" in pure_path.parts
    )


def _task_context(task: Mapping[str, Any], task_commands: list[str]) -> str:
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("acceptance_criteria")),
        *task_commands,
    ]
    return " ".join(value for value in values if value)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            commands.append(command)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
    return _dedupe(commands)


def _plan_validation_commands(plan: Mapping[str, Any]) -> list[str]:
    metadata = plan.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    commands: list[str] = []
    commands.extend(_commands_from_value(metadata.get("validation_commands")))
    commands.extend(_commands_from_value(metadata.get("validation_command")))
    commands.extend(_commands_from_value(metadata.get("test_commands")))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_strings(item))
        return strings
    if isinstance(value, (list, tuple, set)):
        strings: list[str] = []
        for item in value:
            strings.extend(_strings(item))
        return strings
    text = _text(value)
    return [text] if text else []


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(re.findall(r"[a-z0-9][a-z0-9-]*", text.lower())) & tokens)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


__all__ = [
    "IMPACT_CLASSIFICATIONS",
    "TaskTestImpact",
    "TestImpactSummary",
    "build_test_impact_summary",
    "test_impact_summary_to_dict",
]
