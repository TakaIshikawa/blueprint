"""Derive completion evidence requirements for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


@dataclass(frozen=True)
class TaskEvidenceRequirement:
    """Completion evidence required for one execution task."""

    task_id: str
    title: str
    required_artifacts: list[str] = field(default_factory=list)
    suggested_commands: list[str] = field(default_factory=list)
    risk_notes: list[str] = field(default_factory=list)
    completion_checklist: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "required_artifacts": list(self.required_artifacts),
            "suggested_commands": list(self.suggested_commands),
            "risk_notes": list(self.risk_notes),
            "completion_checklist": list(self.completion_checklist),
        }


def build_task_evidence_requirements(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> list[TaskEvidenceRequirement]:
    """Return deterministic evidence requirements for each task in plan order."""
    plan_payload = _validated_plan_payload(plan)
    plan_commands = _plan_validation_commands(plan_payload)
    requirements: list[TaskEvidenceRequirement] = []

    for index, task in enumerate(_list_of_task_payloads(plan_payload.get("tasks")), start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title"))
        files = _string_list(task.get("files_or_modules"))
        acceptance_criteria = _string_list(task.get("acceptance_criteria"))
        task_commands = _task_validation_commands(task)
        suggested_commands = _dedupe([*task_commands, *plan_commands])
        context_text = _task_context_text(task, acceptance_criteria, files)

        ui_facing = _is_ui_facing(files, context_text)
        migration_or_data = _is_migration_or_data(files, context_text)
        high_risk = _is_high_risk(task)
        cross_cutting = _is_cross_cutting(files, context_text)

        requirements.append(
            TaskEvidenceRequirement(
                task_id=task_id,
                title=title,
                required_artifacts=_required_artifacts(
                    ui_facing=ui_facing,
                    migration_or_data=migration_or_data,
                    has_commands=bool(suggested_commands),
                    needs_reviewer_notes=high_risk or cross_cutting,
                ),
                suggested_commands=suggested_commands,
                risk_notes=_risk_notes(
                    task,
                    files,
                    acceptance_criteria,
                    high_risk=high_risk,
                    cross_cutting=cross_cutting,
                    ui_facing=ui_facing,
                    migration_or_data=migration_or_data,
                ),
                completion_checklist=_completion_checklist(
                    files,
                    acceptance_criteria,
                    suggested_commands,
                    ui_facing=ui_facing,
                    migration_or_data=migration_or_data,
                    needs_reviewer_notes=high_risk or cross_cutting,
                ),
            )
        )

    return requirements


def _validated_plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _list_of_task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _required_artifacts(
    *,
    ui_facing: bool,
    migration_or_data: bool,
    has_commands: bool,
    needs_reviewer_notes: bool,
) -> list[str]:
    artifacts = ["implementation summary"]
    if has_commands:
        artifacts.append("test output")
    if ui_facing:
        artifacts.append("screenshots")
    if migration_or_data:
        artifacts.append("migration or data operation logs")
    if needs_reviewer_notes:
        artifacts.append("reviewer notes")
    return artifacts


def _completion_checklist(
    files: list[str],
    acceptance_criteria: list[str],
    suggested_commands: list[str],
    *,
    ui_facing: bool,
    migration_or_data: bool,
    needs_reviewer_notes: bool,
) -> list[str]:
    checklist = ["Summarize the implementation and any intentional scope limits."]
    if files:
        checklist.append("Confirm the touched files or modules match the task scope.")
    else:
        checklist.append("Identify the files or modules changed while completing the task.")

    if acceptance_criteria:
        checklist.extend(
            f"Verify acceptance criterion: {criterion}" for criterion in acceptance_criteria
        )
    else:
        checklist.append("Record the observable behavior that proves the task is complete.")

    if suggested_commands:
        checklist.append("Run the suggested validation commands and capture their output.")
    else:
        checklist.append("Run the smallest relevant validation available and note the result.")
    if ui_facing:
        checklist.append("Capture before/after or final-state screenshots for changed UI surfaces.")
    if migration_or_data:
        checklist.append("Capture migration, backfill, import, or data verification logs.")
    if needs_reviewer_notes:
        checklist.append("Add reviewer notes covering risk, rollout, and affected shared surfaces.")
    return checklist


def _risk_notes(
    task: Mapping[str, Any],
    files: list[str],
    acceptance_criteria: list[str],
    *,
    high_risk: bool,
    cross_cutting: bool,
    ui_facing: bool,
    migration_or_data: bool,
) -> list[str]:
    notes: list[str] = []
    risk_level = _text(task.get("risk_level")).lower()
    if high_risk:
        notes.append(f"Task risk level is {risk_level or 'high'}; include reviewer notes.")
    if cross_cutting:
        notes.append("Task appears cross-cutting; call out shared behavior and rollout concerns.")
    if ui_facing:
        notes.append("UI-facing change; screenshots are required as visual evidence.")
    if migration_or_data:
        notes.append("Migration or data change; preserve execution logs and verification counts.")
    if not files:
        notes.append("No task files were listed; evidence should name the files actually changed.")
    if not acceptance_criteria:
        notes.append(
            "No acceptance criteria were listed; evidence should define the observed "
            "completion signal."
        )

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        notes.extend(f"Task risk: {risk}" for risk in _string_list(metadata.get("risks")))

    return _dedupe(notes)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        value = task.get(key)
        if _text(value):
            commands.append(_text(value))

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
    return _commands_from_value(metadata.get("validation_commands"))


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    if isinstance(value, list):
        return _string_list(value)
    command = _text(value)
    return [command] if command else []


def _is_ui_facing(files: list[str], context_text: str) -> bool:
    if any(_path_matches(file_path, _UI_PATH_PARTS, _UI_SUFFIXES) for file_path in files):
        return True
    return _has_token(
        context_text,
        {
            "browser",
            "component",
            "frontend",
            "responsive",
            "screen",
            "screenshot",
            "style",
            "ui",
            "ux",
            "visual",
        },
    )


def _is_migration_or_data(files: list[str], context_text: str) -> bool:
    if any(_path_matches(file_path, _DATA_PATH_PARTS, _DATA_SUFFIXES) for file_path in files):
        return True
    return _has_token(
        context_text,
        {
            "backfill",
            "database",
            "dataset",
            "etl",
            "import",
            "migration",
            "schema",
            "sql",
        },
    )


def _is_high_risk(task: Mapping[str, Any]) -> bool:
    risk_level = _text(task.get("risk_level")).lower()
    if risk_level in {"high", "critical", "blocker"}:
        return True
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        return _has_token(" ".join(_string_list(metadata.get("risks"))), {"critical", "high"})
    return False


def _is_cross_cutting(files: list[str], context_text: str) -> bool:
    directories = {_top_directory(file_path) for file_path in files if _top_directory(file_path)}
    if len(files) >= 5 or len(directories) >= 3:
        return True
    if any(file_path in _CROSS_CUTTING_FILES for file_path in files):
        return True
    return _has_token(
        context_text,
        {"architecture", "cross-cutting", "global", "refactor", "shared", "system-wide"},
    )


def _task_context_text(
    task: Mapping[str, Any],
    acceptance_criteria: list[str],
    files: list[str],
) -> str:
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *acceptance_criteria,
        *files,
    ]
    return " ".join(value for value in values if value).lower()


def _path_matches(path: str, path_parts: set[str], suffixes: tuple[str, ...]) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    if pure_path.suffix in suffixes:
        return True
    return any(part in path_parts for part in pure_path.parts)


def _top_directory(path: str) -> str:
    normalized = path.strip().replace("\\", "/").strip("/")
    return normalized.split("/", 1)[0] if normalized else ""


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(re.findall(r"[a-z0-9][a-z0-9-]*", text.lower())) & tokens)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


_UI_PATH_PARTS = {
    "components",
    "frontend",
    "pages",
    "public",
    "screens",
    "styles",
    "ui",
    "views",
}
_UI_SUFFIXES = (".css", ".html", ".jsx", ".scss", ".svelte", ".tsx", ".vue")
_DATA_PATH_PARTS = {
    "alembic",
    "data",
    "db",
    "etl",
    "importers",
    "migrations",
    "schema",
    "sql",
}
_DATA_SUFFIXES = (".sql", ".db", ".sqlite")
_CROSS_CUTTING_FILES = {
    "package.json",
    "poetry.lock",
    "pyproject.toml",
    "requirements.txt",
    "src/blueprint/cli.py",
}


__all__ = [
    "TaskEvidenceRequirement",
    "build_task_evidence_requirements",
]
