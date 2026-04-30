"""Build concise reviewer packets for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class TaskReviewPacket:
    """Review summary for one autonomous-agent task."""

    task_id: str
    title: str
    expected_files: tuple[str, ...] = field(default_factory=tuple)
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    dependencies: tuple[str, ...] = field(default_factory=tuple)
    risk_indicators: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)
    reviewer_focus_areas: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "expected_files": list(self.expected_files),
            "acceptance_criteria": list(self.acceptance_criteria),
            "dependencies": list(self.dependencies),
            "risk_indicators": list(self.risk_indicators),
            "validation_commands": list(self.validation_commands),
            "reviewer_focus_areas": list(self.reviewer_focus_areas),
        }


def build_task_review_packets(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> dict[str, TaskReviewPacket]:
    """Build one deterministic review packet per task, keyed by task id."""
    payload = _plan_payload(plan)
    packets: dict[str, TaskReviewPacket] = {}

    for index, task in enumerate(_task_payloads(payload.get("tasks")), start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        expected_files = tuple(_task_paths(task))
        acceptance_criteria = tuple(_strings(task.get("acceptance_criteria")))
        dependencies = tuple(_strings(task.get("depends_on")))
        validation_commands = tuple(_task_validation_commands(task))
        risk_indicators = tuple(
            _risk_indicators(
                task,
                expected_files=expected_files,
                dependencies=dependencies,
                acceptance_criteria=acceptance_criteria,
                validation_commands=validation_commands,
            )
        )

        packets[task_id] = TaskReviewPacket(
            task_id=task_id,
            title=title,
            expected_files=expected_files,
            acceptance_criteria=acceptance_criteria,
            dependencies=dependencies,
            risk_indicators=risk_indicators,
            validation_commands=validation_commands,
            reviewer_focus_areas=tuple(
                _reviewer_focus_areas(
                    expected_files=expected_files,
                    dependencies=dependencies,
                    risk_indicators=risk_indicators,
                    acceptance_criteria=acceptance_criteria,
                    validation_commands=validation_commands,
                )
            ),
        )

    return packets


def task_review_packets_to_dict(
    packets: Mapping[str, TaskReviewPacket],
) -> dict[str, dict[str, Any]]:
    """Serialize task review packets to plain dictionaries."""
    return {task_id: packet.to_dict() for task_id, packet in packets.items()}


def _risk_indicators(
    task: Mapping[str, Any],
    *,
    expected_files: tuple[str, ...],
    dependencies: tuple[str, ...],
    acceptance_criteria: tuple[str, ...],
    validation_commands: tuple[str, ...],
) -> list[str]:
    indicators: list[str] = []

    for path in expected_files:
        for category in _path_categories(path):
            indicators.append(f"{category} path: {path}")

    risk_level = _text(task.get("risk_level")).lower()
    if risk_level in {"medium", "high", "critical"}:
        indicators.append(f"{risk_level} risk task metadata")

    if len(dependencies) >= _DEPENDENCY_HEAVY_THRESHOLD:
        indicators.append(f"depends on {len(dependencies)} task(s)")

    if not acceptance_criteria:
        indicators.append("missing acceptance criteria")
    elif _has_weak_acceptance_criteria(acceptance_criteria):
        indicators.append("weak acceptance criteria")

    if not validation_commands:
        indicators.append("missing validation command")

    return _dedupe(indicators)


def _reviewer_focus_areas(
    *,
    expected_files: tuple[str, ...],
    dependencies: tuple[str, ...],
    risk_indicators: tuple[str, ...],
    acceptance_criteria: tuple[str, ...],
    validation_commands: tuple[str, ...],
) -> list[str]:
    focus: list[str] = []

    risky_paths = [
        indicator for indicator in risk_indicators if indicator.endswith(tuple(expected_files))
    ]
    if risky_paths:
        focus.append("Inspect risky file changes for configuration, data, schema, or shared-impact regressions.")

    if len(dependencies) >= _DEPENDENCY_HEAVY_THRESHOLD:
        focus.append("Verify dependency contracts and task ordering across upstream work.")

    if not validation_commands:
        focus.append("Require concrete validation evidence because no task-level test command is listed.")

    if not acceptance_criteria:
        focus.append("Clarify expected behavior because acceptance criteria are missing.")
    elif _has_weak_acceptance_criteria(acceptance_criteria):
        focus.append("Translate broad acceptance criteria into observable review checks.")

    risk_levels = {"high risk task metadata", "critical risk task metadata"}
    if set(risk_indicators) & risk_levels:
        focus.append("Give the implementation a deeper review because task risk is elevated.")

    if not focus:
        focus.append("Confirm changed files match the task scope and validation command output.")

    return _dedupe(focus)


def _path_categories(path: str) -> list[str]:
    normalized = path.lower()
    path_obj = PurePosixPath(normalized)
    parts = set(path_obj.parts)
    tokens = set(_word_tokens(normalized))
    categories: list[str] = []

    if path_obj.name in _CONFIG_FILENAMES or path_obj.suffix in _CONFIG_SUFFIXES:
        categories.append("config")
    if parts & _SCHEMA_PARTS or tokens & _SCHEMA_TOKENS:
        categories.append("schema")
    if parts & _DATABASE_PARTS or tokens & _DATABASE_TOKENS:
        categories.append("database")
    if parts & _SHARED_PARTS or (
        len(path_obj.parts) >= 2 and path_obj.parts[0] in {"packages", "libs"}
    ):
        categories.append("shared")
    if parts & _CLI_PARTS or path_obj.name in _CLI_FILENAMES:
        categories.append("CLI")

    return categories


def _has_weak_acceptance_criteria(criteria: tuple[str, ...]) -> bool:
    if len(criteria) == 1 and len(criteria[0].split()) <= 4:
        return True
    normalized = " ".join(criteria).lower()
    return bool(_WEAK_ACCEPTANCE_RE.search(normalized))


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    if isinstance(plan, Mapping):
        return dict(plan)
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        pass
    return {}


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


def _task_paths(task: Mapping[str, Any]) -> list[str]:
    paths = [
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("expectedFiles")),
        *_strings(task.get("expected_files")),
    ]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        paths.extend(_strings(metadata.get("expectedFiles")))
        paths.extend(_strings(metadata.get("expected_files")))
        paths.extend(_strings(metadata.get("files")))
    return _dedupe([_normalized_path(path) for path in paths])


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


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


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
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return []


def _normalized_path(value: str) -> str:
    return value.strip().replace("\\", "/").strip("/")


def _word_tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9-]*", value.lower())


def _dedupe(values: list[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_DEPENDENCY_HEAVY_THRESHOLD = 2
_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "dockerfile",
    "package.json",
    "pyproject.toml",
    "settings.py",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".json", ".toml", ".yaml", ".yml")
_SCHEMA_PARTS = {"schema", "schemas"}
_SCHEMA_TOKENS = {"schema", "schemas", "graphql", "openapi", "proto", "protobuf"}
_DATABASE_PARTS = {"db", "database", "databases", "migration", "migrations", "sql"}
_DATABASE_TOKENS = {"database", "migration", "migrations", "sql", "sqlite"}
_SHARED_PARTS = {"common", "core", "infra", "infrastructure", "shared"}
_CLI_PARTS = {"cli", "commands"}
_CLI_FILENAMES = {"cli.py", "command.py", "commands.py"}
_WEAK_ACCEPTANCE_RE = re.compile(
    r"\b(done|works|complete|completed|implemented|handled|covered)\b"
)


__all__ = [
    "TaskReviewPacket",
    "build_task_review_packets",
    "task_review_packets_to_dict",
]
