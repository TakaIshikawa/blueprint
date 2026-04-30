"""Build ordered validation command sequences for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
import shlex
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


ValidationPhaseName = Literal["quick", "targeted", "integration", "broad"]
CommandClassification = Literal[
    "format",
    "lint",
    "typecheck",
    "targeted_test",
    "integration_test",
    "build",
    "broad_test",
    "custom_validation",
]
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ValidationCommandStep:
    """One de-duplicated validation command with task provenance."""

    command: str
    phase: ValidationPhaseName
    classification: CommandClassification
    task_ids: tuple[str, ...]
    reused: bool
    reason: str
    escalation_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "command": self.command,
            "phase": self.phase,
            "classification": self.classification,
            "task_ids": list(self.task_ids),
            "reused": self.reused,
            "reason": self.reason,
            "escalation_reasons": list(self.escalation_reasons),
        }


@dataclass(frozen=True, slots=True)
class ValidationPhase:
    """Validation commands grouped under one execution phase."""

    name: ValidationPhaseName
    commands: tuple[ValidationCommandStep, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "name": self.name,
            "commands": [command.to_dict() for command in self.commands],
        }


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    """Structured validation sequencing finding."""

    code: str
    severity: str
    task_id: str
    title: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "code": self.code,
            "severity": self.severity,
            "task_id": self.task_id,
            "title": self.title,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class TaskValidationSequence:
    """Ordered validation phases and task-level findings for a plan."""

    plan_id: str | None
    phases: tuple[ValidationPhase, ...]
    findings: tuple[ValidationFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "phases": [phase.to_dict() for phase in self.phases],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def build_task_validation_sequence(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskValidationSequence:
    """Build a de-duplicated validation sequence without executing commands."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    drafts: dict[str, _CommandDraft] = {}
    findings: list[ValidationFinding] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        commands = _task_validation_commands(task)

        if not commands:
            findings.append(
                ValidationFinding(
                    code="missing_task_validation_command",
                    severity="warning",
                    task_id=task_id,
                    title=title,
                    message="Task has no task-level validation command.",
                )
            )
            continue

        escalation_reasons = _path_escalation_reasons(_task_paths(task))
        for command in commands:
            draft = drafts.setdefault(command, _CommandDraft(command=command))
            draft.task_ids.append(task_id)
            draft.first_task_index = min(draft.first_task_index, index)
            draft.escalation_reasons.extend(escalation_reasons)

    steps = [_step_from_draft(draft) for draft in drafts.values()]
    first_task_indexes = {
        draft.command: draft.first_task_index
        for draft in drafts.values()
    }
    phases = tuple(
        ValidationPhase(
            name=phase_name,
            commands=tuple(
                sorted(
                    (step for step in steps if step.phase == phase_name),
                    key=lambda step: (
                        _classification_rank(step.classification),
                        first_task_indexes.get(step.command, 1_000_000),
                        step.command,
                    ),
                )
            ),
        )
        for phase_name in _PHASE_ORDER
    )

    return TaskValidationSequence(
        plan_id=_optional_text(payload.get("id")),
        phases=phases,
        findings=tuple(findings),
    )


def task_validation_sequence_to_dict(sequence: TaskValidationSequence) -> dict[str, Any]:
    """Serialize a task validation sequence to a dictionary."""
    return sequence.to_dict()


task_validation_sequence_to_dict.__test__ = False


@dataclass
class _CommandDraft:
    command: str
    task_ids: list[str] = field(default_factory=list)
    first_task_index: int = 1_000_000
    escalation_reasons: list[str] = field(default_factory=list)


def _step_from_draft(draft: _CommandDraft) -> ValidationCommandStep:
    classification = _classify_command(draft.command)
    escalation_reasons = tuple(_dedupe(draft.escalation_reasons))
    phase = "broad" if escalation_reasons else _phase_for_classification(classification)
    return ValidationCommandStep(
        command=draft.command,
        phase=phase,
        classification=classification,
        task_ids=tuple(_dedupe(draft.task_ids)),
        reused=len(_dedupe(draft.task_ids)) > 1,
        reason=_reason_for_phase(phase, classification, bool(escalation_reasons)),
        escalation_reasons=escalation_reasons,
    )


def _classify_command(command: str) -> CommandClassification:
    tokens = _command_tokens(command)
    normalized = _normalized_command(command)

    if _is_format_command(tokens, normalized):
        return "format"
    if _is_lint_command(tokens, normalized):
        return "lint"
    if _is_typecheck_command(tokens, normalized):
        return "typecheck"
    if _is_integration_command(tokens, normalized):
        return "integration_test"
    if _is_build_command(tokens, normalized):
        return "build"
    if _is_test_command(tokens, normalized):
        return "targeted_test" if _has_targeted_test_selector(tokens) else "broad_test"
    return "custom_validation"


def _phase_for_classification(classification: CommandClassification) -> ValidationPhaseName:
    if classification in {"format", "lint", "typecheck"}:
        return "quick"
    if classification in {"targeted_test", "custom_validation"}:
        return "targeted"
    if classification in {"integration_test", "build"}:
        return "integration"
    return "broad"


def _reason_for_phase(
    phase: ValidationPhaseName,
    classification: CommandClassification,
    escalated: bool,
) -> str:
    if escalated:
        return "Risk-sensitive file scope requires broad validation coverage."
    if phase == "quick":
        return "Fast static validation should run before task-specific tests."
    if phase == "targeted":
        return "Focused task-level validation should run before broader checks."
    if phase == "integration":
        return "Integration validation covers cross-boundary behavior."
    if classification == "broad_test":
        return "Broad test command should run after narrower validation."
    return "Broad validation should run after narrower validation."


def _classification_rank(classification: CommandClassification) -> int:
    return {
        "format": 0,
        "lint": 1,
        "typecheck": 2,
        "targeted_test": 3,
        "custom_validation": 4,
        "integration_test": 5,
        "build": 6,
        "broad_test": 7,
    }[classification]


def _is_format_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"format", "prettier", "black"}) or "ruff format" in normalized


def _is_lint_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"lint", "eslint", "ruff", "flake8", "pylint", "golangci-lint"}) or (
        "ruff check" in normalized
    )


def _is_typecheck_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"mypy", "pyright", "typecheck", "tsc"}) or "tsc --noemit" in normalized


def _is_build_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"build"}) or normalized in {"poetry build", "python -m build"}


def _is_integration_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"playwright", "cypress"}) or bool(
        {"integration", "e2e"} & set(_word_tokens(normalized))
    )


def _is_test_command(tokens: list[str], normalized: str) -> bool:
    return (
        _has_any(tokens, {"pytest", "test", "tests", "jest", "vitest", "mocha", "unittest", "tox"})
        or "python -m pytest" in normalized
    )


def _has_targeted_test_selector(tokens: list[str]) -> bool:
    for token in tokens:
        normalized = token.strip()
        if "::" in normalized:
            return True
        if normalized.startswith("-k") and len(normalized) > 2:
            return True
        if normalized in {"-k", "-m"}:
            return True
        if _looks_like_test_path(normalized):
            return True
    return False


def _looks_like_test_path(value: str) -> bool:
    normalized = value.replace("\\", "/").strip("'\"")
    if not normalized or normalized.startswith("-"):
        return False
    path_parts = normalized.lower().split("/")
    name = path_parts[-1]
    return (
        "tests" in path_parts
        or name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith(".test.ts")
        or name.endswith(".test.tsx")
        or name.endswith(".spec.ts")
        or name.endswith(".spec.tsx")
    )


def _path_escalation_reasons(paths: list[str]) -> list[str]:
    reasons: list[str] = []
    for path in paths:
        category = _risky_path_category(path)
        if category:
            reasons.append(f"{category} path requires broad validation: {path}")
    return _dedupe(reasons)


def _risky_path_category(path: str) -> str:
    normalized = path.lower()
    path_obj = PurePosixPath(normalized)
    parts = set(path_obj.parts)
    tokens = set(_word_tokens(normalized))

    if path_obj.name in _CONFIG_FILENAMES or path_obj.suffix in _CONFIG_SUFFIXES:
        return "config"
    if parts & _SCHEMA_PARTS or tokens & _SCHEMA_TOKENS:
        return "schema"
    if parts & _DATABASE_PARTS or tokens & _DATABASE_TOKENS:
        return "database"
    if parts & _SHARED_PARTS or (
        len(path_obj.parts) >= 2 and path_obj.parts[0] in {"packages", "libs"}
    ):
        return "shared"
    return ""


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
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


def _command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def _normalized_command(command: str) -> str:
    return " ".join(command.lower().split())


def _word_tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9-]*", value.lower())


def _has_any(tokens: list[str], values: set[str]) -> bool:
    normalized_tokens = {_token_name(token) for token in tokens}
    return bool(normalized_tokens & values)


def _token_name(token: str) -> str:
    value = token.strip().lower()
    if value.startswith("run:"):
        value = value.removeprefix("run:")
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    return value


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_PHASE_ORDER: tuple[ValidationPhaseName, ...] = ("quick", "targeted", "integration", "broad")
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


__all__ = [
    "CommandClassification",
    "TaskValidationSequence",
    "ValidationCommandStep",
    "ValidationFinding",
    "ValidationPhase",
    "ValidationPhaseName",
    "build_task_validation_sequence",
    "task_validation_sequence_to_dict",
]
