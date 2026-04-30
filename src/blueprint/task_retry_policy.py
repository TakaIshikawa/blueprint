"""Derive retry and escalation policies for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


@dataclass(frozen=True)
class TaskRetryPolicy:
    """Retry policy for one autonomous execution task."""

    task_id: str
    title: str
    max_attempts: int
    retryable_failure_patterns: list[str] = field(default_factory=list)
    stop_conditions: list[str] = field(default_factory=list)
    escalation_reasons: list[str] = field(default_factory=list)
    required_context_on_retry: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "max_attempts": self.max_attempts,
            "retryable_failure_patterns": list(self.retryable_failure_patterns),
            "stop_conditions": list(self.stop_conditions),
            "escalation_reasons": list(self.escalation_reasons),
            "required_context_on_retry": list(self.required_context_on_retry),
        }


def generate_task_retry_policies(
    plan: Mapping[str, Any] | ExecutionPlan,
    default_max_attempts: int = 2,
) -> list[TaskRetryPolicy]:
    """Return deterministic retry policies for each task in plan order."""
    plan_payload = _validated_plan_payload(plan)
    tasks = _list_of_task_payloads(plan_payload.get("tasks"))
    task_statuses = {
        _text(task.get("id")): _text(task.get("status")).lower() or "pending"
        for task in tasks
        if _text(task.get("id"))
    }
    plan_commands = _plan_validation_commands(plan_payload)
    policies: list[TaskRetryPolicy] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title"))
        files = _string_list(task.get("files_or_modules"))
        acceptance_criteria = _string_list(task.get("acceptance_criteria"))
        commands = _dedupe([*_task_validation_commands(task), *plan_commands])
        context_text = _task_context_text(task, acceptance_criteria, files, commands)
        dependencies = _string_list(task.get("depends_on"))
        unresolved_dependencies = _unresolved_dependencies(dependencies, task_statuses)

        high_risk = _is_high_risk(task, context_text)
        migration_or_data = _is_migration_or_data(files, context_text)
        security_sensitive = _is_security_sensitive(files, context_text)
        destructive_commands = _destructive_commands(commands)
        has_validation = bool(commands)

        policies.append(
            TaskRetryPolicy(
                task_id=task_id,
                title=title,
                max_attempts=_max_attempts(
                    default_max_attempts,
                    unresolved_dependencies=unresolved_dependencies,
                    high_risk=high_risk,
                    migration_or_data=migration_or_data,
                    security_sensitive=security_sensitive,
                    destructive_commands=destructive_commands,
                ),
                retryable_failure_patterns=_retryable_failure_patterns(
                    has_validation=has_validation,
                    unresolved_dependencies=unresolved_dependencies,
                    destructive_commands=destructive_commands,
                ),
                stop_conditions=_stop_conditions(
                    unresolved_dependencies=unresolved_dependencies,
                    high_risk=high_risk,
                    migration_or_data=migration_or_data,
                    security_sensitive=security_sensitive,
                    destructive_commands=destructive_commands,
                ),
                escalation_reasons=_escalation_reasons(
                    unresolved_dependencies=unresolved_dependencies,
                    high_risk=high_risk,
                    migration_or_data=migration_or_data,
                    security_sensitive=security_sensitive,
                    destructive_commands=destructive_commands,
                ),
                required_context_on_retry=_required_context_on_retry(
                    has_validation=has_validation,
                    migration_or_data=migration_or_data,
                    security_sensitive=security_sensitive,
                    destructive_commands=destructive_commands,
                ),
            )
        )

    return policies


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


def _max_attempts(
    default_max_attempts: int,
    *,
    unresolved_dependencies: list[str],
    high_risk: bool,
    migration_or_data: bool,
    security_sensitive: bool,
    destructive_commands: list[str],
) -> int:
    baseline = max(0, default_max_attempts)
    if unresolved_dependencies:
        return 0
    if destructive_commands:
        return min(baseline, 1)
    if high_risk or migration_or_data or security_sensitive:
        return min(baseline, 1)
    return baseline


def _retryable_failure_patterns(
    *,
    has_validation: bool,
    unresolved_dependencies: list[str],
    destructive_commands: list[str],
) -> list[str]:
    if unresolved_dependencies:
        return []

    patterns = ["transient tooling or environment failure"]
    if has_validation:
        patterns.extend(
            [
                "test failure after implementation changes",
                "lint, format, typecheck, or build validation failure",
            ]
        )
    if destructive_commands:
        patterns.append("non-destructive validation failure before any data-changing command runs")
    return _dedupe(patterns)


def _stop_conditions(
    *,
    unresolved_dependencies: list[str],
    high_risk: bool,
    migration_or_data: bool,
    security_sensitive: bool,
    destructive_commands: list[str],
) -> list[str]:
    conditions = [
        "Stop when max_attempts is reached.",
        "Stop if the next attempt would modify files outside the task scope.",
    ]
    if unresolved_dependencies:
        conditions.insert(
            0,
            "Do not start or retry while dependencies are unresolved: "
            f"{', '.join(unresolved_dependencies)}.",
        )
    if high_risk:
        conditions.append("Stop after one failed attempt on high-risk task behavior.")
    if migration_or_data:
        conditions.append(
            "Stop before rerunning migration, backfill, import, or data operation steps."
        )
    if security_sensitive:
        conditions.append("Stop before changing authentication, authorization, or secret handling.")
    if destructive_commands:
        rendered = "; ".join(destructive_commands)
        conditions.append(f"Stop immediately if a destructive command fails or must be rerun: {rendered}.")
    return _dedupe(conditions)


def _escalation_reasons(
    *,
    unresolved_dependencies: list[str],
    high_risk: bool,
    migration_or_data: bool,
    security_sensitive: bool,
    destructive_commands: list[str],
) -> list[str]:
    reasons = ["Retry budget exhausted without satisfying the task acceptance criteria."]
    if unresolved_dependencies:
        reasons.append(
            "Dependencies are missing or not completed: " f"{', '.join(unresolved_dependencies)}."
        )
    if high_risk:
        reasons.append("High-risk task failed and needs reviewer direction before another attempt.")
    if migration_or_data:
        reasons.append("Migration or data operation failed and needs logs, rollback status, and review.")
    if security_sensitive:
        reasons.append("Security-sensitive path changed or failed validation and needs reviewer review.")
    if destructive_commands:
        reasons.append("Destructive command failed, was requested, or would need to be rerun.")
    return _dedupe(reasons)


def _required_context_on_retry(
    *,
    has_validation: bool,
    migration_or_data: bool,
    security_sensitive: bool,
    destructive_commands: list[str],
) -> list[str]:
    context = [
        "previous attempt summary",
        "files changed in the previous attempt",
        "current diff or patch summary",
    ]
    if has_validation:
        context.append("validation command output")
    if migration_or_data:
        context.append("migration, backfill, import, or data verification logs")
    if security_sensitive:
        context.append("security-sensitive files changed and reviewer concern summary")
    if destructive_commands:
        context.append("destructive command output and rollback or recovery status")
    return _dedupe(context)


def _unresolved_dependencies(
    dependencies: list[str],
    task_statuses: Mapping[str, str],
) -> list[str]:
    unresolved: list[str] = []
    for dependency in dependencies:
        status = task_statuses.get(dependency)
        if status != "completed":
            unresolved.append(dependency)
    return unresolved


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
        commands.extend(_commands_from_value(metadata.get("commands")))

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


def _is_high_risk(task: Mapping[str, Any], context_text: str) -> bool:
    risk_level = _text(task.get("risk_level")).lower()
    if risk_level in {"high", "critical", "blocker"}:
        return True
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        risks = " ".join(_string_list(metadata.get("risks")))
        if _has_token(risks, {"critical", "high", "destructive", "security"}):
            return True
    return _has_token(context_text, {"high-risk", "critical"})


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


def _is_security_sensitive(files: list[str], context_text: str) -> bool:
    if any(
        _path_matches(file_path, _SECURITY_PATH_PARTS, _SECURITY_SUFFIXES)
        or _security_path_name(file_path)
        for file_path in files
    ):
        return True
    return _has_token(
        context_text,
        {
            "auth",
            "authentication",
            "authorization",
            "credential",
            "crypto",
            "encryption",
            "permission",
            "secret",
            "security",
            "token",
        },
    )


def _destructive_commands(commands: list[str]) -> list[str]:
    return [command for command in commands if _is_destructive_command(command)]


def _is_destructive_command(command: str) -> bool:
    normalized = re.sub(r"\s+", " ", command.strip().lower())
    destructive_patterns = [
        r"\brm\s+(-[a-z]*r[a-z]*f|-rf|-fr)\b",
        r"\bgit\s+reset\s+--hard\b",
        r"\bgit\s+clean\s+-[a-z]*f",
        r"\bdrop\s+(table|database|schema)\b",
        r"\btruncate\s+(table\s+)?[a-z0-9_.]+",
        r"\bdelete\s+from\s+[a-z0-9_.]+",
        r"\bdestroy\b",
    ]
    return any(re.search(pattern, normalized) for pattern in destructive_patterns)


def _task_context_text(
    task: Mapping[str, Any],
    acceptance_criteria: list[str],
    files: list[str],
    commands: list[str],
) -> str:
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *acceptance_criteria,
        *files,
        *commands,
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


def _security_path_name(path: str) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    parts = set(PurePosixPath(normalized).parts)
    name = PurePosixPath(normalized).name
    return name in _SECURITY_FILENAMES or bool(parts & _SECURITY_NAME_PARTS)


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
_SECURITY_PATH_PARTS = {
    "auth",
    "authentication",
    "authorization",
    "crypto",
    "iam",
    "oauth",
    "permissions",
    "security",
    "secrets",
}
_SECURITY_SUFFIXES = (".pem", ".key", ".crt", ".p12")
_SECURITY_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "credentials.json",
    "secrets.yml",
    "secrets.yaml",
}
_SECURITY_NAME_PARTS = {
    "auth",
    "credentials",
    "jwt",
    "oauth",
    "permissions",
    "secrets",
    "security",
    "tokens",
}


__all__ = [
    "TaskRetryPolicy",
    "generate_task_retry_policies",
]
