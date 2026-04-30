"""Derive rollback guidance for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


@dataclass(frozen=True)
class TaskRollbackPlan:
    """Rollback guidance for one autonomous execution task."""

    task_id: str
    rollback_strategy: str
    checkpoint_files: list[str] = field(default_factory=list)
    verification_commands: list[str] = field(default_factory=list)
    manual_review_required: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "rollback_strategy": self.rollback_strategy,
            "checkpoint_files": list(self.checkpoint_files),
            "verification_commands": list(self.verification_commands),
            "manual_review_required": self.manual_review_required,
            "notes": list(self.notes),
        }


def generate_task_rollback_plans(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> list[TaskRollbackPlan]:
    """Return deterministic rollback plans for each task in plan order."""
    plan_payload = _validated_plan_payload(plan)
    plan_commands = _plan_validation_commands(plan_payload)
    rollback_plans: list[TaskRollbackPlan] = []

    for index, task in enumerate(_list_of_task_payloads(plan_payload.get("tasks")), start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        files = _string_list(task.get("files_or_modules"))
        acceptance_criteria = _string_list(task.get("acceptance_criteria"))
        task_commands = _task_validation_commands(task)
        commands = _dedupe([*task_commands, *plan_commands])
        context_text = _task_context_text(task, acceptance_criteria, files)
        risk_profile = _risk_profile(task, files, context_text)
        metadata_hints = _metadata_rollback_hints(task.get("metadata"))
        manual_review_required = _manual_review_required(task, risk_profile)

        rollback_plans.append(
            TaskRollbackPlan(
                task_id=task_id,
                rollback_strategy=_rollback_strategy(
                    files,
                    risk_profile,
                    manual_review_required=manual_review_required,
                    has_metadata_hints=bool(metadata_hints),
                ),
                checkpoint_files=files,
                verification_commands=_verification_commands(
                    commands,
                    risk_profile,
                    manual_review_required=manual_review_required,
                ),
                manual_review_required=manual_review_required,
                notes=_notes(files, acceptance_criteria, risk_profile, metadata_hints),
            )
        )

    return rollback_plans


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


def _rollback_strategy(
    files: list[str],
    risk_profile: list[str],
    *,
    manual_review_required: bool,
    has_metadata_hints: bool,
) -> str:
    if not files:
        return (
            "Capture the actual files changed, then revert only this task's diff after reviewer "
            "confirmation."
        )
    if "data/schema" in risk_profile:
        return (
            "Create a pre-change backup or migration checkpoint, apply a reviewed down migration "
            "or restore path, and revert related code changes."
        )
    if "auth/security" in risk_profile:
        return (
            "Restore the previous auth or security behavior from checkpointed files and confirm "
            "credential, token, and permission handling with a reviewer."
        )
    if "config" in risk_profile:
        return (
            "Restore the previous configuration values from checkpointed files and verify the "
            "application starts with the old settings."
        )
    if "integration" in risk_profile:
        return (
            "Disable or revert the integration change, restore previous contract settings, and "
            "validate dependent service behavior."
        )
    if manual_review_required:
        return "Revert the task-scoped diff from checkpointed files after manual review."
    if has_metadata_hints:
        return "Use the task rollback hint first, then revert the task-scoped file changes."
    if _is_lightweight_scope(files):
        return "Revert the documented or isolated file changes from version control."
    return "Revert the task-scoped file changes from version control and rerun focused validation."


def _verification_commands(
    commands: list[str],
    risk_profile: list[str],
    *,
    manual_review_required: bool,
) -> list[str]:
    verification = [*commands]
    if manual_review_required:
        verification.append("git diff --check")
    if "data/schema" in risk_profile:
        verification.append("Verify migration rollback or data restore on a non-production snapshot.")
    if "auth/security" in risk_profile:
        verification.append("Run focused auth, permission, and secret-handling validation.")
    if "config" in risk_profile:
        verification.append("Run application startup or configuration validation with restored settings.")
    if "integration" in risk_profile:
        verification.append("Run integration contract or smoke validation for affected external systems.")
    if not verification:
        verification.append("Review git diff to confirm only intended task files changed.")
    return _dedupe(verification)


def _notes(
    files: list[str],
    acceptance_criteria: list[str],
    risk_profile: list[str],
    metadata_hints: list[str],
) -> list[str]:
    notes: list[str] = []
    if not files:
        notes.append("No files_or_modules were listed; identify changed files before rollback.")
    if not acceptance_criteria:
        notes.append("No acceptance criteria were listed; define the post-rollback behavior to verify.")
    if risk_profile:
        notes.append(f"Manual review required for risk profile: {', '.join(risk_profile)}.")
    if metadata_hints:
        notes.extend(f"Task rollback hint: {hint}" for hint in metadata_hints)
    if _is_lightweight_scope(files) and not risk_profile:
        notes.append("Lightweight scope; version-control rollback and a diff review should be enough.")
    return _dedupe(notes)


def _manual_review_required(task: Mapping[str, Any], risk_profile: list[str]) -> bool:
    risk_level = _text(task.get("risk_level")).lower()
    return risk_level in {"high", "critical", "blocker"} or bool(risk_profile)


def _risk_profile(task: Mapping[str, Any], files: list[str], context_text: str) -> list[str]:
    profile: list[str] = []
    risk_level = _text(task.get("risk_level")).lower()
    if risk_level in {"high", "critical", "blocker"}:
        profile.append("high-risk")
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        profile.extend(
            "high-risk"
            for risk in _string_list(metadata.get("risks"))
            if _has_token(risk, {"critical", "destructive", "high"})
        )
    if _is_data_or_schema(files, context_text):
        profile.append("data/schema")
    if _is_auth_or_security(files, context_text):
        profile.append("auth/security")
    if _is_config(files, context_text):
        profile.append("config")
    if _is_integration(files, context_text):
        profile.append("integration")
    return _dedupe(profile)


def _metadata_rollback_hints(metadata: Any) -> list[str]:
    if not isinstance(metadata, Mapping):
        return []

    hints: list[str] = []
    for key in (
        "rollback",
        "rollback_hint",
        "rollback_hints",
        "rollback_plan",
        "rollback_strategy",
        "rollback_steps",
    ):
        hints.extend(_strings_from_value(metadata.get(key)))
    return _dedupe(hints)


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
    return _strings_from_value(value)


def _strings_from_value(value: Any) -> list[str]:
    if isinstance(value, list):
        return _string_list(value)
    if isinstance(value, Mapping):
        return _string_list(list(value.values()))
    text = _text(value)
    return [text] if text else []


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


def _is_data_or_schema(files: list[str], context_text: str) -> bool:
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


def _is_auth_or_security(files: list[str], context_text: str) -> bool:
    if any(
        _path_matches(file_path, _SECURITY_PATH_PARTS, _SECURITY_SUFFIXES)
        or _sensitive_filename(file_path)
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
            "permission",
            "secret",
            "security",
            "token",
        },
    )


def _is_config(files: list[str], context_text: str) -> bool:
    if any(_path_matches(file_path, _CONFIG_PATH_PARTS, _CONFIG_SUFFIXES) for file_path in files):
        return True
    if any(_normalized_path(file_path) in _CONFIG_FILENAMES for file_path in files):
        return True
    return _has_token(
        context_text,
        {"config", "configuration", "environment", "feature-flag", "setting", "settings"},
    )


def _is_integration(files: list[str], context_text: str) -> bool:
    if any(_path_matches(file_path, _INTEGRATION_PATH_PARTS, ()) for file_path in files):
        return True
    return _has_token(
        context_text,
        {
            "external",
            "integration",
            "oauth",
            "third-party",
            "webhook",
        },
    )


def _is_lightweight_scope(files: list[str]) -> bool:
    if not files:
        return False
    return all(
        _path_matches(file_path, {"docs", "documentation", "exporters"}, _DOC_SUFFIXES)
        for file_path in files
    )


def _path_matches(path: str, path_parts: set[str], suffixes: tuple[str, ...]) -> bool:
    normalized = _normalized_path(path)
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    if suffixes and pure_path.suffix in suffixes:
        return True
    return any(part in path_parts for part in pure_path.parts)


def _sensitive_filename(path: str) -> bool:
    normalized = _normalized_path(path)
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    return pure_path.name in _SECURITY_FILENAMES


def _normalized_path(path: str) -> str:
    return path.strip().replace("\\", "/").lower().strip("/")


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
_CONFIG_PATH_PARTS = {
    "config",
    "configuration",
    "settings",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".toml", ".yaml", ".yml")
_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    "docker-compose.yml",
    "package.json",
    "pyproject.toml",
}
_INTEGRATION_PATH_PARTS = {
    "clients",
    "connectors",
    "integrations",
    "providers",
    "webhooks",
}
_DOC_SUFFIXES = (".md", ".mdx", ".rst", ".txt")


__all__ = [
    "TaskRollbackPlan",
    "generate_task_rollback_plans",
]
