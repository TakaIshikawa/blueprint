"""Recommend validation artifacts for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


ArtifactType = Literal[
    "api_sample",
    "config_review",
    "log_excerpt",
    "manual_verification_note",
    "migration_note",
    "schema_review",
    "screenshot",
    "test_output",
]

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")


@dataclass(frozen=True, slots=True)
class TaskValidationArtifact:
    """One expected validation artifact for a task."""

    type: ArtifactType
    label: str
    reason: str
    command: str | None = None
    paths: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "type": self.type,
            "label": self.label,
            "reason": self.reason,
            "command": self.command,
            "paths": list(self.paths),
        }


@dataclass(frozen=True, slots=True)
class TaskValidationArtifactEntry:
    """Validation artifact expectations for one execution task."""

    task_id: str
    title: str
    risk_level: str
    artifacts: tuple[TaskValidationArtifact, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }


@dataclass(frozen=True, slots=True)
class TaskValidationArtifactPlan:
    """Validation artifact plan for an execution plan."""

    plan_id: str | None
    tasks: tuple[TaskValidationArtifactEntry, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "tasks": [task.to_dict() for task in self.tasks],
            "summary": dict(self.summary),
        }


def build_task_validation_artifact_plan(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskValidationArtifactPlan:
    """Recommend validation artifacts for each task in an execution plan."""
    payload = _plan_payload(plan)
    entries = tuple(
        _task_entry(task, index)
        for index, task in enumerate(_task_payloads(payload.get("tasks")), start=1)
    )
    return TaskValidationArtifactPlan(
        plan_id=_optional_text(payload.get("id")),
        tasks=entries,
        summary=_summary(entries),
    )


def task_validation_artifact_plan_to_dict(
    artifact_plan: TaskValidationArtifactPlan,
) -> dict[str, Any]:
    """Serialize a task validation artifact plan to a dictionary."""
    return artifact_plan.to_dict()


task_validation_artifact_plan_to_dict.__test__ = False


def _task_entry(task: Mapping[str, Any], index: int) -> TaskValidationArtifactEntry:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or ""
    risk_level = _risk_level(task)
    files = _strings(task.get("files_or_modules"))
    acceptance_criteria = _strings(task.get("acceptance_criteria"))
    test_command = _optional_text(task.get("test_command"))
    context_text = _context_text(task, files, acceptance_criteria)

    artifacts = _artifacts(
        task=task,
        files=files,
        context_text=context_text,
        test_command=test_command,
        risk_level=risk_level,
    )
    return TaskValidationArtifactEntry(
        task_id=task_id,
        title=title,
        risk_level=risk_level,
        artifacts=tuple(artifacts),
    )


def _artifacts(
    *,
    task: Mapping[str, Any],
    files: list[str],
    context_text: str,
    test_command: str | None,
    risk_level: str,
) -> list[TaskValidationArtifact]:
    artifacts: list[TaskValidationArtifact] = []
    if test_command:
        artifacts.append(
            TaskValidationArtifact(
                type="test_output",
                label="Test output",
                reason="Task includes a test_command; capture the command output.",
                command=test_command,
            )
        )

    if _is_ui_or_screenshot(files, context_text, task):
        artifacts.append(
            TaskValidationArtifact(
                type="screenshot",
                label="Screenshot",
                reason="Task changes or validates UI-facing behavior.",
                paths=tuple(_matching_paths(files, _UI_PATH_PARTS, _UI_SUFFIXES)),
            )
        )

    migration_paths = _matching_paths(files, _MIGRATION_PATH_PARTS, _MIGRATION_SUFFIXES)
    if migration_paths or _has_token(context_text, _MIGRATION_TOKENS):
        artifacts.append(
            TaskValidationArtifact(
                type="migration_note",
                label="Migration note",
                reason="Migration or data movement work needs reviewable execution notes.",
                paths=tuple(migration_paths),
            )
        )
        artifacts.append(
            TaskValidationArtifact(
                type="log_excerpt",
                label="Migration log excerpt",
                reason="Migration validation should include applied rows, counts, or command logs.",
                paths=tuple(migration_paths),
            )
        )

    schema_paths = _matching_paths(files, _SCHEMA_PATH_PARTS, _SCHEMA_SUFFIXES)
    if schema_paths or _has_token(context_text, _SCHEMA_TOKENS):
        artifacts.append(
            TaskValidationArtifact(
                type="schema_review",
                label="Schema review note",
                reason="Schema changes need review notes covering compatibility and consumers.",
                paths=tuple(schema_paths),
            )
        )

    config_paths = _matching_config_paths(files)
    if config_paths or _has_token(context_text, _CONFIG_TOKENS):
        artifacts.append(
            TaskValidationArtifact(
                type="config_review",
                label="Config review note",
                reason="Configuration changes need reviewer-visible defaults and rollout notes.",
                paths=tuple(config_paths),
            )
        )

    if _is_api_task(files, context_text):
        artifacts.append(
            TaskValidationArtifact(
                type="api_sample",
                label="API sample",
                reason="API-facing behavior should include a sample request and response.",
                paths=tuple(_matching_paths(files, _API_PATH_PARTS, _API_SUFFIXES)),
            )
        )

    if _needs_manual_verification(task, test_command, risk_level):
        artifacts.append(
            TaskValidationArtifact(
                type="manual_verification_note",
                label="Manual verification note",
                reason="Task needs an explicit human-readable validation note.",
            )
        )

    artifacts.extend(_metadata_artifacts(task.get("metadata")))
    return _dedupe_artifacts(artifacts)


def _summary(entries: tuple[TaskValidationArtifactEntry, ...]) -> dict[str, Any]:
    artifact_counts: dict[str, int] = {}
    risk_counts: dict[str, int] = {}
    for entry in entries:
        risk_counts[entry.risk_level] = risk_counts.get(entry.risk_level, 0) + 1
        for artifact in entry.artifacts:
            artifact_counts[artifact.type] = artifact_counts.get(artifact.type, 0) + 1
    return {
        "task_count": len(entries),
        "artifact_counts": dict(sorted(artifact_counts.items())),
        "risk_counts": dict(sorted(risk_counts.items())),
    }


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


def _metadata_artifacts(value: Any) -> list[TaskValidationArtifact]:
    if not isinstance(value, Mapping):
        return []

    artifacts: list[TaskValidationArtifact] = []
    explicit_types = _strings(value.get("validation_artifacts"))
    explicit_types.extend(_strings(value.get("required_artifacts")))
    for artifact_type in explicit_types:
        normalized = artifact_type.lower().replace("-", "_").replace(" ", "_")
        if normalized in _ARTIFACT_LABELS:
            artifacts.append(
                TaskValidationArtifact(
                    type=normalized,  # type: ignore[arg-type]
                    label=_ARTIFACT_LABELS[normalized],
                    reason="Task metadata explicitly requests this validation artifact.",
                )
            )

    if _truthy(value.get("requires_screenshot")):
        artifacts.append(
            TaskValidationArtifact(
                type="screenshot",
                label="Screenshot",
                reason="Task metadata explicitly requests screenshot evidence.",
            )
        )
    if _truthy(value.get("requires_manual_verification")):
        artifacts.append(
            TaskValidationArtifact(
                type="manual_verification_note",
                label="Manual verification note",
                reason="Task metadata explicitly requests manual verification.",
            )
        )
    return artifacts


def _risk_level(task: Mapping[str, Any]) -> str:
    return (_optional_text(task.get("risk_level")) or "unspecified").lower()


def _needs_manual_verification(
    task: Mapping[str, Any],
    test_command: str | None,
    risk_level: str,
) -> bool:
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping) and _truthy(metadata.get("requires_manual_verification")):
        return True
    if risk_level in {"high", "critical", "blocker"}:
        return True
    return not test_command and _has_token(
        _context_text(
            task,
            _strings(task.get("files_or_modules")),
            _strings(task.get("acceptance_criteria")),
        ),
        {"manual", "manually", "inspect"},
    )


def _is_ui_or_screenshot(
    files: list[str],
    context_text: str,
    task: Mapping[str, Any],
) -> bool:
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping) and _truthy(metadata.get("requires_screenshot")):
        return True
    if any(_path_matches(file_path, _UI_PATH_PARTS, _UI_SUFFIXES) for file_path in files):
        return True
    return _has_token(context_text, _UI_TOKENS)


def _is_api_task(files: list[str], context_text: str) -> bool:
    if any(_path_matches(file_path, _API_PATH_PARTS, _API_SUFFIXES) for file_path in files):
        return True
    return _has_token(context_text, _API_TOKENS)


def _matching_paths(
    files: list[str],
    path_parts: set[str],
    suffixes: tuple[str, ...],
) -> list[str]:
    return [file_path for file_path in files if _path_matches(file_path, path_parts, suffixes)]


def _matching_config_paths(files: list[str]) -> list[str]:
    return [
        file_path
        for file_path in files
        if _path_matches(file_path, _CONFIG_PATH_PARTS, _CONFIG_SUFFIXES)
        or PurePosixPath(file_path.strip().replace("\\", "/").lower()).name
        in _CONFIG_FILENAMES
    ]


def _path_matches(path: str, path_parts: set[str], suffixes: tuple[str, ...]) -> bool:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return False
    pure_path = PurePosixPath(normalized)
    if pure_path.suffix in suffixes:
        return True
    return any(part in path_parts for part in pure_path.parts)


def _context_text(
    task: Mapping[str, Any],
    files: list[str],
    acceptance_criteria: list[str],
) -> str:
    values = [
        _optional_text(task.get("title")),
        _optional_text(task.get("description")),
        _optional_text(task.get("suggested_engine")),
        *files,
        *acceptance_criteria,
    ]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        values.extend(_strings(metadata.get("notes")))
        values.extend(_strings(metadata.get("validation_notes")))
        values.extend(_strings(metadata.get("risks")))
    return " ".join(value for value in values if value).lower()


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(_TOKEN_RE.findall(text.lower())) & tokens)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _dedupe_artifacts(
    artifacts: list[TaskValidationArtifact],
) -> list[TaskValidationArtifact]:
    deduped: list[TaskValidationArtifact] = []
    seen: set[ArtifactType] = set()
    for artifact in artifacts:
        if artifact.type in seen:
            continue
        deduped.append(artifact)
        seen.add(artifact.type)
    return deduped


_ARTIFACT_LABELS = {
    "api_sample": "API sample",
    "config_review": "Config review note",
    "log_excerpt": "Log excerpt",
    "manual_verification_note": "Manual verification note",
    "migration_note": "Migration note",
    "schema_review": "Schema review note",
    "screenshot": "Screenshot",
    "test_output": "Test output",
}
_UI_PATH_PARTS = {"app", "components", "frontend", "pages", "screens", "styles", "ui", "views"}
_UI_SUFFIXES = (".css", ".html", ".jsx", ".scss", ".svelte", ".tsx", ".vue")
_UI_TOKENS = {
    "browser",
    "component",
    "frontend",
    "responsive",
    "screen",
    "screenshot",
    "screenshots",
    "style",
    "ui",
    "ux",
    "visual",
}
_MIGRATION_PATH_PARTS = {"alembic", "backfills", "migrations"}
_MIGRATION_SUFFIXES = (".sql",)
_MIGRATION_TOKENS = {"backfill", "etl", "import", "migration", "migrations"}
_SCHEMA_PATH_PARTS = {"schemas", "schema"}
_SCHEMA_SUFFIXES = (".graphql", ".json", ".jsonschema", ".proto", ".sql")
_SCHEMA_TOKENS = {"contract", "graphql", "schema", "schemas"}
_CONFIG_PATH_PARTS = {"config", "configs", "settings"}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".toml", ".yaml", ".yml")
_CONFIG_FILENAMES = {
    ".env",
    "dockerfile",
    "package.json",
    "poetry.lock",
    "pyproject.toml",
    "requirements.txt",
}
_CONFIG_TOKENS = {"configuration", "config", "env", "flag"}
_API_PATH_PARTS = {"api", "controllers", "handlers", "routes"}
_API_SUFFIXES = (".graphql", ".proto")
_API_TOKENS = {"api", "endpoint", "graphql", "request", "response", "rest", "webhook"}


__all__ = [
    "ArtifactType",
    "TaskValidationArtifact",
    "TaskValidationArtifactEntry",
    "TaskValidationArtifactPlan",
    "build_task_validation_artifact_plan",
    "task_validation_artifact_plan_to_dict",
]
