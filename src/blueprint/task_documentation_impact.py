"""Infer task-level documentation impacts for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DocumentationTarget = Literal[
    "README",
    "api_docs",
    "migration_notes",
    "runbooks",
    "user_facing_docs",
    "changelog_release_notes",
    "configuration_docs",
    "troubleshooting_docs",
]
DocumentationStatus = Literal["required", "optional"]
RiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TARGET_ORDER: dict[DocumentationTarget, int] = {
    "README": 0,
    "api_docs": 1,
    "migration_notes": 2,
    "runbooks": 3,
    "user_facing_docs": 4,
    "changelog_release_notes": 5,
    "configuration_docs": 6,
    "troubleshooting_docs": 7,
}

_API_RE = re.compile(
    r"\b(?:api|endpoint|route|controller|handler|request|response|graphql|grpc|rest|"
    r"openapi|swagger|client contract)\b",
    re.IGNORECASE,
)
_CONFIG_RE = re.compile(
    r"\b(?:config|configuration|setting|settings|env var|environment variable|"
    r"feature flag|flag|toggle|yaml|toml|ini)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|schema|database|db|sql|alembic|backfill|seed|"
    r"index|reindex|existing rows|data migration|rollback)\b",
    re.IGNORECASE,
)
_CLI_RE = re.compile(
    r"\b(?:cli|command line|terminal|argparse|click|typer|console|script|"
    r"subcommand|option|dry[- ]?run)\b",
    re.IGNORECASE,
)
_UI_RE = re.compile(
    r"\b(?:frontend|ui|ux|browser|component|page|screen|view|form|"
    r"checkout|onboarding|workflow|user flow|accessibility)\b",
    re.IGNORECASE,
)
_OPERATIONS_RE = re.compile(
    r"\b(?:runbook|operational|operations|ops|deploy|deployment|rollout|rollback|"
    r"incident|alert|monitor|dashboard|cron|scheduled|worker|queue|job|retry|"
    r"webhook|integration|external|provider|troubleshoot|failure|timeout)\b",
    re.IGNORECASE,
)
_DOCUMENTED_RE = re.compile(
    r"\b(?:doc|docs|documentation|readme|runbook|changelog|release notes?|"
    r"migration notes?|api docs?|openapi|troubleshooting)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskDocumentationTargetImpact:
    """One documentation target recommended for a task."""

    doc_target: DocumentationTarget
    status: DocumentationStatus
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "doc_target": self.doc_target,
            "status": self.status,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDocumentationImpact:
    """Documentation impact plan for one execution task."""

    task_id: str
    title: str
    doc_targets: tuple[TaskDocumentationTargetImpact, ...] = field(default_factory=tuple)
    suggested_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "doc_targets": [target.to_dict() for target in self.doc_targets],
            "suggested_acceptance_criteria": list(self.suggested_acceptance_criteria),
        }


@dataclass(frozen=True, slots=True)
class TaskDocumentationImpactPlan:
    """Documentation impact recommendations for a plan or task collection."""

    plan_id: str | None = None
    task_impacts: tuple[TaskDocumentationImpact, ...] = field(default_factory=tuple)
    required_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_impacts": [impact.to_dict() for impact in self.task_impacts],
            "required_task_ids": list(self.required_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return task impact records as plain dictionaries."""
        return [impact.to_dict() for impact in self.task_impacts]

    def to_markdown(self) -> str:
        """Render the documentation impact plan as deterministic Markdown."""
        title = "# Task Documentation Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.task_impacts:
            lines.extend(["", "No documentation impacts were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Target | Status | Evidence | Suggested Acceptance Criteria |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for impact in self.task_impacts:
            criteria = "<br>".join(
                _markdown_cell(item) for item in impact.suggested_acceptance_criteria
            ) or "None"
            if not impact.doc_targets:
                lines.append(
                    "| "
                    f"`{_markdown_cell(impact.task_id)}` | None | optional | None | {criteria} |"
                )
                continue
            for target in impact.doc_targets:
                evidence = "<br>".join(_markdown_cell(value) for value in target.evidence)
                lines.append(
                    "| "
                    f"`{_markdown_cell(impact.task_id)}` | "
                    f"{target.doc_target} | "
                    f"{target.status} | "
                    f"{evidence or 'None'} | "
                    f"{criteria} |"
                )
        return "\n".join(lines)


def build_task_documentation_impact_plan(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskDocumentationImpactPlan:
    """Recommend required and optional documentation updates for execution tasks."""
    plan_id, tasks = _source_payload(source)
    impacts = tuple(_task_impact(task, index) for index, task in enumerate(tasks, start=1))
    return TaskDocumentationImpactPlan(
        plan_id=plan_id,
        task_impacts=impacts,
        required_task_ids=tuple(
            impact.task_id
            for impact in impacts
            if any(target.status == "required" for target in impact.doc_targets)
        ),
    )


def task_documentation_impact_plan_to_dict(
    result: TaskDocumentationImpactPlan,
) -> dict[str, Any]:
    """Serialize a documentation impact plan to a plain dictionary."""
    return result.to_dict()


task_documentation_impact_plan_to_dict.__test__ = False


def task_documentation_impact_plan_to_markdown(
    result: TaskDocumentationImpactPlan,
) -> str:
    """Render a documentation impact plan as Markdown."""
    return result.to_markdown()


task_documentation_impact_plan_to_markdown.__test__ = False


@dataclass(slots=True)
class _TargetBuilder:
    status: DocumentationStatus
    evidence: list[str] = field(default_factory=list)


def _task_impact(task: Mapping[str, Any], index: int) -> TaskDocumentationImpact:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    risk_level = _risk_level(task.get("risk_level") or task.get("risk"))
    signals = _signals(task)
    existing_criteria = _strings(task.get("acceptance_criteria"))
    builders: dict[DocumentationTarget, _TargetBuilder] = {}

    if "api" in signals:
        _put(builders, "api_docs", "required", signals["api"])
        _put(builders, "changelog_release_notes", "required", signals["api"])
        _put(builders, "troubleshooting_docs", "optional", signals["api"])
        _put(builders, "README", "optional", signals["api"])
    if "config" in signals:
        _put(builders, "configuration_docs", "required", signals["config"])
        _put(builders, "changelog_release_notes", "required", signals["config"])
        _put(builders, "README", "optional", signals["config"])
        _put(builders, "troubleshooting_docs", "optional", signals["config"])
    if "migration" in signals:
        _put(builders, "migration_notes", "required", signals["migration"])
        _put(builders, "changelog_release_notes", "required", signals["migration"])
        _put(builders, "README", "optional", signals["migration"])
        _put(builders, "runbooks", "optional", signals["migration"])
        _put(builders, "troubleshooting_docs", "optional", signals["migration"])
    if "cli" in signals:
        _put(builders, "user_facing_docs", "required", signals["cli"])
        _put(builders, "changelog_release_notes", "required", signals["cli"])
        _put(builders, "README", "optional", signals["cli"])
        _put(builders, "troubleshooting_docs", "optional", signals["cli"])
    if "ui" in signals:
        _put(builders, "user_facing_docs", "required", signals["ui"])
        _put(builders, "changelog_release_notes", "required", signals["ui"])
        _put(builders, "troubleshooting_docs", "optional", signals["ui"])
    if "operations" in signals:
        required = "required" if risk_level == "high" or _has_strong_ops_signal(signals["operations"]) else "optional"
        _put(builders, "runbooks", required, signals["operations"])
        _put(builders, "troubleshooting_docs", required, signals["operations"])
        _put(builders, "changelog_release_notes", "required" if risk_level == "high" else "optional", signals["operations"])

    if risk_level == "high" and builders:
        _elevate(builders, "changelog_release_notes")

    doc_targets = tuple(
        TaskDocumentationTargetImpact(
            doc_target=target,
            status=builder.status,
            evidence=tuple(_dedupe(builder.evidence)),
        )
        for target, builder in sorted(builders.items(), key=lambda item: _TARGET_ORDER[item[0]])
    )
    return TaskDocumentationImpact(
        task_id=task_id,
        title=title,
        doc_targets=doc_targets,
        suggested_acceptance_criteria=tuple(
            _suggested_acceptance_criteria(doc_targets, existing_criteria)
        ),
    )


def _put(
    builders: dict[DocumentationTarget, _TargetBuilder],
    target: DocumentationTarget,
    status: DocumentationStatus,
    evidence: Iterable[str],
) -> None:
    if target not in builders:
        builders[target] = _TargetBuilder(status=status)
    elif status == "required":
        builders[target].status = "required"
    builders[target].evidence.extend(evidence)


def _elevate(builders: dict[DocumentationTarget, _TargetBuilder], target: DocumentationTarget) -> None:
    if target in builders:
        builders[target].status = "required"


def _signals(task: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    signals: dict[str, list[str]] = {}
    for path in _strings(task.get("files_or_modules")):
        normalized = _normalized_path(path)
        _add_path_signals(signals, path, normalized)

    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _test_command_texts(task):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(task.get("metadata")):
        field = source_field.casefold()
        if "doc" in field or "documentation" in field:
            _append(signals, "documentation", f"{source_field}: {text}")
        _add_text_signals(signals, source_field, text)

    return {category: tuple(_dedupe(evidence)) for category, evidence in signals.items()}


def _add_path_signals(signals: dict[str, list[str]], original: str, normalized: str) -> None:
    path = PurePosixPath(normalized.lower())
    parts = set(path.parts)
    evidence = f"files_or_modules: {original}"
    if bool({"api", "apis", "controllers", "handlers", "routes", "server"} & parts) or path.name in {
        "openapi.yaml",
        "openapi.yml",
        "swagger.yaml",
        "swagger.yml",
    }:
        _append(signals, "api", evidence)
    if path.suffix in {".env", ".yaml", ".yml", ".toml", ".ini"} and bool(
        {"config", "configs", "settings", "environments"} & parts
    ) or bool({"config", "configs", "settings"} & parts) or path.name in {
        ".env",
        ".env.example",
        "pyproject.toml",
    }:
        _append(signals, "config", evidence)
    if path.suffix == ".sql" or bool(
        {"alembic", "database", "db", "migrations", "schema"} & parts
    ):
        _append(signals, "migration", evidence)
    if bool({"bin", "cli", "commands", "scripts"} & parts) or path.name in {
        "cli.py",
        "commands.py",
    } or path.name.endswith("_cli.py"):
        _append(signals, "cli", evidence)
    if path.suffix in {".css", ".scss", ".sass", ".tsx", ".jsx", ".vue", ".svelte"} or bool(
        {"app", "client", "components", "frontend", "pages", "screens", "ui", "views"} & parts
    ):
        _append(signals, "ui", evidence)
    if bool(
        {"deploy", "deployment", "infra", "ops", "runbooks", "jobs", "workers", "queues", "cron", "integrations", "webhooks"}
        & parts
    ) or "webhook" in path.name:
        _append(signals, "operations", evidence)


def _add_text_signals(signals: dict[str, list[str]], source_field: str, text: str) -> None:
    evidence = f"{source_field}: {text}"
    if _API_RE.search(text):
        _append(signals, "api", evidence)
    if _CONFIG_RE.search(text):
        _append(signals, "config", evidence)
    if _MIGRATION_RE.search(text):
        _append(signals, "migration", evidence)
    if _CLI_RE.search(text):
        _append(signals, "cli", evidence)
    if _UI_RE.search(text):
        _append(signals, "ui", evidence)
    if _OPERATIONS_RE.search(text):
        _append(signals, "operations", evidence)
    if _DOCUMENTED_RE.search(text):
        _append(signals, "documentation", evidence)


def _suggested_acceptance_criteria(
    doc_targets: Iterable[TaskDocumentationTargetImpact],
    existing_criteria: Iterable[str],
) -> list[str]:
    existing_text = " ".join(existing_criteria).casefold()
    suggestions: list[str] = []
    for target in doc_targets:
        if target.status != "required":
            continue
        if _target_is_covered(target.doc_target, existing_text):
            continue
        suggestions.append(_criterion_for_target(target.doc_target))
    return _dedupe(suggestions)


def _target_is_covered(target: DocumentationTarget, existing_text: str) -> bool:
    keywords = {
        "README": ("readme",),
        "api_docs": ("api doc", "openapi", "swagger"),
        "migration_notes": ("migration note", "rollback note", "data migration doc"),
        "runbooks": ("runbook", "operational doc"),
        "user_facing_docs": ("user-facing doc", "user docs", "help doc", "cli doc"),
        "changelog_release_notes": ("changelog", "release note"),
        "configuration_docs": ("configuration doc", "config doc", "environment variable", "env var"),
        "troubleshooting_docs": ("troubleshooting", "troubleshoot"),
    }[target]
    return any(keyword in existing_text for keyword in keywords)


def _criterion_for_target(target: DocumentationTarget) -> str:
    return {
        "README": "README or setup documentation is updated for the changed workflow.",
        "api_docs": "API documentation is updated for the changed endpoint, request, or response contract.",
        "migration_notes": "Migration notes document rollout, rollback, and data verification steps.",
        "runbooks": "Runbook updates describe how operators deploy, monitor, and recover the changed path.",
        "user_facing_docs": "User-facing documentation is updated for the changed UI or CLI behavior.",
        "changelog_release_notes": "Changelog or release notes summarize the user-visible or operator-visible change.",
        "configuration_docs": "Configuration documentation lists new or changed settings, defaults, and required environment variables.",
        "troubleshooting_docs": "Troubleshooting documentation covers common failure modes and recovery steps for the change.",
    }[target]


def _has_strong_ops_signal(evidence: Iterable[str]) -> bool:
    text = " ".join(evidence).casefold()
    return any(
        phrase in text
        for phrase in (
            "runbook",
            "incident",
            "alert",
            "deployment",
            "deploy",
            "rollback",
            "cron",
            "worker",
            "queue",
            "webhook",
        )
    )


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _test_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            texts.append((key, command))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            for index, command in enumerate(_commands_from_value(metadata.get(key))):
                texts.append((f"metadata.{key}[{index}]", command))
    return texts


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif (text := _optional_text(child)):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif (text := _optional_text(item)):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _risk_level(value: Any) -> RiskLevel:
    text = (_optional_text(value) or "").casefold()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _append(signals: dict[str, list[str]], category: str, evidence: str) -> None:
    signals.setdefault(category, []).append(evidence)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "DocumentationStatus",
    "DocumentationTarget",
    "TaskDocumentationImpact",
    "TaskDocumentationImpactPlan",
    "TaskDocumentationTargetImpact",
    "build_task_documentation_impact_plan",
    "task_documentation_impact_plan_to_dict",
    "task_documentation_impact_plan_to_markdown",
]
