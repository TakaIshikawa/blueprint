"""Plan task-level configuration drift checks for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ConfigDriftCategory = Literal[
    "environment",
    "infrastructure",
    "ci",
    "feature_flag",
    "config_file",
    "migration",
    "deployment",
]
ConfigDriftSeverity = Literal["low", "medium", "high"]
EnvironmentName = Literal["local", "ci", "staging", "production"]
_T = TypeVar("_T")

_ENV_VAR_RE = re.compile(r"\b[A-Z][A-Z0-9]*_[A-Z0-9_]*\b")
_ENV_TEXT_RE = re.compile(
    r"\b(?:env(?:ironment)?\s*(?:var(?:iable)?s?)?|\.env|secret|secrets|"
    r"credential|credentials|api key|token|config var)\b",
    re.IGNORECASE,
)
_IAC_TEXT_RE = re.compile(
    r"\b(?:terraform|terragrunt|iac|infrastructure as code|helm|kubernetes|"
    r"k8s|kubectl|namespace|ingress|service account|cloudformation|pulumi)\b",
    re.IGNORECASE,
)
_CI_TEXT_RE = re.compile(
    r"\b(?:ci|workflow|github actions|gitlab ci|circleci|jenkins|azure pipelines|"
    r"buildkite|pipeline|runner)\b",
    re.IGNORECASE,
)
_FEATURE_FLAG_TEXT_RE = re.compile(
    r"\b(?:feature flag|feature toggle|flagged rollout|launchdarkly|unleash|"
    r"split\.io|experiment flag|kill switch|remote config|rollout flag)\b",
    re.IGNORECASE,
)
_CONFIG_TEXT_RE = re.compile(
    r"\b(?:config(?:uration)?|settings|yaml|yml|toml|json config|ini|properties|"
    r"runtime setting|deployment setting)\b",
    re.IGNORECASE,
)
_MIGRATION_TEXT_RE = re.compile(
    r"\b(?:migration|migrations|alembic|liquibase|flyway|schema change|"
    r"database migration|db migration|backfill)\b",
    re.IGNORECASE,
)
_DEPLOY_TEXT_RE = re.compile(
    r"\b(?:deploy|deployment|release workflow|rollout|canary|blue green|"
    r"production|staging|health check|smoke test)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|backout|back out|disable flag|"
    r"previous version|downgrade)\b",
    re.IGNORECASE,
)

_FILE_SIGNAL_PATTERNS: tuple[tuple[ConfigDriftCategory, re.Pattern[str]], ...] = (
    ("ci", re.compile(r"(^|/)\.github/workflows/[^/]+\.ya?ml$", re.I)),
    ("ci", re.compile(r"(^|/)\.gitlab-ci\.ya?ml$|(^|/)circleci/config\.ya?ml$", re.I)),
    ("ci", re.compile(r"(^|/)azure-pipelines\.ya?ml$|(^|/)jenkinsfile$", re.I)),
    ("infrastructure", re.compile(r"\.tf$|(^|/)(terraform|terragrunt|infra|iac)(/|$)", re.I)),
    ("infrastructure", re.compile(r"(^|/)(helm|charts?|k8s|kubernetes)(/|$)", re.I)),
    ("deployment", re.compile(r"(^|/)(deploy|deployment|manifests?)(/|$)", re.I)),
    ("deployment", re.compile(r"(^|/)(dockerfile|docker-compose\.ya?ml|fly\.toml|render\.ya?ml)$", re.I)),
    ("environment", re.compile(r"(^|/)\.env(?:\.[^/]+)?(?:\.(?:example|sample|template))?$", re.I)),
    ("migration", re.compile(r"(^|/)(migrations|alembic)(/|$)|\.(?:sql|ddl)$", re.I)),
    ("config_file", re.compile(r"(^|/)(config|configs|settings)(/|$)", re.I)),
    ("config_file", re.compile(r"(^|/)(appsettings|settings|config)[^/]*\.(?:ya?ml|json|toml|ini|properties)$", re.I)),
    ("config_file", re.compile(r"\.(?:ya?ml|toml|ini|properties)$", re.I)),
)

_TEXT_SIGNAL_PATTERNS: tuple[tuple[ConfigDriftCategory, re.Pattern[str]], ...] = (
    ("environment", _ENV_TEXT_RE),
    ("environment", _ENV_VAR_RE),
    ("infrastructure", _IAC_TEXT_RE),
    ("ci", _CI_TEXT_RE),
    ("feature_flag", _FEATURE_FLAG_TEXT_RE),
    ("config_file", _CONFIG_TEXT_RE),
    ("migration", _MIGRATION_TEXT_RE),
    ("deployment", _DEPLOY_TEXT_RE),
)

_BASE_PREVENTION = (
    "Document the intended source of truth for the changed configuration.",
    "Apply the change consistently across affected environments.",
    "Capture expected values, owners, and approval notes before implementation.",
)
_CATEGORY_PREVENTION: dict[ConfigDriftCategory, tuple[str, ...]] = {
    "environment": (
        "Update .env examples, secret manager entries, and CI variable definitions together.",
    ),
    "infrastructure": (
        "Run an IaC plan or dry run against non-production before applying changes.",
    ),
    "ci": ("Validate workflow syntax and required runner secrets before merge.",),
    "feature_flag": (
        "Define default flag state, targeting rules, and owner before rollout.",
    ),
    "config_file": (
        "Keep local examples, CI fixtures, staging, and production config keys aligned.",
    ),
    "migration": (
        "Rehearse forward and rollback migration steps on staging-like data.",
    ),
    "deployment": (
        "Confirm deployment manifests, health checks, and rollout gates match the target environment.",
    ),
}
_ROLLBACK_NOTES: dict[ConfigDriftCategory, tuple[str, ...]] = {
    "environment": (
        "Restore the previous secret or env var value and restart affected services if needed.",
    ),
    "infrastructure": (
        "Keep the prior IaC revision available and capture the command to revert or re-apply it.",
    ),
    "ci": (
        "Revert the workflow change or disable the new job while preserving the last known good pipeline.",
    ),
    "feature_flag": (
        "Use the flag kill switch or restore the previous targeting rule before reverting code.",
    ),
    "config_file": (
        "Revert the config file and redeploy or reload the affected environment.",
    ),
    "migration": (
        "Record downgrade, restore, or compensating data steps before production rollout.",
    ),
    "deployment": (
        "Roll back to the previous release artifact, manifest, or deployment revision.",
    ),
}


@dataclass(frozen=True, slots=True)
class TaskConfigDriftRecord:
    """One task-level configuration drift concern."""

    task_id: str
    title: str
    severity: ConfigDriftSeverity
    drift_source: str
    category: ConfigDriftCategory
    affected_environments: tuple[EnvironmentName, ...] = field(default_factory=tuple)
    prevention_checklist: tuple[str, ...] = field(default_factory=tuple)
    detection_evidence: tuple[str, ...] = field(default_factory=tuple)
    rollback_notes: tuple[str, ...] = field(default_factory=tuple)
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "drift_source": self.drift_source,
            "category": self.category,
            "affected_environments": list(self.affected_environments),
            "prevention_checklist": list(self.prevention_checklist),
            "detection_evidence": list(self.detection_evidence),
            "rollback_notes": list(self.rollback_notes),
            "validation_evidence": list(self.validation_evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskConfigDriftPlan:
    """Configuration drift guidance for execution-plan tasks."""

    plan_id: str | None = None
    records: tuple[TaskConfigDriftRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
        }

    def to_markdown(self) -> str:
        """Render configuration drift guidance as deterministic Markdown."""
        title = "# Task Configuration Drift Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No task configuration drift signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Category | Severity | Environments | Source | Evidence | Rollback |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.category} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.affected_environments))} | "
                f"{_markdown_cell(record.drift_source)} | "
                f"{_markdown_cell('; '.join(record.detection_evidence))} | "
                f"{_markdown_cell('; '.join(record.rollback_notes))} |"
            )
        return "\n".join(lines)


def build_task_config_drift_plan(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskConfigDriftPlan:
    """Detect task changes likely to introduce configuration drift."""
    plan_id, plan_validation, tasks = _source_payload(source)
    records = tuple(
        record
        for index, task in enumerate(tasks, start=1)
        for record in _task_records(task, index, plan_validation)
    )
    return TaskConfigDriftPlan(plan_id=plan_id, records=records)


def derive_task_config_drift_plan(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskConfigDriftPlan:
    """Compatibility alias for building a task configuration drift plan."""
    return build_task_config_drift_plan(source)


def build_task_config_drift(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskConfigDriftPlan:
    """Compatibility alias for building a task configuration drift plan."""
    return build_task_config_drift_plan(source)


def derive_task_config_drift(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskConfigDriftPlan:
    """Compatibility alias for building a task configuration drift plan."""
    return build_task_config_drift_plan(source)


def task_config_drift_plan_to_dict(plan: TaskConfigDriftPlan) -> dict[str, Any]:
    """Serialize a task configuration drift plan to a plain dictionary."""
    return plan.to_dict()


task_config_drift_plan_to_dict.__test__ = False


def task_config_drift_plan_to_markdown(plan: TaskConfigDriftPlan) -> str:
    """Render a task configuration drift plan as Markdown."""
    return plan.to_markdown()


task_config_drift_plan_to_markdown.__test__ = False

task_config_drift_to_dict = task_config_drift_plan_to_dict
task_config_drift_to_dict.__test__ = False
task_config_drift_to_markdown = task_config_drift_plan_to_markdown
task_config_drift_to_markdown.__test__ = False


def _task_records(
    task: Mapping[str, Any],
    index: int,
    plan_validation: tuple[str, ...],
) -> list[TaskConfigDriftRecord]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    evidence_by_category = _detected_evidence(task)
    task_validation = tuple(_validation_values(task, include_plan=False))
    validation_evidence = tuple(_dedupe([*task_validation, *plan_validation]))
    has_task_validation = bool(task_validation)

    return [
        TaskConfigDriftRecord(
            task_id=task_id,
            title=title,
            severity=_severity(category, has_task_validation),
            drift_source=_drift_source(category),
            category=category,
            affected_environments=tuple(_affected_environments(category, task)),
            prevention_checklist=tuple(_prevention_checklist(category, has_task_validation)),
            detection_evidence=evidence,
            rollback_notes=tuple(_rollback_notes(category, task)),
            validation_evidence=validation_evidence,
        )
        for category, evidence in evidence_by_category.items()
    ]


def _detected_evidence(task: Mapping[str, Any]) -> dict[ConfigDriftCategory, tuple[str, ...]]:
    evidence_by_category: dict[ConfigDriftCategory, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for category, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _append_evidence(evidence_by_category, category, f"files_or_modules: {path}")

    for field_path, text in _task_texts(task):
        for category, pattern in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    for field_path, text in _metadata_texts(task.get("metadata")):
        if _metadata_key_is_config_hint(field_path):
            category = _metadata_hint_category(field_path)
            _append_evidence(evidence_by_category, category, f"{field_path}: {text}")
        for category, pattern in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    return {
        category: tuple(_dedupe(evidence))
        for category, evidence in evidence_by_category.items()
    }


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, tuple[str, ...], list[dict[str, Any]]]:
    if source is None:
        return None, (), []
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_validation_evidence(plan)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                tuple(_plan_validation_evidence(plan)),
                _task_payloads(plan.get("tasks")),
            )
        return None, (), [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_validation_evidence(plan)),
            _task_payloads(plan.get("tasks")),
        )

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
    return None, (), tasks


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
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_strategy",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _plan_validation_evidence(plan: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    if text := _optional_text(plan.get("test_strategy")):
        evidence.append(f"test_strategy: {text}")
    evidence.extend(_validation_values(plan, include_plan=True))
    return _dedupe(evidence)


def _validation_evidence(
    task: Mapping[str, Any],
    plan_validation: tuple[str, ...],
) -> list[str]:
    evidence = _validation_values(task, include_plan=False)
    evidence.extend(plan_validation)
    return _dedupe(evidence)


def _validation_values(item: Mapping[str, Any], *, include_plan: bool) -> list[str]:
    values: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command", "validation_plan"):
        if text := _optional_text(item.get(key)):
            values.append(f"{key}: {text}")
    metadata = item.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_plan",
            "validation_plans",
            "validation_gates",
            "validation_gate",
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                values.extend(f"metadata.{key}: {command}" for command in flatten_validation_commands(value))
            else:
                values.extend(f"metadata.{key}: {text}" for text in _strings(value))
    if include_plan and isinstance(metadata, Mapping):
        for key in ("smoke_tests", "deployment_verification", "rollback_plan"):
            values.extend(f"metadata.{key}: {text}" for text in _strings(metadata.get(key)))
    return _dedupe(values)


def _severity(category: ConfigDriftCategory, has_validation: bool) -> ConfigDriftSeverity:
    base: ConfigDriftSeverity = "high" if category in {"infrastructure", "ci", "migration", "deployment"} else "medium"
    if not has_validation:
        return base
    if base == "high":
        return "medium"
    return "low"


def _affected_environments(
    category: ConfigDriftCategory,
    task: Mapping[str, Any],
) -> list[EnvironmentName]:
    explicit = _explicit_environments(task)
    if explicit:
        return explicit
    defaults: dict[ConfigDriftCategory, tuple[EnvironmentName, ...]] = {
        "environment": ("local", "ci", "staging", "production"),
        "infrastructure": ("staging", "production"),
        "ci": ("ci",),
        "feature_flag": ("staging", "production"),
        "config_file": ("local", "ci", "staging", "production"),
        "migration": ("staging", "production"),
        "deployment": ("ci", "staging", "production"),
    }
    return list(defaults[category])


def _explicit_environments(task: Mapping[str, Any]) -> list[EnvironmentName]:
    values: list[str] = []
    for key in ("environments", "environment", "affected_environments"):
        values.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("environments", "environment", "affected_environments"):
            values.extend(_strings(metadata.get(key)))
    allowed = {"local", "ci", "staging", "production"}
    return [
        value  # type: ignore[list-item]
        for value in _dedupe(text.casefold() for text in values)
        if value in allowed
    ]


def _prevention_checklist(
    category: ConfigDriftCategory,
    has_validation: bool,
) -> list[str]:
    checklist = [*_BASE_PREVENTION, *_CATEGORY_PREVENTION[category]]
    if has_validation:
        checklist.append("Run the task validation plan in every affected environment before release.")
    else:
        checklist.append("Add an explicit validation plan before implementation or deployment.")
    return _dedupe(checklist)


def _rollback_notes(
    category: ConfigDriftCategory,
    task: Mapping[str, Any],
) -> list[str]:
    notes = list(_ROLLBACK_NOTES[category])
    for field_path, text in [*_task_texts(task), *_metadata_texts(task.get("metadata"))]:
        if _ROLLBACK_RE.search(text):
            notes.append(f"{field_path}: {text}")
    return _dedupe(notes)


def _drift_source(category: ConfigDriftCategory) -> str:
    return {
        "environment": "environment variable or secret change",
        "infrastructure": "infrastructure-as-code change",
        "ci": "CI workflow or runner configuration change",
        "feature_flag": "feature flag or remote configuration change",
        "config_file": "configuration file change",
        "migration": "migration or schema deployment change",
        "deployment": "deployment setting change",
    }[category]


def _metadata_key_is_config_hint(source_field: str) -> bool:
    field = source_field.casefold()
    return any(
        token in field
        for token in (
            "env",
            "secret",
            "credential",
            "config",
            "setting",
            "flag",
            "terraform",
            "helm",
            "k8s",
            "deploy",
            "ci",
            "migration",
        )
    )


def _metadata_hint_category(source_field: str) -> ConfigDriftCategory:
    field = source_field.casefold()
    if any(token in field for token in ("env", "secret", "credential")):
        return "environment"
    if any(token in field for token in ("terraform", "helm", "k8s", "infra")):
        return "infrastructure"
    if ".ci" in field or field.endswith(".ci") or "workflow" in field:
        return "ci"
    if "flag" in field:
        return "feature_flag"
    if "migration" in field:
        return "migration"
    if "deploy" in field:
        return "deployment"
    return "config_file"


def _append_evidence(
    evidence_by_category: dict[ConfigDriftCategory, list[str]],
    category: ConfigDriftCategory,
    evidence: str,
) -> None:
    evidence_by_category.setdefault(category, []).append(evidence)


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
    normalized = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.strip("/")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "ConfigDriftCategory",
    "ConfigDriftSeverity",
    "EnvironmentName",
    "TaskConfigDriftPlan",
    "TaskConfigDriftRecord",
    "build_task_config_drift",
    "build_task_config_drift_plan",
    "derive_task_config_drift",
    "derive_task_config_drift_plan",
    "task_config_drift_to_dict",
    "task_config_drift_to_markdown",
    "task_config_drift_plan_to_dict",
    "task_config_drift_plan_to_markdown",
]
