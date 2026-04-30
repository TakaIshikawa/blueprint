"""Infer fixture and setup requirements for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FixtureCategory = Literal[
    "database_seed_data",
    "file_fixtures",
    "external_service_mocks",
    "auth_users",
    "migration_snapshots",
    "cleanup_reset",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DATABASE_RE = re.compile(
    r"\b(?:database|db|sql|sqlite|postgres|mysql|repository|repositories|storage|"
    r"persist(?:ed|ence)?|record|records|row|rows|table|tables|seed|seeded|"
    r"reference\s+data|catalog|fixture\s+data)\b",
    re.IGNORECASE,
)
_FILE_RE = re.compile(
    r"\b(?:file|files|fixture|fixtures|upload|uploads|download|csv|json|"
    r"yaml|yml|xml|pdf|image|images|screenshot|attachment|document)\b",
    re.IGNORECASE,
)
_EXTERNAL_RE = re.compile(
    r"\b(?:external|third[-\s]?party|api|apis|http|request|requests|webhook|webhooks|"
    r"stripe|github|slack|email|smtp|s3|oauth|provider|service|services|mock|stub)\b",
    re.IGNORECASE,
)
_AUTH_RE = re.compile(
    r"\b(?:auth|authentication|authorization|user|users|account|accounts|admin|role|"
    r"roles|permission|permissions|login|session|token|tokens|oauth|rbac)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|alembic|revision|schema|backfill|downgrade|upgrade|"
    r"snapshot|snapshots|rollback|restore|before\s+and\s+after)\b",
    re.IGNORECASE,
)
_CLEANUP_RE = re.compile(
    r"\b(?:cleanup|clean\s+up|reset|teardown|tear\s+down|isolate|isolated|idempotent|"
    r"temporary|temp|cache|queue|delete|deleted|remove|purge|truncate)\b",
    re.IGNORECASE,
)

_CATEGORY_ORDER: tuple[FixtureCategory, ...] = (
    "auth_users",
    "database_seed_data",
    "file_fixtures",
    "external_service_mocks",
    "migration_snapshots",
    "cleanup_reset",
)
_PATH_PATTERNS: tuple[tuple[FixtureCategory, re.Pattern[str]], ...] = (
    (
        "auth_users",
        re.compile(
            r"(?:^|/)(?:auth|authentication|authorization|permissions?|users?|accounts?)(?:/|$)",
            re.IGNORECASE,
        ),
    ),
    (
        "database_seed_data",
        re.compile(
            r"\.(?:sql|db|sqlite)$|(?:^|/)(?:db|database|store|repositories|repository|"
            r"models?|seeds?)(?:/|$)",
            re.IGNORECASE,
        ),
    ),
    (
        "file_fixtures",
        re.compile(
            r"(?:^|/)(?:fixtures?|testdata|samples?)(?:/|$)|"
            r"\.(?:csv|json|ya?ml|xml|pdf|png|jpe?g|gif|webp|txt)$",
            re.IGNORECASE,
        ),
    ),
    (
        "external_service_mocks",
        re.compile(
            r"(?:^|/)(?:clients?|integrations?|services?|webhooks?|mocks?)(?:/|$)",
            re.IGNORECASE,
        ),
    ),
    (
        "migration_snapshots",
        re.compile(r"(?:^|/)(?:migrations?|alembic|snapshots?)(?:/|$)", re.IGNORECASE),
    ),
)
_TEXT_PATTERNS: tuple[tuple[FixtureCategory, re.Pattern[str]], ...] = (
    ("auth_users", _AUTH_RE),
    ("database_seed_data", _DATABASE_RE),
    ("file_fixtures", _FILE_RE),
    ("external_service_mocks", _EXTERNAL_RE),
    ("migration_snapshots", _MIGRATION_RE),
    ("cleanup_reset", _CLEANUP_RE),
)
_METADATA_KEY_HINTS: tuple[tuple[FixtureCategory, tuple[str, ...]], ...] = (
    ("auth_users", ("auth", "user", "account", "role", "permission", "token")),
    ("database_seed_data", ("data", "database", "db", "seed", "record", "fixture")),
    ("file_fixtures", ("file", "fixture", "sample", "upload", "attachment")),
    ("external_service_mocks", ("mock", "stub", "api", "webhook", "external", "service")),
    ("migration_snapshots", ("migration", "schema", "snapshot", "rollback", "backfill")),
    ("cleanup_reset", ("cleanup", "reset", "teardown", "isolation")),
)


@dataclass(frozen=True, slots=True)
class FixtureRequirement:
    """One required fixture or setup dependency for task validation."""

    category: FixtureCategory
    requirement: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    setup_hints: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "requirement": self.requirement,
            "evidence": list(self.evidence),
            "setup_hints": list(self.setup_hints),
        }


@dataclass(frozen=True, slots=True)
class TaskFixturePlan:
    """Fixture and setup requirements for one execution task."""

    task_id: str
    title: str
    requirements: tuple[FixtureRequirement, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
        }

    def to_markdown(self) -> str:
        """Render this task fixture plan as stable markdown."""
        lines = [f"## {self.task_id} - {self.title}"]
        if not self.requirements:
            lines.append("- No fixture requirements inferred.")
            return "\n".join(lines)
        for requirement in self.requirements:
            lines.append(
                f"- **{_category_label(requirement.category)}**: {requirement.requirement}"
            )
            if requirement.setup_hints:
                lines.append(f"  - Setup: {'; '.join(requirement.setup_hints)}")
            if requirement.evidence:
                lines.append(f"  - Evidence: {'; '.join(requirement.evidence)}")
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class TaskFixtureRequirementsResult:
    """Complete fixture requirement plan for a plan or task collection."""

    plan_id: str | None = None
    task_count: int = 0
    plans: tuple[TaskFixturePlan, ...] = field(default_factory=tuple)
    requirement_counts_by_category: dict[FixtureCategory, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "plans": [plan.to_dict() for plan in self.plans],
            "requirement_counts_by_category": dict(self.requirement_counts_by_category),
        }

    def to_markdown(self) -> str:
        """Render fixture requirements as stable markdown."""
        title = "# Task Fixture Requirements"
        lines = [title]
        if self.plan_id:
            lines.append(f"Plan: {self.plan_id}")
        if not self.plans:
            lines.append("No tasks found.")
            return "\n\n".join(lines)
        lines.extend(plan.to_markdown() for plan in self.plans)
        return "\n\n".join(lines)


def build_task_fixture_requirements(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskFixtureRequirementsResult:
    """Infer deterministic fixture/setup requirements from execution tasks."""
    plan_id, tasks = _source_payload(source)
    plans = tuple(_task_fixture_plan(task, index) for index, task in enumerate(tasks, start=1))
    return TaskFixtureRequirementsResult(
        plan_id=plan_id,
        task_count=len(plans),
        plans=plans,
        requirement_counts_by_category=_requirement_counts(plans),
    )


def task_fixture_requirements_to_dict(
    result: TaskFixtureRequirementsResult,
) -> dict[str, Any]:
    """Serialize task fixture requirements to a plain dictionary."""
    return result.to_dict()


task_fixture_requirements_to_dict.__test__ = False


def _task_fixture_plan(task: Mapping[str, Any], index: int) -> TaskFixturePlan:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    evidence_by_category = _detected_evidence(task)
    requirements = tuple(
        FixtureRequirement(
            category=category,
            requirement=_requirement_text(category),
            evidence=tuple(evidence_by_category[category]),
            setup_hints=_setup_hints(category),
        )
        for category in _CATEGORY_ORDER
        if category in evidence_by_category
    )
    return TaskFixturePlan(task_id=task_id, title=title, requirements=requirements)


def _detected_evidence(task: Mapping[str, Any]) -> dict[FixtureCategory, tuple[str, ...]]:
    evidence_by_category: dict[FixtureCategory, list[str]] = {}
    for path in _strings(task.get("files_or_modules")):
        normalized = _normalized_path(path)
        for category, pattern in _PATH_PATTERNS:
            if pattern.search(normalized):
                _append_evidence(evidence_by_category, category, f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        for category, pattern in _TEXT_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{source_field}: {text}")

    for source_field, text in _metadata_texts(task.get("metadata")):
        field = _metadata_hint_field(source_field)
        for category, hints in _METADATA_KEY_HINTS:
            if any(hint in field for hint in hints):
                _append_evidence(evidence_by_category, category, f"{source_field}: {text}")
        for category, pattern in _TEXT_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{source_field}: {text}")

    return {
        category: tuple(_dedupe(evidence_by_category[category]))
        for category in _CATEGORY_ORDER
        if category in evidence_by_category
    }


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "test_command"):
        text = _optional_text(task.get(field_name))
        if text:
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


def _metadata_hint_field(source_field: str) -> str:
    parts = source_field.casefold().split(".", 1)
    return parts[1] if len(parts) == 2 and parts[0] == "metadata" else source_field.casefold()


def _source_payload(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
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


def _requirement_counts(plans: Iterable[TaskFixturePlan]) -> dict[FixtureCategory, int]:
    counts: dict[FixtureCategory, int] = {}
    for plan in plans:
        for requirement in plan.requirements:
            counts[requirement.category] = counts.get(requirement.category, 0) + 1
    return {category: counts[category] for category in _CATEGORY_ORDER if category in counts}


def _requirement_text(category: FixtureCategory) -> str:
    return {
        "auth_users": "Prepare users, roles, permissions, sessions, or tokens needed to validate access paths.",
        "database_seed_data": "Prepare deterministic seed records and reference data before validation.",
        "file_fixtures": "Prepare stable files, uploads, exports, samples, or fixture directories.",
        "external_service_mocks": "Prepare mocks or stubs for external APIs, webhooks, providers, or services.",
        "migration_snapshots": "Capture schema/data snapshots needed to validate migration and rollback behavior.",
        "cleanup_reset": "Define cleanup, reset, teardown, or isolation steps for generated validation state.",
    }[category]


def _setup_hints(category: FixtureCategory) -> tuple[str, ...]:
    return {
        "auth_users": (
            "Name required personas and privilege levels.",
            "Keep credentials or tokens test-scoped.",
        ),
        "database_seed_data": (
            "Make seed data idempotent.",
            "Record expected identifiers or row counts.",
        ),
        "file_fixtures": (
            "Store fixture files under a deterministic test path.",
            "Document required file names and formats.",
        ),
        "external_service_mocks": (
            "Stub success, failure, and retry responses.",
            "Avoid live third-party calls during validation.",
        ),
        "migration_snapshots": (
            "Capture before and after schema/data state.",
            "Include rollback or downgrade validation data.",
        ),
        "cleanup_reset": (
            "Reset persisted state between validation runs.",
            "Remove temporary files, queues, or cached records.",
        ),
    }[category]


def _category_label(category: FixtureCategory) -> str:
    return {
        "auth_users": "Auth/users",
        "database_seed_data": "Database seed data",
        "file_fixtures": "File fixtures",
        "external_service_mocks": "External service mocks",
        "migration_snapshots": "Migration snapshots",
        "cleanup_reset": "Cleanup/reset",
    }[category]


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _append_evidence(
    evidence_by_category: dict[FixtureCategory, list[str]],
    category: FixtureCategory,
    evidence: str,
) -> None:
    evidence_by_category.setdefault(category, []).append(evidence)


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "FixtureCategory",
    "FixtureRequirement",
    "TaskFixturePlan",
    "TaskFixtureRequirementsResult",
    "build_task_fixture_requirements",
    "task_fixture_requirements_to_dict",
]
