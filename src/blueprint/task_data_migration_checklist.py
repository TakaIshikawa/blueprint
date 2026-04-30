"""Build data migration safety checklists for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MigrationRiskCategory = Literal[
    "database",
    "schema",
    "model",
    "migration",
    "backfill",
    "seed",
    "index",
    "destructive",
    "metadata",
]
MigrationRiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_COMMAND_HINT_RE = re.compile(
    r"\b(?:pytest|tox|nox|alembic|flask\s+db|django-admin|manage\.py|"
    r"rails\s+db|prisma|sequelize|dbmate|golang-migrate|sqlx)\b",
    re.IGNORECASE,
)
_DATABASE_RE = re.compile(
    r"\b(?:database|db|storage|persist(?:ed|ence)?|repository|sql|postgres|"
    r"mysql|sqlite|record|records|table|tables)\b",
    re.IGNORECASE,
)
_SCHEMA_RE = re.compile(
    r"\b(?:schema|column|columns|field|fields|constraint|foreign\s+key|"
    r"primary\s+key|nullable|not\s+null|create\s+table|alter\s+table|"
    r"add_column|drop_column)\b",
    re.IGNORECASE,
)
_MODEL_RE = re.compile(
    r"\b(?:sqlalchemy|orm|model|models|mapped_column|relationship|declarative|"
    r"pydantic\s+model|entity)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|alembic|revision|upgrade|downgrade|"
    r"op\.create_table|op\.add_column|op\.drop_column)\b",
    re.IGNORECASE,
)
_BACKFILL_RE = re.compile(
    r"\b(?:backfill|data\s+migration|migrate\s+existing|existing\s+rows|"
    r"populate\s+existing|rehydrate|recompute|reindex)\b",
    re.IGNORECASE,
)
_SEED_RE = re.compile(r"\b(?:seed|seeded|fixture|reference\s+data|initial\s+data)\b", re.IGNORECASE)
_INDEX_RE = re.compile(r"\b(?:index|indexes|indices|unique\s+index|reindex)\b", re.IGNORECASE)
_DESTRUCTIVE_RE = re.compile(
    r"\b(?:drop|delete\s+existing|truncate|destructive|irreversible|data\s+loss|"
    r"remove\s+column|purge|wipe)\b",
    re.IGNORECASE,
)

_FILE_SIGNAL_PATTERNS: tuple[tuple[MigrationRiskCategory, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"(?:^|/)(?:migrations|alembic)(?:/|$)|versions/.*\.py$", re.I)),
    ("schema", re.compile(r"(?:^|/)(?:schemas?|db|database)(?:/|$)|schema\.(?:json|ya?ml)$", re.I)),
    ("model", re.compile(r"(?:^|/)(?:models?|entities)(?:/|$)|models?\.py$", re.I)),
    ("database", re.compile(r"\.(?:sql|ddl)$|(?:^|/)(?:store|repository|repositories)(?:/|$)", re.I)),
)

_TEXT_SIGNAL_PATTERNS: tuple[
    tuple[MigrationRiskCategory, re.Pattern[str], tuple[str, ...]],
    ...,
] = (
    ("destructive", _DESTRUCTIVE_RE, ("Plan rollback or restore steps before implementation.",)),
    ("backfill", _BACKFILL_RE, ("Define idempotent backfill batches and progress tracking.",)),
    ("migration", _MIGRATION_RE, ("Create or review forward and rollback migration steps.",)),
    ("schema", _SCHEMA_RE, ("Confirm compatibility for schema readers and writers.",)),
    ("model", _MODEL_RE, ("Keep ORM/model definitions and persisted schema aligned.",)),
    ("index", _INDEX_RE, ("Validate index build strategy and query-plan impact.",)),
    ("seed", _SEED_RE, ("Document seed data source, idempotency, and rollback behavior.",)),
    ("database", _DATABASE_RE, ("Capture storage impact and data ownership assumptions.",)),
)

_BASE_STEPS = (
    "Identify affected tables, models, records, and storage owners.",
    "Document forward migration steps and the rollback or restore path.",
    "Validate on a non-production dataset before release.",
)
_CATEGORY_STEPS: dict[MigrationRiskCategory, tuple[str, ...]] = {
    "database": ("Check application reads and writes against the changed storage behavior.",),
    "schema": ("Confirm schema compatibility, defaults, constraints, and nullable transitions.",),
    "model": ("Update model-level tests for the persisted field or relationship changes.",),
    "migration": ("Review migration upgrade and downgrade behavior before applying it.",),
    "backfill": ("Make the backfill idempotent and record expected row counts.",),
    "seed": ("Verify seed data can be applied repeatedly without duplicates.",),
    "index": ("Check index creation does not block critical production writes.",),
    "destructive": ("Require explicit backup, restore, and reviewer signoff for destructive changes.",),
    "metadata": ("Honor the task metadata migration hint in the implementation plan.",),
}


@dataclass(frozen=True, slots=True)
class DataMigrationRiskNote:
    """One detected data migration risk with task-field evidence."""

    category: MigrationRiskCategory
    message: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "message": self.message,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataMigrationChecklist:
    """Data migration safeguards for one execution task."""

    task_id: str
    title: str
    risk_level: MigrationRiskLevel
    required_steps: tuple[str, ...] = field(default_factory=tuple)
    risk_notes: tuple[DataMigrationRiskNote, ...] = field(default_factory=tuple)
    suggested_validation_commands: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "required_steps": list(self.required_steps),
            "risk_notes": [note.to_dict() for note in self.risk_notes],
            "suggested_validation_commands": list(self.suggested_validation_commands),
        }


@dataclass(frozen=True, slots=True)
class TaskDataMigrationChecklistResult:
    """Complete data migration checklist result."""

    plan_id: str | None = None
    checklists: tuple[TaskDataMigrationChecklist, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "checklists": [checklist.to_dict() for checklist in self.checklists],
        }


def build_task_data_migration_checklist(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskDataMigrationChecklistResult:
    """Detect data-impacting execution tasks and build migration safety checklists."""
    plan_id, tasks = _source_payload(source)
    checklists = tuple(
        checklist
        for index, task in enumerate(tasks, start=1)
        if (checklist := _task_checklist(task, index)) is not None
    )
    return TaskDataMigrationChecklistResult(plan_id=plan_id, checklists=checklists)


def task_data_migration_checklist_to_dict(
    result: TaskDataMigrationChecklistResult,
) -> dict[str, Any]:
    """Serialize a data migration checklist result to a plain dictionary."""
    return result.to_dict()


task_data_migration_checklist_to_dict.__test__ = False


def _task_checklist(
    task: Mapping[str, Any],
    index: int,
) -> TaskDataMigrationChecklist | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    evidence_by_category = _detected_evidence(task)
    if not evidence_by_category:
        return None

    categories = tuple(evidence_by_category)
    return TaskDataMigrationChecklist(
        task_id=task_id,
        title=title,
        risk_level=_risk_level(categories),
        required_steps=tuple(_required_steps(categories)),
        risk_notes=tuple(
            DataMigrationRiskNote(
                category=category,
                message=_risk_message(category),
                evidence=tuple(evidence),
            )
            for category, evidence in evidence_by_category.items()
        ),
        suggested_validation_commands=tuple(_validation_commands(task)),
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


def _detected_evidence(task: Mapping[str, Any]) -> dict[MigrationRiskCategory, tuple[str, ...]]:
    evidence_by_category: dict[MigrationRiskCategory, list[str]] = {}
    for path in _strings(task.get("files_or_modules")):
        for category, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(_normalized_path(path)):
                _append_evidence(evidence_by_category, category, f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        for category, pattern, _steps in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{source_field}: {text}")

    for source_field, text in _metadata_texts(task.get("metadata")):
        if _metadata_key_is_data_hint(source_field) or _DATABASE_RE.search(text) or _MIGRATION_RE.search(text):
            _append_evidence(evidence_by_category, "metadata", f"{source_field}: {text}")
        for category, pattern, _steps in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(text):
                _append_evidence(evidence_by_category, category, f"{source_field}: {text}")

    return {
        category: tuple(_dedupe(evidence))
        for category, evidence in evidence_by_category.items()
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


def _metadata_key_is_data_hint(source_field: str) -> bool:
    field = source_field.casefold()
    return any(
        token in field
        for token in (
            "data",
            "database",
            "schema",
            "migration",
            "backfill",
            "storage",
            "seed",
            "index",
        )
    )


def _required_steps(categories: Iterable[MigrationRiskCategory]) -> list[str]:
    steps = list(_BASE_STEPS)
    for category in categories:
        steps.extend(_CATEGORY_STEPS[category])
    return _dedupe(steps)


def _risk_message(category: MigrationRiskCategory) -> str:
    return {
        "database": "Task appears to change database or storage behavior.",
        "schema": "Task appears to change persisted schema shape or constraints.",
        "model": "Task appears to change persistence models or ORM mappings.",
        "migration": "Task appears to add or depend on migration steps.",
        "backfill": "Task appears to move, populate, or recompute existing data.",
        "seed": "Task appears to change seed or reference data.",
        "index": "Task appears to add or change database indexes.",
        "destructive": "Task includes potentially destructive data operations.",
        "metadata": "Task metadata explicitly hints at migration or data-impacting work.",
    }[category]


def _risk_level(categories: Iterable[MigrationRiskCategory]) -> MigrationRiskLevel:
    category_set = set(categories)
    if category_set & {"destructive", "backfill"}:
        return "high"
    if category_set & {"migration", "schema", "model", "index"}:
        return "medium"
    return "low"


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    candidate_texts: list[tuple[str, str]] = []
    test_command = _optional_text(task.get("test_command"))
    if test_command:
        candidate_texts.append(("test_command", test_command))
    candidate_texts.extend(
        (f"acceptance_criteria[{index}]", text)
        for index, text in enumerate(_strings(task.get("acceptance_criteria")))
    )
    candidate_texts.extend(_metadata_texts(task.get("metadata")))

    for source_field, text in candidate_texts:
        if _COMMAND_HINT_RE.search(text):
            commands.append(text if source_field == "test_command" else _extract_command(text))
    return _dedupe(command for command in commands if command)


def _extract_command(text: str) -> str:
    text = text.strip()
    if ":" in text and _COMMAND_HINT_RE.search(text.split(":", 1)[1]):
        return text.split(":", 1)[1].strip()
    return text


def _append_evidence(
    evidence_by_category: dict[MigrationRiskCategory, list[str]],
    category: MigrationRiskCategory,
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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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


__all__ = [
    "DataMigrationRiskNote",
    "TaskDataMigrationChecklist",
    "TaskDataMigrationChecklistResult",
    "build_task_data_migration_checklist",
    "task_data_migration_checklist_to_dict",
]
