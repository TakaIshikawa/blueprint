"""Plan database schema migration readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SchemaMigrationSignal = Literal[
    "migration",
    "schema_change",
    "table_creation",
    "column_rename",
    "backfill",
    "index_creation",
    "foreign_key",
    "constraint",
    "data_migration",
]
SchemaMigrationSafeguard = Literal[
    "backwards_compatible_rollout",
    "expand_contract_steps",
    "backfill_plan",
    "lock_timeout",
    "rollback_strategy",
    "migration_test",
    "production_volume_check",
    "monitoring",
]
SchemaMigrationReadinessLevel = Literal["ready", "partial", "missing_safeguards"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[SchemaMigrationSignal, int] = {
    "migration": 0,
    "schema_change": 1,
    "table_creation": 2,
    "column_rename": 3,
    "backfill": 4,
    "index_creation": 5,
    "foreign_key": 6,
    "constraint": 7,
    "data_migration": 8,
}
_SAFEGUARD_ORDER: tuple[SchemaMigrationSafeguard, ...] = (
    "backwards_compatible_rollout",
    "expand_contract_steps",
    "backfill_plan",
    "lock_timeout",
    "rollback_strategy",
    "migration_test",
    "production_volume_check",
    "monitoring",
)
_READINESS_ORDER: dict[SchemaMigrationReadinessLevel, int] = {
    "missing_safeguards": 0,
    "partial": 1,
    "ready": 2,
}
_PATH_SIGNAL_PATTERNS: tuple[tuple[SchemaMigrationSignal, re.Pattern[str]], ...] = (
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|db/versions|prisma/migrations|liquibase|flyway)(?:/|$)", re.I)),
    ("schema_change", re.compile(r"(?:^|/)(?:ddl|schema|schemas|database|db)(?:/|$)|\.(?:sql|ddl)$|schema\.prisma$", re.I)),
    ("backfill", re.compile(r"(?:^|/)(?:backfills?|batch[-_]?jobs?)(?:/|$)|backfill", re.I)),
    ("data_migration", re.compile(r"(?:^|/)(?:data[-_]?migrations?|data[-_]?fixes?)(?:/|$)|data[-_]?migration", re.I)),
    ("index_creation", re.compile(r"(?:^|/)(?:indexes?|indices)(?:/|$)|(?:create|add)[-_]?index|index", re.I)),
)
_TEXT_SIGNAL_PATTERNS: dict[SchemaMigrationSignal, re.Pattern[str]] = {
    "migration": re.compile(r"\b(?:schema migration|database migration|db migration|migration|migrate|alembic|flyway|liquibase)\b", re.I),
    "schema_change": re.compile(
        r"\b(?:schema change|ddl|alter table|add column|drop column|rename table|not null|non[- ]nullable|"
        r"primary key|foreign key|constraint|create table|drop table|create index|drop index)\b",
        re.I,
    ),
    "table_creation": re.compile(r"\b(?:create table|new table|add table|table creation|create the [a-z0-9_ -]+ table)\b", re.I),
    "column_rename": re.compile(r"\b(?:rename column|column rename|rename [a-z0-9_ -]+ column|renaming column)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|back fill|populate existing|rehydrate existing|recompute existing|batch update existing)\b", re.I),
    "index_creation": re.compile(r"\b(?:create index|add index|new index|unique index|concurrent index|index creation|reindex)\b", re.I),
    "foreign_key": re.compile(r"\b(?:foreign key|fk constraint|referential integrity|references [a-z0-9_]+)\b", re.I),
    "constraint": re.compile(r"\b(?:constraint|check constraint|unique constraint|not null|non[- ]nullable|primary key)\b", re.I),
    "data_migration": re.compile(r"\b(?:data migration|migrate existing data|transform existing records|data fix|data correction)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[SchemaMigrationSafeguard, re.Pattern[str]] = {
    "backwards_compatible_rollout": re.compile(
        r"\b(?:backward[- ]compatible|backwards[- ]compatible|forward[- ]compatible|additive|nullable first|"
        r"dual[- ]?(?:read|write)|old and new (?:schema|columns?)|compatibility window|feature flag)\b",
        re.I,
    ),
    "expand_contract_steps": re.compile(
        r"\b(?:expand[/-]?contract|expand contract|expand and contract|expand phase|contract phase|"
        r"deploy expand|deploy contract|two[- ]phase migration|multi[- ]phase migration)\b",
        re.I,
    ),
    "backfill_plan": re.compile(
        r"\b(?:backfill plan|backfill job|chunk(?:ed)? backfill|batch(?:ed)? backfill|idempotent backfill|"
        r"resume(?:able)? backfill|throttle(?:d)? backfill|populate existing records)\b",
        re.I,
    ),
    "lock_timeout": re.compile(
        r"\b(?:lock timeout|statement timeout|online migration|concurrent(?:ly)? index|create index concurrently|"
        r"non[- ]blocking|avoid table lock|table lock budget|maintenance window|no table rewrite)\b",
        re.I,
    ),
    "rollback_strategy": re.compile(
        r"\b(?:rollback strategy|rollback plan|roll back|rollback|revert plan|down migration|undo migration|"
        r"restore snapshot|point[- ]in[- ]time|pitr)\b",
        re.I,
    ),
    "migration_test": re.compile(
        r"\b(?:migration test|test migration|migration applies|migration dry[- ]run|schema test|"
        r"alembic upgrade|alembic downgrade|migrate up and down|staging migration)\b",
        re.I,
    ),
    "production_volume_check": re.compile(
        r"\b(?:production volume|prod volume|production table|large table|row count|record count|millions of rows|table size|"
        r"explain analyze|query plan|cardinality|volume check)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitoring|monitor|alert(?:ing)?|dashboard|metric|logs?|migration telemetry|watch errors|"
        r"slow query|deadlock|replication lag)\b",
        re.I,
    ),
}
_RECOMMENDED_STEPS: dict[SchemaMigrationSafeguard, str] = {
    "backwards_compatible_rollout": "Define how application code stays compatible while old and new schema versions coexist.",
    "expand_contract_steps": "Split risky changes into expand, backfill, validate, and contract steps with clear deployment ordering.",
    "backfill_plan": "Document batch size, throttling, resumability, idempotency, and ownership for any existing-record backfill.",
    "lock_timeout": "Set lock or statement timeout expectations and use online or concurrent DDL where the datastore supports it.",
    "rollback_strategy": "Attach rollback, revert, or recovery steps that preserve data created during the migration window.",
    "migration_test": "Run migration apply and rollback or downgrade checks in CI, staging, or an equivalent disposable database.",
    "production_volume_check": "Confirm row counts, table size, query plans, or equivalent production-volume estimates before rollout.",
    "monitoring": "Plan migration metrics, logs, alerts, or launch watch checks for execution and post-deploy validation.",
}


@dataclass(frozen=True, slots=True)
class TaskSchemaMigrationReadinessRecord:
    """Readiness guidance for one task that changes database schema or migrated data."""

    task_id: str
    title: str
    detected_signals: tuple[SchemaMigrationSignal, ...]
    present_safeguards: tuple[SchemaMigrationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SchemaMigrationSafeguard, ...] = field(default_factory=tuple)
    readiness_level: SchemaMigrationReadinessLevel = "missing_safeguards"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskSchemaMigrationReadinessPlan:
    """Plan-level schema migration readiness review."""

    plan_id: str | None = None
    records: tuple[TaskSchemaMigrationReadinessRecord, ...] = field(default_factory=tuple)
    migration_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "migration_task_ids": list(self.migration_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render schema migration readiness as deterministic Markdown."""
        title = "# Task Schema Migration Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Migration task count: {self.summary.get('migration_task_count', 0)}",
            f"- No-signal task count: {self.summary.get('no_signal_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No schema migration readiness records were inferred."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_schema_migration_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSchemaMigrationReadinessPlan:
    """Build schema migration readiness records for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    migration_task_ids = tuple(record.task_id for record in records)
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskSchemaMigrationReadinessPlan(
        plan_id=plan_id,
        records=records,
        migration_task_ids=migration_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary=_summary(records, task_count=len(tasks), no_signal_task_ids=no_signal_task_ids),
    )


def analyze_task_schema_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSchemaMigrationReadinessPlan:
    """Compatibility alias for building schema migration readiness plans."""
    return build_task_schema_migration_readiness_plan(source)


def summarize_task_schema_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSchemaMigrationReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSchemaMigrationReadinessPlan:
    """Build a schema migration readiness plan, accepting an existing plan unchanged."""
    if isinstance(source, TaskSchemaMigrationReadinessPlan):
        return source
    return build_task_schema_migration_readiness_plan(source)


def extract_task_schema_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSchemaMigrationReadinessPlan:
    """Compatibility alias for extracting schema migration readiness."""
    return build_task_schema_migration_readiness_plan(source)


def generate_task_schema_migration_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSchemaMigrationReadinessPlan:
    """Compatibility alias for generating schema migration readiness plans."""
    return build_task_schema_migration_readiness_plan(source)


def task_schema_migration_readiness_plan_to_dict(
    result: TaskSchemaMigrationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a schema migration readiness plan to a plain dictionary."""
    return result.to_dict()


task_schema_migration_readiness_plan_to_dict.__test__ = False


def task_schema_migration_readiness_plan_to_dicts(
    source: TaskSchemaMigrationReadinessPlan | Iterable[TaskSchemaMigrationReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize schema migration readiness records to plain dictionaries."""
    if isinstance(source, TaskSchemaMigrationReadinessPlan):
        return source.to_dicts()
    return [record.to_dict() for record in source]


task_schema_migration_readiness_plan_to_dicts.__test__ = False


def task_schema_migration_readiness_plan_to_markdown(
    result: TaskSchemaMigrationReadinessPlan,
) -> str:
    """Render a schema migration readiness plan as Markdown."""
    return result.to_markdown()


task_schema_migration_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[SchemaMigrationSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SchemaMigrationSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskSchemaMigrationReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _required_safeguards(signals.signals) if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskSchemaMigrationReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[SchemaMigrationSignal] = set()
    safeguard_hits: set[SchemaMigrationSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals = _path_signals(normalized)
        path_text_signals = _text_signals(normalized.replace("/", " ").replace("_", " ").replace("-", " "))
        detected = tuple(_ordered_dedupe([*path_signals, *path_text_signals], tuple(_SIGNAL_ORDER)))
        if detected:
            signal_hits.update(detected)
            signal_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched_signals = _text_signals(text)
        if matched_signals:
            signal_hits.update(matched_signals)
            signal_evidence.append(_evidence_snippet(source_field, text))
        matched_safeguards = _text_safeguards(text)
        if matched_safeguards:
            safeguard_hits.update(matched_safeguards)
            safeguard_evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(_ordered_dedupe(signal_hits, tuple(_SIGNAL_ORDER))),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(_ordered_dedupe(safeguard_hits, _SAFEGUARD_ORDER)),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(original: str) -> tuple[SchemaMigrationSignal, ...]:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return ()
    path = PurePosixPath(normalized)
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: list[SchemaMigrationSignal] = []
    for signal, pattern in _PATH_SIGNAL_PATTERNS:
        if pattern.search(normalized):
            signals.append(signal)
    if path.suffix in {".sql", ".ddl"}:
        signals.extend(("migration", "schema_change"))
    signals.extend(_text_signals(text))
    return tuple(_ordered_dedupe(signals, tuple(_SIGNAL_ORDER)))


def _text_signals(text: str) -> tuple[SchemaMigrationSignal, ...]:
    return tuple(signal for signal in _SIGNAL_ORDER if _TEXT_SIGNAL_PATTERNS[signal].search(text))


def _text_safeguards(text: str) -> tuple[SchemaMigrationSafeguard, ...]:
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if _SAFEGUARD_PATTERNS[safeguard].search(text))


def _required_safeguards(signals: tuple[SchemaMigrationSignal, ...]) -> tuple[SchemaMigrationSafeguard, ...]:
    required: list[SchemaMigrationSafeguard] = [
        "backwards_compatible_rollout",
        "rollback_strategy",
        "migration_test",
        "production_volume_check",
        "monitoring",
    ]
    if any(signal in signals for signal in ("schema_change", "column_rename", "foreign_key", "constraint", "data_migration")):
        required.append("expand_contract_steps")
    if any(signal in signals for signal in ("backfill", "data_migration", "column_rename")):
        required.append("backfill_plan")
    if any(signal in signals for signal in ("schema_change", "table_creation", "index_creation", "foreign_key", "constraint")):
        required.append("lock_timeout")
    return tuple(_ordered_dedupe(required, _SAFEGUARD_ORDER))


def _readiness_level(
    present: tuple[SchemaMigrationSafeguard, ...],
    missing: tuple[SchemaMigrationSafeguard, ...],
) -> SchemaMigrationReadinessLevel:
    if not missing:
        return "ready"
    if present:
        return "partial"
    return "missing_safeguards"


def _summary(
    records: tuple[TaskSchemaMigrationReadinessRecord, ...],
    *,
    task_count: int,
    no_signal_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    readiness_counts = {
        level: sum(1 for record in records if record.readiness_level == level)
        for level in _READINESS_ORDER
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_signals)
        for signal in _SIGNAL_ORDER
    }
    safeguard_counts = {
        safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
        for safeguard in _SAFEGUARD_ORDER
    }
    missing_safeguard_counts = {
        safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
        for safeguard in _SAFEGUARD_ORDER
    }
    missing_count = sum(len(record.missing_safeguards) for record in records)
    return {
        "task_count": task_count,
        "migration_task_count": len(records),
        "migration_task_ids": [record.task_id for record in records],
        "no_signal_task_count": len(no_signal_task_ids),
        "no_signal_task_ids": list(no_signal_task_ids),
        "missing_safeguard_count": missing_count,
        "readiness_counts": readiness_counts,
        "signal_counts": signal_counts,
        "safeguard_counts": safeguard_counts,
        "missing_safeguard_counts": missing_safeguard_counts,
        "status": (
            "no_schema_migration_signals"
            if not records
            else "ready"
            if missing_count == 0
            else "missing_schema_migration_safeguards"
        ),
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "tags",
        "labels",
        "notes",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _text_signals(key_text) or _text_safeguards(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _text_signals(key_text) or _text_safeguards(key_text):
                texts.append((field, str(key)))
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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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
    text = _clean_text(str(value))
    return [text] if text else []


def _ordered_dedupe(items: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    seen = set(items)
    return [item for item in order if item in seen]


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_snippet(text)}"


def _snippet(text: str, limit: int = 180) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "SchemaMigrationReadinessLevel",
    "SchemaMigrationSafeguard",
    "SchemaMigrationSignal",
    "TaskSchemaMigrationReadinessPlan",
    "TaskSchemaMigrationReadinessRecord",
    "analyze_task_schema_migration_readiness",
    "build_task_schema_migration_readiness_plan",
    "extract_task_schema_migration_readiness",
    "generate_task_schema_migration_readiness",
    "summarize_task_schema_migration_readiness",
    "task_schema_migration_readiness_plan_to_dict",
    "task_schema_migration_readiness_plan_to_dicts",
    "task_schema_migration_readiness_plan_to_markdown",
]
