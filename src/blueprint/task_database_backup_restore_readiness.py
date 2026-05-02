"""Plan database backup and restore readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DatabaseTaskSignal = Literal[
    "database",
    "schema_migration",
    "ddl",
    "orm_model",
    "persistence_layer",
    "storage",
    "destructive_data_flow",
]
DatabaseSafeguard = Literal[
    "backup_verification",
    "restore_rehearsal",
    "point_in_time_recovery",
    "rollback_data_snapshot",
    "owner_evidence",
]
DatabaseReadinessRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[DatabaseTaskSignal, int] = {
    "database": 0,
    "schema_migration": 1,
    "ddl": 2,
    "orm_model": 3,
    "persistence_layer": 4,
    "storage": 5,
    "destructive_data_flow": 6,
}
_SAFEGUARD_ORDER: tuple[DatabaseSafeguard, ...] = (
    "backup_verification",
    "restore_rehearsal",
    "point_in_time_recovery",
    "rollback_data_snapshot",
    "owner_evidence",
)
_RISK_ORDER: dict[DatabaseReadinessRisk, int] = {"high": 0, "medium": 1, "low": 2}
_PATH_SIGNAL_PATTERNS: tuple[tuple[DatabaseTaskSignal, re.Pattern[str]], ...] = (
    ("schema_migration", re.compile(r"(?:^|/)(?:migrations?|alembic|db/versions|prisma/migrations)(?:/|$)", re.I)),
    ("ddl", re.compile(r"(?:^|/)(?:ddl|schema|schemas|database|db)(?:/|$)|\.(?:sql|ddl)$", re.I)),
    (
        "orm_model",
        re.compile(r"(?:^|/)(?:models?|entities|orm|prisma|sequelize|typeorm|sqlalchemy)(?:/|$)|schema\.prisma$", re.I),
    ),
    (
        "persistence_layer",
        re.compile(r"(?:^|/)(?:repositories|repository|dao|persistence|store|stores|queries|dbal)(?:/|$)", re.I),
    ),
    (
        "storage",
        re.compile(r"(?:^|/)(?:storage|uploads?|blobs?|buckets?|s3|gcs|azure[-_]?blob|dynamodb|redis|cache)(?:/|$)", re.I),
    ),
)
_TEXT_SIGNAL_PATTERNS: dict[DatabaseTaskSignal, re.Pattern[str]] = {
    "database": re.compile(
        r"\b(?:database|datastore|data store|postgres|postgresql|mysql|mariadb|sqlite|mongodb|mongo|"
        r"dynamodb|redis|cassandra|cockroach|aurora|rds|cloud sql|spanner|elasticsearch|opensearch)\b",
        re.I,
    ),
    "schema_migration": re.compile(r"\b(?:schema migration|database migration|db migration|migration|migrate|alembic|flyway|liquibase)\b", re.I),
    "ddl": re.compile(
        r"\b(?:ddl|alter table|create table|drop table|rename table|rename column|drop column|"
        r"truncate table|add column|not null|foreign key|primary key|create index|drop index)\b",
        re.I,
    ),
    "orm_model": re.compile(r"\b(?:orm|model change|prisma|sequelize|typeorm|sqlalchemy|django model|entity model)\b", re.I),
    "persistence_layer": re.compile(
        r"\b(?:repository|repositories|dao|persistence layer|stored records?|write path|read path|query layer|upsert|persist)\b",
        re.I,
    ),
    "storage": re.compile(
        r"\b(?:storage|object storage|bucket|s3|gcs|azure blob|blob store|uploads?|files? store|cache|redis|dynamodb)\b",
        re.I,
    ),
    "destructive_data_flow": re.compile(
        r"\b(?:drop|delete all|bulk delete|purge|truncate|wipe|erase|hard delete|destroy|remove existing|"
        r"overwrite existing|backfill overwrite|rename column|drop column|contract phase)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[DatabaseSafeguard, re.Pattern[str]] = {
    "backup_verification": re.compile(
        r"\b(?:backup verification|verified backups?|backup integrity|backup checksum|backup exists|"
        r"validated snapshot|snapshot validation|backup validation)\b",
        re.I,
    ),
    "restore_rehearsal": re.compile(
        r"\b(?:restore rehearsal|restore drill|restore test|rehearse restore|test restore|restoration test|"
        r"dry[- ]run restore|recover(?:y)? drill)\b",
        re.I,
    ),
    "point_in_time_recovery": re.compile(r"\b(?:point[- ]in[- ]time recovery|pitr|wal archive|binlog|redo log|recovery point|rpo)\b", re.I),
    "rollback_data_snapshot": re.compile(
        r"\b(?:rollback snapshot|data snapshot|pre[- ]change snapshot|before snapshot|snapshot before|rollback dataset|"
        r"export before|pre[- ]migration dump|database dump)\b",
        re.I,
    ),
    "owner_evidence": re.compile(
        r"\b(?:data owner|database owner|storage owner|service owner|dba|owner approval|owner sign[- ]?off|"
        r"approved by|change approval|release owner)\b",
        re.I,
    ),
}
_DATA_STORE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("postgresql", re.compile(r"\b(?:postgres|postgresql|pg_)\b|(?:^|/)postgres(?:ql)?(?:/|$)", re.I)),
    ("mysql", re.compile(r"\b(?:mysql|mariadb|binlog)\b|(?:^|/)mysql(?:/|$)", re.I)),
    ("sqlite", re.compile(r"\bsqlite\b|\.sqlite3?$", re.I)),
    ("mongodb", re.compile(r"\b(?:mongodb|mongo)\b", re.I)),
    ("dynamodb", re.compile(r"\bdynamodb\b", re.I)),
    ("redis", re.compile(r"\bredis\b", re.I)),
    ("s3", re.compile(r"\b(?:s3|aws bucket|s3 bucket)\b", re.I)),
    ("gcs", re.compile(r"\b(?:gcs|google cloud storage)\b", re.I)),
    ("azure_blob", re.compile(r"\b(?:azure blob|blob storage)\b", re.I)),
    ("elasticsearch", re.compile(r"\b(?:elasticsearch|opensearch)\b", re.I)),
)
_GENERIC_STORE_BY_SIGNAL: dict[DatabaseTaskSignal, str] = {
    "persistence_layer": "application_database",
    "orm_model": "application_database",
    "database": "database",
    "schema_migration": "database",
    "ddl": "database",
    "destructive_data_flow": "database",
}
_RECOMMENDED_STEPS: dict[DatabaseSafeguard, str] = {
    "backup_verification": "Verify current backups or snapshots are complete, recent, and restorable before execution.",
    "restore_rehearsal": "Rehearse restore on a staging or disposable environment and record elapsed time and outcome.",
    "point_in_time_recovery": "Confirm point-in-time recovery coverage, retention, and target recovery point for the affected store.",
    "rollback_data_snapshot": "Capture a rollback data snapshot or export immediately before destructive or migration work starts.",
    "owner_evidence": "Attach data, database, storage, or service owner approval evidence to the task.",
}


@dataclass(frozen=True, slots=True)
class TaskDatabaseBackupRestoreReadinessRecord:
    """Readiness guidance for one task touching database or persistence data."""

    task_id: str
    title: str
    affected_data_stores: tuple[str, ...]
    detected_signals: tuple[DatabaseTaskSignal, ...]
    present_safeguards: tuple[DatabaseSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DatabaseSafeguard, ...] = field(default_factory=tuple)
    risk_level: DatabaseReadinessRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "affected_data_stores": list(self.affected_data_stores),
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskDatabaseBackupRestoreReadinessPlan:
    """Plan-level database backup and restore readiness review."""

    plan_id: str | None = None
    records: tuple[TaskDatabaseBackupRestoreReadinessRecord, ...] = field(default_factory=tuple)
    database_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "database_task_ids": list(self.database_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render database backup and restore readiness as deterministic Markdown."""
        title = "# Task Database Backup Restore Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Database task count: {self.summary.get('database_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No database backup or restore readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Data Stores | Signals | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.affected_data_stores) or 'none')} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_database_backup_restore_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseBackupRestoreReadinessPlan:
    """Build backup and restore readiness records for database-impacting tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    database_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDatabaseBackupRestoreReadinessPlan(
        plan_id=plan_id,
        records=records,
        database_task_ids=database_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_database_backup_restore_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseBackupRestoreReadinessPlan:
    """Compatibility alias for building database backup and restore readiness plans."""
    return build_task_database_backup_restore_readiness_plan(source)


def summarize_task_database_backup_restore_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseBackupRestoreReadinessPlan:
    """Compatibility alias for building database backup and restore readiness plans."""
    return build_task_database_backup_restore_readiness_plan(source)


def extract_task_database_backup_restore_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseBackupRestoreReadinessPlan:
    """Compatibility alias for building database backup and restore readiness plans."""
    return build_task_database_backup_restore_readiness_plan(source)


def generate_task_database_backup_restore_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDatabaseBackupRestoreReadinessPlan:
    """Compatibility alias for generating database backup and restore readiness plans."""
    return build_task_database_backup_restore_readiness_plan(source)


def task_database_backup_restore_readiness_plan_to_dict(
    result: TaskDatabaseBackupRestoreReadinessPlan,
) -> dict[str, Any]:
    """Serialize a database backup and restore readiness plan to a plain dictionary."""
    return result.to_dict()


task_database_backup_restore_readiness_plan_to_dict.__test__ = False


def task_database_backup_restore_readiness_plan_to_markdown(
    result: TaskDatabaseBackupRestoreReadinessPlan,
) -> str:
    """Render a database backup and restore readiness plan as Markdown."""
    return result.to_markdown()


task_database_backup_restore_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DatabaseTaskSignal, ...] = field(default_factory=tuple)
    affected_data_stores: tuple[str, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DatabaseSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDatabaseBackupRestoreReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDatabaseBackupRestoreReadinessRecord(
        task_id=task_id,
        title=title,
        affected_data_stores=signals.affected_data_stores,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DatabaseTaskSignal] = set()
    safeguard_hits: set[DatabaseSafeguard] = set()
    store_hits: list[str] = []
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        store_hits.extend(_data_stores(f"{normalized} {searchable}"))

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_signal = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched_signal = True
        if matched_signal:
            signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)
        store_hits.extend(_data_stores(text))

    signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits)
    for signal in signals:
        if store := _GENERIC_STORE_BY_SIGNAL.get(signal):
            store_hits.append(store)
    return _Signals(
        signals=signals,
        affected_data_stores=tuple(_ordered_stores(store_hits)),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[DatabaseTaskSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[DatabaseTaskSignal] = set()
    for signal, pattern in _PATH_SIGNAL_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            signals.add(signal)
    if re.search(r"\b(?:drop|delete|purge|truncate|wipe|destroy|overwrite)\b", text):
        signals.add("destructive_data_flow")
    if signals & {"schema_migration", "ddl", "orm_model", "persistence_layer"}:
        signals.add("database")
    name = PurePosixPath(normalized).name
    if name in {"schema.sql", "database.sql", "db.sql", "models.py", "repository.py"}:
        signals.add("database")
    return signals


def _risk_level(
    signals: tuple[DatabaseTaskSignal, ...],
    present: tuple[DatabaseSafeguard, ...],
    missing: tuple[DatabaseSafeguard, ...],
) -> DatabaseReadinessRisk:
    if not missing:
        return "low"
    present_set = set(present)
    missing_set = set(missing)
    destructive = "destructive_data_flow" in signals
    migration = bool({"schema_migration", "ddl"} & set(signals))
    if destructive and {"backup_verification", "restore_rehearsal"} & missing_set:
        return "high"
    if migration and {"backup_verification", "restore_rehearsal", "rollback_data_snapshot"} & missing_set:
        return "high"
    if {"backup_verification", "restore_rehearsal"} <= present_set:
        return "medium"
    if len(missing) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskDatabaseBackupRestoreReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "database_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "data_store_counts": {
            store: sum(1 for record in records if store in record.affected_data_stores)
            for store in sorted({store for record in records for store in record.affected_data_stores})
        },
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
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _data_stores(text: str) -> list[str]:
    stores = [store for store, pattern in _DATA_STORE_PATTERNS if pattern.search(text)]
    if re.search(r"\b(?:database|datastore|data store|schema migration|ddl)\b", text, re.I):
        stores.append("database")
    if re.search(r"\b(?:storage|bucket|object storage|blob store|uploads?)\b", text, re.I):
        stores.append("object_storage")
    return stores


def _ordered_stores(values: Iterable[str]) -> list[str]:
    priority = {
        "postgresql": 0,
        "mysql": 1,
        "sqlite": 2,
        "mongodb": 3,
        "dynamodb": 4,
        "redis": 5,
        "s3": 6,
        "gcs": 7,
        "azure_blob": 8,
        "elasticsearch": 9,
        "object_storage": 10,
        "application_database": 11,
        "database": 12,
    }
    return sorted(_dedupe(values), key=lambda store: (priority.get(store, 99), store))


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
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "DatabaseReadinessRisk",
    "DatabaseSafeguard",
    "DatabaseTaskSignal",
    "TaskDatabaseBackupRestoreReadinessPlan",
    "TaskDatabaseBackupRestoreReadinessRecord",
    "analyze_task_database_backup_restore_readiness",
    "build_task_database_backup_restore_readiness_plan",
    "extract_task_database_backup_restore_readiness",
    "generate_task_database_backup_restore_readiness",
    "summarize_task_database_backup_restore_readiness",
    "task_database_backup_restore_readiness_plan_to_dict",
    "task_database_backup_restore_readiness_plan_to_markdown",
]
