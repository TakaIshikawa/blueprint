"""Plan backup and restore readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


BackupRestoreSignal = Literal[
    "backup_restore",
    "data_store",
    "backup_cadence",
    "restore_drill",
    "recovery_point_objective",
    "recovery_time_objective",
    "data_integrity_verification",
    "access_controls",
    "operational_runbook",
    "destructive_data_change",
]
BackupRestoreReadinessCategory = Literal[
    "backup_cadence",
    "restore_validation",
    "recovery_point_objective",
    "recovery_time_objective",
    "data_integrity_verification",
    "access_controls",
    "operational_runbook",
]
BackupRestoreReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[BackupRestoreReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[BackupRestoreSignal, ...] = (
    "backup_restore",
    "data_store",
    "backup_cadence",
    "restore_drill",
    "recovery_point_objective",
    "recovery_time_objective",
    "data_integrity_verification",
    "access_controls",
    "operational_runbook",
    "destructive_data_change",
)
_CATEGORY_ORDER: tuple[BackupRestoreReadinessCategory, ...] = (
    "backup_cadence",
    "restore_validation",
    "recovery_point_objective",
    "recovery_time_objective",
    "data_integrity_verification",
    "access_controls",
    "operational_runbook",
)
_SIGNAL_TO_CATEGORY: dict[BackupRestoreSignal, BackupRestoreReadinessCategory] = {
    "backup_cadence": "backup_cadence",
    "restore_drill": "restore_validation",
    "recovery_point_objective": "recovery_point_objective",
    "recovery_time_objective": "recovery_time_objective",
    "data_integrity_verification": "data_integrity_verification",
    "access_controls": "access_controls",
    "operational_runbook": "operational_runbook",
}
_SIGNAL_PATTERNS: dict[BackupRestoreSignal, re.Pattern[str]] = {
    "backup_restore": re.compile(
        r"\b(?:backup(?:s)?|snapshot(?:s)?|restore|restoration|recovery|disaster recovery|dr plan|"
        r"recover(?:ed|y)?|point[- ]in[- ]time restore|pitr)\b",
        re.I,
    ),
    "data_store": re.compile(
        r"\b(?:database|datastore|data store|storage|object storage|bucket|blob store|uploads?|files? store|"
        r"postgres|postgresql|mysql|mongodb|mongo|redis|dynamodb|s3|gcs|azure blob|rds|cloud sql|"
        r"persistent data|customer data|tenant data|stored records?)\b",
        re.I,
    ),
    "backup_cadence": re.compile(
        r"\b(?:backup cadence|backup schedule|backup frequency|daily backup|hourly backup|nightly backup|"
        r"backups? run (?:daily|hourly|nightly|weekly)|snapshot cadence|snapshot schedule|"
        r"retention period|retention policy)\b",
        re.I,
    ),
    "restore_drill": re.compile(
        r"\b(?:restore drill|restore rehearsal|restore validation|restore test|test restore|rehearse restore|"
        r"recovery drill|disaster recovery drill|dr drill|restore in staging|restoration test)\b",
        re.I,
    ),
    "recovery_point_objective": re.compile(
        r"\b(?:recovery point objective|rpo|maximum data loss|data loss objective|point[- ]in[- ]time recovery|pitr)\b",
        re.I,
    ),
    "recovery_time_objective": re.compile(
        r"\b(?:recovery time objective|rto|time to restore|restore within|recovery time|maximum outage|"
        r"service restoration target)\b",
        re.I,
    ),
    "data_integrity_verification": re.compile(
        r"\b(?:data integrity|integrity verification|checksum|checksums|hash verification|row count|record count|"
        r"consistency check|referential integrity|backup integrity|restore integrity)\b",
        re.I,
    ),
    "access_controls": re.compile(
        r"\b(?:access controls?|least privilege|restore permission|backup permission|break[- ]glass|"
        r"operator access|admin approval|encrypted backup|kms|iam|audit log|restore approval)\b",
        re.I,
    ),
    "operational_runbook": re.compile(
        r"\b(?:runbook|playbook|operational procedure|ops procedure|incident procedure|on[- ]call|"
        r"escalation path|restore procedure|recovery procedure)\b",
        re.I,
    ),
    "destructive_data_change": re.compile(
        r"\b(?:drop table|drop column|truncate|bulk delete|hard delete|purge|wipe|erase|overwrite existing|"
        r"destructive migration|delete existing|remove existing|contract migration)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[BackupRestoreSignal, re.Pattern[str]] = {
    "backup_restore": re.compile(r"(?:backup|restore|snapshot|recovery|disaster[_-]?recovery|pitr)", re.I),
    "data_store": re.compile(
        r"(?:database|datastore|storage|bucket|blob|uploads?|files?|migrations?|schema|models?|repositories|"
        r"postgres|mysql|mongo|redis|dynamodb|s3|gcs)",
        re.I,
    ),
    "backup_cadence": re.compile(r"(?:backup[_-]?schedule|backup[_-]?cadence|snapshot[_-]?schedule|retention)", re.I),
    "restore_drill": re.compile(r"(?:restore[_-]?(?:drill|test|validation|rehearsal)|dr[_-]?drill)", re.I),
    "recovery_point_objective": re.compile(r"(?:rpo|recovery[_-]?point|pitr)", re.I),
    "recovery_time_objective": re.compile(r"(?:rto|recovery[_-]?time)", re.I),
    "data_integrity_verification": re.compile(r"(?:integrity|checksum|consistency|reconcile)", re.I),
    "access_controls": re.compile(r"(?:access|permission|iam|kms|audit|approval)", re.I),
    "operational_runbook": re.compile(r"(?:runbook|playbook|ops|incident)", re.I),
    "destructive_data_change": re.compile(r"(?:drop|truncate|purge|wipe|hard[_-]?delete|destructive)", re.I),
}
_NO_BACKUP_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:backup|restore|restoration|recovery|snapshot|data store|storage)"
    r"\b.{0,80}\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_DATA_STORE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("postgresql", re.compile(r"\b(?:postgres|postgresql|pg_)\b|(?:^|/)postgres(?:ql)?(?:/|$)", re.I)),
    ("mysql", re.compile(r"\b(?:mysql|mariadb|binlog)\b|(?:^|/)mysql(?:/|$)", re.I)),
    ("mongodb", re.compile(r"\b(?:mongodb|mongo)\b", re.I)),
    ("redis", re.compile(r"\bredis\b", re.I)),
    ("dynamodb", re.compile(r"\bdynamodb\b", re.I)),
    ("s3", re.compile(r"\b(?:s3|aws bucket|s3 bucket)\b", re.I)),
    ("gcs", re.compile(r"\b(?:gcs|google cloud storage)\b", re.I)),
    ("azure_blob", re.compile(r"\b(?:azure blob|blob storage)\b", re.I)),
)
_CATEGORY_GUIDANCE: dict[BackupRestoreReadinessCategory, tuple[str, tuple[str, ...]]] = {
    "backup_cadence": (
        "Define the backup or snapshot cadence for every affected data store.",
        (
            "Backup frequency, retention, and owner are documented for each affected data store.",
            "The cadence is enforced through infrastructure, scheduler, or managed-service configuration.",
            "Monitoring or evidence proves the latest backup completed inside the expected window.",
        ),
    ),
    "restore_validation": (
        "Validate restore behavior before shipping data or storage changes.",
        (
            "A restore drill runs in staging, a disposable environment, or another approved non-production target.",
            "The drill records restore outcome, elapsed time, operator, and source backup identifier.",
            "Rollout is blocked when the latest restore validation is missing or failed.",
        ),
    ),
    "recovery_point_objective": (
        "Set and prove the recovery point objective for the affected data.",
        (
            "The maximum acceptable data loss is stated as an RPO target.",
            "Backup, snapshot, log archive, or replication settings can meet the RPO target.",
            "Validation evidence shows the newest restorable point falls inside the RPO window.",
        ),
    ),
    "recovery_time_objective": (
        "Set and prove the recovery time objective for restore operations.",
        (
            "The maximum acceptable restore duration is stated as an RTO target.",
            "Restore drill timing demonstrates the target can be met for the expected data volume.",
            "Escalation or rollback steps are documented for an RTO miss.",
        ),
    ),
    "data_integrity_verification": (
        "Verify restored data integrity before declaring recovery complete.",
        (
            "Checksums, row counts, object counts, or domain consistency checks compare source and restored data.",
            "Integrity checks fail closed and produce reviewable evidence.",
            "The task names who signs off on restored data correctness.",
        ),
    ),
    "access_controls": (
        "Restrict backup and restore access to approved operators and audited paths.",
        (
            "Backup artifacts and restore actions use least-privilege access controls.",
            "Sensitive backups are encrypted and key access is documented.",
            "Restore attempts, approvals, and privileged access are audit logged.",
        ),
    ),
    "operational_runbook": (
        "Publish the operational runbook for backup failure and restore execution.",
        (
            "The runbook names owners, prerequisites, restore commands, verification steps, and escalation contacts.",
            "The runbook covers backup failure, restore failure, and post-restore customer or stakeholder communication.",
            "On-call or operations staff can execute the runbook without product engineering context.",
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class BackupRestoreReadinessTask:
    """One generated implementation task for backup and restore readiness."""

    category: BackupRestoreReadinessCategory
    title: str
    description: str
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBackupRestoreReadinessRecord:
    """Backup and restore readiness guidance for one execution task."""

    task_id: str
    title: str
    affected_data_stores: tuple[str, ...]
    detected_signals: tuple[BackupRestoreSignal, ...]
    present_expectations: tuple[BackupRestoreReadinessCategory, ...] = field(default_factory=tuple)
    missing_expectations: tuple[BackupRestoreReadinessCategory, ...] = field(default_factory=tuple)
    generated_tasks: tuple[BackupRestoreReadinessTask, ...] = field(default_factory=tuple)
    readiness: BackupRestoreReadinessLevel = "needs_planning"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[BackupRestoreSignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[BackupRestoreReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

    @property
    def acceptance_criteria(self) -> tuple[str, ...]:
        """Flatten generated task acceptance criteria for simple consumers."""
        return tuple(criteria for task in self.generated_tasks for criteria in task.acceptance_criteria)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "affected_data_stores": list(self.affected_data_stores),
            "detected_signals": list(self.detected_signals),
            "present_expectations": list(self.present_expectations),
            "missing_expectations": list(self.missing_expectations),
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBackupRestoreReadinessPlan:
    """Plan-level backup and restore readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskBackupRestoreReadinessRecord, ...] = field(default_factory=tuple)
    backup_restore_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskBackupRestoreReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskBackupRestoreReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.backup_restore_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "backup_restore_task_ids": list(self.backup_restore_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return backup and restore readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render backup and restore readiness guidance as deterministic Markdown."""
        title = "# Task Backup Restore Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Backup-restore task count: {self.summary.get('backup_restore_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task backup restore readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Data Stores | Signals | Missing Expectations | Generated Tasks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.affected_data_stores) or 'unspecified')} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_expectations) or 'none')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_backup_restore_readiness_plan(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Build backup and restore readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.missing_expectations),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    backup_restore_task_ids = tuple(record.task_id for record in records)
    impacted = set(backup_restore_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskBackupRestoreReadinessPlan(
        plan_id=plan_id,
        records=records,
        backup_restore_task_ids=backup_restore_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for building backup and restore readiness plans."""
    return build_task_backup_restore_readiness_plan(source)


def recommend_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for recommending backup and restore readiness tasks."""
    return build_task_backup_restore_readiness_plan(source)


def summarize_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for summarizing backup and restore readiness plans."""
    return build_task_backup_restore_readiness_plan(source)


def generate_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for generating backup and restore readiness plans."""
    return build_task_backup_restore_readiness_plan(source)


def extract_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for extracting backup and restore readiness plans."""
    return build_task_backup_restore_readiness_plan(source)


def derive_task_backup_restore_readiness(source: Any) -> TaskBackupRestoreReadinessPlan:
    """Compatibility alias for deriving backup and restore readiness plans."""
    return build_task_backup_restore_readiness_plan(source)


def task_backup_restore_readiness_plan_to_dict(result: TaskBackupRestoreReadinessPlan) -> dict[str, Any]:
    """Serialize a backup and restore readiness plan to a plain dictionary."""
    return result.to_dict()


task_backup_restore_readiness_plan_to_dict.__test__ = False


def task_backup_restore_readiness_plan_to_dicts(
    result: TaskBackupRestoreReadinessPlan | Iterable[TaskBackupRestoreReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize backup and restore readiness records to plain dictionaries."""
    if isinstance(result, TaskBackupRestoreReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_backup_restore_readiness_plan_to_dicts.__test__ = False
task_backup_restore_readiness_to_dicts = task_backup_restore_readiness_plan_to_dicts
task_backup_restore_readiness_to_dicts.__test__ = False


def task_backup_restore_readiness_plan_to_markdown(result: TaskBackupRestoreReadinessPlan) -> str:
    """Render a backup and restore readiness plan as Markdown."""
    return result.to_markdown()


task_backup_restore_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[BackupRestoreSignal, ...] = field(default_factory=tuple)
    affected_data_stores: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskBackupRestoreReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not _is_relevant(signals.signals):
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    present = _present_expectations(signals.signals)
    missing = tuple(category for category in _CATEGORY_ORDER if category not in present)
    generated_tasks = _generated_tasks(title, signals, missing)
    return TaskBackupRestoreReadinessRecord(
        task_id=task_id,
        title=title,
        affected_data_stores=signals.affected_data_stores,
        detected_signals=signals.signals,
        present_expectations=present,
        missing_expectations=missing,
        generated_tasks=generated_tasks,
        readiness=_readiness(present, missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[BackupRestoreSignal] = set()
    store_hits: list[str] = []
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        if _path_implies_data_store(normalized):
            signal_hits.add("data_store")
            matched = True
        store_hits.extend(_data_stores(f"{normalized} {searchable}"))
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_BACKUP_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        store_hits.extend(_data_stores(text))
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits & {"backup_cadence", "restore_drill", "recovery_point_objective", "recovery_time_objective"}:
        signal_hits.add("backup_restore")
    signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits)
    return _Signals(
        signals=signals,
        affected_data_stores=tuple(_ordered_stores(store_hits)),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _is_relevant(signals: tuple[BackupRestoreSignal, ...]) -> bool:
    if "backup_restore" in signals or "destructive_data_change" in signals:
        return True
    return "data_store" in signals and bool(set(signals) - {"data_store"})


def _present_expectations(signals: tuple[BackupRestoreSignal, ...]) -> tuple[BackupRestoreReadinessCategory, ...]:
    categories = {_SIGNAL_TO_CATEGORY[signal] for signal in signals if signal in _SIGNAL_TO_CATEGORY}
    return tuple(category for category in _CATEGORY_ORDER if category in categories)


def _generated_tasks(
    title: str,
    signals: _Signals,
    missing: tuple[BackupRestoreReadinessCategory, ...],
) -> tuple[BackupRestoreReadinessTask, ...]:
    evidence = signals.evidence[:3]
    stores = ", ".join(signals.affected_data_stores) if signals.affected_data_stores else "affected data stores"
    signal_text = ", ".join(signals.signals)
    tasks: list[BackupRestoreReadinessTask] = []
    for category in _CATEGORY_ORDER:
        description, acceptance = _CATEGORY_GUIDANCE[category]
        status = "Missing expectation" if category in missing else "Existing expectation"
        tasks.append(
            BackupRestoreReadinessTask(
                category=category,
                title=f"{_title_case(category)} for {title}",
                description=(
                    f"{description} Scope: {stores}. {status}. "
                    f"Rationale: detected backup/restore signals: {signal_text}."
                ),
                acceptance_criteria=acceptance,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _readiness(
    present: tuple[BackupRestoreReadinessCategory, ...],
    missing: tuple[BackupRestoreReadinessCategory, ...],
) -> BackupRestoreReadinessLevel:
    if not missing:
        return "ready"
    if present:
        return "partial"
    return "needs_planning"


def _summary(
    records: tuple[TaskBackupRestoreReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "backup_restore_task_count": len(records),
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": sum(len(record.generated_tasks) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness == level)
            for level in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_expectation_counts": {
            category: sum(1 for record in records if category in record.present_expectations)
            for category in _CATEGORY_ORDER
        },
        "missing_expectation_counts": {
            category: sum(1 for record in records if category in record.missing_expectations)
            for category in _CATEGORY_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for record in records for task in record.generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
        },
        "data_store_counts": {
            store: sum(1 for record in records if store in record.affected_data_stores)
            for store in sorted({store for record in records for store in record.affected_data_stores})
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)
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
        "validation_commands",
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_PATH_SIGNAL_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    try:
        commands = flatten_validation_commands(task.get("validation_commands"))
    except (TypeError, ValueError):
        commands = ()
    return [
        ("validation_commands", command)
        for command in _dedupe([*commands, *_strings(task.get("validation_commands"))])
    ]


def _path_implies_data_store(path: str) -> bool:
    normalized = path.casefold()
    name = PurePosixPath(normalized).name
    if name in {"schema.sql", "database.sql", "db.sql", "models.py", "repository.py", "schema.prisma"}:
        return True
    return bool(re.search(r"(?:^|/)(?:migrations?|repositories|models?|storage|uploads?|buckets?)(?:/|$)", normalized))


def _data_stores(text: str) -> list[str]:
    stores = [store for store, pattern in _DATA_STORE_PATTERNS if pattern.search(text)]
    if re.search(r"\b(?:database|datastore|data store|schema migration|ddl|persistent data)\b", text, re.I):
        stores.append("database")
    if re.search(r"\b(?:storage|bucket|object storage|blob store|uploads?|files? store)\b", text, re.I):
        stores.append("object_storage")
    return stores


def _ordered_stores(values: Iterable[str]) -> list[str]:
    priority = {
        "postgresql": 0,
        "mysql": 1,
        "mongodb": 2,
        "redis": 3,
        "dynamodb": 4,
        "s3": 5,
        "gcs": 6,
        "azure_blob": 7,
        "object_storage": 8,
        "database": 9,
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


def _title_case(value: str) -> str:
    return value.replace("_", " ").title()


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
    "BackupRestoreReadinessCategory",
    "BackupRestoreReadinessLevel",
    "BackupRestoreReadinessTask",
    "BackupRestoreSignal",
    "TaskBackupRestoreReadinessPlan",
    "TaskBackupRestoreReadinessRecord",
    "analyze_task_backup_restore_readiness",
    "build_task_backup_restore_readiness_plan",
    "derive_task_backup_restore_readiness",
    "extract_task_backup_restore_readiness",
    "generate_task_backup_restore_readiness",
    "recommend_task_backup_restore_readiness",
    "summarize_task_backup_restore_readiness",
    "task_backup_restore_readiness_plan_to_dict",
    "task_backup_restore_readiness_plan_to_dicts",
    "task_backup_restore_readiness_plan_to_markdown",
    "task_backup_restore_readiness_to_dicts",
]
