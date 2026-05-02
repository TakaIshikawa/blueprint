"""Plan deletion, purge, and right-to-erasure safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DataDeletionSignal = Literal[
    "hard_delete",
    "purge",
    "erasure",
    "account_deletion",
    "gdpr_deletion",
    "tombstone",
    "cascading_delete",
    "retention_exception",
    "backup_deletion",
    "search_index_removal",
    "analytics_removal",
    "audit_evidence",
]
DataDeletionSafeguard = Literal[
    "dry_run_counts",
    "cascade_inventory",
    "backup_restore_implications",
    "legal_hold_check",
    "audit_trail",
    "idempotency",
    "downstream_deletion_propagation",
    "customer_confirmation",
]
DataDeletionRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[DataDeletionRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[DataDeletionSignal, ...] = (
    "hard_delete",
    "purge",
    "erasure",
    "account_deletion",
    "gdpr_deletion",
    "tombstone",
    "cascading_delete",
    "retention_exception",
    "backup_deletion",
    "search_index_removal",
    "analytics_removal",
    "audit_evidence",
)
_SAFEGUARD_ORDER: tuple[DataDeletionSafeguard, ...] = (
    "dry_run_counts",
    "cascade_inventory",
    "backup_restore_implications",
    "legal_hold_check",
    "audit_trail",
    "idempotency",
    "downstream_deletion_propagation",
    "customer_confirmation",
)
_IRREVERSIBLE_SIGNALS = {
    "hard_delete",
    "purge",
    "erasure",
    "account_deletion",
    "gdpr_deletion",
    "cascading_delete",
}
_PATH_SIGNAL_PATTERNS: dict[DataDeletionSignal, re.Pattern[str]] = {
    "hard_delete": re.compile(r"(?:hard[-_]?delete|permanent[-_]?delete|delete[-_]?forever|destroy)", re.I),
    "purge": re.compile(r"(?:purge|purger|purging|cleanup[-_]?purge)", re.I),
    "erasure": re.compile(r"(?:erasure|erase|right[-_]?to[-_]?erasure|forgotten)", re.I),
    "account_deletion": re.compile(r"(?:account[-_]?delet|delete[-_]?account|user[-_]?delet)", re.I),
    "gdpr_deletion": re.compile(r"(?:gdpr|dsar|privacy[-_]?delete)", re.I),
    "tombstone": re.compile(r"(?:tombstone|graveyard)", re.I),
    "cascading_delete": re.compile(r"(?:cascade|cascading[-_]?delete|delete[-_]?children)", re.I),
    "retention_exception": re.compile(r"(?:retention[-_]?exception|retention[-_]?override|legal[-_]?hold)", re.I),
    "backup_deletion": re.compile(r"(?:backup|restore|snapshot)", re.I),
    "search_index_removal": re.compile(r"(?:search[-_]?index|opensearch|elasticsearch|solr)", re.I),
    "analytics_removal": re.compile(r"(?:analytics|warehouse|events?|metrics|segments?)", re.I),
    "audit_evidence": re.compile(r"(?:audit|evidence|deletion[-_]?receipt)", re.I),
}
_TEXT_SIGNAL_PATTERNS: dict[DataDeletionSignal, re.Pattern[str]] = {
    "hard_delete": re.compile(
        r"\b(?:hard[- ]?delet(?:e|es|ed|ing|ion)?|permanent(?:ly)? delet(?:e|es|ed|ing|ion)?|"
        r"delete forever|destroy(?:ed|ing)? records?|physical delete)\b",
        re.I,
    ),
    "purge": re.compile(r"\b(?:purge|purges|purged|purging|purge job|data purge)\b", re.I),
    "erasure": re.compile(
        r"\b(?:erase|erases|erased|erasing|erasure|right to erasure|right-to-erasure|"
        r"right to be forgotten|forget me)\b",
        re.I,
    ),
    "account_deletion": re.compile(
        r"\b(?:account delet(?:e|es|ed|ing|ion)?|delete account|close account|user delet(?:e|es|ed|ing|ion)?)\b",
        re.I,
    ),
    "gdpr_deletion": re.compile(
        r"\b(?:gdpr delet(?:e|es|ed|ing|ion)?|gdpr erasure|dsar deletion|data subject deletion|privacy deletion)\b",
        re.I,
    ),
    "tombstone": re.compile(r"\b(?:tombstone|tombstoned|tombstones|deletion marker|graveyard record)\b", re.I),
    "cascading_delete": re.compile(
        r"\b(?:cascad(?:e|es|ed|ing) delet(?:e|es|ed|ing|ion)?|cascade inventory|delete child records?|"
        r"remove dependent records?|orphan cleanup)\b",
        re.I,
    ),
    "retention_exception": re.compile(
        r"\b(?:retention exception|retention override|retention hold|legal hold|litigation hold|do not delete)\b",
        re.I,
    ),
    "backup_deletion": re.compile(
        r"\b(?:backups?|snapshots?|restore implications?|backup restore|backup purge|remove from backups?)\b",
        re.I,
    ),
    "search_index_removal": re.compile(
        r"\b(?:search[- ]?index removal|remove from search|delete from search|opensearch|elasticsearch|solr|"
        r"index tombstone|search index)\b",
        re.I,
    ),
    "analytics_removal": re.compile(
        r"\b(?:analytics removal|remove from analytics|delete analytics events?|warehouse deletion|"
        r"delete from warehouse|metrics removal|segment deletion|(?:remove|removes|delete|deletes).{0,80}\banalytics)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|deletion receipt|erasure certificate|audit log|audit trail|auditable deletion)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[DataDeletionSafeguard, re.Pattern[str]] = {
    "dry_run_counts": re.compile(
        r"\b(?:dry[- ]?run|preview mode|count(?:s)? before delete|affected row counts?|deletion counts?|"
        r"would delete|no-op mode)\b",
        re.I,
    ),
    "cascade_inventory": re.compile(
        r"\b(?:cascade inventory|dependency inventory|relationship inventory|deletion graph|referential impact|"
        r"(?:dependent|child) records?.{0,40}inventor(?:y|ied)|inventor(?:y|ied).{0,40}(?:dependent|child) records?)\b",
        re.I,
    ),
    "backup_restore_implications": re.compile(
        r"\b(?:backup/restore implications|backup restore implications|restore implications|backup retention|"
        r"backup purge|snapshots?.{0,50}(?:delete|purge|restore)|restore.{0,50}deleted data)\b",
        re.I,
    ),
    "legal_hold_check": re.compile(
        r"\b(?:legal hold check|legal hold|litigation hold|retention hold|do not delete|compliance hold|hold check)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|auditable|deletion event|erasure event|actor recorded|audit evidence|"
        r"deletion receipt|evidence record)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotent|idempotency|retry safe|retry-safe|safe retry|already deleted|repeatable deletion)\b",
        re.I,
    ),
    "downstream_deletion_propagation": re.compile(
        r"\b(?:downstream deletion propagation|downstream propagation|propagate deletion|propagate erasure|"
        r"delete from downstream|webhook deletion|"
        r"partner deletion|processor deletion)\b",
        re.I,
    ),
    "customer_confirmation": re.compile(
        r"\b(?:customer confirmation|user confirmation|confirm deletion|confirmation step|explicit confirmation|"
        r"re-authentication|reauthentication|two-step confirmation)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[DataDeletionSafeguard, str] = {
    "dry_run_counts": "Add a dry run or preview that reports exact records and counts before irreversible deletion.",
    "cascade_inventory": "Inventory cascading deletes, dependent records, tombstones, and orphan cleanup before execution.",
    "backup_restore_implications": "Document backup, snapshot, and restore behavior for deleted data and evidence retention.",
    "legal_hold_check": "Check legal holds, retention exceptions, and compliance blocks before deleting records.",
    "audit_trail": "Record auditable deletion evidence including actor, request, target scope, timing, and outcome.",
    "idempotency": "Make deletion execution idempotent and retry-safe for already-deleted or partially propagated records.",
    "downstream_deletion_propagation": "Propagate deletions to downstream stores such as search indexes, analytics, warehouses, and processors.",
    "customer_confirmation": "Require explicit customer confirmation for account deletion or right-to-erasure requests.",
}


@dataclass(frozen=True, slots=True)
class TaskDataDeletionReadinessRecord:
    """Readiness guidance for one task touching irreversible data deletion behavior."""

    task_id: str
    title: str
    matched_deletion_signals: tuple[DataDeletionSignal, ...]
    required_safeguards: tuple[DataDeletionSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DataDeletionSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DataDeletionSafeguard, ...] = field(default_factory=tuple)
    risk_level: DataDeletionRisk = "medium"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_deletion_signals": list(self.matched_deletion_signals),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataDeletionReadinessPlan:
    """Plan-level data deletion readiness review."""

    plan_id: str | None = None
    records: tuple[TaskDataDeletionReadinessRecord, ...] = field(default_factory=tuple)
    deletion_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskDataDeletionReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "deletion_task_ids": list(self.deletion_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render data deletion readiness as deterministic Markdown."""
        title = "# Task Data Deletion Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Deletion task count: {self.summary.get('deletion_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task data deletion readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.matched_deletion_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_data_deletion_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDataDeletionReadinessPlan:
    """Build deletion and right-to-erasure readiness records for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDataDeletionReadinessPlan(
        plan_id=plan_id,
        records=records,
        deletion_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_data_deletion_readiness(source: Any) -> TaskDataDeletionReadinessPlan:
    """Compatibility alias for building data deletion readiness records."""
    return build_task_data_deletion_readiness_plan(source)


def summarize_task_data_deletion_readiness(source: Any) -> TaskDataDeletionReadinessPlan:
    """Compatibility alias for building data deletion readiness records."""
    return build_task_data_deletion_readiness_plan(source)


def extract_task_data_deletion_readiness(source: Any) -> TaskDataDeletionReadinessPlan:
    """Compatibility alias for building data deletion readiness records."""
    return build_task_data_deletion_readiness_plan(source)


def generate_task_data_deletion_readiness(source: Any) -> TaskDataDeletionReadinessPlan:
    """Compatibility alias for generating data deletion readiness records."""
    return build_task_data_deletion_readiness_plan(source)


def recommend_task_data_deletion_readiness(source: Any) -> TaskDataDeletionReadinessPlan:
    """Compatibility alias for recommending data deletion safeguards."""
    return build_task_data_deletion_readiness_plan(source)


def task_data_deletion_readiness_plan_to_dict(
    result: TaskDataDeletionReadinessPlan,
) -> dict[str, Any]:
    """Serialize a data deletion readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_deletion_readiness_plan_to_dict.__test__ = False


def task_data_deletion_readiness_plan_to_dicts(
    result: TaskDataDeletionReadinessPlan | Iterable[TaskDataDeletionReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize data deletion readiness records to plain dictionaries."""
    if isinstance(result, TaskDataDeletionReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_deletion_readiness_plan_to_dicts.__test__ = False


def task_data_deletion_readiness_plan_to_markdown(
    result: TaskDataDeletionReadinessPlan,
) -> str:
    """Render a data deletion readiness plan as Markdown."""
    return result.to_markdown()


task_data_deletion_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DataDeletionSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DataDeletionSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDataDeletionReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    required = _required_safeguards(signals.signals)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDataDeletionReadinessRecord(
        task_id=task_id,
        title=title,
        matched_deletion_signals=signals.signals,
        required_safeguards=required,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DataDeletionSignal] = set()
    safeguard_hits: set[DataDeletionSafeguard] = set()
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
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

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

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[DataDeletionSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals = {
        signal
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items()
        if pattern.search(normalized) or pattern.search(text)
    }
    name = PurePosixPath(normalized).name
    if name in {"purge.py", "erasure.py", "hard_delete.py", "account_deletion.py", "tombstone.py"}:
        signals.add(
            "hard_delete" if name == "hard_delete.py" else name.removesuffix(".py")  # type: ignore[arg-type]
        )
    return signals


def _required_safeguards(
    signals: tuple[DataDeletionSignal, ...],
) -> tuple[DataDeletionSafeguard, ...]:
    signal_set = set(signals)
    required: set[DataDeletionSafeguard] = {"dry_run_counts", "legal_hold_check", "audit_trail", "idempotency"}
    if signal_set & {"cascading_delete", "account_deletion", "tombstone"}:
        required.add("cascade_inventory")
    if signal_set & {"backup_deletion", "purge", "hard_delete", "erasure", "gdpr_deletion"}:
        required.add("backup_restore_implications")
    if signal_set & {"search_index_removal", "analytics_removal", "gdpr_deletion", "erasure", "account_deletion"}:
        required.add("downstream_deletion_propagation")
    if signal_set & {"account_deletion", "erasure", "gdpr_deletion"}:
        required.add("customer_confirmation")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    signals: tuple[DataDeletionSignal, ...],
    missing: tuple[DataDeletionSafeguard, ...],
) -> DataDeletionRisk:
    if not missing:
        return "low"
    missing_set = set(missing)
    irreversible = bool(set(signals) & _IRREVERSIBLE_SIGNALS)
    if irreversible and ({"dry_run_counts", "legal_hold_check"} & missing_set):
        return "high"
    if len(missing) >= 4 or "downstream_deletion_propagation" in missing_set:
        return "high"
    if irreversible or len(missing) >= 2:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskDataDeletionReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "deletion_task_count": len(records),
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_deletion_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "deletion_task_ids": [record.task_id for record in records],
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
        if task := _task_payload(item):
            tasks.append(task)
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
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


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
        "validation_plan",
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
        "risks",
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
        "validation_plan",
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
    "DataDeletionRisk",
    "DataDeletionSafeguard",
    "DataDeletionSignal",
    "TaskDataDeletionReadinessPlan",
    "TaskDataDeletionReadinessRecord",
    "analyze_task_data_deletion_readiness",
    "build_task_data_deletion_readiness_plan",
    "extract_task_data_deletion_readiness",
    "generate_task_data_deletion_readiness",
    "recommend_task_data_deletion_readiness",
    "summarize_task_data_deletion_readiness",
    "task_data_deletion_readiness_plan_to_dict",
    "task_data_deletion_readiness_plan_to_dicts",
    "task_data_deletion_readiness_plan_to_markdown",
]
