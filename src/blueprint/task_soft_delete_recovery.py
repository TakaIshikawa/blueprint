"""Plan soft-delete recovery readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SoftDeleteSignal = Literal[
    "soft_delete",
    "archive",
    "restore",
    "trash",
    "undelete",
    "tombstone",
    "retention_window",
    "permanent_delete",
    "filtering",
]
SoftDeleteSafeguard = Literal[
    "restore_path_validation",
    "uniqueness_with_deleted_records",
    "restore_permanent_delete_authorization",
    "retention_expiry_behavior",
    "audit_trail_coverage",
    "search_list_filtering_semantics",
]
SoftDeleteReadinessLevel = Literal["not_ready", "needs_review", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[SoftDeleteSignal, ...] = (
    "soft_delete",
    "archive",
    "restore",
    "trash",
    "undelete",
    "tombstone",
    "retention_window",
    "permanent_delete",
    "filtering",
)
_SAFEGUARD_ORDER: tuple[SoftDeleteSafeguard, ...] = (
    "restore_path_validation",
    "uniqueness_with_deleted_records",
    "restore_permanent_delete_authorization",
    "retention_expiry_behavior",
    "audit_trail_coverage",
    "search_list_filtering_semantics",
)
_READINESS_ORDER: dict[SoftDeleteReadinessLevel, int] = {"not_ready": 0, "needs_review": 1, "ready": 2}
_PATH_SIGNAL_PATTERNS: dict[SoftDeleteSignal, re.Pattern[str]] = {
    "soft_delete": re.compile(r"(?:soft[-_]?delet|deleted[-_]?at|delete[-_]?flag|is[-_]?deleted)", re.I),
    "archive": re.compile(r"(?:archive|archival|archived)", re.I),
    "restore": re.compile(r"(?:restore|recover|recovery)", re.I),
    "trash": re.compile(r"(?:trash|recycle[-_]?bin)", re.I),
    "undelete": re.compile(r"(?:undelete|un[-_]?delete)", re.I),
    "tombstone": re.compile(r"(?:tombstone|graveyard)", re.I),
    "retention_window": re.compile(r"(?:retention|expiry|expiration|ttl|cleanup)", re.I),
    "permanent_delete": re.compile(r"(?:permanent[-_]?delete|hard[-_]?delete|purge|destroy)", re.I),
    "filtering": re.compile(
        r"(?:(?:deleted|trash|archive|soft[-_]?delete).{0,30}(?:filters?|search|lists?|queries|scope)|"
        r"(?:filters?|search|lists?|queries|scope).{0,30}(?:deleted|trash|archive|soft[-_]?delete))",
        re.I,
    ),
}
_TEXT_SIGNAL_PATTERNS: dict[SoftDeleteSignal, re.Pattern[str]] = {
    "soft_delete": re.compile(
        r"\b(?:soft[- ]?delet(?:e|es|ed|ing|ion)?|deleted_at|deleted at|is_deleted|delete flag|"
        r"mark(?:ed)? as deleted|logical delete)\b",
        re.I,
    ),
    "archive": re.compile(r"\b(?:archive|archives|archived|archival)\b", re.I),
    "restore": re.compile(r"\b(?:restore|restores|restored|restoring|recovery path|recover deleted)\b", re.I),
    "trash": re.compile(r"\b(?:trash|trashed|recycle bin|bin view)\b", re.I),
    "undelete": re.compile(r"\b(?:undelete|undeletes|undeleted|undeleting|un-delete|un deleted)\b", re.I),
    "tombstone": re.compile(r"\b(?:tombstone|tombstoned|tombstones|graveyard record)\b", re.I),
    "retention_window": re.compile(
        r"\b(?:retention window|retention period|retention expiry|retention expiration|expires? after|"
        r"expiry|expiration|ttl|time to live|cleanup after|delete after \d+)\b",
        re.I,
    ),
    "permanent_delete": re.compile(
        r"\b(?:permanent(?:ly)? delet(?:e|es|ed|ing|ion)?|hard[- ]?delet(?:e|es|ed|ing|ion)?|"
        r"purge|purges|purged|purging|destroy(?:ed|ing)? records?)\b",
        re.I,
    ),
    "filtering": re.compile(
        r"\b(?:exclude deleted|include deleted|with deleted|without deleted|show deleted|hide deleted|"
        r"search results?|list views?|query filters?|default scope|deleted records? filter)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[SoftDeleteSafeguard, re.Pattern[str]] = {
    "restore_path_validation": re.compile(
        r"\b(?:restore path|restore validation|validate restore|recover(?:y)? validation|"
        r"undelete validation|restore deleted records?)\b",
        re.I,
    ),
    "uniqueness_with_deleted_records": re.compile(
        r"\b(?:unique(?:ness)? constraint|unique index|partial unique index|duplicate key|slug conflict|"
        r"email conflict|deleted records?.{0,50}unique|unique.{0,50}deleted records?)\b",
        re.I,
    ),
    "restore_permanent_delete_authorization": re.compile(
        r"\b(?:(?:restore|undelete|permanent(?:ly)? delete|hard[- ]?delete|purge).{0,70}"
        r"(?:authorization|permission|rbac|role|policy|admin|owner)|"
        r"(?:authorization|permission|rbac|role|policy|admin|owner).{0,70}"
        r"(?:restore|undelete|permanent(?:ly)? delete|hard[- ]?delete|purge))\b",
        re.I,
    ),
    "retention_expiry_behavior": re.compile(
        r"\b(?:retention expiry|retention expiration|ttl expiry|"
        r"expiry behavior|expiration behavior|expired deleted records?|cleanup job)\b",
        re.I,
    ),
    "audit_trail_coverage": re.compile(
        r"\b(?:audit trail|audit log|auditable|who deleted|who restored|deletion event|restore event|"
        r"permanent delete event|actor recorded|audit coverage)\b",
        re.I,
    ),
    "search_list_filtering_semantics": re.compile(
        r"\b(?:search/list filtering|search filtering|list filtering|filtering semantics|exclude deleted|"
        r"include deleted|with deleted|without deleted|default scope|list views?.{0,50}deleted|"
        r"search results?.{0,50}deleted)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[SoftDeleteSafeguard, str] = {
    "restore_path_validation": "Validate restore and undelete paths, including conflict, missing parent, and already-restored cases.",
    "uniqueness_with_deleted_records": "Verify uniqueness constraints and indexes behave correctly when deleted records still exist.",
    "restore_permanent_delete_authorization": "Verify authorization for restore, undelete, hard delete, and purge operations.",
    "retention_expiry_behavior": "Test retention expiry behavior, cleanup timing, and what happens after the recovery window closes.",
    "audit_trail_coverage": "Confirm delete, restore, retention expiry, and permanent-delete events are covered by audit trails.",
    "search_list_filtering_semantics": "Verify search, list, count, and default query filtering semantics for deleted records.",
}


@dataclass(frozen=True, slots=True)
class TaskSoftDeleteRecoveryRecord:
    """Readiness guidance for one task touching soft-delete recovery behavior."""

    task_id: str
    title: str
    matched_deletion_signals: tuple[SoftDeleteSignal, ...]
    present_safeguards: tuple[SoftDeleteSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SoftDeleteSafeguard, ...] = field(default_factory=tuple)
    readiness_level: SoftDeleteReadinessLevel = "needs_review"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_deletion_signals": list(self.matched_deletion_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSoftDeleteRecoveryPlan:
    """Plan-level soft-delete recovery readiness review."""

    plan_id: str | None = None
    records: tuple[TaskSoftDeleteRecoveryRecord, ...] = field(default_factory=tuple)
    soft_delete_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskSoftDeleteRecoveryRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "soft_delete_task_ids": list(self.soft_delete_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render soft-delete recovery readiness as deterministic Markdown."""
        title = "# Task Soft Delete Recovery Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Soft-delete task count: {self.summary.get('soft_delete_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No soft-delete recovery readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.matched_deletion_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_soft_delete_recovery_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSoftDeleteRecoveryPlan:
    """Build soft-delete recovery readiness records for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_READINESS_ORDER[record.readiness_level], record.task_id, record.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskSoftDeleteRecoveryPlan(
        plan_id=plan_id,
        records=records,
        soft_delete_task_ids=tuple(record.task_id for record in records),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_soft_delete_recovery(source: Any) -> TaskSoftDeleteRecoveryPlan:
    """Compatibility alias for building soft-delete recovery readiness plans."""
    return build_task_soft_delete_recovery_plan(source)


def summarize_task_soft_delete_recovery(source: Any) -> TaskSoftDeleteRecoveryPlan:
    """Compatibility alias for building soft-delete recovery readiness plans."""
    return build_task_soft_delete_recovery_plan(source)


def extract_task_soft_delete_recovery(source: Any) -> TaskSoftDeleteRecoveryPlan:
    """Compatibility alias for building soft-delete recovery readiness plans."""
    return build_task_soft_delete_recovery_plan(source)


def generate_task_soft_delete_recovery(source: Any) -> TaskSoftDeleteRecoveryPlan:
    """Compatibility alias for generating soft-delete recovery readiness plans."""
    return build_task_soft_delete_recovery_plan(source)


def recommend_task_soft_delete_recovery(source: Any) -> TaskSoftDeleteRecoveryPlan:
    """Compatibility alias for recommending soft-delete recovery safeguards."""
    return build_task_soft_delete_recovery_plan(source)


def task_soft_delete_recovery_plan_to_dict(result: TaskSoftDeleteRecoveryPlan) -> dict[str, Any]:
    """Serialize a soft-delete recovery readiness plan to a plain dictionary."""
    return result.to_dict()


task_soft_delete_recovery_plan_to_dict.__test__ = False


def task_soft_delete_recovery_plan_to_dicts(
    result: TaskSoftDeleteRecoveryPlan | Iterable[TaskSoftDeleteRecoveryRecord],
) -> list[dict[str, Any]]:
    """Serialize soft-delete recovery readiness records to plain dictionaries."""
    if isinstance(result, TaskSoftDeleteRecoveryPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_soft_delete_recovery_plan_to_dicts.__test__ = False


def task_soft_delete_recovery_plan_to_markdown(result: TaskSoftDeleteRecoveryPlan) -> str:
    """Render a soft-delete recovery readiness plan as Markdown."""
    return result.to_markdown()


task_soft_delete_recovery_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[SoftDeleteSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SoftDeleteSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskSoftDeleteRecoveryRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskSoftDeleteRecoveryRecord(
        task_id=task_id,
        title=title,
        matched_deletion_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[SoftDeleteSignal] = set()
    safeguard_hits: set[SoftDeleteSafeguard] = set()
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


def _path_signals(path: str) -> set[SoftDeleteSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals = {
        signal
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items()
        if pattern.search(normalized) or pattern.search(text)
    }
    name = PurePosixPath(normalized).name
    if name in {"trash.py", "archive.py", "restore.py", "soft_delete.py", "tombstone.py"}:
        signals.add("soft_delete" if name == "soft_delete.py" else name.removesuffix(".py"))  # type: ignore[arg-type]
    if {"restore", "undelete"} & signals:
        signals.add("soft_delete")
    return signals


def _readiness_level(
    signals: tuple[SoftDeleteSignal, ...],
    missing: tuple[SoftDeleteSafeguard, ...],
) -> SoftDeleteReadinessLevel:
    if not missing:
        return "ready"
    missing_set = set(missing)
    restore_sensitive = bool({"restore", "undelete", "trash"} & set(signals))
    destructive = "permanent_delete" in signals
    if restore_sensitive and "restore_path_validation" in missing_set:
        return "not_ready"
    if destructive and "restore_permanent_delete_authorization" in missing_set:
        return "not_ready"
    if len(missing) >= 4:
        return "not_ready"
    return "needs_review"


def _summary(
    records: tuple[TaskSoftDeleteRecoveryRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "soft_delete_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
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
        "soft_delete_task_ids": [record.task_id for record in records],
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
    "SoftDeleteReadinessLevel",
    "SoftDeleteSafeguard",
    "SoftDeleteSignal",
    "TaskSoftDeleteRecoveryPlan",
    "TaskSoftDeleteRecoveryRecord",
    "analyze_task_soft_delete_recovery",
    "build_task_soft_delete_recovery_plan",
    "extract_task_soft_delete_recovery",
    "generate_task_soft_delete_recovery",
    "recommend_task_soft_delete_recovery",
    "summarize_task_soft_delete_recovery",
    "task_soft_delete_recovery_plan_to_dict",
    "task_soft_delete_recovery_plan_to_dicts",
    "task_soft_delete_recovery_plan_to_markdown",
]
