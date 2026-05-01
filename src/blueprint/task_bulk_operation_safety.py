"""Plan bulk-operation safeguards for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


BulkOperationType = Literal[
    "bulk_edit",
    "batch_import",
    "mass_notification",
    "backfill",
    "migration",
    "wide_fanout_write",
]
BulkOperationSafeguard = Literal[
    "dry_run",
    "batching",
    "sampling",
    "rate_limiting",
    "rollback",
    "operator_approval",
    "progress_monitoring",
]
BulkOperationRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[BulkOperationRisk, int] = {"high": 0, "medium": 1, "low": 2}
_OPERATION_ORDER: dict[BulkOperationType, int] = {
    "bulk_edit": 0,
    "batch_import": 1,
    "mass_notification": 2,
    "backfill": 3,
    "migration": 4,
    "wide_fanout_write": 5,
}
_SAFEGUARD_ORDER: tuple[BulkOperationSafeguard, ...] = (
    "dry_run",
    "batching",
    "sampling",
    "rate_limiting",
    "rollback",
    "operator_approval",
    "progress_monitoring",
)
_TEXT_OPERATION_PATTERNS: tuple[tuple[BulkOperationType, re.Pattern[str]], ...] = (
    (
        "bulk_edit",
        re.compile(
            r"\b(?:bulk edit|bulk update|bulk delete|bulk change|mass edit|mass update|"
            r"bulk operation|bulk action|update all|delete all|rewrite existing)\b",
            re.I,
        ),
    ),
    (
        "batch_import",
        re.compile(
            r"\b(?:batch import|bulk import|csv import|import batch|import existing|"
            r"bulk upload|data import|loader job|ingest csv)\b",
            re.I,
        ),
    ),
    (
        "mass_notification",
        re.compile(
            r"\b(?:mass notification|bulk email|bulk sms|mass email|mass message|"
            r"notify all|send to all|broadcast|push notification campaign|email campaign)\b",
            re.I,
        ),
    ),
    ("backfill", re.compile(r"\b(?:backfill|rehydrate|recompute existing|data repair|data migration)\b", re.I)),
    (
        "migration",
        re.compile(r"\b(?:migration|migrate existing|schema migration|database migration|move all records)\b", re.I),
    ),
    (
        "wide_fanout_write",
        re.compile(
            r"\b(?:fan[- ]?out|wide write|write to many|many records|all tenants|"
            r"all users|all accounts|production records|millions of records|large dataset)\b",
            re.I,
        ),
    ),
)
_PATH_OPERATION_PATTERNS: tuple[tuple[BulkOperationType, re.Pattern[str]], ...] = (
    ("bulk_edit", re.compile(r"(?:bulk|mass)[_-]?(?:edit|update|delete|action|operation)|bulk", re.I)),
    ("batch_import", re.compile(r"(?:importers?|imports?|loaders?|csv|etl|ingest)", re.I)),
    ("mass_notification", re.compile(r"(?:notifications?|email|sms|push|campaigns?|broadcast)", re.I)),
    ("backfill", re.compile(r"(?:backfills?|rehydrat|recompute|data[-_]?migrations?)", re.I)),
    ("migration", re.compile(r"(?:migrations?|alembic|db/versions|schema|ddl)", re.I)),
    ("wide_fanout_write", re.compile(r"(?:fanout|fan[-_]?out|workers?|batch[-_]?jobs?|jobs?)", re.I)),
)
_SAFEGUARD_PATTERNS: dict[BulkOperationSafeguard, re.Pattern[str]] = {
    "dry_run": re.compile(r"\b(?:dry[- ]?run|preview|simulation|rehearsal|no[- ]op|shadow mode)\b", re.I),
    "batching": re.compile(r"\b(?:batch(?:ed|ing)?|chunk(?:ed|ing)?|page(?:d| through)|windowed|resume)\b", re.I),
    "sampling": re.compile(r"\b(?:sample|sampling|canary|pilot|subset|limited rollout|small cohort)\b", re.I),
    "rate_limiting": re.compile(r"\b(?:rate limit(?:ing|s|ed)?|throttl|qps|rps|backoff|pace|concurrency limit)\b", re.I),
    "rollback": re.compile(r"\b(?:rollback|roll back|revert|restore|undo|compensating action|recovery path)\b", re.I),
    "operator_approval": re.compile(
        r"\b(?:operator approval|manual approval|human approval|sign[- ]?off|two[- ]person|confirmation gate)\b",
        re.I,
    ),
    "progress_monitoring": re.compile(
        r"\b(?:progress|monitor(?:ing)?|metrics?|dashboard|audit log|checkpoint|status report|alert)\b",
        re.I,
    ),
}
_HIGH_RISK_RE = re.compile(
    r"\b(?:production|all users|all tenants|all accounts|delete|destructive|irreversible|"
    r"millions?|large dataset|customer[- ]facing|external notification|email|sms|push|"
    r"payment|billing|ledger|permissions?)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskBulkOperationSafetyRecord:
    """Bulk-operation safety guidance for one execution task."""

    task_id: str
    title: str
    risk_level: BulkOperationRisk
    operation_type: BulkOperationType
    safeguards: tuple[BulkOperationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[BulkOperationSafeguard, ...] = field(default_factory=tuple)
    recommended_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "operation_type": self.operation_type,
            "safeguards": list(self.safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_acceptance_criteria": list(self.recommended_acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBulkOperationSafetyPlan:
    """Task-level safety plan for bulk operations and wide writes."""

    plan_id: str | None = None
    records: tuple[TaskBulkOperationSafetyRecord, ...] = field(default_factory=tuple)
    bulk_operation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "bulk_operation_task_ids": list(self.bulk_operation_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return bulk-operation records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the bulk-operation safety plan as deterministic Markdown."""
        title = "# Task Bulk Operation Safety Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        operation_counts = self.summary.get("operation_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Bulk operation task count: {self.summary.get('bulk_operation_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Operation counts: "
            + ", ".join(
                f"{operation} {operation_counts.get(operation, 0)}"
                for operation in _OPERATION_ORDER
            ),
        ]
        if not self.records:
            lines.extend(["", "No bulk-operation tasks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Operation | Safeguards | Missing Safeguards | Recommended Acceptance Criteria | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{record.operation_type} | "
                f"{_markdown_cell('; '.join(record.safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_bulk_operation_safety_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBulkOperationSafetyPlan:
    """Build bulk-operation safety guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index)) is not None
    ]
    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    operation_counts = {
        operation: sum(1 for record in result if record.operation_type == operation)
        for operation in _OPERATION_ORDER
    }
    return TaskBulkOperationSafetyPlan(
        plan_id=plan_id,
        records=result,
        bulk_operation_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "bulk_operation_task_count": len(result),
            "missing_safeguard_count": sum(len(record.missing_safeguards) for record in result),
            "risk_counts": risk_counts,
            "operation_counts": operation_counts,
        },
    )


def summarize_task_bulk_operation_safety(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBulkOperationSafetyPlan:
    """Compatibility alias for building bulk-operation safety plans."""
    return build_task_bulk_operation_safety_plan(source)


def task_bulk_operation_safety_plan_to_dict(
    result: TaskBulkOperationSafetyPlan,
) -> dict[str, Any]:
    """Serialize a bulk-operation safety plan to a plain dictionary."""
    return result.to_dict()


task_bulk_operation_safety_plan_to_dict.__test__ = False


def task_bulk_operation_safety_plan_to_markdown(
    result: TaskBulkOperationSafetyPlan,
) -> str:
    """Render a bulk-operation safety plan as Markdown."""
    return result.to_markdown()


task_bulk_operation_safety_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    operations: tuple[BulkOperationType, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    high_risk_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(task: Mapping[str, Any], index: int) -> TaskBulkOperationSafetyRecord | None:
    signals = _signals(task)
    if not signals.operations:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    operation_type = _primary_operation(signals.operations)
    safeguards = _required_safeguards(signals.operations)
    acceptance_context = _acceptance_context(task)
    missing = tuple(safeguard for safeguard in safeguards if not _SAFEGUARD_PATTERNS[safeguard].search(acceptance_context))
    return TaskBulkOperationSafetyRecord(
        task_id=task_id,
        title=title,
        risk_level=_risk_level(signals, operation_type, missing),
        operation_type=operation_type,
        safeguards=safeguards,
        missing_safeguards=missing,
        recommended_acceptance_criteria=_recommended_acceptance_criteria(missing, operation_type),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    operations: set[BulkOperationType] = set()
    evidence: list[str] = []
    high_risk_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for operation, pattern in _PATH_OPERATION_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                operations.add(operation)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
        if _HIGH_RISK_RE.search(path_text):
            high_risk_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched = False
        for operation, pattern in _TEXT_OPERATION_PATTERNS:
            if pattern.search(text):
                operations.add(operation)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
        if _HIGH_RISK_RE.search(text):
            high_risk_evidence.append(_evidence_snippet(source_field, text))

    ordered_operations = tuple(operation for operation in _OPERATION_ORDER if operation in operations)
    return _Signals(
        operations=ordered_operations,
        evidence=tuple(_dedupe(evidence)),
        high_risk_evidence=tuple(_dedupe(high_risk_evidence)),
    )


def _primary_operation(operations: tuple[BulkOperationType, ...]) -> BulkOperationType:
    return min(operations, key=lambda operation: _OPERATION_ORDER[operation])


def _required_safeguards(
    operations: tuple[BulkOperationType, ...],
) -> tuple[BulkOperationSafeguard, ...]:
    if not operations:
        return ()
    required: set[BulkOperationSafeguard] = {"dry_run", "batching", "rollback", "progress_monitoring"}
    if "mass_notification" in operations or "wide_fanout_write" in operations:
        required.update({"sampling", "rate_limiting", "operator_approval"})
    if "batch_import" in operations or "backfill" in operations or "migration" in operations:
        required.update({"sampling", "rate_limiting"})
    if "bulk_edit" in operations:
        required.add("operator_approval")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    signals: _Signals,
    operation_type: BulkOperationType,
    missing: tuple[BulkOperationSafeguard, ...],
) -> BulkOperationRisk:
    missing_set = set(missing)
    if signals.high_risk_evidence and missing:
        return "high"
    if operation_type in ("mass_notification", "wide_fanout_write") and (
        "rate_limiting" in missing_set or "operator_approval" in missing_set
    ):
        return "high"
    if operation_type in ("bulk_edit", "backfill", "migration") and (
        "dry_run" in missing_set or "rollback" in missing_set
    ):
        return "high"
    if len(missing) >= 4:
        return "high"
    if missing:
        return "medium"
    return "low"


def _recommended_acceptance_criteria(
    missing: tuple[BulkOperationSafeguard, ...],
    operation_type: BulkOperationType,
) -> tuple[str, ...]:
    templates = {
        "dry_run": f"Dry-run mode previews the {operation_type.replace('_', ' ')} impact without mutating production data.",
        "batching": "Execution runs in bounded, resumable batches with explicit batch-size controls.",
        "sampling": "A sampled or canary run validates representative records before full rollout.",
        "rate_limiting": "Rate limits or throttling protect downstream services and write paths during execution.",
        "rollback": "Rollback or compensating steps are documented and tested for partial completion.",
        "operator_approval": "Operator approval is required before full-scale execution begins.",
        "progress_monitoring": "Progress, failures, and completion metrics are monitored with audit evidence.",
    }
    return tuple(templates[safeguard] for safeguard in missing)


def _acceptance_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in ("acceptance_criteria", "criteria", "risks", "risk"):
        values.extend(_strings(task.get(field_name)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for source_field, text in _metadata_texts(metadata):
            normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
            if any(
                keyword in normalized
                for keyword in (
                    "acceptance",
                    "criteria",
                    "safeguard",
                    "rollout",
                    "rollback",
                    "approval",
                    "monitor",
                    "rate",
                    "batch",
                    "dry",
                )
            ):
                values.append(text)
    return " ".join(values)


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
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    for field_name in (
        "acceptance_criteria",
        "criteria",
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
    return texts


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
                if any(pattern.search(key_text) for _, pattern in _TEXT_OPERATION_PATTERNS):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for _, pattern in _TEXT_OPERATION_PATTERNS):
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
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "BulkOperationRisk",
    "BulkOperationSafeguard",
    "BulkOperationType",
    "TaskBulkOperationSafetyPlan",
    "TaskBulkOperationSafetyRecord",
    "build_task_bulk_operation_safety_plan",
    "summarize_task_bulk_operation_safety",
    "task_bulk_operation_safety_plan_to_dict",
    "task_bulk_operation_safety_plan_to_markdown",
]
