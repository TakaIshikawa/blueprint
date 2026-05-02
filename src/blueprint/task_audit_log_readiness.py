"""Plan audit-log readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AuditLogSignal = Literal[
    "audit_log",
    "activity_history",
    "admin_event",
    "security_trail",
    "compliance_logging",
]
AuditLogSafeguard = Literal[
    "immutable_event_capture",
    "actor_resource_action_fields",
    "tamper_resistance",
    "retention_policy",
    "query_export_validation",
]
AuditLogReadiness = Literal["strong", "partial", "missing"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[AuditLogReadiness, int] = {"missing": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: dict[AuditLogSignal, int] = {
    "audit_log": 0,
    "activity_history": 1,
    "admin_event": 2,
    "security_trail": 3,
    "compliance_logging": 4,
}
_SAFEGUARD_ORDER: dict[AuditLogSafeguard, int] = {
    "immutable_event_capture": 0,
    "actor_resource_action_fields": 1,
    "tamper_resistance": 2,
    "retention_policy": 3,
    "query_export_validation": 4,
}
_SIGNAL_PATTERNS: dict[AuditLogSignal, re.Pattern[str]] = {
    "audit_log": re.compile(
        r"\b(?:audit[- ]logs?|audit logging|audit trail|audit events?|auditable events?|event audit)\b",
        re.I,
    ),
    "activity_history": re.compile(
        r"\b(?:activity histor(?:y|ies)|activity logs?|change histor(?:y|ies)|history feed|user activity|action history)\b",
        re.I,
    ),
    "admin_event": re.compile(
        r"\b(?:admin events?|administrator events?|admin actions?|administrator actions?|admin activity|staff actions?)\b",
        re.I,
    ),
    "security_trail": re.compile(
        r"\b(?:security trails?|security logs?|security events?|investigation trails?|forensic trails?|access trails?)\b",
        re.I,
    ),
    "compliance_logging": re.compile(
        r"\b(?:compliance logging|compliance logs?|compliance events?|sox logs?|hipaa logs?|gdpr logs?|regulatory logs?)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[AuditLogSignal, re.Pattern[str]] = {
    "audit_log": re.compile(r"audit[_-]?(?:log|logs|logging|trail|event|events)|(?:^|/)audits?(?:/|$)", re.I),
    "activity_history": re.compile(r"activity[_-]?(?:log|logs|history)|change[_-]?history|history[_-]?feed", re.I),
    "admin_event": re.compile(r"admin[_-]?(?:event|events|action|actions|activity)|staff[_-]?actions?", re.I),
    "security_trail": re.compile(r"security[_-]?(?:trail|trails|log|logs|event|events)|forensic|investigation", re.I),
    "compliance_logging": re.compile(r"compliance[_-]?(?:log|logs|logging|event|events)|sox|hipaa|gdpr|regulatory", re.I),
}
_SAFEGUARD_PATTERNS: dict[AuditLogSafeguard, re.Pattern[str]] = {
    "immutable_event_capture": re.compile(
        r"\b(?:immutable event capture|append[- ]only|append only|event capture|capture every event|"
        r"cannot be edited|non[- ]editable|write once|immutable logs?)\b",
        re.I,
    ),
    "actor_resource_action_fields": re.compile(
        r"\b(?:(?:actor|user|principal)[, /]+(?:resource|entity|object)[, /]+(?:action|operation)|"
        r"(?:actor|user|principal).{0,80}(?:resource|entity|object).{0,80}(?:action|operation)|"
        r"(?:who|what|when)|ip address|request id|correlation id)\b",
        re.I,
    ),
    "tamper_resistance": re.compile(
        r"\b(?:tamper[- ]resistan(?:t|ce)|tamper[- ]evident|hash chain|signed logs?|signature|"
        r"integrity check|integrity validation|wORM|write once read many)\b",
        re.I,
    ),
    "retention_policy": re.compile(
        r"\b(?:retention policy|retention period|retention window|data retention|expire logs?|"
        r"archive logs?|purge logs?|legal hold|ttl)\b",
        re.I,
    ),
    "query_export_validation": re.compile(
        r"\b(?:query validation|export validation|validate exports?|export permissions?|filtered exports?|"
        r"search permissions?|query permissions?|redacted exports?|csv export|download audit logs?)\b",
        re.I,
    ),
}
_CHECKS: dict[AuditLogSafeguard, str] = {
    "immutable_event_capture": "Verify audit events are captured append-only for all relevant create, update, delete, auth, and admin actions.",
    "actor_resource_action_fields": "Require actor, resource, action, timestamp, outcome, IP or request context, and correlation identifiers.",
    "tamper_resistance": "Validate tamper resistance with restricted writes, integrity checks, signatures, or immutable storage.",
    "retention_policy": "Define retention, archival, purge, legal-hold, and privacy expectations for audit records.",
    "query_export_validation": "Test audit-log query, filtering, access control, redaction, pagination, and export behavior.",
}


@dataclass(frozen=True, slots=True)
class TaskAuditLogReadinessRecord:
    """Audit-log readiness guidance for one execution task."""

    task_id: str
    title: str
    matched_audit_signals: tuple[AuditLogSignal, ...]
    readiness: AuditLogReadiness
    missing_safeguards: tuple[AuditLogSafeguard, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_audit_signals": list(self.matched_audit_signals),
            "readiness": self.readiness,
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAuditLogReadinessPlan:
    """Plan-level audit-log readiness review."""

    plan_id: str | None = None
    records: tuple[TaskAuditLogReadinessRecord, ...] = field(default_factory=tuple)
    audit_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "audit_task_ids": list(self.audit_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return audit-log readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def recommendations(self) -> tuple[TaskAuditLogReadinessRecord, ...]:
        """Compatibility view for callers that use recommendation terminology."""
        return self.records

    def to_markdown(self) -> str:
        """Render the audit-log readiness plan as deterministic Markdown."""
        title = "# Task Audit Log Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('audit_task_count', 0)} audit-log tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(strong: {counts.get('strong', 0)}, partial: {counts.get('partial', 0)}, "
                f"missing: {counts.get('missing', 0)})."
            ),
        ]
        if not self.records:
            lines.extend(["", "No audit-log readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Signals | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.matched_audit_signals))} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_audit_log_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogReadinessPlan:
    """Build audit-log readiness records for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    audit_task_ids = tuple(record.task_id for record in records)
    audit_task_id_set = set(audit_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in audit_task_id_set
    )
    return TaskAuditLogReadinessPlan(
        plan_id=plan_id,
        records=records,
        audit_task_ids=audit_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, total_task_count=len(tasks), not_applicable_task_count=len(not_applicable_task_ids)),
    )


def generate_task_audit_log_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskAuditLogReadinessRecord, ...]:
    """Return audit-log readiness records for relevant execution tasks."""
    return build_task_audit_log_readiness_plan(source).records


def recommend_task_audit_log_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskAuditLogReadinessRecord, ...]:
    """Compatibility alias for returning audit-log readiness records."""
    return generate_task_audit_log_readiness(source)


def summarize_task_audit_log_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogReadinessPlan:
    """Compatibility alias for building audit-log readiness plans."""
    return build_task_audit_log_readiness_plan(source)


def analyze_task_audit_log_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogReadinessPlan:
    """Compatibility alias for building audit-log readiness plans."""
    return build_task_audit_log_readiness_plan(source)


def extract_task_audit_log_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogReadinessPlan:
    """Compatibility alias for building audit-log readiness plans."""
    return build_task_audit_log_readiness_plan(source)


def task_audit_log_readiness_plan_to_dict(result: TaskAuditLogReadinessPlan) -> dict[str, Any]:
    """Serialize an audit-log readiness plan to a plain dictionary."""
    return result.to_dict()


task_audit_log_readiness_plan_to_dict.__test__ = False


def task_audit_log_readiness_to_dicts(
    records: (
        tuple[TaskAuditLogReadinessRecord, ...]
        | list[TaskAuditLogReadinessRecord]
        | TaskAuditLogReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize audit-log readiness records to dictionaries."""
    if isinstance(records, TaskAuditLogReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_audit_log_readiness_to_dicts.__test__ = False


def task_audit_log_readiness_plan_to_markdown(result: TaskAuditLogReadinessPlan) -> str:
    """Render an audit-log readiness plan as Markdown."""
    return result.to_markdown()


task_audit_log_readiness_plan_to_markdown.__test__ = False


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskAuditLogReadinessRecord | None:
    signals: dict[AuditLogSignal, list[str]] = {}
    safeguards: set[AuditLogSafeguard] = set()
    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        _inspect_path(path, signals, safeguards)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, signals, safeguards)

    if not signals:
        return None

    matched_signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards)
    task_id = _task_id(task, index)
    return TaskAuditLogReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_audit_signals=matched_signals,
        readiness=_readiness(safeguards, missing_safeguards),
        missing_safeguards=missing_safeguards,
        recommended_checks=tuple(_CHECKS[safeguard] for safeguard in missing_safeguards),
        evidence=tuple(
            _dedupe(
                evidence
                for signal in matched_signals
                for evidence in signals.get(signal, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    signals: dict[AuditLogSignal, list[str]],
    safeguards: set[AuditLogSafeguard],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for signal, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            signals.setdefault(signal, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            safeguards.add(safeguard)


def _inspect_text(
    source_field: str,
    text: str,
    signals: dict[AuditLogSignal, list[str]],
    safeguards: set[AuditLogSafeguard],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            signals.setdefault(signal, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(text):
            safeguards.add(safeguard)


def _readiness(
    safeguards: set[AuditLogSafeguard],
    missing_safeguards: tuple[AuditLogSafeguard, ...],
) -> AuditLogReadiness:
    if not missing_safeguards:
        return "strong"
    if safeguards:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskAuditLogReadinessRecord, ...],
    *,
    total_task_count: int,
    not_applicable_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "audit_task_count": len(records),
        "not_applicable_task_count": not_applicable_task_count,
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in ("strong", "partial", "missing")
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_audit_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
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
        "expected_file_paths",
        "expected_files",
        "acceptance_criteria",
        "validation_plan",
        "validation_plans",
        "definition_of_done",
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
        "test_strategy",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "validation_plans",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
        "risks",
        "dependencies",
        "depends_on",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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
    "AuditLogReadiness",
    "AuditLogSafeguard",
    "AuditLogSignal",
    "TaskAuditLogReadinessPlan",
    "TaskAuditLogReadinessRecord",
    "analyze_task_audit_log_readiness",
    "build_task_audit_log_readiness_plan",
    "extract_task_audit_log_readiness",
    "generate_task_audit_log_readiness",
    "recommend_task_audit_log_readiness",
    "summarize_task_audit_log_readiness",
    "task_audit_log_readiness_plan_to_dict",
    "task_audit_log_readiness_plan_to_markdown",
    "task_audit_log_readiness_to_dicts",
]
