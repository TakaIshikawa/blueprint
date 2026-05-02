"""Plan audit log coverage for audit-sensitive execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AuditEventCategory = Literal[
    "user_visible",
    "admin",
    "auth",
    "billing",
    "data_export",
    "destructive",
    "permission_change",
]
AuditCoverageRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[AuditCoverageRisk, int] = {"high": 0, "medium": 1, "low": 2}
_EVENT_ORDER: tuple[AuditEventCategory, ...] = (
    "user_visible",
    "admin",
    "auth",
    "billing",
    "data_export",
    "destructive",
    "permission_change",
)
_TEXT_EVENT_PATTERNS: dict[AuditEventCategory, re.Pattern[str]] = {
    "user_visible": re.compile(
        r"\b(?:user[- ]visible|customer[- ]visible|customer account data|user account data|"
        r"profile change|settings change|account update|notification preference|email change|"
        r"user action|self[- ]service)\b",
        re.I,
    ),
    "admin": re.compile(
        r"\b(?:admin|administrator|operator|support console|backoffice|back office|staff tool|"
        r"moderation|impersonat(?:e|ion)|tenant settings)\b",
        re.I,
    ),
    "auth": re.compile(
        r"\b(?:auth|authentication|authorization|login|logout|sso|mfa|2fa|password reset|"
        r"session|token|oauth|oidc|saml|api key)\b",
        re.I,
    ),
    "billing": re.compile(
        r"\b(?:billing|payment|invoice|subscription|plan change|refund|charge|credit card|"
        r"pricing|checkout|ledger|entitlement)\b",
        re.I,
    ),
    "data_export": re.compile(
        r"\b(?:data export|export data|csv export|download report|bulk download|extract data|"
        r"report export|portable data|gdpr export|data extract)\b",
        re.I,
    ),
    "destructive": re.compile(
        r"\b(?:delete|deleted|deleting|remove|removed|purge|wipe|erase|destroy|truncate|"
        r"revoke|disable|deactivate|archive|bulk update|backfill|drop table)\b",
        re.I,
    ),
    "permission_change": re.compile(
        r"\b(?:permission|permissions|role|roles|rbac|access grant|grant access|revoke access|"
        r"policy change|scope change|privilege|entitlement|group mapping|acl)\b",
        re.I,
    ),
}
_PATH_EVENT_PATTERNS: dict[AuditEventCategory, re.Pattern[str]] = {
    "user_visible": re.compile(r"(?:profile|settings|account|preferences|user[-_]?actions?)", re.I),
    "admin": re.compile(r"(?:admin|operator|support|backoffice|moderation|impersonat)", re.I),
    "auth": re.compile(r"(?:auth|login|sessions?|tokens?|oauth|oidc|saml|sso|mfa|api[-_]?keys?)", re.I),
    "billing": re.compile(r"(?:billing|payments?|invoices?|subscriptions?|checkout|ledger)", re.I),
    "data_export": re.compile(r"(?:exports?|downloads?|reports?|csv|extracts?)", re.I),
    "destructive": re.compile(r"(?:delete|remove|destroy|purge|truncate|revoke|disable|archive|backfill)", re.I),
    "permission_change": re.compile(r"(?:permissions?|roles?|rbac|polic(?:y|ies)|scopes?|acl|entitlements?)", re.I),
}
_EXPLICIT_AUDIT_RE = re.compile(
    r"\b(?:audit logs?|audit trail|audit event|security event|activity log|event history|"
    r"who did what|change log|compliance evidence)\b",
    re.I,
)
_LOW_NOISE_RE = re.compile(
    r"\b(?:copy|wording|typo|docs?|readme|style|css|storybook|mock data|fixture|read[- ]only|"
    r"dashboard|analytics view|report only|label|tooltip|formatting)\b",
    re.I,
)
_EVIDENCE_REQUIREMENTS: dict[AuditEventCategory, str] = {
    "user_visible": "Capture actor, target user, visible action, before/after state, request id, timestamp, and source IP when available.",
    "admin": "Capture admin actor, target tenant or account, privileged action, reason, request id, timestamp, and support context.",
    "auth": "Capture actor, auth method, credential or session event type, client, IP, user agent, success or failure, and timestamp.",
    "billing": "Capture actor, account, subscription or payment object, monetary state change, processor reference, request id, and timestamp.",
    "data_export": "Capture actor, export scope, filters, destination or download id, record count when available, request id, and timestamp.",
    "destructive": "Capture actor, destructive action, target identifiers, before/after state or tombstone, approval reference, request id, and timestamp.",
    "permission_change": "Capture actor, target principal, role or permission delta, scope, approver when required, request id, and timestamp.",
}
_VALIDATION_HINTS: dict[AuditEventCategory, str] = {
    "user_visible": "Assert user-visible mutations emit one audit event with stable actor, target, action, and correlation fields.",
    "admin": "Test privileged admin paths, support-tool paths, and denied attempts for audit event emission.",
    "auth": "Cover success and failure cases for login, token, session, SSO, MFA, and API-key audit events.",
    "billing": "Reconcile billing audit events with subscription, invoice, payment, refund, and entitlement state changes.",
    "data_export": "Verify exports record scope, filters, requester, and completion or failure status without logging sensitive payloads.",
    "destructive": "Exercise dry-run, approved, denied, rollback, and partial-failure paths for destructive audit events.",
    "permission_change": "Test grant, revoke, role mapping, and denied permission changes with before/after role evidence.",
}
_ROLLBACK_CONSIDERATIONS: dict[AuditEventCategory, str] = {
    "user_visible": "Rollback must preserve the original audit event and emit a compensating event for reverted visible changes.",
    "admin": "Admin rollback should retain privileged actor evidence and link reversal events to the original action.",
    "auth": "Auth rollback should avoid deleting security events and should log session or credential invalidation follow-up actions.",
    "billing": "Billing rollback should link refunds, reversals, or entitlement repairs to the original billing event.",
    "data_export": "Export rollback should retain request evidence and record cancellation, expiry, or revoked download access.",
    "destructive": "Destructive rollback should log restore attempts, restored targets, skipped targets, and unrecoverable items.",
    "permission_change": "Permission rollback should emit a compensating grant or revoke event with the previous scope restored.",
}


@dataclass(frozen=True, slots=True)
class TaskAuditLogCoverageRecord:
    """Audit log coverage guidance for one audit-sensitive execution task."""

    task_id: str
    title: str
    audit_events: tuple[AuditEventCategory, ...]
    coverage_risk: AuditCoverageRisk
    evidence_requirements: tuple[str, ...] = field(default_factory=tuple)
    validation_hints: tuple[str, ...] = field(default_factory=tuple)
    rollback_considerations: tuple[str, ...] = field(default_factory=tuple)
    existing_audit_coverage: bool = False
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "audit_events": list(self.audit_events),
            "coverage_risk": self.coverage_risk,
            "evidence_requirements": list(self.evidence_requirements),
            "validation_hints": list(self.validation_hints),
            "rollback_considerations": list(self.rollback_considerations),
            "existing_audit_coverage": self.existing_audit_coverage,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAuditLogCoveragePlan:
    """Plan-level audit log coverage recommendations."""

    plan_id: str | None = None
    records: tuple[TaskAuditLogCoverageRecord, ...] = field(default_factory=tuple)
    audit_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskAuditLogCoverageRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "audit_task_ids": list(self.audit_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return audit coverage records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render audit log coverage recommendations as deterministic Markdown."""
        title = "# Task Audit Log Coverage Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Audit-sensitive task count: {self.summary.get('audit_task_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No audit log coverage requirements were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Audit Events | Existing Audit Coverage | Evidence Requirements | Validation Hints | Rollback Considerations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.coverage_risk} | "
                f"{_markdown_cell(', '.join(record.audit_events))} | "
                f"{str(record.existing_audit_coverage).lower()} | "
                f"{_markdown_cell('; '.join(record.evidence_requirements))} | "
                f"{_markdown_cell('; '.join(record.validation_hints))} | "
                f"{_markdown_cell('; '.join(record.rollback_considerations))} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_audit_log_coverage_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogCoveragePlan:
    """Build audit log coverage recommendations for audit-sensitive execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.coverage_risk], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index) for index, task in enumerate(tasks, start=1) if candidates[index - 1] is None
    )
    return TaskAuditLogCoveragePlan(
        plan_id=plan_id,
        records=records,
        audit_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def derive_task_audit_log_coverage_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAuditLogCoveragePlan:
    """Compatibility alias for building audit log coverage recommendations."""
    return build_task_audit_log_coverage_plan(source)


def task_audit_log_coverage_to_dict(result: TaskAuditLogCoveragePlan) -> dict[str, Any]:
    """Serialize an audit log coverage plan to a plain dictionary."""
    return result.to_dict()


task_audit_log_coverage_to_dict.__test__ = False


def task_audit_log_coverage_to_markdown(result: TaskAuditLogCoveragePlan) -> str:
    """Render an audit log coverage plan as Markdown."""
    return result.to_markdown()


task_audit_log_coverage_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    audit_events: tuple[AuditEventCategory, ...] = field(default_factory=tuple)
    existing_audit_coverage: bool = False
    event_evidence: tuple[str, ...] = field(default_factory=tuple)
    audit_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(task: Mapping[str, Any], index: int) -> TaskAuditLogCoverageRecord | None:
    signals = _signals(task)
    if not signals.audit_events:
        return None
    if _low_noise_task(task) and not {"destructive", "permission_change", "billing"} & set(signals.audit_events):
        return None
    task_id = _task_id(task, index)
    return TaskAuditLogCoverageRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        audit_events=signals.audit_events,
        coverage_risk=_coverage_risk(signals.audit_events, signals.existing_audit_coverage),
        evidence_requirements=tuple(_EVIDENCE_REQUIREMENTS[event] for event in signals.audit_events),
        validation_hints=tuple(_VALIDATION_HINTS[event] for event in signals.audit_events),
        rollback_considerations=tuple(_ROLLBACK_CONSIDERATIONS[event] for event in signals.audit_events),
        existing_audit_coverage=signals.existing_audit_coverage,
        evidence=tuple(_dedupe([*signals.event_evidence, *signals.audit_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    event_hits: set[AuditEventCategory] = set()
    event_evidence: list[str] = []
    audit_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_hits = [event for event, pattern in _PATH_EVENT_PATTERNS.items() if pattern.search(normalized)]
        if path_hits:
            event_hits.update(path_hits)
            event_evidence.append(f"files_or_modules: {path}")
        if _EXPLICIT_AUDIT_RE.search(normalized.replace("_", " ").replace("-", " ")):
            audit_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched_event = False
        for event, pattern in _TEXT_EVENT_PATTERNS.items():
            if pattern.search(text):
                event_hits.add(event)
                matched_event = True
        if matched_event:
            event_evidence.append(_evidence_snippet(source_field, text))
        if _EXPLICIT_AUDIT_RE.search(text):
            audit_evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        audit_events=tuple(event for event in _EVENT_ORDER if event in event_hits),
        existing_audit_coverage=bool(audit_evidence),
        event_evidence=tuple(_dedupe(event_evidence)),
        audit_evidence=tuple(_dedupe(audit_evidence)),
    )


def _coverage_risk(
    audit_events: tuple[AuditEventCategory, ...],
    existing_audit_coverage: bool,
) -> AuditCoverageRisk:
    high_events = {"billing", "destructive", "permission_change"}
    if high_events & set(audit_events) and not existing_audit_coverage:
        return "high"
    if len(audit_events) >= 3 and not existing_audit_coverage:
        return "high"
    if existing_audit_coverage:
        return "low" if len(audit_events) <= 2 else "medium"
    return "medium"


def _summary(
    records: tuple[TaskAuditLogCoverageRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "audit_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "risk_counts": {risk: sum(1 for record in records if record.coverage_risk == risk) for risk in _RISK_ORDER},
        "event_counts": {
            event: sum(1 for record in records if event in record.audit_events) for event in _EVENT_ORDER
        },
        "existing_audit_coverage_count": sum(1 for record in records if record.existing_audit_coverage),
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
    return _EXPLICIT_AUDIT_RE.search(value) is not None or any(
        pattern.search(value) for pattern in _TEXT_EVENT_PATTERNS.values()
    )


def _low_noise_task(task: Mapping[str, Any]) -> bool:
    combined = " ".join(text for _, text in _candidate_texts(task))
    return bool(_LOW_NOISE_RE.search(combined))


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
        for key, child in sorted(value.items(), key=lambda item: str(item[0])):
            if key_text := _optional_text(key):
                strings.append(key_text.replace("_", " ").replace("-", " "))
            strings.extend(_strings(child))
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
    "AuditCoverageRisk",
    "AuditEventCategory",
    "TaskAuditLogCoveragePlan",
    "TaskAuditLogCoverageRecord",
    "build_task_audit_log_coverage_plan",
    "derive_task_audit_log_coverage_plan",
    "task_audit_log_coverage_to_dict",
    "task_audit_log_coverage_to_markdown",
]
