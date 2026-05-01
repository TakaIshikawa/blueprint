"""Derive incident response readiness records for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


IncidentSeverity = Literal["low", "medium", "high", "critical"]
_T = TypeVar("_T")

_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_LOW_RISK_VALUES = {"", "docs", "documentation", "info", "informational", "low", "trivial"}
_DOC_SUFFIXES = (".adoc", ".md", ".mdx", ".rst", ".txt")
_DOC_PATH_PARTS = {"docs", "documentation", "guides", "readme"}
_SIGNAL_ORDER = (
    "customer-impact",
    "data-risk",
    "external-service",
    "migration",
    "rollback",
    "degraded-service",
    "alerting",
    "paging",
    "retry",
    "queue",
    "production",
)
_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "customer-impact": re.compile(
        r"\b(?:customer|user|client|tenant|public|availability|outage|sla)\b",
        re.IGNORECASE,
    ),
    "data-risk": re.compile(
        r"\b(?:data loss|corruption|backup|restore|delete|destructive|pii|"
        r"database|dataset|backfill)\b",
        re.IGNORECASE,
    ),
    "external-service": re.compile(
        r"\b(?:external|third[- ]party|integration|webhook|partner|vendor|"
        r"api gateway|oauth|stripe|slack|pagerduty|opsgenie)\b",
        re.IGNORECASE,
    ),
    "migration": re.compile(
        r"\b(?:migration|migrations|migrate|schema|ddl|alembic|liquibase|flyway)\b",
        re.IGNORECASE,
    ),
    "rollback": re.compile(r"\b(?:rollback|roll back|revert|recovery|recover)\b", re.IGNORECASE),
    "degraded-service": re.compile(
        r"\b(?:degraded|latency|timeout|partial outage|brownout|unavailable|"
        r"service down|error rate)\b",
        re.IGNORECASE,
    ),
    "alerting": re.compile(r"\b(?:alert|alerts|alerting|monitor|monitoring|alarm)\b", re.IGNORECASE),
    "paging": re.compile(r"\b(?:page|paging|pager|on-call|oncall)\b", re.IGNORECASE),
    "retry": re.compile(r"\b(?:retry|retries|idempotent|backoff|rerun)\b", re.IGNORECASE),
    "queue": re.compile(r"\b(?:queue|queues|worker|job|consumer|dead letter|dlq)\b", re.IGNORECASE),
    "production": re.compile(r"\b(?:production|prod|live|release|deploy|rollout)\b", re.IGNORECASE),
}


@dataclass(frozen=True, slots=True)
class TaskIncidentResponseRecord:
    """Incident response guidance for one execution-plan task."""

    task_id: str
    severity: IncidentSeverity
    detected_signals: tuple[str, ...] = field(default_factory=tuple)
    responder_checklist: tuple[str, ...] = field(default_factory=tuple)
    escalation_notes: tuple[str, ...] = field(default_factory=tuple)
    validation_evidence_requirements: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "severity": self.severity,
            "detected_signals": list(self.detected_signals),
            "responder_checklist": list(self.responder_checklist),
            "escalation_notes": list(self.escalation_notes),
            "validation_evidence_requirements": list(self.validation_evidence_requirements),
        }


@dataclass(frozen=True, slots=True)
class TaskIncidentResponsePlan:
    """Incident response readiness records for execution-plan tasks."""

    plan_id: str | None = None
    records: tuple[TaskIncidentResponseRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
        }

    def to_markdown(self) -> str:
        """Render incident response readiness as deterministic Markdown."""
        title = "# Task Incident Response Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No incident response records were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Task Records",
                "",
                "| Task | Severity | Signals | Checklist | Escalation | Validation Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.responder_checklist) or 'none')} | "
                f"{_markdown_cell('; '.join(record.escalation_notes) or 'none')} | "
                f"{_markdown_cell('; '.join(record.validation_evidence_requirements) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_incident_response_plan(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskIncidentResponsePlan:
    """Build incident response readiness records for execution-plan tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(_record_for_task(record) for record in _task_records(tasks))
    return TaskIncidentResponsePlan(plan_id=plan_id, records=records)


def derive_task_incident_response_plan(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskIncidentResponsePlan:
    """Compatibility alias for building task incident response readiness."""
    return build_task_incident_response_plan(source)


def generate_task_incident_response_records(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> list[TaskIncidentResponseRecord]:
    """Return incident response records as a task-level list."""
    return list(build_task_incident_response_plan(source).records)


def task_incident_response_plan_to_dict(plan: TaskIncidentResponsePlan) -> dict[str, Any]:
    """Serialize an incident response plan to a plain dictionary."""
    return plan.to_dict()


task_incident_response_plan_to_dict.__test__ = False


def task_incident_response_plan_to_markdown(plan: TaskIncidentResponsePlan) -> str:
    """Render an incident response plan as Markdown."""
    return plan.to_markdown()


task_incident_response_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    risk_level: str
    files_or_modules: tuple[str, ...]
    validation_commands: tuple[str, ...]
    context: str


def _record_for_task(record: _TaskRecord) -> TaskIncidentResponseRecord:
    signals = _signals_for_record(record)
    severity = _severity_for_record(record, signals)
    return TaskIncidentResponseRecord(
        task_id=record.task_id,
        severity=severity,
        detected_signals=signals,
        responder_checklist=tuple(_responder_checklist(severity, signals)),
        escalation_notes=tuple(_escalation_notes(record, severity, signals)),
        validation_evidence_requirements=tuple(_validation_evidence(record, signals)),
    )


def _signals_for_record(record: _TaskRecord) -> tuple[str, ...]:
    if _is_docs_only(record):
        return ()
    signals = [signal for signal in _SIGNAL_ORDER if _SIGNAL_PATTERNS[signal].search(record.context)]
    if record.risk_level in _HIGH_RISK_VALUES and "production" not in signals:
        signals.append("production")
    return tuple(_dedupe(signals))


def _severity_for_record(record: _TaskRecord, signals: tuple[str, ...]) -> IncidentSeverity:
    if record.risk_level in {"blocker", "critical"}:
        return "critical"
    if "data-risk" in signals and (
        "migration" in signals or record.risk_level in _HIGH_RISK_VALUES
    ):
        return "critical"
    if "customer-impact" in signals and (
        "degraded-service" in signals or "paging" in signals or record.risk_level in _HIGH_RISK_VALUES
    ):
        return "critical"
    if record.risk_level in _HIGH_RISK_VALUES:
        return "high"
    if any(signal in signals for signal in ("customer-impact", "data-risk", "migration")):
        return "high"
    if any(
        signal in signals
        for signal in ("external-service", "rollback", "degraded-service", "paging", "queue")
    ):
        return "medium"
    if any(signal in signals for signal in ("alerting", "retry", "production")):
        return "medium"
    return "low"


def _responder_checklist(severity: IncidentSeverity, signals: tuple[str, ...]) -> list[str]:
    if severity == "low" and not signals:
        return [
            "Confirm task scope is documentation-only or low risk.",
            "Attach completion evidence or reviewer sign-off.",
        ]

    checklist = [
        "Name the incident commander, primary responder, and backup before execution.",
        "Capture baseline health and active alerts for affected production surfaces.",
    ]
    if "customer-impact" in signals:
        checklist.append("Prepare customer-impact status text and support handoff notes.")
    if "data-risk" in signals or "migration" in signals:
        checklist.append("Verify backup, restore, or migration recovery path before starting.")
    if "external-service" in signals:
        checklist.append("Confirm external service owner, status page, and contract rollback contact.")
    if "degraded-service" in signals or "queue" in signals or "retry" in signals:
        checklist.append("Track saturation, retry, queue depth, latency, and error-rate indicators.")
    if "alerting" in signals or "paging" in signals:
        checklist.append("Confirm paging route, alert thresholds, and acknowledgement owner.")
    if "rollback" in signals:
        checklist.append("Set the rollback trigger, decision owner, and recovery time expectation.")
    checklist.append("Record validation evidence and post-change incident timeline notes.")
    return _dedupe(checklist)


def _escalation_notes(
    record: _TaskRecord,
    severity: IncidentSeverity,
    signals: tuple[str, ...],
) -> list[str]:
    if severity == "low" and not signals:
        return ["Escalate only if validation fails or scope expands beyond documentation."]

    notes = [f"Escalate as {severity} severity if production health regresses during {record.task_id}."]
    if "customer-impact" in signals:
        notes.append("Notify support and product owners when customer-visible behavior changes.")
    if "data-risk" in signals or "migration" in signals:
        notes.append("Escalate before rerunning destructive data, migration, or recovery steps.")
    if "external-service" in signals:
        notes.append("Escalate to the external integration owner if dependency errors persist.")
    if "degraded-service" in signals or "queue" in signals:
        notes.append("Escalate when latency, queue depth, or error rates remain elevated after rollback.")
    if "paging" in signals:
        notes.append("Page the owning service team when alert acknowledgement is missed.")
    return _dedupe(notes)


def _validation_evidence(record: _TaskRecord, signals: tuple[str, ...]) -> list[str]:
    evidence = [f"Command output: {command}" for command in record.validation_commands]
    if "customer-impact" in signals or "degraded-service" in signals:
        evidence.append("Before and after service health, latency, and error-rate snapshot.")
    if "data-risk" in signals or "migration" in signals:
        evidence.append("Backup, restore, migration dry-run, or row-count reconciliation evidence.")
    if "external-service" in signals:
        evidence.append("External integration smoke test or provider status evidence.")
    if "alerting" in signals or "paging" in signals:
        evidence.append("Alert route, dashboard, and paging acknowledgement evidence.")
    if "retry" in signals or "queue" in signals:
        evidence.append("Retry budget, queue depth, and dead-letter queue inspection evidence.")
    if "rollback" in signals:
        evidence.append("Rollback trigger verification and recovery validation evidence.")
    if not evidence:
        evidence.append("Reviewer sign-off or diff summary showing no production behavior change.")
    return _dedupe(evidence)


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
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


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        files = tuple(_dedupe(_strings(task.get("files_or_modules") or task.get("files"))))
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk_level"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk"))
                    or "unspecified"
                ).casefold(),
                files_or_modules=files,
                validation_commands=tuple(_task_validation_commands(task)),
                context=_task_context(task),
            )
        )
    return tuple(records)


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "risk",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "risk_level",
        "risk",
        "test_command",
        "suggested_test_command",
        "validation_command",
    ):
        if text := _optional_text(task.get(field_name)):
            values.append(text)
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(_strings(task.get("acceptance_criteria")))
    values.extend(_metadata_texts(task.get("metadata")))
    return " ".join(values)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.append(str(key))
            texts.extend(_metadata_texts(value[key]))
        return texts
    return _strings(value)


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


def _is_docs_only(record: _TaskRecord) -> bool:
    if record.risk_level not in _LOW_RISK_VALUES:
        return False
    if not record.files_or_modules:
        return False
    return all(_is_doc_path(path) for path in record.files_or_modules)


def _is_doc_path(path: str) -> bool:
    normalized = path.replace("\\", "/").casefold()
    parts = {part for part in normalized.split("/") if part}
    return (
        bool(parts & _DOC_PATH_PARTS)
        or normalized.endswith(_DOC_SUFFIXES)
        or normalized in {"readme", "readme.md"}
    )


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "IncidentSeverity",
    "TaskIncidentResponsePlan",
    "TaskIncidentResponseRecord",
    "build_task_incident_response_plan",
    "derive_task_incident_response_plan",
    "generate_task_incident_response_records",
    "task_incident_response_plan_to_dict",
    "task_incident_response_plan_to_markdown",
]
