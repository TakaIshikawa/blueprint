"""Build incident communication matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IncidentCommunicationAudience = Literal[
    "customers",
    "admins",
    "support",
    "customer_success",
    "operations",
    "engineering",
    "data_governance",
    "security",
    "vendor_partner",
    "executives",
]
IncidentCommunicationRisk = Literal[
    "reliability",
    "data_integrity",
    "migration",
    "external_integration",
    "customer_facing",
]
IncidentCommunicationPriority = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_PRIORITY_ORDER: dict[IncidentCommunicationPriority, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_AUDIENCE_ORDER: tuple[IncidentCommunicationAudience, ...] = (
    "customers",
    "admins",
    "support",
    "customer_success",
    "operations",
    "engineering",
    "data_governance",
    "security",
    "vendor_partner",
    "executives",
)
_RISK_ORDER: tuple[IncidentCommunicationRisk, ...] = (
    "reliability",
    "data_integrity",
    "migration",
    "external_integration",
    "customer_facing",
)

_RELIABILITY_RE = re.compile(
    r"\b(?:availability|downtime|outage|degradation|degraded|latency|timeout|"
    r"error rate|slo|sla|queue|worker|background job|retry|dead letter|read[- ]only|"
    r"service interruption|incident|rollback|kill switch|production risk)\b",
    re.I,
)
_DATA_RE = re.compile(
    r"\b(?:data integrity|data loss|corruption|delete|deletion|backfill|bulk update|"
    r"reconcile|reconciliation|customer data|pii|personal data|schema|database|"
    r"records?|export|import|dual write)\b",
    re.I,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrate|migrating|cutover|schema migration|database migration|"
    r"legacy data|existing customers?|existing accounts?|backfill)\b",
    re.I,
)
_INTEGRATION_RE = re.compile(
    r"\b(?:external|third[- ]party|vendor|partner|integration|webhook|oauth|"
    r"api provider|dependency|stripe|salesforce|slack|twilio|sendgrid|provider)\b",
    re.I,
)
_CUSTOMER_RE = re.compile(
    r"\b(?:customer[- ]facing|customer[- ]visible|user[- ]facing|user[- ]visible|"
    r"customers?|end users?|admins?|billing|payment|checkout|notification|email|"
    r"dashboard|ui|release notes?|status page)\b",
    re.I,
)
_SUPPRESS_RE = re.compile(
    r"\b(?:docs?|documentation|readme|copy|typo|formatting|comment-only|style-only|"
    r"test fixture|mock data)\b",
    re.I,
)
_PATH_PATTERNS: tuple[tuple[IncidentCommunicationRisk, re.Pattern[str]], ...] = (
    ("reliability", re.compile(r"(?:^|/)(?:ops|alerts?|workers?|queues?|slo|runbooks?)(?:/|$)", re.I)),
    ("data_integrity", re.compile(r"(?:^|/)(?:data|models?|db|database|schemas?)(?:/|$)|\.sql$", re.I)),
    ("migration", re.compile(r"(?:^|/)(?:migrations?|backfills?)(?:/|$)|\.sql$", re.I)),
    ("external_integration", re.compile(r"(?:^|/)(?:integrations?|webhooks?|clients?|providers?|oauth)(?:/|$)", re.I)),
    ("customer_facing", re.compile(r"(?:^|/)(?:app|web|ui|frontend|pages?|routes?|billing|payments?)(?:/|$)", re.I)),
)
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_hints",
    "assignee",
    "assignees",
    "dri",
    "oncall",
    "on_call",
    "team",
)


@dataclass(frozen=True, slots=True)
class PlanIncidentCommunicationRow:
    """Audience-specific incident communication guidance for a risky task."""

    task_id: str
    title: str
    audience: IncidentCommunicationAudience
    risk_categories: tuple[IncidentCommunicationRisk, ...] = field(default_factory=tuple)
    trigger_conditions: tuple[str, ...] = field(default_factory=tuple)
    draft_message_topics: tuple[str, ...] = field(default_factory=tuple)
    owner_suggestions: tuple[str, ...] = field(default_factory=tuple)
    escalation_timing: str = ""
    priority: IncidentCommunicationPriority = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "audience": self.audience,
            "risk_categories": list(self.risk_categories),
            "trigger_conditions": list(self.trigger_conditions),
            "draft_message_topics": list(self.draft_message_topics),
            "owner_suggestions": list(self.owner_suggestions),
            "escalation_timing": self.escalation_timing,
            "priority": self.priority,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanIncidentCommunicationMatrix:
    """Plan-level incident communication matrix."""

    plan_id: str | None = None
    rows: tuple[PlanIncidentCommunicationRow, ...] = field(default_factory=tuple)
    incident_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanIncidentCommunicationRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "incident_task_ids": list(self.incident_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return incident communication rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the incident communication matrix as deterministic Markdown."""
        title = "# Plan Incident Communication Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        priority_counts = self.summary.get("priority_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Incident task count: {self.summary.get('incident_task_count', 0)}",
            f"- Communication row count: {self.summary.get('communication_row_count', 0)}",
            "- Priority counts: "
            + ", ".join(
                f"{priority} {priority_counts.get(priority, 0)}" for priority in _PRIORITY_ORDER
            ),
        ]
        if not self.rows:
            lines.extend(["", "No incident communication rows were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Audience | Priority | Risks | Triggers | Topics | Owners | Timing | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.audience} | "
                f"{row.priority} | "
                f"{_markdown_cell('; '.join(row.risk_categories) or 'none')} | "
                f"{_markdown_cell('; '.join(row.trigger_conditions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.draft_message_topics) or 'none')} | "
                f"{_markdown_cell('; '.join(row.owner_suggestions) or 'none')} | "
                f"{_markdown_cell(row.escalation_timing or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def generate_plan_incident_communication_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanIncidentCommunicationMatrix:
    """Derive audience-specific incident communication rows from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    rows: list[PlanIncidentCommunicationRow] = []

    for index, task in enumerate(tasks, start=1):
        rows.extend(_task_rows(task, index))

    rows.sort(
        key=lambda row: (
            _PRIORITY_ORDER[row.priority],
            row.task_id,
            _AUDIENCE_ORDER.index(row.audience),
        )
    )
    result = tuple(rows)
    incident_task_ids = tuple(_dedupe(row.task_id for row in result))
    return PlanIncidentCommunicationMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=result,
        incident_task_ids=incident_task_ids,
        summary={
            "task_count": len(tasks),
            "incident_task_count": len(incident_task_ids),
            "communication_row_count": len(result),
            "priority_counts": {
                priority: sum(1 for row in result if row.priority == priority)
                for priority in _PRIORITY_ORDER
            },
            "audience_counts": {
                audience: sum(1 for row in result if row.audience == audience)
                for audience in _AUDIENCE_ORDER
            },
            "risk_counts": {
                risk: sum(1 for row in result if risk in row.risk_categories)
                for risk in _RISK_ORDER
            },
        },
    )


def build_plan_incident_communication_matrix(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> PlanIncidentCommunicationMatrix:
    """Compatibility alias for generating incident communication matrices."""
    return generate_plan_incident_communication_matrix(source)


def derive_plan_incident_communication_matrix(
    source: Mapping[str, Any] | ExecutionPlan | PlanIncidentCommunicationMatrix | object,
) -> PlanIncidentCommunicationMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanIncidentCommunicationMatrix):
        return source
    return generate_plan_incident_communication_matrix(source)


def summarize_plan_incident_communication_matrix(
    source: Mapping[str, Any] | ExecutionPlan | PlanIncidentCommunicationMatrix | object,
) -> PlanIncidentCommunicationMatrix:
    """Compatibility alias for incident communication summaries."""
    return derive_plan_incident_communication_matrix(source)


def plan_incident_communication_matrix_to_dict(
    matrix: PlanIncidentCommunicationMatrix,
) -> dict[str, Any]:
    """Serialize an incident communication matrix to a plain dictionary."""
    return matrix.to_dict()


plan_incident_communication_matrix_to_dict.__test__ = False


def plan_incident_communication_matrix_to_markdown(
    matrix: PlanIncidentCommunicationMatrix,
) -> str:
    """Render an incident communication matrix as Markdown."""
    return matrix.to_markdown()


plan_incident_communication_matrix_to_markdown.__test__ = False


def _task_rows(task: Mapping[str, Any], index: int) -> list[PlanIncidentCommunicationRow]:
    risks, evidence = _task_risks(task)
    if not risks or _suppressed(task, risks):
        return []

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    priority = _priority(task, risks)
    owners = tuple(_dedupe([*_owner_hints(task), *_default_owners(risks)]))
    audiences = _audiences(risks, priority)
    return [
        PlanIncidentCommunicationRow(
            task_id=task_id,
            title=title,
            audience=audience,
            risk_categories=risks,
            trigger_conditions=_trigger_conditions(risks, audience),
            draft_message_topics=_message_topics(risks, audience),
            owner_suggestions=owners,
            escalation_timing=_escalation_timing(priority, audience),
            priority=priority,
            evidence=evidence,
        )
        for audience in audiences
    ]


def _task_risks(
    task: Mapping[str, Any],
) -> tuple[tuple[IncidentCommunicationRisk, ...], tuple[str, ...]]:
    risks: list[IncidentCommunicationRisk] = []
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = _path_text(normalized)
        matched = False
        for risk, pattern in _PATH_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                risks.append(risk)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        before = len(risks)
        _apply_text_risks(text, risks)
        if len(risks) > before:
            evidence.append(_evidence_snippet(source_field, text))

    ordered = tuple(risk for risk in _RISK_ORDER if risk in set(risks))
    return ordered, tuple(_dedupe(evidence))


def _apply_text_risks(text: str, risks: list[IncidentCommunicationRisk]) -> None:
    if _RELIABILITY_RE.search(text):
        risks.append("reliability")
    if _DATA_RE.search(text):
        risks.append("data_integrity")
    if _MIGRATION_RE.search(text):
        risks.append("migration")
    if _INTEGRATION_RE.search(text):
        risks.append("external_integration")
    if _CUSTOMER_RE.search(text):
        risks.append("customer_facing")


def _suppressed(task: Mapping[str, Any], risks: tuple[IncidentCommunicationRisk, ...]) -> bool:
    text = " ".join(value for _, value in _candidate_texts(task))
    paths = " ".join(_strings(task.get("files_or_modules") or task.get("files")))
    if _SUPPRESS_RE.search(f"{text} {paths}") and str(task.get("risk_level", "")).casefold() == "low":
        return True
    return bool(_SUPPRESS_RE.search(f"{text} {paths}")) and not any(
        risk in risks for risk in ("reliability", "data_integrity", "migration", "external_integration")
    )


def _priority(
    task: Mapping[str, Any],
    risks: tuple[IncidentCommunicationRisk, ...],
) -> IncidentCommunicationPriority:
    text = " ".join(value for _, value in _candidate_texts(task)).casefold()
    if _optional_text(task.get("risk_level")) and "high" in str(task.get("risk_level")).casefold():
        return "high"
    if any(risk in risks for risk in ("reliability", "data_integrity", "migration")):
        return "high"
    if "customer_facing" in risks and "external_integration" in risks:
        return "high"
    if re.search(r"\b(?:critical|sev[ -]?[012]|outage|data loss|downtime)\b", text, re.I):
        return "high"
    return "medium"


def _audiences(
    risks: tuple[IncidentCommunicationRisk, ...],
    priority: IncidentCommunicationPriority,
) -> tuple[IncidentCommunicationAudience, ...]:
    audiences: list[IncidentCommunicationAudience] = ["operations", "engineering", "support"]
    if "customer_facing" in risks or priority == "high":
        audiences.extend(["customers", "customer_success"])
    if "data_integrity" in risks or "migration" in risks:
        audiences.extend(["admins", "data_governance"])
    if "external_integration" in risks:
        audiences.extend(["vendor_partner", "security"])
    if priority == "high":
        audiences.append("executives")
    return tuple(audience for audience in _AUDIENCE_ORDER if audience in set(audiences))


def _trigger_conditions(
    risks: tuple[IncidentCommunicationRisk, ...],
    audience: IncidentCommunicationAudience,
) -> tuple[str, ...]:
    triggers: list[str] = []
    if "reliability" in risks:
        triggers.append("Notify if availability, latency, queue health, or error rates breach launch guardrails.")
    if "data_integrity" in risks:
        triggers.append("Notify if validation finds missing, duplicated, corrupted, or unreconciled records.")
    if "migration" in risks:
        triggers.append("Notify if migration progress stalls, rollback starts, or cutover verification fails.")
    if "external_integration" in risks:
        triggers.append("Notify if provider errors, webhook failures, auth failures, or dependency latency spike.")
    if "customer_facing" in risks or audience in {"customers", "admins", "customer_success", "support"}:
        triggers.append("Notify if customers see degraded behavior, incorrect UI state, or support volume spikes.")
    return tuple(_dedupe(triggers))


def _message_topics(
    risks: tuple[IncidentCommunicationRisk, ...],
    audience: IncidentCommunicationAudience,
) -> tuple[str, ...]:
    topics: list[str] = []
    if audience in {"customers", "admins"}:
        topics.extend(["customer impact", "current workaround", "next update timing"])
    if audience in {"support", "customer_success"}:
        topics.extend(["known symptoms", "ticket triage guidance", "customer-safe status summary"])
    if audience in {"operations", "engineering"}:
        topics.extend(["incident scope", "rollback or mitigation status", "owner and handoff"])
    if audience == "data_governance" or "data_integrity" in risks:
        topics.append("data validation and reconciliation status")
    if audience in {"vendor_partner", "security"} or "external_integration" in risks:
        topics.append("provider escalation status and auth/security impact")
    if audience == "executives":
        topics.extend(["business impact", "customer exposure", "ETA and decision needs"])
    if "migration" in risks:
        topics.append("migration phase and rollback readiness")
    return tuple(_dedupe(topics))


def _escalation_timing(
    priority: IncidentCommunicationPriority,
    audience: IncidentCommunicationAudience,
) -> str:
    if audience in {"customers", "admins"}:
        return "Within 30 minutes of confirmed customer impact, then every 60 minutes until stable."
    if priority == "high":
        return "Immediately on incident declaration, then at every mitigation or rollback checkpoint."
    return "During launch watch if guardrails breach or support volume materially increases."


def _default_owners(
    risks: tuple[IncidentCommunicationRisk, ...],
) -> tuple[str, ...]:
    owners: list[str] = ["incident commander", "engineering owner"]
    if "reliability" in risks:
        owners.append("on-call owner")
    if "data_integrity" in risks or "migration" in risks:
        owners.extend(["database owner", "data governance owner"])
    if "external_integration" in risks:
        owners.extend(["integration owner", "vendor escalation owner"])
    if "customer_facing" in risks:
        owners.extend(["support lead", "customer success owner"])
    return tuple(_dedupe(owners))


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        hints.extend(_metadata_key_values(metadata, _OWNER_KEYS))
    return hints


def _metadata_key_values(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    wanted = {key.casefold() for key in keys}
    for key, value in _walk_metadata(metadata):
        normalized = key.casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            values.extend(_strings(value))
    return values


def _walk_metadata(value: Mapping[str, Any]) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        key_text = str(key)
        child = value[key]
        pairs.append((key_text, child))
        if isinstance(child, Mapping):
            pairs.extend(_walk_metadata(child))
    return pairs


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            for text in _strings(value):
                texts.append((f"metadata.{key}", text))
    return texts


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if isinstance(plan, ExecutionPlan):
        return dict(plan.model_dump(mode="python"))
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(dict(item.model_dump(mode="python")))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    return value.replace("\\", "/").casefold()


def _path_text(path: str) -> str:
    return " ".join(part.replace("_", " ").replace("-", " ") for part in PurePosixPath(path).parts)


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
    "IncidentCommunicationAudience",
    "IncidentCommunicationPriority",
    "IncidentCommunicationRisk",
    "PlanIncidentCommunicationMatrix",
    "PlanIncidentCommunicationRow",
    "build_plan_incident_communication_matrix",
    "derive_plan_incident_communication_matrix",
    "generate_plan_incident_communication_matrix",
    "plan_incident_communication_matrix_to_dict",
    "plan_incident_communication_matrix_to_markdown",
    "summarize_plan_incident_communication_matrix",
]
