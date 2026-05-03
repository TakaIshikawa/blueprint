"""Build incident severity triage matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IncidentSeverityTriageField = Literal[
    "triage_owner",
    "response_sla",
    "communication_path",
    "escalation_path",
]
IncidentSeverityLevel = Literal["sev0", "sev1", "sev2", "sev3", "sev4", "unspecified"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REQUIRED_FIELDS: tuple[IncidentSeverityTriageField, ...] = (
    "triage_owner",
    "response_sla",
    "communication_path",
    "escalation_path",
)
_SEVERITY_ORDER: dict[IncidentSeverityLevel, int] = {
    "sev0": 0,
    "sev1": 1,
    "sev2": 2,
    "sev3": 3,
    "sev4": 4,
    "unspecified": 5,
}
_INCIDENT_RE = re.compile(
    r"\b(?:incident|outage|degradation|degraded|paging|page(?:r|d)?|on[- ]?call|"
    r"escalation|customer impact|customer[- ]visible|security incident|data incident|"
    r"support escalation|sev(?:erity)?\s*[0-4]|sev[- ]?[0-4]|p[0-4]|production issue|"
    r"major incident|availability|downtime|data loss|breach|status page)\b",
    re.I,
)
_SUPPRESS_RE = re.compile(r"\b(?:docs?|documentation|readme|typo|formatting|test fixture)\b", re.I)
_SEVERITY_PATTERNS: tuple[tuple[IncidentSeverityLevel, re.Pattern[str]], ...] = (
    (
        "sev0",
        re.compile(
            r"\b(?:sev(?:erity)?[- ]?0|p0|critical|major incident|breach|data loss|"
            r"data incident|pii)\b",
            re.I,
        ),
    ),
    ("sev1", re.compile(r"\b(?:sev(?:erity)?[- ]?1|p1|outage|downtime|customer[- ]visible)\b", re.I)),
    ("sev2", re.compile(r"\b(?:sev(?:erity)?[- ]?2|p2|degradation|degraded|partial outage)\b", re.I)),
    ("sev3", re.compile(r"\b(?:sev(?:erity)?[- ]?3|p3|elevated errors)\b", re.I)),
    ("sev4", re.compile(r"\b(?:sev(?:erity)?[- ]?4|p4|minor incident|low impact)\b", re.I)),
)
_TRIGGER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("security incident", re.compile(r"\bsecurity incident\b|\bbreach\b|\bcredential\b", re.I)),
    ("data incident", re.compile(r"\bdata incident\b|\bdata loss\b|\bcorruption\b|\bpii\b", re.I)),
    ("customer impact", re.compile(r"\bcustomer impact\b|\bcustomer[- ]visible\b|\buser[- ]visible\b", re.I)),
    ("outage", re.compile(r"\boutage\b|\bdowntime\b|\bunavailable\b", re.I)),
    ("degradation", re.compile(r"\bdegradation\b|\bdegraded\b|\blatency\b|\belevated errors\b", re.I)),
    ("paging", re.compile(r"\bpaging\b|\bpage(?:r|d)?\b|\bon[- ]?call\b", re.I)),
    ("support escalation", re.compile(r"\bsupport escalation\b|\bticket spike\b|\bsupport volume\b", re.I)),
    ("escalation", re.compile(r"\bescalation\b|\bescalate\b", re.I)),
    ("incident", re.compile(r"\bincident\b|\bproduction issue\b", re.I)),
)
_TRIGGER_ORDER = {trigger: index for index, (trigger, _) in enumerate(_TRIGGER_PATTERNS)}
_FIELD_KEY_ALIASES: dict[IncidentSeverityTriageField, tuple[str, ...]] = {
    "triage_owner": (
        "triage_owner",
        "incident_owner",
        "incident_commander",
        "owner",
        "owners",
        "dri",
        "assignee",
        "team",
        "oncall",
        "on_call",
    ),
    "response_sla": (
        "response_sla",
        "sla",
        "response_time",
        "response_target",
        "ack_sla",
        "acknowledgement_sla",
        "urgency",
    ),
    "communication_path": (
        "communication_path",
        "communications",
        "comms",
        "comms_path",
        "status_page",
        "customer_comms",
        "notification_path",
    ),
    "escalation_path": (
        "escalation_path",
        "escalation",
        "escalations",
        "paging_path",
        "pager",
        "pagerduty",
        "war_room",
    ),
}
_FIELD_PATTERNS: dict[IncidentSeverityTriageField, tuple[re.Pattern[str], ...]] = {
    "triage_owner": (
        re.compile(
            r"\b(?:triage owner|incident owner|incident commander|owner|dri|on[- ]?call|team)"
            r"\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
    ),
    "response_sla": (
        re.compile(
            r"\b(?:response sla|ack sla|acknowledgement sla|response target|response time|sla|urgency)"
            r"\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(
            r"\b(?:respond|acknowledge|page|triage)\s+(?:within|in)\s+(\d+\s*(?:minutes?|mins?|hours?|hrs?))\b",
            re.I,
        ),
    ),
    "communication_path": (
        re.compile(
            r"\b(?:communication path|communications?|comms path|customer comms|notify|notification path|status page)"
            r"\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
        re.compile(r"\b((?:status page|slack|email|support macro|customer success|zendesk)[^.;\n]*)", re.I),
    ),
    "escalation_path": (
        re.compile(
            r"\b(?:escalation path|escalation|escalate to|paging path|pagerduty|war room)"
            r"\s*[:=-]\s*([^.;\n]+)",
            re.I,
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class PlanIncidentSeverityTriageMatrixRow:
    """Incident severity triage details for one incident-related task."""

    task_id: str
    title: str
    severity_level: IncidentSeverityLevel = "unspecified"
    trigger_signal: str = ""
    triage_owner: str = ""
    response_sla: str = ""
    communication_path: str = ""
    escalation_path: str = ""
    missing_fields: tuple[IncidentSeverityTriageField, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity_level": self.severity_level,
            "trigger_signal": self.trigger_signal,
            "triage_owner": self.triage_owner,
            "response_sla": self.response_sla,
            "communication_path": self.communication_path,
            "escalation_path": self.escalation_path,
            "missing_fields": list(self.missing_fields),
            "evidence": list(self.evidence),
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True, slots=True)
class PlanIncidentSeverityTriageMatrix:
    """Plan-level incident severity triage matrix."""

    plan_id: str | None = None
    rows: tuple[PlanIncidentSeverityTriageMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanIncidentSeverityTriageMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return incident severity triage rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the severity triage matrix as deterministic Markdown."""
        title = "# Plan Incident Severity Triage Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Incident triage tasks: {self.summary.get('incident_task_count', 0)}",
            f"- Triage row count: {self.summary.get('row_count', 0)}",
            f"- Missing triage fields: {self.summary.get('missing_field_count', 0)}",
            "- Severity counts: "
            + ", ".join(
                f"{severity} {self.summary.get('severity_counts', {}).get(severity, 0)}"
                for severity in _SEVERITY_ORDER
            ),
        ]
        if not self.rows:
            lines.extend(["", "No incident severity triage rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                (
                    "| Task | Title | Severity | Trigger | Triage Owner | Response SLA | "
                    "Communication Path | Escalation Path | Missing Fields | Recommendation | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{_markdown_cell(row.title)} | "
                f"{row.severity_level} | "
                f"{_markdown_cell(row.trigger_signal or 'unspecified')} | "
                f"{_markdown_cell(row.triage_owner or 'unspecified')} | "
                f"{_markdown_cell(row.response_sla or 'unspecified')} | "
                f"{_markdown_cell(row.communication_path or 'unspecified')} | "
                f"{_markdown_cell(row.escalation_path or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell(row.recommendation)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_incident_severity_triage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanIncidentSeverityTriageMatrix:
    """Build severity triage rows for incident-related execution tasks."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index)) is not None
            ),
            key=lambda row: (
                _SEVERITY_ORDER[row.severity_level],
                len(row.missing_fields),
                row.task_id,
                row.title.casefold(),
            ),
        )
    )
    return PlanIncidentSeverityTriageMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, total_task_count=len(tasks)),
    )


def generate_plan_incident_severity_triage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[PlanIncidentSeverityTriageMatrixRow, ...]:
    """Return incident severity triage rows for relevant execution tasks."""
    return build_plan_incident_severity_triage_matrix(source).rows


def derive_plan_incident_severity_triage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanIncidentSeverityTriageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanIncidentSeverityTriageMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanIncidentSeverityTriageMatrix):
        return source
    return build_plan_incident_severity_triage_matrix(source)


def summarize_plan_incident_severity_triage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanIncidentSeverityTriageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanIncidentSeverityTriageMatrix:
    """Compatibility alias for incident severity triage summaries."""
    return derive_plan_incident_severity_triage_matrix(source)


def plan_incident_severity_triage_matrix_to_dict(
    matrix: PlanIncidentSeverityTriageMatrix,
) -> dict[str, Any]:
    """Serialize an incident severity triage matrix to a plain dictionary."""
    return matrix.to_dict()


plan_incident_severity_triage_matrix_to_dict.__test__ = False


def plan_incident_severity_triage_matrix_to_dicts(
    rows: (
        PlanIncidentSeverityTriageMatrix
        | tuple[PlanIncidentSeverityTriageMatrixRow, ...]
        | list[PlanIncidentSeverityTriageMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize incident severity triage rows to dictionaries."""
    if isinstance(rows, PlanIncidentSeverityTriageMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_incident_severity_triage_matrix_to_dicts.__test__ = False


def plan_incident_severity_triage_matrix_to_markdown(
    matrix: PlanIncidentSeverityTriageMatrix,
) -> str:
    """Render an incident severity triage matrix as Markdown."""
    return matrix.to_markdown()


plan_incident_severity_triage_matrix_to_markdown.__test__ = False


def _row_for_task(
    task: Mapping[str, Any],
    index: int,
) -> PlanIncidentSeverityTriageMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    fields: dict[IncidentSeverityTriageField, list[str]] = {key: [] for key in _REQUIRED_FIELDS}
    severities: list[IncidentSeverityLevel] = []
    triggers: list[str] = []
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _INCIDENT_RE.search(text):
            evidence.append(snippet)
            _apply_signal_detection(text, severities, triggers)
        for field_name, patterns in _FIELD_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(text):
                    value = next((group for group in match.groups() if group), match.group(0))
                    fields[field_name].append(_clean(value))
                    evidence.append(snippet)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for field_name, aliases in _FIELD_KEY_ALIASES.items():
            for source_field, value in _metadata_values(metadata, aliases):
                for item in _strings(value):
                    fields[field_name].insert(0, _clean(item))
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, value in _metadata_values(metadata, ("severity", "severity_level", "sev")):
            for item in _strings(value):
                if severity := _severity_from_text(item):
                    severities.insert(0, severity)
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, text in _metadata_texts(metadata):
            key_text = source_field.replace("_", " ")
            if _INCIDENT_RE.search(text) or _INCIDENT_RE.search(key_text):
                evidence.append(_evidence_snippet(source_field, text or key_text))
                _apply_signal_detection(f"{key_text} {text}", severities, triggers)

    for key in ("owner", "assignee", "dri", "team", "owner_type"):
        if owner := _optional_text(task.get(key)):
            fields["triage_owner"].append(owner)
            evidence.append(_evidence_snippet(key, owner))

    if not evidence or _suppressed(task):
        return None

    severity_level = _first_severity(severities)
    trigger_signal = _first_trigger(triggers) or "incident"
    triage_owner = _first(fields["triage_owner"])
    response_sla = _first(fields["response_sla"])
    communication_path = _first(fields["communication_path"])
    escalation_path = _first(fields["escalation_path"])
    missing_fields = tuple(
        field_name
        for field_name in _REQUIRED_FIELDS
        if (
            (field_name == "triage_owner" and not triage_owner)
            or (field_name == "response_sla" and not response_sla)
            or (field_name == "communication_path" and not communication_path)
            or (field_name == "escalation_path" and not escalation_path)
        )
    )
    return PlanIncidentSeverityTriageMatrixRow(
        task_id=task_id,
        title=title,
        severity_level=severity_level,
        trigger_signal=trigger_signal,
        triage_owner=triage_owner,
        response_sla=response_sla,
        communication_path=communication_path,
        escalation_path=escalation_path,
        missing_fields=missing_fields,
        evidence=tuple(_dedupe(evidence)),
        recommendation=_recommendation(severity_level, missing_fields),
    )


def _apply_signal_detection(
    text: str,
    severities: list[IncidentSeverityLevel],
    triggers: list[str],
) -> None:
    if severity := _severity_from_text(text):
        if _explicit_severity_from_text(text):
            severities.insert(0, severity)
        else:
            severities.append(severity)
    for trigger, pattern in _TRIGGER_PATTERNS:
        if pattern.search(text):
            triggers.append(trigger)


def _severity_from_text(text: str) -> IncidentSeverityLevel | None:
    if explicit := _explicit_severity_from_text(text):
        return explicit
    for severity, pattern in _SEVERITY_PATTERNS:
        if pattern.search(text):
            return severity
    return None


def _explicit_severity_from_text(text: str) -> IncidentSeverityLevel | None:
    explicit = re.search(r"\b(?:sev(?:erity)?[- ]?|p)([0-4])\b", text, re.I)
    if not explicit:
        return None
    value = f"sev{explicit.group(1)}"
    return value if value in _SEVERITY_ORDER else None  # type: ignore[return-value]


def _first_severity(values: Iterable[IncidentSeverityLevel]) -> IncidentSeverityLevel:
    severities = _dedupe(values)
    if not severities:
        return "unspecified"
    return severities[0]


def _first_trigger(values: Iterable[str]) -> str:
    triggers = _dedupe(values)
    if not triggers:
        return ""
    return sorted(triggers, key=lambda value: _TRIGGER_ORDER.get(value, 99))[0]


def _suppressed(task: Mapping[str, Any]) -> bool:
    text = " ".join(value for _, value in _candidate_texts(task))
    return bool(_SUPPRESS_RE.search(text)) and not re.search(
        r"\b(?:outage|security incident|data incident|customer impact|sev[- ]?[0-2]|p[0-2])\b",
        text,
        re.I,
    )


def _recommendation(
    severity_level: IncidentSeverityLevel,
    missing_fields: tuple[IncidentSeverityTriageField, ...],
) -> str:
    if not missing_fields:
        return (
            "Ready: incident triage names the severity, owner, response SLA, "
            "communication path, and escalation path."
        )
    actions = {
        "triage_owner": "assign the incident commander or triage owner",
        "response_sla": "state the response SLA or acknowledgement target",
        "communication_path": "define the customer and internal communication path",
        "escalation_path": "document the paging or executive escalation path",
    }
    parts = [actions[field_name] for field_name in missing_fields]
    if severity_level == "unspecified":
        parts.insert(0, "classify the severity level")
    return "Before handoff, " + "; ".join(parts) + "."


def _summary(
    rows: tuple[PlanIncidentSeverityTriageMatrixRow, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "incident_task_count": len(rows),
        "unrelated_task_count": max(total_task_count - len(rows), 0),
        "row_count": len(rows),
        "tasks_missing_owner": sum(1 for row in rows if "triage_owner" in row.missing_fields),
        "tasks_missing_sla": sum(1 for row in rows if "response_sla" in row.missing_fields),
        "tasks_missing_escalation": sum(
            1 for row in rows if "escalation_path" in row.missing_fields
        ),
        "tasks_missing_communication": sum(
            1 for row in rows if "communication_path" in row.missing_fields
        ),
        "missing_field_count": sum(len(row.missing_fields) for row in rows),
        "missing_field_counts": {
            field_name: sum(1 for row in rows if field_name in row.missing_fields)
            for field_name in _REQUIRED_FIELDS
        },
        "severity_counts": {
            severity: sum(1 for row in rows if row.severity_level == severity)
            for severity in _SEVERITY_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
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
    return tasks


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
    for field_name in (
        "acceptance_criteria",
        "definition_of_done",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_values(
    value: Any,
    aliases: tuple[str, ...],
    prefix: str = "metadata",
) -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    wanted = {alias.casefold() for alias in aliases}
    matches: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        child = value[key]
        field = f"{prefix}.{key}"
        normalized = str(key).casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            matches.append((field, child))
        if isinstance(child, Mapping):
            matches.extend(_metadata_values(child, aliases, field))
    return matches


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if _INCIDENT_RE.search(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _first(values: Iterable[str]) -> str:
    values = _dedupe(_clean(value) for value in values)
    return values[0] if values else ""


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "IncidentSeverityLevel",
    "IncidentSeverityTriageField",
    "PlanIncidentSeverityTriageMatrix",
    "PlanIncidentSeverityTriageMatrixRow",
    "build_plan_incident_severity_triage_matrix",
    "derive_plan_incident_severity_triage_matrix",
    "generate_plan_incident_severity_triage_matrix",
    "plan_incident_severity_triage_matrix_to_dict",
    "plan_incident_severity_triage_matrix_to_dicts",
    "plan_incident_severity_triage_matrix_to_markdown",
    "summarize_plan_incident_severity_triage_matrix",
]
