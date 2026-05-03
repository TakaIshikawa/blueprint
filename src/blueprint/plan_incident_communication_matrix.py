"""Build incident communication readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IncidentCommunicationComponent = Literal[
    "customer_updates",
    "internal_escalation",
    "owner_assignment",
    "severity_thresholds",
    "communication_channels",
    "follow_up_tasks",
    "timing_sla_expectations",
]
IncidentCommunicationReadiness = Literal["ready", "partial", "missing"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_COMPONENT_ORDER: tuple[IncidentCommunicationComponent, ...] = (
    "customer_updates",
    "internal_escalation",
    "owner_assignment",
    "severity_thresholds",
    "communication_channels",
    "follow_up_tasks",
    "timing_sla_expectations",
)
_READINESS_ORDER: dict[IncidentCommunicationReadiness, int] = {
    "missing": 0,
    "partial": 1,
    "ready": 2,
}

_INCIDENT_RE = re.compile(
    r"\b(?:incident|outage|degradation|degraded|downtime|service interruption|"
    r"sev[ -]?[0-4]|severity|rollback|roll back|all clear|postmortem|post[- ]incident|"
    r"status page|statuspage|customer update|customer comms?|internal escalation|"
    r"war room|incident commander|on[- ]?call|pager|launch watch|sla|slo)\b",
    re.I,
)
_PATH_RE = re.compile(
    r"(?:^|/)(?:incidents?|runbooks?|ops|comms?|communications?|status|support)(?:/|$)",
    re.I,
)
_COMPONENT_PATTERNS: dict[IncidentCommunicationComponent, re.Pattern[str]] = {
    "customer_updates": re.compile(
        r"\b(?:customer updates?|customer comms?|customer communication|notify customers?|"
        r"status page|statuspage|public status|external update|customer email|in[- ]app banner|"
        r"support portal|all clear)\b",
        re.I,
    ),
    "internal_escalation": re.compile(
        r"\b(?:internal escalation|escalation path|escalate to|war room|incident commander|"
        r"on[- ]?call|pager|pagerduty|opsgenie|slack bridge|engineering escalation|"
        r"support escalation)\b",
        re.I,
    ),
    "owner_assignment": re.compile(
        r"\b(?:owner|owners|dri|responsible|assignee|incident commander|comms lead|"
        r"communications lead|support lead|on[- ]?call|team)\b",
        re.I,
    ),
    "severity_thresholds": re.compile(
        r"\b(?:sev[ -]?[0-4]|severity|threshold|trigger|declare an incident|incident declaration|"
        r"error budget|slo|sla breach|availability below|latency above|error rate)\b",
        re.I,
    ),
    "communication_channels": re.compile(
        r"\b(?:channel|channels|slack|email|status page|statuspage|sms|pager|in[- ]app|"
        r"support portal|zendesk|intercom|bridge|war room)\b",
        re.I,
    ),
    "follow_up_tasks": re.compile(
        r"\b(?:follow[- ]up|postmortem|post[- ]incident|retrospective|retro|action items?|"
        r"corrective actions?|rca|root cause|lessons learned|ticket follow[- ]up)\b",
        re.I,
    ),
    "timing_sla_expectations": re.compile(
        r"\b(?:timing|cadence|sla|slo|within \d+|every \d+|minutes?|hours?|next update|"
        r"update interval|response time|acknowledge|ack|t\+|by .*(?:minute|hour))\b",
        re.I,
    ),
}
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
_MISSING_REASON: dict[IncidentCommunicationComponent, str] = {
    "customer_updates": "Missing customer-facing update plan, such as status page, customer email, in-app banner, or all-clear communication.",
    "internal_escalation": "Missing internal escalation route for incident commander, on-call, support, or engineering response.",
    "owner_assignment": "Missing named communication owner, DRI, incident commander, on-call owner, or responsible team.",
    "severity_thresholds": "Missing severity thresholds or declaration triggers that decide when communication starts.",
    "communication_channels": "Missing communication channels for internal and external incident updates.",
    "follow_up_tasks": "Missing post-incident follow-up tasks such as postmortem, RCA, retro, or corrective action items.",
    "timing_sla_expectations": "Missing timing or SLA expectations for first response, update cadence, or all-clear communication.",
}


@dataclass(frozen=True, slots=True)
class PlanIncidentCommunicationRow:
    """One task-level incident communication readiness row."""

    task_id: str
    title: str
    required_components: tuple[IncidentCommunicationComponent, ...] = _COMPONENT_ORDER
    present_components: tuple[IncidentCommunicationComponent, ...] = field(default_factory=tuple)
    missing_components: tuple[IncidentCommunicationComponent, ...] = field(default_factory=tuple)
    readiness_level: IncidentCommunicationReadiness = "missing"
    gap_reasons: tuple[str, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    channel_hints: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "required_components": list(self.required_components),
            "present_components": list(self.present_components),
            "missing_components": list(self.missing_components),
            "readiness_level": self.readiness_level,
            "gap_reasons": list(self.gap_reasons),
            "owner_hints": list(self.owner_hints),
            "channel_hints": list(self.channel_hints),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanIncidentCommunicationMatrix:
    """Plan-level incident communication readiness matrix."""

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
            "records": [row.to_dict() for row in self.records],
            "incident_task_ids": list(self.incident_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return incident communication rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the incident communication readiness matrix as deterministic Markdown."""
        title = "# Plan Incident Communication Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Incident communication task count: {self.summary.get('incident_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            f"- Gap count: {self.summary.get('gap_count', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No incident communication readiness rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Task | Title | Readiness | Present Components | Missing Components | "
                    "Owners | Channels | Gap Reasons | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.readiness_level} | "
                f"{_markdown_cell(', '.join(row.present_components) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_components) or 'none')} | "
                f"{_markdown_cell(', '.join(row.owner_hints) or 'none')} | "
                f"{_markdown_cell(', '.join(row.channel_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gap_reasons) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_incident_communication_matrix(source: Any) -> PlanIncidentCommunicationMatrix:
    """Build task-level incident communication readiness for an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    rows = tuple(
        sorted(
            (row for index, task in enumerate(tasks, start=1) if (row := _task_row(task, index))),
            key=lambda row: (
                _READINESS_ORDER[row.readiness_level],
                -len(row.missing_components),
                row.task_id,
            ),
        )
    )
    return PlanIncidentCommunicationMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        incident_task_ids=tuple(row.task_id for row in rows),
        summary=_summary(tasks, rows),
    )


def generate_plan_incident_communication_matrix(source: Any) -> PlanIncidentCommunicationMatrix:
    """Generate an incident communication readiness matrix from a plan-like source."""
    return build_plan_incident_communication_matrix(source)


def analyze_plan_incident_communication_matrix(source: Any) -> PlanIncidentCommunicationMatrix:
    """Analyze an execution plan for incident communication readiness."""
    return build_plan_incident_communication_matrix(source)


def derive_plan_incident_communication_matrix(source: Any) -> PlanIncidentCommunicationMatrix:
    """Return an existing matrix or derive one from a plan-like source."""
    if isinstance(source, PlanIncidentCommunicationMatrix):
        return source
    return analyze_plan_incident_communication_matrix(source)


def summarize_plan_incident_communication_matrix(source: Any) -> PlanIncidentCommunicationMatrix:
    """Compatibility alias for incident communication readiness summaries."""
    return derive_plan_incident_communication_matrix(source)


def plan_incident_communication_matrix_to_dict(
    matrix: PlanIncidentCommunicationMatrix,
) -> dict[str, Any]:
    """Serialize an incident communication readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_incident_communication_matrix_to_dict.__test__ = False


def plan_incident_communication_matrix_to_dicts(
    matrix: PlanIncidentCommunicationMatrix,
) -> list[dict[str, Any]]:
    """Serialize incident communication readiness rows to plain dictionaries."""
    return matrix.to_dicts()


plan_incident_communication_matrix_to_dicts.__test__ = False


def plan_incident_communication_matrix_to_markdown(
    matrix: PlanIncidentCommunicationMatrix,
) -> str:
    """Render an incident communication readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_incident_communication_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanIncidentCommunicationRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    present, evidence = _component_coverage(task)
    incident_signals = _incident_signal_count(task, present)
    if not present and not incident_signals:
        return None

    present_components = tuple(component for component in _COMPONENT_ORDER if component in present)
    missing_components = tuple(
        component for component in _COMPONENT_ORDER if component not in set(present_components)
    )
    gap_reasons = tuple(_MISSING_REASON[component] for component in missing_components)
    return PlanIncidentCommunicationRow(
        task_id=task_id,
        title=title,
        present_components=present_components,
        missing_components=missing_components,
        readiness_level=_readiness_level(present_components),
        gap_reasons=gap_reasons,
        owner_hints=tuple(
            _dedupe([*_owner_hints(task), *_default_owner_hints(present_components)])
        ),
        channel_hints=tuple(
            _dedupe([*_channel_hints(task), *_default_channel_hints(present_components)])
        ),
        evidence=evidence,
    )


def _component_coverage(
    task: Mapping[str, Any],
) -> tuple[set[IncidentCommunicationComponent], tuple[str, ...]]:
    present: set[IncidentCommunicationComponent] = set()
    evidence: list[str] = []
    if _owner_hints(task):
        present.add("owner_assignment")
        evidence.append("metadata: owner hint")

    for source_field, text in _candidate_texts(task):
        matched: list[str] = []
        for component, pattern in _COMPONENT_PATTERNS.items():
            if pattern.search(text):
                present.add(component)
                matched.append(component)
        if matched:
            evidence.append(f"{_evidence_snippet(source_field, text)} ({', '.join(matched)})")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_RE.search(_normalized_path(path)):
            evidence.append(f"files_or_modules: {path}")

    return present, tuple(_dedupe(evidence))


def _incident_signal_count(
    task: Mapping[str, Any],
    present: set[IncidentCommunicationComponent],
) -> int:
    count = len(present)
    for _, text in _candidate_texts(task):
        if _INCIDENT_RE.search(text):
            count += 1
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_RE.search(_normalized_path(path)):
            count += 1
    return count


def _readiness_level(
    present_components: tuple[IncidentCommunicationComponent, ...],
) -> IncidentCommunicationReadiness:
    if len(present_components) == len(_COMPONENT_ORDER):
        return "ready"
    if present_components:
        return "partial"
    return "missing"


def _summary(
    tasks: list[dict[str, Any]],
    rows: tuple[PlanIncidentCommunicationRow, ...],
) -> dict[str, Any]:
    return {
        "task_count": len(tasks),
        "incident_task_count": len(rows),
        "ready_task_count": sum(1 for row in rows if row.readiness_level == "ready"),
        "partial_task_count": sum(1 for row in rows if row.readiness_level == "partial"),
        "missing_task_count": sum(1 for row in rows if row.readiness_level == "missing"),
        "gap_count": sum(len(row.missing_components) for row in rows),
        "readiness_counts": {
            readiness: sum(1 for row in rows if row.readiness_level == readiness)
            for readiness in _READINESS_ORDER
        },
        "component_coverage_counts": {
            component: sum(1 for row in rows if component in row.present_components)
            for component in _COMPONENT_ORDER
        },
        "missing_component_counts": {
            component: sum(1 for row in rows if component in row.missing_components)
            for component in _COMPONENT_ORDER
        },
    }


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            normalized = key.casefold().replace("-", "_").replace(" ", "_")
            if normalized in _OWNER_KEYS:
                hints.extend(_strings(value))
    return hints


def _default_owner_hints(
    present_components: tuple[IncidentCommunicationComponent, ...],
) -> tuple[str, ...]:
    if "internal_escalation" in present_components:
        return ("incident commander", "on-call owner")
    if "customer_updates" in present_components:
        return ("communications lead",)
    return ()


def _channel_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    text = " ".join(value for _, value in _candidate_texts(task)).casefold()
    channel_terms = (
        ("status page", "status page"),
        ("statuspage", "status page"),
        ("slack", "Slack"),
        ("email", "email"),
        ("in-app", "in-app"),
        ("in app", "in-app"),
        ("support portal", "support portal"),
        ("zendesk", "Zendesk"),
        ("intercom", "Intercom"),
        ("pager", "pager"),
        ("sms", "SMS"),
    )
    for token, label in channel_terms:
        if token in text:
            hints.append(label)
    return hints


def _default_channel_hints(
    present_components: tuple[IncidentCommunicationComponent, ...],
) -> tuple[str, ...]:
    hints: list[str] = []
    if "customer_updates" in present_components:
        hints.append("status page")
    if "internal_escalation" in present_components:
        hints.append("Slack")
    return tuple(hints)


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
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
    ):
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


def _plan_payload(plan: Any) -> dict[str, Any]:
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


def _walk_metadata(value: Mapping[str, Any]) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        key_text = str(key)
        child = value[key]
        pairs.append((key_text, child))
        if isinstance(child, Mapping):
            pairs.extend(_walk_metadata(child))
    return pairs


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").casefold()


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
    "IncidentCommunicationComponent",
    "IncidentCommunicationReadiness",
    "PlanIncidentCommunicationMatrix",
    "PlanIncidentCommunicationRow",
    "analyze_plan_incident_communication_matrix",
    "build_plan_incident_communication_matrix",
    "derive_plan_incident_communication_matrix",
    "generate_plan_incident_communication_matrix",
    "plan_incident_communication_matrix_to_dict",
    "plan_incident_communication_matrix_to_dicts",
    "plan_incident_communication_matrix_to_markdown",
    "summarize_plan_incident_communication_matrix",
]
