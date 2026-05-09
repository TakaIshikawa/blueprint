"""Build incident response readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IncidentResponseAspect = Literal[
    "severity_levels_defined",
    "response_procedures_documented",
    "escalation_paths_clear",
    "communication_protocols_set",
    "runbooks_available",
    "severity_definitions_clear",
    "missing_runbooks_identified",
    "incomplete_escalation_detected",
    "untested_procedures_flagged",
]
IncidentResponseReadiness = Literal["ready", "partial", "missing"]
IncidentType = Literal[
    "service_outage",
    "data_corruption",
    "security_breach",
    "performance_degradation",
    "dependency_failure",
    "deployment_failure",
    "data_loss",
    "cascading_failure",
]
SeverityLevel = Literal["P0", "P1", "P2", "P3", "P4"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_ASPECT_ORDER: tuple[IncidentResponseAspect, ...] = (
    "severity_levels_defined",
    "response_procedures_documented",
    "escalation_paths_clear",
    "communication_protocols_set",
    "runbooks_available",
    "severity_definitions_clear",
    "missing_runbooks_identified",
    "incomplete_escalation_detected",
    "untested_procedures_flagged",
)
_READINESS_ORDER: dict[IncidentResponseReadiness, int] = {
    "missing": 0,
    "partial": 1,
    "ready": 2,
}
_INCIDENT_TYPE_ORDER: tuple[IncidentType, ...] = (
    "service_outage",
    "data_corruption",
    "security_breach",
    "performance_degradation",
    "dependency_failure",
    "deployment_failure",
    "data_loss",
    "cascading_failure",
)
_SEVERITY_ORDER: tuple[SeverityLevel, ...] = ("P0", "P1", "P2", "P3", "P4")

_INCIDENT_RESPONSE_RE = re.compile(
    r"\b(?:incident response|incident|outage|degradation|degraded|downtime|"
    r"service interruption|sev[ -]?[0-4]|severity|p[0-4]|priority|rollback|"
    r"roll back|postmortem|post[- ]incident|runbook|playbook|escalation|"
    r"on[- ]?call|pager|drill|game day|fire drill|incident commander|"
    r"war room|response procedure|emergency)\b",
    re.I,
)
_PATH_RE = re.compile(
    r"(?:^|/)(?:incidents?|runbooks?|playbooks?|ops|operations|sre|response|"
    r"escalation|procedures?)(?:/|$)",
    re.I,
)

_ASPECT_PATTERNS: dict[IncidentResponseAspect, re.Pattern[str]] = {
    "severity_levels_defined": re.compile(
        r"\b(?:sev[ -]?[0-4]|severity|p[0-4]|priority|critical|high|medium|low|"
        r"severity level|severity definition|impact level)\b",
        re.I,
    ),
    "response_procedures_documented": re.compile(
        r"\b(?:response procedure|procedure|process|workflow|step|action|"
        r"runbook|playbook|response plan|incident workflow|documented)\b",
        re.I,
    ),
    "escalation_paths_clear": re.compile(
        r"\b(?:escalation path|escalation|escalate to|escalation chain|"
        r"on[- ]?call|pager|pagerduty|opsgenie|incident commander|escalation policy|"
        r"primary|secondary|escalation route|escalation matrix)\b",
        re.I,
    ),
    "communication_protocols_set": re.compile(
        r"\b(?:communication protocol|notification|alert|notify|status page|"
        r"statuspage|customer update|stakeholder|announce|war room|slack|"
        r"email|sms|communication plan|status update|coordinate|"
        r"cross[- ]team|multi[- ]team|communications? teams?)\b",
        re.I,
    ),
    "runbooks_available": re.compile(
        r"\b(?:runbook|playbook|procedure|guide|documentation|docs|wiki|"
        r"operational guide|response guide|incident guide)\b",
        re.I,
    ),
    "severity_definitions_clear": re.compile(
        r"\b(?:severity definition|impact criteria|severity criteria|"
        r"definition|criteria|threshold|sla|slo|availability|latency|error rate|"
        r"customer impact|revenue impact|user impact)\b",
        re.I,
    ),
    "missing_runbooks_identified": re.compile(
        r"\b(?:missing runbook|runbook gap|missing playbook|missing procedure|"
        r"runbook coverage|procedure gap|undocumented|no runbook|need runbook|"
        r"gaps? identified|gaps? documented|identify gaps?|missing)\b",
        re.I,
    ),
    "incomplete_escalation_detected": re.compile(
        r"\b(?:incomplete escalation|escalation gap|missing escalation|"
        r"escalation coverage|no escalation|escalation chain gap|unclear escalation|"
        r"completeness.*verified|verify.*completeness)\b",
        re.I,
    ),
    "untested_procedures_flagged": re.compile(
        r"\b(?:untested|not tested|need test|test|drill|game day|fire drill|"
        r"test plan|testing|rehearsal|dry run|simulation|chaos)\b",
        re.I,
    ),
}

_INCIDENT_TYPE_PATTERNS: dict[IncidentType, re.Pattern[str]] = {
    "service_outage": re.compile(
        r"\b(?:service outage|service outages|outage|outages|downtime|unavailable|down|offline|"
        r"total failure|complete failure)\b",
        re.I,
    ),
    "data_corruption": re.compile(
        r"\b(?:data corruption|corrupt|corrupted data|data integrity|"
        r"invalid data|bad data|data loss)\b",
        re.I,
    ),
    "security_breach": re.compile(
        r"\b(?:security breach|security breaches|breach|breaches|security incident|unauthorized access|"
        r"intrusion|compromise|attack|vulnerability|exploit)\b",
        re.I,
    ),
    "performance_degradation": re.compile(
        r"\b(?:performance degradation|degradation|degraded|slow|latency|"
        r"timeout|response time|performance issue)\b",
        re.I,
    ),
    "dependency_failure": re.compile(
        r"\b(?:dependency failure|dependency outage|third[- ]party|vendor|"
        r"external service|upstream|downstream|integration failure)\b",
        re.I,
    ),
    "deployment_failure": re.compile(
        r"\b(?:deployment failure|deploy fail|rollout fail|release fail|"
        r"bad deploy|failed release|deployment issue)\b",
        re.I,
    ),
    "data_loss": re.compile(
        r"\b(?:data loss|lost data|missing data|deleted data|unrecoverable|"
        r"backup failure|restore fail)\b",
        re.I,
    ),
    "cascading_failure": re.compile(
        r"\b(?:cascading failure|cascade|cascading|chain reaction|domino|"
        r"multi[- ]service|cross[- ]service|widespread)\b",
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
    "teams",
    "incident_commander",
    "secondary",
)

_MISSING_REASON: dict[IncidentResponseAspect, str] = {
    "severity_levels_defined": "Missing clear severity level definitions (P0, P1, P2, etc.) for incident classification.",
    "response_procedures_documented": "Missing documented response procedures, workflows, or step-by-step incident handling guides.",
    "escalation_paths_clear": "Missing clear escalation paths, on-call rotation, or escalation chain definitions.",
    "communication_protocols_set": "Missing communication protocols for notifications, status updates, and stakeholder communication.",
    "runbooks_available": "Missing operational runbooks or playbooks for incident response procedures.",
    "severity_definitions_clear": "Missing clear severity definitions with impact criteria, thresholds, and SLA/SLO mappings.",
    "missing_runbooks_identified": "Missing identification of runbook gaps or undocumented incident scenarios.",
    "incomplete_escalation_detected": "Missing detection of incomplete or unclear escalation chain coverage.",
    "untested_procedures_flagged": "Missing identification of untested procedures or lack of drill/rehearsal plans.",
}


@dataclass(frozen=True, slots=True)
class IncidentResponseMatrixEntry:
    """Single incident response matrix entry."""

    incident_type: IncidentType
    severity_level: SeverityLevel
    response_procedure: str | None = None
    escalation_chain: tuple[str, ...] = field(default_factory=tuple)
    on_call_rotation: tuple[str, ...] = field(default_factory=tuple)
    communication_channels: tuple[str, ...] = field(default_factory=tuple)
    runbook_reference: str | None = None
    drill_status: str = "untested"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "incident_type": self.incident_type,
            "severity_level": self.severity_level,
            "response_procedure": self.response_procedure,
            "escalation_chain": list(self.escalation_chain),
            "on_call_rotation": list(self.on_call_rotation),
            "communication_channels": list(self.communication_channels),
            "runbook_reference": self.runbook_reference,
            "drill_status": self.drill_status,
        }


@dataclass(frozen=True, slots=True)
class PlanIncidentResponseRow:
    """One task-level incident response readiness row."""

    task_id: str
    title: str
    required_aspects: tuple[IncidentResponseAspect, ...] = _ASPECT_ORDER
    present_aspects: tuple[IncidentResponseAspect, ...] = field(default_factory=tuple)
    missing_aspects: tuple[IncidentResponseAspect, ...] = field(default_factory=tuple)
    readiness_level: IncidentResponseReadiness = "missing"
    gap_reasons: tuple[str, ...] = field(default_factory=tuple)
    incident_types: tuple[IncidentType, ...] = field(default_factory=tuple)
    severity_levels: tuple[SeverityLevel, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matrix_entries: tuple[IncidentResponseMatrixEntry, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "required_aspects": list(self.required_aspects),
            "present_aspects": list(self.present_aspects),
            "missing_aspects": list(self.missing_aspects),
            "readiness_level": self.readiness_level,
            "gap_reasons": list(self.gap_reasons),
            "incident_types": list(self.incident_types),
            "severity_levels": list(self.severity_levels),
            "owner_hints": list(self.owner_hints),
            "evidence": list(self.evidence),
            "matrix_entries": [entry.to_dict() for entry in self.matrix_entries],
        }


@dataclass(frozen=True, slots=True)
class PlanIncidentResponseMatrix:
    """Plan-level incident response readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanIncidentResponseRow, ...] = field(default_factory=tuple)
    incident_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    incident_matrix: tuple[IncidentResponseMatrixEntry, ...] = field(default_factory=tuple)

    @property
    def records(self) -> tuple[PlanIncidentResponseRow, ...]:
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
            "incident_matrix": [entry.to_dict() for entry in self.incident_matrix],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return incident response rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the incident response readiness matrix as deterministic Markdown."""
        title = "# Plan Incident Response Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        scoring = self.summary.get("scoring", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Incident response task count: {self.summary.get('incident_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            f"- Gap count: {self.summary.get('gap_count', 0)}",
            "",
            "### Readiness Scoring",
            "",
            f"- Procedure coverage: {scoring.get('procedure_coverage', 0):.1f}% (weight: 30%)",
            f"- Automation level: {scoring.get('automation_level', 0):.1f}% (weight: 20%)",
            f"- Team preparedness: {scoring.get('team_preparedness', 0):.1f}% (weight: 25%)",
            f"- Documentation quality: {scoring.get('documentation_quality', 0):.1f}% (weight: 25%)",
            f"- Overall score: {scoring.get('overall_score', 0):.1f}%",
        ]

        if not self.rows:
            lines.extend(["", "No incident response readiness rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Task-Level Readiness",
                "",
                (
                    "| Task | Title | Readiness | Present Aspects | Missing Aspects | "
                    "Incident Types | Severity Levels | Owners | Evidence |"
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
                f"{_markdown_cell(', '.join(row.present_aspects) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_aspects) or 'none')} | "
                f"{_markdown_cell(', '.join(row.incident_types) or 'none')} | "
                f"{_markdown_cell(', '.join(row.severity_levels) or 'none')} | "
                f"{_markdown_cell(', '.join(row.owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence[:2]) or 'none')} |"
            )

        if self.incident_matrix:
            lines.extend(
                [
                    "",
                    "## Incident Response Matrix",
                    "",
                    (
                        "| Incident Type | Severity | Response Procedure | Escalation Chain | "
                        "On-Call | Communication | Runbook | Drill Status |"
                    ),
                    "| --- | --- | --- | --- | --- | --- | --- | --- |",
                ]
            )
            for entry in self.incident_matrix:
                lines.append(
                    "| "
                    f"{entry.incident_type} | "
                    f"{entry.severity_level} | "
                    f"{_markdown_cell(entry.response_procedure or 'N/A')} | "
                    f"{_markdown_cell(', '.join(entry.escalation_chain) or 'none')} | "
                    f"{_markdown_cell(', '.join(entry.on_call_rotation) or 'none')} | "
                    f"{_markdown_cell(', '.join(entry.communication_channels) or 'none')} | "
                    f"{_markdown_cell(entry.runbook_reference or 'N/A')} | "
                    f"{entry.drill_status} |"
                )

        return "\n".join(lines)


def build_plan_incident_response_matrix(source: Any) -> PlanIncidentResponseMatrix:
    """Build task-level incident response readiness for an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    rows = tuple(
        sorted(
            (row for index, task in enumerate(tasks, start=1) if (row := _task_row(task, index))),
            key=lambda row: (
                _READINESS_ORDER[row.readiness_level],
                -len(row.missing_aspects),
                row.task_id,
            ),
        )
    )
    incident_matrix = _build_incident_matrix(rows)
    return PlanIncidentResponseMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        incident_task_ids=tuple(row.task_id for row in rows),
        summary=_summary(tasks, rows, incident_matrix),
        incident_matrix=incident_matrix,
    )


def generate_plan_incident_response_matrix(source: Any) -> PlanIncidentResponseMatrix:
    """Generate an incident response readiness matrix from a plan-like source."""
    return build_plan_incident_response_matrix(source)


def analyze_plan_incident_response_matrix(source: Any) -> PlanIncidentResponseMatrix:
    """Analyze an execution plan for incident response readiness."""
    return build_plan_incident_response_matrix(source)


def derive_plan_incident_response_matrix(source: Any) -> PlanIncidentResponseMatrix:
    """Return an existing matrix or derive one from a plan-like source."""
    if isinstance(source, PlanIncidentResponseMatrix):
        return source
    return analyze_plan_incident_response_matrix(source)


def summarize_plan_incident_response_matrix(source: Any) -> PlanIncidentResponseMatrix:
    """Compatibility alias for incident response readiness summaries."""
    return derive_plan_incident_response_matrix(source)


def plan_incident_response_matrix_to_dict(
    matrix: PlanIncidentResponseMatrix,
) -> dict[str, Any]:
    """Serialize an incident response readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_incident_response_matrix_to_dict.__test__ = False


def plan_incident_response_matrix_to_dicts(
    matrix: PlanIncidentResponseMatrix,
) -> list[dict[str, Any]]:
    """Serialize incident response readiness rows to plain dictionaries."""
    return matrix.to_dicts()


plan_incident_response_matrix_to_dicts.__test__ = False


def plan_incident_response_matrix_to_markdown(
    matrix: PlanIncidentResponseMatrix,
) -> str:
    """Render an incident response readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_incident_response_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanIncidentResponseRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    present, evidence = _aspect_coverage(task)
    incident_signals = _incident_signal_count(task, present)
    if not present and not incident_signals:
        return None

    present_aspects = tuple(aspect for aspect in _ASPECT_ORDER if aspect in present)
    missing_aspects = tuple(aspect for aspect in _ASPECT_ORDER if aspect not in set(present_aspects))
    gap_reasons = tuple(_MISSING_REASON[aspect] for aspect in missing_aspects)
    incident_types = _detect_incident_types(task)
    severity_levels = _detect_severity_levels(task)
    matrix_entries = _create_matrix_entries(task, incident_types, severity_levels)

    return PlanIncidentResponseRow(
        task_id=task_id,
        title=title,
        present_aspects=present_aspects,
        missing_aspects=missing_aspects,
        readiness_level=_readiness_level(present_aspects),
        gap_reasons=gap_reasons,
        incident_types=incident_types,
        severity_levels=severity_levels,
        owner_hints=tuple(_dedupe(_owner_hints(task))),
        evidence=evidence,
        matrix_entries=matrix_entries,
    )


def _aspect_coverage(
    task: Mapping[str, Any],
) -> tuple[set[IncidentResponseAspect], tuple[str, ...]]:
    present: set[IncidentResponseAspect] = set()
    evidence: list[str] = []

    if _owner_hints(task):
        present.add("escalation_paths_clear")
        evidence.append("metadata: owner hint")

    for source_field, text in _candidate_texts(task):
        matched: list[str] = []
        for aspect, pattern in _ASPECT_PATTERNS.items():
            if pattern.search(text):
                present.add(aspect)
                matched.append(aspect)
        if matched:
            evidence.append(f"{_evidence_snippet(source_field, text)} ({', '.join(matched)})")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_RE.search(_normalized_path(path)):
            present.add("runbooks_available")
            evidence.append(f"files_or_modules: {path}")

    return present, tuple(_dedupe(evidence))


def _incident_signal_count(
    task: Mapping[str, Any],
    present: set[IncidentResponseAspect],
) -> int:
    count = len(present)
    for _, text in _candidate_texts(task):
        if _INCIDENT_RESPONSE_RE.search(text):
            count += 1
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_RE.search(_normalized_path(path)):
            count += 1
    return count


def _detect_incident_types(task: Mapping[str, Any]) -> tuple[IncidentType, ...]:
    detected: set[IncidentType] = set()
    text = " ".join(value for _, value in _candidate_texts(task))
    for incident_type, pattern in _INCIDENT_TYPE_PATTERNS.items():
        if pattern.search(text):
            detected.add(incident_type)
    return tuple(incident_type for incident_type in _INCIDENT_TYPE_ORDER if incident_type in detected)


def _detect_severity_levels(task: Mapping[str, Any]) -> tuple[SeverityLevel, ...]:
    detected: set[SeverityLevel] = set()
    text = " ".join(value for _, value in _candidate_texts(task))
    for severity in _SEVERITY_ORDER:
        if re.search(rf"\b{re.escape(severity)}\b", text, re.I):
            detected.add(severity)
    return tuple(severity for severity in _SEVERITY_ORDER if severity in detected)


def _create_matrix_entries(
    task: Mapping[str, Any],
    incident_types: tuple[IncidentType, ...],
    severity_levels: tuple[SeverityLevel, ...],
) -> tuple[IncidentResponseMatrixEntry, ...]:
    if not incident_types or not severity_levels:
        return ()

    entries: list[IncidentResponseMatrixEntry] = []
    owner_hints = _owner_hints(task)
    text = " ".join(value for _, value in _candidate_texts(task))

    # Extract communication channels
    channels: list[str] = []
    channel_terms = {
        "slack": "Slack",
        "email": "email",
        "pagerduty": "PagerDuty",
        "opsgenie": "OpsGenie",
        "status page": "status page",
        "statuspage": "status page",
    }
    for pattern, label in channel_terms.items():
        if re.search(rf"\b{re.escape(pattern)}\b", text, re.I):
            if label not in channels:
                channels.append(label)

    # Extract runbook references
    runbook_ref = None
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_RE.search(_normalized_path(path)):
            runbook_ref = path
            break

    # Determine drill status
    drill_status = "untested"
    if re.search(r"\b(?:tested|drill|game day|rehearsal|simulation)\b", text, re.I):
        drill_status = "tested"

    for incident_type in incident_types:
        for severity in severity_levels:
            entries.append(
                IncidentResponseMatrixEntry(
                    incident_type=incident_type,
                    severity_level=severity,
                    response_procedure=_extract_response_procedure(task, incident_type),
                    escalation_chain=tuple(owner_hints[:3]) if owner_hints else (),
                    on_call_rotation=tuple(owner_hints) if owner_hints else (),
                    communication_channels=tuple(channels),
                    runbook_reference=runbook_ref,
                    drill_status=drill_status,
                )
            )

    return tuple(entries)


def _extract_response_procedure(task: Mapping[str, Any], incident_type: IncidentType) -> str | None:
    description = _optional_text(task.get("description"))
    if not description:
        return None

    # Extract first sentence or up to 100 chars
    sentences = description.split(".")
    if sentences:
        procedure = sentences[0].strip()
        if len(procedure) > 100:
            procedure = procedure[:97] + "..."
        return procedure if procedure else None
    return None


def _build_incident_matrix(
    rows: tuple[PlanIncidentResponseRow, ...],
) -> tuple[IncidentResponseMatrixEntry, ...]:
    """Build aggregated incident response matrix from all task rows."""
    all_entries: list[IncidentResponseMatrixEntry] = []
    for row in rows:
        all_entries.extend(row.matrix_entries)

    # Deduplicate by (incident_type, severity_level)
    seen: set[tuple[IncidentType, SeverityLevel]] = set()
    unique_entries: list[IncidentResponseMatrixEntry] = []
    for entry in all_entries:
        key = (entry.incident_type, entry.severity_level)
        if key not in seen:
            seen.add(key)
            unique_entries.append(entry)

    return tuple(
        sorted(
            unique_entries,
            key=lambda e: (_INCIDENT_TYPE_ORDER.index(e.incident_type), _SEVERITY_ORDER.index(e.severity_level)),
        )
    )


def _readiness_level(
    present_aspects: tuple[IncidentResponseAspect, ...],
) -> IncidentResponseReadiness:
    core_aspects = {
        "severity_levels_defined",
        "response_procedures_documented",
        "escalation_paths_clear",
        "communication_protocols_set",
        "runbooks_available",
    }
    present_set = set(present_aspects)
    core_present = present_set & core_aspects

    if len(core_present) == len(core_aspects):
        return "ready"
    if core_present:
        return "partial"
    return "missing"


def _summary(
    tasks: list[dict[str, Any]],
    rows: tuple[PlanIncidentResponseRow, ...],
    incident_matrix: tuple[IncidentResponseMatrixEntry, ...],
) -> dict[str, Any]:
    # Calculate scoring metrics
    total_aspects = len(_ASPECT_ORDER)
    total_possible = total_aspects * len(rows) if rows else 1
    total_present = sum(len(row.present_aspects) for row in rows)

    # Procedure coverage: percentage of core aspects present
    core_aspects = {
        "severity_levels_defined",
        "response_procedures_documented",
        "escalation_paths_clear",
        "communication_protocols_set",
        "runbooks_available",
    }
    total_core_possible = len(core_aspects) * len(rows) if rows else 1
    total_core_present = sum(
        len(set(row.present_aspects) & core_aspects) for row in rows
    )
    procedure_coverage = (total_core_present / total_core_possible) * 100

    # Automation level: based on runbook availability and documentation
    automation_aspects = {"runbooks_available", "response_procedures_documented"}
    total_automation_possible = len(automation_aspects) * len(rows) if rows else 1
    total_automation_present = sum(
        len(set(row.present_aspects) & automation_aspects) for row in rows
    )
    automation_level = (total_automation_present / total_automation_possible) * 100

    # Team preparedness: based on escalation and communication
    preparedness_aspects = {
        "escalation_paths_clear",
        "communication_protocols_set",
        "untested_procedures_flagged",
    }
    total_prep_possible = len(preparedness_aspects) * len(rows) if rows else 1
    total_prep_present = sum(
        len(set(row.present_aspects) & preparedness_aspects) for row in rows
    )
    team_preparedness = (total_prep_present / total_prep_possible) * 100

    # Documentation quality: based on severity definitions and gap identification
    doc_aspects = {
        "severity_definitions_clear",
        "missing_runbooks_identified",
        "incomplete_escalation_detected",
    }
    total_doc_possible = len(doc_aspects) * len(rows) if rows else 1
    total_doc_present = sum(
        len(set(row.present_aspects) & doc_aspects) for row in rows
    )
    documentation_quality = (total_doc_present / total_doc_possible) * 100

    # Overall score (weighted)
    overall_score = (
        procedure_coverage * 0.30
        + automation_level * 0.20
        + team_preparedness * 0.25
        + documentation_quality * 0.25
    )

    return {
        "task_count": len(tasks),
        "incident_task_count": len(rows),
        "ready_task_count": sum(1 for row in rows if row.readiness_level == "ready"),
        "partial_task_count": sum(1 for row in rows if row.readiness_level == "partial"),
        "missing_task_count": sum(1 for row in rows if row.readiness_level == "missing"),
        "gap_count": sum(len(row.missing_aspects) for row in rows),
        "matrix_entry_count": len(incident_matrix),
        "readiness_counts": {
            readiness: sum(1 for row in rows if row.readiness_level == readiness)
            for readiness in _READINESS_ORDER
        },
        "aspect_coverage_counts": {
            aspect: sum(1 for row in rows if aspect in row.present_aspects)
            for aspect in _ASPECT_ORDER
        },
        "missing_aspect_counts": {
            aspect: sum(1 for row in rows if aspect in row.missing_aspects)
            for aspect in _ASPECT_ORDER
        },
        "incident_type_counts": {
            incident_type: sum(
                1 for row in rows if incident_type in row.incident_types
            )
            for incident_type in _INCIDENT_TYPE_ORDER
        },
        "severity_level_counts": {
            severity: sum(1 for row in rows if severity in row.severity_levels)
            for severity in _SEVERITY_ORDER
        },
        "scoring": {
            "procedure_coverage": procedure_coverage,
            "automation_level": automation_level,
            "team_preparedness": team_preparedness,
            "documentation_quality": documentation_quality,
            "overall_score": overall_score,
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
    "IncidentResponseAspect",
    "IncidentResponseMatrixEntry",
    "IncidentResponseReadiness",
    "IncidentType",
    "PlanIncidentResponseMatrix",
    "PlanIncidentResponseRow",
    "SeverityLevel",
    "analyze_plan_incident_response_matrix",
    "build_plan_incident_response_matrix",
    "derive_plan_incident_response_matrix",
    "generate_plan_incident_response_matrix",
    "plan_incident_response_matrix_to_dict",
    "plan_incident_response_matrix_to_dicts",
    "plan_incident_response_matrix_to_markdown",
    "summarize_plan_incident_response_matrix",
]
