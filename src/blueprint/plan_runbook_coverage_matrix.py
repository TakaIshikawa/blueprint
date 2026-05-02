"""Build plan-level operational runbook coverage matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief


OperationalScenarioCategory = Literal[
    "deploy",
    "rollback",
    "incident_response",
    "data_repair",
    "support_triage",
    "on_call_handoff",
    "dependency_outage",
    "customer_communication",
]
CoverageStatus = Literal["missing", "partial", "covered"]

_CATEGORY_ORDER: tuple[OperationalScenarioCategory, ...] = (
    "deploy",
    "rollback",
    "incident_response",
    "data_repair",
    "support_triage",
    "on_call_handoff",
    "dependency_outage",
    "customer_communication",
)
_SPACE_RE = re.compile(r"\s+")
_DEPLOY_RE = re.compile(
    r"\b(?:deploy|deployment|release|rollout|production rollout|canary|"
    r"blue[- ]green|ship to production|launch)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|downgrade|disable flag|abort rollout|"
    r"kill switch|backout)\b",
    re.I,
)
_INCIDENT_RE = re.compile(
    r"\b(?:incident|sev[ -]?[0-9]|outage response|degradation|postmortem|hotfix|"
    r"emergency fix|service restoration)\b",
    re.I,
)
_DATA_REPAIR_RE = re.compile(
    r"\b(?:data repair|backfill|back-fill|reprocess|replay|bulk update|batch repair|"
    r"reconciliation|repair records|historical data)\b",
    re.I,
)
_SUPPORT_TRIAGE_RE = re.compile(
    r"\b(?:support triage|triage|support escalation|ticket|helpdesk|customer support|"
    r"support queue|diagnostic steps|troubleshooting)\b",
    re.I,
)
_ON_CALL_HANDOFF_RE = re.compile(
    r"\b(?:on-call handoff|on call handoff|handoff|on-call|on call|pager|primary responder|"
    r"secondary responder|escalation policy|support rotation)\b",
    re.I,
)
_DEPENDENCY_OUTAGE_RE = re.compile(
    r"\b(?:dependency outage|vendor outage|third[- ]party outage|external service outage|"
    r"partner outage|api outage|vendor degradation|rate limit|fallback|circuit breaker|"
    r"stripe|salesforce|slack api)\b",
    re.I,
)
_CUSTOMER_COMMUNICATION_RE = re.compile(
    r"\b(?:customer communication|customer comms|status page|customer notice|"
    r"stakeholder update|support macro|email customers|customer impact|customer-facing update|"
    r"incident communication)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[OperationalScenarioCategory, re.Pattern[str]] = {
    "deploy": _DEPLOY_RE,
    "rollback": _ROLLBACK_RE,
    "incident_response": _INCIDENT_RE,
    "data_repair": _DATA_REPAIR_RE,
    "support_triage": _SUPPORT_TRIAGE_RE,
    "on_call_handoff": _ON_CALL_HANDOFF_RE,
    "dependency_outage": _DEPENDENCY_OUTAGE_RE,
    "customer_communication": _CUSTOMER_COMMUNICATION_RE,
}
_RECOMMENDED_SECTIONS: dict[OperationalScenarioCategory, tuple[str, ...]] = {
    "deploy": ("pre_checks", "execution_steps", "monitoring", "rollback", "post_checks"),
    "rollback": ("rollback", "monitoring", "escalation", "post_checks"),
    "incident_response": ("monitoring", "escalation", "customer_communication", "post_checks"),
    "data_repair": ("pre_checks", "execution_steps", "monitoring", "rollback", "post_checks"),
    "support_triage": ("triage_steps", "escalation", "customer_communication"),
    "on_call_handoff": ("handoff", "monitoring", "escalation"),
    "dependency_outage": ("monitoring", "fallback", "escalation", "customer_communication"),
    "customer_communication": ("customer_communication", "escalation", "post_checks"),
}
_SECTION_PATTERNS: dict[str, re.Pattern[str]] = {
    "pre_checks": re.compile(r"\b(?:pre[- ]?checks?|readiness checks?|before rollout)\b", re.I),
    "execution_steps": re.compile(
        r"\b(?:execution steps?|run steps?|operating steps?|procedure)\b", re.I
    ),
    "monitoring": re.compile(r"\b(?:monitoring|dashboard|metrics?|alerts?|health checks?)\b", re.I),
    "rollback": _ROLLBACK_RE,
    "escalation": re.compile(
        r"\b(?:escalation|escalate|owner|decision maker|incident commander)\b", re.I
    ),
    "post_checks": re.compile(
        r"\b(?:post[- ]?checks?|validation checks?|verify after|sign[- ]?off)\b", re.I
    ),
    "triage_steps": re.compile(
        r"\b(?:triage steps?|diagnostic steps?|troubleshooting|support playbook)\b", re.I
    ),
    "handoff": re.compile(r"\b(?:handoff|shift change|next responder|current state)\b", re.I),
    "fallback": re.compile(
        r"\b(?:fallback|circuit breaker|degraded mode|failover|disable integration)\b", re.I
    ),
    "customer_communication": _CUSTOMER_COMMUNICATION_RE,
}
_PLAN_FIELDS = (
    "target_repo",
    "project_type",
    "test_strategy",
    "handoff_prompt",
    "milestones",
    "metadata",
)
_BRIEF_FIELDS = (
    "title",
    "workflow_context",
    "problem_statement",
    "mvp_goal",
    "scope",
    "assumptions",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "validation_plan",
    "definition_of_done",
)


@dataclass(frozen=True, slots=True)
class PlanRunbookCoverageRow:
    """Coverage row for one operational scenario across the plan."""

    category: OperationalScenarioCategory
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    coverage_status: CoverageStatus = "missing"
    missing_sections: tuple[str, ...] = field(default_factory=tuple)
    recommended_runbook_sections: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "affected_task_ids": list(self.affected_task_ids),
            "evidence": list(self.evidence),
            "coverage_status": self.coverage_status,
            "missing_sections": list(self.missing_sections),
            "recommended_runbook_sections": list(self.recommended_runbook_sections),
        }


@dataclass(frozen=True, slots=True)
class PlanRunbookCoverageMatrix:
    """Plan-level operational runbook coverage matrix."""

    plan_id: str | None = None
    implementation_brief_id: str | None = None
    rows: tuple[PlanRunbookCoverageRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanRunbookCoverageRow, ...]:
        """Compatibility view matching report-style modules."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "implementation_brief_id": self.implementation_brief_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
            "records": [row.to_dict() for row in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Runbook Coverage Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Operational scenarios: {self.summary.get('scenario_count', 0)}",
            f"- Covered: {self.summary.get('covered_count', 0)}",
            f"- Partial: {self.summary.get('partial_count', 0)}",
            f"- Missing: {self.summary.get('missing_count', 0)}",
            "- Scenario counts: "
            + ", ".join(
                f"{category} {self.summary.get('category_counts', {}).get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
        ]
        if not self.rows:
            lines.extend(["", "No operational runbook scenarios were found in the plan."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Scenarios",
                "",
                "| Scenario | Status | Affected Tasks | Missing Sections | Recommended Sections | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.category} | "
                f"{row.coverage_status} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_sections) or 'none')} | "
                f"{_markdown_cell(', '.join(row.recommended_runbook_sections))} | "
                f"{_markdown_cell('; '.join(row.evidence))} |"
            )
        return "\n".join(lines)


def build_plan_runbook_coverage_matrix(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> PlanRunbookCoverageMatrix:
    """Build a plan-level matrix of operational runbook coverage."""
    plan_id, implementation_brief_id, plan_payload, tasks = _plan_payload(plan)
    brief_id, brief_payload = _brief_payload(brief)
    implementation_brief_id = implementation_brief_id or brief_id

    buckets: dict[OperationalScenarioCategory, _Bucket] = {}
    for segment in _plan_segments(plan_payload):
        _collect_segment(buckets, segment)
    for segment in _brief_segments(brief_payload):
        _collect_segment(buckets, segment)
    for task in tasks:
        task_id = _optional_text(task.get("id"))
        for segment in _task_segments(task, task_id):
            _collect_segment(buckets, segment)

    rows = tuple(
        _row(category, buckets[category]) for category in _CATEGORY_ORDER if category in buckets
    )
    return PlanRunbookCoverageMatrix(
        plan_id=plan_id,
        implementation_brief_id=implementation_brief_id,
        rows=rows,
        summary=_summary(rows),
    )


def generate_plan_runbook_coverage_matrix(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> PlanRunbookCoverageMatrix:
    """Compatibility helper for callers that use generate_* naming."""
    return build_plan_runbook_coverage_matrix(plan, brief)


def extract_plan_runbook_coverage_rows(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> tuple[PlanRunbookCoverageRow, ...]:
    """Return runbook coverage rows extracted from plan-shaped input."""
    return build_plan_runbook_coverage_matrix(plan, brief).rows


def summarize_plan_runbook_coverage_matrix(
    plan_or_matrix: Mapping[str, Any] | ExecutionPlan | PlanRunbookCoverageMatrix | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> dict[str, Any]:
    """Return the deterministic runbook coverage summary."""
    if isinstance(plan_or_matrix, PlanRunbookCoverageMatrix):
        return dict(plan_or_matrix.summary)
    return build_plan_runbook_coverage_matrix(plan_or_matrix, brief).summary


def plan_runbook_coverage_matrix_to_dict(matrix: PlanRunbookCoverageMatrix) -> dict[str, Any]:
    """Serialize a plan runbook coverage matrix to a plain dictionary."""
    return matrix.to_dict()


plan_runbook_coverage_matrix_to_dict.__test__ = False


def plan_runbook_coverage_matrix_to_dicts(
    rows: (
        tuple[PlanRunbookCoverageRow, ...]
        | list[PlanRunbookCoverageRow]
        | PlanRunbookCoverageMatrix
    ),
) -> list[dict[str, Any]]:
    """Serialize plan runbook coverage rows to dictionaries."""
    if isinstance(rows, PlanRunbookCoverageMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_runbook_coverage_matrix_to_dicts.__test__ = False


def plan_runbook_coverage_matrix_to_markdown(matrix: PlanRunbookCoverageMatrix) -> str:
    """Render a plan runbook coverage matrix as Markdown."""
    return matrix.to_markdown()


plan_runbook_coverage_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    task_id: str | None = None


@dataclass(slots=True)
class _Bucket:
    task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    covered_sections: set[str] = field(default_factory=set)


def _plan_payload(
    source: Mapping[str, Any] | ExecutionPlan | object,
) -> tuple[str | None, str | None, dict[str, Any], list[dict[str, Any]]]:
    payload: dict[str, Any]
    if isinstance(source, ExecutionPlan):
        payload = dict(source.model_dump(mode="python"))
    elif hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        try:
            payload = dict(ExecutionPlan.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
    else:
        payload = _object_payload(source)

    tasks_value = payload.get("tasks")
    tasks: list[dict[str, Any]] = []
    if isinstance(tasks_value, list):
        for item in tasks_value:
            normalized = _task_payload(item)
            if normalized:
                tasks.append(normalized)
    return (
        _optional_text(payload.get("id")),
        _optional_text(payload.get("implementation_brief_id")),
        payload,
        tasks,
    )


def _brief_payload(
    source: Mapping[str, Any] | ImplementationBrief | object | None,
) -> tuple[str | None, dict[str, Any]]:
    if source is None:
        return None, {}
    if isinstance(source, ImplementationBrief):
        payload = dict(source.model_dump(mode="python"))
        return _optional_text(payload.get("id")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _optional_text(payload.get("id")), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(ImplementationBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _optional_text(payload.get("id")), payload
    payload = _object_payload(source)
    return _optional_text(payload.get("id")), payload


def _task_payload(source: Any) -> dict[str, Any]:
    if isinstance(source, ExecutionTask):
        return dict(source.model_dump(mode="python"))
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        try:
            return dict(ExecutionTask.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            return dict(source)
    return _object_payload(source)


def _plan_segments(payload: Mapping[str, Any]) -> Iterable[_Segment]:
    for field_name in _PLAN_FIELDS:
        if field_name in payload:
            yield from _value_segments(f"plan.{field_name}", payload[field_name], None)


def _brief_segments(payload: Mapping[str, Any]) -> Iterable[_Segment]:
    for field_name in _BRIEF_FIELDS:
        if field_name in payload:
            yield from _value_segments(f"brief.{field_name}", payload[field_name], None)


def _task_segments(task: Mapping[str, Any], task_id: str | None) -> Iterable[_Segment]:
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        source_field = "task.files_or_modules"
        yield _Segment(source_field, path, task_id)
    for field_name in (
        "title",
        "description",
        "acceptance_criteria",
        "test_command",
        "blocked_reason",
        "metadata",
    ):
        if field_name in task:
            yield from _value_segments(f"task.{field_name}", task[field_name], task_id)


def _value_segments(source_field: str, value: Any, task_id: str | None) -> Iterable[_Segment]:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            yield from _value_segments(f"{source_field}.{key}", value[key], task_id)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            yield from _value_segments(f"{source_field}[{index}]", item, task_id)
        return
    if text := _optional_text(value):
        yield _Segment(source_field, text, task_id)


def _collect_segment(
    buckets: dict[OperationalScenarioCategory, _Bucket],
    segment: _Segment,
) -> None:
    categories = _segment_categories(segment)
    if not categories:
        return
    sections = _covered_sections(segment)
    evidence = _evidence_snippet(segment)
    for category in categories:
        bucket = buckets.setdefault(category, _Bucket())
        if segment.task_id:
            _append_unique(bucket.task_ids, segment.task_id)
        _append_unique(bucket.evidence, evidence)
        bucket.covered_sections.update(
            section for section in sections if section in _RECOMMENDED_SECTIONS[category]
        )


def _segment_categories(segment: _Segment) -> tuple[OperationalScenarioCategory, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    path_category = _path_category(segment)
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable) or category == path_category
    ]
    return tuple(categories)


def _path_category(segment: _Segment) -> OperationalScenarioCategory | None:
    if segment.source_field != "task.files_or_modules":
        return None
    folded = _normalized_path(segment.text).casefold()
    if not folded:
        return None
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    if {"deploy", "deploys", "deployment", "deployments", "release", "releases"} & parts:
        return "deploy"
    if {"rollback", "rollbacks", "revert"} & parts or "rollback" in name:
        return "rollback"
    if {"incidents", "incident", "runbooks", "hotfix"} & parts:
        return "incident_response"
    if {"backfills", "backfill", "data_repairs", "repairs"} & parts:
        return "data_repair"
    if {"support", "triage", "helpdesk"} & parts:
        return "support_triage"
    if {"oncall", "on-call", "handoff"} & parts:
        return "on_call_handoff"
    if {"integrations", "vendors", "partners", "webhooks", "external"} & parts:
        return "dependency_outage"
    if {"comms", "communications", "status-page", "status_page"} & parts:
        return "customer_communication"
    return None


def _covered_sections(segment: _Segment) -> set[str]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    return {section for section, pattern in _SECTION_PATTERNS.items() if pattern.search(searchable)}


def _row(category: OperationalScenarioCategory, bucket: _Bucket) -> PlanRunbookCoverageRow:
    recommended = _RECOMMENDED_SECTIONS[category]
    covered = {section for section in bucket.covered_sections if section in recommended}
    missing = tuple(section for section in recommended if section not in covered)
    if not covered:
        status: CoverageStatus = "missing"
    elif missing:
        status = "partial"
    else:
        status = "covered"
    return PlanRunbookCoverageRow(
        category=category,
        affected_task_ids=tuple(bucket.task_ids),
        evidence=tuple(bucket.evidence[:8]),
        coverage_status=status,
        missing_sections=missing,
        recommended_runbook_sections=recommended,
    )


def _summary(rows: tuple[PlanRunbookCoverageRow, ...]) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    for row in rows:
        category_counts[row.category] = 1
    status_counts = {
        "missing": sum(1 for row in rows if row.coverage_status == "missing"),
        "partial": sum(1 for row in rows if row.coverage_status == "partial"),
        "covered": sum(1 for row in rows if row.coverage_status == "covered"),
    }
    return {
        "scenario_count": len(rows),
        "covered_count": status_counts["covered"],
        "partial_count": status_counts["partial"],
        "missing_count": status_counts["missing"],
        "category_counts": category_counts,
        "categories": [row.category for row in rows],
        "affected_task_ids": list(
            _dedupe(task_id for row in rows for task_id in row.affected_task_ids)
        ),
    }


def _object_payload(source: object) -> dict[str, Any]:
    if source is None or isinstance(source, (str, bytes, bytearray)):
        return {}
    payload: dict[str, Any] = {}
    for name in dir(source):
        if name.startswith("_"):
            continue
        try:
            value = getattr(source, name)
        except Exception:
            continue
        if callable(value):
            continue
        payload[name] = value
    return payload


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return tuple(text for item in items if (text := _optional_text(item)))
    return ()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip("/")


def _field_words(value: str) -> str:
    return value.replace("_", " ").replace(".", " ").replace("[", " ").replace("]", " ")


def _evidence_snippet(segment: _Segment) -> str:
    prefix = segment.source_field
    if segment.task_id:
        prefix = f"{segment.task_id}.{prefix}"
    return f"{prefix}: {segment.text}"


def _append_unique(items: list[str], item: str) -> None:
    if item not in items:
        items.append(item)


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
