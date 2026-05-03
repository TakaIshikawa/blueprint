"""Build plan-level go-live support coverage matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


GoLiveSupportArea = Literal[
    "launch_support_owner",
    "on_call_coverage",
    "support_macros",
    "escalation_routes",
    "customer_success_handoff",
    "status_page_updates",
    "runbook_links",
    "office_hours",
    "known_issue_triage",
    "post_launch_monitoring",
]
CoverageStatus = Literal["complete", "partial", "missing"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_AREA_ORDER: tuple[GoLiveSupportArea, ...] = (
    "launch_support_owner",
    "on_call_coverage",
    "support_macros",
    "escalation_routes",
    "customer_success_handoff",
    "status_page_updates",
    "runbook_links",
    "office_hours",
    "known_issue_triage",
    "post_launch_monitoring",
)
_STATUS_ORDER: tuple[CoverageStatus, ...] = ("complete", "partial", "missing")
_LAUNCH_RE = re.compile(
    r"\b(?:go[- ]live|launch|rollout|roll out|release|deploy|deployment|production|"
    r"canary|beta|early access|feature flag|ramp|wave|cutover|ship)\b",
    re.I,
)
_CUSTOMER_IMPACT_RE = re.compile(
    r"\b(?:customer[- ]facing|customer[- ]visible|user[- ]facing|user[- ]visible|"
    r"customers?|users?|admins?|tenant|dashboard|ui|checkout|billing|invoice|"
    r"subscription|payment|email|notification|status page|support|cs|customer success)\b",
    re.I,
)
_NO_IMPACT_RE = re.compile(
    r"\b(?:internal only|no customer impact|non[- ]customer|refactor|test fixture|"
    r"copy only|documentation only|readme|comment[- ]only|style[- ]only)\b",
    re.I,
)
_OWNER_KEY_RE = re.compile(
    r"\b(?:owner|owners|dri|team|lead|assignee|oncall|on-call|support|cs|customer_success)\b",
    re.I,
)
_AREA_PATTERNS: dict[GoLiveSupportArea, re.Pattern[str]] = {
    "launch_support_owner": re.compile(
        r"\b(?:launch support owner|launch owner|support owner|support dri|launch dri|"
        r"go[- ]live owner|release captain|launch captain|support lead)\b",
        re.I,
    ),
    "on_call_coverage": re.compile(
        r"\b(?:on[- ]call|oncall|pager|pagerduty|opsgenie|coverage window|watch window|"
        r"launch coverage|support coverage|war room|launch watch)\b",
        re.I,
    ),
    "support_macros": re.compile(
        r"\b(?:support macros?|macros?|canned responses?|agent scripts?|ticket snippets?|"
        r"zendesk macros?|intercom macros?)\b",
        re.I,
    ),
    "escalation_routes": re.compile(
        r"\b(?:escalation routes?|escalation path|escalation policy|escalate to|"
        r"tier ?2|tier ?3|incident commander|support escalation)\b",
        re.I,
    ),
    "customer_success_handoff": re.compile(
        r"\b(?:customer success handoff|cs handoff|csm handoff|success handoff|"
        r"account team handoff|customer success notes?|csm notes?)\b",
        re.I,
    ),
    "status_page_updates": re.compile(
        r"\b(?:status page|statuspage|public status|incident update|maintenance notice|"
        r"service status|customer status update)\b",
        re.I,
    ),
    "runbook_links": re.compile(
        r"\b(?:runbook|playbook|operational guide|ops guide|launch checklist|"
        r"troubleshooting guide|diagnostic steps?)\b",
        re.I,
    ),
    "office_hours": re.compile(
        r"\b(?:office hours|office-hours|support hours|drop[- ]in|open clinic|"
        r"support clinic|customer clinic)\b",
        re.I,
    ),
    "known_issue_triage": re.compile(
        r"\b(?:known issues?|known limitations?|known issue triage|issue triage|"
        r"triage queue|bug triage|workarounds?|support caveats?)\b",
        re.I,
    ),
    "post_launch_monitoring": re.compile(
        r"\b(?:post[- ]launch monitoring|post launch monitoring|launch monitoring|"
        r"monitoring|dashboard|metrics?|alerts?|slo|telemetry|health checks?)\b",
        re.I,
    ),
}
_OWNER_DEFAULTS: dict[GoLiveSupportArea, str] = {
    "launch_support_owner": "Launch support lead",
    "on_call_coverage": "On-call manager",
    "support_macros": "Support operations",
    "escalation_routes": "Support escalation owner",
    "customer_success_handoff": "Customer success lead",
    "status_page_updates": "Incident communications owner",
    "runbook_links": "Operations owner",
    "office_hours": "Customer success lead",
    "known_issue_triage": "Support triage owner",
    "post_launch_monitoring": "Launch monitoring owner",
}
_MISSING_TEXT: dict[GoLiveSupportArea, str] = {
    "launch_support_owner": "Assign a launch support owner or DRI.",
    "on_call_coverage": "Define on-call or launch-watch coverage.",
    "support_macros": "Prepare support macros or agent scripts.",
    "escalation_routes": "Document escalation routes for launch issues.",
    "customer_success_handoff": "Hand off customer success context and account guidance.",
    "status_page_updates": "Plan status page or customer status updates.",
    "runbook_links": "Link the launch support runbook or playbook.",
    "office_hours": "Schedule office hours or a support clinic.",
    "known_issue_triage": "Document known issue triage and workarounds.",
    "post_launch_monitoring": "Define post-launch monitoring dashboards, alerts, or checks.",
}


@dataclass(frozen=True, slots=True)
class PlanGoLiveSupportCoverageRow:
    """Go-live support coverage for one launch-facing task."""

    task_id: str
    task_title: str = ""
    coverage_status: CoverageStatus = "missing"
    present_coverage: tuple[GoLiveSupportArea, ...] = field(default_factory=tuple)
    missing_coverage: tuple[GoLiveSupportArea, ...] = field(default_factory=tuple)
    missing_coverage_flags: tuple[str, ...] = field(default_factory=tuple)
    team_owner_hints: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "coverage_status": self.coverage_status,
            "present_coverage": list(self.present_coverage),
            "missing_coverage": list(self.missing_coverage),
            "missing_coverage_flags": list(self.missing_coverage_flags),
            "team_owner_hints": list(self.team_owner_hints),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanGoLiveSupportCoverageMatrix:
    """Plan-level go-live support coverage matrix."""

    plan_id: str | None = None
    rows: tuple[PlanGoLiveSupportCoverageRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanGoLiveSupportCoverageRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return coverage rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the go-live support coverage matrix as deterministic Markdown."""
        title = "# Plan Go-Live Support Coverage Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        area_counts = self.summary.get("support_area_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Launch-facing tasks: {self.summary.get('launch_facing_task_count', 0)}",
            f"- Complete coverage: {self.summary.get('complete_count', 0)}",
            f"- Partial coverage: {self.summary.get('partial_count', 0)}",
            f"- Missing coverage: {self.summary.get('missing_count', 0)}",
            "- Support area counts: "
            + ", ".join(f"{area} {area_counts.get(area, 0)}" for area in _AREA_ORDER),
            "- No-impact tasks: "
            + _markdown_cell(", ".join(self.summary.get("no_impact_task_ids", [])) or "none"),
        ]
        if not self.rows:
            lines.extend(["", "No launch-facing tasks were found for go-live support coverage."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task ID | Status | Present Coverage | Missing Coverage | Team/Owner Hints | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{row.coverage_status} | "
                f"{_markdown_cell(', '.join(row.present_coverage) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_coverage) or 'none')} | "
                f"{_markdown_cell('; '.join(row.team_owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_go_live_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanGoLiveSupportCoverageMatrix:
    """Build a task-level go-live support coverage matrix for an execution plan."""
    plan_id, plan_context, tasks = _source_payload(source)
    rows: list[PlanGoLiveSupportCoverageRow] = []
    no_impact_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        title = _optional_text(task.get("title")) or ""
        signals = _task_signals(task, plan_context)
        if not signals.launch_facing:
            no_impact_task_ids.append(task_id)
            continue
        rows.append(_row(task_id, title, signals))

    status_counts = {
        status: sum(1 for row in rows if row.coverage_status == status)
        for status in _STATUS_ORDER
    }
    support_area_counts = {
        area: sum(1 for row in rows if area in row.present_coverage) for area in _AREA_ORDER
    }
    missing_support_area_counts = {
        area: sum(1 for row in rows if area in row.missing_coverage) for area in _AREA_ORDER
    }
    return PlanGoLiveSupportCoverageMatrix(
        plan_id=plan_id,
        rows=tuple(rows),
        summary={
            "total_task_count": len(tasks),
            "launch_facing_task_count": len(rows),
            "no_impact_task_ids": list(_dedupe(no_impact_task_ids)),
            "support_area_counts": support_area_counts,
            "missing_support_area_counts": missing_support_area_counts,
            "complete_count": status_counts["complete"],
            "partial_count": status_counts["partial"],
            "missing_count": status_counts["missing"],
            "status_counts": status_counts,
            "missing_coverage_flag_count": sum(len(row.missing_coverage_flags) for row in rows),
            "launch_facing_task_ids": [row.task_id for row in rows],
        },
    )


def generate_plan_go_live_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanGoLiveSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanGoLiveSupportCoverageMatrix:
    """Compatibility alias for building go-live support coverage matrices."""
    if isinstance(source, PlanGoLiveSupportCoverageMatrix):
        return source
    return build_plan_go_live_support_coverage_matrix(source)


def derive_plan_go_live_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanGoLiveSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanGoLiveSupportCoverageMatrix:
    """Compatibility alias for deriving go-live support coverage matrices."""
    return generate_plan_go_live_support_coverage_matrix(source)


def summarize_plan_go_live_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanGoLiveSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanGoLiveSupportCoverageMatrix:
    """Compatibility alias for summarizing go-live support coverage matrices."""
    return derive_plan_go_live_support_coverage_matrix(source)


def plan_go_live_support_coverage_matrix_to_dict(
    matrix: PlanGoLiveSupportCoverageMatrix,
) -> dict[str, Any]:
    """Serialize a go-live support coverage matrix to a plain dictionary."""
    return matrix.to_dict()


plan_go_live_support_coverage_matrix_to_dict.__test__ = False


def plan_go_live_support_coverage_matrix_to_markdown(
    matrix: PlanGoLiveSupportCoverageMatrix,
) -> str:
    """Render a go-live support coverage matrix as Markdown."""
    return matrix.to_markdown()


plan_go_live_support_coverage_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskSignals:
    launch_facing: bool = False
    area_evidence: dict[GoLiveSupportArea, tuple[str, ...]] = field(default_factory=dict)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)
    launch_evidence: tuple[str, ...] = field(default_factory=tuple)


def _row(
    task_id: str,
    title: str,
    signals: _TaskSignals,
) -> PlanGoLiveSupportCoverageRow:
    present = tuple(area for area in _AREA_ORDER if signals.area_evidence.get(area))
    missing = tuple(area for area in _AREA_ORDER if area not in present)
    if not present:
        status: CoverageStatus = "missing"
    elif not missing:
        status = "complete"
    else:
        status = "partial"
    evidence = list(signals.launch_evidence)
    for area in present:
        evidence.extend(signals.area_evidence[area])
    return PlanGoLiveSupportCoverageRow(
        task_id=task_id,
        task_title=title,
        coverage_status=status,
        present_coverage=present,
        missing_coverage=missing,
        missing_coverage_flags=tuple(_MISSING_TEXT[area] for area in missing),
        team_owner_hints=tuple(_dedupe(signals.owner_hints))
        or tuple(_OWNER_DEFAULTS[area] for area in missing[:1]),
        evidence=tuple(_dedupe(evidence)),
    )


def _task_signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> _TaskSignals:
    area_evidence: dict[GoLiveSupportArea, list[str]] = {}
    owner_hints: list[str] = []
    launch_evidence: list[str] = []
    launch_hit = False
    customer_hit = False
    no_impact_hit = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        text = _normalized_path(path).replace("/", " ").replace("_", " ").replace("-", " ")
        evidence = f"files_or_modules: {path}"
        launch_hit = launch_hit or bool(_LAUNCH_RE.search(text))
        customer_hit = customer_hit or bool(_CUSTOMER_IMPACT_RE.search(text))
        no_impact_hit = no_impact_hit or bool(_NO_IMPACT_RE.search(text))
        for area, pattern in _AREA_PATTERNS.items():
            if pattern.search(text):
                _append(area_evidence, area, evidence)

    for source_field, text in (*_candidate_texts(task), *plan_context):
        evidence = _evidence_snippet(source_field, text)
        if _LAUNCH_RE.search(text):
            launch_hit = True
            launch_evidence.append(evidence)
        if _CUSTOMER_IMPACT_RE.search(text):
            customer_hit = True
        if _NO_IMPACT_RE.search(text):
            no_impact_hit = True
        if _OWNER_KEY_RE.search(source_field.replace("_", " ")) or _OWNER_KEY_RE.search(text):
            owner_hints.extend(_owner_hints(text))
        for area, pattern in _AREA_PATTERNS.items():
            if pattern.search(text):
                _append(area_evidence, area, evidence)

    launch_facing = (launch_hit and customer_hit) or bool(area_evidence)
    if no_impact_hit and not area_evidence:
        launch_facing = False
    return _TaskSignals(
        launch_facing=launch_facing,
        area_evidence={area: tuple(_dedupe(values)) for area, values in area_evidence.items()},
        owner_hints=tuple(_dedupe(owner_hints)),
        launch_evidence=tuple(_dedupe(launch_evidence)),
    )


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return (
                _optional_text(payload.get("id")),
                tuple(_plan_context(payload)),
                _task_payloads(payload.get("tasks")),
            )
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            _task_payloads(payload.get("tasks")),
        )

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []

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
    return None, (), tasks


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


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields = (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "generation_prompt",
        "risk",
    )
    texts: list[tuple[str, str]] = []
    for field_name in fields:
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "risks", "acceptance_criteria", "metadata", "implementation_brief", "brief"):
        for source_field, text in _metadata_texts(plan.get(field_name), prefix=field_name):
            texts.append((source_field, text))
    return texts


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
        "owner_role",
        "owner",
        "assignee",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
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
        "milestones",
        "implementation_brief",
        "brief",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "owner_role",
        "owner",
        "assignee",
        "suggested_engine",
        "risk",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
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
    return tuple(texts)


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
                if any(pattern.search(key_text) for pattern in _AREA_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _AREA_PATTERNS.values()):
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


def _owner_hints(text: str) -> tuple[str, ...]:
    return (_clean_text(text)[:120].rstrip(),) if _optional_text(text) else ()


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
    text = _clean_text(str(value))
    return [text] if text else []


def _append(
    values: dict[GoLiveSupportArea, list[str]],
    key: GoLiveSupportArea,
    value: str,
) -> None:
    values.setdefault(key, []).append(value)


def _dedupe(items: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    result: list[_T] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_snippet(text)}"


def _snippet(text: str, limit: int = 180) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "CoverageStatus",
    "GoLiveSupportArea",
    "PlanGoLiveSupportCoverageMatrix",
    "PlanGoLiveSupportCoverageRow",
    "build_plan_go_live_support_coverage_matrix",
    "derive_plan_go_live_support_coverage_matrix",
    "generate_plan_go_live_support_coverage_matrix",
    "plan_go_live_support_coverage_matrix_to_dict",
    "plan_go_live_support_coverage_matrix_to_markdown",
    "summarize_plan_go_live_support_coverage_matrix",
]
