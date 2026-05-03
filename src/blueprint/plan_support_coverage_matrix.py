"""Evaluate support coverage readiness across execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SupportCoverageArea = Literal[
    "support_docs",
    "support_macros",
    "internal_faq",
    "support_tooling",
    "triage_ownership",
    "launch_staffing",
    "known_issues",
    "customer_communication_handoff",
]
SupportCoverageStatus = Literal["ready", "partial", "missing"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_AREA_ORDER: tuple[SupportCoverageArea, ...] = (
    "support_docs",
    "support_macros",
    "internal_faq",
    "support_tooling",
    "triage_ownership",
    "launch_staffing",
    "known_issues",
    "customer_communication_handoff",
)
_STATUS_ORDER: tuple[SupportCoverageStatus, ...] = ("ready", "partial", "missing")

_CUSTOMER_IMPACT_RE = re.compile(
    r"\b(?:customer[- ]facing|customer[- ]visible|user[- ]facing|user[- ]visible|"
    r"end users?|customers?|admins?|dashboard|ui|ux|checkout|billing|invoice|"
    r"subscription|payment|email|notification|message|release|launch|rollout|"
    r"migrat(?:e|ion)|existing accounts?|existing customers?|self[- ]serve|"
    r"settings|permissions?|rbac|workflow|onboarding)\b",
    re.I,
)
_ROLL_OUT_RE = re.compile(
    r"\b(?:launch|rollout|roll out|release|deploy|deployment|canary|beta|"
    r"early access|feature flag|flagged)\b",
    re.I,
)
_SUPPORT_TOOLING_RE = re.compile(
    r"\b(?:support tool(?:ing)?|support console|agent console|helpdesk|ticket queue|"
    r"ticket routing|zendesk|intercom|case view|support dashboard)\b",
    re.I,
)
_OWNER_KEY_RE = re.compile(r"\b(?:owner|dri|responsible|team|lead|assignee|oncall|on-call)\b", re.I)
_READY_RE = re.compile(
    r"\b(?:ready|done|complete|completed|published|linked|approved|documented|"
    r"staffed|assigned|owner|owned|owns|covered|prepared|created)\b",
    re.I,
)

_AREA_PATTERNS: dict[SupportCoverageArea, re.Pattern[str]] = {
    "support_docs": re.compile(
        r"\b(?:support docs?|support documentation|support guide|help center|"
        r"help article|knowledge base|kb article|troubleshooting docs?)\b",
        re.I,
    ),
    "support_macros": re.compile(
        r"\b(?:support macros?|macros?|canned responses?|agent scripts?|"
        r"ticket snippets?|zendesk macros?|intercom macros?)\b",
        re.I,
    ),
    "internal_faq": re.compile(
        r"\b(?:internal faq|support faq|agent faq|internal q&a|support q&a|"
        r"known questions|faq for support)\b",
        re.I,
    ),
    "support_tooling": _SUPPORT_TOOLING_RE,
    "triage_ownership": re.compile(
        r"\b(?:triage owner|support owner|support dri|escalation owner|"
        r"owning team|support queue|ticket owner|tier 2|tier two|on-call|oncall)\b",
        re.I,
    ),
    "launch_staffing": re.compile(
        r"\b(?:launch staffing|support staffing|staffed launch|launch watch|"
        r"watch window|war room|coverage window|support coverage|on-call coverage)\b",
        re.I,
    ),
    "known_issues": re.compile(
        r"\b(?:known issues?|known limitations?|workarounds?|troubleshooting notes?|"
        r"issue list|support caveats?)\b",
        re.I,
    ),
    "customer_communication_handoff": re.compile(
        r"\b(?:customer communication(?: handoff)?|customer comms?|customer email|"
        r"customer notice|announcement|release notes?|changelog|comms handoff|"
        r"customer-facing message)\b",
        re.I,
    ),
}
_MISSING_TEXT: dict[SupportCoverageArea, str] = {
    "support_docs": "Add help center, support guide, or troubleshooting documentation before launch.",
    "support_macros": "Prepare support macros, canned responses, or ticket snippets for expected questions.",
    "internal_faq": "Create an internal FAQ covering likely agent and customer-success questions.",
    "support_tooling": "Confirm support tooling, ticket routing, or agent-console changes are ready.",
    "triage_ownership": "Assign triage ownership and escalation routing for launch tickets.",
    "launch_staffing": "Define launch staffing, watch-window coverage, or on-call support coverage.",
    "known_issues": "Document known issues, limitations, and workarounds for support teams.",
    "customer_communication_handoff": "Hand off customer communication, release-note, or announcement guidance.",
}
_OWNER_DEFAULTS: dict[SupportCoverageArea, str] = {
    "support_docs": "Support content owner",
    "support_macros": "Support operations",
    "internal_faq": "Support enablement",
    "support_tooling": "Support operations",
    "triage_ownership": "Support lead",
    "launch_staffing": "Support manager",
    "known_issues": "Product owner",
    "customer_communication_handoff": "Product marketing",
}


@dataclass(frozen=True, slots=True)
class PlanSupportCoverageMatrixRow:
    """Support readiness coverage for one plan-level coverage area."""

    coverage_area: SupportCoverageArea
    detected_evidence: tuple[str, ...] = field(default_factory=tuple)
    readiness_status: SupportCoverageStatus = "missing"
    missing_coverage_items: tuple[str, ...] = field(default_factory=tuple)
    recommended_owner: str = ""
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "coverage_area": self.coverage_area,
            "detected_evidence": list(self.detected_evidence),
            "readiness_status": self.readiness_status,
            "missing_coverage_items": list(self.missing_coverage_items),
            "recommended_owner": self.recommended_owner,
            "affected_task_ids": list(self.affected_task_ids),
        }


@dataclass(frozen=True, slots=True)
class PlanSupportCoverageMatrix:
    """Plan-level matrix of support coverage readiness."""

    plan_id: str | None = None
    rows: tuple[PlanSupportCoverageMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanSupportCoverageMatrixRow, ...]:
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
        """Return support coverage rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the support coverage matrix as deterministic Markdown."""
        title = "# Plan Support Coverage Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Customer-impacting tasks: {self.summary.get('customer_impacting_task_count', 0)}",
            f"- Coverage areas: {self.summary.get('coverage_area_count', 0)}",
            f"- Ready areas: {self.summary.get('ready_count', 0)}",
            f"- Partial areas: {self.summary.get('partial_count', 0)}",
            f"- Missing areas: {self.summary.get('missing_count', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No support coverage needs were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Coverage Area | Status | Recommended Owner | Affected Tasks | Missing Coverage | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.coverage_area} | "
                f"{row.readiness_status} | "
                f"{_markdown_cell(row.recommended_owner)} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or '-')} | "
                f"{_markdown_cell('; '.join(row.missing_coverage_items) or '-')} | "
                f"{_markdown_cell('; '.join(row.detected_evidence) or '-')} |"
            )
        return "\n".join(lines)


def build_plan_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportCoverageMatrix:
    """Build a plan-level matrix of support coverage readiness."""
    plan_id, plan_context, tasks = _source_payload(source)
    builders: dict[SupportCoverageArea, _RowBuilder] = {}
    customer_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        signals = _task_signals(task, plan_context)
        if signals.customer_impacting:
            customer_task_ids.append(task_id)
            for area in _required_areas(signals):
                builders.setdefault(area, _RowBuilder(area=area)).task_ids.append(task_id)

        for area in _AREA_ORDER:
            evidence = signals.area_evidence.get(area, ())
            if not evidence:
                continue
            builder = builders.setdefault(area, _RowBuilder(area=area))
            builder.evidence.extend(evidence)
            builder.task_ids.append(task_id)
            if any(_READY_RE.search(item) for item in evidence):
                builder.ready_evidence.extend(evidence)

        for area in _AREA_ORDER:
            if signals.owner_hints.get(area):
                builders.setdefault(area, _RowBuilder(area=area)).owner_hints.extend(
                    signals.owner_hints[area]
                )

    rows = tuple(_row(builders[area]) for area in _AREA_ORDER if area in builders)
    status_counts = {
        status: sum(1 for row in rows if row.readiness_status == status)
        for status in _STATUS_ORDER
    }
    return PlanSupportCoverageMatrix(
        plan_id=plan_id,
        rows=rows,
        summary={
            "total_task_count": len(tasks),
            "customer_impacting_task_count": len(set(customer_task_ids)),
            "coverage_area_count": len(rows),
            "ready_count": status_counts["ready"],
            "partial_count": status_counts["partial"],
            "missing_count": status_counts["missing"],
            "status_counts": status_counts,
            "missing_coverage_item_count": sum(len(row.missing_coverage_items) for row in rows),
            "customer_impacting_task_ids": list(_dedupe(customer_task_ids)),
        },
    )


def generate_plan_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportCoverageMatrix:
    """Compatibility alias for building support coverage matrices."""
    if isinstance(source, PlanSupportCoverageMatrix):
        return source
    return build_plan_support_coverage_matrix(source)


def derive_plan_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportCoverageMatrix:
    """Compatibility alias for deriving support coverage matrices."""
    return generate_plan_support_coverage_matrix(source)


def summarize_plan_support_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanSupportCoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanSupportCoverageMatrix:
    """Compatibility alias for summarizing support coverage matrices."""
    return derive_plan_support_coverage_matrix(source)


def plan_support_coverage_matrix_to_dict(matrix: PlanSupportCoverageMatrix) -> dict[str, Any]:
    """Serialize a support coverage matrix to a plain dictionary."""
    return matrix.to_dict()


plan_support_coverage_matrix_to_dict.__test__ = False


def plan_support_coverage_matrix_to_markdown(matrix: PlanSupportCoverageMatrix) -> str:
    """Render a support coverage matrix as Markdown."""
    return matrix.to_markdown()


plan_support_coverage_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    area: SupportCoverageArea
    evidence: list[str] = field(default_factory=list)
    ready_evidence: list[str] = field(default_factory=list)
    owner_hints: list[str] = field(default_factory=list)
    task_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _TaskSignals:
    customer_impacting: bool = False
    rollout_related: bool = False
    support_tooling_related: bool = False
    area_evidence: dict[SupportCoverageArea, tuple[str, ...]] = field(default_factory=dict)
    owner_hints: dict[SupportCoverageArea, tuple[str, ...]] = field(default_factory=dict)


def _row(builder: _RowBuilder) -> PlanSupportCoverageMatrixRow:
    evidence = tuple(_dedupe(builder.evidence))
    if not evidence:
        status: SupportCoverageStatus = "missing"
    elif builder.ready_evidence:
        status = "ready"
    else:
        status = "partial"
    return PlanSupportCoverageMatrixRow(
        coverage_area=builder.area,
        detected_evidence=evidence,
        readiness_status=status,
        missing_coverage_items=() if status == "ready" else (_MISSING_TEXT[builder.area],),
        recommended_owner=(tuple(_dedupe(builder.owner_hints)) or (_OWNER_DEFAULTS[builder.area],))[0],
        affected_task_ids=tuple(_dedupe(builder.task_ids)),
    )


def _task_signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> _TaskSignals:
    area_evidence: dict[SupportCoverageArea, list[str]] = {}
    owner_hints: dict[SupportCoverageArea, list[str]] = {}
    customer_impacting = False
    rollout_related = False
    support_tooling_related = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        text = _normalized_path(path).replace("/", " ").replace("_", " ").replace("-", " ")
        evidence = f"files_or_modules: {path}"
        for area, pattern in _AREA_PATTERNS.items():
            if pattern.search(text):
                _append(area_evidence, area, evidence)
        if _CUSTOMER_IMPACT_RE.search(text):
            customer_impacting = True
        if _ROLL_OUT_RE.search(text):
            rollout_related = True
        if _SUPPORT_TOOLING_RE.search(text):
            support_tooling_related = True

    for source_field, text in (*_candidate_texts(task), *plan_context):
        if _OWNER_KEY_RE.search(source_field.replace("_", " ")):
            hints = _owner_hints(text)
            for area in _owner_areas(source_field, text):
                owner_hints.setdefault(area, []).extend(hints)

        evidence = _evidence_snippet(source_field, text)
        if _CUSTOMER_IMPACT_RE.search(text):
            customer_impacting = True
        if _ROLL_OUT_RE.search(text):
            rollout_related = True
        if _SUPPORT_TOOLING_RE.search(text):
            support_tooling_related = True
        for area, pattern in _AREA_PATTERNS.items():
            if pattern.search(text):
                _append(area_evidence, area, evidence)

    return _TaskSignals(
        customer_impacting=customer_impacting,
        rollout_related=rollout_related,
        support_tooling_related=support_tooling_related,
        area_evidence={area: tuple(_dedupe(values)) for area, values in area_evidence.items()},
        owner_hints={area: tuple(_dedupe(values)) for area, values in owner_hints.items()},
    )


def _required_areas(signals: _TaskSignals) -> tuple[SupportCoverageArea, ...]:
    areas: list[SupportCoverageArea] = [
        "support_docs",
        "support_macros",
        "internal_faq",
        "triage_ownership",
        "known_issues",
        "customer_communication_handoff",
    ]
    if signals.rollout_related:
        areas.append("launch_staffing")
    if signals.support_tooling_related:
        areas.append("support_tooling")
    return tuple(_ordered_dedupe(areas, _AREA_ORDER))


def _owner_areas(source_field: str, text: str) -> tuple[SupportCoverageArea, ...]:
    haystack = f"{source_field} {text}".casefold().replace("-", "_").replace(" ", "_")
    areas = [area for area in _AREA_ORDER if area in haystack]
    if "docs" in haystack or "documentation" in haystack or "help_center" in haystack:
        areas.append("support_docs")
    if "macro" in haystack:
        areas.append("support_macros")
    if "faq" in haystack:
        areas.append("internal_faq")
    if "tool" in haystack or "helpdesk" in haystack:
        areas.append("support_tooling")
    if "triage" in haystack or "escalation" in haystack:
        areas.append("triage_ownership")
    if "staff" in haystack or "launch" in haystack or "oncall" in haystack:
        areas.append("launch_staffing")
    if "known_issue" in haystack or "workaround" in haystack:
        areas.append("known_issues")
    if "communication" in haystack or "comms" in haystack or "release_note" in haystack:
        areas.append("customer_communication_handoff")
    return tuple(_ordered_dedupe(areas or list(_AREA_ORDER), _AREA_ORDER))


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


def _append(values: dict[SupportCoverageArea, list[str]], key: SupportCoverageArea, value: str) -> None:
    values.setdefault(key, []).append(value)


def _ordered_dedupe(items: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    seen = set(items)
    return [item for item in order if item in seen]


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
    "PlanSupportCoverageMatrix",
    "PlanSupportCoverageMatrixRow",
    "SupportCoverageArea",
    "SupportCoverageStatus",
    "build_plan_support_coverage_matrix",
    "derive_plan_support_coverage_matrix",
    "generate_plan_support_coverage_matrix",
    "plan_support_coverage_matrix_to_dict",
    "plan_support_coverage_matrix_to_markdown",
    "summarize_plan_support_coverage_matrix",
]
