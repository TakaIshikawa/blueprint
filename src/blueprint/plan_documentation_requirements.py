"""Build plan-level documentation requirements matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief


DocumentationType = Literal[
    "api_documentation",
    "architecture_diagrams",
    "runbooks",
    "user_guides",
    "onboarding_materials",
    "troubleshooting_guides",
    "api_reference",
    "deployment_guides",
]
CoverageStatus = Literal["missing", "partial", "covered"]

_TYPE_ORDER: tuple[DocumentationType, ...] = (
    "api_documentation",
    "architecture_diagrams",
    "runbooks",
    "user_guides",
    "onboarding_materials",
    "troubleshooting_guides",
    "api_reference",
    "deployment_guides",
)
_SPACE_RE = re.compile(r"\s+")
_API_DOCS_RE = re.compile(
    r"\b(?:api\s+(?:documentation|docs?|reference|spec)|openapi|swagger|"
    r"rest\s+api\s+docs?|graphql\s+(?:schema|docs?)|endpoint\s+documentation|"
    r"api\s+guide)\b",
    re.I,
)
_ARCHITECTURE_RE = re.compile(
    r"\b(?:architecture\s+(?:diagram|documentation|design)|system\s+design|"
    r"design\s+(?:doc|documentation)|technical\s+architecture|"
    r"architecture\s+(?:overview|decision|record)|adr|c4\s+(?:model|diagram)|"
    r"sequence\s+diagram|component\s+diagram)\b",
    re.I,
)
_RUNBOOK_RE = re.compile(
    r"\b(?:runbook|run\s+book|operational\s+(?:guide|documentation|playbook)|"
    r"ops\s+(?:guide|playbook)|incident\s+(?:runbook|playbook|response\s+guide)|"
    r"deployment\s+runbook|rollback\s+(?:guide|procedure))\b",
    re.I,
)
_USER_GUIDE_RE = re.compile(
    r"\b(?:user\s+(?:guide|documentation|manual)|end[- ]user\s+(?:docs?|guide)|"
    r"how[- ]to\s+guide|tutorial|getting\s+started\s+guide|"
    r"customer\s+(?:documentation|guide)|feature\s+guide)\b",
    re.I,
)
_ONBOARDING_RE = re.compile(
    r"\b(?:onboarding\s+(?:documentation|guide|materials?)|developer\s+onboarding|"
    r"team\s+onboarding|new\s+hire\s+guide|setup\s+guide|"
    r"getting\s+started|quickstart|quick\s+start)\b",
    re.I,
)
_TROUBLESHOOTING_RE = re.compile(
    r"\b(?:troubleshooting\s+(?:guide|documentation|steps?)|"
    r"debugging\s+guide|faq|frequently\s+asked\s+questions|"
    r"common\s+(?:issues|problems|errors)|known\s+issues|"
    r"diagnostic\s+guide|support\s+guide)\b",
    re.I,
)
_API_REFERENCE_RE = re.compile(
    r"\b(?:api\s+reference|reference\s+documentation|method\s+reference|"
    r"endpoint\s+reference|sdk\s+reference|client\s+reference|"
    r"function\s+reference|class\s+reference)\b",
    re.I,
)
_DEPLOYMENT_GUIDE_RE = re.compile(
    r"\b(?:deployment\s+(?:guide|documentation|instructions?)|"
    r"release\s+(?:guide|process|documentation)|"
    r"installation\s+guide|setup\s+instructions?|"
    r"rollout\s+(?:guide|plan)|go[- ]live\s+(?:guide|checklist))\b",
    re.I,
)
_TYPE_PATTERNS: dict[DocumentationType, re.Pattern[str]] = {
    "api_documentation": _API_DOCS_RE,
    "architecture_diagrams": _ARCHITECTURE_RE,
    "runbooks": _RUNBOOK_RE,
    "user_guides": _USER_GUIDE_RE,
    "onboarding_materials": _ONBOARDING_RE,
    "troubleshooting_guides": _TROUBLESHOOTING_RE,
    "api_reference": _API_REFERENCE_RE,
    "deployment_guides": _DEPLOYMENT_GUIDE_RE,
}
_RECOMMENDED_ATTRIBUTES: dict[DocumentationType, tuple[str, ...]] = {
    "api_documentation": ("content", "ownership", "timeline", "review_process"),
    "architecture_diagrams": ("content", "ownership", "timeline", "maintenance_plan"),
    "runbooks": ("content", "ownership", "timeline", "review_process"),
    "user_guides": ("content", "ownership", "timeline", "accessibility"),
    "onboarding_materials": ("content", "ownership", "timeline", "accessibility"),
    "troubleshooting_guides": ("content", "ownership", "timeline", "review_process"),
    "api_reference": ("content", "ownership", "timeline", "accuracy"),
    "deployment_guides": ("content", "ownership", "timeline", "review_process"),
}
_ATTRIBUTE_PATTERNS: dict[str, re.Pattern[str]] = {
    "content": re.compile(
        r"\b(?:document|documentation|write|create|generate|content|material)\b", re.I
    ),
    "ownership": re.compile(
        r"\b(?:owner|owned\s+by|assigned\s+to|responsible|maintainer|author)\b", re.I
    ),
    "timeline": re.compile(
        r"\b(?:timeline|deadline|due\s+(?:date|by)|delivery\s+date|schedule|"
        r"by\s+\d{4}-\d{2}-\d{2}|before\s+launch|pre[- ]launch)\b",
        re.I,
    ),
    "review_process": re.compile(
        r"\b(?:review\s+(?:process|cycle|required)|peer\s+review|approval|"
        r"sign[- ]?off|stakeholder\s+review|technical\s+review)\b",
        re.I,
    ),
    "maintenance_plan": re.compile(
        r"\b(?:maintenance\s+plan|keep\s+(?:updated|current)|update\s+(?:schedule|frequency)|"
        r"versioning|revision\s+schedule)\b",
        re.I,
    ),
    "accessibility": re.compile(
        r"\b(?:accessibility|accessible|public|external|customer[- ]facing|available\s+to)\b",
        re.I,
    ),
    "accuracy": re.compile(
        r"\b(?:accuracy|accurate|up[- ]to[- ]date|current|correct|validated|verified)\b",
        re.I,
    ),
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
class PlanDocumentationRequirementsRow:
    """Documentation requirements row for one doc type across the plan."""

    doc_type: DocumentationType
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    coverage_status: CoverageStatus = "missing"
    missing_attributes: tuple[str, ...] = field(default_factory=tuple)
    recommended_attributes: tuple[str, ...] = field(default_factory=tuple)
    owner: str | None = None
    delivery_timeline: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "doc_type": self.doc_type,
            "affected_task_ids": list(self.affected_task_ids),
            "evidence": list(self.evidence),
            "coverage_status": self.coverage_status,
            "missing_attributes": list(self.missing_attributes),
            "recommended_attributes": list(self.recommended_attributes),
            "owner": self.owner,
            "delivery_timeline": self.delivery_timeline,
        }


@dataclass(frozen=True, slots=True)
class PlanDocumentationRequirementsMatrix:
    """Plan-level documentation requirements matrix."""

    plan_id: str | None = None
    implementation_brief_id: str | None = None
    rows: tuple[PlanDocumentationRequirementsRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanDocumentationRequirementsRow, ...]:
        """Compatibility view matching report-style modules."""
        return self.rows

    @property
    def completeness_score(self) -> float:
        """Calculate documentation completeness score (0.0 to 1.0)."""
        if not self.rows:
            return 0.0
        covered = sum(1 for row in self.rows if row.coverage_status == "covered")
        partial = sum(1 for row in self.rows if row.coverage_status == "partial")
        total = len(self.rows)
        return (covered + 0.5 * partial) / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "implementation_brief_id": self.implementation_brief_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
            "completeness_score": self.completeness_score,
            "records": [row.to_dict() for row in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Documentation Requirements Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Documentation types: {self.summary.get('doc_type_count', 0)}",
            f"- Covered: {self.summary.get('covered_count', 0)}",
            f"- Partial: {self.summary.get('partial_count', 0)}",
            f"- Missing: {self.summary.get('missing_count', 0)}",
            f"- Completeness score: {self.completeness_score:.2f}",
            "- Type counts: "
            + ", ".join(
                f"{doc_type} {self.summary.get('type_counts', {}).get(doc_type, 0)}"
                for doc_type in _TYPE_ORDER
            ),
        ]
        if not self.rows:
            lines.extend(["", "No documentation requirements were found in the plan."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Documentation Requirements",
                "",
                "| Doc Type | Status | Owner | Timeline | Affected Tasks | Missing Attributes | Recommended Attributes | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.doc_type} | "
                f"{row.coverage_status} | "
                f"{_markdown_cell(row.owner or 'unassigned')} | "
                f"{_markdown_cell(row.delivery_timeline or 'not specified')} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_attributes) or 'none')} | "
                f"{_markdown_cell(', '.join(row.recommended_attributes))} | "
                f"{_markdown_cell('; '.join(row.evidence))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class DocumentationRequirementsMatrixGenerator:
    """Generator for building documentation requirements matrices from execution plans."""

    def generate_matrix(
        self,
        plan: Mapping[str, Any] | ExecutionPlan | object,
        brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
    ) -> PlanDocumentationRequirementsMatrix:
        """Generate a documentation requirements matrix from a plan."""
        return build_plan_documentation_requirements_matrix(plan, brief)


def build_plan_documentation_requirements_matrix(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> PlanDocumentationRequirementsMatrix:
    """Build a plan-level matrix of documentation requirements."""
    plan_id, implementation_brief_id, plan_payload, tasks = _plan_payload(plan)
    brief_id, brief_payload = _brief_payload(brief)
    implementation_brief_id = implementation_brief_id or brief_id

    buckets: dict[DocumentationType, _Bucket] = {}
    for segment in _plan_segments(plan_payload):
        _collect_segment(buckets, segment)
    for segment in _brief_segments(brief_payload):
        _collect_segment(buckets, segment)
    for task in tasks:
        task_id = _optional_text(task.get("id"))
        for segment in _task_segments(task, task_id):
            _collect_segment(buckets, segment)

    rows = tuple(
        _row(doc_type, buckets[doc_type]) for doc_type in _TYPE_ORDER if doc_type in buckets
    )
    return PlanDocumentationRequirementsMatrix(
        plan_id=plan_id,
        implementation_brief_id=implementation_brief_id,
        rows=rows,
        summary=_summary(rows),
    )


def generate_plan_documentation_requirements_matrix(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> PlanDocumentationRequirementsMatrix:
    """Compatibility helper for callers that use generate_* naming."""
    return build_plan_documentation_requirements_matrix(plan, brief)


def extract_plan_documentation_requirements_rows(
    plan: Mapping[str, Any] | ExecutionPlan | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> tuple[PlanDocumentationRequirementsRow, ...]:
    """Return documentation requirements rows extracted from plan-shaped input."""
    return build_plan_documentation_requirements_matrix(plan, brief).rows


def summarize_plan_documentation_requirements_matrix(
    plan_or_matrix: Mapping[str, Any] | ExecutionPlan | PlanDocumentationRequirementsMatrix | object,
    brief: Mapping[str, Any] | ImplementationBrief | object | None = None,
) -> dict[str, Any]:
    """Return the deterministic documentation requirements summary."""
    if isinstance(plan_or_matrix, PlanDocumentationRequirementsMatrix):
        return dict(plan_or_matrix.summary)
    return build_plan_documentation_requirements_matrix(plan_or_matrix, brief).summary


def plan_documentation_requirements_matrix_to_dict(
    matrix: PlanDocumentationRequirementsMatrix,
) -> dict[str, Any]:
    """Serialize a plan documentation requirements matrix to a plain dictionary."""
    return matrix.to_dict()


plan_documentation_requirements_matrix_to_dict.__test__ = False


def plan_documentation_requirements_matrix_to_dicts(
    rows: (
        tuple[PlanDocumentationRequirementsRow, ...]
        | list[PlanDocumentationRequirementsRow]
        | PlanDocumentationRequirementsMatrix
    ),
) -> list[dict[str, Any]]:
    """Serialize plan documentation requirements rows to dictionaries."""
    if isinstance(rows, PlanDocumentationRequirementsMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_documentation_requirements_matrix_to_dicts.__test__ = False


def plan_documentation_requirements_matrix_to_markdown(
    matrix: PlanDocumentationRequirementsMatrix,
) -> str:
    """Render a plan documentation requirements matrix as Markdown."""
    return matrix.to_markdown()


plan_documentation_requirements_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    task_id: str | None = None


@dataclass(slots=True)
class _Bucket:
    task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    covered_attributes: set[str] = field(default_factory=set)
    owners: list[str] = field(default_factory=list)
    timelines: list[str] = field(default_factory=list)


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
    buckets: dict[DocumentationType, _Bucket],
    segment: _Segment,
) -> None:
    doc_types = _segment_doc_types(segment)
    if not doc_types:
        return
    attributes = _covered_attributes(segment)
    evidence = _evidence_snippet(segment)
    owner = _extract_owner(segment)
    timeline = _extract_timeline(segment)
    for doc_type in doc_types:
        bucket = buckets.setdefault(doc_type, _Bucket())
        if segment.task_id:
            _append_unique(bucket.task_ids, segment.task_id)
        _append_unique(bucket.evidence, evidence)
        bucket.covered_attributes.update(
            attr for attr in attributes if attr in _RECOMMENDED_ATTRIBUTES[doc_type]
        )
        if owner:
            _append_unique(bucket.owners, owner)
        if timeline:
            _append_unique(bucket.timelines, timeline)


def _segment_doc_types(segment: _Segment) -> tuple[DocumentationType, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    path_type = _path_doc_type(segment)
    types = [
        doc_type
        for doc_type in _TYPE_ORDER
        if _TYPE_PATTERNS[doc_type].search(searchable) or doc_type == path_type
    ]
    return tuple(types)


def _path_doc_type(segment: _Segment) -> DocumentationType | None:
    if segment.source_field != "task.files_or_modules":
        return None
    folded = _normalized_path(segment.text).casefold()
    if not folded:
        return None
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    if {"api", "apis", "api-docs", "api_docs"} & parts or "openapi" in name or "swagger" in name:
        return "api_documentation"
    if {"architecture", "diagrams", "design", "adr", "adrs"} & parts:
        return "architecture_diagrams"
    if {"runbooks", "runbook", "playbooks", "playbook"} & parts:
        return "runbooks"
    if {"guides", "guide", "user-guides", "tutorials"} & parts:
        return "user_guides"
    if {"onboarding", "getting-started", "quickstart"} & parts:
        return "onboarding_materials"
    if {"troubleshooting", "faq", "faqs", "debugging"} & parts:
        return "troubleshooting_guides"
    if {"reference", "references", "sdk-reference"} & parts:
        return "api_reference"
    if {"deployment", "deployments", "installation", "setup"} & parts:
        return "deployment_guides"
    return None


def _covered_attributes(segment: _Segment) -> set[str]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    return {
        attribute
        for attribute, pattern in _ATTRIBUTE_PATTERNS.items()
        if pattern.search(searchable)
    }


def _extract_owner(segment: _Segment) -> str | None:
    """Extract owner information from segment text."""
    match = re.search(
        r"(?:owner|owned\s+by|assigned\s+to|responsible)[:\s]+([a-zA-Z0-9@._-]+)",
        segment.text,
        re.I,
    )
    return match.group(1) if match else None


def _extract_timeline(segment: _Segment) -> str | None:
    """Extract timeline/deadline information from segment text."""
    # Try to match date patterns
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", segment.text)
    if date_match:
        return date_match.group(0)
    # Try to match relative deadlines
    relative_match = re.search(
        r"(?:by|before|due)\s+([a-zA-Z0-9\s-]+?)(?:\.|,|$)", segment.text, re.I
    )
    return relative_match.group(1).strip() if relative_match else None


def _row(doc_type: DocumentationType, bucket: _Bucket) -> PlanDocumentationRequirementsRow:
    recommended = _RECOMMENDED_ATTRIBUTES[doc_type]
    covered = {attr for attr in bucket.covered_attributes if attr in recommended}
    missing = tuple(attr for attr in recommended if attr not in covered)
    if not covered:
        status: CoverageStatus = "missing"
    elif missing:
        status = "partial"
    else:
        status = "covered"
    return PlanDocumentationRequirementsRow(
        doc_type=doc_type,
        affected_task_ids=tuple(bucket.task_ids),
        evidence=tuple(bucket.evidence[:8]),
        coverage_status=status,
        missing_attributes=missing,
        recommended_attributes=recommended,
        owner=bucket.owners[0] if bucket.owners else None,
        delivery_timeline=bucket.timelines[0] if bucket.timelines else None,
    )


def _summary(rows: tuple[PlanDocumentationRequirementsRow, ...]) -> dict[str, Any]:
    type_counts = {doc_type: 0 for doc_type in _TYPE_ORDER}
    for row in rows:
        type_counts[row.doc_type] = 1
    status_counts = {
        "missing": sum(1 for row in rows if row.coverage_status == "missing"),
        "partial": sum(1 for row in rows if row.coverage_status == "partial"),
        "covered": sum(1 for row in rows if row.coverage_status == "covered"),
    }
    return {
        "doc_type_count": len(rows),
        "covered_count": status_counts["covered"],
        "partial_count": status_counts["partial"],
        "missing_count": status_counts["missing"],
        "type_counts": type_counts,
        "doc_types": [row.doc_type for row in rows],
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


__all__ = [
    "DocumentationType",
    "CoverageStatus",
    "PlanDocumentationRequirementsRow",
    "PlanDocumentationRequirementsMatrix",
    "DocumentationRequirementsMatrixGenerator",
    "build_plan_documentation_requirements_matrix",
    "generate_plan_documentation_requirements_matrix",
    "extract_plan_documentation_requirements_rows",
    "summarize_plan_documentation_requirements_matrix",
    "plan_documentation_requirements_matrix_to_dict",
    "plan_documentation_requirements_matrix_to_dicts",
    "plan_documentation_requirements_matrix_to_markdown",
]
