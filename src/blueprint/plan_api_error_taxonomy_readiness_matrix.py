"""Build plan-level API error taxonomy readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiErrorTaxonomyReadinessArea = Literal[
    "error_codes",
    "http_status_mapping",
    "validation_errors",
    "retryable_errors",
    "machine_readable_details",
    "documentation",
]
ApiErrorTaxonomyReadiness = Literal["ready", "partial", "blocked"]
ApiErrorTaxonomyRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ApiErrorTaxonomyReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_RISK_ORDER: dict[ApiErrorTaxonomyRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AREA_ORDER: dict[ApiErrorTaxonomyReadinessArea, int] = {
    "error_codes": 0,
    "http_status_mapping": 1,
    "validation_errors": 2,
    "retryable_errors": 3,
    "machine_readable_details": 4,
    "documentation": 5,
}
_ERROR_TAXONOMY_RE = re.compile(
    r"\b(?:api|endpoint|rest|graphql)\b.*\b(?:error|errors|error code|error response|"
    r"error handling|error taxonomy|problem details)\b|"
    r"\b(?:error code|error taxonomy|error response|error handling|http status|status code|"
    r"validation error|retryable error|problem details|rfc 7807|correlation id)\b",
    re.I,
)
_AREA_PATTERNS: dict[ApiErrorTaxonomyReadinessArea, re.Pattern[str]] = {
    "error_codes": re.compile(
        r"\b(?:error code|error_code|error codes|error enum|stable error code|"
        r"machine[- ]readable code|error identifier|error id|error type)\b",
        re.I,
    ),
    "http_status_mapping": re.compile(
        r"\b(?:http status|status code|400|401|403|404|422|429|500|502|503|"
        r"bad request|unauthorized|forbidden|not found|unprocessable|rate limit|"
        r"internal server error|status mapping|http mapping)\b",
        re.I,
    ),
    "validation_errors": re.compile(
        r"\b(?:validation error|field error|field-level error|input validation|"
        r"schema validation|parameter error|body validation|request validation|"
        r"field path|error field|field name)\b",
        re.I,
    ),
    "retryable_errors": re.compile(
        r"\b(?:retryable|retry|retryability|transient error|temporary error|"
        r"retry-after|backoff|idempotent retry|safe to retry|do not retry)\b",
        re.I,
    ),
    "machine_readable_details": re.compile(
        r"\b(?:machine[- ]readable|problem details|rfc 7807|json problem|"
        r"structured error|error detail|correlation id|request id|trace id|"
        r"error context|error metadata)\b",
        re.I,
    ),
    "documentation": re.compile(
        r"\b(?:error documentation|error docs|api docs|openapi|swagger|"
        r"error catalog|error reference|error guide|client error handling|"
        r"sdk error|error examples)\b",
        re.I,
    ),
}
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_team",
    "team",
    "dri",
    "api_owner",
    "backend_owner",
    "client_owner",
    "docs_owner",
    "qa_owner",
)
_DEFAULT_OWNERS: dict[ApiErrorTaxonomyReadinessArea, str] = {
    "error_codes": "api_owner",
    "http_status_mapping": "api_owner",
    "validation_errors": "api_owner",
    "retryable_errors": "api_owner",
    "machine_readable_details": "api_owner",
    "documentation": "developer_experience_owner",
}
_GAP_MESSAGES: dict[ApiErrorTaxonomyReadinessArea, str] = {
    "error_codes": "Missing stable error codes.",
    "http_status_mapping": "Missing HTTP status mapping.",
    "validation_errors": "Missing validation error structure.",
    "retryable_errors": "Missing retryability indicators.",
    "machine_readable_details": "Missing machine-readable error details.",
    "documentation": "Missing error documentation.",
}
_NEXT_ACTIONS: dict[ApiErrorTaxonomyReadinessArea, str] = {
    "error_codes": "Define stable, machine-readable error codes for all API error conditions.",
    "http_status_mapping": "Map error codes to appropriate HTTP status codes (400, 401, 403, 404, 422, 500, etc.).",
    "validation_errors": "Provide field-level validation error structure with field paths and messages.",
    "retryable_errors": "Indicate retryability for each error type and provide retry-after guidance where applicable.",
    "machine_readable_details": "Implement RFC 7807 problem details or structured error response with correlation IDs.",
    "documentation": "Document all error codes, status mappings, and client error-handling patterns.",
}
_HIGH_GAP_AREAS: frozenset[ApiErrorTaxonomyReadinessArea] = frozenset(
    {"error_codes", "http_status_mapping", "machine_readable_details"}
)


@dataclass(frozen=True, slots=True)
class PlanApiErrorTaxonomyReadinessRow:
    """One plan-level API error taxonomy readiness row."""

    area: ApiErrorTaxonomyReadinessArea
    owner: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiErrorTaxonomyReadiness = "partial"
    risk: ApiErrorTaxonomyRisk = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "area": self.area,
            "owner": self.owner,
            "evidence": list(self.evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "risk": self.risk,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class PlanApiErrorTaxonomyReadinessMatrix:
    """Plan-level API error taxonomy readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanApiErrorTaxonomyReadinessRow, ...] = field(default_factory=tuple)
    error_taxonomy_task_ids: tuple[str, ...] = field(default_factory=tuple)
    gap_areas: tuple[ApiErrorTaxonomyReadinessArea, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiErrorTaxonomyReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "error_taxonomy_task_ids": list(self.error_taxonomy_task_ids),
            "gap_areas": list(self.gap_areas),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API error taxonomy readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the API error taxonomy readiness matrix as deterministic Markdown."""
        title = "# Plan API Error Taxonomy Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ready_area_count', 0)} of "
                f"{self.summary.get('area_count', 0)} API error taxonomy readiness areas ready "
                f"(high: {risk_counts.get('high', 0)}, medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No API error taxonomy readiness rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Area | Owner | Readiness | Risk | Evidence | Gaps | Next Action | Tasks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.area} | "
                f"{_markdown_cell(row.owner)} | "
                f"{row.readiness} | "
                f"{row.risk} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell(row.next_action)} | "
                f"{_markdown_cell(', '.join(row.task_ids) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_error_taxonomy_readiness_matrix(source: Any) -> PlanApiErrorTaxonomyReadinessMatrix:
    """Build required API error taxonomy readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    area_evidence: dict[ApiErrorTaxonomyReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    area_task_ids: dict[ApiErrorTaxonomyReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    owner_hints: dict[ApiErrorTaxonomyReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    error_taxonomy_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        task_has_error_taxonomy_signal = bool(_ERROR_TAXONOMY_RE.search(context))
        owners = _owner_hints(task)
        for area, pattern in _AREA_PATTERNS.items():
            matches = [
                _evidence_snippet(source_field, text)
                for source_field, text in texts
                if pattern.search(text)
            ]
            if matches:
                task_has_error_taxonomy_signal = True
                area_evidence[area].extend(matches)
                area_task_ids[area].append(task_id)
                owner_hints[area].extend(owners)
        if task_has_error_taxonomy_signal:
            error_taxonomy_task_ids.append(task_id)
            for area in _AREA_ORDER:
                owner_hints[area].extend(owners)

    if not error_taxonomy_task_ids:
        rows: tuple[PlanApiErrorTaxonomyReadinessRow, ...] = ()
    else:
        rows = tuple(_row(area, area_evidence[area], area_task_ids[area], owner_hints[area]) for area in _AREA_ORDER)
    return PlanApiErrorTaxonomyReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        error_taxonomy_task_ids=tuple(_dedupe(error_taxonomy_task_ids)),
        gap_areas=tuple(row.area for row in rows if row.gaps),
        summary=_summary(len(tasks), rows, error_taxonomy_task_ids),
    )


def generate_plan_api_error_taxonomy_readiness_matrix(source: Any) -> PlanApiErrorTaxonomyReadinessMatrix:
    """Generate an API error taxonomy readiness matrix from a plan-like source."""
    return build_plan_api_error_taxonomy_readiness_matrix(source)


def plan_api_error_taxonomy_readiness_matrix_to_dict(
    matrix: PlanApiErrorTaxonomyReadinessMatrix,
) -> dict[str, Any]:
    """Serialize an API error taxonomy readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_api_error_taxonomy_readiness_matrix_to_dict.__test__ = False


def plan_api_error_taxonomy_readiness_matrix_to_dicts(
    matrix: PlanApiErrorTaxonomyReadinessMatrix | Iterable[PlanApiErrorTaxonomyReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize API error taxonomy readiness rows to plain dictionaries."""
    if isinstance(matrix, PlanApiErrorTaxonomyReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_error_taxonomy_readiness_matrix_to_dicts.__test__ = False


def plan_api_error_taxonomy_readiness_matrix_to_markdown(
    matrix: PlanApiErrorTaxonomyReadinessMatrix,
) -> str:
    """Render an API error taxonomy readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_api_error_taxonomy_readiness_matrix_to_markdown.__test__ = False


def _row(
    area: ApiErrorTaxonomyReadinessArea,
    evidence: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
) -> PlanApiErrorTaxonomyReadinessRow:
    evidence_tuple = tuple(_dedupe(evidence))
    gaps = () if evidence_tuple else (_GAP_MESSAGES[area],)
    readiness: ApiErrorTaxonomyReadiness = "ready" if not gaps else "partial"
    risk: ApiErrorTaxonomyRisk = "low" if not gaps else ("high" if area in _HIGH_GAP_AREAS else "medium")
    return PlanApiErrorTaxonomyReadinessRow(
        area=area,
        owner=next(iter(_dedupe(owners)), _DEFAULT_OWNERS[area]),
        evidence=evidence_tuple,
        gaps=gaps,
        readiness=readiness,
        risk=risk,
        next_action="Ready for API error taxonomy handoff." if not gaps else _NEXT_ACTIONS[area],
        task_ids=tuple(_dedupe(task_ids)),
    )


def _summary(
    task_count: int,
    rows: Iterable[PlanApiErrorTaxonomyReadinessRow],
    error_taxonomy_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "area_count": len(row_list),
        "ready_area_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_area_count": sum(1 for row in row_list if row.gaps),
        "error_taxonomy_task_count": len(tuple(_dedupe(error_taxonomy_task_ids))),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "risk_counts": {risk: sum(1 for row in row_list if row.risk == risk) for risk in _RISK_ORDER},
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
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
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes", "risks"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    owners = []
    if owner := _optional_text(task.get("owner_type")):
        owners.append(owner)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings(metadata.get(key)))
    return _dedupe(owners)


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
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "status",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "ApiErrorTaxonomyReadiness",
    "ApiErrorTaxonomyReadinessArea",
    "ApiErrorTaxonomyRisk",
    "PlanApiErrorTaxonomyReadinessMatrix",
    "PlanApiErrorTaxonomyReadinessRow",
    "build_plan_api_error_taxonomy_readiness_matrix",
    "generate_plan_api_error_taxonomy_readiness_matrix",
    "plan_api_error_taxonomy_readiness_matrix_to_dict",
    "plan_api_error_taxonomy_readiness_matrix_to_dicts",
    "plan_api_error_taxonomy_readiness_matrix_to_markdown",
]
