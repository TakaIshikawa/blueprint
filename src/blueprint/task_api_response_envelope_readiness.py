"""Plan API response envelope readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiResponseEnvelopeSignal = Literal[
    "data_field",
    "errors_field",
    "meta_field",
    "pagination_metadata",
    "links",
    "warnings",
    "request_id",
    "partial_success",
    "batch_results",
]
ApiResponseEnvelopeRequirement = Literal[
    "contract_tests",
    "sdk_compatibility",
    "backwards_compatibility",
    "error_envelope",
    "pagination_metadata_tests",
    "documentation",
]
ApiResponseEnvelopeRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ApiResponseEnvelopeSignal, ...] = (
    "data_field",
    "errors_field",
    "meta_field",
    "pagination_metadata",
    "links",
    "warnings",
    "request_id",
    "partial_success",
    "batch_results",
)
_REQUIREMENT_ORDER: tuple[ApiResponseEnvelopeRequirement, ...] = (
    "contract_tests",
    "sdk_compatibility",
    "backwards_compatibility",
    "error_envelope",
    "pagination_metadata_tests",
    "documentation",
)
_RISK_ORDER: dict[ApiResponseEnvelopeRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_PATTERNS: dict[ApiResponseEnvelopeSignal, re.Pattern[str]] = {
    "data_field": re.compile(
        r"\b(?:top[- ]level data|data field|data envelope|data wrapper|response\.data|\.data|data:)\b",
        re.I,
    ),
    "errors_field": re.compile(
        r"\b(?:top[- ]level errors?|errors? field|errors? envelope|errors? array|response\.errors?|\.errors?|errors?:)\b",
        re.I,
    ),
    "meta_field": re.compile(
        r"\b(?:top[- ]level meta|meta field|metadata field|response\.meta|\.meta|meta:|response metadata)\b",
        re.I,
    ),
    "pagination_metadata": re.compile(
        r"\b(?:pagination meta(?:data)?|page meta(?:data)?|next_cursor|prev_cursor|has_more|total_count|"
        r"page_info|pagination info|cursor meta)\b",
        re.I,
    ),
    "links": re.compile(
        r"\b(?:response links|links field|links envelope|hypermedia links|HATEOAS|next link|prev link|self link|"
        r"links:|response\.links)\b",
        re.I,
    ),
    "warnings": re.compile(
        r"\b(?:warnings? field|warnings? array|deprecation warnings?|response warnings?|warnings?:|response\.warnings?)\b",
        re.I,
    ),
    "request_id": re.compile(
        r"\b(?:request[- ]?id|request_id|correlation[- ]?id|trace[- ]?id|request identifier|response\.request_id)\b",
        re.I,
    ),
    "partial_success": re.compile(
        r"\b(?:partial success|partial failure|partial error|mixed results?|partial results?|some success|some failure)\b",
        re.I,
    ),
    "batch_results": re.compile(
        r"\b(?:batch results?|batch items?|bulk results?|multi[- ]?item results?|item results?|batch response|bulk response)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ApiResponseEnvelopeSignal, re.Pattern[str]] = {
    "data_field": re.compile(r"response[_-]?envelope|envelope|data[_-]?wrapper", re.I),
    "errors_field": re.compile(r"error[_-]?envelope|errors?[_-]?wrapper", re.I),
    "meta_field": re.compile(r"meta(?:data)?[_-]?field|response[_-]?meta", re.I),
    "pagination_metadata": re.compile(r"paginat(?:ion|e)[_-]?meta|page[_-]?info", re.I),
    "links": re.compile(r"links?[_-]?field|hateoas|hypermedia", re.I),
    "warnings": re.compile(r"warnings?[_-]?field", re.I),
    "request_id": re.compile(r"request[_-]?id|correlation[_-]?id|trace[_-]?id", re.I),
    "partial_success": re.compile(r"partial[_-]?success|mixed[_-]?results?", re.I),
    "batch_results": re.compile(r"batch[_-]?results?|bulk[_-]?results?|multi[_-]?item", re.I),
}
_REQUIREMENT_PATTERNS: dict[ApiResponseEnvelopeRequirement, re.Pattern[str]] = {
    "contract_tests": re.compile(
        r"\b(?:contract tests?|schema tests?|response schema tests?|envelope tests?|API contract tests?|"
        r"response validation tests?|envelope validation)\b",
        re.I,
    ),
    "sdk_compatibility": re.compile(
        r"\b(?:SDK compatibility|SDK compat|client SDK|SDK breaking|SDK deserialization|SDK tests?|"
        r"client compatibility|client breaking|client deserialization)\b",
        re.I,
    ),
    "backwards_compatibility": re.compile(
        r"\b(?:backwards? compat(?:ibility)?|backward[s]? compat(?:ibility)?|existing clients?|legacy clients?|"
        r"non-breaking|additive change|breaking change|version compat)\b",
        re.I,
    ),
    "error_envelope": re.compile(
        r"\b(?:error envelope|error response shape|error schema|error format|error structure|"
        r"problem details|rfc 7807|error consistency)\b",
        re.I,
    ),
    "pagination_metadata_tests": re.compile(
        r"\b(?:pagination meta(?:data)? tests?|cursor tests?|page info tests?|has_more tests?|"
        r"next_cursor tests?|pagination envelope tests?)\b",
        re.I,
    ),
    "documentation": re.compile(
        r"\b(?:documentation|docs|openapi|swagger|api docs|response docs|envelope docs|schema docs|"
        r"client guide|integration guide|migration notes)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:response envelope|envelope change|data field|meta field|"
    r"errors field|envelope)\b.{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[ApiResponseEnvelopeRequirement, str] = {
    "contract_tests": "Add contract tests validating response schema, envelope structure, required fields, and optional fields.",
    "sdk_compatibility": "Test SDK deserialization, client compatibility, and backwards compatibility with existing SDK versions.",
    "backwards_compatibility": "Verify backwards compatibility with existing clients, ensure additive changes, and document breaking changes.",
    "error_envelope": "Define consistent error envelope structure, error field format, and problem details schema.",
    "pagination_metadata_tests": "Add tests for pagination metadata fields, cursor tokens, has_more flags, and page info completeness.",
    "documentation": "Document response envelope structure, field meanings, optional/required fields, and SDK usage examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiResponseEnvelopeReadinessFinding:
    """API response envelope readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ApiResponseEnvelopeSignal, ...] = field(default_factory=tuple)
    present_requirements: tuple[ApiResponseEnvelopeRequirement, ...] = field(default_factory=tuple)
    missing_requirements: tuple[ApiResponseEnvelopeRequirement, ...] = field(default_factory=tuple)
    risk_level: ApiResponseEnvelopeRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_requirements": list(self.present_requirements),
            "missing_requirements": list(self.missing_requirements),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "actionable_gaps": list(self.actionable_gaps),
        }


@dataclass(frozen=True, slots=True)
class TaskApiResponseEnvelopeReadinessPlan:
    """Plan-level API response envelope readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiResponseEnvelopeReadinessFinding, ...] = field(default_factory=tuple)
    envelope_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiResponseEnvelopeReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [record.to_dict() for record in self.findings],
            "envelope_task_ids": list(self.envelope_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_api_response_envelope_readiness_plan(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Build API response envelope readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_RISK_ORDER[finding.risk_level], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskApiResponseEnvelopeReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        envelope_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_api_response_envelope_readiness(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Compatibility alias for building API response envelope readiness plans."""
    return build_task_api_response_envelope_readiness_plan(source)


def summarize_task_api_response_envelope_readiness(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Compatibility alias for building API response envelope readiness plans."""
    return build_task_api_response_envelope_readiness_plan(source)


def extract_task_api_response_envelope_readiness(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Compatibility alias for extracting API response envelope readiness plans."""
    return build_task_api_response_envelope_readiness_plan(source)


def generate_task_api_response_envelope_readiness(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Compatibility alias for generating API response envelope readiness plans."""
    return build_task_api_response_envelope_readiness_plan(source)


def recommend_task_api_response_envelope_readiness(source: Any) -> TaskApiResponseEnvelopeReadinessPlan:
    """Compatibility alias for recommending API response envelope readiness gaps."""
    return build_task_api_response_envelope_readiness_plan(source)


def task_api_response_envelope_readiness_plan_to_dict(result: TaskApiResponseEnvelopeReadinessPlan) -> dict[str, Any]:
    """Serialize an API response envelope readiness plan to a plain dictionary."""
    return result.to_dict()


task_api_response_envelope_readiness_plan_to_dict.__test__ = False


def task_api_response_envelope_readiness_plan_to_dicts(
    result: TaskApiResponseEnvelopeReadinessPlan | Iterable[TaskApiResponseEnvelopeReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize API response envelope readiness findings to plain dictionaries."""
    if isinstance(result, TaskApiResponseEnvelopeReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_response_envelope_readiness_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ApiResponseEnvelopeSignal, ...] = field(default_factory=tuple)
    requirements: tuple[ApiResponseEnvelopeRequirement, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskApiResponseEnvelopeReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing = tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement not in signals.requirements)
    return TaskApiResponseEnvelopeReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_requirements=signals.requirements,
        missing_requirements=missing,
        risk_level=_risk_level(signals.signals, missing),
        evidence=signals.evidence,
        actionable_gaps=tuple(_ACTIONABLE_GAPS[requirement] for requirement in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ApiResponseEnvelopeSignal] = set()
    requirement_hits: set[ApiResponseEnvelopeRequirement] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        requirements=tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement in requirement_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _risk_level(
    signals: tuple[ApiResponseEnvelopeSignal, ...],
    missing: tuple[ApiResponseEnvelopeRequirement, ...],
) -> ApiResponseEnvelopeRisk:
    if not missing:
        return "low"
    missing_set = set(missing)
    critical = {"contract_tests", "sdk_compatibility", "backwards_compatibility"}
    if len(missing) >= 5:
        return "high"
    if critical & missing_set and len(missing) >= 3:
        return "high"
    if {"data_field", "errors_field", "meta_field"} <= set(signals) and {"contract_tests", "backwards_compatibility"} <= missing_set:
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskApiResponseEnvelopeReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "envelope_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_requirement_count": sum(len(finding.missing_requirements) for finding in findings),
        "risk_counts": {risk: sum(1 for finding in findings if finding.risk_level == risk) for risk in _RISK_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.present_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
        "missing_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.missing_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
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
        if task := _task_payload(item):
            tasks.append(task)
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
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_REQUIREMENT_PATTERNS.values()])


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


def _normalized_path(value: str) -> str:
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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
