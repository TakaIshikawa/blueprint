"""Recommend API contract test coverage for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


ContractTestType = Literal[
    "schema",
    "request_validation",
    "response_shape",
    "error_case",
    "backward_compatibility",
]
ApiContractRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_REST_SURFACE_RE = re.compile(
    r"\b(?:rest|http|https|api|apis|endpoint|endpoints|route|routes|controller|"
    r"openapi|swagger|status code|status codes)\b",
    re.IGNORECASE,
)
_REST_RE = re.compile(
    r"\b(?:rest|http|https|api|apis|endpoint|endpoints|route|routes|controller|"
    r"openapi|swagger|request|response|status code|status codes)\b",
    re.IGNORECASE,
)
_GRAPHQL_RE = re.compile(r"\b(?:graphql|gql|query|mutation|resolver)\b", re.IGNORECASE)
_SCHEMA_RE = re.compile(r"\b(?:schema|openapi|swagger|json schema)\b", re.IGNORECASE)
_WEBHOOK_RE = re.compile(r"\b(?:webhook|webhooks|callback|event payload)\b", re.IGNORECASE)
_CLIENT_RE = re.compile(
    r"\b(?:clients?|sdks?|external service|third[- ]party|partner)\b",
    re.IGNORECASE,
)
_EXTERNAL_CLIENT_RE = re.compile(
    r"\b(?:sdks?|external service|third[- ]party|partner)\b",
    re.IGNORECASE,
)
_PATH_API_RE = re.compile(
    r"(?:^|/)(?:api|apis|graphql|webhooks?|openapi)(?:/|\.|$)|"
    r"\b(?:routes?|controllers?|clients?|sdk|openapi|schema)\b",
    re.IGNORECASE,
)
_HTTP_PATH_RE = re.compile(
    r"\b(?:GET|POST|PUT|PATCH|DELETE)\s+(/[a-z0-9_./{}:-]+)|"
    r"(?<![a-z0-9])(/[a-z0-9][a-z0-9_./{}:-]*(?:/[a-z0-9_./{}:-]+)+)",
    re.IGNORECASE,
)
_EXTERNAL_NAME_RE = re.compile(
    r"\b(?:stripe|salesforce|shopify|slack|github|google|twilio|sendgrid|segment|"
    r"zendesk|hubspot|intercom|adyen|paypal|quickbooks|netsuite)\b",
    re.IGNORECASE,
)
_SCHEMA_EVIDENCE_RE = re.compile(
    r"\b(?:schema|contract|openapi|swagger|graphql|json schema)\b", re.I
)
_REQUEST_EVIDENCE_RE = re.compile(r"\b(?:request|input|payload|parameter|param|validation)\b", re.I)
_RESPONSE_EVIDENCE_RE = re.compile(r"\b(?:response|output|shape|field|status code|payload)\b", re.I)
_ERROR_EVIDENCE_RE = re.compile(r"\b(?:error|failure|invalid|4xx|5xx|exception)\b", re.I)
_COMPAT_EVIDENCE_RE = re.compile(
    r"\b(?:compatibility|compatible|backward compatible|backwards compatible|version|versioning|"
    r"migration|existing client|existing clients|non[- ]breaking|breaking change)\b",
    re.I,
)
_GENERIC_WORDS = {
    "add",
    "api",
    "build",
    "client",
    "endpoint",
    "external",
    "graphql",
    "implement",
    "integrate",
    "integration",
    "request",
    "response",
    "rest",
    "route",
    "schema",
    "service",
    "task",
    "test",
    "update",
    "webhook",
}
_RISK_ORDER: dict[ApiContractRisk, int] = {"high": 0, "medium": 1, "low": 2}


@dataclass(frozen=True, slots=True)
class TaskApiContractTestRecommendation:
    """Contract-test guidance for one API-facing execution task."""

    task_id: str
    title: str
    contract_surfaces: tuple[str, ...]
    suggested_test_types: tuple[ContractTestType, ...]
    missing_acceptance_criteria: tuple[ContractTestType, ...] = field(default_factory=tuple)
    risk_level: ApiContractRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "contract_surfaces": list(self.contract_surfaces),
            "suggested_test_types": list(self.suggested_test_types),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskApiContractTestPlan:
    """API contract-test recommendations for an execution plan."""

    plan_id: str | None = None
    recommendations: tuple[TaskApiContractTestRecommendation, ...] = field(default_factory=tuple)
    api_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "api_task_ids": list(self.api_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]


def build_task_api_contract_test_plan(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TaskApiContractTestPlan:
    """Recommend contract-test coverage for tasks touching API boundaries."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    recommendations = [
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index)) is not None
    ]
    recommendations.sort(
        key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id, item.title.casefold())
    )
    result = tuple(recommendations)
    risk_counts = {
        risk: sum(1 for item in result if item.risk_level == risk) for risk in _RISK_ORDER
    }

    return TaskApiContractTestPlan(
        plan_id=_optional_text(plan.get("id")),
        recommendations=result,
        api_task_ids=tuple(item.task_id for item in result),
        summary={
            "task_count": len(tasks),
            "api_task_count": len(result),
            "high_risk_count": risk_counts["high"],
            "medium_risk_count": risk_counts["medium"],
            "low_risk_count": risk_counts["low"],
            "missing_acceptance_criteria_count": sum(
                len(item.missing_acceptance_criteria) for item in result
            ),
        },
    )


def task_api_contract_test_plan_to_dict(
    result: TaskApiContractTestPlan,
) -> dict[str, Any]:
    """Serialize an API contract-test plan to a plain dictionary."""
    return result.to_dict()


task_api_contract_test_plan_to_dict.__test__ = False


def recommend_task_api_contract_tests(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TaskApiContractTestPlan:
    """Compatibility alias for building API contract-test recommendations."""
    return build_task_api_contract_test_plan(source)


def _recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskApiContractTestRecommendation | None:
    evidence = _api_evidence(task)
    if not evidence:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task)
    surfaces = _contract_surfaces(task, context)
    test_types = _suggested_test_types(context)
    missing = _missing_acceptance_criteria(test_types, task)
    risk = _risk_level(context, missing)

    return TaskApiContractTestRecommendation(
        task_id=task_id,
        title=title,
        contract_surfaces=surfaces,
        suggested_test_types=test_types,
        missing_acceptance_criteria=missing,
        risk_level=risk,
        evidence=tuple(_dedupe(evidence)),
    )


def _api_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_API_RE.search(_normalized_path(path)):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        if _is_api_text(text):
            evidence.append(f"{source_field}: {text}")

    for source_field, text in _metadata_texts(task.get("metadata")):
        if _is_api_text(f"{source_field} {text}"):
            evidence.append(f"{source_field}: {text}")

    for source_field, text in _tag_texts(task):
        if _is_api_text(text):
            evidence.append(f"{source_field}: {text}")

    return evidence


def _is_api_text(text: str) -> bool:
    return bool(
        _REST_RE.search(text)
        or _GRAPHQL_RE.search(text)
        or _SCHEMA_RE.search(text)
        or _WEBHOOK_RE.search(text)
        or _CLIENT_RE.search(text)
        or _EXTERNAL_NAME_RE.search(text)
        or _HTTP_PATH_RE.search(text)
    )


def _contract_surfaces(task: Mapping[str, Any], context: str) -> tuple[str, ...]:
    surfaces: list[str] = []
    for match in _HTTP_PATH_RE.finditer(context):
        path = match.group(1) or match.group(2)
        if path:
            surfaces.append(f"REST endpoint {path.rstrip('.,;')}")

    if _GRAPHQL_RE.search(context):
        surfaces.append(f"GraphQL schema: {_surface_name(task)}")
    if _WEBHOOK_RE.search(context):
        surfaces.append(f"Webhook payload: {_surface_name(task)}")
    if _EXTERNAL_CLIENT_RE.search(context) or _EXTERNAL_NAME_RE.search(context):
        service = _external_service_name(context)
        surfaces.append(f"External service contract: {service or _surface_name(task)}")
    if _REST_SURFACE_RE.search(context) and not any(
        surface.startswith("REST endpoint") for surface in surfaces
    ):
        surfaces.append(f"REST endpoint: {_surface_name(task)}")

    return tuple(_dedupe(surfaces)) or (f"API contract: {_surface_name(task)}",)


def _suggested_test_types(context: str) -> tuple[ContractTestType, ...]:
    return (
        "schema",
        "request_validation",
        "response_shape",
        "error_case",
        "backward_compatibility",
    )


def _missing_acceptance_criteria(
    test_types: tuple[ContractTestType, ...],
    task: Mapping[str, Any],
) -> tuple[ContractTestType, ...]:
    acceptance_text = " ".join(_strings(task.get("acceptance_criteria")))
    evidence_by_type: dict[ContractTestType, re.Pattern[str]] = {
        "schema": _SCHEMA_EVIDENCE_RE,
        "request_validation": _REQUEST_EVIDENCE_RE,
        "response_shape": _RESPONSE_EVIDENCE_RE,
        "error_case": _ERROR_EVIDENCE_RE,
        "backward_compatibility": _COMPAT_EVIDENCE_RE,
    }
    return tuple(
        test_type
        for test_type in test_types
        if not evidence_by_type[test_type].search(acceptance_text)
    )


def _risk_level(
    context: str,
    missing_acceptance_criteria: tuple[ContractTestType, ...],
) -> ApiContractRisk:
    if (
        _WEBHOOK_RE.search(context)
        or _EXTERNAL_NAME_RE.search(context)
        or _EXTERNAL_CLIENT_RE.search(context)
    ):
        return "high"
    if len(missing_acceptance_criteria) >= 3:
        return "high"
    if missing_acceptance_criteria:
        return "medium"
    return "low"


def _surface_name(task: Mapping[str, Any]) -> str:
    metadata = task.get("metadata")
    for value in (
        task.get("surface"),
        task.get("api_surface"),
        metadata.get("surface") if isinstance(metadata, Mapping) else None,
        metadata.get("api_surface") if isinstance(metadata, Mapping) else None,
    ):
        if text := _optional_text(value):
            return text

    title = _optional_text(task.get("title")) or _optional_text(task.get("id")) or "API"
    words = [
        word
        for word in _TOKEN_RE.findall(title.casefold())
        if word not in _GENERIC_WORDS and len(word) > 1
    ]
    return " ".join(words[:5]) or title


def _external_service_name(context: str) -> str | None:
    if match := _EXTERNAL_NAME_RE.search(context):
        return match.group(0).title()
    if re.search(r"\bexternal service\b", context, re.IGNORECASE):
        return "external service"
    return None


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
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
    ):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("depends_on"))):
        texts.append((f"depends_on[{index}]", text))
    return texts


def _tag_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
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


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(text for _, text in _metadata_texts(task.get("metadata")))
    values.extend(text for _, text in _tag_texts(task))
    return " ".join(values)


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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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
    "ApiContractRisk",
    "ContractTestType",
    "TaskApiContractTestPlan",
    "TaskApiContractTestRecommendation",
    "build_task_api_contract_test_plan",
    "recommend_task_api_contract_tests",
    "task_api_contract_test_plan_to_dict",
]
