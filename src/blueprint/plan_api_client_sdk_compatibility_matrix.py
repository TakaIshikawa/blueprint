"""Build plan-level API client SDK compatibility matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_secrets_rotation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


ApiClientSdkCompatibilityImpact = Literal["high", "medium", "low"]

_IMPACT_ORDER: dict[ApiClientSdkCompatibilityImpact, int] = {"high": 0, "medium": 1, "low": 2}
_SDK_RE = re.compile(
    r"\b(?:sdk|client library|generated client|typed client|api client|openapi|swagger|code[- ]?gen(?:eration)?)\b",
    re.I,
)
_CLIENT_RE = re.compile(
    r"\b(?:mobile|web client|frontend|ios|android|react native|flutter|electron|browser|app)\b",
    re.I,
)
_BREAKING_RE = re.compile(
    r"\b(?:breaking (?:change|field)|remove (?:field|endpoint)|rename|response shape|contract change|deprecat(?:e|ed|ion)|sunset)\b",
    re.I,
)
_SAMPLE_RE = re.compile(r"\b(?:sample|example|snippet|usage example|code sample|quickstart|cookbook)\b", re.I)
_DEPRECATION_RE = re.compile(
    r"\b(?:deprecation window|sunset date|migration window|end[- ]of[- ]life|eol|notice period|grace period)\b",
    re.I,
)
_CONTRACT_TEST_RE = re.compile(
    r"\b(?:consumer contract|contract test|pact|sdk test|client test|integration test|compatibility test)\b",
    re.I,
)
_OPENAPI_RE = re.compile(r"\b(?:openapi|swagger|spec|schema|api definition|protobuf|proto|graphql schema)\b", re.I)

_SAFEGUARD_PATTERNS: dict[str, re.Pattern[str]] = {
    "sdk_generation": re.compile(
        r"\b(?:generate|codegen|sdk generation|openapi[- ]generator|swagger[- ]codegen|generated? (?:client )?sdk|sdk (?:is )?(?:validate|generat))\b",
        re.I,
    ),
    "typed_client": re.compile(
        r"\b(?:typed|type[- ]safe|typescript client|strongly typed|type annotations|typed client)\b",
        re.I,
    ),
    "sample_requests": re.compile(
        r"\b(?:sample|example|snippet|cookbook|quickstart|usage|demo|code sample)\b",
        re.I,
    ),
    "contract_tests": re.compile(
        r"\b(?:consumer contract|contract tests?|pact|compatibility tests?|sdk tests?|client tests?)\b",
        re.I,
    ),
    "deprecation_window": re.compile(
        r"\b(?:deprecation window|sunset date|migration window|notice period|grace period|eol)\b",
        re.I,
    ),
    "version_negotiation": re.compile(
        r"\b(?:version negotiation|api version|version header|accept[- ]version|content negotiation|(?:old|new) (?:and|clients?)|both (?:old|new)|supports? (?:old|new) (?:and|clients?))\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanApiClientSdkCompatibilityRow:
    """Client SDK compatibility signals for one API change task."""

    task_id: str
    title: str
    affected_clients: tuple[str, ...]
    present_safeguards: tuple[str, ...]
    missing_safeguards: tuple[str, ...]
    recommended_validation: tuple[str, ...]
    impact: ApiClientSdkCompatibilityImpact
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "affected_clients": list(self.affected_clients),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_validation": list(self.recommended_validation),
            "impact": self.impact,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanApiClientSdkCompatibilityMatrix:
    """Plan-level API client SDK compatibility matrix."""

    plan_id: str | None = None
    rows: tuple[PlanApiClientSdkCompatibilityRow, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiClientSdkCompatibilityRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan API Client SDK Compatibility Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        if not self.rows:
            return "\n".join([title, "", "No API client SDK compatibility rows were inferred."])
        lines = [
            title,
            "",
            "| Task | Title | Affected Clients | Present Safeguards | Missing Safeguards | Recommended Validation | Impact |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | "
                f"{_markdown_cell(', '.join(row.affected_clients) or 'api_clients')} | "
                f"{_markdown_cell(', '.join(row.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(row.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(row.recommended_validation))} | "
                f"{row.impact} |"
            )
        return "\n".join(lines)


def build_plan_api_client_sdk_compatibility_matrix(source: Any) -> PlanApiClientSdkCompatibilityMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiClientSdkCompatibilityRow] = []
    no_impact_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_impact_task_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_IMPACT_ORDER[row.impact], -len(row.missing_safeguards), row.task_id))
    result = tuple(rows)
    return PlanApiClientSdkCompatibilityMatrix(
        plan_id=plan_id,
        rows=result,
        impacted_task_ids=tuple(row.task_id for row in result),
        no_impact_task_ids=tuple(no_impact_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_api_client_sdk_compatibility_matrix(source: Any) -> PlanApiClientSdkCompatibilityMatrix:
    return build_plan_api_client_sdk_compatibility_matrix(source)


def analyze_plan_api_client_sdk_compatibility_matrix(source: Any) -> PlanApiClientSdkCompatibilityMatrix:
    if isinstance(source, PlanApiClientSdkCompatibilityMatrix):
        return source
    return build_plan_api_client_sdk_compatibility_matrix(source)


def derive_plan_api_client_sdk_compatibility_matrix(source: Any) -> PlanApiClientSdkCompatibilityMatrix:
    return analyze_plan_api_client_sdk_compatibility_matrix(source)


def extract_plan_api_client_sdk_compatibility_matrix(source: Any) -> PlanApiClientSdkCompatibilityMatrix:
    return derive_plan_api_client_sdk_compatibility_matrix(source)


def summarize_plan_api_client_sdk_compatibility_matrix(
    source: PlanApiClientSdkCompatibilityMatrix | Iterable[PlanApiClientSdkCompatibilityRow] | Any,
) -> dict[str, Any] | PlanApiClientSdkCompatibilityMatrix:
    if isinstance(source, PlanApiClientSdkCompatibilityMatrix):
        return dict(source.summary)
    if isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)) or hasattr(source, "tasks") or hasattr(source, "title"):
        return build_plan_api_client_sdk_compatibility_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_api_client_sdk_compatibility_matrix_to_dict(matrix: PlanApiClientSdkCompatibilityMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_client_sdk_compatibility_matrix_to_dict.__test__ = False


def plan_api_client_sdk_compatibility_matrix_to_dicts(
    matrix: PlanApiClientSdkCompatibilityMatrix | Iterable[PlanApiClientSdkCompatibilityRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiClientSdkCompatibilityMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_client_sdk_compatibility_matrix_to_dicts.__test__ = False


def plan_api_client_sdk_compatibility_matrix_to_markdown(matrix: PlanApiClientSdkCompatibilityMatrix) -> str:
    return matrix.to_markdown()


plan_api_client_sdk_compatibility_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanApiClientSdkCompatibilityRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)

    # Must have SDK/client signal or OpenAPI/breaking change signal
    has_sdk_signal = _SDK_RE.search(context)
    has_client_signal = _CLIENT_RE.search(context)
    has_openapi_signal = _OPENAPI_RE.search(context)
    has_breaking_signal = _BREAKING_RE.search(context)

    if not (has_sdk_signal or has_client_signal or (has_openapi_signal and has_breaking_signal)):
        return None

    affected_clients = _detect_affected_clients(texts)
    present_safeguards = _detect_present_safeguards(texts)
    all_safeguards = set(_SAFEGUARD_PATTERNS.keys())
    missing_safeguards = tuple(sorted(all_safeguards - set(present_safeguards)))

    impact = _calculate_impact(texts, present_safeguards, missing_safeguards)
    recommended_validation = _build_recommendations(affected_clients, missing_safeguards, impact)

    return PlanApiClientSdkCompatibilityRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        affected_clients=tuple(affected_clients),
        present_safeguards=tuple(present_safeguards),
        missing_safeguards=missing_safeguards,
        recommended_validation=tuple(recommended_validation),
        impact=impact,
        evidence=tuple(
            _dedupe(
                _evidence_snippet(field, text)
                for field, text in texts
                if any(
                    pattern.search(text)
                    for pattern in [
                        _SDK_RE,
                        _CLIENT_RE,
                        _BREAKING_RE,
                        _OPENAPI_RE,
                        _CONTRACT_TEST_RE,
                    ]
                )
            )
        ),
    )


def _detect_affected_clients(texts: Iterable[tuple[str, str]]) -> list[str]:
    clients: list[str] = []
    context = " ".join(text for _, text in texts)

    if re.search(r"\b(?:sdk|client library|generated client|api client)\b", context, re.I):
        clients.append("sdk")
    if re.search(r"\b(?:mobile|ios|android|react native|flutter)\b", context, re.I):
        clients.append("mobile")
    if re.search(r"\b(?:web|browser|frontend|javascript|typescript)\b", context, re.I):
        clients.append("web")
    if re.search(r"\b(?:openapi|swagger|spec)\b", context, re.I):
        clients.append("openapi_consumers")
    if re.search(r"\b(?:graphql|schema)\b", context, re.I):
        clients.append("graphql_consumers")
    if re.search(r"\b(?:protobuf|grpc|proto)\b", context, re.I):
        clients.append("grpc_consumers")

    return _dedupe(clients) or ["api_clients"]


def _detect_present_safeguards(texts: Iterable[tuple[str, str]]) -> list[str]:
    safeguards: list[str] = []
    context = " ".join(text for _, text in texts)

    for safeguard_name, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(context):
            safeguards.append(safeguard_name)

    return safeguards


def _calculate_impact(
    texts: Iterable[tuple[str, str]],
    present_safeguards: list[str],
    missing_safeguards: tuple[str, ...],
) -> ApiClientSdkCompatibilityImpact:
    context = " ".join(text for _, text in texts)

    has_breaking_change = _BREAKING_RE.search(context)
    has_mobile_client = re.search(r"\b(?:mobile|ios|android)\b", context, re.I)
    has_contract_tests = "contract_tests" in present_safeguards
    has_deprecation_window = "deprecation_window" in present_safeguards

    # High impact: breaking changes without contract tests, mobile clients affected
    if has_breaking_change and not has_contract_tests:
        return "high"
    if has_mobile_client and len(missing_safeguards) >= 4:
        return "high"

    # Low impact: most safeguards present (2 or fewer missing)
    if len(missing_safeguards) <= 2:
        return "low"

    # Medium impact: moderate safeguards missing
    if len(missing_safeguards) >= 4:
        return "medium"
    if has_breaking_change and not has_deprecation_window:
        return "medium"

    # Default to low for 3 missing safeguards without breaking changes
    return "low"


def _build_recommendations(
    affected_clients: tuple[str, ...] | list[str],
    missing_safeguards: tuple[str, ...],
    impact: ApiClientSdkCompatibilityImpact,
) -> list[str]:
    recommendations: list[str] = []

    if "contract_tests" in missing_safeguards:
        recommendations.append("Add consumer contract tests covering SDK and client behavior.")
    if "sdk_generation" in missing_safeguards and "openapi_consumers" in affected_clients:
        recommendations.append("Ensure OpenAPI spec is updated and SDK generation is validated.")
    if "sample_requests" in missing_safeguards:
        recommendations.append("Update code samples and usage examples for affected clients.")
    if "deprecation_window" in missing_safeguards and impact in ("high", "medium"):
        recommendations.append("Document deprecation timeline and migration window.")
    if "typed_client" in missing_safeguards and "web" in affected_clients:
        recommendations.append("Verify typed client compatibility and regenerate if needed.")
    if "version_negotiation" in missing_safeguards and impact == "high":
        recommendations.append("Add version negotiation support for backward compatibility.")

    if not recommendations:
        recommendations.append("Verify client compatibility and update documentation.")

    return recommendations


def _summary(task_count: int, rows: Iterable[PlanApiClientSdkCompatibilityRow]) -> dict[str, Any]:
    row_list = list(rows)

    # Count client surfaces
    client_surfaces: dict[str, int] = {}
    for row in row_list:
        for client in row.affected_clients:
            client_surfaces[client] = client_surfaces.get(client, 0) + 1

    # Count safeguards
    safeguard_coverage: dict[str, int] = {}
    for safeguard in _SAFEGUARD_PATTERNS.keys():
        safeguard_coverage[safeguard] = sum(1 for row in row_list if safeguard in row.present_safeguards)

    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "impacted_task_count": len(row_list),
        "no_impact_task_count": task_count - len(row_list),
        "impact_counts": {
            impact: sum(1 for row in row_list if row.impact == impact)
            for impact in _IMPACT_ORDER
        },
        "client_surface_counts": client_surfaces,
        "safeguard_coverage": safeguard_coverage,
    }
