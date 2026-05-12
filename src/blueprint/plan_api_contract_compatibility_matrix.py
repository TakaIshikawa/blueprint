"""Build plan-level API contract compatibility readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiContractCompatibilityReadinessArea = Literal[
    "versioning",
    "backward_compatibility",
    "schema_examples",
    "consumer_testing",
    "changelog_notes",
    "rollout_guardrails",
]
ApiContractCompatibilityReadiness = Literal["ready", "partial", "blocked"]
ApiContractCompatibilityRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ApiContractCompatibilityReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_RISK_ORDER: dict[ApiContractCompatibilityRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AREA_ORDER: dict[ApiContractCompatibilityReadinessArea, int] = {
    "versioning": 0,
    "backward_compatibility": 1,
    "schema_examples": 2,
    "consumer_testing": 3,
    "changelog_notes": 4,
    "rollout_guardrails": 5,
}
_TASK_RE = re.compile(
    r"\b(?:api contract|endpoint|rest api|graphql|schema|sdk|client library|webhook|breaking change|"
    r"backward compatible|backwards compatible|versioning|openapi|consumer contract|compatibility)\b",
    re.I,
)
_AREA_PATTERNS: dict[ApiContractCompatibilityReadinessArea, re.Pattern[str]] = {
    "versioning": re.compile(r"\b(?:versioning|api version|v1|v2|version header|semver|deprecation window)\b", re.I),
    "backward_compatibility": re.compile(r"\b(?:backward compatible|backwards compatible|non[- ]breaking|additive|legacy client|legacy clients?)\b", re.I),
    "schema_examples": re.compile(r"\b(?:schema example|examples?|openapi|swagger|graphql schema|sample request|sample response|contract example)\b", re.I),
    "consumer_testing": re.compile(r"\b(?:consumer tests?|contract tests?|sdk tests?|integration tests?|client tests?|pact|compatibility tests?)\b", re.I),
    "changelog_notes": re.compile(r"\b(?:changelog|change log|release note|migration note|upgrade guide|developer docs)\b", re.I),
    "rollout_guardrails": re.compile(r"\b(?:rollout guardrail|feature flag|canary|phased rollout|monitoring|rollback|kill switch)\b", re.I),
}
_DEFAULT_OWNERS: dict[ApiContractCompatibilityReadinessArea, str] = {
    "versioning": "api_owner",
    "backward_compatibility": "api_owner",
    "schema_examples": "developer_experience_owner",
    "consumer_testing": "qa_owner",
    "changelog_notes": "developer_experience_owner",
    "rollout_guardrails": "release_owner",
}
_GAPS: dict[ApiContractCompatibilityReadinessArea, str] = {
    "versioning": "Missing versioning strategy.",
    "backward_compatibility": "Missing backward compatibility coverage.",
    "schema_examples": "Missing schema examples.",
    "consumer_testing": "Missing consumer testing.",
    "changelog_notes": "Missing changelog notes.",
    "rollout_guardrails": "Missing rollout guardrails.",
}
_ACTIONS: dict[ApiContractCompatibilityReadinessArea, str] = {
    "versioning": "Define API or SDK versioning and deprecation expectations for this contract change.",
    "backward_compatibility": "Document additive behavior, legacy-client behavior, and any breaking-change mitigations.",
    "schema_examples": "Add OpenAPI, GraphQL, SDK, or webhook payload examples for request and response shapes.",
    "consumer_testing": "Add contract, SDK, or integration tests that exercise existing and updated consumers.",
    "changelog_notes": "Publish changelog, migration, or developer-documentation notes for the contract delta.",
    "rollout_guardrails": "Add feature flags, canaries, monitoring, rollback criteria, or phased rollout controls.",
}
_HIGH_GAP_AREAS = frozenset({"versioning", "backward_compatibility", "consumer_testing"})
_OWNER_KEYS = ("owner", "owners", "owner_hint", "owner_team", "team", "dri", "api_owner", "backend_owner")


@dataclass(frozen=True, slots=True)
class PlanApiContractCompatibilityReadinessRow:
    """One plan-level API contract compatibility readiness row."""

    area: ApiContractCompatibilityReadinessArea
    owner: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiContractCompatibilityReadiness = "partial"
    risk: ApiContractCompatibilityRisk = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    score: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "area": self.area,
            "owner": self.owner,
            "evidence": list(self.evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "risk": self.risk,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
            "score": self.score,
        }


@dataclass(frozen=True, slots=True)
class PlanApiContractCompatibilityReadinessMatrix:
    """Plan-level API contract compatibility readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanApiContractCompatibilityReadinessRow, ...] = field(default_factory=tuple)
    compatibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    gap_areas: tuple[ApiContractCompatibilityReadinessArea, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiContractCompatibilityReadinessRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "compatibility_task_ids": list(self.compatibility_task_ids),
            "gap_areas": list(self.gap_areas),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan API Contract Compatibility Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ready_area_count', 0)} of "
                f"{self.summary.get('area_count', 0)} API contract compatibility readiness areas ready "
                f"(score: {self.summary.get('score', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No API contract compatibility readiness rows were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Area | Owner | Readiness | Score | Risk | Evidence | Gaps | Next Action | Tasks |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"])
        for row in self.rows:
            lines.append(
                "| "
                f"{row.area} | {_markdown_cell(row.owner)} | {row.readiness} | {row.score} | {row.risk} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell(row.next_action)} | {_markdown_cell(', '.join(row.task_ids) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_contract_compatibility_readiness_matrix(source: Any) -> PlanApiContractCompatibilityReadinessMatrix:
    """Build required API contract compatibility readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    evidence: dict[ApiContractCompatibilityReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    task_ids: dict[ApiContractCompatibilityReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    owners: dict[ApiContractCompatibilityReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    detected: list[str] = []
    blocked = False
    for index, task in enumerate(tasks, start=1):
        tid = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        task_signal = bool(_TASK_RE.search(context))
        blocked = blocked or bool(re.search(r"\b(?:blocked|cannot proceed|missing dependency)\b", context, re.I))
        hints = _owner_hints(task)
        for area, pattern in _AREA_PATTERNS.items():
            matches = [_evidence_snippet(field, text) for field, text in texts if pattern.search(text)]
            if matches:
                task_signal = True
                evidence[area].extend(matches)
                task_ids[area].append(tid)
                owners[area].extend(hints)
        if task_signal:
            detected.append(tid)
            for area in _AREA_ORDER:
                owners[area].extend(hints)
    rows = () if not detected else tuple(_row(area, evidence[area], task_ids[area], owners[area], blocked) for area in _AREA_ORDER)
    return PlanApiContractCompatibilityReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        compatibility_task_ids=tuple(_dedupe(detected)),
        gap_areas=tuple(row.area for row in rows if row.gaps),
        summary=_summary(len(tasks), rows, detected),
    )


def generate_plan_api_contract_compatibility_readiness_matrix(source: Any) -> PlanApiContractCompatibilityReadinessMatrix:
    return build_plan_api_contract_compatibility_readiness_matrix(source)


def plan_api_contract_compatibility_readiness_matrix_to_dict(matrix: PlanApiContractCompatibilityReadinessMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_contract_compatibility_readiness_matrix_to_dict.__test__ = False


def plan_api_contract_compatibility_readiness_matrix_to_dicts(
    matrix: PlanApiContractCompatibilityReadinessMatrix | Iterable[PlanApiContractCompatibilityReadinessRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiContractCompatibilityReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_contract_compatibility_readiness_matrix_to_dicts.__test__ = False


def plan_api_contract_compatibility_readiness_matrix_to_markdown(matrix: PlanApiContractCompatibilityReadinessMatrix) -> str:
    return matrix.to_markdown()


plan_api_contract_compatibility_readiness_matrix_to_markdown.__test__ = False


build_plan_api_contract_compatibility_matrix = build_plan_api_contract_compatibility_readiness_matrix
generate_plan_api_contract_compatibility_matrix = generate_plan_api_contract_compatibility_readiness_matrix
plan_api_contract_compatibility_matrix_to_dict = plan_api_contract_compatibility_readiness_matrix_to_dict
plan_api_contract_compatibility_matrix_to_dicts = plan_api_contract_compatibility_readiness_matrix_to_dicts
plan_api_contract_compatibility_matrix_to_markdown = plan_api_contract_compatibility_readiness_matrix_to_markdown


def _row(
    area: ApiContractCompatibilityReadinessArea,
    evidence: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
    blocked: bool,
) -> PlanApiContractCompatibilityReadinessRow:
    evidence_tuple = tuple(_dedupe(evidence))
    gaps = () if evidence_tuple else (_GAPS[area],)
    readiness: ApiContractCompatibilityReadiness = "ready" if not gaps else ("blocked" if blocked else "partial")
    risk: ApiContractCompatibilityRisk = "low" if not gaps else ("high" if area in _HIGH_GAP_AREAS or blocked else "medium")
    return PlanApiContractCompatibilityReadinessRow(
        area=area,
        owner=next(iter(_dedupe(owners)), _DEFAULT_OWNERS[area]),
        evidence=evidence_tuple,
        gaps=gaps,
        readiness=readiness,
        risk=risk,
        next_action="Ready for API contract compatibility handoff." if not gaps else _ACTIONS[area],
        task_ids=tuple(_dedupe(task_ids)),
        score=100 if not gaps else 0,
    )


def _summary(task_count: int, rows: Iterable[PlanApiContractCompatibilityReadinessRow], ids: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    total = sum(row.score for row in row_list)
    return {
        "task_count": task_count,
        "area_count": len(row_list),
        "ready_area_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_area_count": sum(1 for row in row_list if row.gaps),
        "compatibility_task_count": len(tuple(_dedupe(ids))),
        "score": round(total / len(row_list)) if row_list else 0,
        "readiness_counts": {key: sum(1 for row in row_list if row.readiness == key) for key in _READINESS_ORDER},
        "risk_counts": {key: sum(1 for row in row_list if row.risk == key) for key in _RISK_ORDER},
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            return _optional_text(source.get("id")), _task_payloads(source.get("tasks"))
        return None, [dict(source)]
    if not isinstance(source, (str, bytes)) and hasattr(source, "tasks"):
        return _optional_text(getattr(source, "id", None)), _task_payloads(getattr(source, "tasks", []))
    try:
        return None, _task_payloads(list(source))
    except TypeError:
        return None, []


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif not isinstance(item, (str, bytes)):
            tasks.append({name: getattr(item, name) for name in ("id", "title", "description", "acceptance_criteria", "metadata") if hasattr(item, name)})
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field in ("title", "description", "milestone", "owner_type", "risk_level", "blocked_reason"):
        if text := _optional_text(task.get(field)):
            texts.append((field, text))
    for field in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field))):
            texts.append((f"{field}[{index}]", text))
    for field, text in _metadata_texts(task.get("metadata")):
        texts.append((field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
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
        return [(f"{prefix}[{i}]", text) for i, item in enumerate(items) for text in _strings(item)]
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    owners = _strings(task.get("owner_type"))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings(metadata.get(key)))
    return _dedupe(owners)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [text] if (text := _optional_text(value)) else []
    if isinstance(value, Mapping):
        return [text for key in sorted(value, key=lambda item: str(item)) for text in _strings(value[key])]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    return [text] if (text := _optional_text(value)) else []


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
    "ApiContractCompatibilityReadiness",
    "ApiContractCompatibilityReadinessArea",
    "ApiContractCompatibilityRisk",
    "PlanApiContractCompatibilityReadinessMatrix",
    "PlanApiContractCompatibilityReadinessRow",
    "build_plan_api_contract_compatibility_matrix",
    "build_plan_api_contract_compatibility_readiness_matrix",
    "generate_plan_api_contract_compatibility_matrix",
    "generate_plan_api_contract_compatibility_readiness_matrix",
    "plan_api_contract_compatibility_matrix_to_dict",
    "plan_api_contract_compatibility_matrix_to_dicts",
    "plan_api_contract_compatibility_matrix_to_markdown",
    "plan_api_contract_compatibility_readiness_matrix_to_dict",
    "plan_api_contract_compatibility_readiness_matrix_to_dicts",
    "plan_api_contract_compatibility_readiness_matrix_to_markdown",
]
