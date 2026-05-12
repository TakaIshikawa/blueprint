"""Build plan-level API contract compatibility matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.plan_secrets_rotation_readiness_matrix import _candidate_texts, _dedupe, _evidence_snippet, _markdown_cell, _optional_text, _source_payload, _task_id

ApiContractCompatibilityReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiContractCompatibilityReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SURFACE_RE = re.compile(r"\b(?:api|endpoint|route|rest|sdk|client library|graphql|schema|mutation|query|webhook|openapi|swagger|protobuf|grpc|contract)\b", re.I)
_CHANGE_RE = re.compile(r"\b(?:change|add|remove|rename|deprecat|breaking|compatible|contract|schema|field|response|request|payload|version|migration)\b", re.I)
_VERSION_RE = re.compile(r"\b(?:version|v[0-9]+|versioning|version header|accept[- ]version|content negotiation|sunset|deprecation window)\b", re.I)
_BACKCOMPAT_RE = re.compile(r"\b(?:backward compatible|backwards compatible|non[- ]breaking|optional field|preserve|legacy client|old client|compatible response|additive)\b", re.I)
_EXAMPLES_RE = re.compile(r"\b(?:schema example|example|sample|openapi|swagger|graphql schema|payload example|request example|response example|fixture)\b", re.I)
_CONSUMER_TEST_RE = re.compile(r"\b(?:consumer tests?|consumer contract|contract tests?|pact|integration tests?|sdk tests?|client tests?|compatibility tests?)\b", re.I)
_CHANGELOG_RE = re.compile(r"\b(?:changelog|release notes|migration guide|developer docs|docs update|announcement|upgrade notes)\b", re.I)
_ROLLOUT_RE = re.compile(r"\b(?:rollout|canary|feature flag|guardrail|gradual|monitor|rollback|kill switch|dual[- ]run|shadow)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanApiContractCompatibilityRow:
    task_id: str
    title: str
    surfaces: tuple[str, ...] = field(default_factory=tuple)
    versioning: str = "missing"
    backward_compatibility: str = "missing"
    schema_examples: str = "missing"
    consumer_testing: str = "missing"
    changelog_notes: str = "missing"
    rollout_guardrails: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiContractCompatibilityReadiness = "partial"
    readiness_score: float = 0.0
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "surfaces": list(self.surfaces),
            "versioning": self.versioning,
            "backward_compatibility": self.backward_compatibility,
            "schema_examples": self.schema_examples,
            "consumer_testing": self.consumer_testing,
            "changelog_notes": self.changelog_notes,
            "rollout_guardrails": self.rollout_guardrails,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "readiness_score": self.readiness_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanApiContractCompatibilityMatrix:
    plan_id: str | None = None
    rows: tuple[PlanApiContractCompatibilityRow, ...] = field(default_factory=tuple)
    compatibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_compatibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiContractCompatibilityRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "compatibility_task_ids": list(self.compatibility_task_ids),
            "no_compatibility_task_ids": list(self.no_compatibility_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan API Contract Compatibility Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, ""]
        if not self.rows:
            lines.append("No API contract compatibility rows were inferred.")
            return "\n".join(lines)
        lines.extend([
            "| Task | Title | Surfaces | Versioning | Backcompat | Examples | Consumer Tests | Changelog | Guardrails | Readiness | Score | Gaps |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | {_markdown_cell(', '.join(row.surfaces))} | "
                f"{row.versioning} | {row.backward_compatibility} | {row.schema_examples} | {row.consumer_testing} | "
                f"{row.changelog_notes} | {row.rollout_guardrails} | {row.readiness} | {row.readiness_score:.2f} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiContractCompatibilityRow] = []
    no_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id))
    result = tuple(rows)
    return PlanApiContractCompatibilityMatrix(plan_id, result, tuple(row.task_id for row in result), tuple(no_ids), _summary(len(tasks), result))


def analyze_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    if isinstance(source, PlanApiContractCompatibilityMatrix):
        return source
    return build_plan_api_contract_compatibility_matrix(source)


def generate_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    return build_plan_api_contract_compatibility_matrix(source)


def plan_api_contract_compatibility_matrix_to_dict(matrix: PlanApiContractCompatibilityMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_contract_compatibility_matrix_to_dict.__test__ = False


def plan_api_contract_compatibility_matrix_to_dicts(matrix: PlanApiContractCompatibilityMatrix | Iterable[PlanApiContractCompatibilityRow]) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiContractCompatibilityMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_contract_compatibility_matrix_to_dicts.__test__ = False


def plan_api_contract_compatibility_matrix_to_markdown(matrix: PlanApiContractCompatibilityMatrix) -> str:
    return matrix.to_markdown()


plan_api_contract_compatibility_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanApiContractCompatibilityRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_SURFACE_RE.search(context) and _CHANGE_RE.search(context)):
        return None
    statuses = {
        "versioning": _status(_VERSION_RE, texts),
        "backward_compatibility": _status(_BACKCOMPAT_RE, texts),
        "schema_examples": _status(_EXAMPLES_RE, texts),
        "consumer_testing": _status(_CONSUMER_TEST_RE, texts),
        "changelog_notes": _status(_CHANGELOG_RE, texts),
        "rollout_guardrails": _status(_ROLLOUT_RE, texts),
    }
    gaps = tuple(f"Missing {label}." for field, label in (
        ("versioning", "versioning strategy"),
        ("backward_compatibility", "backward compatibility plan"),
        ("schema_examples", "schema examples"),
        ("consumer_testing", "consumer testing"),
        ("changelog_notes", "changelog notes"),
        ("rollout_guardrails", "rollout guardrails"),
    ) if statuses[field] == "missing")
    score = round(sum(1 for status in statuses.values() if status == "present") / len(statuses), 2)
    if statuses["backward_compatibility"] == "missing" or statuses["consumer_testing"] == "missing":
        readiness: ApiContractCompatibilityReadiness = "blocked"
    elif gaps:
        readiness = "partial"
    else:
        readiness = "ready"
    return PlanApiContractCompatibilityRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        surfaces=tuple(_surfaces(context)),
        gaps=gaps,
        readiness=readiness,
        readiness_score=score,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _SURFACE_RE.search(text) or _CHANGE_RE.search(text))),
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _surfaces(context: str) -> tuple[str, ...]:
    values: list[str] = []
    for name, pattern in (
        ("endpoint", r"\b(?:api|endpoint|route|rest)\b"),
        ("sdk", r"\b(?:sdk|client library|generated client)\b"),
        ("graphql", r"\b(?:graphql|mutation|query)\b"),
        ("webhook", r"\b(?:webhook|callback)\b"),
        ("schema", r"\b(?:openapi|swagger|schema|protobuf|grpc)\b"),
    ):
        if re.search(pattern, context, re.I):
            values.append(name)
    return _dedupe(values) or ("api",)


def _summary(task_count: int, rows: Iterable[PlanApiContractCompatibilityRow]) -> dict[str, Any]:
    row_list = list(rows)
    surface_counts: dict[str, int] = {}
    for row in row_list:
        for surface in row.surfaces:
            surface_counts[surface] = surface_counts.get(surface, 0) + 1
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "compatibility_task_count": len(row_list),
        "no_compatibility_task_count": task_count - len(row_list),
        "readiness_counts": {name: sum(1 for row in row_list if row.readiness == name) for name in _READINESS_ORDER},
        "surface_counts": surface_counts,
        "average_readiness_score": round(sum(row.readiness_score for row in row_list) / len(row_list), 2) if row_list else 0.0,
    }


__all__ = [
    "ApiContractCompatibilityReadiness",
    "PlanApiContractCompatibilityMatrix",
    "PlanApiContractCompatibilityRow",
    "analyze_plan_api_contract_compatibility_matrix",
    "build_plan_api_contract_compatibility_matrix",
    "generate_plan_api_contract_compatibility_matrix",
    "plan_api_contract_compatibility_matrix_to_dict",
    "plan_api_contract_compatibility_matrix_to_dicts",
    "plan_api_contract_compatibility_matrix_to_markdown",
]
