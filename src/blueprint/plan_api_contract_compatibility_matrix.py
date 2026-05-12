"""Build plan-level API contract compatibility matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_cache_invalidation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _looks_like_plan,
    _looks_like_task,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


ApiContractCompatibilityReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiContractCompatibilityReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_COMPATIBILITY_RE = re.compile(r"\b(?:api|endpoint|sdk|graphql|schema|webhook|contract|breaking|backward|compatib|version|openapi|client|consumer)\b", re.I)
_SIGNALS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("versioning", "Missing versioning strategy.", re.compile(r"\b(?:version|v[0-9]+|semver|compatibility window|media type|header version)\b", re.I)),
    ("backward_compatibility", "Missing backward compatibility plan.", re.compile(r"\b(?:backward|backwards|compatible|non-breaking|additive|deprecated field|optional field)\b", re.I)),
    ("schema_examples", "Missing schema examples.", re.compile(r"\b(?:schema|openapi|swagger|graphql type|example|sample payload|contract fixture)\b", re.I)),
    ("consumer_testing", "Missing consumer testing.", re.compile(r"\b(?:consumer tests?|contract tests?|pact|sdk tests?|integration tests?|client tests?|downstream tests?)\b", re.I)),
    ("changelog_notes", "Missing changelog notes.", re.compile(r"\b(?:changelog|release note|migration guide|docs|developer portal|announcement)\b", re.I)),
    ("rollout_guardrails", "Missing rollout guardrails.", re.compile(r"\b(?:rollout|canary|feature flag|guardrail|rollback|monitor|alert|traffic split)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class PlanApiContractCompatibilityRow:
    task_id: str
    title: str
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
        counts = self.summary.get("readiness_counts", {})
        lines = [title, "", f"Summary: {self.summary.get('compatibility_task_count', 0)} of {self.summary.get('task_count', 0)} tasks require API contract compatibility review (blocked: {counts.get('blocked', 0)}, partial: {counts.get('partial', 0)}, ready: {counts.get('ready', 0)})."]
        if not self.rows:
            lines.extend(["", "No API contract compatibility rows were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Task | Title | Versioning | Backward Compatibility | Schema Examples | Consumer Testing | Changelog | Rollout | Readiness | Score | Gaps | Evidence |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"])
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | {_markdown_cell(row.title)} | {row.versioning} | {row.backward_compatibility} | "
                f"{row.schema_examples} | {row.consumer_testing} | {row.changelog_notes} | {row.rollout_guardrails} | "
                f"{row.readiness} | {row.readiness_score:.2f} | {_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiContractCompatibilityRow] = []
    skipped: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _row(task, index)
        if row is None:
            skipped.append(_task_id(task, index))
        else:
            rows.append(row)
    sorted_rows = tuple(sorted(rows, key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id)))
    return PlanApiContractCompatibilityMatrix(
        plan_id=plan_id,
        rows=sorted_rows,
        compatibility_task_ids=tuple(row.task_id for row in sorted_rows),
        no_compatibility_task_ids=tuple(skipped),
        summary=_summary(len(tasks), sorted_rows, skipped),
    )


def generate_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    if isinstance(source, PlanApiContractCompatibilityMatrix):
        return source
    return build_plan_api_contract_compatibility_matrix(source)


def analyze_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    return generate_plan_api_contract_compatibility_matrix(source)


def derive_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    return analyze_plan_api_contract_compatibility_matrix(source)


def extract_plan_api_contract_compatibility_matrix(source: Any) -> PlanApiContractCompatibilityMatrix:
    return derive_plan_api_contract_compatibility_matrix(source)


def summarize_plan_api_contract_compatibility_matrix(
    source: PlanApiContractCompatibilityMatrix | Iterable[PlanApiContractCompatibilityRow] | Any,
) -> dict[str, Any] | PlanApiContractCompatibilityMatrix:
    if isinstance(source, PlanApiContractCompatibilityMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_api_contract_compatibility_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_api_contract_compatibility_matrix_to_dict(matrix: PlanApiContractCompatibilityMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_contract_compatibility_matrix_to_dict.__test__ = False


def plan_api_contract_compatibility_matrix_to_dicts(
    matrix: PlanApiContractCompatibilityMatrix | Iterable[PlanApiContractCompatibilityRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiContractCompatibilityMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_contract_compatibility_matrix_to_dicts.__test__ = False


def plan_api_contract_compatibility_matrix_to_markdown(matrix: PlanApiContractCompatibilityMatrix) -> str:
    return matrix.to_markdown()


plan_api_contract_compatibility_matrix_to_markdown.__test__ = False


def _row(task: Mapping[str, Any], index: int) -> PlanApiContractCompatibilityRow | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not _COMPATIBILITY_RE.search(context):
        return None
    statuses = {name: _status(pattern, texts) for name, _, pattern in _SIGNALS}
    gaps = tuple(message for name, message, _ in _SIGNALS if statuses[name] == "missing")
    present = sum(1 for value in statuses.values() if value == "present")
    evidence = tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _COMPATIBILITY_RE.search(text) or any(pattern.search(text) for _, _, pattern in _SIGNALS)))
    return PlanApiContractCompatibilityRow(
        task_id=task_id,
        title=title,
        gaps=gaps,
        readiness=_readiness(statuses),
        readiness_score=round(present / len(_SIGNALS), 2),
        evidence=evidence,
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _readiness(statuses: Mapping[str, str]) -> ApiContractCompatibilityReadiness:
    if statuses["backward_compatibility"] == "missing" or statuses["schema_examples"] == "missing":
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _summary(task_count: int, rows: Iterable[PlanApiContractCompatibilityRow], skipped: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "compatibility_task_count": len(row_list),
        "no_compatibility_task_count": len(tuple(skipped)),
        "readiness_counts": {readiness: sum(1 for row in row_list if row.readiness == readiness) for readiness in _READINESS_ORDER},
        "gap_counts": {gap: sum(1 for row in row_list if gap in row.gaps) for gap in sorted({gap for row in row_list for gap in row.gaps})},
    }
