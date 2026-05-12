"""Build plan-level API idempotency readiness matrices."""

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


ApiIdempotencyReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiIdempotencyReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_API_IDEMPOTENCY_RE = re.compile(
    r"\b(?:api|endpoint|route|rest|graphql|sdk|webhook|post|put|patch|payment|order|checkout|"
    r"create|creation|retry|retries|duplicate request|idempotenc(?:y|e)|idempotency key)\b",
    re.I,
)
_MUTATION_RE = re.compile(
    r"\b(?:post|put|patch|mutation|create|creation|submit|checkout|payment|charge|order|provision|write)\b",
    re.I,
)
_SIGNALS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("idempotency_key", "Missing idempotency key contract.", re.compile(r"\b(?:idempotency key|idempotency-key|dedupe key|request key|operation id|client token)\b", re.I)),
    ("retry_semantics", "Missing retry semantics.", re.compile(r"\b(?:retry|retries|retryable|backoff|retry-after|safe to retry|at least once|transient)\b", re.I)),
    ("duplicate_suppression", "Missing duplicate suppression.", re.compile(r"\b(?:duplicate|dedupe|de-dupe|suppress|de-duplicate|replay protection|exactly once|once only)\b", re.I)),
    ("conflict_response", "Missing conflict response behavior.", re.compile(r"\b(?:409|conflict|already exists|same response|cached response|status code|problem details)\b", re.I)),
    ("observability", "Missing idempotency observability.", re.compile(r"\b(?:metric|monitor|alert|dashboard|log|trace|telemetry|audit|correlation id)\b", re.I)),
    ("rollback_criteria", "Missing rollback criteria.", re.compile(r"\b(?:rollback|roll back|revert|fallback|disable|kill switch|abort|recovery)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class PlanApiIdempotencyReadinessRow:
    """One API idempotency readiness row."""

    task_id: str
    title: str
    idempotency_key: str = "missing"
    retry_semantics: str = "missing"
    duplicate_suppression: str = "missing"
    conflict_response: str = "missing"
    observability: str = "missing"
    rollback_criteria: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiIdempotencyReadiness = "partial"
    readiness_score: float = 0.0
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "idempotency_key": self.idempotency_key,
            "retry_semantics": self.retry_semantics,
            "duplicate_suppression": self.duplicate_suppression,
            "conflict_response": self.conflict_response,
            "observability": self.observability,
            "rollback_criteria": self.rollback_criteria,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "readiness_score": self.readiness_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanApiIdempotencyReadinessMatrix:
    """Plan-level API idempotency readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanApiIdempotencyReadinessRow, ...] = field(default_factory=tuple)
    idempotency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_idempotency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanApiIdempotencyReadinessRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "idempotency_task_ids": list(self.idempotency_task_ids),
            "no_idempotency_task_ids": list(self.no_idempotency_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan API Idempotency Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            f"Summary: {self.summary.get('idempotency_task_count', 0)} of {self.summary.get('task_count', 0)} tasks require API idempotency readiness (blocked: {counts.get('blocked', 0)}, partial: {counts.get('partial', 0)}, ready: {counts.get('ready', 0)}).",
        ]
        if not self.rows:
            lines.extend(["", "No API idempotency readiness rows were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Task | Title | Key | Retry | Duplicate Suppression | Conflict | Observability | Rollback | Readiness | Score | Gaps | Evidence |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"])
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | {_markdown_cell(row.title)} | {row.idempotency_key} | "
                f"{row.retry_semantics} | {row.duplicate_suppression} | {row.conflict_response} | "
                f"{row.observability} | {row.rollback_criteria} | {row.readiness} | {row.readiness_score:.2f} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | {_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiIdempotencyReadinessRow] = []
    skipped: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _row(task, index)
        if row is None:
            skipped.append(_task_id(task, index))
        else:
            rows.append(row)
    sorted_rows = tuple(sorted(rows, key=_row_sort_key))
    return PlanApiIdempotencyReadinessMatrix(
        plan_id=plan_id,
        rows=sorted_rows,
        idempotency_task_ids=tuple(row.task_id for row in sorted_rows),
        no_idempotency_task_ids=tuple(skipped),
        summary=_summary(len(tasks), sorted_rows, skipped),
    )


def generate_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    if isinstance(source, PlanApiIdempotencyReadinessMatrix):
        return source
    return build_plan_api_idempotency_readiness_matrix(source)


def analyze_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    if isinstance(source, PlanApiIdempotencyReadinessMatrix):
        return source
    return build_plan_api_idempotency_readiness_matrix(source)


def derive_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    return analyze_plan_api_idempotency_readiness_matrix(source)


def extract_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    return derive_plan_api_idempotency_readiness_matrix(source)


def summarize_plan_api_idempotency_readiness_matrix(
    source: PlanApiIdempotencyReadinessMatrix | Iterable[PlanApiIdempotencyReadinessRow] | Any,
) -> dict[str, Any] | PlanApiIdempotencyReadinessMatrix:
    if isinstance(source, PlanApiIdempotencyReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_api_idempotency_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_api_idempotency_readiness_matrix_to_dict(matrix: PlanApiIdempotencyReadinessMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_api_idempotency_readiness_matrix_to_dict.__test__ = False


def plan_api_idempotency_readiness_matrix_to_dicts(
    matrix: PlanApiIdempotencyReadinessMatrix | Iterable[PlanApiIdempotencyReadinessRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanApiIdempotencyReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_api_idempotency_readiness_matrix_to_dicts.__test__ = False


def plan_api_idempotency_readiness_matrix_to_markdown(matrix: PlanApiIdempotencyReadinessMatrix) -> str:
    return matrix.to_markdown()


plan_api_idempotency_readiness_matrix_to_markdown.__test__ = False


def _row(task: Mapping[str, Any], index: int) -> PlanApiIdempotencyReadinessRow | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_API_IDEMPOTENCY_RE.search(context) and (_MUTATION_RE.search(context) or "idempot" in context.casefold())):
        return None
    statuses = {name: _status(pattern, texts) for name, _, pattern in _SIGNALS}
    gaps = tuple(message for name, message, _ in _SIGNALS if statuses[name] == "missing")
    present = sum(1 for value in statuses.values() if value == "present")
    readiness = _readiness(statuses)
    evidence = tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _API_IDEMPOTENCY_RE.search(text) or any(pattern.search(text) for _, _, pattern in _SIGNALS)))
    return PlanApiIdempotencyReadinessRow(
        task_id=task_id,
        title=title,
        gaps=gaps,
        readiness=readiness,
        readiness_score=round(present / len(_SIGNALS), 2),
        evidence=evidence,
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _readiness(statuses: Mapping[str, str]) -> ApiIdempotencyReadiness:
    if statuses["idempotency_key"] == "missing" or statuses["duplicate_suppression"] == "missing":
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _summary(task_count: int, rows: Iterable[PlanApiIdempotencyReadinessRow], skipped: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "idempotency_task_count": len(row_list),
        "no_idempotency_task_count": len(tuple(skipped)),
        "readiness_counts": {readiness: sum(1 for row in row_list if row.readiness == readiness) for readiness in _READINESS_ORDER},
        "gap_counts": {gap: sum(1 for row in row_list if gap in row.gaps) for gap in sorted({gap for row in row_list for gap in row.gaps})},
    }


def _row_sort_key(row: PlanApiIdempotencyReadinessRow) -> tuple[int, str]:
    return (_READINESS_ORDER[row.readiness], row.task_id)
