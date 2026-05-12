"""Build plan-level API idempotency readiness matrices."""

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

ApiIdempotencyReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiIdempotencyReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_API_WRITE_RE = re.compile(
    r"\b(?:api|endpoint|route|rest|graphql|webhook|sdk|post|put|patch|payment|order|checkout|create|mutation)\b",
    re.I,
)
_IDEMPOTENCY_RE = re.compile(r"\b(?:idempotenc(?:y|e)|idempotent|duplicate request|duplicate submission|dedup(?:e|lication))\b", re.I)
_KEY_RE = re.compile(r"\b(?:idempotency[-_ ]?key|request key|dedupe key|operation id|client token|unique request)\b", re.I)
_RETRY_RE = re.compile(r"\b(?:retry|retries|retryable|backoff|at[- ]least[- ]once|network timeout|transient failure)\b", re.I)
_DUP_RE = re.compile(r"\b(?:duplicate suppression|dedupe|deduplication|duplicate request|replay protection|exactly once|once-only)\b", re.I)
_CONFLICT_RE = re.compile(r"\b(?:409|conflict|already exists|same response|cached response|request mismatch|validation error)\b", re.I)
_OBS_RE = re.compile(r"\b(?:log|logs|logging|metric|metrics|monitor|trace|audit|dashboard|alert|telemetry|observability)\b", re.I)
_ROLLBACK_RE = re.compile(r"\b(?:rollback|roll back|revert|compensat(?:e|ing)|refund|void|cancel|fallback|reconcile)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanApiIdempotencyReadinessRow:
    """Idempotency readiness signals for one API task."""

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
        lines = [title, ""]
        if not self.rows:
            lines.append("No API idempotency readiness rows were inferred.")
            return "\n".join(lines)
        lines.extend([
            "| Task | Title | Key | Retries | Duplicates | Conflicts | Observability | Rollback | Readiness | Score | Gaps | Evidence |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | "
                f"{row.idempotency_key} | {row.retry_semantics} | {row.duplicate_suppression} | "
                f"{row.conflict_response} | {row.observability} | {row.rollback_criteria} | "
                f"{row.readiness} | {row.readiness_score:.2f} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanApiIdempotencyReadinessRow] = []
    no_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id))
    result = tuple(rows)
    return PlanApiIdempotencyReadinessMatrix(
        plan_id=plan_id,
        rows=result,
        idempotency_task_ids=tuple(row.task_id for row in result),
        no_idempotency_task_ids=tuple(no_ids),
        summary=_summary(len(tasks), result),
    )


def analyze_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    if isinstance(source, PlanApiIdempotencyReadinessMatrix):
        return source
    return build_plan_api_idempotency_readiness_matrix(source)


def generate_plan_api_idempotency_readiness_matrix(source: Any) -> PlanApiIdempotencyReadinessMatrix:
    return build_plan_api_idempotency_readiness_matrix(source)


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


def _task_row(task: Mapping[str, Any], index: int) -> PlanApiIdempotencyReadinessRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not (_API_WRITE_RE.search(context) and (_IDEMPOTENCY_RE.search(context) or re.search(r"\b(?:post|put|patch|payment|order|checkout|create)\b", context, re.I))):
        return None
    statuses = {
        "idempotency_key": _status(_KEY_RE, texts),
        "retry_semantics": _status(_RETRY_RE, texts),
        "duplicate_suppression": _status(_DUP_RE, texts),
        "conflict_response": _status(_CONFLICT_RE, texts),
        "observability": _status(_OBS_RE, texts),
        "rollback_criteria": _status(_ROLLBACK_RE, texts),
    }
    gaps = tuple(f"Missing {label}." for field, label in (
        ("idempotency_key", "idempotency key handling"),
        ("retry_semantics", "retry semantics"),
        ("duplicate_suppression", "duplicate suppression"),
        ("conflict_response", "conflict response behavior"),
        ("observability", "observability"),
        ("rollback_criteria", "rollback criteria"),
    ) if statuses[field] == "missing")
    present = sum(1 for status in statuses.values() if status == "present")
    score = round(present / len(statuses), 2)
    if statuses["idempotency_key"] == "missing" or statuses["duplicate_suppression"] == "missing":
        readiness: ApiIdempotencyReadiness = "blocked"
    elif gaps:
        readiness = "partial"
    else:
        readiness = "ready"
    return PlanApiIdempotencyReadinessRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        readiness_score=score,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _API_WRITE_RE.search(text) or _IDEMPOTENCY_RE.search(text))),
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[PlanApiIdempotencyReadinessRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "idempotency_task_count": len(row_list),
        "no_idempotency_task_count": task_count - len(row_list),
        "readiness_counts": {name: sum(1 for row in row_list if row.readiness == name) for name in _READINESS_ORDER},
        "average_readiness_score": round(sum(row.readiness_score for row in row_list) / len(row_list), 2) if row_list else 0.0,
    }


__all__ = [
    "ApiIdempotencyReadiness",
    "PlanApiIdempotencyReadinessMatrix",
    "PlanApiIdempotencyReadinessRow",
    "analyze_plan_api_idempotency_readiness_matrix",
    "build_plan_api_idempotency_readiness_matrix",
    "generate_plan_api_idempotency_readiness_matrix",
    "plan_api_idempotency_readiness_matrix_to_dict",
    "plan_api_idempotency_readiness_matrix_to_dicts",
    "plan_api_idempotency_readiness_matrix_to_markdown",
]
