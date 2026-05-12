"""Build plan-level background job readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.plan_secrets_rotation_readiness_matrix import _candidate_texts, _dedupe, _evidence_snippet, _markdown_cell, _optional_text, _source_payload, _task_id

BackgroundJobReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[BackgroundJobReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_JOB_RE = re.compile(r"\b(?:background job|async job|queue|worker|cron|scheduler|scheduled task|batch processing|batch job|task queue|celery|sidekiq)\b", re.I)
_RETRY_RE = re.compile(r"\b(?:retry|retries|backoff|exponential|retryable|attempts)\b", re.I)
_IDEMPOTENCY_RE = re.compile(r"\b(?:idempotent|idempotency|dedupe|duplicate|exactly once|operation key)\b", re.I)
_TIMEOUT_RE = re.compile(r"\b(?:timeout|deadline|max runtime|lease|heartbeat|stuck job|lock expiry)\b", re.I)
_POISON_RE = re.compile(r"\b(?:poison|dead[- ]letter|dlq|quarantine|failed queue|parking lot|discard)\b", re.I)
_PROGRESS_RE = re.compile(r"\b(?:progress|status|percent|checkpoint|resume|job state|tracking)\b", re.I)
_CANCEL_RE = re.compile(r"\b(?:cancel|cancellation|abort|pause|stop|terminate|kill)\b", re.I)
_MONITOR_RE = re.compile(r"\b(?:monitor|metric|alert|dashboard|log|logs|trace|observability|runbook)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanBackgroundJobReadinessRow:
    task_id: str
    title: str
    retry_backoff: str = "missing"
    idempotency: str = "missing"
    timeout_handling: str = "missing"
    poison_message_handling: str = "missing"
    progress_tracking: str = "missing"
    cancellation: str = "missing"
    operational_monitoring: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: BackgroundJobReadiness = "partial"
    readiness_score: float = 0.0
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "retry_backoff": self.retry_backoff,
            "idempotency": self.idempotency,
            "timeout_handling": self.timeout_handling,
            "poison_message_handling": self.poison_message_handling,
            "progress_tracking": self.progress_tracking,
            "cancellation": self.cancellation,
            "operational_monitoring": self.operational_monitoring,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "readiness_score": self.readiness_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanBackgroundJobReadinessMatrix:
    plan_id: str | None = None
    rows: tuple[PlanBackgroundJobReadinessRow, ...] = field(default_factory=tuple)
    background_job_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_background_job_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanBackgroundJobReadinessRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "background_job_task_ids": list(self.background_job_task_ids),
            "no_background_job_task_ids": list(self.no_background_job_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan Background Job Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, ""]
        if not self.rows:
            lines.append("No background job readiness rows were inferred.")
            return "\n".join(lines)
        lines.extend([
            "| Task | Title | Retry | Idempotency | Timeout | Poison | Progress | Cancel | Monitoring | Readiness | Score | Gaps |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {_markdown_cell(row.title)} | {row.retry_backoff} | {row.idempotency} | "
                f"{row.timeout_handling} | {row.poison_message_handling} | {row.progress_tracking} | "
                f"{row.cancellation} | {row.operational_monitoring} | {row.readiness} | {row.readiness_score:.2f} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanBackgroundJobReadinessRow] = []
    no_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_ids.append(_task_id(task, index))
    rows.sort(key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id))
    result = tuple(rows)
    return PlanBackgroundJobReadinessMatrix(plan_id, result, tuple(row.task_id for row in result), tuple(no_ids), _summary(len(tasks), result))


def analyze_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    if isinstance(source, PlanBackgroundJobReadinessMatrix):
        return source
    return build_plan_background_job_readiness_matrix(source)


def generate_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    return build_plan_background_job_readiness_matrix(source)


def plan_background_job_readiness_matrix_to_dict(matrix: PlanBackgroundJobReadinessMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_background_job_readiness_matrix_to_dict.__test__ = False


def plan_background_job_readiness_matrix_to_dicts(matrix: PlanBackgroundJobReadinessMatrix | Iterable[PlanBackgroundJobReadinessRow]) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanBackgroundJobReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_background_job_readiness_matrix_to_dicts.__test__ = False


def plan_background_job_readiness_matrix_to_markdown(matrix: PlanBackgroundJobReadinessMatrix) -> str:
    return matrix.to_markdown()


plan_background_job_readiness_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanBackgroundJobReadinessRow | None:
    texts = _candidate_texts(task)
    if not any(_JOB_RE.search(text) for _, text in texts):
        return None
    statuses = {
        "retry_backoff": _status(_RETRY_RE, texts),
        "idempotency": _status(_IDEMPOTENCY_RE, texts),
        "timeout_handling": _status(_TIMEOUT_RE, texts),
        "poison_message_handling": _status(_POISON_RE, texts),
        "progress_tracking": _status(_PROGRESS_RE, texts),
        "cancellation": _status(_CANCEL_RE, texts),
        "operational_monitoring": _status(_MONITOR_RE, texts),
    }
    gaps = tuple(f"Missing {label}." for field, label in (
        ("retry_backoff", "retry/backoff"),
        ("idempotency", "idempotency"),
        ("timeout_handling", "timeout handling"),
        ("poison_message_handling", "poison-message handling"),
        ("progress_tracking", "progress tracking"),
        ("cancellation", "cancellation"),
        ("operational_monitoring", "operational monitoring"),
    ) if statuses[field] == "missing")
    score = round(sum(1 for status in statuses.values() if status == "present") / len(statuses), 2)
    if statuses["retry_backoff"] == "missing" or statuses["idempotency"] == "missing":
        readiness: BackgroundJobReadiness = "blocked"
    elif gaps:
        readiness = "partial"
    else:
        readiness = "ready"
    return PlanBackgroundJobReadinessRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        readiness_score=score,
        evidence=tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _JOB_RE.search(text))),
        **statuses,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[PlanBackgroundJobReadinessRow]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "background_job_task_count": len(row_list),
        "no_background_job_task_count": task_count - len(row_list),
        "readiness_counts": {name: sum(1 for row in row_list if row.readiness == name) for name in _READINESS_ORDER},
        "average_readiness_score": round(sum(row.readiness_score for row in row_list) / len(row_list), 2) if row_list else 0.0,
    }


__all__ = [
    "BackgroundJobReadiness",
    "PlanBackgroundJobReadinessMatrix",
    "PlanBackgroundJobReadinessRow",
    "analyze_plan_background_job_readiness_matrix",
    "build_plan_background_job_readiness_matrix",
    "generate_plan_background_job_readiness_matrix",
    "plan_background_job_readiness_matrix_to_dict",
    "plan_background_job_readiness_matrix_to_dicts",
    "plan_background_job_readiness_matrix_to_markdown",
]
