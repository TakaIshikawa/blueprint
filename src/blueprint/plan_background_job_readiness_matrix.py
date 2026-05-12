"""Build plan-level background job readiness matrices."""

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


BackgroundJobReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[BackgroundJobReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_BACKGROUND_JOB_RE = re.compile(
    r"\b(?:background job|async job|queue|queued|worker|cron|scheduler|scheduled|batch|batch-processing|"
    r"job runner|task runner|celery|sidekiq|rq|kafka consumer|message consumer)\b",
    re.I,
)
_SIGNALS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    ("retry_backoff", "Missing retry and backoff policy.", re.compile(r"\b(?:retry|retries|backoff|exponential|attempt|retry-after|transient)\b", re.I)),
    ("idempotency", "Missing idempotency controls.", re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplicate|duplicate|exactly once|operation key)\b", re.I)),
    ("timeout", "Missing timeout or deadline.", re.compile(r"\b(?:timeout|time out|deadline|max runtime|lease|heartbeat|visibility timeout)\b", re.I)),
    ("poison_message_handling", "Missing poison-message handling.", re.compile(r"\b(?:poison|dead[- ]letter|dlq|quarantine|discard|parking lot|failed queue)\b", re.I)),
    ("progress_tracking", "Missing progress tracking.", re.compile(r"\b(?:progress|status|checkpoint|percent|job state|heartbeat|cursor|resume)\b", re.I)),
    ("cancellation", "Missing cancellation controls.", re.compile(r"\b(?:cancel|cancellation|abort|stop job|kill switch|pause|resume)\b", re.I)),
    ("operational_monitoring", "Missing operational monitoring.", re.compile(r"\b(?:monitor|monitoring|metrics?|alerts?|dashboard|logs?|telemetry|slo|queue depth|lag|runbook)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class PlanBackgroundJobReadinessRow:
    task_id: str
    title: str
    retry_backoff: str = "missing"
    idempotency: str = "missing"
    timeout: str = "missing"
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
            "timeout": self.timeout,
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
        counts = self.summary.get("readiness_counts", {})
        lines = [title, "", f"Summary: {self.summary.get('background_job_task_count', 0)} of {self.summary.get('task_count', 0)} tasks require background job readiness (blocked: {counts.get('blocked', 0)}, partial: {counts.get('partial', 0)}, ready: {counts.get('ready', 0)})."]
        if not self.rows:
            lines.extend(["", "No background job readiness rows were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Task | Title | Retry/Backoff | Idempotency | Timeout | Poison Messages | Progress | Cancellation | Monitoring | Readiness | Score | Gaps | Evidence |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"])
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | {_markdown_cell(row.title)} | {row.retry_backoff} | {row.idempotency} | "
                f"{row.timeout} | {row.poison_message_handling} | {row.progress_tracking} | {row.cancellation} | "
                f"{row.operational_monitoring} | {row.readiness} | {row.readiness_score:.2f} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | {_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanBackgroundJobReadinessRow] = []
    skipped: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _row(task, index)
        if row is None:
            skipped.append(_task_id(task, index))
        else:
            rows.append(row)
    sorted_rows = tuple(sorted(rows, key=lambda row: (_READINESS_ORDER[row.readiness], row.task_id)))
    return PlanBackgroundJobReadinessMatrix(
        plan_id=plan_id,
        rows=sorted_rows,
        background_job_task_ids=tuple(row.task_id for row in sorted_rows),
        no_background_job_task_ids=tuple(skipped),
        summary=_summary(len(tasks), sorted_rows, skipped),
    )


def generate_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    if isinstance(source, PlanBackgroundJobReadinessMatrix):
        return source
    return build_plan_background_job_readiness_matrix(source)


def analyze_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    return generate_plan_background_job_readiness_matrix(source)


def derive_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    return analyze_plan_background_job_readiness_matrix(source)


def extract_plan_background_job_readiness_matrix(source: Any) -> PlanBackgroundJobReadinessMatrix:
    return derive_plan_background_job_readiness_matrix(source)


def summarize_plan_background_job_readiness_matrix(
    source: PlanBackgroundJobReadinessMatrix | Iterable[PlanBackgroundJobReadinessRow] | Any,
) -> dict[str, Any] | PlanBackgroundJobReadinessMatrix:
    if isinstance(source, PlanBackgroundJobReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_background_job_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_background_job_readiness_matrix_to_dict(matrix: PlanBackgroundJobReadinessMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_background_job_readiness_matrix_to_dict.__test__ = False


def plan_background_job_readiness_matrix_to_dicts(
    matrix: PlanBackgroundJobReadinessMatrix | Iterable[PlanBackgroundJobReadinessRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanBackgroundJobReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_background_job_readiness_matrix_to_dicts.__test__ = False


def plan_background_job_readiness_matrix_to_markdown(matrix: PlanBackgroundJobReadinessMatrix) -> str:
    return matrix.to_markdown()


plan_background_job_readiness_matrix_to_markdown.__test__ = False


def _row(task: Mapping[str, Any], index: int) -> PlanBackgroundJobReadinessRow | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    if not _BACKGROUND_JOB_RE.search(context):
        return None
    statuses = {name: _status(pattern, texts) for name, _, pattern in _SIGNALS}
    gaps = tuple(message for name, message, _ in _SIGNALS if statuses[name] == "missing")
    present = sum(1 for value in statuses.values() if value == "present")
    evidence = tuple(_dedupe(_evidence_snippet(field, text) for field, text in texts if _BACKGROUND_JOB_RE.search(text) or any(pattern.search(text) for _, _, pattern in _SIGNALS)))
    return PlanBackgroundJobReadinessRow(
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


def _readiness(statuses: Mapping[str, str]) -> BackgroundJobReadiness:
    if statuses["retry_backoff"] == "missing" or statuses["idempotency"] == "missing":
        return "blocked"
    if any(value == "missing" for value in statuses.values()):
        return "partial"
    return "ready"


def _summary(task_count: int, rows: Iterable[PlanBackgroundJobReadinessRow], skipped: Iterable[str]) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "background_job_task_count": len(row_list),
        "no_background_job_task_count": len(tuple(skipped)),
        "readiness_counts": {readiness: sum(1 for row in row_list if row.readiness == readiness) for readiness in _READINESS_ORDER},
        "gap_counts": {gap: sum(1 for row in row_list if gap in row.gaps) for gap in sorted({gap for row in row_list for gap in row.gaps})},
    }
