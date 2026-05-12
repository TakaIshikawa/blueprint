"""Assess readiness for task data backfill, replay, and recompute work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDataBackfillReadinessPlan = SimpleReadinessPlan
TaskDataBackfillReadinessRecord = SimpleReadinessRecord
TaskDataBackfillReadinessFinding = SimpleReadinessRecord
TaskDataBackfillReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "data_backfill": re.compile(
        r"\b(?:data backfill|backfill(?:ing)?|back fill|back-populate|backpopulate|"
        r"populate historical|historical import|legacy import)\b",
        re.I,
    ),
    "replay": re.compile(
        r"\b(?:replay|replayed|replaying|event replay|job replay|reprocess(?:ing)?|"
        r"reingest(?:ion)?|re-ingest)\b",
        re.I,
    ),
    "recompute": re.compile(
        r"\b(?:recompute|recalculation|recalculate|rebuild derived|refresh aggregate|"
        r"refresh aggregates|repair derived|data repair)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "data_backfill": re.compile(r"backfill|back[_-]?fill|backpopulate|historical|legacy[_-]?import", re.I),
    "replay": re.compile(r"replay|reprocess|rerun|re[_-]?run|reingest|re[_-]?ingest", re.I),
    "recompute": re.compile(r"recompute|recalculate|aggregate|derived|repair", re.I),
}
_CRITERIA = {
    "scope_boundaries": re.compile(
        r"\b(?:scope|boundary|tenant|cohort|account|customer|date range|time range|"
        r"watermark|allowlist|segment|subset|where clause|record range)\b",
        re.I,
    ),
    "batching": re.compile(
        r"\b(?:batch|batches|batching|chunk|chunks|chunking|page through|pagination|"
        r"cursor|window|throttle|rate limit|concurrency limit)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotent|idempotency|safe to rerun|rerunnable|de-?dupe|deduplicate|"
        r"upsert|exactly once|duplicate guard)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|restore|backup|snapshot|undo|revert|point-in-time|"
        r"recovery plan|kill switch|abort)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitor|monitoring|metric|metrics|alert|alerting|dashboard|progress|"
        r"observability|logs?|traces?|failure rate)\b",
        re.I,
    ),
    "reconciliation": re.compile(
        r"\b(?:reconcile|reconciliation|validation|validate|verify|row count|record count|"
        r"checksum|diff|parity|audit sample|dry run|post-run check)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "scope_boundaries": "Define exact scope boundaries such as tenant, cohort, date range, or record filter.",
    "batching": "Specify batch size, cursoring, throttling, and pause or resume behavior.",
    "idempotency": "Make the job safe to rerun without duplicate writes or corrupted derived data.",
    "rollback": "Document rollback, restore, snapshot, or abort steps before execution.",
    "monitoring": "Add progress metrics, dashboards, logs, and alerts for the run.",
    "reconciliation": "Add reconciliation checks for counts, checksums, diffs, or sampled records.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:backfill|replay|reprocess|recompute|data repair)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_data_backfill_readiness_plan(source: Any) -> TaskDataBackfillReadinessPlan:
    """Build data backfill readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Data Backfill Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_data_backfill_readiness(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def extract_task_data_backfill_readiness(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def generate_task_data_backfill_readiness(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def derive_task_data_backfill_readiness(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def summarize_task_data_backfill_readiness(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def summarize_task_data_backfill_readiness_plan(source: Any) -> TaskDataBackfillReadinessPlan:
    return build_task_data_backfill_readiness_plan(source)


def recommend_task_data_backfill_readiness(source: Any) -> tuple[TaskDataBackfillReadinessRecord, ...]:
    return build_task_data_backfill_readiness_plan(source).records


def task_data_backfill_readiness_plan_to_dict(result: TaskDataBackfillReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_data_backfill_readiness_plan_to_dict.__test__ = False


def task_data_backfill_readiness_plan_to_dicts(
    result: TaskDataBackfillReadinessPlan | Iterable[TaskDataBackfillReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_backfill_readiness_plan_to_dicts.__test__ = False
task_data_backfill_readiness_to_dicts = task_data_backfill_readiness_plan_to_dicts
task_data_backfill_readiness_to_dicts.__test__ = False


def task_data_backfill_readiness_plan_to_markdown(result: TaskDataBackfillReadinessPlan) -> str:
    return result.to_markdown()


task_data_backfill_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskDataBackfillReadinessFinding",
    "TaskDataBackfillReadinessPlan",
    "TaskDataBackfillReadinessRecord",
    "TaskDataBackfillReadinessRecommendation",
    "analyze_task_data_backfill_readiness",
    "build_task_data_backfill_readiness_plan",
    "derive_task_data_backfill_readiness",
    "extract_task_data_backfill_readiness",
    "generate_task_data_backfill_readiness",
    "recommend_task_data_backfill_readiness",
    "summarize_task_data_backfill_readiness",
    "summarize_task_data_backfill_readiness_plan",
    "task_data_backfill_readiness_plan_to_dict",
    "task_data_backfill_readiness_plan_to_dicts",
    "task_data_backfill_readiness_plan_to_markdown",
    "task_data_backfill_readiness_to_dicts",
]
