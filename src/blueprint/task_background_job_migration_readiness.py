"""Analyze background job migration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

_QUEUE_MAPPING_RE = re.compile(
    r"\b(?:queue|topic|worker|consumer|job)[_\s-]+(?:mapping|map|route|routing)|"
    r"(?:old|source|legacy)[_\s-]+(?:queue|topic|worker)[_\s-]+(?:to|->)[_\s-]+(?:new|target|replacement)|"
    r"(?:queue|topic)[_\s-]+inventory|"
    r"(?:job|worker)[_\s-]+(?:routing|remap|migration map)\b",
    re.I,
)
_IDEMPOTENCY_RE = re.compile(
    r"\b(?:idempoten(?:t|cy)|deduplicat(?:e|ion)|duplicate[_\s-]+(?:detection|prevention)|"
    r"safe[_\s-]+to[_\s-]+replay|replay[_\s-]+safe|job[_\s-]+key|"
    r"unique[_\s-]+(?:job|message)[_\s-]+id|exactly[_\s-]+once)\b",
    re.I,
)
_RETRY_BACKOFF_RE = re.compile(
    r"\b(?:retry|retries|retry[_\s-]+policy|retry[_\s-]+budget|backoff|exponential[_\s-]+backoff|"
    r"jitter|max[_\s-]+attempts?|dead[_\s-]+letter|dlq|poison[_\s-]+job)\b",
    re.I,
)
_DRAIN_STRATEGY_RE = re.compile(
    r"\b(?:drain|draining|queue[_\s-]+drain|topic[_\s-]+drain|worker[_\s-]+drain|"
    r"pause[_\s-]+(?:enqueue|producer|worker)|stop[_\s-]+(?:enqueue|producer)|"
    r"in[_\s-]+flight[_\s-]+jobs?|backlog[_\s-]+(?:drain|clear)|quiesce|quiescence)\b",
    re.I,
)
_SCHEDULING_RE = re.compile(
    r"\b(?:schedule|scheduled|scheduler|cron|cadence|interval|periodic|timer|"
    r"reschedule|disable[_\s-]+cron|job[_\s-]+timing|run[_\s-]+window)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll[_\s-]?back|revert|backout|fallback|restore[_\s-]+(?:old|legacy)|"
    r"switch[_\s-]+back|disable[_\s-]+new[_\s-]+worker|feature[_\s-]+flag|kill[_\s-]+switch)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitoring|observability|metrics?|dashboard|alert(?:ing)?|logs?|traces?|"
    r"queue[_\s-]+depth|consumer[_\s-]+lag|job[_\s-]+latency|failure[_\s-]+rate|"
    r"success[_\s-]+rate|dlq[_\s-]+alert)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|owned[_\s-]+by|ownership|responsible[_\s-]+(?:team|engineer|owner)|"
    r"on[_\s-]+call|dri|service[_\s-]+owner|team)\b",
    re.I,
)

_REQUIREMENT_LABELS = {
    "queue_mapping": "queue/topic mapping",
    "idempotency": "idempotency",
    "retry_backoff": "retry/backoff behavior",
    "drain_strategy": "drain strategy",
    "scheduling": "scheduling changes",
    "rollback": "rollback path",
    "monitoring": "monitoring",
    "ownership": "owner",
}
_ACTIONABLE_GAPS = {
    "queue_mapping": "Map every legacy queue/topic and worker to its migration target.",
    "idempotency": "Document replay safety, deduplication keys, or idempotent job handling.",
    "retry_backoff": "Specify retry limits, backoff behavior, and failure queue handling.",
    "drain_strategy": "Define how producers pause and existing in-flight or backlog jobs drain.",
    "scheduling": "Call out cron, cadence, scheduler, or run-window changes during migration.",
    "rollback": "Provide a rollback or fallback path to the previous job pipeline.",
    "monitoring": "Add metrics, alerts, dashboards, or logs for migration health.",
    "ownership": "Name the responsible owner, team, DRI, or on-call group.",
}


@dataclass(frozen=True, slots=True)
class TaskBackgroundJobMigrationReadiness:
    """Background job migration readiness analysis for a task."""

    queue_mapping_defined: bool = False
    idempotency_addressed: bool = False
    retry_backoff_defined: bool = False
    drain_strategy_defined: bool = False
    scheduling_changes_defined: bool = False
    rollback_path_defined: bool = False
    monitoring_defined: bool = False
    ownership_defined: bool = False
    missing_requirements: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score from 0.0 to 1.0."""
        passed = sum(
            [
                self.queue_mapping_defined,
                self.idempotency_addressed,
                self.retry_backoff_defined,
                self.drain_strategy_defined,
                self.scheduling_changes_defined,
                self.rollback_path_defined,
                self.monitoring_defined,
                self.ownership_defined,
            ]
        )
        return passed / len(_REQUIREMENT_LABELS)

    @property
    def is_ready(self) -> bool:
        """Return whether all readiness requirements are satisfied."""
        return not self.missing_requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "queue_mapping_defined": self.queue_mapping_defined,
            "idempotency_addressed": self.idempotency_addressed,
            "retry_backoff_defined": self.retry_backoff_defined,
            "drain_strategy_defined": self.drain_strategy_defined,
            "scheduling_changes_defined": self.scheduling_changes_defined,
            "rollback_path_defined": self.rollback_path_defined,
            "monitoring_defined": self.monitoring_defined,
            "ownership_defined": self.ownership_defined,
            "missing_requirements": list(self.missing_requirements),
            "actionable_gaps": list(self.actionable_gaps),
            "readiness_score": self.readiness_score,
            "is_ready": self.is_ready,
        }


def analyze_task_background_job_migration_readiness(
    task_data: Mapping[str, Any],
) -> TaskBackgroundJobMigrationReadiness:
    """Analyze background job migration readiness from task data."""
    if not isinstance(task_data, Mapping):
        return _build_readiness({})

    searchable_text = _extract_searchable_text(task_data)
    checks = {
        "queue_mapping": bool(_QUEUE_MAPPING_RE.search(searchable_text)),
        "idempotency": bool(_IDEMPOTENCY_RE.search(searchable_text)),
        "retry_backoff": bool(_RETRY_BACKOFF_RE.search(searchable_text)),
        "drain_strategy": bool(_DRAIN_STRATEGY_RE.search(searchable_text)),
        "scheduling": bool(_SCHEDULING_RE.search(searchable_text)),
        "rollback": bool(_ROLLBACK_RE.search(searchable_text)),
        "monitoring": bool(_MONITORING_RE.search(searchable_text)),
        "ownership": bool(_OWNER_RE.search(searchable_text)),
    }
    return _build_readiness(checks)


def summarize_task_background_job_migration_readiness(
    task_data: Mapping[str, Any],
) -> TaskBackgroundJobMigrationReadiness:
    """Compatibility alias for background job migration readiness."""
    return analyze_task_background_job_migration_readiness(task_data)


def task_background_job_migration_readiness_to_dict(
    result: TaskBackgroundJobMigrationReadiness,
) -> dict[str, Any]:
    """Serialize background job migration readiness to a plain dictionary."""
    return result.to_dict()


task_background_job_migration_readiness_to_dict.__test__ = False


def _build_readiness(checks: Mapping[str, bool]) -> TaskBackgroundJobMigrationReadiness:
    missing = tuple(key for key in _REQUIREMENT_LABELS if not checks.get(key, False))
    return TaskBackgroundJobMigrationReadiness(
        queue_mapping_defined=bool(checks.get("queue_mapping", False)),
        idempotency_addressed=bool(checks.get("idempotency", False)),
        retry_backoff_defined=bool(checks.get("retry_backoff", False)),
        drain_strategy_defined=bool(checks.get("drain_strategy", False)),
        scheduling_changes_defined=bool(checks.get("scheduling", False)),
        rollback_path_defined=bool(checks.get("rollback", False)),
        monitoring_defined=bool(checks.get("monitoring", False)),
        ownership_defined=bool(checks.get("ownership", False)),
        missing_requirements=missing,
        actionable_gaps=tuple(_ACTIONABLE_GAPS[key] for key in missing),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field_name in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field_name)
        if isinstance(value, str):
            parts.append(value)

    for field_name in (
        "acceptance_criteria",
        "requirements",
        "notes",
        "risks",
        "definition_of_done",
    ):
        value = task_data.get(field_name)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value if item)

    metadata = task_data.get("metadata")
    if isinstance(metadata, Mapping):
        parts.extend(_metadata_text(metadata))

    combined = " ".join(parts)
    return _SPACE_RE.sub(" ", combined).strip()


def _metadata_text(metadata: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    for value in metadata.values():
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, Mapping):
            parts.extend(_metadata_text(value))
        elif isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value if item)
    return parts


__all__ = [
    "TaskBackgroundJobMigrationReadiness",
    "analyze_task_background_job_migration_readiness",
    "summarize_task_background_job_migration_readiness",
    "task_background_job_migration_readiness_to_dict",
]
