"""Analyze background job processing strategy for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for background job processing concepts
_JOB_TYPES_RE = re.compile(
    r"(?:job[_\s]+types?|background[_\s]+jobs?|async(?:hronous)?[_\s]+jobs?|"
    r"(?:scheduled|recurring|one[_\s-]*time|periodic)[_\s]+jobs?|"
    r"job[_\s]+(?:categories|classification)|"
    r"(?:celery|sidekiq|resque|bull|delayed[_\s]*job)[_\s]+(?:tasks?|jobs?)|"
    r"worker[_\s]+tasks?)",
    re.I,
)
_SCHEDULING_PATTERNS_RE = re.compile(
    r"(?:scheduling[_\s]+patterns?|job[_\s]+scheduling|"
    r"cron[_\s]+(?:expression|schedule|pattern)|"
    r"(?:schedule|scheduled)[_\s]+(?:at|for|on)|"
    r"job[_\s]+(?:frequency|interval|recurrence)|"
    r"(?:run|execute)[_\s]+(?:every|at|on)|"
    r"time[_\s-]*based[_\s]+(?:execution|scheduling)|"
    r"(?:immediate|delayed)[_\s]+execution)",
    re.I,
)
_PRIORITY_LEVELS_RE = re.compile(
    r"(?:priority[_\s]+(?:levels?|queue|system)|"
    r"job[_\s]+priority|high[_\s]+priority[_\s]+jobs?|"
    r"(?:low|medium|high|critical)[_\s]+priority|"
    r"priority[_\s-]*based[_\s]+(?:execution|processing|queue)|"
    r"prioritize[_\s]+jobs?)",
    re.I,
)
_TIMEOUT_CONFIG_RE = re.compile(
    r"(?:job[_\s]+timeout|timeout[_\s]+(?:configuration|handling|policy)|"
    r"execution[_\s]+timeout|processing[_\s]+timeout|"
    r"(?:set|configure|define)[_\s]+timeout|"
    r"timeout[_\s]+(?:limit|duration|threshold)|"
    r"long[_\s-]*running[_\s]+job|"
    r"max(?:imum)?[_\s]+(?:execution|processing)[_\s]+time)",
    re.I,
)
_RETRY_POLICY_RE = re.compile(
    r"(?:retry[_\s]+(?:policy|strategy|mechanism|logic|configuration)|"
    r"job[_\s]+retry|retry[_\s]+(?:failed|on[_\s]+failure)|"
    r"(?:implement|configure)[_\s]+retr(?:y|ies)|"
    r"exponential[_\s]+backoff|backoff[_\s]+(?:strategy|policy)|"
    r"max(?:imum)?[_\s]+retr(?:y|ies)|retry[_\s]+(?:count|limit|attempts?))",
    re.I,
)
_QUEUE_MANAGEMENT_RE = re.compile(
    r"(?:queue[_\s]+(?:management|configuration|setup)|"
    r"(?:manage|configure)[_\s]+queues?|"
    r"job[_\s]+queues?|task[_\s]+queues?|"
    r"(?:priority|fifo|lifo)[_\s]+queues?|"
    r"queue[_\s]+(?:depth|length|size)|"
    r"multiple[_\s]+queues?|queue[_\s]+workers?)",
    re.I,
)
_IDEMPOTENCY_RE = re.compile(
    r"(?:idempoten(?:t|cy)|idempotent[_\s]+(?:jobs?|operations?|processing)|"
    r"ensure[_\s]+idempotency|idempotency[_\s]+(?:key|token|check)|"
    r"duplicate[_\s]+(?:detection|prevention|job)|"
    r"re[_\s-]*entrant|safe[_\s]+to[_\s]+retry|"
    r"at[_\s-]*(?:least|most)[_\s-]*once[_\s]+(?:delivery|processing)|"
    r"prevent[_\s]+duplicate[_\s]+(?:execution|processing))",
    re.I,
)
_FAILURE_HANDLING_RE = re.compile(
    r"(?:failure[_\s]+(?:handling|recovery|management|strategy)|"
    r"(?:handle|manage)[_\s]+(?:job[_\s]+)?failures?|"
    r"failed[_\s]+job(?:s)?[_\s]+(?:handling|recovery|processing)|"
    r"error[_\s]+(?:handling|recovery)|"
    r"dead[_\s]*letter[_\s]+queue|dlq|"
    r"job[_\s]+(?:failure|error)[_\s]+(?:callback|handler)|"
    r"failure[_\s]+(?:notification|alert))",
    re.I,
)
_PROGRESS_TRACKING_RE = re.compile(
    r"(?:progress[_\s]+(?:tracking|monitoring|reporting)|"
    r"track[_\s]+(?:job[_\s]+)?progress|"
    r"job[_\s]+(?:status|progress|state)|"
    r"monitor[_\s]+(?:job[_\s]+)?progress|"
    r"(?:track|report|update)[_\s]+(?:job[_\s]+)?status|"
    r"job[_\s]+completion[_\s]+(?:status|tracking)|"
    r"progress[_\s]+(?:percentage|indicator|updates?))",
    re.I,
)
_RESOURCE_CONTENTION_RE = re.compile(
    r"(?:resource[_\s]+(?:contention|management|limits?|allocation)|"
    r"(?:manage|limit|control)[_\s]+resources?|"
    r"(?:cpu|memory|disk|network)[_\s]+(?:limits?|usage|constraints?)|"
    r"worker[_\s]+(?:pool|concurrency|limits?)|"
    r"concurrent[_\s]+(?:job|execution|worker)[_\s]+limits?|"
    r"rate[_\s]+limit(?:ing)?|throttle|throttling)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class BackgroundJobProcessing:
    """Background job processing strategy analysis for a task."""

    job_types_defined: bool = False
    scheduling_patterns_configured: bool = False
    priority_levels_configured: bool = False
    timeout_configured: bool = False
    retry_policy_implemented: bool = False
    queue_management_planned: bool = False
    idempotency_ensured: bool = False
    failure_handling_implemented: bool = False
    progress_tracking_enabled: bool = False
    resource_contention_managed: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.job_types_defined,
            self.scheduling_patterns_configured,
            self.priority_levels_configured,
            self.timeout_configured,
            self.retry_policy_implemented,
            self.queue_management_planned,
            self.idempotency_ensured,
            self.failure_handling_implemented,
            self.progress_tracking_enabled,
            self.resource_contention_managed,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "job_types_defined": self.job_types_defined,
            "scheduling_patterns_configured": self.scheduling_patterns_configured,
            "priority_levels_configured": self.priority_levels_configured,
            "timeout_configured": self.timeout_configured,
            "retry_policy_implemented": self.retry_policy_implemented,
            "queue_management_planned": self.queue_management_planned,
            "idempotency_ensured": self.idempotency_ensured,
            "failure_handling_implemented": self.failure_handling_implemented,
            "progress_tracking_enabled": self.progress_tracking_enabled,
            "resource_contention_managed": self.resource_contention_managed,
            "readiness_score": self.readiness_score,
        }


def analyze_background_job_processing(task_data: Mapping[str, Any]) -> BackgroundJobProcessing:
    """
    Analyze background job processing strategy from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        BackgroundJobProcessing with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return BackgroundJobProcessing()

    searchable_text = _extract_searchable_text(task_data)

    return BackgroundJobProcessing(
        job_types_defined=bool(_JOB_TYPES_RE.search(searchable_text)),
        scheduling_patterns_configured=bool(_SCHEDULING_PATTERNS_RE.search(searchable_text)),
        priority_levels_configured=bool(_PRIORITY_LEVELS_RE.search(searchable_text)),
        timeout_configured=bool(_TIMEOUT_CONFIG_RE.search(searchable_text)),
        retry_policy_implemented=bool(_RETRY_POLICY_RE.search(searchable_text)),
        queue_management_planned=bool(_QUEUE_MANAGEMENT_RE.search(searchable_text)),
        idempotency_ensured=bool(_IDEMPOTENCY_RE.search(searchable_text)),
        failure_handling_implemented=bool(_FAILURE_HANDLING_RE.search(searchable_text)),
        progress_tracking_enabled=bool(_PROGRESS_TRACKING_RE.search(searchable_text)),
        resource_contention_managed=bool(_RESOURCE_CONTENTION_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "BackgroundJobProcessing",
    "analyze_background_job_processing",
]
