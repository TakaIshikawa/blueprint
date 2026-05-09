"""Analyze batch processing strategy for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for batch processing concepts
_BATCH_SIZE_RE = re.compile(
    r"\b(?:batch[_\s]+size|batch[_\s]+(?:limit|count|length)|"
    r"chunk(?:s)?(?:[_\s]+(?:size|of))?|page[_\s]+size|"
    r"process(?:ing)?[_\s]+(?:in[_\s]+)?batch(?:es)?|"
    r"bulk[_\s]+(?:size|operation|processing)|"
    r"batch[_\s]+configuration|configure[_\s]+batch[_\s]+size|"
    r"set[_\s]+batch[_\s]+size|define[_\s]+batch[_\s]+size|"
    r"test_batch_size)\b",
    re.I,
)
_PARALLELISM_RE = re.compile(
    r"\b(?:parallel(?:ism)?(?:[_\s]+(?:level|degree|configuration|processing|workers?|execution))?|"
    r"concurrent[_\s]+(?:batch(?:es)?|processing|workers?|executions?)|"
    r"worker[_\s]+(?:pool|count|threads?|processes?)|"
    r"multi[_\s-]*threaded?[_\s]+(?:batch|processing)|"
    r"async(?:hronous)?[_\s]+(?:batch|processing)|"
    r"process[_\s]+in[_\s]+parallel)\b",
    re.I,
)
_CHECKPOINTING_RE = re.compile(
    r"\b(?:checkpoint(?:ing)?|check[_\s]*point(?:s)?[_\s]+(?:strategy|mechanism)|"
    r"save[_\s]+(?:progress|state|checkpoint|batch[_\s]+state)|"
    r"progress[_\s]+(?:tracking|checkpoint|persistence|save)|"
    r"resume[_\s]+(?:from[_\s]+checkpoint|processing|batch)|"
    r"restart[_\s]+(?:from[_\s]+)?(?:checkpoint|last[_\s]+state)|"
    r"incremental[_\s]+(?:processing|checkpoint)|"
    r"batch[_\s]+(?:state|progress)[_\s]+(?:save|persistence)|"
    r"test_checkpointing)\b",
    re.I,
)
_PARTIAL_FAILURE_RE = re.compile(
    r"\b(?:partial[_\s]+failure(?:s)?(?:[_\s]+(?:handling|recovery|strategy))?|"
    r"handle[_\s]+partial[_\s]+failure(?:s)?|"
    r"batch[_\s]+failure[_\s]+(?:handling|recovery|isolation)|"
    r"failed[_\s]+(?:item|record|batch)(?:es|s)?[_\s]+handling|"
    r"continue[_\s]+(?:processing[_\s]+)?(?:batch[_\s]+)?on[_\s]+(?:failure|error)|"
    r"skip[_\s]+failed[_\s]+(?:items?|records?)|"
    r"fail[_\s]+(?:fast|slow)|error[_\s]+(?:isolation|containment)|"
    r"failure(?:s)?[_\s]+in[_\s]+(?:dependent[_\s]+)?batch(?:es)?|"
    r"dead[_\s]*letter[_\s]+queue|dlq)\b",
    re.I,
)
_PROGRESS_TRACKING_RE = re.compile(
    r"\b(?:progress[_\s]+(?:tracking|monitoring|reporting|indicator|bar)|"
    r"track[_\s]+(?:batch[_\s]+)?progress|"
    r"batch[_\s]+(?:status|completion|progress)|"
    r"monitor[_\s]+(?:batch[_\s]+)?progress|"
    r"report[_\s]+progress|processed[_\s]+count|"
    r"completion[_\s]+(?:percentage|status|tracking)|"
    r"(?:track|log|record)[_\s]+processed[_\s]+(?:items?|records?))\b",
    re.I,
)
_MEMORY_CONSTRAINTS_RE = re.compile(
    r"\b(?:memory[_\s]+(?:constraint(?:s)?|limit|management|optimization|usage|leak(?:s)?)|"
    r"(?:manage|limit|optimize)[_\s]+memory|"
    r"avoid[_\s]+(?:memory[_\s]+)?(?:leak(?:s)?|exhaustion)|"
    r"heap[_\s]+(?:size|limit|management)|"
    r"buffer[_\s]+(?:size|management|limit)|"
    r"streaming[_\s]+(?:processing|batch)|"
    r"batch[_\s]+memory[_\s]+(?:usage|footprint|constraint))\b",
    re.I,
)
_TIMEOUT_RE = re.compile(
    r"\b(?:batch[_\s]+timeout|timeout[_\s]+(?:configuration|handling|management)|"
    r"processing[_\s]+timeout|execution[_\s]+timeout|"
    r"(?:set|configure|handle)[_\s]+timeout|"
    r"long[_\s-]*running[_\s]+(?:batch|operation|task)|"
    r"timeout[_\s]+(?:strategy|policy)|"
    r"prevent[_\s]+timeout)\b",
    re.I,
)
_RETRY_LOGIC_RE = re.compile(
    r"\b(?:retry[_\s]+(?:logic|strategy|policy|mechanism|configuration|on[_\s]+failure)|"
    r"(?:implement|configure)[_\s]+retr(?:y|ies)|"
    r"batch[_\s]+retry|retry[_\s]+failed[_\s]+(?:batch(?:es)?|items?)|"
    r"exponential[_\s]+backoff|backoff[_\s]+strategy|"
    r"max(?:imum)?[_\s]+retr(?:y|ies)|retry[_\s]+(?:count|limit|attempts?))\b",
    re.I,
)
_IDEMPOTENCY_RE = re.compile(
    r"\b(?:idempoten(?:t|cy)|idempotent[_\s]+(?:operation|processing|batch)|"
    r"ensure[_\s]+idempotency|idempotency[_\s]+(?:key|token)|"
    r"duplicate[_\s]+(?:detection|prevention)|"
    r"re[_\s-]*entrant|safe[_\s]+to[_\s]+retry|"
    r"at[_\s-]*(?:least|most)[_\s-]*once[_\s]+(?:delivery|processing)|"
    r"test_idempotency)\b",
    re.I,
)
_RESULT_AGGREGATION_RE = re.compile(
    r"\b(?:result[_\s]+aggregation|aggregate[_\s]+(?:batch[_\s]+)?results?|"
    r"batch[_\s]+(?:results?|outcome)(?:s)?(?:[_\s]+(?:aggregation|collection))?|"
    r"collect[_\s]+(?:batch[_\s]+)?results?|"
    r"combine[_\s]+(?:batch[_\s]+)?results?|"
    r"consolidate[_\s]+(?:batch[_\s]+)?(?:results?|outcomes?)|"
    r"merge[_\s]+(?:batch[_\s]+)?(?:results?|outcome))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class BatchProcessingStrategy:
    """Batch processing strategy analysis for a task."""

    batch_size_defined: bool = False
    parallelism_configured: bool = False
    checkpointing_enabled: bool = False
    partial_failure_handled: bool = False
    progress_tracked: bool = False
    memory_constraints_managed: bool = False
    timeout_configured: bool = False
    retry_logic_implemented: bool = False
    idempotency_ensured: bool = False
    result_aggregation_planned: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.batch_size_defined,
            self.parallelism_configured,
            self.checkpointing_enabled,
            self.partial_failure_handled,
            self.progress_tracked,
            self.memory_constraints_managed,
            self.timeout_configured,
            self.retry_logic_implemented,
            self.idempotency_ensured,
            self.result_aggregation_planned,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "batch_size_defined": self.batch_size_defined,
            "parallelism_configured": self.parallelism_configured,
            "checkpointing_enabled": self.checkpointing_enabled,
            "partial_failure_handled": self.partial_failure_handled,
            "progress_tracked": self.progress_tracked,
            "memory_constraints_managed": self.memory_constraints_managed,
            "timeout_configured": self.timeout_configured,
            "retry_logic_implemented": self.retry_logic_implemented,
            "idempotency_ensured": self.idempotency_ensured,
            "result_aggregation_planned": self.result_aggregation_planned,
            "readiness_score": self.readiness_score,
        }


def analyze_batch_processing_strategy(task_data: Mapping[str, Any]) -> BatchProcessingStrategy:
    """
    Analyze batch processing strategy from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        BatchProcessingStrategy with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return BatchProcessingStrategy()

    searchable_text = _extract_searchable_text(task_data)

    return BatchProcessingStrategy(
        batch_size_defined=bool(_BATCH_SIZE_RE.search(searchable_text)),
        parallelism_configured=bool(_PARALLELISM_RE.search(searchable_text)),
        checkpointing_enabled=bool(_CHECKPOINTING_RE.search(searchable_text)),
        partial_failure_handled=bool(_PARTIAL_FAILURE_RE.search(searchable_text)),
        progress_tracked=bool(_PROGRESS_TRACKING_RE.search(searchable_text)),
        memory_constraints_managed=bool(_MEMORY_CONSTRAINTS_RE.search(searchable_text)),
        timeout_configured=bool(_TIMEOUT_RE.search(searchable_text)),
        retry_logic_implemented=bool(_RETRY_LOGIC_RE.search(searchable_text)),
        idempotency_ensured=bool(_IDEMPOTENCY_RE.search(searchable_text)),
        result_aggregation_planned=bool(_RESULT_AGGREGATION_RE.search(searchable_text)),
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
    "BatchProcessingStrategy",
    "analyze_batch_processing_strategy",
]
