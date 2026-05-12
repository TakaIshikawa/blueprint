"""Analyze cache invalidation readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

_CACHE_KEYS_RE = re.compile(
    r"\b(?:cache[_\s-]+keys?|key[_\s-]+schema|key[_\s-]+namespace|namespaced[_\s-]+keys?|"
    r"tenant[_\s-]+(?:scoped|aware)[_\s-]+keys?|user[_\s-]+(?:scoped|aware)[_\s-]+keys?|"
    r"redis[_\s-]+keys?|cache[_\s-]+namespace|key[_\s-]+pattern)\b",
    re.I,
)
_INVALIDATION_TRIGGERS_RE = re.compile(
    r"\b(?:invalidation[_\s-]+triggers?|invalidate[_\s-]+on|evict[_\s-]+on|purge[_\s-]+on|"
    r"data[_\s-]+change[_\s-]+trigger|write[_\s-]+through|after[_\s-]+(?:save|update|delete)|"
    r"publish[_\s-]+event|domain[_\s-]+event|webhook[_\s-]+trigger)\b",
    re.I,
)
_TTL_RE = re.compile(
    r"\b(?:ttl|time[_\s-]+to[_\s-]+live|expires?|expiration|expiry|max[_\s-]+age|s-maxage|"
    r"cache[_\s-]+duration|lifetime|retention[_\s-]+window)\b",
    re.I,
)
_STALE_READ_RE = re.compile(
    r"\b(?:stale[_\s-]+read|stale[_\s-]+data|staleness|stale[_\s-]+while[_\s-]+revalidate|"
    r"serve[_\s-]+stale|stale[_\s-]+tolerance|max[_\s-]+staleness|freshness[_\s-]+bound|"
    r"read[_\s-]+after[_\s-]+write)\b",
    re.I,
)
_WARMUP_BACKFILL_RE = re.compile(
    r"\b(?:warmup|warm[_\s-]+up|prewarm|pre[_\s-]+warm|backfill|rehydrate|hydrate|"
    r"prime[_\s-]+cache|cache[_\s-]+priming|seed[_\s-]+cache|rebuild[_\s-]+cache)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll[_\s-]?back|revert|backout|fallback|kill[_\s-]+switch|disable[_\s-]+invalidation|"
    r"restore[_\s-]+cache|manual[_\s-]+purge[_\s-]+fallback|bypass[_\s-]+cache)\b",
    re.I,
)
_OBSERVABILITY_RE = re.compile(
    r"\b(?:observability|monitoring|metrics?|dashboard|alerts?|logs?|traces?|cache[_\s-]+hit[_\s-]+rate|"
    r"miss[_\s-]+rate|stale[_\s-]+rate|purge[_\s-]+latency|invalidation[_\s-]+failures?)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|owned[_\s-]+by|ownership|responsible[_\s-]+(?:team|engineer|owner)|"
    r"on[_\s-]+call|dri|service[_\s-]+owner|team)\b",
    re.I,
)

_REQUIREMENT_LABELS = {
    "cache_keys": "cache keys",
    "invalidation_triggers": "invalidation triggers",
    "ttl_behavior": "TTL behavior",
    "stale_read_tolerance": "stale-read tolerance",
    "warmup_backfill": "warmup/backfill plan",
    "rollback": "rollback path",
    "observability": "observability",
    "ownership": "owner",
}
_ACTIONABLE_GAPS = {
    "cache_keys": "Document affected cache keys, namespaces, and tenant or user scoping.",
    "invalidation_triggers": "Specify the events, writes, deletes, or deploys that trigger invalidation.",
    "ttl_behavior": "Define TTL, max-age, expiry, or cache lifetime behavior.",
    "stale_read_tolerance": "State tolerated stale-read windows and freshness guarantees.",
    "warmup_backfill": "Add a cache warmup, rehydration, priming, or backfill plan.",
    "rollback": "Provide rollback, bypass, kill-switch, or manual purge fallback steps.",
    "observability": "Add metrics, alerts, dashboards, or logs for invalidation health.",
    "ownership": "Name the responsible owner, team, DRI, or on-call group.",
}


@dataclass(frozen=True, slots=True)
class TaskCacheInvalidationReadiness:
    """Cache invalidation readiness analysis for a task."""

    cache_keys_defined: bool = False
    invalidation_triggers_defined: bool = False
    ttl_behavior_defined: bool = False
    stale_read_tolerance_defined: bool = False
    warmup_backfill_defined: bool = False
    rollback_path_defined: bool = False
    observability_defined: bool = False
    ownership_defined: bool = False
    missing_requirements: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score from 0.0 to 1.0."""
        passed = sum(
            [
                self.cache_keys_defined,
                self.invalidation_triggers_defined,
                self.ttl_behavior_defined,
                self.stale_read_tolerance_defined,
                self.warmup_backfill_defined,
                self.rollback_path_defined,
                self.observability_defined,
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
            "cache_keys_defined": self.cache_keys_defined,
            "invalidation_triggers_defined": self.invalidation_triggers_defined,
            "ttl_behavior_defined": self.ttl_behavior_defined,
            "stale_read_tolerance_defined": self.stale_read_tolerance_defined,
            "warmup_backfill_defined": self.warmup_backfill_defined,
            "rollback_path_defined": self.rollback_path_defined,
            "observability_defined": self.observability_defined,
            "ownership_defined": self.ownership_defined,
            "missing_requirements": list(self.missing_requirements),
            "actionable_gaps": list(self.actionable_gaps),
            "readiness_score": self.readiness_score,
            "is_ready": self.is_ready,
        }


def analyze_task_cache_invalidation_readiness(
    task_data: Mapping[str, Any],
) -> TaskCacheInvalidationReadiness:
    """Analyze cache invalidation readiness from task data."""
    if not isinstance(task_data, Mapping):
        return _build_readiness({})

    searchable_text = _extract_searchable_text(task_data)
    checks = {
        "cache_keys": bool(_CACHE_KEYS_RE.search(searchable_text)),
        "invalidation_triggers": bool(_INVALIDATION_TRIGGERS_RE.search(searchable_text)),
        "ttl_behavior": bool(_TTL_RE.search(searchable_text)),
        "stale_read_tolerance": bool(_STALE_READ_RE.search(searchable_text)),
        "warmup_backfill": bool(_WARMUP_BACKFILL_RE.search(searchable_text)),
        "rollback": bool(_ROLLBACK_RE.search(searchable_text)),
        "observability": bool(_OBSERVABILITY_RE.search(searchable_text)),
        "ownership": bool(_OWNER_RE.search(searchable_text)),
    }
    return _build_readiness(checks)


def summarize_task_cache_invalidation_readiness(
    task_data: Mapping[str, Any],
) -> TaskCacheInvalidationReadiness:
    """Compatibility alias for cache invalidation readiness."""
    return analyze_task_cache_invalidation_readiness(task_data)


def task_cache_invalidation_readiness_to_dict(
    result: TaskCacheInvalidationReadiness,
) -> dict[str, Any]:
    """Serialize cache invalidation readiness to a plain dictionary."""
    return result.to_dict()


task_cache_invalidation_readiness_to_dict.__test__ = False


def _build_readiness(checks: Mapping[str, bool]) -> TaskCacheInvalidationReadiness:
    missing = tuple(key for key in _REQUIREMENT_LABELS if not checks.get(key, False))
    return TaskCacheInvalidationReadiness(
        cache_keys_defined=bool(checks.get("cache_keys", False)),
        invalidation_triggers_defined=bool(checks.get("invalidation_triggers", False)),
        ttl_behavior_defined=bool(checks.get("ttl_behavior", False)),
        stale_read_tolerance_defined=bool(checks.get("stale_read_tolerance", False)),
        warmup_backfill_defined=bool(checks.get("warmup_backfill", False)),
        rollback_path_defined=bool(checks.get("rollback", False)),
        observability_defined=bool(checks.get("observability", False)),
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
    "TaskCacheInvalidationReadiness",
    "analyze_task_cache_invalidation_readiness",
    "summarize_task_cache_invalidation_readiness",
    "task_cache_invalidation_readiness_to_dict",
]
