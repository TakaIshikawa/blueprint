"""Analyze caching strategy for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for caching strategy concepts
_CACHE_LAYERS_RE = re.compile(
    r"(?:cache[_\s]+layers?|caching[_\s]+layers?|"
    r"(?:application|cdn|database|redis|memcached|varnish)[_\s]+cache|"
    r"multi[_\s-]*tier[_\s]+cach(?:e|ing)|distributed[_\s]+cache|"
    r"in[_\s-]*memory[_\s]+cache|edge[_\s]+cache|"
    r"cache[_\s]+(?:hierarchy|tiers?|levels?))",
    re.I,
)
_CACHE_KEYS_RE = re.compile(
    r"(?:cache[_\s]+keys?|caching[_\s]+keys?|"
    r"key[_\s]+(?:strategy|design|generation|pattern)|"
    r"(?:generate|define|design)[_\s]+cache[_\s]+keys?|"
    r"cache[_\s]+key[_\s]+(?:format|structure|pattern|naming)|"
    r"invalidation[_\s]+keys?)",
    re.I,
)
_TTL_POLICY_RE = re.compile(
    r"(?:ttl(?:[_\s]|\.py\b|\b)|time[_\s]+to[_\s]+live|expir(?:ation|y|e)|"
    r"cache[_\s]+(?:ttl|expir(?:ation|y|e)|lifetime|duration)|"
    r"(?:set|configure|define)[_\s]+(?:ttl|expir(?:ation|y))|"
    r"expir(?:y|ation)[_\s]+(?:time|policy|strategy)|"
    r"cache[_\s]+(?:timeout|retention)|"
    r"max[_\s-]*age)",
    re.I,
)
_INVALIDATION_STRATEGY_RE = re.compile(
    r"(?:\bcache[_\s]+invalidation|invalidat(?:e|ion)(?:[_\s]+(?:cache|strategy|policy|pattern|logic)|\b)|"
    r"(?:purge|clear|evict|flush)[_\s]+cache|"
    r"cache[_\s]+(?:purge|clear|evict|flush|bust(?:ing)?)|"
    r"(?:stale|expired)[_\s]+cache[_\s]+(?:removal|cleanup)|"
    r"cache[_\s]+refresh|refresh[_\s]+cache)",
    re.I,
)
_CACHE_STAMPEDE_RE = re.compile(
    r"\b(?:cache[_\s]+stampede|stampede[_\s]+(?:prevention|protection|handling)|"
    r"thundering[_\s]+herd|dog[_\s-]*pile[_\s]+(?:effect|prevention|protection)|"
    r"prevent[_\s]+(?:cache[_\s]+)?stampede|"
    r"cache[_\s]+lock(?:ing)?|lock[_\s]+(?:on[_\s]+)?cache|"
    r"single[_\s]+(?:flight|request)[_\s]+(?:cache|pattern)|"
    r"cache[_\s]+(?:deduplication|coalescing))\b",
    re.I,
)
_STALE_DATA_RE = re.compile(
    r"\b(?:stale[_\s]+(?:cache|data)|cache[_\s]+staleness|"
    r"(?:handle|manage|prevent)[_\s]+stale[_\s]+(?:cache|data)|"
    r"stale[_\s-]*while[_\s-]*revalidate|swr|"
    r"cache[_\s]+(?:freshness|consistency|coherence)|"
    r"(?:eventual|strong)[_\s]+consistency|"
    r"cache[_\s]+(?:sync(?:hronization)?|coherency))\b",
    re.I,
)
_CACHE_COHERENCE_RE = re.compile(
    r"\b(?:cache[_\s]+coherenc(?:e|y)|coherent[_\s]+cach(?:e|ing)|"
    r"distributed[_\s]+cache[_\s]+(?:consistency|coherence)|"
    r"cache[_\s]+(?:sync(?:hronization)?|replication)|"
    r"multi[_\s-]*region[_\s]+cache|"
    r"cross[_\s-]*datacenter[_\s]+cache|"
    r"cache[_\s]+propagation)\b",
    re.I,
)
_COLD_START_RE = re.compile(
    r"\b(?:cold[_\s]+start|cache[_\s]+(?:warm(?:ing|up)?|priming|preload(?:ing)?)|"
    r"warm(?:ing)?[_\s]+(?:cache|up)|prime[_\s]+cache|"
    r"pre[_\s-]*(?:load|populate|fill)[_\s]+cache|"
    r"cache[_\s]+initialization|initial[_\s]+cache[_\s]+population|"
    r"(?:prevent|avoid)[_\s]+cold[_\s]+(?:start|cache))\b",
    re.I,
)
_MEMORY_LIMITS_RE = re.compile(
    r"(?:cache[_\s]+(?:memory|size)[_\s]+(?:limits?|constraints?|management)|"
    r"memory[_\s]+(?:limits?|constraints?|pressure|usage)(?:[_\s]+(?:for[_\s]+)?cache)?|"
    r"cache[_\s]+(?:eviction|expulsion)[_\s]+(?:policy|strategy)|"
    r"(?:lru|lfu|fifo)[_\s]+(?:eviction|cache)|"
    r"cache[_\s]+(?:capacity|quota|bounds?)|"
    r"max[_\s]+cache[_\s]+(?:size|entries|memory)|"
    r"cache[_\s]+overflow)",
    re.I,
)
_CACHE_MONITORING_RE = re.compile(
    r"\b(?:cache[_\s]+(?:monitoring|metrics|observability)|"
    r"(?:monitor|track|measure)[_\s]+cache|"
    r"cache[_\s]+(?:hit(?:s)?|miss(?:es)?)[_\s]+(?:rate|ratio|tracking|monitoring)|"
    r"cache[_\s]+(?:performance|analytics|statistics)|"
    r"(?:hit|miss)[_\s]+ratio|"
    r"cache[_\s]+(?:instrumentation|telemetry))\b",
    re.I,
)

# Caching patterns
_WRITE_THROUGH_RE = re.compile(
    r"\b(?:write[_\s-]*through[_\s]+cach(?:e|ing)|"
    r"write[_\s]+to[_\s]+(?:cache[_\s]+and[_\s]+)?(?:database|storage|backend))\b",
    re.I,
)
_WRITE_BEHIND_RE = re.compile(
    r"\b(?:write[_\s-]*behind[_\s]+cach(?:e|ing)|write[_\s-]*back[_\s]+cache|"
    r"async(?:hronous)?[_\s]+(?:write|cache[_\s]+write)|"
    r"lazy[_\s]+(?:write|persist))\b",
    re.I,
)
_CACHE_ASIDE_RE = re.compile(
    r"\b(?:cache[_\s-]*aside|lazy[_\s]+(?:load(?:ing)?|cach(?:e|ing))|"
    r"on[_\s-]*demand[_\s]+cach(?:e|ing)|"
    r"read[_\s-]*through[_\s]+cach(?:e|ing))\b",
    re.I,
)
_NEGATIVE_CACHING_RE = re.compile(
    r"\b(?:negative[_\s]+cach(?:e|ing)|"
    r"cache[_\s]+(?:null|empty|not[_\s]+found|404)[_\s]+(?:response|result)(?:s)?|"
    r"(?:null|empty)[_\s]+(?:cache|caching)|"
    r"cache[_\s]+(?:miss|absence))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class CachingStrategy:
    """Caching strategy analysis for a task."""

    cache_layers_defined: bool = False
    cache_keys_designed: bool = False
    ttl_policy_configured: bool = False
    invalidation_strategy_planned: bool = False
    cache_stampede_prevented: bool = False
    stale_data_handled: bool = False
    cache_coherence_maintained: bool = False
    cold_start_optimized: bool = False
    memory_limits_managed: bool = False
    cache_monitoring_enabled: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.cache_layers_defined,
            self.cache_keys_designed,
            self.ttl_policy_configured,
            self.invalidation_strategy_planned,
            self.cache_stampede_prevented,
            self.stale_data_handled,
            self.cache_coherence_maintained,
            self.cold_start_optimized,
            self.memory_limits_managed,
            self.cache_monitoring_enabled,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "cache_layers_defined": self.cache_layers_defined,
            "cache_keys_designed": self.cache_keys_designed,
            "ttl_policy_configured": self.ttl_policy_configured,
            "invalidation_strategy_planned": self.invalidation_strategy_planned,
            "cache_stampede_prevented": self.cache_stampede_prevented,
            "stale_data_handled": self.stale_data_handled,
            "cache_coherence_maintained": self.cache_coherence_maintained,
            "cold_start_optimized": self.cold_start_optimized,
            "memory_limits_managed": self.memory_limits_managed,
            "cache_monitoring_enabled": self.cache_monitoring_enabled,
            "readiness_score": self.readiness_score,
        }


def analyze_caching_strategy(task_data: Mapping[str, Any]) -> CachingStrategy:
    """
    Analyze caching strategy from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        CachingStrategy with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return CachingStrategy()

    searchable_text = _extract_searchable_text(task_data)

    # Check for cache layers including caching patterns
    cache_layers = bool(
        _CACHE_LAYERS_RE.search(searchable_text)
        or _WRITE_THROUGH_RE.search(searchable_text)
        or _WRITE_BEHIND_RE.search(searchable_text)
        or _CACHE_ASIDE_RE.search(searchable_text)
        or _NEGATIVE_CACHING_RE.search(searchable_text)
    )

    return CachingStrategy(
        cache_layers_defined=cache_layers,
        cache_keys_designed=bool(_CACHE_KEYS_RE.search(searchable_text)),
        ttl_policy_configured=bool(_TTL_POLICY_RE.search(searchable_text)),
        invalidation_strategy_planned=bool(_INVALIDATION_STRATEGY_RE.search(searchable_text)),
        cache_stampede_prevented=bool(_CACHE_STAMPEDE_RE.search(searchable_text)),
        stale_data_handled=bool(_STALE_DATA_RE.search(searchable_text)),
        cache_coherence_maintained=bool(_CACHE_COHERENCE_RE.search(searchable_text)),
        cold_start_optimized=bool(_COLD_START_RE.search(searchable_text)),
        memory_limits_managed=bool(_MEMORY_LIMITS_RE.search(searchable_text)),
        cache_monitoring_enabled=bool(_CACHE_MONITORING_RE.search(searchable_text)),
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
    "CachingStrategy",
    "analyze_caching_strategy",
]
