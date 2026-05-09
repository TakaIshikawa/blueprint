"""Analyze database sharding readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for database sharding concepts
_SHARDING_KEY_SELECTION_RE = re.compile(
    r"\b(?:sharding[_\s]+key[_\s]+select(?:ed|ion)|"
    r"shard[_\s]+key[_\s]+defin(?:ed|ition)|"
    r"partition[_\s]+key[_\s]+chos(?:en|ice)|"
    r"sharding[_\s]+key[_\s]+appropriate|"
    r"distribution[_\s]+key[_\s]+configur(?:ed|ation)|"
    r"shard[_\s]+key[_\s]+validat(?:ed|ion))\b",
    re.I,
)
_SHARD_ROUTING_RE = re.compile(
    r"\b(?:shard[_\s]+routing[_\s]+implement(?:ed|ation)|"
    r"routing[_\s]+logic[_\s]+configur(?:ed|ation)|"
    r"shard[_\s]+lookup[_\s]+implement(?:ed|ation)|"
    r"shard[_\s]+map[_\s]+configur(?:ed|ation)|"
    r"route[_\s]+to[_\s]+shard[_\s]+implement(?:ed|ation)|"
    r"shard[_\s]+directory[_\s]+setup)\b",
    re.I,
)
_CROSS_SHARD_QUERY_RE = re.compile(
    r"\b(?:cross[_\s]+shard[_\s]+query[_\s]+optimiz(?:ed|ation)|"
    r"scatter[_\s]+gather[_\s]+optimiz(?:ed|ation)|"
    r"fan[_\s]+out[_\s]+query[_\s]+tun(?:ed|ing)|"
    r"distributed[_\s]+query[_\s]+optimiz(?:ed|ation)|"
    r"cross[_\s]+shard[_\s]+performance|"
    r"query[_\s]+routing[_\s]+optimiz(?:ed|ation))\b",
    re.I,
)
_REBALANCING_RE = re.compile(
    r"\b(?:rebalancing[_\s]+automat(?:ed|ion)|"
    r"auto[_\s]+rebalanc(?:e|ing)|"
    r"dynamic[_\s]+shard[_\s]+rebalancing|"
    r"shard[_\s]+scaling[_\s]+automat(?:ed|ion)|"
    r"automated[_\s]+resharding|"
    r"rebalance[_\s]+on[_\s]+demand)\b",
    re.I,
)
_CONSISTENT_HASHING_RE = re.compile(
    r"\b(?:consistent[_\s]+hashing[_\s]+configur(?:ed|ation)|"
    r"hash[_\s]+ring[_\s]+setup|"
    r"virtual[_\s]+nodes[_\s]+configur(?:ed|ation)|"
    r"hash[_\s]+partitioning[_\s]+implement(?:ed|ation)|"
    r"consistent[_\s]+hash[_\s]+deploy(?:ed|ment)|"
    r"hash[_\s]+function[_\s]+configur(?:ed|ation))\b",
    re.I,
)
_SHARD_MIGRATION_RE = re.compile(
    r"\b(?:shard[_\s]+migration[_\s]+test(?:ed|ing)|"
    r"migration[_\s]+procedure[_\s]+verif(?:ied|ication)|"
    r"shard[_\s]+cutover[_\s]+test(?:ed|ing)|"
    r"online[_\s]+resharding[_\s]+test(?:ed|ing)|"
    r"shard[_\s]+data[_\s]+migration[_\s]+validat(?:ed|ion)|"
    r"shard[_\s]+transfer[_\s]+test(?:ed|ing))\b",
    re.I,
)
_CROSS_SHARD_TRANSACTION_RE = re.compile(
    r"\b(?:cross[_\s]+shard[_\s]+transaction[_\s]+handl(?:ed|ing)|"
    r"distributed[_\s]+transaction[_\s]+implement(?:ed|ation)|"
    r"saga[_\s]+pattern[_\s]+implement(?:ed|ation)|"
    r"two[_\s]+phase[_\s]+commit[_\s]+configur(?:ed|ation)|"
    r"transaction[_\s]+coordinator[_\s]+deploy(?:ed|ment)|"
    r"global[_\s]+transaction[_\s]+manag(?:ed|ement))\b",
    re.I,
)
_SHARD_HEALTH_RE = re.compile(
    r"\b(?:shard[_\s]+health[_\s]+monitor(?:ed|ing)|"
    r"shard[_\s]+metrics[_\s]+collect(?:ed|ion)|"
    r"shard[_\s]+alerting[_\s]+configur(?:ed|ation)|"
    r"shard[_\s]+dashboard[_\s]+deploy(?:ed|ment)|"
    r"shard[_\s]+performance[_\s]+track(?:ed|ing)|"
    r"shard[_\s]+utilization[_\s]+monitor(?:ed|ing)|"
    r"shard[_\s]+status[_\s]+check(?:ed|ing))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DatabaseShardingReadiness:
    """Database sharding readiness analysis for a task."""

    sharding_key_selection_appropriate: bool = False
    shard_routing_implemented: bool = False
    cross_shard_query_optimized: bool = False
    rebalancing_automated: bool = False
    consistent_hashing_configured: bool = False
    shard_migration_procedures_tested: bool = False
    cross_shard_transaction_handled: bool = False
    shard_health_monitored: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.sharding_key_selection_appropriate,
            self.shard_routing_implemented,
            self.cross_shard_query_optimized,
            self.rebalancing_automated,
            self.consistent_hashing_configured,
            self.shard_migration_procedures_tested,
            self.cross_shard_transaction_handled,
            self.shard_health_monitored,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "sharding_key_selection_appropriate": self.sharding_key_selection_appropriate,
            "shard_routing_implemented": self.shard_routing_implemented,
            "cross_shard_query_optimized": self.cross_shard_query_optimized,
            "rebalancing_automated": self.rebalancing_automated,
            "consistent_hashing_configured": self.consistent_hashing_configured,
            "shard_migration_procedures_tested": self.shard_migration_procedures_tested,
            "cross_shard_transaction_handled": self.cross_shard_transaction_handled,
            "shard_health_monitored": self.shard_health_monitored,
            "readiness_score": self.readiness_score,
        }


def analyze_database_sharding_readiness(task_data: Mapping[str, Any]) -> DatabaseShardingReadiness:
    """
    Analyze database sharding readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        DatabaseShardingReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return DatabaseShardingReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return DatabaseShardingReadiness(
        sharding_key_selection_appropriate=bool(_SHARDING_KEY_SELECTION_RE.search(searchable_text)),
        shard_routing_implemented=bool(_SHARD_ROUTING_RE.search(searchable_text)),
        cross_shard_query_optimized=bool(_CROSS_SHARD_QUERY_RE.search(searchable_text)),
        rebalancing_automated=bool(_REBALANCING_RE.search(searchable_text)),
        consistent_hashing_configured=bool(_CONSISTENT_HASHING_RE.search(searchable_text)),
        shard_migration_procedures_tested=bool(_SHARD_MIGRATION_RE.search(searchable_text)),
        cross_shard_transaction_handled=bool(_CROSS_SHARD_TRANSACTION_RE.search(searchable_text)),
        shard_health_monitored=bool(_SHARD_HEALTH_RE.search(searchable_text)),
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
    "DatabaseShardingReadiness",
    "analyze_database_sharding_readiness",
]
