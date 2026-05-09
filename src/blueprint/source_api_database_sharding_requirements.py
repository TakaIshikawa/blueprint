"""Extract database sharding and horizontal partitioning requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for database sharding concepts
_SHARDING_KEY_SELECTION_RE = re.compile(
    r"\b(?:sharding[_\s]+key|shard[_\s]+key|partition[_\s]+key[_\s]+selection|"
    r"shard[_\s]+by|partition[_\s]+by|sharding[_\s]+strategy|"
    r"shard[_\s]+column|distribution[_\s]+key|hash[_\s]+key)\b",
    re.I,
)
_SHARD_ROUTING_LOGIC_RE = re.compile(
    r"\b(?:shard[_\s]+routing|routing[_\s]+logic|shard[_\s]+lookup|"
    r"shard[_\s]+map|route[_\s]+to[_\s]+shard|shard[_\s]+directory|"
    r"shard[_\s]+locator|consistent[_\s]+hashing|hash[_\s]+ring)\b",
    re.I,
)
_CROSS_SHARD_QUERY_HANDLING_RE = re.compile(
    r"\b(?:cross[_\s]+shard[_\s]+query|cross[_\s]+shard[_\s]+join|"
    r"scatter[_\s]+gather|fan[_\s]+out[_\s]+query|distributed[_\s]+query|"
    r"global[_\s]+query|cross[_\s]+partition[_\s]+query|federated[_\s]+query)\b",
    re.I,
)
_SHARD_REBALANCING_STRATEGY_RE = re.compile(
    r"\b(?:shard[_\s]+rebalancing|rebalance[_\s]+shards|shard[_\s]+migration|"
    r"move[_\s]+shard|split[_\s]+shard|merge[_\s]+shard|"
    r"shard[_\s]+redistribution|shard[_\s]+scaling|dynamic[_\s]+sharding)\b",
    re.I,
)
_CONSISTENT_HASHING_RE = re.compile(
    r"\b(?:consistent[_\s]+hashing|hash[_\s]+ring|virtual[_\s]+node|"
    r"vnodes|consistent[_\s]+hash|hash[_\s]+partitioning|"
    r"hash[_\s]+based[_\s]+routing|hash[_\s]+function|hash[_\s]+distribution)\b",
    re.I,
)
_SHARD_MIGRATION_PROCEDURES_RE = re.compile(
    r"\b(?:shard[_\s]+migration|migrate[_\s]+shard|shard[_\s]+data[_\s]+migration|"
    r"shard[_\s]+cutover|shard[_\s]+transfer|"
    r"move[_\s]+data[_\s]+between[_\s]+shards|online[_\s]+resharding)\b",
    re.I,
)
_CROSS_SHARD_TRANSACTION_HANDLING_RE = re.compile(
    r"\b(?:cross[_\s]+shard[_\s]+transaction|distributed[_\s]+transaction|"
    r"two[_\s]+phase[_\s]+commit|2pc|saga[_\s]+pattern|"
    r"cross[_\s]+partition[_\s]+transaction|global[_\s]+transaction|"
    r"transaction[_\s]+coordinator)\b",
    re.I,
)
_SHARD_MONITORING_RE = re.compile(
    r"\b(?:shard[_\s]+monitoring|shard[_\s]+health|shard[_\s]+metrics|"
    r"shard[_\s]+dashboard|monitor[_\s]+shards|shard[_\s]+alerting|"
    r"shard[_\s]+performance|shard[_\s]+status|shard[_\s]+utilization)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DatabaseShardingRequirements:
    """Database sharding and horizontal partitioning requirements extracted from source brief."""

    sharding_key_selection_specified: bool = False
    shard_routing_logic_defined: bool = False
    cross_shard_query_handling_specified: bool = False
    shard_rebalancing_strategy_defined: bool = False
    consistent_hashing_specified: bool = False
    shard_migration_procedures_defined: bool = False
    cross_shard_transaction_handling_specified: bool = False
    shard_monitoring_included: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.sharding_key_selection_specified,
            self.shard_routing_logic_defined,
            self.cross_shard_query_handling_specified,
            self.shard_rebalancing_strategy_defined,
            self.consistent_hashing_specified,
            self.shard_migration_procedures_defined,
            self.cross_shard_transaction_handling_specified,
            self.shard_monitoring_included,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "sharding_key_selection_specified": self.sharding_key_selection_specified,
            "shard_routing_logic_defined": self.shard_routing_logic_defined,
            "cross_shard_query_handling_specified": self.cross_shard_query_handling_specified,
            "shard_rebalancing_strategy_defined": self.shard_rebalancing_strategy_defined,
            "consistent_hashing_specified": self.consistent_hashing_specified,
            "shard_migration_procedures_defined": self.shard_migration_procedures_defined,
            "cross_shard_transaction_handling_specified": self.cross_shard_transaction_handling_specified,
            "shard_monitoring_included": self.shard_monitoring_included,
            "completeness_score": self.completeness_score,
        }


def extract_database_sharding_requirements(source_data: Mapping[str, Any]) -> DatabaseShardingRequirements:
    """
    Extract database sharding requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        DatabaseShardingRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return DatabaseShardingRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return DatabaseShardingRequirements(
        sharding_key_selection_specified=bool(_SHARDING_KEY_SELECTION_RE.search(searchable_text)),
        shard_routing_logic_defined=bool(_SHARD_ROUTING_LOGIC_RE.search(searchable_text)),
        cross_shard_query_handling_specified=bool(_CROSS_SHARD_QUERY_HANDLING_RE.search(searchable_text)),
        shard_rebalancing_strategy_defined=bool(_SHARD_REBALANCING_STRATEGY_RE.search(searchable_text)),
        consistent_hashing_specified=bool(_CONSISTENT_HASHING_RE.search(searchable_text)),
        shard_migration_procedures_defined=bool(_SHARD_MIGRATION_PROCEDURES_RE.search(searchable_text)),
        cross_shard_transaction_handling_specified=bool(_CROSS_SHARD_TRANSACTION_HANDLING_RE.search(searchable_text)),
        shard_monitoring_included=bool(_SHARD_MONITORING_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "summary", "rationale"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "constraints", "notes", "definition_of_done"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DatabaseShardingRequirements",
    "extract_database_sharding_requirements",
]
