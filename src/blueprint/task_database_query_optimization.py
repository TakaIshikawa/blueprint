"""Analyze database query performance optimization readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for database query optimization concepts
_N_PLUS_ONE_RE = re.compile(
    r"\b(?:n\+1\s+(?:query|queries|problem|issue|pattern)|"
    r"n\s*\+\s*1|avoid\s+n\+1|prevent\s+n\+1|"
    r"eager\s+load(?:ing)?|prefetch(?:ing)?|batch\s+load(?:ing)?|"
    r"lazy\s+load(?:ing)?\s+(?:problem|issue)|"
    r"select\s+n\+1|query\s+in\s+loop)\b",
    re.I,
)
_INDEX_STRATEGY_RE = re.compile(
    r"\b(?:index(?:es|ing)?\s+(?:strategy|optimization|coverage|design)|"
    r"add\s+index(?:es)?|create\s+index(?:es)?|missing\s+index(?:es)?|"
    r"database\s+index(?:es)?|composite\s+index(?:es)?|"
    r"index\s+(?:column|field)s?|b-tree\s+index|hash\s+index|"
    r"covering\s+index|partial\s+index|index\s+hint)\b",
    re.I,
)
_INEFFICIENT_JOINS_RE = re.compile(
    r"\b(?:inefficient\s+joins?|optimize\s+joins?|joins?\s+optimization|"
    r"joins?\s+(?:strategy|performance|improvement)|"
    r"(?:inner|left|right|outer)\s+joins?\s+optimization|"
    r"joins?\s+(?:order|ordering)|avoid\s+cross\s+joins?|"
    r"cartesian\s+product|joins?\s+buffer)\b",
    re.I,
)
_FULL_TABLE_SCAN_RE = re.compile(
    r"\b(?:full\s+table\s+scan|(?:full\s+)?table\s+scans?|seq(?:uential)?\s+scans?|"
    r"avoid\s+(?:full\s+)?table\s+scans?|prevent\s+(?:full\s+)?table\s+scans?|"
    r"index\s+scans?|index\s+seeks?|scans?\s+vs\s+seeks?)\b",
    re.I,
)
_PAGINATION_OPTIMIZATION_RE = re.compile(
    r"\b(?:pagination\s+(?:optimization|strategy|performance)|"
    r"optimize\s+pagination|cursor[- ]based\s+pagination|"
    r"keyset\s+pagination|offset\s+pagination|"
    r"limit\s+offset\s+(?:problem|issue|optimization|performance)|"
    r"page\s+size\s+optimization|efficient\s+pagination|"
    r"address\s+limit\s+offset)\b",
    re.I,
)
_QUERY_PLAN_ANALYSIS_RE = re.compile(
    r"\b(?:query\s+plan|execution\s+plan|explain\s+plan|"
    r"explain\s+(?:analyze|query)|query\s+execution|"
    r"analyze\s+query\s+(?:plan|performance)|"
    r"query\s+optimizer|query\s+cost|query\s+hint)\b",
    re.I,
)
_QUERY_CACHING_RE = re.compile(
    r"\b(?:query\s+cach(?:e|ing)|cache\s+(?:query|queries|result)|"
    r"result\s+cach(?:e|ing)|materialized\s+view|"
    r"query\s+result\s+cach(?:e|ing)|redis\s+cach(?:e|ing)|"
    r"memcached|in[- ]memory\s+cach(?:e|ing))\b",
    re.I,
)
_CONNECTION_POOLING_RE = re.compile(
    r"\b(?:connection\s+pool(?:ing)?|database\s+connection\s+pool|"
    r"pool\s+(?:size|configuration)|pgbouncer|pgpool|"
    r"connection\s+(?:reuse|management|limit)|"
    r"max\s+connections|pool\s+timeout)\b",
    re.I,
)
_READ_REPLICAS_RE = re.compile(
    r"\b(?:read\s+replicas?|replicas?\s+(?:database|server|instances?)|"
    r"read[- ]write\s+split(?:ting)?|master[- ]slave|"
    r"primary[- ]replicas?|followers?\s+(?:read|instances?)|"
    r"route\s+reads?\s+to\s+(?:replica|follower)|read\s+scaling)\b",
    re.I,
)
_QUERY_COMPLEXITY_RE = re.compile(
    r"\b(?:query\s+complexity|complex\s+(?:query|queries)|"
    r"simplify\s+query|optimize\s+(?:complex\s+)?query|"
    r"subquery\s+optimization|cte\b|common\s+table\s+expressions?|"
    r"recursive\s+query|window\s+functions?|aggregates?\s+optimization|"
    r"optimize\s+aggregates?|refactor\s+query)\b",
    re.I,
)
_MONITORING_METRICS_RE = re.compile(
    r"\b(?:query\s+(?:monitoring|metrics|performance\s+monitoring)|"
    r"slow\s+query\s+log|query\s+logging|"
    r"(?:monitor|track|measure)\s+query\s+(?:performance|time|latency)|"
    r"apm|application\s+performance\s+monitoring|"
    r"query\s+telemetry|database\s+observability)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DatabaseQueryOptimizationReadiness:
    """Database query performance optimization readiness analysis for a change brief."""

    n_plus_one_addressed: bool = False
    index_strategy_defined: bool = False
    inefficient_joins_optimized: bool = False
    full_table_scans_avoided: bool = False
    pagination_optimized: bool = False
    query_plan_analysis_included: bool = False
    query_caching_configured: bool = False
    connection_pooling_implemented: bool = False
    read_replicas_considered: bool = False
    query_complexity_managed: bool = False
    monitoring_metrics_planned: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "n_plus_one_addressed": self.n_plus_one_addressed,
            "index_strategy_defined": self.index_strategy_defined,
            "inefficient_joins_optimized": self.inefficient_joins_optimized,
            "full_table_scans_avoided": self.full_table_scans_avoided,
            "pagination_optimized": self.pagination_optimized,
            "query_plan_analysis_included": self.query_plan_analysis_included,
            "query_caching_configured": self.query_caching_configured,
            "connection_pooling_implemented": self.connection_pooling_implemented,
            "read_replicas_considered": self.read_replicas_considered,
            "query_complexity_managed": self.query_complexity_managed,
            "monitoring_metrics_planned": self.monitoring_metrics_planned,
        }


def analyze_database_query_optimization(change_brief: Mapping[str, Any]) -> DatabaseQueryOptimizationReadiness:
    """
    Analyze database query performance optimization readiness from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        DatabaseQueryOptimizationReadiness with boolean flags for each optimization aspect.
    """
    if not isinstance(change_brief, Mapping):
        return DatabaseQueryOptimizationReadiness()

    searchable_text = _extract_searchable_text(change_brief)

    return DatabaseQueryOptimizationReadiness(
        n_plus_one_addressed=bool(_N_PLUS_ONE_RE.search(searchable_text)),
        index_strategy_defined=bool(_INDEX_STRATEGY_RE.search(searchable_text)),
        inefficient_joins_optimized=bool(_INEFFICIENT_JOINS_RE.search(searchable_text)),
        full_table_scans_avoided=bool(_FULL_TABLE_SCAN_RE.search(searchable_text)),
        pagination_optimized=bool(_PAGINATION_OPTIMIZATION_RE.search(searchable_text)),
        query_plan_analysis_included=bool(_QUERY_PLAN_ANALYSIS_RE.search(searchable_text)),
        query_caching_configured=bool(_QUERY_CACHING_RE.search(searchable_text)),
        connection_pooling_implemented=bool(_CONNECTION_POOLING_RE.search(searchable_text)),
        read_replicas_considered=bool(_READ_REPLICAS_RE.search(searchable_text)),
        query_complexity_managed=bool(_QUERY_COMPLEXITY_RE.search(searchable_text)),
        monitoring_metrics_planned=bool(_MONITORING_METRICS_RE.search(searchable_text)),
    )


def _extract_searchable_text(change_brief: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the change brief for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = change_brief.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = change_brief.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = change_brief.get("validation_command") or change_brief.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DatabaseQueryOptimizationReadiness",
    "analyze_database_query_optimization",
]
