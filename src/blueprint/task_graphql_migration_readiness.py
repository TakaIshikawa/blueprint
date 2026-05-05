"""Analyze GraphQL migration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for GraphQL migration concepts
_SCHEMA_DEFINED_RE = re.compile(
    r"\b(?:graphql[_\s]+schema|schema[_\s]+(?:definition|defined|design)|"
    r"type[_\s]+definitions?|define[_\s]+(?:graphql[_\s]+)?schema|"
    r"schema[_\s]+(?:file|document)|\.graphql|gql[_\s]+schema|"
    r"sdl[_\s]+schema|schema[_\s]+first|test[_\s]+(?:graphql[_\s]+)?schema|"
    r"test[_\s]+schema[_\s]+definition)\b",
    re.I,
)
_RESOLVERS_PLANNED_RE = re.compile(
    r"\b(?:resolvers?[_\s]+(?:plan|planned|implementation|strategy|design|architecture)|"
    r"plan[_\s]+resolvers?|implement[_\s]+resolvers?|"
    r"resolvers?[_\s]+(?:function|logic|layer|mapping)|"
    r"query[_\s]+resolvers?|mutation[_\s]+resolvers?|field[_\s]+resolvers?|"
    r"test_resolver_implementation)\b",
    re.I,
)
_N_PLUS_ONE_RE = re.compile(
    r"\b(?:n[+\s]*1[_\s]+(?:query|queries|problem|issue)|"
    r"n\+1|dataloader|test_dataloader|batch[_\s]+(?:loading|loader)|"
    r"query[_\s]+optimization|prevent[_\s]+(?:over)?fetch(?:ing)?|"
    r"minimize[_\s]+(?:database[_\s]+)?queries|query[_\s]+batching)\b",
    re.I,
)
_CACHING_STRATEGY_RE = re.compile(
    r"\b(?:caching[_\s]+strategy|cache[_\s]+(?:strategy|plan|policy|layer)|"
    r"apollo[_\s]+cache|graphql[_\s]+cache|query[_\s]+caching|"
    r"response[_\s]+caching|cache[_\s]+invalidation|cdn[_\s]+caching)\b",
    re.I,
)
_BACKWARDS_COMPATIBILITY_RE = re.compile(
    r"\b(?:backward[s]?[_\s]+compatib(?:ility|le)|"
    r"maintain[_\s]+compatibility|preserve[_\s]+(?:existing[_\s]+)?api|"
    r"api[_\s]+versioning|graphql[_\s]+versioning|deprecat(?:e|ion)|"
    r"non[_\s-]*breaking[_\s]+change|schema[_\s]+evolution|"
    r"gradual[_\s]+migration|phased[_\s]+rollout)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class GraphQLMigrationReadiness:
    """GraphQL migration readiness analysis for a task."""

    schema_defined: bool = False
    resolvers_planned: bool = False
    n_plus_one_queries_addressed: bool = False
    caching_strategy_specified: bool = False
    backwards_compatibility_maintained: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 5
        passed_checks = sum([
            self.schema_defined,
            self.resolvers_planned,
            self.n_plus_one_queries_addressed,
            self.caching_strategy_specified,
            self.backwards_compatibility_maintained,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "schema_defined": self.schema_defined,
            "resolvers_planned": self.resolvers_planned,
            "n_plus_one_queries_addressed": self.n_plus_one_queries_addressed,
            "caching_strategy_specified": self.caching_strategy_specified,
            "backwards_compatibility_maintained": self.backwards_compatibility_maintained,
            "readiness_score": self.readiness_score,
        }


def analyze_graphql_migration_readiness(task_data: Mapping[str, Any]) -> GraphQLMigrationReadiness:
    """
    Analyze GraphQL migration readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        GraphQLMigrationReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return GraphQLMigrationReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return GraphQLMigrationReadiness(
        schema_defined=bool(_SCHEMA_DEFINED_RE.search(searchable_text)),
        resolvers_planned=bool(_RESOLVERS_PLANNED_RE.search(searchable_text)),
        n_plus_one_queries_addressed=bool(_N_PLUS_ONE_RE.search(searchable_text)),
        caching_strategy_specified=bool(_CACHING_STRATEGY_RE.search(searchable_text)),
        backwards_compatibility_maintained=bool(_BACKWARDS_COMPATIBILITY_RE.search(searchable_text)),
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
    "GraphQLMigrationReadiness",
    "analyze_graphql_migration_readiness",
]
