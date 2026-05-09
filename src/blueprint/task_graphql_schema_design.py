"""Analyze GraphQL schema design readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for GraphQL schema design concepts
_TYPE_DEFINITIONS_RE = re.compile(
    r"\b(?:type[_\s-]+definition[s]?|graphql[_\s-]+type[s]?|object[_\s-]+type[s]?|"
    r"interface[_\s-]+type[s]?|union[_\s-]+type[s]?|enum[_\s-]+type[s]?|"
    r"scalar[_\s-]+type[s]?|input[_\s-]+type[s]?|"
    r"type[_\s-]+system|schema[_\s-]+type[s]?|"
    r"implement[_\s-]+interface[s]?|type[_\s-]+extension[s]?|"
    r"test[_\s-]+type[_\s-]+definition)\b",
    re.I,
)
_QUERY_MUTATION_DESIGN_RE = re.compile(
    r"\b(?:query[_\s-]+(?:design|pattern[s]?|structure)|"
    r"mutation[_\s-]+(?:design|pattern[s]?|structure)|"
    r"design[_\s-]+(?:query|queries|mutation[s]?)|"
    r"query[_\s-]+type|mutation[_\s-]+type|"
    r"root[_\s-]+(?:query|mutation)|graphql[_\s-]+(?:query|mutation)[s]?|"
    r"query[_\s-]+field[s]?|mutation[_\s-]+field[s]?|"
    r"subscription[_\s-]+type|test[_\s-]+(?:query|mutation))\b",
    re.I,
)
_FIELD_RESOLVERS_RE = re.compile(
    r"\b(?:field[_\s-]+resolver[s]?|resolver[_\s-]+function[s]?|"
    r"resolve[_\s-]+field[s]?|resolver[_\s-]+implementation[s]?|"
    r"resolver[_\s-]+map|resolver[_\s-]+chain|"
    r"custom[_\s-]+resolver[s]?|async[_\s-]+resolver[s]?|"
    r"parent[_\s-]+resolver[s]?|context[_\s-]+resolver[s]?|"
    r"context\s+in\s+resolver[s]?|"
    r"test[_\s-]+resolver[s]?)\b",
    re.I,
)
_PAGINATION_PATTERNS_RE = re.compile(
    r"\b(?:pagination[_\s-]*(?:pattern[s]?|strategy|strateg(?:y|ies))?|"
    r"relay[_\s-]+(?:pagination|cursor|connection[s]?|edge[s]?)|"
    r"cursor[_\s-]+(?:based|pagination)|offset[_\s-]+(?:based|pagination|limit)|"
    r"page[_\s-]*info|page[_\s-]+(?:based)|connection[_\s-]+type[s]?|"
    r"edge[_\s-]+type[s]?|has[_\s-]*(?:next|previous)[_\s-]*page|"
    r"test[_\s-]+pagination)\b",
    re.I,
)
_ERROR_HANDLING_STRATEGY_RE = re.compile(
    r"\b(?:graphql[_\s-]+error[s]?|error[_\s-]+(?:type[s]?|handling|extension[s]?)|"
    r"handle[_\s-]+error[s]?|handling[_\s-]+error[s]?|"
    r"error[_\s-]+response[s]?|error[_\s-]+field[s]?|"
    r"error[_\s-]+code[s]?|error[_\s-]+message[s]?|"
    r"error[_\s-]+path[s]?|error[_\s-]+location[s]?|"
    r"union[_\s-]+error[s]?|result[_\s-]+type[s]?|"
    r"test[_\s-]+(?:graphql[_\s-]+)?error)\b",
    re.I,
)
_N_PLUS_ONE_PREVENTION_RE = re.compile(
    r"\b(?:n[_\s-]*(?:\+|plus)[_\s-]*(?:1|one)|"
    r"dataloader|data[_\s-]+loader|batch[_\s-]+(?:loading|loader)|"
    r"batching[_\s-]+(?:strateg(?:y|ies)|request[s]?)|"
    r"query[_\s-]+(?:batching|optimization)|"
    r"eager[_\s-]+loading|lazy[_\s-]+loading|"
    r"test[_\s-]+(?:dataloader|n[_\s-]*(?:\+|plus)[_\s-]*one))\b",
    re.I,
)
_OVERFETCHING_MINIMIZED_RE = re.compile(
    r"\b(?:overfetching|over[_\s-]*fetching|"
    r"field[_\s-]+selection|selective[_\s-]+(?:field[s]?|query|queries)|"
    r"fragment[s]?|graphql[_\s-]+fragment[s]?|"
    r"inline[_\s-]+fragment[s]?|named[_\s-]+fragment[s]?|"
    r"spread[_\s-]+fragment[s]?|fragment[_\s-]+composition|"
    r"query[_\s-]+depth[_\s-]+limit|query[_\s-]+complexity|"
    r"test[_\s-]+overfetching)\b",
    re.I,
)
_SCHEMA_STITCHING_RE = re.compile(
    r"\b(?:schema[_\s-]+(?:stitching|federation)|"
    r"federated[_\s-]+(?:schema|graphql|gateway)|"
    r"apollo[_\s-]+federation|subgraph[s]?|"
    r"gateway[_\s-]+(?:layer|pattern|implementation)|"
    r"merge[_\s-]+schema[s]?|remote[_\s-]+schema[s]?|"
    r"schema[_\s-]+composition|extend[_\s-]+type[s]?|"
    r"test[_\s-]+(?:federation|stitching))\b",
    re.I,
)
_VERSIONING_STRATEGY_RE = re.compile(
    r"\b(?:(?:schema|api)[_\s-]+versioning|version[_\s-]+(?:strateg(?:y|ies)|control)|"
    r"implement[_\s-]+versioning|"
    r"backward[_\s-]+compatib(?:le|ility)|forward[_\s-]+compatib(?:le|ility)|"
    r"schema[_\s-]+evolution|breaking[_\s-]+change[s]?|"
    r"non[_\s-]*breaking[_\s-]+change[s]?|additive[_\s-]+change[s]?|"
    r"version[_\s-]+number|api[_\s-]+version|"
    r"test[_\s-]+versioning)\b",
    re.I,
)
_DEPRECATION_HANDLING_RE = re.compile(
    r"\b(?:deprecat(?:e|ed|ion|ing)[_\s-]*(?:field[s]?|type[s]?|argument[s]?|directive[s]?)?|"
    r"@deprecated|deprecation[_\s-]+(?:reason|message|notice|warning)|"
    r"migration[_\s-]+(?:path|strateg(?:y|ies)|guide)|"
    r"sunset[_\s-]+(?:field[s]?|type[s]?)|remove[_\s-]+deprecated|"
    r"deprecation[_\s-]+polic(?:y|ies)|"
    r"test[_\s-]+deprecation)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class GraphQLSchemaDesignReadiness:
    """GraphQL schema design readiness analysis for a task."""

    type_definitions_complete: bool = False
    query_mutation_design: bool = False
    field_resolvers_implemented: bool = False
    pagination_patterns: bool = False
    error_handling_strategy: bool = False
    n_plus_one_prevention: bool = False
    overfetching_minimized: bool = False
    schema_stitching_ready: bool = False
    versioning_strategy: bool = False
    deprecation_handling: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        # Performance optimization (critical for scalability)
        performance_checks = [
            self.n_plus_one_prevention,
            self.overfetching_minimized,
            self.pagination_patterns,
        ]

        # Client ergonomics (critical for DX)
        client_checks = [
            self.query_mutation_design,
            self.error_handling_strategy,
            self.type_definitions_complete,
        ]

        # Extensibility (important for long-term maintenance)
        extensibility_checks = [
            self.versioning_strategy,
            self.deprecation_handling,
            self.schema_stitching_ready,
        ]

        # Documentation quality (important for adoption)
        documentation_checks = [
            self.field_resolvers_implemented,
        ]

        # Weight: performance_optimization=30%, client_ergonomics=25%, extensibility=25%, documentation_quality=20%
        performance_score = sum(performance_checks) / len(performance_checks) * 0.3
        client_score = sum(client_checks) / len(client_checks) * 0.25
        extensibility_score = sum(extensibility_checks) / len(extensibility_checks) * 0.25
        documentation_score = sum(documentation_checks) / len(documentation_checks) * 0.2

        return performance_score + client_score + extensibility_score + documentation_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "type_definitions_complete": self.type_definitions_complete,
            "query_mutation_design": self.query_mutation_design,
            "field_resolvers_implemented": self.field_resolvers_implemented,
            "pagination_patterns": self.pagination_patterns,
            "error_handling_strategy": self.error_handling_strategy,
            "n_plus_one_prevention": self.n_plus_one_prevention,
            "overfetching_minimized": self.overfetching_minimized,
            "schema_stitching_ready": self.schema_stitching_ready,
            "versioning_strategy": self.versioning_strategy,
            "deprecation_handling": self.deprecation_handling,
            "readiness_score": self.readiness_score,
        }


def analyze_graphql_schema_design_readiness(task_data: Mapping[str, Any]) -> GraphQLSchemaDesignReadiness:
    """
    Analyze GraphQL schema design readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        GraphQLSchemaDesignReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return GraphQLSchemaDesignReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return GraphQLSchemaDesignReadiness(
        type_definitions_complete=bool(_TYPE_DEFINITIONS_RE.search(searchable_text)),
        query_mutation_design=bool(_QUERY_MUTATION_DESIGN_RE.search(searchable_text)),
        field_resolvers_implemented=bool(_FIELD_RESOLVERS_RE.search(searchable_text)),
        pagination_patterns=bool(_PAGINATION_PATTERNS_RE.search(searchable_text)),
        error_handling_strategy=bool(_ERROR_HANDLING_STRATEGY_RE.search(searchable_text)),
        n_plus_one_prevention=bool(_N_PLUS_ONE_PREVENTION_RE.search(searchable_text)),
        overfetching_minimized=bool(_OVERFETCHING_MINIMIZED_RE.search(searchable_text)),
        schema_stitching_ready=bool(_SCHEMA_STITCHING_RE.search(searchable_text)),
        versioning_strategy=bool(_VERSIONING_STRATEGY_RE.search(searchable_text)),
        deprecation_handling=bool(_DEPRECATION_HANDLING_RE.search(searchable_text)),
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
    "GraphQLSchemaDesignReadiness",
    "analyze_graphql_schema_design_readiness",
]
