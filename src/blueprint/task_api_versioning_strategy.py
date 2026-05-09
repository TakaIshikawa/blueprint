"""Analyze API versioning strategy for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for API versioning concepts
_URL_VERSIONING_RE = re.compile(
    r"\b(?:url[_\s-]*version(?:ing)?|/v\d+|api[_\s-]*v\d+|path[_\s-]*version|"
    r"version(?:ed)?[_\s-]*url|endpoint[_\s-]*version|"
    r"test[_\s-]*url[_\s-]*version)\b",
    re.I,
)
_HEADER_VERSIONING_RE = re.compile(
    r"\b(?:header[_\s-]*version(?:ing)?|accept[_\s-]*version|api[_\s-]*version[_\s-]*header|"
    r"custom[_\s-]*header[_\s-]*version|version[_\s-]*header|"
    r"test[_\s-]*header[_\s-]*version)\b",
    re.I,
)
_CONTENT_NEGOTIATION_RE = re.compile(
    r"\b(?:content[_\s-]*negotiation|media[_\s-]*type[_\s-]*version|"
    r"accept[_\s-]*header|mime[_\s-]*type[_\s-]*version|"
    r"test[_\s-]*content[_\s-]*negotiation)\b",
    re.I,
)
_BREAKING_CHANGE_RE = re.compile(
    r"\b(?:breaking[_\s-]*change[s]?|non[_\s-]*breaking|backward[s]?[_\s-]*incompatib(?:le|ility)|"
    r"major[_\s-]*version|breaking[_\s-]*api[_\s-]*change|"
    r"test[_\s-]*breaking[_\s-]*change)\b",
    re.I,
)
_BACKWARDS_COMPATIBILITY_RE = re.compile(
    r"\b(?:backward[s]?[_\s-]*compatib(?:le|ility)|maintain[_\s-]*compatibility|"
    r"preserve[_\s-]*(?:existing[_\s-]*)?api|api[_\s-]*compatibility|"
    r"non[_\s-]*breaking[_\s-]*change|compatible[_\s-]*change|"
    r"test[_\s-]*(?:backward[s]?[_\s-]*)?compatibility)\b",
    re.I,
)
_DEPRECATION_TIMELINE_RE = re.compile(
    r"\b(?:deprecat(?:e|ion|ed)[_\s-]*(?:timeline|schedule|policy|notice)|"
    r"sunset[_\s-]*(?:timeline|schedule|policy|date)|"
    r"end[_\s-]*of[_\s-]*life|eol[_\s-]*(?:timeline|schedule)|"
    r"phase[_\s-]*out|retirement[_\s-]*schedule|"
    r"test[_\s-]*deprecation)\b",
    re.I,
)
_MIGRATION_PATH_RE = re.compile(
    r"\b(?:migration[_\s-]*path|upgrade[_\s-]*path|migration[_\s-]*(?:guide|strategy|plan)|"
    r"client[_\s-]*migration|version[_\s-]*migration|"
    r"gradual[_\s-]*migration|migration[_\s-]*document|"
    r"test[_\s-]*migration[_\s-]*path)\b",
    re.I,
)
_SEMANTIC_VERSIONING_RE = re.compile(
    r"\b(?:semantic[_\s-]*version(?:ing)?|semver|major[._]minor[._]patch|"
    r"version[_\s-]*(?:numbering|scheme)|calver|"
    r"test[_\s-]*semantic[_\s-]*version)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ApiVersioningStrategy:
    """API versioning strategy analysis for a task."""

    url_versioning_used: bool = False
    header_versioning_used: bool = False
    content_negotiation_used: bool = False
    breaking_changes_identified: bool = False
    backwards_compatibility_maintained: bool = False
    deprecation_timeline_defined: bool = False
    migration_path_documented: bool = False
    semantic_versioning_followed: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        # Versioning approach (at least one required)
        versioning_approaches = [
            self.url_versioning_used,
            self.header_versioning_used,
            self.content_negotiation_used,
        ]

        # Change management (critical for safe versioning)
        change_management = [
            self.breaking_changes_identified,
            self.backwards_compatibility_maintained,
            self.deprecation_timeline_defined,
            self.migration_path_documented,
        ]

        # Weight: versioning approach=30%, change management=60%, semantic versioning=10%
        approach_score = (1.0 if any(versioning_approaches) else 0.0) * 0.3
        change_score = sum(change_management) / len(change_management) * 0.6
        semver_score = (1.0 if self.semantic_versioning_followed else 0.0) * 0.1

        return approach_score + change_score + semver_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "url_versioning_used": self.url_versioning_used,
            "header_versioning_used": self.header_versioning_used,
            "content_negotiation_used": self.content_negotiation_used,
            "breaking_changes_identified": self.breaking_changes_identified,
            "backwards_compatibility_maintained": self.backwards_compatibility_maintained,
            "deprecation_timeline_defined": self.deprecation_timeline_defined,
            "migration_path_documented": self.migration_path_documented,
            "semantic_versioning_followed": self.semantic_versioning_followed,
            "readiness_score": self.readiness_score,
        }


def analyze_api_versioning_strategy(task_data: Mapping[str, Any]) -> ApiVersioningStrategy:
    """
    Analyze API versioning strategy from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        ApiVersioningStrategy with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return ApiVersioningStrategy()

    searchable_text = _extract_searchable_text(task_data)

    return ApiVersioningStrategy(
        url_versioning_used=bool(_URL_VERSIONING_RE.search(searchable_text)),
        header_versioning_used=bool(_HEADER_VERSIONING_RE.search(searchable_text)),
        content_negotiation_used=bool(_CONTENT_NEGOTIATION_RE.search(searchable_text)),
        breaking_changes_identified=bool(_BREAKING_CHANGE_RE.search(searchable_text)),
        backwards_compatibility_maintained=bool(_BACKWARDS_COMPATIBILITY_RE.search(searchable_text)),
        deprecation_timeline_defined=bool(_DEPRECATION_TIMELINE_RE.search(searchable_text)),
        migration_path_documented=bool(_MIGRATION_PATH_RE.search(searchable_text)),
        semantic_versioning_followed=bool(_SEMANTIC_VERSIONING_RE.search(searchable_text)),
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
    "ApiVersioningStrategy",
    "analyze_api_versioning_strategy",
]
