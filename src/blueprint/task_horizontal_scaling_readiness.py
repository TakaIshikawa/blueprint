"""Analyze horizontal scaling readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for horizontal scaling concepts
_STATELESS_DESIGN_RE = re.compile(
    r"\b(?:stateless\s+(?:design|architecture|service|application)|"
    r"stateless\s+components?|design\s+stateless|"
    r"make\s+(?:\w+\s+)?stateless|ensure\s+stateless|"
    r"(?:background\s+jobs?|scheduled\s+tasks?)\s+stateless|"
    r"stateless\s+(?:api|endpoint|handler|background\s+jobs?))\b",
    re.I,
)
_SESSION_HANDLING_RE = re.compile(
    r"\b(?:sessions?\s+(?:handling|management|storage|store)|"
    r"(?:external|distributed|centralized)\s+sessions?\s+(?:storage|store)|"
    r"(?:externalize|externalized?)\s+sessions?|"
    r"sessions?\s+(?:redis|memcached|database)|"
    r"shared\s+sessions?|sessions?\s+persistence|"
    r"sticky\s+sessions?|sessions?\s+affinity)\b",
    re.I,
)
_SHARED_STATE_RE = re.compile(
    r"\b(?:shared\s+state|(?:external|distributed|centralized)\s+state|"
    r"state\s+(?:storage|store|management)|"
    r"state\s+(?:redis|memcached|database)|"
    r"(?:redis|memcached)\s+(?:for\s+)?state|"
    r"distributed\s+(?:cache|storage))\b",
    re.I,
)
_LOAD_BALANCING_RE = re.compile(
    r"\b(?:load\s+balanc(?:er|ing)|load[- ]balanced?|"
    r"round[- ]robin|least[- ]connections?|"
    r"(?:nginx|haproxy|alb|elb|nlb)\s+load\s+balanc|"
    r"distribute\s+(?:traffic|load|requests?))\b",
    re.I,
)
_IN_MEMORY_STATE_RE = re.compile(
    r"\b(?:in[- ]memory\s+state|(?:avoid|eliminate|remove)\s+in[- ]memory\s+state|"
    r"local\s+state|instance[- ]local\s+state|"
    r"in[- ]process\s+(?:state|cache|storage)|"
    r"memory[- ]based\s+state)\b",
    re.I,
)
_FILE_SYSTEM_DEPENDENCIES_RE = re.compile(
    r"\b(?:file\s+system\s+dependenc(?:y|ies)|local\s+file\s+system|"
    r"(?:avoid|eliminate|remove)\s+file\s+system\s+dependenc|"
    r"shared\s+file\s+system|network\s+file\s+system|nfs|"
    r"(?:s3|blob\s+storage|object\s+storage)\s+instead\s+of|"
    r"(?:move|migrate)\s+(?:files?\s+)?to\s+(?:s3|blob\s+storage|object\s+storage)|"
    r"local\s+(?:disk|storage)\s+dependenc)\b",
    re.I,
)
_SINGLETON_PATTERNS_RE = re.compile(
    r"\b(?:singleton\s+(?:pattern|instance|class)|"
    r"(?:avoid|eliminate|remove)\s+singleton|"
    r"singleton\s+(?:problem|issue|concern)|"
    r"singleton\s+(?:anti[- ]pattern|blocker))\b",
    re.I,
)
_DISTRIBUTED_LOCKS_RE = re.compile(
    r"\b(?:distributed\s+locks?|distributed\s+locking|"
    r"(?:redis|zookeeper|etcd)\s+locks?|"
    r"lock\s+(?:coordination|management)|"
    r"pessimistic\s+locking|optimistic\s+locking|"
    r"distributed\s+(?:mutex|semaphore))\b",
    re.I,
)
_STATELESSNESS_RE = re.compile(
    r"\b(?:statelessness|achieve\s+statelessness|"
    r"ensure\s+statelessness|stateless\s+(?:instances?|servers?|nodes?))\b",
    re.I,
)
_EXTERNAL_STATE_STORAGE_RE = re.compile(
    r"\b(?:external\s+state\s+storage|externalize\s+state|"
    r"store\s+(?:\w+\s+)?state\s+(?:externally|in\s+(?:redis|database|cache))|"
    r"move\s+state\s+to\s+(?:redis|database|cache)|"
    r"store\s+(?:transaction|session|application)\s+state\s+externally)\b",
    re.I,
)
_CACHE_COHERENCE_RE = re.compile(
    r"\b(?:cache\s+coherence|cache\s+consistency|"
    r"cache\s+(?:invalidation|synchronization)|"
    r"distributed\s+cache|cache\s+(?:warming|coordination))\b",
    re.I,
)
_DEPLOYMENT_FLEXIBILITY_RE = re.compile(
    r"\b(?:deployment\s+flexibility|flexible\s+deployment|"
    r"(?:horizontal|scale[- ]out)\s+deployment|"
    r"(?:add|adding)\s+(?:instances?|servers?|nodes?)\s+(?:dynamically)?|"
    r"(?:support|enable|allow)\s+adding\s+(?:instances?|servers?|nodes?)|"
    r"scale\s+(?:horizontally|out)|auto[- ]?scal(?:e|ing))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class HorizontalScalingReadiness:
    """Horizontal scaling readiness analysis for a change brief."""

    stateless_design_implemented: bool = False
    session_handling_externalized: bool = False
    shared_state_managed: bool = False
    load_balancing_configured: bool = False
    in_memory_state_avoided: bool = False
    file_system_dependencies_removed: bool = False
    singleton_patterns_eliminated: bool = False
    distributed_locks_implemented: bool = False
    statelessness_achieved: bool = False
    external_state_storage_configured: bool = False
    cache_coherence_addressed: bool = False
    deployment_flexibility_enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "stateless_design_implemented": self.stateless_design_implemented,
            "session_handling_externalized": self.session_handling_externalized,
            "shared_state_managed": self.shared_state_managed,
            "load_balancing_configured": self.load_balancing_configured,
            "in_memory_state_avoided": self.in_memory_state_avoided,
            "file_system_dependencies_removed": self.file_system_dependencies_removed,
            "singleton_patterns_eliminated": self.singleton_patterns_eliminated,
            "distributed_locks_implemented": self.distributed_locks_implemented,
            "statelessness_achieved": self.statelessness_achieved,
            "external_state_storage_configured": self.external_state_storage_configured,
            "cache_coherence_addressed": self.cache_coherence_addressed,
            "deployment_flexibility_enabled": self.deployment_flexibility_enabled,
        }


def analyze_horizontal_scaling_readiness(change_brief: Mapping[str, Any]) -> HorizontalScalingReadiness:
    """
    Analyze horizontal scaling readiness from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        HorizontalScalingReadiness with boolean flags for each scaling aspect.
    """
    if not isinstance(change_brief, Mapping):
        return HorizontalScalingReadiness()

    searchable_text = _extract_searchable_text(change_brief)

    return HorizontalScalingReadiness(
        stateless_design_implemented=bool(_STATELESS_DESIGN_RE.search(searchable_text)),
        session_handling_externalized=bool(_SESSION_HANDLING_RE.search(searchable_text)),
        shared_state_managed=bool(_SHARED_STATE_RE.search(searchable_text)),
        load_balancing_configured=bool(_LOAD_BALANCING_RE.search(searchable_text)),
        in_memory_state_avoided=bool(_IN_MEMORY_STATE_RE.search(searchable_text)),
        file_system_dependencies_removed=bool(_FILE_SYSTEM_DEPENDENCIES_RE.search(searchable_text)),
        singleton_patterns_eliminated=bool(_SINGLETON_PATTERNS_RE.search(searchable_text)),
        distributed_locks_implemented=bool(_DISTRIBUTED_LOCKS_RE.search(searchable_text)),
        statelessness_achieved=bool(_STATELESSNESS_RE.search(searchable_text)),
        external_state_storage_configured=bool(_EXTERNAL_STATE_STORAGE_RE.search(searchable_text)),
        cache_coherence_addressed=bool(_CACHE_COHERENCE_RE.search(searchable_text)),
        deployment_flexibility_enabled=bool(_DEPLOYMENT_FLEXIBILITY_RE.search(searchable_text)),
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
    "HorizontalScalingReadiness",
    "analyze_horizontal_scaling_readiness",
]
