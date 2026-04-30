"""Deterministic conflict detection for SourceBrief-shaped dictionaries."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal


SourceBriefConflictSeverity = Literal["low", "medium", "high"]

NESTED_MAPPING_FIELDS = frozenset({"source_payload", "source_links"})
OPTIONAL_FIELDS = frozenset({"domain", "created_at", "updated_at"})
PRIORITY_FIELDS = (
    "priority",
    "priority_level",
    "severity",
    "urgency",
    "impact",
)
CONFLICT_FIELD_ORDER = {
    "scope_vs_non_goals": 0,
    "constraints": 1,
    "target_repo": 2,
    "priority": 3,
}

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_LEADING_SCOPE_RE = re.compile(
    r"^(add|build|create|deliver|enable|implement|include|support|ship|update)\s+"
)
_LEADING_NON_GOAL_RE = re.compile(
    r"^(avoid|do\s+not|don't|exclude|no|not|out\s+of\s+scope|skip|without)\s+"
)
_LEADING_CONSTRAINT_RE = re.compile(
    r"^(must|should|shall|need\s+to|needs\s+to|require|requires|use|using)\s+"
)
_NEGATIVE_CONSTRAINT_RE = re.compile(
    r"\b(avoid|cannot|can't|do\s+not|don't|must\s+not|never|no|not|without)\b"
)


@dataclass(frozen=True, slots=True)
class SourceBriefConflict:
    """One likely conflict across SourceBrief inputs."""

    field: str
    source_ids: list[str]
    values: list[dict[str, Any]]
    severity: SourceBriefConflictSeverity
    explanation: str

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable conflict payload."""
        return {
            "field": self.field,
            "source_ids": self.source_ids,
            "values": self.values,
            "severity": self.severity,
            "explanation": self.explanation,
        }


def detect_source_brief_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    """Return deterministic heuristic conflicts across SourceBrief dictionaries."""
    normalized = [
        _normalize_source_brief(source_brief, index)
        for index, source_brief in enumerate(source_briefs)
    ]

    conflicts: list[SourceBriefConflict] = []
    conflicts.extend(_scope_non_goal_conflicts(normalized))
    conflicts.extend(_target_repo_conflicts(normalized))
    conflicts.extend(_priority_conflicts(normalized))
    conflicts.extend(_constraint_conflicts(normalized))

    return sorted(conflicts, key=_conflict_sort_key)


def _normalize_source_brief(value: dict[str, Any], index: int) -> dict[str, Any]:
    normalized = dict(value)
    for field_name in OPTIONAL_FIELDS:
        normalized.setdefault(field_name, None)
    for field_name in NESTED_MAPPING_FIELDS:
        if field_name not in normalized or normalized[field_name] is None:
            normalized[field_name] = {}
    if not isinstance(normalized["source_payload"], dict):
        normalized["source_payload"] = {}
    normalized["_conflict_source_id"] = _source_identifier(normalized, index)
    return normalized


def _scope_non_goal_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    scope_entries = _text_entries(source_briefs, "scope", "scope")
    non_goal_entries = _text_entries(source_briefs, "non_goals", "non-goal")
    conflicts: list[SourceBriefConflict] = []
    seen: set[tuple[str, str, str]] = set()

    for scope in scope_entries:
        for non_goal in non_goal_entries:
            if scope["source_id"] == non_goal["source_id"]:
                continue
            if scope["key"] != non_goal["key"]:
                continue
            seen_key = (
                scope["key"],
                scope["source_id"],
                non_goal["source_id"],
            )
            if seen_key in seen:
                continue
            seen.add(seen_key)
            conflicts.append(
                _conflict(
                    field="scope_vs_non_goals",
                    values=[scope, non_goal],
                    severity="high",
                    explanation=(
                        "A scoped item in one source brief is explicitly excluded "
                        "by another source brief."
                    ),
                )
            )
    return conflicts


def _target_repo_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    entries = _scalar_entries(source_briefs, "target_repo")
    normalized_values = {_normalize_repo(entry["value"]) for entry in entries}
    if len(normalized_values) <= 1:
        return []
    return [
        _conflict(
            field="target_repo",
            values=entries,
            severity="high",
            explanation="Source briefs point to different target repositories.",
        )
    ]


def _priority_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    conflicts: list[SourceBriefConflict] = []
    for field_name in PRIORITY_FIELDS:
        entries = _scalar_entries(source_briefs, field_name)
        normalized_values = {_normalize_text(str(entry["value"])) for entry in entries}
        if len(normalized_values) <= 1:
            continue
        conflicts.append(
            _conflict(
                field="priority",
                values=entries,
                severity="medium",
                explanation=(
                    f"Priority-like metadata '{field_name}' has incompatible values."
                ),
            )
        )
    return conflicts


def _constraint_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    conflicts = _structured_constraint_conflicts(source_briefs)
    conflicts.extend(_text_constraint_conflicts(source_briefs))
    return conflicts


def _structured_constraint_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    entries_by_key: dict[str, list[dict[str, Any]]] = {}
    for source_brief in source_briefs:
        constraints = _field_value(source_brief, "constraints")
        if not isinstance(constraints, dict):
            continue
        for key in sorted(constraints, key=str):
            value = constraints[key]
            if _is_empty(value):
                continue
            entries_by_key.setdefault(str(key), []).append(
                {
                    "source_id": source_brief["_conflict_source_id"],
                    "role": "constraint",
                    "value": value,
                }
            )

    conflicts: list[SourceBriefConflict] = []
    for key in sorted(entries_by_key):
        entries = entries_by_key[key]
        normalized_values = {_freeze_jsonish(entry["value"]) for entry in entries}
        if len(normalized_values) <= 1:
            continue
        conflicts.append(
            _conflict(
                field="constraints",
                values=entries,
                severity="medium",
                explanation=f"Structured constraint '{key}' has incompatible values.",
            )
        )
    return conflicts


def _text_constraint_conflicts(
    source_briefs: list[dict[str, Any]],
) -> list[SourceBriefConflict]:
    entries = _text_entries(source_briefs, "constraints", "constraint")
    conflicts: list[SourceBriefConflict] = []
    seen: set[tuple[str, str, str]] = set()

    for left_index, left in enumerate(entries):
        for right in entries[left_index + 1 :]:
            if left["source_id"] == right["source_id"]:
                continue
            if left["key"] != right["key"]:
                continue
            if left["negative"] == right["negative"]:
                continue
            source_pair = tuple(sorted((left["source_id"], right["source_id"])))
            seen_key = (left["key"], source_pair[0], source_pair[1])
            if seen_key in seen:
                continue
            seen.add(seen_key)
            conflicts.append(
                _conflict(
                    field="constraints",
                    values=[left, right],
                    severity="medium",
                    explanation=(
                        "Constraint language requires and forbids the same item "
                        "across source briefs."
                    ),
                )
            )
    return conflicts


def _text_entries(
    source_briefs: list[dict[str, Any]],
    field_name: str,
    role: str,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for source_brief in source_briefs:
        for value in _string_items(_field_value(source_brief, field_name)):
            key = _canonical_item_key(value, field_name)
            if not key:
                continue
            entry = {
                "source_id": source_brief["_conflict_source_id"],
                "role": role,
                "value": value,
                "key": key,
            }
            if field_name == "constraints":
                entry["negative"] = bool(
                    _NEGATIVE_CONSTRAINT_RE.search(_normalize_text(value))
                )
            entries.append(entry)
    return sorted(
        entries,
        key=lambda entry: (entry["key"], entry["source_id"], entry["value"]),
    )


def _scalar_entries(
    source_briefs: list[dict[str, Any]],
    field_name: str,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for source_brief in source_briefs:
        value = _field_value(source_brief, field_name)
        if _is_empty(value):
            continue
        entries.append(
            {
                "source_id": source_brief["_conflict_source_id"],
                "value": value,
            }
        )
    return sorted(entries, key=lambda entry: (entry["source_id"], str(entry["value"])))


def _field_value(source_brief: dict[str, Any], field_name: str) -> Any:
    value = source_brief.get(field_name)
    if not _is_empty(value):
        return value
    return source_brief.get("source_payload", {}).get(field_name)


def _conflict(
    *,
    field: str,
    values: list[dict[str, Any]],
    severity: SourceBriefConflictSeverity,
    explanation: str,
) -> SourceBriefConflict:
    public_values = [
        {key: value for key, value in entry.items() if key not in {"key", "negative"}}
        for entry in values
    ]
    public_values = sorted(
        public_values,
        key=lambda entry: (
            str(entry.get("source_id")),
            str(entry.get("role", "")),
            str(entry.get("value")),
        ),
    )
    return SourceBriefConflict(
        field=field,
        source_ids=sorted({str(entry["source_id"]) for entry in public_values}),
        values=public_values,
        severity=severity,
        explanation=explanation,
    )


def _conflict_sort_key(conflict: SourceBriefConflict) -> tuple[Any, ...]:
    return (
        CONFLICT_FIELD_ORDER.get(conflict.field, 99),
        conflict.field,
        conflict.source_ids,
        str(conflict.values),
        conflict.explanation,
    )


def _source_identifier(source_brief: dict[str, Any], index: int) -> str:
    for field_name in ("id", "source_id"):
        value = source_brief.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"source-{index + 1}"


def _string_items(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _canonical_item_key(value: str, field_name: str) -> str:
    normalized = _normalize_text(value)
    if field_name == "non_goals":
        normalized = _LEADING_NON_GOAL_RE.sub("", normalized)
    if field_name == "scope":
        normalized = _LEADING_SCOPE_RE.sub("", normalized)
    if field_name == "constraints":
        normalized = _normalize_text(_NEGATIVE_CONSTRAINT_RE.sub("", normalized))
    normalized = _LEADING_SCOPE_RE.sub("", normalized)
    normalized = _strip_leading_constraint_words(normalized)
    return _normalize_text(normalized)


def _normalize_repo(value: Any) -> str:
    normalized = _normalize_text(str(value)).removesuffix(".git")
    for prefix in ("https github com ", "http github com ", "git github com "):
        if normalized.startswith(prefix):
            normalized = normalized.removeprefix(prefix)
    normalized = normalized.removesuffix(" git")
    return normalized


def _normalize_text(value: str) -> str:
    return _NON_ALNUM_RE.sub(" ", value.casefold()).strip()


def _strip_leading_constraint_words(value: str) -> str:
    normalized = value
    while True:
        stripped = _LEADING_CONSTRAINT_RE.sub("", normalized)
        if stripped == normalized:
            return normalized
        normalized = stripped


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _freeze_jsonish(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _freeze_jsonish(item)) for key, item in value.items())
        )
    if isinstance(value, list):
        return tuple(_freeze_jsonish(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_jsonish(item) for item in value)
    return value


__all__ = [
    "SourceBriefConflict",
    "SourceBriefConflictSeverity",
    "detect_source_brief_conflicts",
]
