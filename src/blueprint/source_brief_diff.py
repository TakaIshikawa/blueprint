"""Semantic diff helpers for SourceBrief-shaped dictionaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


SourceBriefDiffStatus = Literal["added", "removed", "changed"]
SourceBriefDiffScope = Literal["top_level", "source_payload", "source_links"]

TIMESTAMP_FIELDS = frozenset({"created_at", "updated_at"})
NESTED_MAPPING_FIELDS = frozenset({"source_payload", "source_links"})
OPTIONAL_FIELDS = frozenset({"domain", "created_at", "updated_at"})
SOURCE_BRIEF_FIELD_ORDER = (
    "id",
    "title",
    "domain",
    "summary",
    "source_project",
    "source_entity_type",
    "source_id",
    "source_payload",
    "source_links",
    "created_at",
    "updated_at",
)

_MISSING = object()


@dataclass(frozen=True, slots=True)
class SourceBriefFieldDiff:
    """One changed SourceBrief field or nested mapping key."""

    path: str
    status: SourceBriefDiffStatus
    before: Any = None
    after: Any = None
    scope: SourceBriefDiffScope = "top_level"

    @property
    def field(self) -> str:
        """Return the final path component for this change."""
        return self.path.rsplit(".", 1)[-1]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable change payload."""
        return {
            "path": self.path,
            "field": self.field,
            "scope": self.scope,
            "status": self.status,
            "before": self.before,
            "after": self.after,
        }


@dataclass(frozen=True, slots=True)
class SourceBriefDiff:
    """Structured semantic diff between two SourceBrief payloads."""

    changes: list[SourceBriefFieldDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return True when the compared briefs differ semantically."""
        return bool(self.changes)

    @property
    def added(self) -> list[SourceBriefFieldDiff]:
        """Return added fields and nested keys."""
        return [change for change in self.changes if change.status == "added"]

    @property
    def removed(self) -> list[SourceBriefFieldDiff]:
        """Return removed fields and nested keys."""
        return [change for change in self.changes if change.status == "removed"]

    @property
    def changed(self) -> list[SourceBriefFieldDiff]:
        """Return changed fields and nested keys."""
        return [change for change in self.changes if change.status == "changed"]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable machine-readable diff payload."""
        return {
            "has_changes": self.has_changes,
            "summary": {
                "added": len(self.added),
                "removed": len(self.removed),
                "changed": len(self.changed),
                "total": len(self.changes),
            },
            "changes": [change.to_dict() for change in self.changes],
        }


def diff_source_briefs(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    include_timestamps: bool = False,
) -> SourceBriefDiff:
    """Compare two SourceBrief-shaped dictionaries and return a stable semantic diff."""
    before_normalized = _normalize_source_brief(before)
    after_normalized = _normalize_source_brief(after)
    fields = _ordered_fields(before_normalized, after_normalized)
    changes: list[SourceBriefFieldDiff] = []

    for field_name in fields:
        if not include_timestamps and field_name in TIMESTAMP_FIELDS:
            continue

        if field_name in NESTED_MAPPING_FIELDS:
            changes.extend(
                _diff_nested_mapping(
                    field_name,
                    before_normalized.get(field_name, {}),
                    after_normalized.get(field_name, {}),
                )
            )
            continue

        change = _diff_value(
            path=field_name,
            scope="top_level",
            before=before_normalized.get(field_name, _MISSING),
            after=after_normalized.get(field_name, _MISSING),
        )
        if change is not None:
            changes.append(change)

    return SourceBriefDiff(changes=changes)


def _normalize_source_brief(value: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    for field_name in OPTIONAL_FIELDS:
        normalized.setdefault(field_name, None)
    for field_name in NESTED_MAPPING_FIELDS:
        if field_name not in normalized or normalized[field_name] is None:
            normalized[field_name] = {}
    return normalized


def _ordered_fields(
    before: dict[str, Any],
    after: dict[str, Any],
) -> list[str]:
    fields = set(before) | set(after)
    ordered = [field_name for field_name in SOURCE_BRIEF_FIELD_ORDER if field_name in fields]
    ordered.extend(sorted(fields - set(SOURCE_BRIEF_FIELD_ORDER)))
    return ordered


def _diff_nested_mapping(
    field_name: str,
    before: Any,
    after: Any,
) -> list[SourceBriefFieldDiff]:
    if not isinstance(before, dict) or not isinstance(after, dict):
        change = _diff_value(
            path=field_name,
            scope="top_level",
            before=before,
            after=after,
        )
        return [] if change is None else [change]

    changes: list[SourceBriefFieldDiff] = []
    for key in sorted(set(before) | set(after), key=str):
        change = _diff_value(
            path=f"{field_name}.{key}",
            scope=field_name,  # type: ignore[arg-type]
            before=before.get(key, _MISSING),
            after=after.get(key, _MISSING),
        )
        if change is not None:
            changes.append(change)
    return changes


def _diff_value(
    *,
    path: str,
    scope: SourceBriefDiffScope,
    before: Any,
    after: Any,
) -> SourceBriefFieldDiff | None:
    if before is _MISSING and after is _MISSING:
        return None
    if before is _MISSING:
        return SourceBriefFieldDiff(
            path=path,
            status="added",
            before=None,
            after=after,
            scope=scope,
        )
    if after is _MISSING:
        return SourceBriefFieldDiff(
            path=path,
            status="removed",
            before=before,
            after=None,
            scope=scope,
        )
    if before != after:
        return SourceBriefFieldDiff(
            path=path,
            status="changed",
            before=before,
            after=after,
            scope=scope,
        )
    return None


__all__ = [
    "SourceBriefDiff",
    "SourceBriefDiffScope",
    "SourceBriefDiffStatus",
    "SourceBriefFieldDiff",
    "diff_source_briefs",
]
