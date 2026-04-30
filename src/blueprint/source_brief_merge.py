"""Deterministic merge helpers for SourceBrief-shaped dictionaries."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from blueprint.domain import SourceBrief


CONFLICT_FIELDS = ("title", "domain", "source_project")
MERGED_PROVENANCE_KEY = "merged_source_provenance"
MERGED_CONFLICTS_KEY = "merged_source_conflicts"


def merge_source_briefs(source_briefs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Merge two or more related SourceBrief dictionaries into one SourceBrief.

    The first brief is treated as primary: its identity is preserved, while
    summaries, links, and source provenance from every brief are consolidated.
    """
    if len(source_briefs) < 2:
        raise ValueError("merge_source_briefs requires at least two source briefs")

    normalized = [
        SourceBrief.model_validate(source_brief).model_dump(mode="python")
        for source_brief in source_briefs
    ]
    primary = normalized[0]

    merged_payload = dict(primary["source_payload"])
    merged_payload[MERGED_PROVENANCE_KEY] = [
        _source_provenance(source_brief) for source_brief in normalized
    ]
    conflicts = _source_conflicts(normalized)
    if conflicts:
        merged_payload[MERGED_CONFLICTS_KEY] = conflicts

    merged = {
        **primary,
        "title": _primary_or_first_present(normalized, "title"),
        "domain": _primary_or_first_present(normalized, "domain"),
        "summary": _merged_summary(normalized),
        "source_project": _primary_or_first_present(normalized, "source_project"),
        "source_payload": merged_payload,
        "source_links": _merged_source_links(normalized),
    }

    return SourceBrief.model_validate(merged).model_dump(mode="python")


def _source_provenance(source_brief: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": source_brief["id"],
        "source_project": source_brief["source_project"],
        "source_entity_type": source_brief["source_entity_type"],
        "source_id": source_brief["source_id"],
        "title": source_brief["title"],
    }


def _source_conflicts(source_briefs: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    conflicts: dict[str, list[dict[str, Any]]] = {}
    for field_name in CONFLICT_FIELDS:
        seen_values = {
            _freeze_jsonish(source_brief.get(field_name)) for source_brief in source_briefs
        }
        if len(seen_values) <= 1:
            continue
        conflicts[field_name] = [
            {
                "id": source_brief["id"],
                "value": source_brief.get(field_name),
            }
            for source_brief in source_briefs
        ]
    return conflicts


def _primary_or_first_present(source_briefs: list[dict[str, Any]], field_name: str) -> Any:
    primary_value = source_briefs[0].get(field_name)
    if primary_value not in ("", None):
        return primary_value
    for source_brief in source_briefs[1:]:
        value = source_brief.get(field_name)
        if value not in ("", None):
            return value
    return primary_value


def _merged_summary(source_briefs: list[dict[str, Any]]) -> str:
    fragments: list[str] = []
    seen: set[str] = set()

    for source_brief in source_briefs:
        label = str(source_brief.get("id") or source_brief.get("source_id"))
        for fragment in _summary_fragments(str(source_brief.get("summary") or "")):
            normalized = _normalize_text(fragment)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            fragments.append(f"{label}: {fragment}")

    return "\n\n".join(fragments)


def _summary_fragments(summary: str) -> list[str]:
    blocks = [block.strip() for block in summary.replace("\r\n", "\n").split("\n\n")]
    return [block for block in blocks if block]


def _merged_source_links(source_briefs: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for source_brief in source_briefs:
        for key in sorted(source_brief["source_links"], key=str):
            value = source_brief["source_links"][key]
            if key not in merged:
                merged[key] = value
                continue
            merged[key] = _merge_link_value(merged[key], value)
    return merged


def _merge_link_value(existing: Any, incoming: Any) -> Any:
    values: list[Any] = []
    for value in _as_link_values(existing) + _as_link_values(incoming):
        if not any(_jsonish_equal(value, current) for current in values):
            values.append(value)
    if len(values) == 1:
        return values[0]
    return values


def _as_link_values(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return [value]


def _jsonish_equal(left: Any, right: Any) -> bool:
    return _freeze_jsonish(left) == _freeze_jsonish(right)


def _freeze_jsonish(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((str(key), _freeze_jsonish(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_freeze_jsonish(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_jsonish(item) for item in value)
    return value


def _normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())


__all__ = [
    "MERGED_CONFLICTS_KEY",
    "MERGED_PROVENANCE_KEY",
    "merge_source_briefs",
]
