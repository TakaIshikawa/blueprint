"""Deterministic priority scoring for normalized SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import re
from typing import Any, Mapping


_HIGH_PRIORITY_VALUES = {
    "blocker",
    "critical",
    "high",
    "highest",
    "immediate",
    "p0",
    "p1",
    "urgent",
}
_MEDIUM_PRIORITY_VALUES = {"medium", "normal", "p2"}
_LOW_PRIORITY_VALUES = {"backlog", "low", "lowest", "p3", "p4"}
_EXPLICIT_PRIORITY_FIELDS = (
    "priority",
    "severity",
    "urgency",
    "impact",
    "business_impact",
    "customer_impact",
)
_TAG_WEIGHTS = {
    "blocker": 35,
    "blocked": 25,
    "customer": 25,
    "customer-blocking": 40,
    "customer_blocking": 40,
    "escalated": 25,
    "escalation": 25,
    "p0": 35,
    "p1": 25,
    "production": 20,
    "urgent": 30,
}
_URGENCY_KEYWORD_WEIGHTS = {
    "blocked": 25,
    "blocker": 25,
    "blocking": 25,
    "critical": 25,
    "customer": 15,
    "escalated": 20,
    "escalation": 20,
    "immediately": 20,
    "outage": 30,
    "regression": 20,
    "urgent": 25,
}
_TAG_FIELDS = ("tags", "labels", "keywords")
_TEXT_FIELDS = ("title", "summary", "body", "description", "mvp_goal", "problem_statement")
_TIMESTAMP_FIELDS = (
    "updated_at",
    "created_at",
    "reported_at",
    "submitted_at",
    "requested_at",
)
_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)*")


@dataclass(frozen=True)
class SourcePriorityComponent:
    """One human-readable reason contributing to a source priority score."""

    code: str
    points: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "points": self.points,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class SourcePriorityScore:
    """Priority score for one SourceBrief-shaped dictionary."""

    source_brief_id: str
    title: str
    score: int
    components: list[SourcePriorityComponent] = field(default_factory=list)
    source_brief: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "score": self.score,
            "components": [component.to_dict() for component in self.components],
        }


def score_source_briefs(source_briefs: list[dict[str, Any]]) -> list[SourcePriorityScore]:
    """Score SourceBrief dictionaries and return them in deterministic priority order."""
    latest_timestamp = _latest_timestamp(source_briefs)
    scored = [
        _score_source_brief(source_brief, latest_timestamp=latest_timestamp, index=index)
        for index, source_brief in enumerate(source_briefs)
    ]
    return sorted(scored, key=_result_sort_key)


def _score_source_brief(
    source_brief: Mapping[str, Any],
    *,
    latest_timestamp: datetime | None,
    index: int,
) -> SourcePriorityScore:
    normalized = _normalized_payload(source_brief)
    components = [
        *_explicit_priority_components(normalized),
        *_tag_components(source_brief, normalized),
        *_source_link_components(source_brief),
        *_text_signal_components(source_brief, normalized),
        *_recency_components(source_brief, normalized, latest_timestamp),
    ]
    score = sum(component.points for component in components)
    source_brief_id = (
        _text(source_brief.get("id"))
        or _text(normalized.get("source_id"))
        or f"source-{index + 1}"
    )
    title = _text(normalized.get("title")) or _text(source_brief.get("title"))
    return SourcePriorityScore(
        source_brief_id=source_brief_id,
        title=title,
        score=score,
        components=components,
        source_brief=dict(source_brief),
    )


def _explicit_priority_components(normalized: Mapping[str, Any]) -> list[SourcePriorityComponent]:
    components: list[SourcePriorityComponent] = []
    seen: set[str] = set()
    for field_name in _EXPLICIT_PRIORITY_FIELDS:
        value = normalized.get(field_name)
        normalized_values = _normalized_values(value)
        if not normalized_values:
            continue
        points = _explicit_priority_points(normalized_values)
        if points == 0:
            continue
        rendered = _render_values(normalized_values)
        key = f"{field_name}:{rendered}:{points}"
        if key in seen:
            continue
        seen.add(key)
        components.append(
            SourcePriorityComponent(
                code=f"explicit_{field_name}",
                points=points,
                reason=f"Explicit {field_name.replace('_', ' ')} is {rendered}.",
            )
        )
    return components


def _explicit_priority_points(values: set[str]) -> int:
    if values & _HIGH_PRIORITY_VALUES:
        return 50
    if values & _MEDIUM_PRIORITY_VALUES:
        return 15
    if values & _LOW_PRIORITY_VALUES:
        return -10
    return 0


def _tag_components(
    source_brief: Mapping[str, Any], normalized: Mapping[str, Any]
) -> list[SourcePriorityComponent]:
    tags: set[str] = set()
    for payload in (normalized, _source_payload(source_brief), source_brief):
        for field_name in _TAG_FIELDS:
            tags.update(_normalized_values(payload.get(field_name)))
    matched = sorted(tag for tag in tags if tag in _TAG_WEIGHTS)
    if not matched:
        return []
    points = sum(_TAG_WEIGHTS[tag] for tag in matched)
    return [
        SourcePriorityComponent(
            code="tag_signals",
            points=points,
            reason=f"Priority-like tags are present: {_render_values(set(matched))}.",
        )
    ]


def _source_link_components(source_brief: Mapping[str, Any]) -> list[SourcePriorityComponent]:
    source_links = source_brief.get("source_links")
    if not isinstance(source_links, Mapping) or not source_links:
        return []

    components = [
        SourcePriorityComponent(
            code="source_links",
            points=5,
            reason="Source links are present for upstream traceability.",
        )
    ]
    link_text = " ".join(
        f"{key} {value}"
        for key, value in sorted(source_links.items(), key=lambda item: str(item[0]))
    )
    link_tokens = _tokens(link_text)
    customer_tokens = {"customer", "support", "ticket", "zendesk", "salesforce", "incident"}
    matched = link_tokens & customer_tokens
    if matched:
        components.append(
            SourcePriorityComponent(
                code="customer_source_link",
                points=15,
                reason=f"Source links include customer or incident context: {_render_values(matched)}.",
            )
        )
    return components


def _text_signal_components(
    source_brief: Mapping[str, Any], normalized: Mapping[str, Any]
) -> list[SourcePriorityComponent]:
    text = _combined_text(source_brief, normalized)
    matched = sorted(token for token in _tokens(text) if token in _URGENCY_KEYWORD_WEIGHTS)
    if not matched:
        return []
    points = min(60, sum(_URGENCY_KEYWORD_WEIGHTS[token] for token in matched))
    return [
        SourcePriorityComponent(
            code="urgency_keywords",
            points=points,
            reason=f"Title or summary contains urgency keywords: {_render_values(set(matched))}.",
        )
    ]


def _recency_components(
    source_brief: Mapping[str, Any],
    normalized: Mapping[str, Any],
    latest_timestamp: datetime | None,
) -> list[SourcePriorityComponent]:
    if latest_timestamp is None:
        return []
    timestamp = _brief_timestamp(source_brief, normalized)
    if timestamp is None:
        return []
    age_days = max(0, (latest_timestamp - timestamp).days)
    if age_days <= 1:
        points = 10
        label = "within 1 day of the newest source brief timestamp"
    elif age_days <= 7:
        points = 7
        label = "within 7 days of the newest source brief timestamp"
    elif age_days <= 30:
        points = 3
        label = "within 30 days of the newest source brief timestamp"
    else:
        return []
    return [
        SourcePriorityComponent(
            code="recency",
            points=points,
            reason=f"Timestamp is {label}.",
        )
    ]


def _latest_timestamp(source_briefs: list[dict[str, Any]]) -> datetime | None:
    timestamps = [
        timestamp
        for source_brief in source_briefs
        if (timestamp := _brief_timestamp(source_brief, _normalized_payload(source_brief))) is not None
    ]
    if not timestamps:
        return None
    return max(timestamps)


def _brief_timestamp(
    source_brief: Mapping[str, Any], normalized: Mapping[str, Any]
) -> datetime | None:
    for payload in (source_brief, normalized, _source_payload(source_brief)):
        for field_name in _TIMESTAMP_FIELDS:
            timestamp = _parse_timestamp(payload.get(field_name))
            if timestamp is not None:
                return timestamp
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _normalized_payload(source_brief: Mapping[str, Any]) -> dict[str, Any]:
    source_payload = _source_payload(source_brief)
    normalized = source_payload.get("normalized")
    if isinstance(normalized, Mapping):
        return dict(normalized)
    return dict(source_payload)


def _source_payload(source_brief: Mapping[str, Any]) -> dict[str, Any]:
    source_payload = source_brief.get("source_payload")
    if isinstance(source_payload, Mapping):
        return dict(source_payload)
    return {}


def _combined_text(source_brief: Mapping[str, Any], normalized: Mapping[str, Any]) -> str:
    fragments: list[str] = []
    for payload in (normalized, source_brief):
        for field_name in _TEXT_FIELDS:
            value = payload.get(field_name)
            if isinstance(value, str):
                fragments.append(value)
            elif isinstance(value, list):
                fragments.extend(str(item) for item in value if item is not None)
    return " ".join(fragments)


def _normalized_values(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, Mapping):
        return _tokens(" ".join(f"{key} {item}" for key, item in value.items()))
    if isinstance(value, (list, tuple, set, frozenset)):
        return _tokens(" ".join(str(item) for item in value if item is not None))
    return _tokens(str(value))


def _tokens(value: str) -> set[str]:
    return {match.group(0) for match in _TOKEN_RE.finditer(value.casefold())}


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _render_values(values: set[str]) -> str:
    return ", ".join(sorted(values))


def _result_sort_key(result: SourcePriorityScore) -> tuple[Any, ...]:
    source_brief = result.source_brief
    return (
        -result.score,
        result.source_brief_id,
        _text(source_brief.get("source_project")),
        _text(source_brief.get("source_entity_type")),
        _text(source_brief.get("source_id")),
        result.title,
    )


__all__ = [
    "SourcePriorityComponent",
    "SourcePriorityScore",
    "score_source_briefs",
]
