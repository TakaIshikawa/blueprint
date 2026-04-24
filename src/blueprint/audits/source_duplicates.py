"""Duplicate group reporting for source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from blueprint.audits.source_similarity import normalize_text_tokens


DEFAULT_THRESHOLD = 0.75
DEFAULT_LIMIT = 20
DUPLICATE_FIELDS = ("title", "summary", "source_links", "source_identity")
FIELD_WEIGHTS = {
    "title": 0.35,
    "summary": 0.35,
    "source_links": 0.20,
    "source_identity": 0.10,
}


@dataclass(frozen=True)
class SourceDuplicateBrief:
    """A source brief included in a duplicate group."""

    id: str
    title: str
    source_project: str
    source_entity_type: str
    source_id: str
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the brief summary as a stable JSON-compatible object."""
        return {
            "id": self.id,
            "title": self.title,
            "source_project": self.source_project,
            "source_entity_type": self.source_entity_type,
            "source_id": self.source_id,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class SourceDuplicatePair:
    """Pairwise duplicate evidence inside a duplicate group."""

    left_id: str
    right_id: str
    score: float
    matched_fields: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the pair as a stable JSON-compatible object."""
        return {
            "left_id": self.left_id,
            "right_id": self.right_id,
            "score": self.score,
            "matched_fields": self.matched_fields,
        }


@dataclass(frozen=True)
class SourceDuplicateGroup:
    """Likely duplicate source briefs with a deterministic canonical suggestion."""

    canonical_id: str
    score: float
    briefs: list[SourceDuplicateBrief] = field(default_factory=list)
    pairs: list[SourceDuplicatePair] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the duplicate group as a stable JSON-compatible object."""
        return {
            "canonical_id": self.canonical_id,
            "score": self.score,
            "briefs": [brief.to_dict() for brief in self.briefs],
            "pairs": [pair.to_dict() for pair in self.pairs],
        }


@dataclass(frozen=True)
class SourceDuplicateReport:
    """Duplicate report for a set of source briefs."""

    threshold: float
    limit: int
    source_project: str | None
    candidate_count: int
    groups: list[SourceDuplicateGroup] = field(default_factory=list)

    @property
    def duplicate_count(self) -> int:
        return sum(len(group.briefs) for group in self.groups)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the report as a stable JSON-compatible object."""
        return {
            "threshold": self.threshold,
            "limit": self.limit,
            "source_project": self.source_project,
            "candidate_count": self.candidate_count,
            "summary": {
                "groups": len(self.groups),
                "duplicates": self.duplicate_count,
            },
            "groups": [group.to_dict() for group in self.groups],
        }


def find_duplicate_source_brief_groups(
    source_briefs: list[dict[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    source_project: str | None = None,
) -> SourceDuplicateReport:
    """Group likely duplicate source briefs using normalized source similarity signals."""
    if limit <= 0:
        return SourceDuplicateReport(
            threshold=threshold,
            limit=limit,
            source_project=source_project,
            candidate_count=len(source_briefs),
            groups=[],
        )

    briefs = sorted(source_briefs, key=_brief_sort_key)
    token_index = {str(brief.get("id") or ""): _brief_tokens(brief) for brief in briefs}
    pair_index: dict[frozenset[str], SourceDuplicatePair] = {}
    adjacency: dict[str, set[str]] = {str(brief.get("id") or ""): set() for brief in briefs}

    for left_index, left in enumerate(briefs):
        left_id = str(left.get("id") or "")
        if not left_id:
            continue
        for right in briefs[left_index + 1 :]:
            right_id = str(right.get("id") or "")
            if not right_id:
                continue

            score, matched_fields = _duplicate_score(token_index[left_id], token_index[right_id])
            if score < threshold:
                continue

            pair = SourceDuplicatePair(
                left_id=min(left_id, right_id),
                right_id=max(left_id, right_id),
                score=score,
                matched_fields=matched_fields,
            )
            pair_index[frozenset((left_id, right_id))] = pair
            adjacency[left_id].add(right_id)
            adjacency[right_id].add(left_id)

    groups = _build_groups(briefs, adjacency, pair_index)
    groups.sort(key=lambda group: (-group.score, group.canonical_id))

    return SourceDuplicateReport(
        threshold=threshold,
        limit=limit,
        source_project=source_project,
        candidate_count=len(source_briefs),
        groups=groups[:limit],
    )


def _build_groups(
    briefs: list[dict[str, Any]],
    adjacency: dict[str, set[str]],
    pair_index: dict[frozenset[str], SourceDuplicatePair],
) -> list[SourceDuplicateGroup]:
    brief_by_id = {str(brief.get("id") or ""): brief for brief in briefs}
    visited: set[str] = set()
    groups: list[SourceDuplicateGroup] = []

    for brief in briefs:
        brief_id = str(brief.get("id") or "")
        if not brief_id or brief_id in visited or not adjacency.get(brief_id):
            continue

        component: set[str] = set()
        stack = [brief_id]
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(sorted(adjacency.get(current, set()) - component, reverse=True))

        visited.update(component)
        component_briefs = sorted(
            (brief_by_id[component_id] for component_id in component),
            key=_brief_sort_key,
        )
        component_pairs = sorted(
            (
                pair
                for pair_key, pair in pair_index.items()
                if pair_key.issubset(component)
            ),
            key=lambda pair: (-pair.score, pair.left_id, pair.right_id),
        )
        if not component_pairs:
            continue

        groups.append(
            SourceDuplicateGroup(
                canonical_id=str(component_briefs[0].get("id") or ""),
                score=max(pair.score for pair in component_pairs),
                briefs=[_brief_summary(brief) for brief in component_briefs],
                pairs=component_pairs,
            )
        )

    return groups


def _duplicate_score(
    left_tokens: dict[str, set[str]],
    right_tokens: dict[str, set[str]],
) -> tuple[float, list[str]]:
    weighted_score = 0.0
    matched_fields: list[str] = []

    for field in DUPLICATE_FIELDS:
        field_score = _jaccard_similarity(left_tokens[field], right_tokens[field])
        weighted_score += FIELD_WEIGHTS[field] * field_score
        if field_score > 0:
            matched_fields.append(field)

    return round(weighted_score, 4), matched_fields


def _brief_tokens(brief: dict[str, Any]) -> dict[str, set[str]]:
    return {
        "title": normalize_text_tokens(brief.get("title")),
        "summary": normalize_text_tokens(brief.get("summary")),
        "source_links": _source_link_tokens(brief.get("source_links")),
        "source_identity": normalize_text_tokens(
            " ".join(
                [
                    str(brief.get("source_project") or ""),
                    str(brief.get("source_entity_type") or ""),
                    str(brief.get("source_id") or ""),
                ]
            )
        ),
    }


def _source_link_tokens(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, dict):
        normalized = json.dumps(value, sort_keys=True, separators=(",", ":"))
    else:
        normalized = str(value)
    return normalize_text_tokens(normalized)


def _brief_summary(brief: dict[str, Any]) -> SourceDuplicateBrief:
    created_at = brief.get("created_at")
    return SourceDuplicateBrief(
        id=str(brief.get("id") or ""),
        title=str(brief.get("title") or ""),
        source_project=str(brief.get("source_project") or ""),
        source_entity_type=str(brief.get("source_entity_type") or ""),
        source_id=str(brief.get("source_id") or ""),
        created_at=str(created_at) if created_at else None,
    )


def _brief_sort_key(brief: dict[str, Any]) -> tuple[str, str]:
    created_at = str(brief.get("created_at") or "")
    return created_at, str(brief.get("id") or "")


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)
