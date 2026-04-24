"""Local duplicate discovery for source briefs."""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata
from typing import Any


SIMILARITY_FIELDS = ("title", "summary", "domain", "source_project", "source_id")
DEFAULT_THRESHOLD = 0.5
DEFAULT_LIMIT = 10


@dataclass(frozen=True)
class SourceBriefSimilarityMatch:
    """A likely duplicate or overlapping source brief."""

    id: str
    title: str
    source_project: str
    score: float
    matched_fields: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the match as a stable JSON-compatible object."""
        return {
            "id": self.id,
            "title": self.title,
            "source_project": self.source_project,
            "score": self.score,
            "matched_fields": self.matched_fields,
        }


def find_similar_source_briefs(
    source_brief: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
) -> list[SourceBriefSimilarityMatch]:
    """Find source briefs similar to source_brief using local token-set similarity."""
    if limit <= 0:
        return []

    source_id = str(source_brief.get("id") or "")
    source_tokens = _brief_field_tokens(source_brief)
    matches: list[SourceBriefSimilarityMatch] = []

    for candidate in candidates:
        candidate_id = str(candidate.get("id") or "")
        if candidate_id == source_id:
            continue

        score, matched_fields = _brief_similarity(source_tokens, _brief_field_tokens(candidate))
        if score < threshold:
            continue

        matches.append(
            SourceBriefSimilarityMatch(
                id=candidate_id,
                title=str(candidate.get("title") or ""),
                source_project=str(candidate.get("source_project") or ""),
                score=score,
                matched_fields=matched_fields,
            )
        )

    matches.sort(key=lambda match: (-match.score, match.id))
    return matches[:limit]


def normalize_text_tokens(value: Any) -> set[str]:
    """Normalize text to a deterministic set of lowercase alphanumeric tokens."""
    if value is None:
        return set()

    text = unicodedata.normalize("NFKD", str(value)).lower()
    ascii_text = text.encode("ascii", "ignore").decode("ascii")
    return set(re.findall(r"[a-z0-9]+", ascii_text))


def _brief_field_tokens(brief: dict[str, Any]) -> dict[str, set[str]]:
    return {
        field: normalize_text_tokens(brief.get(field))
        for field in SIMILARITY_FIELDS
    }


def _brief_similarity(
    source_tokens: dict[str, set[str]],
    candidate_tokens: dict[str, set[str]],
) -> tuple[float, list[str]]:
    scores: list[float] = []
    matched_fields: list[str] = []

    for field in SIMILARITY_FIELDS:
        source_field_tokens = source_tokens[field]
        candidate_field_tokens = candidate_tokens[field]
        if not source_field_tokens and not candidate_field_tokens:
            continue

        field_score = _jaccard_similarity(source_field_tokens, candidate_field_tokens)
        scores.append(field_score)
        if field_score > 0:
            matched_fields.append(field)

    if not scores:
        return 0.0, []

    return round(sum(scores) / len(scores), 4), matched_fields


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)
