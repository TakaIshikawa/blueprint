"""Derive structured impact tags from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class SourceImpactTag:
    """A deterministic impact signal extracted from brief text."""

    tag: str
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "tag": self.tag,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


def derive_source_impact_tags(
    brief: Mapping[str, Any] | Any,
) -> list[SourceImpactTag]:
    """Return impact tags for a SourceBrief, ImplementationBrief, or dict payload."""
    payload = _payload_dict(brief)
    evidence_by_tag: dict[str, list[str]] = {tag: [] for tag in _TAG_KEYWORDS}
    keywords_by_tag: dict[str, set[str]] = {tag: set() for tag in _TAG_KEYWORDS}

    for text in _brief_text_values(payload):
        for tag, keywords in _TAG_KEYWORDS.items():
            matched_keywords = _matched_keywords(text, keywords)
            if not matched_keywords:
                continue
            evidence_by_tag[tag].append(_evidence_snippet(text, matched_keywords[0]))
            keywords_by_tag[tag].update(matched_keywords)

    tags: list[SourceImpactTag] = []
    for tag, evidence in evidence_by_tag.items():
        deduped_evidence = _dedupe(evidence)
        if not deduped_evidence:
            continue
        confidence = _confidence(
            evidence_count=len(deduped_evidence),
            keyword_count=len(keywords_by_tag[tag]),
        )
        tags.append(
            SourceImpactTag(
                tag=tag,
                confidence=confidence,
                evidence=tuple(deduped_evidence[:_MAX_EVIDENCE_PER_TAG]),
            )
        )

    return sorted(tags, key=lambda item: (-item.confidence, item.tag))


def source_impact_tags_to_dicts(
    tags: list[SourceImpactTag] | tuple[SourceImpactTag, ...],
) -> list[dict[str, Any]]:
    """Serialize impact tags to dictionaries."""
    return [tag.to_dict() for tag in tags]


def _payload_dict(brief: Mapping[str, Any] | Any) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    if isinstance(brief, Mapping):
        return dict(brief)
    return {}


def _brief_text_values(payload: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for field_name in (
        "summary",
        "problem_statement",
        "scope",
        "integration_points",
        "risks",
    ):
        values.extend(_strings_from_value(payload.get(field_name)))
    values.extend(_source_payload_strings(payload.get("source_payload")))
    return [_normalize_text(value) for value in values if _normalize_text(value)]


def _strings_from_value(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple | set):
        strings: list[str] = []
        for item in value:
            strings.extend(_strings_from_value(item))
        return strings
    return []


def _source_payload_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_source_payload_strings(item))
        return strings
    if isinstance(value, list | tuple | set):
        strings: list[str] = []
        for item in value:
            strings.extend(_source_payload_strings(item))
        return strings
    return []


def _matched_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
    normalized = text.lower()
    matches = [
        keyword
        for keyword in keywords
        if re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", normalized)
    ]
    return matches


def _evidence_snippet(text: str, keyword: str) -> str:
    match = re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", text, re.I)
    if not match:
        return text[:_MAX_SNIPPET_LENGTH]
    start = max(0, match.start() - _SNIPPET_CONTEXT)
    end = min(len(text), match.end() + _SNIPPET_CONTEXT)
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."
    return snippet


def _confidence(*, evidence_count: int, keyword_count: int) -> float:
    score = 0.55 + (0.15 * min(evidence_count - 1, 2)) + (0.1 * min(keyword_count - 1, 2))
    return round(min(score, 0.95), 2)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "api": (
        "api",
        "apis",
        "endpoint",
        "endpoints",
        "graphql",
        "rest",
        "route",
        "routes",
        "schema",
        "schemas",
        "webhook",
        "webhooks",
    ),
    "auth": (
        "auth",
        "authentication",
        "authorization",
        "credential",
        "login",
        "permission",
        "permissions",
        "role",
        "roles",
        "secret",
        "secrets",
        "session",
        "sessions",
        "token",
        "tokens",
    ),
    "data": (
        "backfill",
        "cache",
        "data",
        "database",
        "databases",
        "dataset",
        "datasets",
        "etl",
        "model",
        "models",
        "postgres",
        "queries",
        "query",
        "sql",
        "storage",
    ),
    "docs": (
        "docs",
        "documentation",
        "guide",
        "markdown",
        "readme",
        "runbook",
        "runbooks",
    ),
    "infra": (
        "aws",
        "ci",
        "deploy",
        "docker",
        "environment",
        "environments",
        "infrastructure",
        "kubernetes",
        "pipeline",
        "pipelines",
        "terraform",
    ),
    "integration": (
        "client",
        "clients",
        "connector",
        "connectors",
        "external",
        "integration",
        "integrations",
        "oauth",
        "provider",
        "providers",
        "third-party",
        "webhook",
        "webhooks",
    ),
    "migration": (
        "alembic",
        "backfill",
        "migration",
        "migrations",
        "schema change",
    ),
    "observability": (
        "alert",
        "alerts",
        "dashboard",
        "dashboards",
        "log",
        "logging",
        "metric",
        "metrics",
        "monitoring",
        "observability",
        "trace",
        "tracing",
    ),
    "testing": (
        "coverage",
        "pytest",
        "qa",
        "regression",
        "test",
        "tests",
        "testing",
        "validation",
    ),
    "ui": (
        "accessibility",
        "component",
        "components",
        "css",
        "dashboard",
        "dashboards",
        "frontend",
        "screen",
        "screens",
        "ui",
        "ux",
        "view",
        "views",
    ),
}
_MAX_EVIDENCE_PER_TAG = 3
_MAX_SNIPPET_LENGTH = 120
_SNIPPET_CONTEXT = 48


__all__ = [
    "SourceImpactTag",
    "derive_source_impact_tags",
    "source_impact_tags_to_dicts",
]
