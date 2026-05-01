"""Extract nonfunctional requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


NonfunctionalRequirementCategory = Literal[
    "performance",
    "reliability",
    "security",
    "privacy",
    "accessibility",
    "compatibility",
    "localization",
    "observability",
    "compliance",
    "scalability",
]

NonfunctionalRequirementConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[NonfunctionalRequirementCategory, ...] = (
    "performance",
    "reliability",
    "security",
    "privacy",
    "accessibility",
    "compatibility",
    "localization",
    "observability",
    "compliance",
    "scalability",
)
_CONFIDENCE_ORDER: dict[NonfunctionalRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_CATEGORY_PATTERNS: dict[NonfunctionalRequirementCategory, re.Pattern[str]] = {
    "performance": re.compile(
        r"\b(?:performance|latency|response time|p95|p99|throughput|load time|"
        r"page speed|render time|fast|cache|caching)\b",
        re.I,
    ),
    "reliability": re.compile(
        r"\b(?:reliability|available|availability|uptime|failover|fallback|retry|"
        r"resilien(?:t|ce)|backup|restore|disaster recovery|rpo|rto|data loss|idempotent)\b",
        re.I,
    ),
    "security": re.compile(
        r"\b(?:security|secure|auth|authentication|authorization|encrypt|encrypted|encryption|"
        r"sso|mfa|rbac|least privilege|permission|secret|vulnerability|csrf|xss)\b",
        re.I,
    ),
    "privacy": re.compile(
        r"\b(?:privacy|pii|personal data|personal information|consent|opt[- ]?in|"
        r"opt[- ]?out|data deletion|right to erasure|anonymi[sz]e|redact|retention)\b",
        re.I,
    ),
    "accessibility": re.compile(
        r"\b(?:accessibility|a11y|wcag|screen reader|keyboard navigation|aria|"
        r"contrast|captions?|section 508)\b",
        re.I,
    ),
    "compatibility": re.compile(
        r"\b(?:compatibility|compatible|browser|safari|chrome|firefox|edge|mobile|"
        r"ios|android|backward compatible|api version|legacy)\b",
        re.I,
    ),
    "localization": re.compile(
        r"\b(?:locali[sz]ation|i18n|locale|translation|translated|language|timezone|"
        r"time zone|currency|date format|rtl|right-to-left)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|logging|logs|metrics|monitoring|tracing|trace|alert|"
        r"dashboard|telemetry|instrumentation|audit log)\b",
        re.I,
    ),
    "compliance": re.compile(
        r"\b(?:compliance|gdpr|ccpa|cpra|hipaa|soc\s*2|soc ii|pci(?:[- ]?dss)?|"
        r"sox|finra|regulatory|regulation|audit evidence|legal)\b",
        re.I,
    ),
    "scalability": re.compile(
        r"\b(?:scalability|scalable|scale|concurrent users?|traffic|volume|"
        r"horizontal scaling|autoscal(?:e|ing)|multi[- ]?tenant|shard)\b",
        re.I,
    ),
}
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_EXPLICIT_RE = re.compile(
    r"\b(?:must|shall|requires?|required|requirement|needs? to|should|ensure|support|"
    r"guarantee|meet|add|done when|no more than|at least|within|under)\b",
    re.I,
)
_MEASURABLE_RE = re.compile(
    r"(?:\b\d+(?:\.\d+)?\s*(?:ms|milliseconds?|s|seconds?|minutes?|hours?|%|percent|"
    r"users?|requests?|rps|qps|kb|mb|gb|tb)\b|[<>]=?\s*\d+|99\.\d+)",
    re.I,
)
_WEAK_CONTEXT_RE = re.compile(r"\b(?:consider|prefer|nice to have|explore|investigate|may)\b", re.I)
_SPACE_RE = re.compile(r"\s+")

_TOP_LEVEL_FIELDS = (
    "title",
    "summary",
    "problem",
    "problem_statement",
    "goals",
    "constraints",
    "acceptance_criteria",
)
_SOURCE_PAYLOAD_FIELDS = (
    "problem",
    "problem_statement",
    "goals",
    "constraints",
    "acceptance_criteria",
    "acceptance",
    "requirements",
    "nonfunctional_requirements",
    "non_functional_requirements",
    "body",
    "description",
    "markdown",
)


@dataclass(frozen=True, slots=True)
class SourceNonfunctionalRequirement:
    """One source-backed nonfunctional requirement candidate."""

    category: NonfunctionalRequirementCategory
    requirement_text: str
    source_field: str
    confidence: NonfunctionalRequirementConfidence
    evidence: str
    suggested_follow_up: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "requirement_text": self.requirement_text,
            "source_field": self.source_field,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "suggested_follow_up": self.suggested_follow_up,
        }


def extract_source_nonfunctional_requirements(
    source_brief: Mapping[str, Any] | SourceBrief,
) -> tuple[SourceNonfunctionalRequirement, ...]:
    """Return nonfunctional requirements from one SourceBrief-shaped record."""
    brief = _source_brief_payload(source_brief)
    if not brief:
        return ()

    records: list[SourceNonfunctionalRequirement] = []
    seen_texts: set[str] = set()
    for source_field, value in _candidate_values(brief):
        for segment in _segments(value):
            categories = _categories(segment)
            if not categories:
                continue
            requirement_text = _clean_text(segment)
            if not _is_requirement_like(requirement_text):
                continue
            dedupe_key = _dedupe_key(requirement_text)
            if dedupe_key in seen_texts:
                continue
            seen_texts.add(dedupe_key)
            category = _primary_category(segment, categories)
            records.append(
                SourceNonfunctionalRequirement(
                    category=category,
                    requirement_text=requirement_text,
                    source_field=source_field,
                    confidence=_confidence(requirement_text, source_field),
                    evidence=_snippet(requirement_text),
                    suggested_follow_up=_follow_up(category, requirement_text),
                )
            )

    return tuple(
        sorted(
            records,
            key=lambda record: (
                _CATEGORY_ORDER.index(record.category),
                _CONFIDENCE_ORDER[record.confidence],
                record.source_field,
                record.requirement_text.casefold(),
            ),
        )
    )


def source_nonfunctional_requirements_to_dicts(
    records: tuple[SourceNonfunctionalRequirement, ...] | list[SourceNonfunctionalRequirement],
) -> list[dict[str, Any]]:
    """Serialize nonfunctional requirement records to dictionaries."""
    return [record.to_dict() for record in records]


def summarize_source_nonfunctional_requirements(
    records_or_source: (
        Mapping[str, Any]
        | SourceBrief
        | tuple[SourceNonfunctionalRequirement, ...]
        | list[SourceNonfunctionalRequirement]
    ),
) -> dict[str, Any]:
    """Return deterministic counts and follow-up guidance for extracted requirements."""
    if _looks_like_records(records_or_source):
        records = tuple(records_or_source)  # type: ignore[arg-type]
    else:
        records = extract_source_nonfunctional_requirements(
            records_or_source  # type: ignore[arg-type]
        )
    category_counts = {
        category: sum(1 for record in records if record.category == category)
        for category in _CATEGORY_ORDER
        if any(record.category == category for record in records)
    }
    confidence_counts = {
        confidence: sum(1 for record in records if record.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    follow_ups = _dedupe(record.suggested_follow_up for record in records)
    return {
        "requirement_count": len(records),
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "follow_ups": list(follow_ups),
    }


def _candidate_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in _TOP_LEVEL_FIELDS:
        if field_name in brief:
            _append_value(candidates, field_name, brief[field_name])

    payload = brief.get("source_payload")
    if isinstance(payload, Mapping):
        visited: set[str] = set()
        for field_name in _SOURCE_PAYLOAD_FIELDS:
            if field_name in payload:
                source_field = f"source_payload.{field_name}"
                _append_value(candidates, source_field, payload[field_name])
                visited.add(source_field)
        for source_field, value in _flatten_payload(payload, prefix="source_payload"):
            if _is_under_visited_field(source_field, visited):
                continue
            _append_value(candidates, source_field, value)
    return candidates


def _append_value(candidates: list[tuple[str, Any]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for child_field, child_value in _flatten_payload(value, prefix=source_field):
            _append_value(candidates, child_field, child_value)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_value(candidates, f"{source_field}[{index}]", item)
        return
    if isinstance(value, str) and value.strip():
        candidates.append((source_field, value))


def _flatten_payload(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return
        flattened.append((path, current))

    append(value, prefix)
    return flattened


def _segments(value: str) -> tuple[str, ...]:
    segments: list[str] = []
    for line in value.splitlines():
        line_text = _clean_text(line)
        if not line_text:
            continue
        if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line):
            segments.append(line_text)
            continue
        for part in _SENTENCE_SPLIT_RE.split(line):
            text = _clean_text(part)
            if text:
                segments.append(text)
    return tuple(segments)


def _categories(text: str) -> tuple[NonfunctionalRequirementCategory, ...]:
    return tuple(
        category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(text)
    )


def _primary_category(
    text: str,
    categories: tuple[NonfunctionalRequirementCategory, ...],
) -> NonfunctionalRequirementCategory:
    lowered = text.casefold()
    if "compliance" in categories and re.search(
        r"\b(?:gdpr|ccpa|cpra|hipaa|soc\s*2|soc ii|pci|sox|finra|compliance|regulatory)\b",
        lowered,
        re.I,
    ):
        return "compliance"
    if "observability" in categories and re.search(
        r"\b(?:observability|logging|logs|metrics|monitoring|tracing|alert|dashboard|telemetry|audit log)\b",
        lowered,
        re.I,
    ):
        return "observability"
    return categories[0]


def _is_requirement_like(text: str) -> bool:
    words = re.findall(r"[A-Za-z0-9]+", text)
    if len(words) < 4:
        return False
    return bool(_EXPLICIT_RE.search(text) or _MEASURABLE_RE.search(text))


def _confidence(text: str, source_field: str) -> NonfunctionalRequirementConfidence:
    explicit = bool(_EXPLICIT_RE.search(text))
    measurable = bool(_MEASURABLE_RE.search(text))
    strong_field = any(
        token in source_field
        for token in ("constraints", "acceptance_criteria", "requirements", "nonfunctional")
    )
    if measurable and (explicit or strong_field):
        return "high"
    if explicit and strong_field and not _WEAK_CONTEXT_RE.search(text):
        return "high"
    if explicit or measurable:
        return "medium"
    return "low"


def _follow_up(category: NonfunctionalRequirementCategory, requirement_text: str) -> str:
    guidance = {
        "performance": "Define measurable latency, throughput, and load-test acceptance criteria.",
        "reliability": "Confirm uptime targets, failure modes, retry behavior, and recovery objectives.",
        "security": "Translate the security requirement into controls, threat checks, and verification tasks.",
        "privacy": "Clarify data classes, retention/deletion rules, consent handling, and review owners.",
        "accessibility": "Specify the accessibility standard and add keyboard, screen reader, and contrast checks.",
        "compatibility": "List supported platforms, browsers, versions, and compatibility test coverage.",
        "localization": "Identify locales, translation workflow, timezone, currency, and format requirements.",
        "observability": "Define required logs, metrics, traces, dashboards, and alert thresholds.",
        "compliance": "Confirm applicable framework obligations, evidence, approvers, and audit-ready controls.",
        "scalability": "Set expected traffic, concurrency, data volume, and scale-test scenarios.",
    }[category]
    if "?" in requirement_text:
        return f"{guidance} Resolve the open question before planning dependent work."
    return guidance


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(source_brief, Mapping):
            return dict(source_brief)
    return {}


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", value.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _dedupe_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(text).casefold()).strip()


def _snippet(text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= 180:
        return cleaned
    return f"{cleaned[:177].rstrip()}..."


def _dedupe(values: Any) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(str(value))
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return tuple(result)


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


def _looks_like_records(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(item, SourceNonfunctionalRequirement) for item in value)


__all__ = [
    "NonfunctionalRequirementCategory",
    "NonfunctionalRequirementConfidence",
    "SourceNonfunctionalRequirement",
    "extract_source_nonfunctional_requirements",
    "source_nonfunctional_requirements_to_dicts",
    "summarize_source_nonfunctional_requirements",
]
