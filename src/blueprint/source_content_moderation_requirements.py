"""Extract source-level content moderation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ContentModerationRequirementCategory = Literal[
    "user_generated_content",
    "abuse_reporting",
    "automated_detection",
    "human_review_queue",
    "policy_taxonomy",
    "appeal_flow",
    "audit_history",
    "safety_escalation",
]
ContentModerationRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ContentModerationRequirementCategory, ...] = (
    "user_generated_content",
    "abuse_reporting",
    "automated_detection",
    "human_review_queue",
    "policy_taxonomy",
    "appeal_flow",
    "audit_history",
    "safety_escalation",
)
_CONFIDENCE_ORDER: dict[ContentModerationRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|workflow|process|policy|support|"
    r"can|includes?)\b",
    re.I,
)
_MODERATION_CONTEXT_RE = re.compile(
    r"\b(?:content moderation|moderation|moderator|trust and safety|abuse|report(?:ing|ed)?|"
    r"flag(?:ged|ging)?|user generated content|ugc|user submitted|community content|"
    r"post(?:s|ed)?|comment(?:s)?|message(?:s)?|media uploads?|upload(?:s|ed)?|"
    r"review queue|human review|policy enforcement|policy taxonomy|appeal(?:s)?|"
    r"automated detection|classifier|toxicity|spam|harassment|hate speech|self harm|"
    r"safety escalation|law enforcement|audit history|decision history|enforcement history)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:moderation|abuse|report(?:ing)?|ugc|user[-_ ]?generated|user[-_ ]?submitted|"
    r"content|policy|appeal|review|queue|safety|trust|enforcement|audit|requirements?|"
    r"acceptance|controls?|compliance|operations|source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:content moderation|moderation|ugc|user generated content|"
    r"abuse reporting|appeals?|review queue|policy enforcement|safety escalation).*?"
    r"\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[ContentModerationRequirementCategory, re.Pattern[str]] = {
    "user_generated_content": re.compile(
        r"\b(?:user[- ]generated content|ugc|user submitted content|user submissions?|"
        r"community content|member content|customer uploads?|media uploads?|user uploads?|"
        r"submitted posts?|submitted comments?|submitted messages?|submitted reviews?)\b",
        re.I,
    ),
    "abuse_reporting": re.compile(
        r"\b(?:abuse reports?|report abuse|report(?:ed|ing)? content|flag(?:ged|ging)? "
        r"(?:content|post|comment|message|user|profile)|user reports?|report button|"
        r"report reason|reporting flow|in-app report)\b",
        re.I,
    ),
    "automated_detection": re.compile(
        r"\b(?:automated detection|auto[- ]?detect|classifier|ml moderation|machine learning "
        r"moderation|toxicity score|spam detection|keyword filter|profanity filter|image "
        r"moderation|nudity detection|hash match|pre[- ]?moderation|risk score)\b",
        re.I,
    ),
    "human_review_queue": re.compile(
        r"\b(?:human review queue|review queue|moderation queue|moderator queue|manual review|"
        r"content review|reviewer workflow|reviewers?|moderators?|triage queue|approve "
        r"content|reject content|remove content|ban users?|suspend users?)\b",
        re.I,
    ),
    "policy_taxonomy": re.compile(
        r"\b(?:policy taxonomy|policy categories|violation categories|moderation policy|"
        r"community guidelines|content policy|enforcement policy|"
        r"hate speech|harassment|spam|self[- ]harm|violent content|adult content|"
        r"illegal content|policy labels?)\b",
        re.I,
    ),
    "appeal_flow": re.compile(
        r"\b(?:appeal flow|appeals?|appeal decision|appeal requests?|dispute moderation|"
        r"contest removal|contest enforcement|reinstatement request|second review)\b",
        re.I,
    ),
    "audit_history": re.compile(
        r"\b(?:audit history|audit trail|decision history|enforcement history|moderation logs?|"
        r"review history|case history|record decisions?|log decisions?|who reviewed|"
        r"reviewer action log)\b",
        re.I,
    ),
    "safety_escalation": re.compile(
        r"\b(?:safety escalation|escalate to trust and safety|trust and safety escalation|"
        r"urgent escalation|high[- ]risk reports?|credible threats?|self[- ]harm|child safety|"
        r"law enforcement|legal escalation|crisis escalation)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[ContentModerationRequirementCategory, str] = {
    "user_generated_content": "product",
    "abuse_reporting": "trust_and_safety",
    "automated_detection": "trust_and_safety",
    "human_review_queue": "operations",
    "policy_taxonomy": "policy",
    "appeal_flow": "trust_and_safety",
    "audit_history": "compliance",
    "safety_escalation": "trust_and_safety",
}
_PLANNING_NOTE_BY_CATEGORY: dict[ContentModerationRequirementCategory, str] = {
    "user_generated_content": "Define submitted content types, lifecycle states, visibility, and moderation entry points.",
    "abuse_reporting": "Specify report reasons, reporter context, intake states, SLAs, and notification behavior.",
    "automated_detection": "Plan detection signals, thresholds, false-positive handling, and human handoff behavior.",
    "human_review_queue": "Design queue assignment, reviewer permissions, decision outcomes, and backlog controls.",
    "policy_taxonomy": "Align violation categories, labels, severities, and enforcement actions with policy owners.",
    "appeal_flow": "Define appeal eligibility, request windows, second-review ownership, and outcome notifications.",
    "audit_history": "Capture reviewer actions, policy basis, timestamps, actor identity, and retention expectations.",
    "safety_escalation": "Document urgent escalation triggers, responder roles, SLA targets, and external handoff rules.",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "security",
    "compliance",
    "moderation",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceContentModerationRequirement:
    """One source-backed content moderation requirement."""

    source_brief_id: str | None
    category: ContentModerationRequirementCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: ContentModerationRequirementConfidence = "medium"
    suggested_owner: str = "product"
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceContentModerationRequirementsReport:
    """Source-level content moderation requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceContentModerationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceContentModerationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return content moderation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Content Moderation Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source content moderation requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Confidence | Owner | Source Field Paths | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.suggested_owner)} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_content_moderation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceContentModerationRequirementsReport:
    """Extract content moderation requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _category_index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceContentModerationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_content_moderation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceContentModerationRequirementsReport:
    """Compatibility alias for building a content moderation requirements report."""
    return build_source_content_moderation_requirements(source)


def generate_source_content_moderation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceContentModerationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_content_moderation_requirements(source)


def derive_source_content_moderation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceContentModerationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_content_moderation_requirements(source)


def summarize_source_content_moderation_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceContentModerationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted content moderation requirements."""
    if isinstance(source_or_result, SourceContentModerationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_content_moderation_requirements(source_or_result).summary


def source_content_moderation_requirements_to_dict(
    report: SourceContentModerationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a content moderation requirements report to a plain dictionary."""
    return report.to_dict()


source_content_moderation_requirements_to_dict.__test__ = False


def source_content_moderation_requirements_to_dicts(
    requirements: (
        tuple[SourceContentModerationRequirement, ...]
        | list[SourceContentModerationRequirement]
        | SourceContentModerationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize content moderation requirement records to dictionaries."""
    if isinstance(requirements, SourceContentModerationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_content_moderation_requirements_to_dicts.__test__ = False


def source_content_moderation_requirements_to_markdown(
    report: SourceContentModerationRequirementsReport,
) -> str:
    """Render a content moderation requirements report as Markdown."""
    return report.to_markdown()


source_content_moderation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: ContentModerationRequirementCategory
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: ContentModerationRequirementConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceContentModerationRequirement]:
    grouped: dict[tuple[str | None, ContentModerationRequirementCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceContentModerationRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceContentModerationRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                confidence=confidence,
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _MODERATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _MODERATION_CONTEXT_RE.search(searchable):
        return False
    if segment.section_context:
        return True
    if segment.source_field == "title" and not _REQUIRED_RE.search(segment.text):
        return False
    return bool(_REQUIRED_RE.search(segment.text))


def _matched_terms(
    category: ContentModerationRequirementCategory,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _CATEGORY_PATTERNS[category].finditer(text)
        )
    )


def _confidence(segment: _Segment) -> ContentModerationRequirementConfidence:
    if _REQUIRED_RE.search(segment.text):
        return "high"
    if segment.section_context:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceContentModerationRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "owner_counts": {
            owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
            for owner in sorted(_dedupe(_OWNER_BY_CATEGORY.values()), key=str.casefold)
        },
        "categories": [requirement.category for requirement in requirements],
        "status": "ready_for_planning" if requirements else "no_moderation_language",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "requirements",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "security",
        "compliance",
        "moderation",
        "operations",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _category_index(category: ContentModerationRequirementCategory) -> int:
    return _CATEGORY_ORDER.index(category)


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "ContentModerationRequirementCategory",
    "ContentModerationRequirementConfidence",
    "SourceContentModerationRequirement",
    "SourceContentModerationRequirementsReport",
    "build_source_content_moderation_requirements",
    "derive_source_content_moderation_requirements",
    "extract_source_content_moderation_requirements",
    "generate_source_content_moderation_requirements",
    "source_content_moderation_requirements_to_dict",
    "source_content_moderation_requirements_to_dicts",
    "source_content_moderation_requirements_to_markdown",
    "summarize_source_content_moderation_requirements",
]
