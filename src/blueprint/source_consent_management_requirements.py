"""Extract source-level consent management requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ConsentManagementCategory = Literal[
    "consent_capture",
    "purpose_specific_consent",
    "withdrawal",
    "consent_history",
    "cookie_banner_consent",
    "marketing_opt_in",
    "privacy_preference_center",
    "proof_of_consent_audit",
]
ConsentManagementConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[ConsentManagementCategory, ...] = (
    "consent_capture",
    "purpose_specific_consent",
    "withdrawal",
    "consent_history",
    "cookie_banner_consent",
    "marketing_opt_in",
    "privacy_preference_center",
    "proof_of_consent_audit",
)
_CONFIDENCE_ORDER: dict[ConsentManagementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_CAPABILITY_BY_CATEGORY: dict[ConsentManagementCategory, str] = {
    "consent_capture": "Capture explicit user consent with a clear affirmative action and durable consent state.",
    "purpose_specific_consent": "Model consent by processing purpose so users can grant or deny each purpose separately.",
    "withdrawal": "Allow users to withdraw or revoke consent and propagate the updated consent state.",
    "consent_history": "Store consent and withdrawal history with timestamps, actor, channel, policy, and purpose context.",
    "cookie_banner_consent": "Present cookie or tracking consent controls before non-essential cookies, tags, or scripts run.",
    "marketing_opt_in": "Capture opt-in consent for marketing messages, subscriptions, and promotional communications.",
    "privacy_preference_center": "Provide a preference center or settings surface for consent and privacy choices.",
    "proof_of_consent_audit": "Produce proof-of-consent evidence, receipts, exports, or audit records for compliance review.",
}
_CATEGORY_PATTERNS: dict[ConsentManagementCategory, re.Pattern[str]] = {
    "consent_capture": re.compile(
        r"\b(?:capture|collect|obtain|request|ask for|record|gather|present|show)\b.{0,70}"
        r"\b(?:explicit\s+|affirmative\s+|user\s+)?(?:consent|permission|authorization|opt[- ]?in)\b|"
        r"\b(?:consent|permission|authorization|opt[- ]?in)\b.{0,70}"
        r"\b(?:captured|collected|obtained|requested|checkbox|toggle|form|modal|dialog|screen|prompt|affirmative action|unchecked)\b",
        re.I,
    ),
    "purpose_specific_consent": re.compile(
        r"\b(?:purpose[- ]specific|granular|per[- ]purpose|by purpose|separate purposes?|processing purpose|"
        r"consent purpose|purpose of consent)\b.{0,90}\b(?:consent|permission|opt[- ]?in|authorization)\b|"
        r"\b(?:consent|permission|opt[- ]?in|authorization)\b.{0,90}\b(?:by purpose|per purpose|"
        r"purpose[- ]specific|granular|processing purpose)\b|"
        r"\b(?:consent|permission|opt[- ]?in|authorization)\b.{0,40}\b(?:for|to)\s+"
        r"(?:analytics|marketing|tracking|profiling|personalization|data sharing|processing|communications?)\b",
        re.I,
    ),
    "withdrawal": re.compile(
        r"\b(?:withdraw consent|withdrawal|revoke consent|revocation|remove consent|delete consent|"
        r"change consent|opt[- ]?out|unsubscribe|stop processing|consent cancellation)\b",
        re.I,
    ),
    "consent_history": re.compile(
        r"\b(?:consent history|consent timeline|consent ledger|consent record history|consent version history|"
        r"history of consent|consent events?|withdrawal history|revocation history|consented_at|"
        r"withdrawn_at|revoked_at)\b",
        re.I,
    ),
    "cookie_banner_consent": re.compile(
        r"\b(?:cookie banner|cookie consent|consent banner|cookie notice|cookie popup|cmp|"
        r"accept all|reject all|manage choices|non[- ]essential cookies?|tracking cookies?|"
        r"third[- ]party tags?|tracking scripts?)\b",
        re.I,
    ),
    "marketing_opt_in": re.compile(
        r"\b(?:marketing opt[- ]?in|marketing consent|promotional consent|email opt[- ]?in|sms opt[- ]?in|"
        r"newsletter opt[- ]?in|subscribe consent|subscription consent|promotional emails?|"
        r"marketing emails?|marketing messages?|commercial messages?|communication preferences?)\b",
        re.I,
    ),
    "privacy_preference_center": re.compile(
        r"\b(?:privacy preference center|privacy preferences?|preference center|consent settings|"
        r"privacy settings|communication preferences|manage preferences|preference portal|consent dashboard)\b",
        re.I,
    ),
    "proof_of_consent_audit": re.compile(
        r"\b(?:proof of consent|consent proof|consent evidence|consent receipt|consent audit|"
        r"audit trail|audit log|audit evidence|export consent|consent export|attestation|"
        r"compliance evidence)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[ConsentManagementCategory, re.Pattern[str]] = {
    "consent_capture": re.compile(r"\b(?:capture|capture mechanism|consent ui|consent form)\b", re.I),
    "purpose_specific_consent": re.compile(r"\b(?:purpose|purposes|granular)\b", re.I),
    "withdrawal": re.compile(r"\b(?:withdraw|withdrawal|revoke|revocation|opt out|unsubscribe)\b", re.I),
    "consent_history": re.compile(r"\b(?:history|ledger|timeline|consent events?|policy version)\b", re.I),
    "cookie_banner_consent": re.compile(r"\b(?:cookie|banner|cmp|tracking tags?)\b", re.I),
    "marketing_opt_in": re.compile(r"\b(?:marketing|newsletter|promotional|email opt in|sms opt in)\b", re.I),
    "privacy_preference_center": re.compile(r"\b(?:preference center|preferences|settings|privacy center)\b", re.I),
    "proof_of_consent_audit": re.compile(r"\b(?:proof|audit|evidence|receipt|export|attestation)\b", re.I),
}
_CONSENT_CONTEXT_RE = re.compile(
    r"\b(?:consent management|consent|permission|authorization|opt[- ]?in|opt[- ]?out|"
    r"withdrawal|revocation|preference center|privacy preferences?|cookie banner|cookie consent|"
    r"marketing consent|marketing preference|proof of consent|consent receipt|consent history|cmp)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:consent|permission|authorization|opt[_ -]?in|opt[_ -]?out|withdraw|revocation|"
    r"preference|privacy|cookie|banner|cmp|marketing|newsletter|communication|history|"
    r"audit|evidence|receipt|proof|purpose|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|needed|required|requires?|requirement|ensure|support|"
    r"allow|honou?r|respect|capture|collect|obtain|request|record|store|retain|log|export|"
    r"show|display|present|block|defer|withdraw|revoke|unsubscribe|before|after|when|if|"
    r"acceptance|done when|cannot ship|compliance)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:consent management|consent|permission|opt[- ]?in|opt[- ]?out|withdrawal|"
    r"cookie banner|preference center|marketing preference)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|impact)\b|"
    r"\b(?:consent management|consent|permission|opt[- ]?in|opt[- ]?out|withdrawal|"
    r"cookie banner|preference center|marketing preference)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|no impact|non[- ]?goal)\b",
    re.I,
)
_GENERIC_PRIVACY_RE = re.compile(
    r"\b(?:privacy policy|privacy notice|privacy page|privacy statement|privacy copy|"
    r"data privacy|respect user privacy|privacy review)\b",
    re.I,
)
_PURPOSE_RE = re.compile(
    r"\b(?:analytics|marketing|tracking|profiling|personalization|data sharing|research|communications?|processing)\b",
    re.I,
)
_MECHANISM_RE = re.compile(
    r"\b(?:checkbox|toggle|form|modal|dialog|screen|prompt|banner|affirmative action|unchecked|not pre[- ]checked)\b",
    re.I,
)
_EVIDENCE_DETAIL_RE = re.compile(
    r"\b(?:timestamp|actor|user id|ip address|channel|source|policy version|purpose|receipt|export|audit|audit trail|audit log|history)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "scope",
    "non_goals",
    "assumptions",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "privacy",
    "compliance",
    "marketing",
    "communications",
    "cookies",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceConsentManagementRequirement:
    """One source-backed consent management requirement."""

    source_brief_id: str | None
    category: ConsentManagementCategory
    required_capability: str
    requirement_text: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ConsentManagementConfidence = "medium"
    source_field: str | None = None
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ConsentManagementCategory:
        """Compatibility alias for callers expecting requirement_category naming."""
        return self.category

    @property
    def requirement_type(self) -> ConsentManagementCategory:
        """Compatibility alias for callers expecting requirement_type naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "required_capability": self.required_capability,
            "requirement_text": self.requirement_text,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "source_field": self.source_field,
            "source_fields": list(self.source_fields),
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class SourceConsentManagementRequirementsReport:
    """Source-level consent management requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceConsentManagementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceConsentManagementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceConsentManagementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return consent management requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Consent Management Requirements Report"
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
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source consent management requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Required Capability | Requirement | Confidence | Source Field | Source Fields | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.required_capability)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.source_fields))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_consent_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentManagementRequirementsReport:
    """Extract source-level consent management requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceConsentManagementRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_consent_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentManagementRequirementsReport:
    """Compatibility alias for building a consent management requirements report."""
    return build_source_consent_management_requirements(source)


def generate_source_consent_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentManagementRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_consent_management_requirements(source)


def derive_source_consent_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentManagementRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_consent_management_requirements(source)


def summarize_source_consent_management_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceConsentManagementRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted consent management requirements."""
    if isinstance(source_or_result, SourceConsentManagementRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_consent_management_requirements(source_or_result).summary


def source_consent_management_requirements_to_dict(
    report: SourceConsentManagementRequirementsReport,
) -> dict[str, Any]:
    """Serialize a consent management requirements report to a plain dictionary."""
    return report.to_dict()


source_consent_management_requirements_to_dict.__test__ = False


def source_consent_management_requirements_to_dicts(
    requirements: (
        tuple[SourceConsentManagementRequirement, ...]
        | list[SourceConsentManagementRequirement]
        | SourceConsentManagementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize consent management requirement records to dictionaries."""
    if isinstance(requirements, SourceConsentManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_consent_management_requirements_to_dicts.__test__ = False


def source_consent_management_requirements_to_markdown(
    report: SourceConsentManagementRequirementsReport,
) -> str:
    """Render a consent management requirements report as Markdown."""
    return report.to_markdown()


source_consent_management_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: ConsentManagementCategory
    requirement_text: str
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: ConsentManagementConfidence


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
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
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


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        if _brief_out_of_scope(payload):
            continue
        for segment in _candidate_segments(payload):
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            if _NEGATED_RE.search(searchable) or _generic_privacy_only(searchable):
                continue
            categories = _categories(segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=tuple(_matched_terms(_CATEGORY_PATTERNS[category], searchable)),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceConsentManagementRequirement]:
    grouped: dict[tuple[str | None, ConsentManagementCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceConsentManagementRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceConsentManagementRequirement(
                source_brief_id=source_brief_id,
                category=category,
                required_capability=_CAPABILITY_BY_CATEGORY[category],
                requirement_text=best.requirement_text,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                source_field=best.source_field,
                source_fields=tuple(_dedupe(item.source_field for item in items)),
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(items, key=lambda candidate: candidate.source_field.casefold())
                        for term in item.matched_terms
                    )
                )[:8],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_consent_context(payload)
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], global_context)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], global_context)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _CONSENT_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(_CONSENT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            text = _clean_text(part)
            if text and not _NEGATED_RE.search(text):
                segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[ConsentManagementCategory, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _generic_privacy_only(searchable):
        return ()
    has_context = bool(
        _CONSENT_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
    )
    if not has_context:
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
    ):
        return ()

    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
        and _CATEGORY_PATTERNS[category].search(searchable)
    ]
    text_categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    categories = _dedupe([*field_categories, *text_categories])
    if "consent_capture" in categories and not _capture_is_explicit(searchable):
        categories.remove("consent_capture")
    return tuple(categories)


def _capture_is_explicit(text: str) -> bool:
    return bool(_MECHANISM_RE.search(text) or re.search(r"\b(?:explicit|affirmative|capture|collect|obtain|request|ask for)\b", text, re.I))


def _confidence(segment: _Segment, category: ConsentManagementCategory) -> ConsentManagementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_requirement = bool(_REQUIREMENT_RE.search(searchable))
    has_structured_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "privacy",
                "compliance",
                "marketing",
                "cookie",
                "source_payload",
            )
        )
    )
    has_detail = _has_detail(category, segment.text)
    if _CATEGORY_PATTERNS[category].search(searchable) and has_requirement and (has_structured_context or has_detail):
        return "high"
    if has_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _has_detail(category: ConsentManagementCategory, text: str) -> bool:
    if category in {"purpose_specific_consent", "marketing_opt_in"}:
        return bool(_PURPOSE_RE.search(text))
    if category in {"consent_capture", "cookie_banner_consent", "privacy_preference_center"}:
        return bool(_MECHANISM_RE.search(text) or re.search(r"\b(?:accept all|reject all|manage choices|settings)\b", text, re.I))
    if category in {"consent_history", "proof_of_consent_audit"}:
        return bool(_EVIDENCE_DETAIL_RE.search(text))
    if category == "withdrawal":
        return bool(re.search(r"\b(?:preference center|settings|unsubscribe|opt[- ]?out|revoke|withdraw)\b", text, re.I))
    return False


def _summary(requirements: tuple[SourceConsentManagementRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_consent_management_planning" if requirements else "no_consent_management_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_RE.search(scoped_text))


def _brief_consent_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_CONSENT_CONTEXT_RE.search(scoped_text) and not _NEGATED_RE.search(scoped_text))


def _generic_privacy_only(text: str) -> bool:
    return bool(_GENERIC_PRIVACY_RE.search(text) and not _CONSENT_CONTEXT_RE.search(text))


def _requirement_text(text: str) -> str:
    return _clean_text(text)[:300]


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_clean_text(text)[:240]}"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, str]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        len(candidate.matched_terms),
        int("acceptance_criteria" in candidate.source_field or "definition_of_done" in candidate.source_field),
        len(candidate.requirement_text),
        candidate.source_field,
    )


def _dedupe_text_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _dedupe_evidence(items: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        _, _, statement = item.partition(": ")
        key = _dedupe_text_key(statement or item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


def _field_words(source_field: str) -> str:
    return _clean_text(re.sub(r"[\[\]._-]+", " ", source_field))


def _object_payload(source: object) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name in dir(source):
        if name.startswith("_"):
            continue
        try:
            value = getattr(source, name)
        except Exception:
            continue
        if callable(value):
            continue
        payload[name] = value
    return payload


def _strings(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return [text for key in sorted(value, key=lambda item: str(item)) for text in _strings(value[key])]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    if text := _optional_text(value):
        return [text]
    return []


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    if isinstance(value, (str, int, float, bool)):
        text = _clean_text(str(value))
        return text or None
    return None


def _clean_text(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip()).strip(" -:\t")


def _dedupe(items: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    deduped: list[Any] = []
    for item in items:
        if item is None or item == "" or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
