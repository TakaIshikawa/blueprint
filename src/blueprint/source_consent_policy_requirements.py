"""Extract consent policy requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ConsentPolicyRequirementCategory = Literal[
    "consent_purpose",
    "capture_mechanism",
    "withdrawal",
    "audit_evidence",
    "retention",
    "regional_legal_basis",
    "minor_consent",
    "third_party_sharing",
]
ConsentPolicyConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[ConsentPolicyRequirementCategory, ...] = (
    "consent_purpose",
    "capture_mechanism",
    "withdrawal",
    "audit_evidence",
    "retention",
    "regional_legal_basis",
    "minor_consent",
    "third_party_sharing",
)
_CONFIDENCE_ORDER: dict[ConsentPolicyConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[ConsentPolicyRequirementCategory, str] = {
    "consent_purpose": "Carry the stated consent purpose into generated tasks and avoid collecting or processing beyond that scope.",
    "capture_mechanism": "Generate tasks that define the user-facing consent capture mechanism and default state.",
    "withdrawal": "Generate tasks for withdrawal, opt-out, revocation, and downstream behavior when consent changes.",
    "audit_evidence": "Generate tasks that record durable consent evidence such as actor, timestamp, policy version, and source.",
    "retention": "Generate tasks that align consent records and withdrawal history with retention and deletion policy.",
    "regional_legal_basis": "Generate tasks that gate consent behavior by jurisdiction, lawful basis, and applicable regulation.",
    "minor_consent": "Generate tasks for age gates, parental or guardian consent, and child privacy handling.",
    "third_party_sharing": "Generate tasks that propagate consent limits to vendors, processors, and third-party sharing flows.",
}
_CATEGORY_PATTERNS: dict[ConsentPolicyRequirementCategory, re.Pattern[str]] = {
    "consent_purpose": re.compile(
        r"\b(?:consent purpose|purpose of consent|purpose[- ]specific consent|"
        r"consent for|permission for|authorization for|use consent to|"
        r"consent.{0,50}(?:analytics|marketing|tracking|profiling|personalization|processing|communication))\b",
        re.I,
    ),
    "capture_mechanism": re.compile(
        r"\b(?:capture|collect|request|ask for|obtain|record|present|show)\s+"
        r"(?:explicit\s+|affirmative\s+|user\s+)?(?:consent|permission|opt[- ]?in)|"
        r"\b(?:consent|permission|opt[- ]?in)\b.{0,40}\b(?:captured|collected|requested|obtained)\s+"
        r"(?:by|through|via|with)\s+(?:a\s+|an\s+|the\s+)?(?:checkbox|banner|modal|dialog|form|screen|prompt|toggle)|"
        r"\b(?:consent|permission|opt[- ]?in)\s+(?:checkbox|banner|modal|dialog|form|screen|prompt|toggle|ui|copy)|"
        r"\b(?:unchecked|not pre[- ]checked|affirmative action|granular consent|clear consent)\b",
        re.I,
    ),
    "withdrawal": re.compile(
        r"\b(?:withdraw(?:al)?|revoke|revocation|remove consent|delete consent|opt[- ]?out|"
        r"unsubscribe|stop processing|preference center|consent cancellation|change consent)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:consent\s+(?:audit|audit trail|audit log|history|event|evidence|receipt|ledger|record)|"
        r"(?:audit trail|audit log|audit event|history|evidence|receipt|timestamp|policy version|"
        r"consented_at|withdrawn_at|revoked_at).{0,50}\bconsent)\b",
        re.I,
    ),
    "retention": re.compile(
        r"\b(?:consent.{0,60}(?:retain|retained|retention|keep|kept|store|stored|delete|deleted|deletion|purge|archive|erase|erasure)|"
        r"(?:retain|retained|retention|keep|kept|store|stored|delete|deleted|deletion|purge|archive|erase|erasure).{0,60}consent)\b",
        re.I,
    ),
    "regional_legal_basis": re.compile(
        r"\b(?:gdpr|ccpa|cpra|lgpd|eprivacy|pecr|lawful basis|legal basis|"
        r"regional basis|jurisdiction|region[- ]specific|market[- ]specific|eu|uk|california|brazil)\b"
        r".{0,80}\b(?:consent|permission|opt[- ]?in|opt[- ]?out|lawful basis|legal basis)\b|"
        r"\b(?:consent|permission|opt[- ]?in|opt[- ]?out)\b.{0,80}"
        r"\b(?:gdpr|ccpa|cpra|lgpd|eprivacy|pecr|lawful basis|legal basis|region|jurisdiction)\b",
        re.I,
    ),
    "minor_consent": re.compile(
        r"\b(?:minor consent|child consent|children'?s consent|parental consent|guardian consent|"
        r"age gate|age verification|minimum age|under 13|under thirteen|under 16|under sixteen|coppa)\b",
        re.I,
    ),
    "third_party_sharing": re.compile(
        r"\b(?:third[- ]party|vendor|processor|subprocessor|partner|crm|marketing platform|"
        r"analytics provider|advertising network|external api|data sharing|share with|sell or share)\b"
        r".{0,80}\b(?:consent|permission|opt[- ]?in|opt[- ]?out|preference|authorization)\b|"
        r"\b(?:consent|permission|opt[- ]?in|opt[- ]?out|preference|authorization)\b.{0,80}"
        r"\b(?:third[- ]party|vendor|processor|subprocessor|partner|crm|marketing platform|"
        r"analytics provider|advertising network|external api|data sharing|share with|sell or share)\b",
        re.I,
    ),
}
_CONSENT_CONTEXT_RE = re.compile(
    r"\b(?:consent|permission|authorization|opt[- ]?in|opt[- ]?out|unsubscribe|"
    r"preference center|privacy preference|cookie banner|marketing preference|lawful basis|"
    r"legal basis|parental consent)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|required|requires?|requirement|ensure|"
    r"support|allow|honou?r|capture|collect|record|store|retain|delete|withdraw|revoke|"
    r"before|after|when|if|unless|policy|compliance)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:consent|privacy|permission|authorization|opt[_ -]?in|opt[_ -]?out|withdrawal|"
    r"lawful[_ -]?basis|legal[_ -]?basis|region|gdpr|ccpa|cpra|coppa|minor|parental|"
    r"third[_ -]?party|vendor|retention|audit|evidence)",
    re.I,
)
_PURPOSE_RE = re.compile(
    r"\b(?:consent|permission|authorization|opt[- ]?in)\s+(?:for|to)\s+([^.;,\n]+)",
    re.I,
)
_MECHANISM_RE = re.compile(
    r"\b(?:unchecked checkbox|not pre[- ]checked|granular consent toggle|granular consent|"
    r"affirmative action|checkbox|banner|modal|dialog|form|screen|prompt|toggle|"
    r"preference center|settings)\b",
    re.I,
)
_FIELD_CATEGORY_PATTERNS: dict[ConsentPolicyRequirementCategory, re.Pattern[str]] = {
    "consent_purpose": re.compile(r"\b(?:purpose|consent purpose)\b", re.I),
    "capture_mechanism": re.compile(r"\b(?:capture|capture mechanism|consent screen|consent ui)\b", re.I),
    "withdrawal": re.compile(r"\b(?:withdrawal|withdraw|revoke|revocation|opt out)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit|evidence|receipt|history)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retain|deletion|delete)\b", re.I),
    "regional_legal_basis": re.compile(r"\b(?:regional basis|legal basis|lawful basis|region|gdpr|ccpa|cpra|lgpd)\b", re.I),
    "minor_consent": re.compile(r"\b(?:minor|child|children|parental|guardian|coppa|age)\b", re.I),
    "third_party_sharing": re.compile(r"\b(?:third party|third-party|vendor|processor|sharing|share)\b", re.I),
}
_REGION_RE = re.compile(
    r"\b(?:GDPR|CCPA|CPRA|LGPD|ePrivacy|PECR|EU|UK|California|Brazil|lawful basis|legal basis)\b",
    re.I,
)
_RETENTION_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|older than|at least|up to)\s+)?(?:\d+(?:\.\d+)?|one|two|three|"
    r"four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|thirty|sixty|ninety)\s+"
    r"(?:days?|weeks?|months?|years?|hours?)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "data_requirements",
    "risks",
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
}


@dataclass(frozen=True, slots=True)
class SourceConsentPolicyRequirement:
    """One source-backed consent policy requirement candidate."""

    source_brief_id: str | None
    category: ConsentPolicyRequirementCategory
    requirement_text: str
    detail: str | None = None
    planning_note: str | None = None
    confidence: ConsentPolicyConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ConsentPolicyRequirementCategory:
        """Compatibility alias for callers expecting a longer category field name."""
        return self.category

    @property
    def planning_notes(self) -> str | None:
        """Compatibility alias for callers expecting plural planning notes."""
        return self.planning_note

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "detail": self.detail,
            "planning_note": self.planning_note,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceConsentPolicyRequirementsReport:
    """Source-level consent policy requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceConsentPolicyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceConsentPolicyRequirement, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceConsentPolicyRequirement, ...]:
        """Compatibility view matching reports that name extracted items findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return consent policy requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Consent Policy Requirements Report"
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
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}"
                for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No consent policy requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Detail | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.detail or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_consent_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentPolicyRequirementsReport:
    """Extract source-level consent policy requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _CATEGORY_ORDER.index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.requirement_text.casefold(),
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceConsentPolicyRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_consent_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentPolicyRequirementsReport:
    """Compatibility alias for building a consent policy requirements report."""
    return build_source_consent_policy_requirements(source)


def generate_source_consent_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentPolicyRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_consent_policy_requirements(source)


def derive_source_consent_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentPolicyRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_consent_policy_requirements(source)


def summarize_source_consent_policy_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceConsentPolicyRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted consent policy requirements."""
    if isinstance(source_or_result, SourceConsentPolicyRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_consent_policy_requirements(source_or_result).summary


def source_consent_policy_requirements_to_dict(
    report: SourceConsentPolicyRequirementsReport,
) -> dict[str, Any]:
    """Serialize a consent policy requirements report to a plain dictionary."""
    return report.to_dict()


source_consent_policy_requirements_to_dict.__test__ = False


def source_consent_policy_requirements_to_dicts(
    requirements: (
        tuple[SourceConsentPolicyRequirement, ...]
        | list[SourceConsentPolicyRequirement]
        | SourceConsentPolicyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize consent policy requirement records to dictionaries."""
    if isinstance(requirements, SourceConsentPolicyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_consent_policy_requirements_to_dicts.__test__ = False


def source_consent_policy_requirements_to_markdown(
    report: SourceConsentPolicyRequirementsReport,
) -> str:
    """Render a consent policy requirements report as Markdown."""
    return report.to_markdown()


source_consent_policy_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: ConsentPolicyRequirementCategory
    requirement_text: str
    detail: str | None
    evidence: str
    confidence: ConsentPolicyConfidence


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
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


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
        for source_field, segment in _candidate_segments(payload):
            categories = _categories(segment, source_field)
            if not categories:
                continue
            for category in categories:
                candidates.append(_candidate(source_brief_id, source_field, segment, category))
    return candidates


def _candidate(
    source_brief_id: str | None,
    source_field: str,
    text: str,
    category: ConsentPolicyRequirementCategory,
) -> _Candidate:
    return _Candidate(
        source_brief_id=source_brief_id,
        category=category,
        requirement_text=_requirement_text(text),
        detail=_detail_for(category, text),
        evidence=_evidence_snippet(source_field, text),
        confidence=_confidence(category, source_field, text),
    )


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceConsentPolicyRequirement]:
    grouped: dict[tuple[str | None, ConsentPolicyRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.category,
                _dedupe_text_key(candidate.requirement_text),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceConsentPolicyRequirement] = []
    for (_source_brief_id, _category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        details = tuple(_dedupe(candidate.detail for candidate in items if candidate.detail))
        requirements.append(
            SourceConsentPolicyRequirement(
                source_brief_id=best.source_brief_id,
                category=best.category,
                requirement_text=best.requirement_text,
                detail=", ".join(details) if details else None,
                planning_note=_PLANNING_NOTES[best.category],
                confidence=best.confidence,
                evidence=tuple(
                    sorted(
                        _dedupe(candidate.evidence for candidate in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _STRUCTURED_FIELD_RE.search(key_text) and not isinstance(
                child, (Mapping, list, tuple, set)
            ):
                if text := _optional_text(child):
                    values.append((child_field, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _categories(
    text: str, source_field: str
) -> tuple[ConsentPolicyRequirementCategory, ...]:
    searchable = _searchable_text(source_field, text)
    field_words = _field_words(source_field)
    if _generic_consent_statement(text):
        return ()
    if not (
        _CONSENT_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    ):
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    ):
        return ()

    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
    ]
    if field_categories:
        return tuple(_dedupe(field_categories))

    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if "retention" in categories and "audit_evidence" in categories and not re.search(
        r"\b(?:audit|evidence|receipt|timestamp|policy version|consented_at|withdrawn_at|"
        r"revoked_at|ledger|log)\b",
        searchable,
        re.I,
    ):
        categories.remove("audit_evidence")
    return tuple(_dedupe(categories))


def _detail_for(category: ConsentPolicyRequirementCategory, text: str) -> str | None:
    if category == "consent_purpose":
        return _detail_match(_PURPOSE_RE, text)
    if category == "capture_mechanism":
        if match := _MECHANISM_RE.search(text):
            return _detail(match.group(0))
    if category == "regional_legal_basis":
        return ", ".join(_dedupe(match.group(0) for match in _REGION_RE.finditer(text))) or None
    if category == "retention":
        if match := _RETENTION_WINDOW_RE.search(text):
            return _detail(match.group(0))
    if category == "minor_consent":
        for pattern in (
            r"\bparental consent\b",
            r"\bguardian consent\b",
            r"\bage gate\b",
            r"\bage verification\b",
            r"\bunder 13\b",
            r"\bunder thirteen\b",
            r"\bunder 16\b",
            r"\bunder sixteen\b",
            r"\bCOPPA\b",
        ):
            if match := re.search(pattern, text, re.I):
                return _detail(match.group(0))
    if category == "third_party_sharing":
        if match := re.search(
            r"\b(?:third[- ]party|vendor|processor|subprocessor|partner|crm|marketing platform|analytics provider|advertising network|external api|data sharing)\b",
            text,
            re.I,
        ):
            return _detail(match.group(0))
    return None


def _detail_match(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    return _detail(match.group(1))


def _confidence(
    category: ConsentPolicyRequirementCategory, source_field: str, text: str
) -> ConsentPolicyConfidence:
    field_words = _field_words(source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(text))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(field_words))
    has_detail = bool(_detail_for(category, text))
    if has_explicit_requirement and (has_structured_context or has_detail):
        return "high"
    if has_explicit_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, str]:
    return (
        bool(candidate.detail),
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        len(candidate.evidence),
        candidate.evidence,
    )


def _summary(
    requirements: tuple[SourceConsentPolicyRequirement, ...], source_count: int
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
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
    }


def _has_structured_consent_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "consent",
            "consent_requirement",
            "consent_policy",
            "purpose",
            "capture",
            "capture_mechanism",
            "withdrawal",
            "audit_evidence",
            "retention",
            "regional_basis",
            "legal_basis",
            "minor_consent",
            "third_party_sharing",
        }
    )


def _structured_evidence(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(str(value))
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts) or _clean_text(str(item))


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _clean_text(str(value))
    return [text] if text else []


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


def _generic_consent_statement(text: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:general\s+)?(?:consent|privacy consent)\s+(?:policy\s+)?requirements?\.?",
            _clean_text(text),
            re.I,
        )
    )


def _detail(value: Any) -> str | None:
    text = _clean_text(str(value)) if value is not None else ""
    text = text.strip("`'\" ;,.")
    if not text:
        return None
    return text[:120].rstrip()


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    value = f"{_field_words(source_field)} {text}"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return value.replace("/", " ").replace("_", " ").replace("-", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_text_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_text_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "ConsentPolicyConfidence",
    "ConsentPolicyRequirementCategory",
    "SourceConsentPolicyRequirement",
    "SourceConsentPolicyRequirementsReport",
    "build_source_consent_policy_requirements",
    "derive_source_consent_policy_requirements",
    "extract_source_consent_policy_requirements",
    "generate_source_consent_policy_requirements",
    "source_consent_policy_requirements_to_dict",
    "source_consent_policy_requirements_to_dicts",
    "source_consent_policy_requirements_to_markdown",
    "summarize_source_consent_policy_requirements",
]
