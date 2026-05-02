"""Extract partner onboarding and external launch requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


PartnerOnboardingCategory = Literal[
    "partner_approval",
    "sandbox_access",
    "certification",
    "contact_handoff",
    "launch_checklist",
    "support_model",
    "contract_terms",
    "operational_runbook",
]
PartnerOnboardingConfidence = Literal["high", "medium", "low"]
PartnerOnboardingMissingDetail = Literal[
    "owner",
    "partner_contact",
    "environment",
    "approval_gate",
    "launch_criteria",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[PartnerOnboardingCategory, ...] = (
    "partner_approval",
    "sandbox_access",
    "certification",
    "contact_handoff",
    "launch_checklist",
    "support_model",
    "contract_terms",
    "operational_runbook",
)
_MISSING_DETAIL_ORDER: tuple[PartnerOnboardingMissingDetail, ...] = (
    "owner",
    "partner_contact",
    "environment",
    "approval_gate",
    "launch_criteria",
)
_CONFIDENCE_ORDER: dict[PartnerOnboardingConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PARTNER_CONTEXT_RE = re.compile(
    r"\b(?:partner|vendor|marketplace|app store|external handoff|external team|"
    r"third[- ]party|integration partner|channel partner|reseller|platform partner)\b",
    re.I,
)
_ONBOARDING_CONTEXT_RE = re.compile(
    r"\b(?:onboard|onboarding|setup|set up|approval|approve|certification|certify|"
    r"sandbox|developer account|credentials?|api keys?|go[- ]live|launch|production access|"
    r"marketplace listing|app review|checklist|handoff|partner contact|runbook|"
    r"support model|sla|contract|terms|msa|dpa)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"before launch|pre[- ]launch|cannot ship|blocked until|gate|gated|done when|"
    r"acceptance|provide|obtain|confirm|complete|document|define|assign)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:partner|vendor|marketplace|external|handoff|onboard|approval|certification|"
    r"sandbox|credential|go[-_ ]?live|launch|checklist|support|contract|terms|"
    r"runbook|requirements?|acceptance|criteria|metadata|source[_ -]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:partner|vendor|marketplace|external|"
    r"third[- ]party|onboarding|approval|certification|sandbox|go[- ]live|launch)\b"
    r".{0,100}\b(?:required|needed|in scope|scope|work|changes?|handoff)\b|"
    r"\b(?:no|not|without)\b.{0,100}\b(?:required|needed|in scope|scope|work|changes?|handoff)\b"
    r".{0,100}\b(?:partner|vendor|marketplace|external|third[- ]party|onboarding|approval|"
    r"certification|sandbox|go[- ]live|launch)\b",
    re.I,
)
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(?:out of scope|non[- ]goal|not part of this release|future consideration|later phase)\b",
    re.I,
)
_GENERIC_VENDOR_RE = re.compile(
    r"\b(?:vendor|partner|third[- ]party|external provider|marketplace)\b",
    re.I,
)
_DETAIL_PATTERNS: dict[PartnerOnboardingMissingDetail, re.Pattern[str]] = {
    "owner": re.compile(r"\b(?:owner|dri|responsible|accountable|assigned to|managed by)\b", re.I),
    "partner_contact": re.compile(
        r"\b(?:partner contact|vendor contact|contact email|technical contact|partner manager|"
        r"account manager|support contact|named contact|escalation contact)\b",
        re.I,
    ),
    "environment": re.compile(
        r"\b(?:sandbox|staging|production|prod|test environment|developer account|"
        r"environment|tenant|workspace)\b",
        re.I,
    ),
    "approval_gate": re.compile(
        r"\b(?:approval|approved|sign[- ]off|review gate|go/no-go|certification|"
        r"app review|gate|blocked until|cannot ship)\b",
        re.I,
    ),
    "launch_criteria": re.compile(
        r"\b(?:launch criteria|go[- ]live criteria|go[- ]live checklist|launch checklist|"
        r"production readiness|cutover|done when|acceptance criteria|launch when|"
        r"ready to launch)\b",
        re.I,
    ),
}
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
    "integration_points",
    "risks",
    "assumptions",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[PartnerOnboardingCategory, re.Pattern[str]] = {
    "partner_approval": re.compile(
        r"\b(?:partner approval|vendor approval|partner review|approval from partner|"
        r"partner sign[- ]off|vendor sign[- ]off|marketplace approval|app review|"
        r"approval gate|approved by (?:the )?(?:partner|vendor|marketplace))\b",
        re.I,
    ),
    "sandbox_access": re.compile(
        r"\b(?:sandbox access|sandbox credentials?|sandbox keys?|developer account|"
        r"test account|test tenant|staging tenant|partner credentials?|api keys?|"
        r"client id|client secret|production credentials?|prod credentials?)\b",
        re.I,
    ),
    "certification": re.compile(
        r"\b(?:certification|certify|certified|compliance review|security review|"
        r"app certification|technical certification|marketplace certification|"
        r"penetration test attestation|oauth verification)\b",
        re.I,
    ),
    "contact_handoff": re.compile(
        r"\b(?:partner contact|vendor contact|external handoff|handoff to partner|"
        r"handoff with partner|technical contact|account manager|partner manager|"
        r"support contact|escalation contact|named contact)\b",
        re.I,
    ),
    "launch_checklist": re.compile(
        r"\b(?:go[- ]live checklist|launch checklist|launch criteria|go[- ]live criteria|"
        r"pre[- ]launch checklist|production readiness checklist|cutover checklist|"
        r"launch gate|go[- ]live gate)\b",
        re.I,
    ),
    "support_model": re.compile(
        r"\b(?:support model|support path|escalation path|support escalation|"
        r"sla|service level|support hours|tier 1|tier 2|incident escalation|"
        r"joint support)\b",
        re.I,
    ),
    "contract_terms": re.compile(
        r"\b(?:contract terms|partner terms|vendor terms|marketplace terms|msa|dpa|"
        r"baa|order form|data processing agreement|commercial terms|legal approval|"
        r"terms of service|rate limits? in contract|sla terms)\b",
        re.I,
    ),
    "operational_runbook": re.compile(
        r"\b(?:operational runbook|partner runbook|vendor runbook|handoff runbook|"
        r"ops runbook|incident runbook|cutover runbook|rollback runbook|"
        r"operational handoff|external handoff procedure)\b",
        re.I,
    ),
}
_CATEGORY_NOTES: dict[PartnerOnboardingCategory, str] = {
    "partner_approval": "Confirm who grants partner approval and which gate blocks launch.",
    "sandbox_access": "Secure sandbox and production credential ownership, environments, and rotation path.",
    "certification": "Track certification evidence, review owner, and pass/fail criteria.",
    "contact_handoff": "Record named partner contacts, escalation channels, and internal handoff owner.",
    "launch_checklist": "Define go-live checklist, launch criteria, and sign-off sequence.",
    "support_model": "Document joint support ownership, escalation path, hours, and SLA expectations.",
    "contract_terms": "Validate partner terms, legal approvals, data terms, and commercial constraints.",
    "operational_runbook": "Create runbooks for cutover, rollback, incident response, and external handoffs.",
}


@dataclass(frozen=True, slots=True)
class SourcePartnerOnboardingRequirement:
    """One source-backed partner onboarding requirement category."""

    source_brief_id: str | None
    category: PartnerOnboardingCategory
    confidence: PartnerOnboardingConfidence
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[PartnerOnboardingMissingDetail, ...] = field(default_factory=tuple)
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "missing_details": list(self.missing_details),
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourcePartnerOnboardingRequirementsReport:
    """Source-level partner onboarding requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourcePartnerOnboardingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePartnerOnboardingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return partner onboarding requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Partner Onboarding Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        missing_detail_counts = self.summary.get("missing_detail_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
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
            "- Missing detail counts: "
            + ", ".join(
                f"{detail} {missing_detail_counts.get(detail, 0)}"
                for detail in _MISSING_DETAIL_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source partner onboarding requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Source Field Paths | Missing Details | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell(', '.join(requirement.missing_details) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_partner_onboarding_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> SourcePartnerOnboardingRequirementsReport:
    """Extract partner onboarding requirement signals from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                _CATEGORY_ORDER.index(requirement.category),
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePartnerOnboardingRequirementsReport(
        source_brief_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_partner_onboarding_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> tuple[SourcePartnerOnboardingRequirement, ...]:
    """Return partner onboarding requirement records extracted from input."""
    return build_source_partner_onboarding_requirements(source).requirements


def generate_source_partner_onboarding_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> SourcePartnerOnboardingRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_partner_onboarding_requirements(source)


def derive_source_partner_onboarding_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> SourcePartnerOnboardingRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_partner_onboarding_requirements(source)


def summarize_source_partner_onboarding_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourcePartnerOnboardingRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic partner onboarding requirements summary."""
    if isinstance(source_or_result, SourcePartnerOnboardingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_partner_onboarding_requirements(source_or_result).summary


def source_partner_onboarding_requirements_to_dict(
    report: SourcePartnerOnboardingRequirementsReport,
) -> dict[str, Any]:
    """Serialize a partner onboarding requirements report to a plain dictionary."""
    return report.to_dict()


source_partner_onboarding_requirements_to_dict.__test__ = False


def source_partner_onboarding_requirements_to_dicts(
    requirements: (
        tuple[SourcePartnerOnboardingRequirement, ...]
        | list[SourcePartnerOnboardingRequirement]
        | SourcePartnerOnboardingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize partner onboarding requirement records to dictionaries."""
    if isinstance(requirements, SourcePartnerOnboardingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_partner_onboarding_requirements_to_dicts.__test__ = False


def source_partner_onboarding_requirements_to_markdown(
    report: SourcePartnerOnboardingRequirementsReport,
) -> str:
    """Render a partner onboarding requirements report as Markdown."""
    return report.to_markdown()


source_partner_onboarding_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_brief_id: str | None
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: PartnerOnboardingCategory
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    present_details: tuple[PartnerOnboardingMissingDetail, ...]
    confidence: PartnerOnboardingConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
            return _source_id(payload), payload
    if isinstance(source, (bytes, bytearray)):
        return None, {}
    payload = _object_payload(source)
    return _source_id(payload), payload


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
        for segment in _candidate_segments(source_brief_id, payload):
            if not _is_requirement(segment):
                continue
            searchable = _searchable_text(segment.source_field, segment.text)
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            if not categories and _is_generic_vendor_requirement(segment):
                categories = ["partner_approval"]
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(category, segment.source_field, segment.text),
                        present_details=_present_details(segment.source_field, segment.text),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePartnerOnboardingRequirement]:
    grouped: dict[tuple[str | None, PartnerOnboardingCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourcePartnerOnboardingRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)
        )
        present_details = set(
            detail for item in items for detail in item.present_details
        )
        missing_details = tuple(
            detail for detail in _MISSING_DETAIL_ORDER if detail not in present_details
        )
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        requirements.append(
            SourcePartnerOnboardingRequirement(
                source_brief_id=source_brief_id,
                category=category,
                confidence=confidence,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                missing_details=missing_details,
                planning_note=_CATEGORY_NOTES[category],
            )
        )
    return requirements


def _candidate_segments(source_brief_id: str | None, payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, source_brief_id, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, source_brief_id, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_brief_id: str | None,
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
                _STRUCTURED_FIELD_RE.search(key_text)
                or _PARTNER_CONTEXT_RE.search(key_text)
                or _ONBOARDING_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, source_brief_id, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, source_brief_id, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(
                _Segment(
                    source_brief_id=source_brief_id,
                    source_field=source_field,
                    text=segment_text,
                    section_context=field_context,
                )
            )


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
            clauses = [part] if _NEGATED_SCOPE_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _OUT_OF_SCOPE_RE.search(searchable) and not _REQUIREMENT_RE.search(segment.text):
        return False
    categories = [
        category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if not categories:
        return _is_generic_vendor_requirement(segment)
    has_partner_context = bool(_PARTNER_CONTEXT_RE.search(searchable))
    has_onboarding_context = bool(_ONBOARDING_CONTEXT_RE.search(searchable))
    has_directive = bool(_REQUIREMENT_RE.search(searchable))
    if has_partner_context and has_onboarding_context:
        return True
    if (field_context or segment.section_context) and (has_onboarding_context or has_directive):
        return True
    return has_directive and has_partner_context


def _is_generic_vendor_requirement(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not _GENERIC_VENDOR_RE.search(searchable):
        return False
    if any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        return False
    return bool(
        _REQUIREMENT_RE.search(searchable)
        and (_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)) or segment.section_context)
    )


def _matched_terms(
    category: PartnerOnboardingCategory,
    source_field: str,
    text: str,
) -> tuple[str, ...]:
    searchable = _searchable_text(source_field, text)
    terms = [
        _clean_text(match.group(0))
        for match in _CATEGORY_PATTERNS[category].finditer(searchable)
    ]
    if not terms:
        terms.extend(_clean_text(match.group(0)) for match in _GENERIC_VENDOR_RE.finditer(searchable))
    return tuple(_dedupe(terms))


def _present_details(
    source_field: str,
    text: str,
) -> tuple[PartnerOnboardingMissingDetail, ...]:
    searchable = _searchable_text(source_field, text)
    return tuple(
        detail
        for detail in _MISSING_DETAIL_ORDER
        if _DETAIL_PATTERNS[detail].search(searchable)
    )


def _confidence(
    segment: _Segment,
    category: PartnerOnboardingCategory,
) -> PartnerOnboardingConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    has_directive = bool(_REQUIREMENT_RE.search(searchable))
    has_partner_context = bool(_PARTNER_CONTEXT_RE.search(searchable))
    has_onboarding_context = bool(_ONBOARDING_CONTEXT_RE.search(searchable))
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_specific_match = bool(_CATEGORY_PATTERNS[category].search(searchable))
    detail_count = len(_present_details(segment.source_field, segment.text))
    if has_specific_match and has_directive and has_partner_context and detail_count >= 1:
        return "high"
    if has_specific_match and (
        has_directive or (has_partner_context and has_onboarding_context) or field_context
    ):
        return "medium"
    if has_directive and (field_context or segment.section_context) and _GENERIC_VENDOR_RE.search(searchable):
        return "low"
    return "low"


def _summary(
    requirements: tuple[SourcePartnerOnboardingRequirement, ...],
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
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_counts": {
            detail: sum(
                1 for requirement in requirements if detail in requirement.missing_details
            )
            for detail in _MISSING_DETAIL_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
        "missing_detail_count": sum(
            len(requirement.missing_details) for requirement in requirements
        ),
    }


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
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "integration_points",
        "risks",
        "assumptions",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


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


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = _clean_text(str(value)).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


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


__all__ = [
    "PartnerOnboardingCategory",
    "PartnerOnboardingConfidence",
    "PartnerOnboardingMissingDetail",
    "SourcePartnerOnboardingRequirement",
    "SourcePartnerOnboardingRequirementsReport",
    "build_source_partner_onboarding_requirements",
    "derive_source_partner_onboarding_requirements",
    "extract_source_partner_onboarding_requirements",
    "generate_source_partner_onboarding_requirements",
    "summarize_source_partner_onboarding_requirements",
    "source_partner_onboarding_requirements_to_dict",
    "source_partner_onboarding_requirements_to_dicts",
    "source_partner_onboarding_requirements_to_markdown",
]
