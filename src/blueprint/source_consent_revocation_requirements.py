"""Extract consent revocation and preference withdrawal requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ConsentRevocationRequirementCategory = Literal[
    "withdrawal_channel",
    "immediate_effect",
    "downstream_propagation",
    "data_processing_stop",
    "audit_evidence",
    "user_confirmation",
    "exception_handling",
    "reconsent_flow",
]
ConsentRevocationConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[ConsentRevocationRequirementCategory, ...] = (
    "withdrawal_channel",
    "immediate_effect",
    "downstream_propagation",
    "data_processing_stop",
    "audit_evidence",
    "user_confirmation",
    "exception_handling",
    "reconsent_flow",
)
_CONFIDENCE_ORDER: dict[ConsentRevocationConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLAN_IMPACTS: dict[ConsentRevocationRequirementCategory, tuple[str, ...]] = {
    "withdrawal_channel": ("Add user-facing withdrawal, opt-out, unsubscribe, or preference-center tasks.",),
    "immediate_effect": ("Plan state changes that make revoked consent effective immediately or within the stated SLA.",),
    "downstream_propagation": ("Add propagation tasks for vendors, processors, warehouses, queues, and suppression systems.",),
    "data_processing_stop": ("Add enforcement tasks that stop processing, targeting, tracking, or sharing after revocation.",),
    "audit_evidence": ("Record durable revocation evidence such as actor, timestamp, channel, policy version, and source.",),
    "user_confirmation": ("Add confirmation or receipt behavior after a user withdraws consent.",),
    "exception_handling": ("Model legal, safety, transactional, and contractual exceptions to revocation behavior.",),
    "reconsent_flow": ("Plan how users can reconsent, resubscribe, or re-enable processing after withdrawal.",),
}
_CATEGORY_PATTERNS: dict[ConsentRevocationRequirementCategory, re.Pattern[str]] = {
    "withdrawal_channel": re.compile(
        r"\b(?:withdraw|revoke|opt[- ]?out|unsubscribe)\b.{0,70}"
        r"\b(?:through|via|from|using|link|center|settings|portal|page|keyword|support|api)\b|"
        r"\b(?:preference center|privacy settings|consent settings|unsubscribe link|"
        r"STOP keyword|suppression request|do not sell|delete consent|remove consent|change consent)\b",
        re.I,
    ),
    "immediate_effect": re.compile(
        r"\b(?:immediate(?:ly)?|real[- ]?time|as soon as|without delay|same[- ]?session|"
        r"effective (?:immediately|upon)|within\s+(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|fifteen|thirty|sixty|ninety)\s+"
        r"(?:seconds?|minutes?|hours?|days?))\b.{0,80}\b(?:withdraw|revoke|revocation|opt[- ]?out|unsubscribe|stop processing)\b|"
        r"\b(?:withdraw|revoke|revocation|opt[- ]?out|unsubscribe|stop processing)\b.{0,80}"
        r"\b(?:immediate(?:ly)?|real[- ]?time|without delay|same[- ]?session|within\s+(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|fifteen|thirty|sixty|ninety)\s+"
        r"(?:seconds?|minutes?|hours?|days?))\b",
        re.I,
    ),
    "downstream_propagation": re.compile(
        r"\b(?:propagat(?:e|ion)|sync|fan[- ]?out|notify|send|publish|webhook|event)\b.{0,90}"
        r"\b(?:withdraw|revocation|opt[- ]?out|unsubscribe|suppression|preference)\b|"
        r"\b(?:withdraw|revocation|opt[- ]?out|unsubscribe|suppression|preference)\b.{0,90}"
        r"\b(?:vendor|processor|subprocessor|partner|third[- ]party|crm|cdp|warehouse|"
        r"marketing platform|analytics provider|ad network|downstream|suppression list|webhook|event)\b",
        re.I,
    ),
    "data_processing_stop": re.compile(
        r"\b(?:stop|cease|halt|disable|suspend|block|suppress|do not)\b.{0,80}"
        r"\b(?:processing|tracking|profiling|targeting|marketing|sharing|selling|personalization|analytics|emails?|messages?)\b|"
        r"\b(?:processing|tracking|profiling|targeting|marketing|sharing|selling|personalization|analytics)\b.{0,80}"
        r"\b(?:stop|cease|halt|disable|suspend|block|suppress|opt[- ]?out|withdraw|revoke)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|audit trail|audit log|history|evidence|receipt|ledger|record|timestamp|"
        r"actor|user id|ip address|policy version|withdrawn_at|revoked_at|unsubscribed_at)\b.{0,90}"
        r"\b(?:withdraw|withdrawal|revocation|revoke|opt[- ]?out|unsubscribe|suppression)\b|"
        r"\b(?:withdraw|withdrawal|revocation|revoke|opt[- ]?out|unsubscribe|suppression)\b.{0,90}"
        r"\b(?:audit|audit trail|audit log|history|evidence|receipt|ledger|record|timestamp|"
        r"actor|policy version|withdrawn_at|revoked_at|unsubscribed_at)\b",
        re.I,
    ),
    "user_confirmation": re.compile(
        r"\b(?:confirm|confirmation|receipt|acknowledge|acknowledgement|success message|toast|email)\b.{0,80}"
        r"\b(?:withdraw|withdrawal|revocation|revoke|opt[- ]?out|unsubscribe|suppression)\b|"
        r"\b(?:withdraw|withdrawal|revocation|revoke|opt[- ]?out|unsubscribe|suppression)\b.{0,80}"
        r"\b(?:confirm|confirmation|receipt|acknowledge|acknowledgement|success message|toast|email)\b",
        re.I,
    ),
    "exception_handling": re.compile(
        r"\b(?:except|exception|unless|legal obligation|required by law|contractual|transactional|"
        r"security|fraud|account|service message|essential|legitimate interest)\b.{0,100}"
        r"\b(?:withdraw|revocation|revoke|opt[- ]?out|unsubscribe|stop processing|suppression)\b|"
        r"\b(?:withdraw|revocation|revoke|opt[- ]?out|unsubscribe|stop processing|suppression)\b.{0,100}"
        r"\b(?:except|exception|unless|legal obligation|required by law|contractual|transactional|"
        r"security|fraud|account|service message|essential|legitimate interest)\b",
        re.I,
    ),
    "reconsent_flow": re.compile(
        r"\b(?:re[- ]?consent|consent again|new opt[- ]?in|fresh consent|resubscribe|"
        r"re[- ]?subscribe|re[- ]?enable|restore consent|renew consent)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[ConsentRevocationRequirementCategory, re.Pattern[str]] = {
    "withdrawal_channel": re.compile(r"\b(?:withdrawal channel|channel|preference center|unsubscribe|opt out)\b", re.I),
    "immediate_effect": re.compile(r"\b(?:immediate|timing|sla|effective|latency)\b", re.I),
    "downstream_propagation": re.compile(r"\b(?:propagation|downstream|vendor|processor|suppression list|sync)\b", re.I),
    "data_processing_stop": re.compile(r"\b(?:processing stop|stop processing|suppression|processing)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit|evidence|receipt|history|record)\b", re.I),
    "user_confirmation": re.compile(r"\b(?:confirmation|confirm|receipt|acknowledge)\b", re.I),
    "exception_handling": re.compile(r"\b(?:exception|exceptions|unless|legal|transactional)\b", re.I),
    "reconsent_flow": re.compile(r"\b(?:reconsent|re consent|resubscribe|re subscribe|re enable)\b", re.I),
}
_REVOCATION_CONTEXT_RE = re.compile(
    r"\b(?:withdraw(?:al)?|revoke|revocation|opt[- ]?out|unsubscribe|stop processing|"
    r"preference center|privacy preference|consent settings|suppression|do not sell|"
    r"remove consent|delete consent|consent cancellation|re[- ]?consent|resubscribe)\b",
    re.I,
)
_CAPTURE_ONLY_RE = re.compile(
    r"\b(?:capture|collect|obtain|request|ask for|record|present|show)\b.{0,70}"
    r"\b(?:consent|permission|opt[- ]?in)\b|"
    r"\b(?:consent|permission|opt[- ]?in)\b.{0,70}"
    r"\b(?:checkbox|banner|modal|dialog|form|screen|prompt|toggle|captured|collected|obtained)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|required|requires?|requirement|ensure|support|"
    r"allow|honou?r|record|store|retain|stop|cease|propagate|confirm|send|within|"
    r"immediately|when|if|unless|after|before|policy|compliance)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:revocation|revoke|withdrawal|withdraw|opt[_ -]?out|unsubscribe|suppression|"
    r"preference|consent[_ -]?settings|processing[_ -]?stop|downstream|"
    r"propagation|vendor|processor|audit|evidence|confirmation|exception|reconsent|"
    r"resubscribe|do[_ -]?not[_ -]?sell)",
    re.I,
)
_CHANNEL_RE = re.compile(
    r"\b(?:preference center|privacy settings|consent settings|account settings|unsubscribe link|"
    r"email link|support ticket|support request|api|webhook|STOP keyword|sms|portal|settings page)\b",
    re.I,
)
_PROPAGATION_RE = re.compile(
    r"\b(?:vendor|processor|subprocessor|partner|third[- ]party|crm|cdp|warehouse|"
    r"marketing platform|analytics provider|ad network|downstream|suppression list|"
    r"webhook|event|queue|data lake)\b",
    re.I,
)
_TIMING_RE = re.compile(
    r"\b(?:immediately|real[- ]?time|without delay|same[- ]?session|as soon as possible|"
    r"within\s+(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|fifteen|thirty|sixty|ninety)\s+"
    r"(?:seconds?|minutes?|hours?|days?))\b",
    re.I,
)
_AUDIT_RE = re.compile(
    r"\b(?:audit trail|audit log|audit event|history|evidence|receipt|ledger|timestamp|"
    r"actor|user id|ip address|policy version|withdrawn_at|revoked_at|unsubscribed_at)\b",
    re.I,
)
_CONFIRMATION_RE = re.compile(
    r"\b(?:confirmation email|confirmation receipt|receipt|success message|toast|acknowledgement|acknowledgment|confirm(?:ation)?)\b",
    re.I,
)
_EXCEPTION_RE = re.compile(
    r"\b(?:legal obligation|required by law|contractual|transactional|security|fraud|"
    r"account notice|service message|essential|legitimate interest|exception|unless|except)\b",
    re.I,
)
_RECONSENT_RE = re.compile(
    r"\b(?:re[- ]?consent|consent again|new opt[- ]?in|fresh consent|resubscribe|"
    r"re[- ]?subscribe|re[- ]?enable|restore consent|renew consent)\b",
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
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceConsentRevocationRequirement:
    """One source-backed consent revocation requirement candidate."""

    source_brief_id: str | None
    category: ConsentRevocationRequirementCategory
    requirement_text: str
    revocation_channel: str | None = None
    propagation: str | None = None
    timing: str | None = None
    audit_evidence: str | None = None
    user_confirmation: str | None = None
    exception_handling: str | None = None
    reconsent_flow: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ConsentRevocationConfidence = "medium"
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ConsentRevocationRequirementCategory:
        """Compatibility alias for callers expecting a longer category field name."""
        return self.category

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting planning notes."""
        return self.suggested_plan_impacts

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "revocation_channel": self.revocation_channel,
            "propagation": self.propagation,
            "timing": self.timing,
            "audit_evidence": self.audit_evidence,
            "user_confirmation": self.user_confirmation,
            "exception_handling": self.exception_handling,
            "reconsent_flow": self.reconsent_flow,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "unresolved_questions": list(self.unresolved_questions),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceConsentRevocationRequirementsReport:
    """Source-level consent revocation requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceConsentRevocationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceConsentRevocationRequirement, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceConsentRevocationRequirement, ...]:
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
        """Return consent revocation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Consent Revocation Requirements Report"
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
            lines.extend(["", "No consent revocation requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Channel | Propagation | Timing | Audit | Confirmation | Exceptions | Reconsent | Source Field | Confidence | Unresolved Questions | Suggested Plan Impacts | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.revocation_channel or '')} | "
                f"{_markdown_cell(requirement.propagation or '')} | "
                f"{_markdown_cell(requirement.timing or '')} | "
                f"{_markdown_cell(requirement.audit_evidence or '')} | "
                f"{_markdown_cell(requirement.user_confirmation or '')} | "
                f"{_markdown_cell(requirement.exception_handling or '')} | "
                f"{_markdown_cell(requirement.reconsent_flow or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_consent_revocation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentRevocationRequirementsReport:
    """Extract source-level consent revocation requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _CATEGORY_ORDER.index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.requirement_text.casefold(),
                requirement.source_field or "",
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceConsentRevocationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_consent_revocation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentRevocationRequirementsReport:
    """Compatibility alias for building a consent revocation requirements report."""
    return build_source_consent_revocation_requirements(source)


def generate_source_consent_revocation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentRevocationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_consent_revocation_requirements(source)


def derive_source_consent_revocation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceConsentRevocationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_consent_revocation_requirements(source)


def summarize_source_consent_revocation_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceConsentRevocationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted consent revocation requirements."""
    if isinstance(source_or_result, SourceConsentRevocationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_consent_revocation_requirements(source_or_result).summary


def source_consent_revocation_requirements_to_dict(
    report: SourceConsentRevocationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a consent revocation requirements report to a plain dictionary."""
    return report.to_dict()


source_consent_revocation_requirements_to_dict.__test__ = False


def source_consent_revocation_requirements_to_dicts(
    requirements: (
        tuple[SourceConsentRevocationRequirement, ...]
        | list[SourceConsentRevocationRequirement]
        | SourceConsentRevocationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize consent revocation requirement records to dictionaries."""
    if isinstance(requirements, SourceConsentRevocationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_consent_revocation_requirements_to_dicts.__test__ = False


def source_consent_revocation_requirements_to_markdown(
    report: SourceConsentRevocationRequirementsReport,
) -> str:
    """Render a consent revocation requirements report as Markdown."""
    return report.to_markdown()


source_consent_revocation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: ConsentRevocationRequirementCategory
    requirement_text: str
    revocation_channel: str | None
    propagation: str | None
    timing: str | None
    audit_evidence: str | None
    user_confirmation: str | None
    exception_handling: str | None
    reconsent_flow: str | None
    source_field: str
    evidence: str
    confidence: ConsentRevocationConfidence


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
    category: ConsentRevocationRequirementCategory,
) -> _Candidate:
    return _Candidate(
        source_brief_id=source_brief_id,
        category=category,
        requirement_text=_requirement_text(text),
        revocation_channel=_joined_details(match.group(0) for match in _CHANNEL_RE.finditer(text)),
        propagation=_propagation_detail(text),
        timing=_match_detail(_TIMING_RE, text),
        audit_evidence=_audit_detail(text),
        user_confirmation=_match_detail(_CONFIRMATION_RE, text),
        exception_handling=_exception_detail(text),
        reconsent_flow=_match_detail(_RECONSENT_RE, text),
        source_field=source_field,
        evidence=_evidence_snippet(source_field, text),
        confidence=_confidence(category, source_field, text),
    )


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceConsentRevocationRequirement]:
    grouped: dict[tuple[str | None, ConsentRevocationRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.category,
                _dedupe_requirement_key(candidate.requirement_text, candidate.category),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceConsentRevocationRequirement] = []
    for (_source_brief_id, _category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceConsentRevocationRequirement(
                source_brief_id=best.source_brief_id,
                category=best.category,
                requirement_text=best.requirement_text,
                revocation_channel=_joined_details(item.revocation_channel for item in items),
                propagation=_joined_details(item.propagation for item in items),
                timing=_joined_details(item.timing for item in items),
                audit_evidence=_joined_details(item.audit_evidence for item in items),
                user_confirmation=_joined_details(item.user_confirmation for item in items),
                exception_handling=_joined_details(item.exception_handling for item in items),
                reconsent_flow=_joined_details(item.reconsent_flow for item in items),
                source_field=best.source_field,
                evidence=tuple(
                    sorted(
                        _dedupe(candidate.evidence for candidate in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
                confidence=best.confidence,
                unresolved_questions=_unresolved_questions(best.category, items),
                suggested_plan_impacts=_PLAN_IMPACTS[best.category],
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
        if _has_structured_revocation_shape(value):
            evidence = _structured_evidence(value)
            if evidence:
                values.append((source_field, evidence))
            return
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
            segments.extend([sentence] if _AUDIT_RE.search(sentence) else _CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _categories(
    text: str, source_field: str
) -> tuple[ConsentRevocationRequirementCategory, ...]:
    searchable = _searchable_text(source_field, text)
    field_words = _field_words(source_field)
    if _generic_revocation_statement(text):
        return ()
    if not (_REVOCATION_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return ()
    if _CAPTURE_ONLY_RE.search(searchable) and not _REVOCATION_CONTEXT_RE.search(searchable):
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
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if (
        "data_processing_stop" in categories
        and "exception_handling" in categories
        and re.search(r"\bstop processing except\b", searchable, re.I)
    ):
        categories.remove("data_processing_stop")
    if (
        "audit_evidence" in categories
        and "user_confirmation" in categories
        and not re.search(
            r"\b(?:audit|audit trail|audit log|history|ledger|timestamp|actor|user id|"
            r"ip address|policy version|withdrawn_at|revoked_at|unsubscribed_at)\b",
            searchable,
            re.I,
        )
    ):
        categories.remove("audit_evidence")
    return tuple(_dedupe(field_categories + categories))


def _confidence(
    category: ConsentRevocationRequirementCategory, source_field: str, text: str
) -> ConsentRevocationConfidence:
    field_words = _field_words(source_field)
    searchable = _searchable_text(source_field, text)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(text))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(field_words))
    has_category = bool(_CATEGORY_PATTERNS[category].search(searchable))
    has_detail = any(
        (
            _joined_details(match.group(0) for match in _CHANNEL_RE.finditer(text)),
            _match_detail(_TIMING_RE, text),
            _propagation_detail(text),
            _audit_detail(text),
            _match_detail(_CONFIRMATION_RE, text),
            _exception_detail(text),
            _match_detail(_RECONSENT_RE, text),
        )
    )
    if has_category and has_explicit_requirement and (has_structured_context or has_detail):
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context or has_detail):
        return "medium"
    return "low"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    detail_count = sum(
        bool(value)
        for value in (
            candidate.revocation_channel,
            candidate.propagation,
            candidate.timing,
            candidate.audit_evidence,
            candidate.user_confirmation,
            candidate.exception_handling,
            candidate.reconsent_flow,
        )
    )
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _summary(
    requirements: tuple[SourceConsentRevocationRequirement, ...], source_count: int
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


def _unresolved_questions(
    category: ConsentRevocationRequirementCategory, items: Iterable[_Candidate]
) -> tuple[str, ...]:
    candidates = tuple(items)
    questions: list[str] = []
    if category == "withdrawal_channel" and not any(item.revocation_channel for item in candidates):
        questions.append("Which withdrawal channel should users use?")
    if category == "downstream_propagation" and not any(item.propagation for item in candidates):
        questions.append("Which downstream systems must receive the revocation?")
    if category in {"immediate_effect", "data_processing_stop"} and not any(item.timing for item in candidates):
        questions.append("What SLA applies after revocation?")
    if category == "audit_evidence" and not any(item.audit_evidence for item in candidates):
        questions.append("Which revocation audit fields are required?")
    if category == "user_confirmation" and not any(item.user_confirmation for item in candidates):
        questions.append("What confirmation should the user receive?")
    if category == "exception_handling" and not any(item.exception_handling for item in candidates):
        questions.append("Which exceptions allow continued processing?")
    if category == "reconsent_flow" and not any(item.reconsent_flow for item in candidates):
        questions.append("How can the user reconsent after withdrawal?")
    return tuple(questions)


def _has_structured_revocation_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if keys <= {"revocation", "consent_revocation", "withdrawal"} and any(
        isinstance(value, (Mapping, list, tuple, set)) for value in item.values()
    ):
        return False
    return bool(
        keys
        & {
            "revocation",
            "consent_revocation",
            "withdrawal",
            "withdrawal_channel",
            "opt_out",
            "unsubscribe",
            "processing_stop",
            "data_processing_stop",
            "propagation",
            "downstream_propagation",
            "audit_evidence",
            "confirmation",
            "user_confirmation",
            "exception_handling",
            "exceptions",
            "reconsent",
            "reconsent_flow",
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


def _generic_revocation_statement(text: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:general\s+)?(?:consent revocation|revocation|withdrawal|opt[- ]?out)\s+requirements?\.?",
            _clean_text(text),
            re.I,
        )
    )


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        return _detail(match.group(0))
    return None


def _exception_detail(text: str) -> str | None:
    for pattern in (
        r"\blegal obligation\b",
        r"\brequired by law\b",
        r"\bcontractual\b",
        r"\btransactional(?:\s+service messages?|\s+account notices?)?\b",
        r"\bsecurity\b",
        r"\bfraud\b",
        r"\bservice messages?\b",
        r"\bessential\b",
        r"\blegitimate interest\b",
        r"\bexception\b",
        r"\bunless\b",
        r"\bexcept\b",
    ):
        if match := re.search(pattern, text, re.I):
            return _detail(match.group(0))
    return None


def _audit_detail(text: str) -> str | None:
    details = _dedupe(
        match.group(0)
        for pattern in (
            r"\btimestamp\b",
            r"\bactor\b",
            r"\buser id\b",
            r"\bip address\b",
            r"\bpolicy version\b",
            r"\bwithdrawn_at\b",
            r"\brevoked_at\b",
            r"\bunsubscribed_at\b",
            r"\baudit trail\b",
            r"\baudit log\b",
            r"\baudit event\b",
            r"\breceipt\b",
            r"\bledger\b",
        )
        for match in re.finditer(pattern, text, re.I)
    )
    return ", ".join(details) if details else None


def _propagation_detail(text: str) -> str | None:
    matches = (
        match.group(0)
        for pattern in (
            r"\bvendors?\b",
            r"\bprocessors?\b",
            r"\bsubprocessors?\b",
            r"\bpartners?\b",
            r"\bthird[- ]part(?:y|ies)\b",
            r"\bCRM\b",
            r"\bCDP\b",
            r"\bwarehouse\b",
            r"\bmarketing platform\b",
            r"\banalytics provider\b",
            r"\bad network\b",
            r"\bdownstream\b",
            r"\bsuppression list\b",
            r"\bwebhook\b",
            r"\bqueue\b",
            r"\bdata lake\b",
        )
        for match in re.finditer(pattern, text, re.I)
    )
    details = _dedupe(sorted(matches, key=lambda value: text.casefold().find(value.casefold())))
    return ", ".join(details) if details else None


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


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


def _dedupe_requirement_key(
    value: str, category: ConsentRevocationRequirementCategory
) -> str:
    text = _clean_text(value).casefold()
    text = re.sub(r"\b(?:withdraw(?:al)?|revoke|revocation|opt[- ]?out|unsubscribe)\b", "revocation", text)
    return f"{category}:{_SPACE_RE.sub(' ', text).strip()}"


def _dedupe_text_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "ConsentRevocationConfidence",
    "ConsentRevocationRequirementCategory",
    "SourceConsentRevocationRequirement",
    "SourceConsentRevocationRequirementsReport",
    "build_source_consent_revocation_requirements",
    "derive_source_consent_revocation_requirements",
    "extract_source_consent_revocation_requirements",
    "generate_source_consent_revocation_requirements",
    "source_consent_revocation_requirements_to_dict",
    "source_consent_revocation_requirements_to_dicts",
    "source_consent_revocation_requirements_to_markdown",
    "summarize_source_consent_revocation_requirements",
]
