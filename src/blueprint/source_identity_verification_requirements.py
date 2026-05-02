"""Extract source-level identity verification requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


IdentityVerificationMethod = Literal[
    "document_upload",
    "selfie_liveness",
    "knowledge_based_verification",
    "phone_email_otp",
    "manual_review",
    "third_party_kyc_provider",
    "re_verification_trigger",
    "retention_rule",
    "failure_handling",
    "accessibility_fallback",
    "audit_evidence",
]
IdentityVerificationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_METHOD_ORDER: tuple[IdentityVerificationMethod, ...] = (
    "document_upload",
    "selfie_liveness",
    "knowledge_based_verification",
    "phone_email_otp",
    "manual_review",
    "third_party_kyc_provider",
    "re_verification_trigger",
    "retention_rule",
    "failure_handling",
    "accessibility_fallback",
    "audit_evidence",
)
_CONFIDENCE_ORDER: dict[IdentityVerificationConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[IdentityVerificationMethod, str] = {
    "document_upload": "Plan accepted document types, capture quality, secure upload, extraction, and rejection reasons.",
    "selfie_liveness": "Plan biometric consent, liveness provider behavior, spoofing controls, and non-biometric alternatives.",
    "knowledge_based_verification": "Define question source, scoring, privacy controls, lockouts, and jurisdiction limits.",
    "phone_email_otp": "Define OTP channel, expiry, resend limits, rate limits, recovery paths, and abuse monitoring.",
    "manual_review": "Assign review queues, reviewer permissions, SLA, escalation, and decision audit trail.",
    "third_party_kyc_provider": "Confirm provider integration, data sharing, webhooks, status mapping, and contract/compliance review.",
    "re_verification_trigger": "Define events that require re-verification, grace periods, user messaging, and access impact.",
    "retention_rule": "Specify retention, deletion, redaction, evidence minimization, and legal hold behavior.",
    "failure_handling": "Define retry, failure states, fallback paths, support escalation, and user-facing messaging.",
    "accessibility_fallback": "Provide accessible alternatives for users who cannot complete camera, document, or OTP flows.",
    "audit_evidence": "Capture verification decisions, actor/system evidence, timestamps, provider references, and tamper-resistant logs.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_VERIFICATION_CONTEXT_RE = re.compile(
    r"\b(?:identity verification|verify identit(?:y|ies)|id verification|kyc|know your customer|"
    r"customer due diligence|cdd|enhanced due diligence|edd|aml onboarding|identity proofing|"
    r"proof of identity|government id|document verification|liveness|selfie verification|"
    r"manual review|verification review|verification fail(?:s|ures?)|re[- ]?verification|"
    r"verify the applicant|verified identity)\b",
    re.I,
)
_AUTH_ONLY_RE = re.compile(
    r"\b(?:login|log in|sign[- ]?in|sso|saml|oidc|oauth|mfa|2fa|totp|passkey|password|"
    r"session|authentication|identity provider|idp|okta|auth0|azure ad)\b",
    re.I,
)
_IDENTITY_PROVIDER_ONLY_RE = re.compile(
    r"\b(?:identity provider|idp|sso provider|oidc provider|saml provider|auth provider|"
    r"okta|auth0|azure ad|entra id)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:identity[_ -]?verification|verification|kyc|aml|cdd|edd|document|selfie|liveness|"
    r"otp|manual[_ -]?review|provider|retention|fallback|accessibility|audit|evidence|"
    r"compliance|privacy|onboarding|requirements?|acceptance|criteria|source[_ -]?payload|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|provide|define|document|record|track|audit|retain|delete|redact|fallback|"
    r"escalate|review|verify|capture|collect|upload|scan|approve|reject|retry|cannot ship|done when|acceptance)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,120}\b(?:identity verification|id verification|kyc|document verification|"
    r"liveness|selfie|manual review|otp verification)\b.{0,80}\b(?:required|needed|in scope|planned|changes?|work)\b|"
    r"\b(?:identity verification|id verification|kyc|document verification|liveness|selfie|manual review|otp verification)\b"
    r".{0,100}\b(?:not required|not needed|out of scope|no changes?|no work|non[- ]?goal)\b",
    re.I,
)
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
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "compliance",
    "privacy",
    "security",
    "operations",
    "identity_verification",
    "kyc",
    "verification",
    "manual_review",
    "metadata",
    "brief_metadata",
    "implementation_notes",
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
    "domain",
    "status",
}
_METHOD_PATTERNS: dict[IdentityVerificationMethod, re.Pattern[str]] = {
    "document_upload": re.compile(
        r"\b(?:document upload|upload (?:a )?(?:document|id)|government[- ]?issued id|government id|"
        r"passport|driver'?s license|driving licence|national id|identity document|id document|"
        r"document scan|document capture|document verification)\b",
        re.I,
    ),
    "selfie_liveness": re.compile(
        r"\b(?:selfie|liveness|life[- ]?ness|face match|facial match|biometric|camera check|"
        r"video verification|proof of life|anti[- ]?spoof)\b",
        re.I,
    ),
    "knowledge_based_verification": re.compile(
        r"\b(?:knowledge[- ]?based|kba|out[- ]of[- ]wallet|security questions?|credit bureau questions?|"
        r"quiz questions?|challenge questions?)\b",
        re.I,
    ),
    "phone_email_otp": re.compile(
        r"\b(?:(?:phone|email|sms|text message)\s+(?:otp|one[- ]time passcode|one[- ]time code|verification code)|"
        r"otp verification|one[- ]time passcode|one[- ]time code|sms code|email code|verify phone|verify email)\b",
        re.I,
    ),
    "manual_review": re.compile(
        r"\b(?:manual review|human review|agent review|review queue|case review|compliance review|"
        r"reviewer|backoffice review|approve or reject|manual approval)\b",
        re.I,
    ),
    "third_party_kyc_provider": re.compile(
        r"\b(?:kyc provider|third[- ]party (?:kyc|verification)|vendor|provider|persona|onfido|jumio|"
        r"trulioo|veriff|stripe identity|alloy|socure|sumsub|webhook|provider status)\b",
        re.I,
    ),
    "re_verification_trigger": re.compile(
        r"\b(?:re[- ]?verification|reverify|re-verify|verify again|periodic review|expired document|"
        r"document expires|risk trigger|name change|address change|suspicious activity|account recovery)\b",
        re.I,
    ),
    "retention_rule": re.compile(
        r"\b(?:retention|retain|delete|deletion|purge|redact|redaction|minimi[sz]e|legal hold|"
        r"ttl|days?|months?|years?|store documents?|storage period)\b",
        re.I,
    ),
    "failure_handling": re.compile(
        r"\b(?:failure|failed verification|verification fails?|retry|resubmit|fallback|fall back|appeal|"
        r"blocked|reject(?:ed|ion)?|decline(?:d)?|support escalation|unable to verify|error state)\b",
        re.I,
    ),
    "accessibility_fallback": re.compile(
        r"\b(?:accessibility|accessible|a11y|screen reader|camera unavailable|no camera|low vision|"
        r"assistive|alternate flow|alternative flow|manual fallback|support-assisted|offline fallback)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|auditable|evidence|decision log|verification log|timestamp|reviewer id|"
        r"provider reference|case id|immutable log|audit trail|consent record)\b",
        re.I,
    ),
}
_FIELD_METHOD_PATTERNS: dict[IdentityVerificationMethod, re.Pattern[str]] = {
    method: re.compile(method.replace("_", r"[_ -]?"), re.I) for method in _METHOD_ORDER
}
_PROVIDER_RE = re.compile(
    r"\b(?:provider|vendor)\s*:?\s*([^.;,\n]*)",
    re.I,
)
_PROVIDER_NAME_RE = re.compile(
    r"\b(persona|onfido|jumio|trulioo|veriff|stripe identity|alloy|socure|sumsub)\b",
    re.I,
)
_RETENTION_RE = re.compile(
    r"\b(?:retain|retention|delete|purge|redact|ttl)\s*:?\s*([^.;\n]*(?:days?|months?|years?|ttl|delete|purge|redact|retain)[^.;\n]*)",
    re.I,
)
_FAILURE_RE = re.compile(
    r"\b(?:failure|failed verification|verification fails?|retry|resubmit|appeal|fallback|support escalation)\s*:?\s*([^.;\n]*)",
    re.I,
)
_TRIGGER_RE = re.compile(
    r"\b(?:re[- ]?verification|reverify|verify again|trigger(?:s)?|when)\s*:?\s*([^.;\n]*(?:expired|change|risk|suspicious|periodic|account recovery|trigger)[^.;\n]*)",
    re.I,
)
_MANUAL_REVIEW_RE = re.compile(
    r"\b(?:manual review|human review|review queue|compliance review)\s*:?\s*([^.;\n]*(?:review|queue|sla|approve|reject|escalat)[^.;\n]*)",
    re.I,
)
_ACCESSIBILITY_RE = re.compile(
    r"\b(?:accessibility|accessible|fallback|alternative flow|manual fallback|support-assisted)\s*:?\s*([^.;\n]*(?:accessib|fallback|alternative|support|camera|screen reader)[^.;\n]*)",
    re.I,
)
_AUDIT_RE = re.compile(
    r"\b(?:audit|evidence|decision log|verification log|audit trail)\s*:?\s*([^.;\n]*(?:audit|evidence|log|timestamp|reviewer|provider|case)[^.;\n]*)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceIdentityVerificationRequirement:
    """One source-backed identity verification requirement."""

    source_brief_id: str | None
    method: IdentityVerificationMethod
    requirement_text: str
    provider: str | None = None
    failure_handling: str | None = None
    fallback: str | None = None
    re_verification_trigger: str | None = None
    retention_rule: str | None = None
    manual_review: str | None = None
    audit_evidence: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: IdentityVerificationConfidence = "medium"
    planning_note: str = ""

    @property
    def requirement_category(self) -> IdentityVerificationMethod:
        """Compatibility alias matching category-oriented reports."""
        return self.method

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "method": self.method,
            "requirement_text": self.requirement_text,
            "provider": self.provider,
            "failure_handling": self.failure_handling,
            "fallback": self.fallback,
            "re_verification_trigger": self.re_verification_trigger,
            "retention_rule": self.retention_rule,
            "manual_review": self.manual_review,
            "audit_evidence": self.audit_evidence,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceIdentityVerificationRequirementsReport:
    """Source-level identity verification requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceIdentityVerificationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def source_brief_id(self) -> str | None:
        """Compatibility alias used by source-brief reports."""
        return self.source_id

    @property
    def records(self) -> tuple[SourceIdentityVerificationRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceIdentityVerificationRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
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
        """Return identity verification requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Identity Verification Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        method_counts = self.summary.get("method_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Method counts: "
            + ", ".join(f"{method} {method_counts.get(method, 0)}" for method in _METHOD_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No identity verification requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Method | Requirement | Provider | Failure Handling | Fallback | Re-verification | Retention | Manual Review | Audit Evidence | Source Field | Matched Terms | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.method)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.provider or '')} | "
                f"{_markdown_cell(requirement.failure_handling or '')} | "
                f"{_markdown_cell(requirement.fallback or '')} | "
                f"{_markdown_cell(requirement.re_verification_trigger or '')} | "
                f"{_markdown_cell(requirement.retention_rule or '')} | "
                f"{_markdown_cell(requirement.manual_review or '')} | "
                f"{_markdown_cell(requirement.audit_evidence or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_identity_verification_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceIdentityVerificationRequirementsReport:
    """Extract source-level identity verification requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceIdentityVerificationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_identity_verification_requirements(source: Any) -> SourceIdentityVerificationRequirementsReport:
    """Compatibility alias for building an identity verification requirements report."""
    return build_source_identity_verification_requirements(source)


def generate_source_identity_verification_requirements(source: Any) -> SourceIdentityVerificationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_identity_verification_requirements(source)


def derive_source_identity_verification_requirements(source: Any) -> SourceIdentityVerificationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_identity_verification_requirements(source)


def summarize_source_identity_verification_requirements(source_or_result: Any) -> dict[str, Any]:
    """Return deterministic counts for extracted identity verification requirements."""
    if isinstance(source_or_result, SourceIdentityVerificationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_identity_verification_requirements(source_or_result).summary


def source_identity_verification_requirements_to_dict(
    report: SourceIdentityVerificationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an identity verification requirements report to a plain dictionary."""
    return report.to_dict()


source_identity_verification_requirements_to_dict.__test__ = False


def source_identity_verification_requirements_to_dicts(
    requirements: (
        tuple[SourceIdentityVerificationRequirement, ...]
        | list[SourceIdentityVerificationRequirement]
        | SourceIdentityVerificationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize identity verification requirement records to dictionaries."""
    if isinstance(requirements, SourceIdentityVerificationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_identity_verification_requirements_to_dicts.__test__ = False


def source_identity_verification_requirements_to_markdown(
    report: SourceIdentityVerificationRequirementsReport,
) -> str:
    """Render an identity verification requirements report as Markdown."""
    return report.to_markdown()


source_identity_verification_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    method: IdentityVerificationMethod
    requirement_text: str
    provider: str | None
    failure_handling: str | None
    fallback: str | None
    re_verification_trigger: str | None
    retention_rule: str | None
    manual_review: str | None
    audit_evidence: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: IdentityVerificationConfidence


def _source_payloads(source: Any) -> list[tuple[str | None, dict[str, Any]]]:
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
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_RE.search(searchable) or _is_auth_only(segment):
                continue
            for method in _methods(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        method=method,
                        requirement_text=_requirement_text(segment.text),
                        provider=_provider_field_detail("provider", segment.text)
                        or _provider_field_detail("kyc_provider", segment.text)
                        or _match_whole(_PROVIDER_NAME_RE, segment.text)
                        or _match_detail(_PROVIDER_RE, segment.text),
                        failure_handling=_field_value_detail("failure_handling", segment.text)
                        or _field_value_detail("failure", segment.text)
                        or _match_detail(_FAILURE_RE, segment.text),
                        fallback=_field_value_detail("fallback", segment.text)
                        or _field_value_detail("accessibility_fallback", segment.text)
                        or _match_detail(_ACCESSIBILITY_RE, segment.text),
                        re_verification_trigger=_field_value_detail("re_verification_trigger", segment.text)
                        or _field_value_detail("reverification_trigger", segment.text)
                        or _match_detail(_TRIGGER_RE, segment.text),
                        retention_rule=_field_value_detail("retention_rule", segment.text)
                        or _field_value_detail("retention", segment.text)
                        or _match_detail(_RETENTION_RE, segment.text),
                        manual_review=_field_value_detail("manual_review", segment.text)
                        or _match_detail(_MANUAL_REVIEW_RE, segment.text),
                        audit_evidence=_field_value_detail("audit_evidence", segment.text)
                        or _field_value_detail("evidence", segment.text)
                        or _match_detail(_AUDIT_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(method, searchable),
                        confidence=_confidence(method, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceIdentityVerificationRequirement]:
    grouped: dict[tuple[str | None, IdentityVerificationMethod, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.method, _dedupe_requirement_key(candidate.requirement_text)),
            [],
        ).append(candidate)

    requirements: list[SourceIdentityVerificationRequirement] = []
    for (_source_brief_id, method, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceIdentityVerificationRequirement(
                source_brief_id=best.source_brief_id,
                method=method,
                requirement_text=best.requirement_text,
                provider=_first_detail(item.provider for item in items),
                failure_handling=_first_detail(item.failure_handling for item in items),
                fallback=_first_detail(item.fallback for item in items),
                re_verification_trigger=_first_detail(item.re_verification_trigger for item in items),
                retention_rule=_first_detail(item.retention_rule for item in items),
                manual_review=_first_detail(item.manual_review for item in items),
                audit_evidence=_first_detail(item.audit_evidence for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[method],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _METHOD_ORDER.index(requirement.method),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
            requirement.evidence,
        ),
    )


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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            for evidence in _structured_segments(value):
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _VERIFICATION_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_VERIFICATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _VERIFICATION_CONTEXT_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _methods(segment: _Segment) -> tuple[IdentityVerificationMethod, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_verification_context = bool(_VERIFICATION_CONTEXT_RE.search(searchable))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    if not (has_verification_context or has_structured_context):
        return ()
    if not (_REQUIREMENT_RE.search(searchable) or has_structured_context):
        return ()
    field_methods = [method for method in _METHOD_ORDER if _FIELD_METHOD_PATTERNS[method].search(field_words)]
    text_methods = [method for method in _METHOD_ORDER if _METHOD_PATTERNS[method].search(segment.text)]
    if "third_party_kyc_provider" in text_methods and not has_verification_context and not re.search(r"\b(?:kyc|verification|identity)\b", searchable, re.I):
        text_methods.remove("third_party_kyc_provider")
    return tuple(_dedupe(field_methods + text_methods))


def _confidence(method: IdentityVerificationMethod, segment: _Segment) -> IdentityVerificationConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_method = bool(_METHOD_PATTERNS[method].search(segment.text) or _FIELD_METHOD_PATTERNS[method].search(field_words))
    detail_count = sum(
        1
        for value in (
            _match_detail(_PROVIDER_RE, searchable),
            _match_detail(_FAILURE_RE, segment.text),
            _match_detail(_RETENTION_RE, segment.text),
            _match_detail(_MANUAL_REVIEW_RE, segment.text),
            _match_detail(_AUDIT_RE, segment.text),
        )
        if value
    )
    if has_method and has_explicit_requirement and has_structured_context and detail_count >= 1:
        return "high"
    if has_method and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceIdentityVerificationRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "method_counts": {
            method: sum(1 for requirement in requirements if requirement.method == method)
            for method in _METHOD_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "methods": [
            method
            for method in _METHOD_ORDER
            if any(requirement.method == method for requirement in requirements)
        ],
        "requires_document_upload": any(requirement.method == "document_upload" for requirement in requirements),
        "requires_liveness_check": any(requirement.method == "selfie_liveness" for requirement in requirements),
        "requires_otp": any(requirement.method == "phone_email_otp" for requirement in requirements),
        "requires_manual_review": any(requirement.method == "manual_review" for requirement in requirements),
        "requires_kyc_provider": any(requirement.method == "third_party_kyc_provider" for requirement in requirements),
        "requires_re_verification": any(requirement.method == "re_verification_trigger" for requirement in requirements),
        "requires_retention_rule": any(requirement.method == "retention_rule" or requirement.retention_rule for requirement in requirements),
        "requires_failure_handling": any(requirement.method == "failure_handling" or requirement.failure_handling for requirement in requirements),
        "requires_accessibility_fallback": any(requirement.method == "accessibility_fallback" or requirement.fallback for requirement in requirements),
        "requires_audit_evidence": any(requirement.method == "audit_evidence" or requirement.audit_evidence for requirement in requirements),
        "status": "ready_for_identity_verification_planning" if requirements else "no_identity_verification_language",
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "method",
            "requirement_category",
            "document_upload",
            "document",
            "selfie",
            "liveness",
            "kba",
            "otp",
            "manual_review",
            "provider",
            "kyc_provider",
            "re_verification_trigger",
            "retention",
            "retention_rule",
            "failure",
            "failure_handling",
            "fallback",
            "accessibility_fallback",
            "audit",
            "audit_evidence",
        }
    )


def _structured_segments(item: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return ["; ".join(parts)] if parts else []


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
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "compliance",
        "privacy",
        "security",
        "operations",
        "identity_verification",
        "kyc",
        "verification",
        "manual_review",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_source_index(candidate.source_field),
    )


def _is_auth_only(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    if _IDENTITY_PROVIDER_ONLY_RE.search(searchable) and not re.search(
        r"\b(?:kyc|know your customer|identity verification|id verification|document verification|"
        r"manual review|liveness|selfie|persona|onfido|jumio|trulioo|veriff|stripe identity|alloy|socure|sumsub)\b",
        searchable,
        re.I,
    ):
        return True
    if _VERIFICATION_CONTEXT_RE.search(searchable):
        return False
    if any(pattern.search(segment.text) for pattern in _METHOD_PATTERNS.values()):
        return False
    return bool(_IDENTITY_PROVIDER_ONLY_RE.search(searchable) or (_AUTH_ONLY_RE.search(searchable) and not segment.section_context))


def _matched_terms(method: IdentityVerificationMethod, text: str) -> tuple[str, ...]:
    terms = [match.group(0).casefold() for match in _METHOD_PATTERNS[method].finditer(text)]
    return tuple(sorted(_dedupe(_clean_text(term) for term in terms), key=str.casefold))


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _match_whole(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return _clean_text(match.group(1) if match.groups() else match.group(0)).rstrip(".").casefold()


def _field_value_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;]+)", re.I)
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _provider_field_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;,.\n]+)", re.I)
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _first_detail(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _source_index(source_field: str) -> int:
    match = re.search(r"\[(\d+)\]", source_field)
    return int(match.group(1)) if match else 0


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
    text = _clean_text(value)
    return [text] if text else []


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
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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


def _dedupe_requirement_key(value: str) -> str:
    text = _clean_text(value).casefold()
    return _SPACE_RE.sub(" ", text).strip()


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
    "IdentityVerificationConfidence",
    "IdentityVerificationMethod",
    "SourceIdentityVerificationRequirement",
    "SourceIdentityVerificationRequirementsReport",
    "build_source_identity_verification_requirements",
    "derive_source_identity_verification_requirements",
    "extract_source_identity_verification_requirements",
    "generate_source_identity_verification_requirements",
    "source_identity_verification_requirements_to_dict",
    "source_identity_verification_requirements_to_dicts",
    "source_identity_verification_requirements_to_markdown",
    "summarize_source_identity_verification_requirements",
]
