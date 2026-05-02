"""Extract source-level account lockout requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AccountLockoutRequirementType = Literal[
    "failed_attempt_threshold",
    "temporary_lockout",
    "captcha_or_step_up",
    "unlock_flow",
    "admin_unlock",
    "notification",
    "audit_evidence",
]
AccountLockoutConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[AccountLockoutRequirementType, ...] = (
    "failed_attempt_threshold",
    "temporary_lockout",
    "captcha_or_step_up",
    "unlock_flow",
    "admin_unlock",
    "notification",
    "audit_evidence",
)
_CONFIDENCE_ORDER: dict[AccountLockoutConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_MISSING_DETAIL_GUIDANCE: dict[AccountLockoutRequirementType, str] = {
    "failed_attempt_threshold": "Confirm the failed-login count, counting window, reset behavior, and whether thresholds vary by role or tenant.",
    "temporary_lockout": "Confirm lockout duration, retry-after messaging, backoff rules, and support impact.",
    "captcha_or_step_up": "Confirm CAPTCHA or step-up trigger conditions, provider behavior, accessibility fallback, and bypass rules.",
    "unlock_flow": "Confirm self-service unlock verification, token expiry, rate limits, and recovery edge cases.",
    "admin_unlock": "Confirm which admin roles can override lockout, approval requirements, and audit logging.",
    "notification": "Confirm who is notified, delivery channels, templates, timing, and suppression rules.",
    "audit_evidence": "Confirm security event schema, retention, export/reporting needs, and evidence ownership.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_LOCKOUT_CONTEXT_RE = re.compile(
    r"\b(?:account lock(?:out|ed)|lock(?:ed)? out|login lock(?:out)?|failed logins?|"
    r"failed sign[- ]?ins?|failed attempts?|password attempts?|brute force|credential stuffing|"
    r"throttl(?:e|ed|ing)|rate limit(?:ed|ing)?|retry limit|captcha|step[- ]up|unlock|"
    r"unlock flow|admin unlock|support unlock|security notification|lockout notification|"
    r"suspicious login|authentication event|security event|audit evidence)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:lockout|lock[_ -]?out|failed[_ -]?(?:login|signin|sign[_ -]?in|attempt)|"
    r"attempt[_ -]?threshold|authentication|auth|security|brute[_ -]?force|throttle|"
    r"captcha|step[_ -]?up|unlock|notification|audit|evidence|policy|requirements?|"
    r"acceptance|definition[_ -]?of[_ -]?done|metadata|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"enforce|lock|throttle|rate[- ]?limit|challenge|prompt|show|send|notify|"
    r"allow|support|provide|enable|disable|override|unlock|log|audit|record|export|"
    r"capture|policy|acceptance|done when|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:lockout|lock out|failed login|failed sign-in|failed attempts?|throttl|captcha|unlock|admin unlock)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:lockout|lock out|failed login|failed sign-in|failed attempts?|throttl|captcha|unlock|admin unlock)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:failed\s*)?(?:attempts?|logins?|sign[- ]?ins?)|after\s+\d+|"
    r"\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|temporary|permanent|cooldown|"
    r"captcha|step[- ]up|email link|self[- ]service|support|admin|security event|audit log|"
    r"notification|email|sms|in[- ]app|webhook|export|report)\b",
    re.I,
)
_SUBJECT_RE = re.compile(
    r"\b(?:for|to|applies to|required for|enforced for|notify)\s+(?:the\s+)?"
    r"((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|enterprise users?|privileged users?|locked users?|affected users?)[^.;,\n]*)",
    re.I,
)
_SUBJECT_PREFIX_RE = re.compile(
    r"^\s*((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|enterprise users?|privileged users?|locked users?|affected users?))\b"
    r"\s+(?:must|shall|required|requires?|need|needs|should|can|may)\b",
    re.I,
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
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "authentication",
    "auth_requirements",
    "security",
    "lockout",
    "account_lockout",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_TYPE_PATTERNS: dict[AccountLockoutRequirementType, re.Pattern[str]] = {
    "failed_attempt_threshold": re.compile(
        r"\b(?:(?:failed\s*)?(?:attempt|login|sign[- ]?in)s?\s*(?:threshold|limit|count)|"
        r"after\s+\d+\s*(?:failed\s*)?(?:attempts?|logins?|sign[- ]?ins?)|"
        r"\d+\s*(?:failed\s*)?(?:attempts?|logins?|sign[- ]?ins?))\b",
        re.I,
    ),
    "temporary_lockout": re.compile(
        r"\b(?:temporary lock(?:out)?|cooldown|"
        r"retry[- ]?after|lockout duration|(?:for\s+)?\d+\s*(?:minutes?|mins?|hours?|hrs?|days?))\b",
        re.I,
    ),
    "captcha_or_step_up": re.compile(
        r"\b(?:captcha|recaptcha|hcaptcha|step[- ]up|additional challenge|challenge users?|risk[- ]based challenge)\b",
        re.I,
    ),
    "unlock_flow": re.compile(
        r"\b(?:unlock flow|self[- ]service unlock|unlock link|unlock email|unlock token|"
        r"recover access|account unlock|unlock their account|password reset unlock)\b",
        re.I,
    ),
    "admin_unlock": re.compile(
        r"\b(?:admin unlock|administrator unlock|support unlock|admin override|manual unlock|"
        r"override lockout|unlock locked users?|support agents? can unlock)\b",
        re.I,
    ),
    "notification": re.compile(
        r"\b(?:notify|notification|alert|email|sms|in[- ]app|webhook|send .*lockout|"
        r"security notification|lockout notification|suspicious login notice)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|logged|logging|evidence|security event|auth(?:entication)? event|"
        r"report|export|attestation|compliance|proof|retention)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceAccountLockoutRequirement:
    """One source-backed account lockout requirement."""

    source_brief_id: str | None
    requirement_type: AccountLockoutRequirementType
    value: str | None = None
    subject: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    confidence: AccountLockoutConfidence = "medium"
    missing_detail_guidance: str | None = None

    @property
    def category(self) -> AccountLockoutRequirementType:
        """Compatibility view for extractors that expose category naming."""
        return self.requirement_type

    @property
    def requirement_category(self) -> AccountLockoutRequirementType:
        """Compatibility view for extractors that expose requirement_category naming."""
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "value": self.value,
            "subject": self.subject,
            "evidence": list(self.evidence),
            "source_field": self.source_field,
            "confidence": self.confidence,
            "missing_detail_guidance": self.missing_detail_guidance,
        }


@dataclass(frozen=True, slots=True)
class SourceAccountLockoutRequirementsReport:
    """Source-level account lockout requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAccountLockoutRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAccountLockoutRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAccountLockoutRequirement, ...]:
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
        """Return account lockout requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Account Lockout Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(f"{requirement_type} {type_counts.get(requirement_type, 0)}" for requirement_type in _TYPE_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source account lockout requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Requirement Type | Value | Subject | Source Field | Confidence | Missing Detail Guidance | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{_markdown_cell(requirement.subject or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.missing_detail_guidance or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_account_lockout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountLockoutRequirementsReport:
    """Extract source-level account lockout requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAccountLockoutRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_account_lockout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountLockoutRequirementsReport:
    """Compatibility alias for building an account lockout requirements report."""
    return build_source_account_lockout_requirements(source)


def generate_source_account_lockout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountLockoutRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_account_lockout_requirements(source)


def derive_source_account_lockout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAccountLockoutRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_account_lockout_requirements(source)


def summarize_source_account_lockout_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAccountLockoutRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted account lockout requirements."""
    if isinstance(source_or_result, SourceAccountLockoutRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_account_lockout_requirements(source_or_result).summary


def source_account_lockout_requirements_to_dict(
    report: SourceAccountLockoutRequirementsReport,
) -> dict[str, Any]:
    """Serialize an account lockout requirements report to a plain dictionary."""
    return report.to_dict()


source_account_lockout_requirements_to_dict.__test__ = False


def source_account_lockout_requirements_to_dicts(
    requirements: (
        tuple[SourceAccountLockoutRequirement, ...]
        | list[SourceAccountLockoutRequirement]
        | SourceAccountLockoutRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize account lockout requirement records to dictionaries."""
    if isinstance(requirements, SourceAccountLockoutRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_account_lockout_requirements_to_dicts.__test__ = False


def source_account_lockout_requirements_to_markdown(
    report: SourceAccountLockoutRequirementsReport,
) -> str:
    """Render an account lockout requirements report as Markdown."""
    return report.to_markdown()


source_account_lockout_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: AccountLockoutRequirementType
    value: str | None
    subject: str | None
    source_field: str
    evidence: str
    confidence: AccountLockoutConfidence


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
            if _NEGATED_SCOPE_RE.search(searchable) or not _is_requirement(segment):
                continue
            requirement_types = [
                requirement_type
                for requirement_type in _TYPE_ORDER
                if _TYPE_PATTERNS[requirement_type].search(searchable)
            ]
            if not requirement_types and re.search(r"\block(?:out|ed)?\b", searchable, re.I):
                requirement_types = ["temporary_lockout"]
            for requirement_type in _dedupe(requirement_types):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        value=_value(requirement_type, segment.text),
                        subject=_subject(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, requirement_type),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAccountLockoutRequirement]:
    grouped: dict[tuple[str | None, AccountLockoutRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(candidate)

    requirements: list[SourceAccountLockoutRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceAccountLockoutRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                value=_joined_details(item.value for item in items),
                subject=_joined_details(item.subject for item in items),
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                source_field=best.source_field,
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                missing_detail_guidance=_MISSING_DETAIL_GUIDANCE[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _TYPE_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_lockout_context(payload)
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
        if _has_structured_shape(value):
            evidence = _structured_text(value)
            if evidence:
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _LOCKOUT_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _TYPE_PATTERNS.values())
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
            section_context = inherited_context or bool(_LOCKOUT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_context = bool(
        _LOCKOUT_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )
    if not has_context or not any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values()):
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _VALUE_RE.search(searchable)
    )


def _confidence(segment: _Segment, requirement_type: AccountLockoutRequirementType) -> AccountLockoutConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_value = bool(_VALUE_RE.search(searchable) or _value(requirement_type, segment.text))
    has_specific_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "authentication",
                "auth",
                "security",
                "lockout",
                "metadata",
            )
        )
    )
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context and has_value:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) or has_specific_context or has_value:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceAccountLockoutRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(1 for requirement in requirements if requirement.requirement_type == requirement_type)
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_account_lockout_planning" if requirements else "no_account_lockout_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_SCOPE_RE.search(scoped_text))


def _brief_lockout_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_LOCKOUT_CONTEXT_RE.search(scoped_text) and not _NEGATED_SCOPE_RE.search(scoped_text))


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "requirement_type",
            "type",
            "threshold",
            "attempts",
            "duration",
            "lockout_duration",
            "subject",
            "audience",
            "captcha",
            "step_up",
            "unlock",
            "admin_unlock",
            "notification",
            "audit",
            "evidence",
            "policy",
        }
    )


def _structured_text(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts)


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
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "authentication",
        "auth_requirements",
        "security",
        "lockout",
        "account_lockout",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _value(requirement_type: AccountLockoutRequirementType, text: str) -> str | None:
    patterns: dict[AccountLockoutRequirementType, re.Pattern[str]] = {
        "failed_attempt_threshold": re.compile(
            r"\b(?:after\s+\d+\s*(?:failed\s*)?(?:attempts?|logins?|sign[- ]?ins?)|\d+\s*(?:failed\s*)?(?:attempts?|logins?|sign[- ]?ins?))\b",
            re.I,
        ),
        "temporary_lockout": re.compile(
            r"\b(?:\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|temporary|cooldown|retry[- ]?after|permanent)\b",
            re.I,
        ),
        "captcha_or_step_up": re.compile(r"\b(?:captcha|recaptcha|hcaptcha|step[- ]up|additional challenge)\b", re.I),
        "unlock_flow": re.compile(r"\b(?:self[- ]service unlock|unlock link|unlock email|email link|password reset|support recovery)\b", re.I),
        "admin_unlock": re.compile(r"\b(?:admin override|admin unlock|support unlock|manual unlock|support agents?)\b", re.I),
        "notification": re.compile(r"\b(?:email|sms|in[- ]app|webhook|notification|alert)\b", re.I),
        "audit_evidence": re.compile(r"\b(?:audit log|security event|authentication event|export|report|retention|evidence)\b", re.I),
    }
    if match := re.search(rf"(?:^|;\s*)(?:{requirement_type}|value|threshold|duration|channel|evidence)\s*:\s*([^;]+)", text, re.I):
        return _detail(match.group(1))
    match = patterns[requirement_type].search(text)
    return _detail(match.group(0)) if match else None


def _subject(text: str) -> str | None:
    if match := re.search(r"(?:^|;\s*)(?:subject|audience|role)\s*:\s*([^;]+)", text, re.I):
        return _detail(match.group(1))
    return _match_detail(_SUBJECT_RE, text) or _match_detail(_SUBJECT_PREFIX_RE, text)


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        group = match.group(1) if match.lastindex else match.group(0)
        return _detail(group)
    return None


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, int, str]:
    detail_count = sum(bool(value) for value in (candidate.value, candidate.subject))
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_field_rank(candidate.source_field),
        candidate.evidence,
    )


def _field_rank(source_field: str) -> int:
    if match := re.search(r"\[(\d+)\]", source_field):
        return int(match.group(1))
    return 0


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
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


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


def _detail(value: Any) -> str | None:
    text = _clean_text(value).strip("`'\" ;,.")
    return text[:160].rstrip() if text else None


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
    "AccountLockoutConfidence",
    "AccountLockoutRequirementType",
    "SourceAccountLockoutRequirement",
    "SourceAccountLockoutRequirementsReport",
    "build_source_account_lockout_requirements",
    "derive_source_account_lockout_requirements",
    "extract_source_account_lockout_requirements",
    "generate_source_account_lockout_requirements",
    "source_account_lockout_requirements_to_dict",
    "source_account_lockout_requirements_to_dicts",
    "source_account_lockout_requirements_to_markdown",
    "summarize_source_account_lockout_requirements",
]
