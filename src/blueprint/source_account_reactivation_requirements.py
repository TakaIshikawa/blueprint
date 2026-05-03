"""Extract source-level account reactivation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AccountReactivationCategory = Literal[
    "reactivation_eligibility",
    "identity_verification",
    "billing_status_checks",
    "entitlement_restoration",
    "audit_trail",
    "support_handoff",
    "customer_notification",
    "abuse_fraud_review",
]
AccountReactivationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[AccountReactivationCategory, ...] = (
    "reactivation_eligibility",
    "identity_verification",
    "billing_status_checks",
    "entitlement_restoration",
    "audit_trail",
    "support_handoff",
    "customer_notification",
    "abuse_fraud_review",
)
_CONFIDENCE_ORDER: dict[AccountReactivationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REACTIVATION_CONTEXT_RE = re.compile(
    r"\b(?:account reactivation|reactivat(?:e|es|ed|ing|ion)|restore account|restore access|"
    r"reopen account|resume account|unlock dormant account|closed account|disabled account|"
    r"suspended account|dormant account|inactive account|deleted account|grace period|"
    r"eligibility|eligible|cooldown|retention window|identity verification|verify identity|"
    r"email verification|mfa|2fa|step[- ]up|billing status|past due|unpaid invoice|payment method|"
    r"subscription status|entitlement|entitlements|restore permissions?|restore workspace|"
    r"restore data|audit trail|audit log|reactivation event|support handoff|support ticket|"
    r"manual review|agent review|customer notification|confirmation email|notify|notification|"
    r"abuse|fraud|risk review|account takeover|ato|chargeback|trust and safety)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:reactivat|reactivation|reopen|restore|resume|dormant|inactive|closed|disabled|suspended|"
    r"eligibility|eligible|grace|retention|identity|verification|mfa|2fa|billing|invoice|payment|"
    r"subscription|entitlement|permission|access|audit|evidence|support|handoff|ticket|agent|"
    r"notification|notify|email|abuse|fraud|risk|trust|safety|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|send|email|verify|challenge|restore|reactivate|reopen|resume|"
    r"check|validate|block|hold|gate|charge|bill|invoice|settle|reinstate|grant|revoke|"
    r"audit|log|record|track|notify|review|approve|escalate|handoff|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:account reactivation|reactivation|reactivate accounts?|restore accounts?|reopen accounts?|"
    r"identity verification|billing status|entitlement restoration|reactivation audit|support handoff|"
    r"reactivation notifications?|abuse review|fraud review)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:account reactivation|reactivation|reactivate accounts?|restore accounts?|reopen accounts?|"
    r"identity verification|billing status|entitlement restoration|reactivation audit|support handoff|"
    r"reactivation notifications?|abuse review|fraud review)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_ACCOUNT_RE = re.compile(
    r"\b(?:no account reactivation|account reactivation is out of scope|reactivation is out of scope|"
    r"without account reactivation|no reactivate account flow|reactivation flows? are excluded|"
    r"non[- ]?goal:?\s+account reactivation)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|\d+\s*failed attempts?|"
    r"(?:grace period|retention window|cooldown|verified email|identity document|mfa|2fa|step[- ]up|"
    r"paid invoice|past due|unpaid invoice|valid payment method|subscription status|"
    r"entitlements?|permissions?|roles?|workspace access|audit log|security event|support ticket|"
    r"confirmation email|in[- ]?app|sms|push|risk review|fraud review|manual review|chargeback))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:hours?|days?|weeks?|months?)\b", re.I)
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
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
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
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "non_goals",
    "assumptions",
    "authentication",
    "auth_requirements",
    "security",
    "account_reactivation",
    "reactivation",
    "lifecycle",
    "identity",
    "billing",
    "entitlements",
    "support",
    "notifications",
    "audit",
    "risk",
    "fraud",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[AccountReactivationCategory, re.Pattern[str]] = {
    "reactivation_eligibility": re.compile(
        r"\b(?:reactivation eligibility|eligible to reactivate|reactivat(?:e|ion).{0,50}eligible|"
        r"closed account.{0,50}reactivat|disabled account.{0,50}reactivat|suspended account.{0,50}reactivat|"
        r"dormant account.{0,50}reactivat|inactive account.{0,50}reactivat|retention window|"
        r"grace period|cooldown|within \d+\s*(?:hours?|days?|weeks?|months?)|before deletion|"
        r"account can be restored|restore account eligibility|reopen account)\b",
        re.I,
    ),
    "identity_verification": re.compile(
        r"\b(?:identity verification|verify identity|verified email|email verification|confirm email|"
        r"mfa|2fa|multi[- ]?factor|step[- ]up|security challenge|password reset before reactivation|"
        r"government id|identity document|account ownership|ownership verification)\b",
        re.I,
    ),
    "billing_status_checks": re.compile(
        r"\b(?:billing status|payment status|subscription status|past due|unpaid invoices?|"
        r"outstanding balance|settle balance|valid payment method|payment method|billing hold|"
        r"chargeback|invoice paid|paid invoice|reactivat(?:e|ion).{0,60}billing)\b",
        re.I,
    ),
    "entitlement_restoration": re.compile(
        r"\b(?:entitlement restoration|restore entitlements?|restore access|restore permissions?|"
        r"reinstate permissions?|reinstate access|workspace access|project access|roles? restored|"
        r"feature flags? restored|subscription entitlements?|data restoration|restore data)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audited|logged|logging|reactivation events?|account restored event|"
        r"security events?|record reactivation|track reactivation|reactivation timestamp|actor and timestamp|"
        r"evidence)\b",
        re.I,
    ),
    "support_handoff": re.compile(
        r"\b(?:support handoff|support ticket|support queue|help desk|agent review|manual review|"
        r"support assisted|support[- ]assisted|escalat(?:e|ion)|manual approval|trust support)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|notify customer|send.{0,50}(?:email|notice|notification)|"
        r"confirmation email|reactivation email|account restored email|in[- ]?app notification|"
        r"sms|push notification|webhook|notify account owner)\b",
        re.I,
    ),
    "abuse_fraud_review": re.compile(
        r"\b(?:abuse review|fraud review|risk review|trust and safety|abuse prevention|fraud prevention|"
        r"account takeover|ato|suspicious activity|chargeback risk|blocked for abuse|manual risk review)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[AccountReactivationCategory, tuple[str, ...]] = {
    "reactivation_eligibility": ("identity", "lifecycle_product"),
    "identity_verification": ("identity", "security"),
    "billing_status_checks": ("billing", "finance"),
    "entitlement_restoration": ("authorization", "backend"),
    "audit_trail": ("security", "compliance"),
    "support_handoff": ("support", "trust_and_safety"),
    "customer_notification": ("lifecycle_messaging", "support"),
    "abuse_fraud_review": ("trust_and_safety", "security"),
}
_PLAN_IMPACTS: dict[AccountReactivationCategory, tuple[str, ...]] = {
    "reactivation_eligibility": ("Define which inactive, closed, suspended, or dormant accounts can be reactivated and within what window.",),
    "identity_verification": ("Specify ownership checks, MFA or email challenges, and identity proofing before restoring access.",),
    "billing_status_checks": ("Gate reactivation on invoices, balances, payment method status, subscription state, and chargebacks.",),
    "entitlement_restoration": ("Restore roles, permissions, data access, workspace membership, and feature entitlements consistently.",),
    "audit_trail": ("Record reactivation actors, timestamps, reasons, source channel, and security events.",),
    "support_handoff": ("Route blocked or manual reactivation cases to support with context and approval states.",),
    "customer_notification": ("Notify account owners and affected users when reactivation succeeds, fails, or needs action.",),
    "abuse_fraud_review": ("Apply risk, fraud, abuse, and account-takeover review before reinstating restricted accounts.",),
}


@dataclass(frozen=True, slots=True)
class SourceAccountReactivationRequirement:
    """One source-backed account reactivation requirement."""

    category: AccountReactivationCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: AccountReactivationConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> AccountReactivationCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> AccountReactivationCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceAccountReactivationRequirementsReport:
    """Source-level account reactivation requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAccountReactivationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAccountReactivationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAccountReactivationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return account reactivation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Account Reactivation Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
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
            lines.extend(["", "No source account reactivation requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_account_reactivation_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAccountReactivationRequirementsReport:
    """Build a account reactivation requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = () if _has_global_no_scope(payload) else tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceAccountReactivationRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_account_reactivation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAccountReactivationRequirementsReport
        | str
        | object
    ),
) -> SourceAccountReactivationRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceAccountReactivationRequirementsReport):
        return dict(source.summary)
    return build_source_account_reactivation_requirements(source)


def derive_source_account_reactivation_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAccountReactivationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_account_reactivation_requirements(source)


def generate_source_account_reactivation_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAccountReactivationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_account_reactivation_requirements(source)


def extract_source_account_reactivation_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAccountReactivationRequirement, ...]:
    """Return account reactivation requirement records from brief-shaped input."""
    return build_source_account_reactivation_requirements(source).requirements


def source_account_reactivation_requirements_to_dict(report: SourceAccountReactivationRequirementsReport) -> dict[str, Any]:
    """Serialize a account reactivation requirements report to a plain dictionary."""
    return report.to_dict()


source_account_reactivation_requirements_to_dict.__test__ = False


def source_account_reactivation_requirements_to_dicts(
    requirements: (
        tuple[SourceAccountReactivationRequirement, ...]
        | list[SourceAccountReactivationRequirement]
        | SourceAccountReactivationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize account reactivation requirement records to dictionaries."""
    if isinstance(requirements, SourceAccountReactivationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_account_reactivation_requirements_to_dicts.__test__ = False


def source_account_reactivation_requirements_to_markdown(report: SourceAccountReactivationRequirementsReport) -> str:
    """Render a account reactivation requirements report as Markdown."""
    return report.to_markdown()


source_account_reactivation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: AccountReactivationCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: AccountReactivationConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAccountReactivationRequirement]:
    grouped: dict[AccountReactivationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAccountReactivationRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceAccountReactivationRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    1 if "same" in item.source_field.casefold() else 0,
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                suggested_plan_impacts=_PLAN_IMPACTS[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
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
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _REACTIVATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
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
            section_context = inherited_context or bool(
                _REACTIVATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _REACTIVATION_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_REACTIVATION_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _REACTIVATION_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:eligible|verified|challenged|checked|validated|paid|settled|restored|"
            r"reinstated|reactivated|reopened|resumed|audited|logged|notified|reviewed|escalated)\b",
            segment.text,
            re.I,
        )
    )


def _value(category: AccountReactivationCategory, text: str) -> str | None:
    if category == "reactivation_eligibility":
        if match := re.search(r"\b(?P<value>\d+\s*(?:hours?|days?|weeks?|months?)|grace period|retention window|cooldown|before deletion)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "identity_verification":
        if match := re.search(r"\b(?P<value>verified email|email verification|mfa|2fa|multi[- ]?factor|step[- ]?up|identity document|security challenge)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "billing_status_checks":
        if match := re.search(r"\b(?P<value>past due|unpaid invoices?|outstanding balance|valid payment method|payment method|paid invoice|chargeback|subscription status)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "entitlement_restoration":
        if match := re.search(r"\b(?P<value>permissions?|roles?|workspace access|project access|feature flags?|data restoration|restore data)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>entitlements?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "audit_trail":
        if match := re.search(r"\b(?P<value>audit log|audit trail|reactivation events?|security events?|actor and timestamp|evidence)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "support_handoff":
        if match := re.search(r"\b(?P<value>support ticket|support queue|agent review|manual review|manual approval|escalation)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "customer_notification":
        if match := re.search(r"\b(?P<value>confirmation email|reactivation email|email|in[- ]?app|sms|push notification|webhook)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "abuse_fraud_review":
        if match := re.search(r"\b(?P<value>abuse review|fraud review|risk review|trust and safety|account takeover|ato|chargeback risk|manual risk review)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            0 if re.search(r"\d", value) else 1,
            0 if _VALUE_RE.search(value) or _DURATION_RE.search(value) else 1,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> AccountReactivationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "billing",
                "authorization",
                "security",
                "reactivation",
                "identity",
                "entitlement",
                "support",
                "audit",
                "notification",
                "risk",
                "fraud",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _REACTIVATION_CONTEXT_RE.search(searchable):
        return "medium"
    if _REACTIVATION_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceAccountReactivationRequirement, ...]) -> dict[str, Any]:
    return {
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
        "status": "ready_for_planning" if requirements else "no_account_reactivation_language",
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
        "mvp_goal",
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
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
        "account_reactivation",
        "reactivation",
        "identity",
        "billing",
        "entitlements",
        "support",
        "notifications",
        "audit",
        "risk",
        "fraud",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: AccountReactivationCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[AccountReactivationCategory, tuple[str, ...]] = {
        "reactivation_eligibility": ("eligibility", "eligible", "grace", "retention", "cooldown"),
        "identity_verification": ("identity", "verification", "mfa", "2fa", "challenge"),
        "billing_status_checks": ("billing", "invoice", "payment", "subscription", "balance"),
        "entitlement_restoration": ("entitlement", "restore", "permission", "access", "role"),
        "audit_trail": ("audit", "evidence", "event", "log", "timestamp"),
        "support_handoff": ("support", "handoff", "ticket", "agent", "escalation"),
        "customer_notification": ("notification", "notify", "email", "sms", "push"),
        "abuse_fraud_review": ("abuse", "fraud", "risk", "trust", "safety"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


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
    "AccountReactivationCategory",
    "AccountReactivationConfidence",
    "SourceAccountReactivationRequirement",
    "SourceAccountReactivationRequirementsReport",
    "build_source_account_reactivation_requirements",
    "derive_source_account_reactivation_requirements",
    "extract_source_account_reactivation_requirements",
    "generate_source_account_reactivation_requirements",
    "summarize_source_account_reactivation_requirements",
    "source_account_reactivation_requirements_to_dict",
    "source_account_reactivation_requirements_to_dicts",
    "source_account_reactivation_requirements_to_markdown",
]
