"""Extract source-level payment method update requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PaymentMethodUpdateConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CONFIDENCE_ORDER: dict[PaymentMethodUpdateConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PAYMENT_METHOD_CONTEXT_RE = re.compile(
    r"\b(?:payment method|payment methods|card|cards|credit card|debit card|bank account|"
    r"ach|sepa|direct debit|default payment|default card|default bank|billing details|"
    r"billing profile|expired card|expiring card|payment details|wallet|mandate)\b",
    re.I,
)
_UPDATE_CONTEXT_RE = re.compile(
    r"\b(?:update|updates|updated|updating|change|changes|changed|changing|replace|"
    r"replac(?:e|es|ed|ing)|add|adds|added|adding|remove|removes|removed|removing|"
    r"select|selection|set as default|make default|defaulting|default payment method|"
    r"expired|expiring|reauthenticat(?:e|es|ed|ion)|3ds|3[- ]?d secure|sca|"
    r"retry|retries|dunning|past due|invoice retry|failed payment)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:payment[_ -]?method|payment|card|bank|ach|sepa|direct[_ -]?debit|default|"
    r"billing|invoice|retry|dunning|sca|3ds|authentication|notification|audit|"
    r"evidence|requirements?|acceptance|criteria|definition[_ -]?of[_ -]?done|"
    r"metadata|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|record|track|audit|notify|send|"
    r"reauthenticat(?:e|es|ed|ion)|authenticate|retry|default|acceptance|done when|"
    r"before launch|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,140}"
    r"\b(?:payment method updates?|payment method changes?|card updates?|card changes?|"
    r"bank account updates?|bank account changes?|default payment method|expired cards?)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|"
    r"impact|for this release)\b|"
    r"\b(?:payment method updates?|payment method changes?|card updates?|card changes?|"
    r"bank account updates?|bank account changes?|default payment method|expired cards?)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|"
    r"no work|no impact|non-goal|non goal)\b",
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
    "billing",
    "payments",
    "payment_methods",
    "payment_method",
    "cards",
    "bank_accounts",
    "invoice",
    "invoices",
    "retry",
    "dunning",
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
}
_PAYMENT_TYPE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("card", re.compile(r"\b(?:card|cards|credit card|debit card|expired card|expiring card|3ds|3[- ]?d secure)\b", re.I)),
    ("bank_account", re.compile(r"\b(?:bank account|ach|sepa|direct debit|mandate)\b", re.I)),
    ("payment_method", re.compile(r"\b(?:payment method|payment methods|payment details|wallet)\b", re.I)),
)
_PAYMENT_TYPE_ORDER: dict[str, int] = {
    "card": 0,
    "bank_account": 1,
    "card_and_bank_account": 2,
    "payment_method": 3,
}
_TRIGGER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("expired_or_expiring", re.compile(r"\b(?:expired|expiring|expiration|expiry)\b", re.I)),
    ("failed_payment", re.compile(r"\b(?:failed payment|payment failure|decline|declined|past due|retry|dunning)\b", re.I)),
    ("customer_requested", re.compile(r"\b(?:customer|user|admin|account owner).{0,80}\b(?:update|change|replace|add|remove|select)\b", re.I)),
    ("checkout_or_invoice", re.compile(r"\b(?:checkout|invoice|subscription|billing profile|billing portal)\b", re.I)),
)
_DEFAULTING_RE = re.compile(
    r"\b(?:default payment method|default card|default bank|set as default|make default|"
    r"make .{0,40} the default|as default|defaulting|"
    r"primary payment method|fallback payment method|"
    r"new payment method should be used for future invoices)\b",
    re.I,
)
_AUTH_RE = re.compile(
    r"\b(?:sca|strong customer authentication|3ds|3[- ]?d secure|reauthenticat(?:e|es|ed|ion)|"
    r"authenticate|authentication required|mandate acceptance|bank mandate|microdeposit)\b",
    re.I,
)
_RETRY_DUNNING_RE = re.compile(
    r"\b(?:retry|retries|invoice retry|retry schedule|dunning|past due|failed payment|"
    r"payment recovery|recover failed invoices?|resume collection|collect open invoices?)\b",
    re.I,
)
_NOTIFICATION_RE = re.compile(
    r"\b(?:notify|notification|email|receipt|in-app notice|message|alert|reminder|"
    r"customer communication|send confirmation|confirmation email)\b",
    re.I,
)
_AUDIT_RE = re.compile(
    r"\b(?:audit evidence|audit log|audit trail|event log|history|record actor|actor|"
    r"timestamp|evidence export|support evidence|change reason|who changed)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourcePaymentMethodUpdateRequirement:
    """One source-backed payment method update requirement."""

    source_brief_id: str | None
    source_field: str
    update_trigger: str | None = None
    payment_method_type: str | None = None
    defaulting_behavior: str | None = None
    authentication_requirement: str | None = None
    retry_or_dunning_linkage: str | None = None
    notification_requirement: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PaymentMethodUpdateConfidence = "medium"
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "source_field": self.source_field,
            "update_trigger": self.update_trigger,
            "payment_method_type": self.payment_method_type,
            "defaulting_behavior": self.defaulting_behavior,
            "authentication_requirement": self.authentication_requirement,
            "retry_or_dunning_linkage": self.retry_or_dunning_linkage,
            "notification_requirement": self.notification_requirement,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourcePaymentMethodUpdateRequirementsReport:
    """Source-level payment method update requirements report."""

    source_id: str | None = None
    requirements: tuple[SourcePaymentMethodUpdateRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePaymentMethodUpdateRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourcePaymentMethodUpdateRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return payment method update requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Payment Method Update Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Payment method types: " + ", ".join(self.summary.get("payment_method_types", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source payment method update requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Source Field | Update Trigger | Payment Method Type | Defaulting Behavior | Authentication Requirement | Retry/Dunning Linkage | Notification Requirement | Confidence | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(requirement.update_trigger or '')} | "
                f"{_markdown_cell(requirement.payment_method_type or '')} | "
                f"{_markdown_cell(requirement.defaulting_behavior or '')} | "
                f"{_markdown_cell(requirement.authentication_requirement or '')} | "
                f"{_markdown_cell(requirement.retry_or_dunning_linkage or '')} | "
                f"{_markdown_cell(requirement.notification_requirement or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_payment_method_update_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePaymentMethodUpdateRequirementsReport:
    """Extract source-level payment method update requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePaymentMethodUpdateRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_payment_method_update_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePaymentMethodUpdateRequirementsReport:
    """Compatibility alias for building a payment method update requirements report."""
    return build_source_payment_method_update_requirements(source)


def generate_source_payment_method_update_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePaymentMethodUpdateRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_payment_method_update_requirements(source)


def derive_source_payment_method_update_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePaymentMethodUpdateRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_payment_method_update_requirements(source)


def summarize_source_payment_method_update_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePaymentMethodUpdateRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted payment method update requirements."""
    if isinstance(source_or_result, SourcePaymentMethodUpdateRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_payment_method_update_requirements(source_or_result).summary


def source_payment_method_update_requirements_to_dict(
    report: SourcePaymentMethodUpdateRequirementsReport,
) -> dict[str, Any]:
    """Serialize a payment method update requirements report to a plain dictionary."""
    return report.to_dict()


source_payment_method_update_requirements_to_dict.__test__ = False


def source_payment_method_update_requirements_to_dicts(
    requirements: (
        tuple[SourcePaymentMethodUpdateRequirement, ...]
        | list[SourcePaymentMethodUpdateRequirement]
        | SourcePaymentMethodUpdateRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize payment method update requirement records to dictionaries."""
    if isinstance(requirements, SourcePaymentMethodUpdateRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_payment_method_update_requirements_to_dicts.__test__ = False


def source_payment_method_update_requirements_to_markdown(
    report: SourcePaymentMethodUpdateRequirementsReport,
) -> str:
    """Render a payment method update requirements report as Markdown."""
    return report.to_markdown()


source_payment_method_update_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    source_field: str
    update_trigger: str | None
    payment_method_type: str | None
    defaulting_behavior: str | None
    authentication_requirement: str | None
    retry_or_dunning_linkage: str | None
    notification_requirement: str | None
    audit_evidence: str | None
    evidence: str
    confidence: PaymentMethodUpdateConfidence


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        segments = _candidate_segments(payload)
        if any(_NEGATED_SCOPE_RE.search(f"{_field_words(segment.source_field)} {segment.text}") for segment in segments):
            continue
        for segment in segments:
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            candidates.append(
                _Candidate(
                    source_brief_id=source_brief_id,
                    source_field=segment.source_field,
                    update_trigger=_trigger(searchable),
                    payment_method_type=_payment_method_type(searchable),
                    defaulting_behavior=_matched_value(_DEFAULTING_RE, segment.text),
                    authentication_requirement=_matched_value(_AUTH_RE, segment.text),
                    retry_or_dunning_linkage=_retry_or_dunning_linkage(segment.text),
                    notification_requirement=_notification_requirement(segment.text),
                    audit_evidence=_matched_value(_AUDIT_RE, segment.text),
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourcePaymentMethodUpdateRequirement]:
    grouped: dict[tuple[str | None, str | None], list[_Candidate]] = {}
    for candidate in candidates:
        key = (candidate.source_brief_id, candidate.payment_method_type)
        grouped.setdefault(key, []).append(candidate)

    requirements: list[SourcePaymentMethodUpdateRequirement] = []
    for (source_brief_id, payment_method_type), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourcePaymentMethodUpdateRequirement(
                source_brief_id=source_brief_id,
                source_field=_best_source_field(items),
                update_trigger=_best_trigger(item.update_trigger for item in items),
                payment_method_type=payment_method_type,
                defaulting_behavior=_best_text(item.defaulting_behavior for item in items),
                authentication_requirement=_best_text(item.authentication_requirement for item in items),
                retry_or_dunning_linkage=_best_ranked_text(
                    (item.retry_or_dunning_linkage for item in items),
                    ("invoice retry", "dunning", "retry", "failed payment", "past due"),
                ),
                notification_requirement=_best_ranked_text(
                    (item.notification_requirement for item in items),
                    ("email", "confirmation email", "notify", "notification", "send confirmation"),
                ),
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
                confidence=confidence,
                planning_notes=_planning_notes(items),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _PAYMENT_TYPE_ORDER.get(requirement.payment_method_type or "", 99),
            requirement.source_field.casefold(),
            requirement.update_trigger or "",
            _CONFIDENCE_ORDER[requirement.confidence],
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
                _STRUCTURED_FIELD_RE.search(key_text)
                or _PAYMENT_METHOD_CONTEXT_RE.search(key_text)
                or _UPDATE_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
                _PAYMENT_METHOD_CONTEXT_RE.search(title)
                or _UPDATE_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _PAYMENT_METHOD_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_PAYMENT_METHOD_CONTEXT_RE.search(searchable) and _UPDATE_CONTEXT_RE.search(searchable)):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(_DEFAULTING_RE.search(searchable) or _AUTH_RE.search(searchable) or _RETRY_DUNNING_RE.search(searchable))


def _payment_method_type(text: str) -> str | None:
    values = [name for name, pattern in _PAYMENT_TYPE_PATTERNS if pattern.search(text)]
    values = _dedupe(values)
    if "card" in values and "bank_account" in values:
        return "card_and_bank_account"
    return values[0] if values else None


def _trigger(text: str) -> str | None:
    values = [name for name, pattern in _TRIGGER_PATTERNS if pattern.search(text)]
    return _dedupe(values)[0] if values else "manual_update"


def _matched_value(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _notification_requirement(text: str) -> str | None:
    if re.search(r"\bemail\b", text, re.I):
        return "email"
    return _matched_value(_NOTIFICATION_RE, text)


def _retry_or_dunning_linkage(text: str) -> str | None:
    for value in ("invoice retry", "dunning", "retry", "failed payment", "past due"):
        if re.search(rf"\b{re.escape(value)}\b", text, re.I):
            return value
    return _matched_value(_RETRY_DUNNING_RE, text)


def _confidence(segment: _Segment) -> PaymentMethodUpdateConfidence:
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
                "payment",
                "billing",
                "invoice",
                "retry",
                "dunning",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _PAYMENT_METHOD_CONTEXT_RE.search(searchable):
        return "medium"
    if _PAYMENT_METHOD_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _planning_notes(items: Iterable[_Candidate]) -> tuple[str, ...]:
    candidates = list(items)
    notes = [
        "Define who can add, replace, remove, and select payment methods and how the billing provider state is synchronized.",
    ]
    if any(item.defaulting_behavior for item in candidates):
        notes.append("Specify default payment method precedence for future invoices, subscriptions, and fallback collection.")
    if any(item.authentication_requirement for item in candidates):
        notes.append("Plan SCA, 3DS, mandate, or bank-account reauthentication before saving or reusing the method.")
    if any(item.retry_or_dunning_linkage for item in candidates):
        notes.append("Connect successful updates to invoice retry, dunning recovery, and past-due state transitions.")
    if any(item.notification_requirement for item in candidates):
        notes.append("Send customer notifications for update success, failure, expiration, and collection recovery states.")
    if any(item.audit_evidence for item in candidates):
        notes.append("Record actor, timestamp, method type, provider reference, and reason for support and audit evidence.")
    return tuple(notes)


def _summary(
    requirements: tuple[SourcePaymentMethodUpdateRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    payment_method_types = _dedupe(
        requirement.payment_method_type for requirement in requirements if requirement.payment_method_type
    )
    update_triggers = _dedupe(
        requirement.update_trigger for requirement in requirements if requirement.update_trigger
    )
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "payment_method_types": payment_method_types,
        "update_triggers": update_triggers,
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "requires_defaulting": any(requirement.defaulting_behavior for requirement in requirements),
        "requires_authentication": any(requirement.authentication_requirement for requirement in requirements),
        "requires_retry_or_dunning_linkage": any(
            requirement.retry_or_dunning_linkage for requirement in requirements
        ),
        "requires_notifications": any(requirement.notification_requirement for requirement in requirements),
        "status": "ready_for_payment_method_update_planning"
        if requirements
        else "no_payment_method_update_language",
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
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
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
        "billing",
        "payments",
        "payment_methods",
        "payment_method",
        "cards",
        "bank_accounts",
        "invoice",
        "invoices",
        "retry",
        "dunning",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _group_field(source_field: str) -> str:
    return re.sub(r"\[\d+\]$", "", source_field)


def _best_source_field(items: Iterable[_Candidate]) -> str:
    fields = sorted(
        {_group_field(item.source_field) for item in items if item.source_field},
        key=lambda field: (field.count("."), field.count("["), field.casefold()),
    )
    return fields[0] if fields else ""


def _best_trigger(values: Iterable[str | None]) -> str | None:
    triggers = _dedupe(value for value in values if value)
    if not triggers:
        return None
    for trigger in ("expired_or_expiring", "failed_payment", "customer_requested", "checkout_or_invoice"):
        if trigger in triggers:
            return trigger
    return triggers[0]


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


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


def _best_text(values: Iterable[str | None]) -> str | None:
    texts = sorted(_dedupe(value for value in values if value), key=lambda value: (len(value), value.casefold()))
    return texts[0] if texts else None


def _best_ranked_text(values: Iterable[str | None], preferred: tuple[str, ...]) -> str | None:
    texts = _dedupe(value for value in values if value)
    if not texts:
        return None
    for target in preferred:
        for text in texts:
            if text == target:
                return text
    return sorted(texts, key=lambda value: (len(value), value.casefold()))[0]


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
    "PaymentMethodUpdateConfidence",
    "SourcePaymentMethodUpdateRequirement",
    "SourcePaymentMethodUpdateRequirementsReport",
    "build_source_payment_method_update_requirements",
    "derive_source_payment_method_update_requirements",
    "extract_source_payment_method_update_requirements",
    "generate_source_payment_method_update_requirements",
    "source_payment_method_update_requirements_to_dict",
    "source_payment_method_update_requirements_to_dicts",
    "source_payment_method_update_requirements_to_markdown",
    "summarize_source_payment_method_update_requirements",
]
