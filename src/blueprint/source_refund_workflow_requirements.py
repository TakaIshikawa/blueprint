"""Extract source-level refund workflow requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


RefundWorkflowConcern = Literal[
    "eligibility",
    "refund_scope",
    "approval",
    "ledger_accounting",
    "customer_notification",
    "provider_reference",
    "idempotency",
    "audit_evidence",
    "dispute_credit_reversal",
]
RefundWorkflowConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CONCERN_ORDER: tuple[RefundWorkflowConcern, ...] = (
    "eligibility",
    "refund_scope",
    "approval",
    "ledger_accounting",
    "customer_notification",
    "provider_reference",
    "idempotency",
    "audit_evidence",
    "dispute_credit_reversal",
)
_CONFIDENCE_ORDER: dict[RefundWorkflowConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|record|track|audit|notify|send|sends|"
    r"store|persist|approve|eligible|prevent|dedupe|idempotent|post|ledger|reconcile|"
    r"before launch|acceptance)\b",
    re.I,
)
_REFUND_CONTEXT_RE = re.compile(
    r"\b(?:refunds?|refunded|refunding|refund workflow|refund request|refund policy|"
    r"refund eligibility|refund window|refund amount|partial refund|full refund|"
    r"payment reversal|charge reversal|reversals?|account credit|store credit|credit note|"
    r"customer credit|disputes?|chargebacks?|refund receipt|refund notification)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:refund|reversal|credit|dispute|chargeback|ledger|accounting|approval|provider|"
    r"stripe|adyen|paypal|payment|billing|notification|receipt|audit|evidence|"
    r"idempotenc|requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|"
    r"metadata|source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,100}"
    r"\b(?:refunds?|refund workflow|refund requests?|reversals?|credits?|disputes?|chargebacks?)\b"
    r".{0,100}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:refunds?|refund workflow|refund requests?|reversals?|credits?|disputes?|chargebacks?)\b"
    r".{0,100}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+(?:\.\d+)?\s?%|"
    r"(?:under|over|above|below|greater than|less than|at least|up to)\s+\$?\d+(?:[,.]\d+)?|"
    r"\$?\d+(?:[,.]\d+)?\s*(?:threshold|limit|refunds?)?|"
    r"(?:partial|full|pro[- ]?rated|prorated|store credit|account credit|credit note)|"
    r"(?:stripe|adyen|paypal|processor|gateway|provider)\s+(?:refund|reference|id)|"
    r"(?:idempotency key|request id|external id|provider reference|refund id|charge id))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\b", re.I)
_AMOUNT_RE = re.compile(
    r"\b(?:\d+(?:\.\d+)?\s?%|\$?\d+(?:[,.]\d+)?|under\s+\$?\d+(?:[,.]\d+)?|"
    r"over\s+\$?\d+(?:[,.]\d+)?)\b",
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
    "billing",
    "payments",
    "refunds",
    "refund",
    "ledger",
    "accounting",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_CONCERN_PATTERNS: dict[RefundWorkflowConcern, re.Pattern[str]] = {
    "eligibility": re.compile(
        r"\b(?:refund eligibility|eligible for refund|refund window|refund policy|refund period|"
        r"within\s+\d+\s*(?:days?|weeks?|months?)|after\s+\d+\s*(?:days?|weeks?|months?)|"
        r"non[- ]?refundable|not refundable|refund cutoff|qualif(?:y|ies) for refund)\b",
        re.I,
    ),
    "refund_scope": re.compile(
        r"\b(?:partial refunds?|full refunds?|full or partial refunds?|partial or full refunds?|"
        r"refund amount|refund balance|prorat(?:e|ed|ion)|pro[- ]?rat(?:e|ed|ion)|"
        r"remaining balance|over[- ]?refund|refund limit)\b",
        re.I,
    ),
    "approval": re.compile(
        r"\b(?:refund approval|approve refunds?|approval threshold|manager approval|finance approval|"
        r"manual approval|requires approval|approval required|auto[- ]?approve|"
        r"refunds?.{0,60}(?:over|above|greater than|exceed).{0,20}(?:approval|approve))\b",
        re.I,
    ),
    "ledger_accounting": re.compile(
        r"\b(?:ledger|accounting|journal entr(?:y|ies)|double[- ]entry|debits?|"
        r"credit note|revenue reversal|reverse revenue|"
        r"general ledger|gl posting|balance impact)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|notify customers?|refund email|refund receipt|email receipt|"
        r"in-app notice|refund status notice|send confirmation|notification after refund)\b",
        re.I,
    ),
    "provider_reference": re.compile(
        r"\b(?:provider reference|processor reference|gateway reference|stripe refund id|adyen psp|"
        r"paypal refund id|refund id|charge id|payment intent|provider refund|external reference)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotenc(?:y|e)|idempotency key|idempotent refund|duplicate refunds?|"
        r"dedupe refund|deduplicate refund|retry safe|safe retries?|request id|exactly once)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit log|audit trail|event log|refund history|evidence export|"
        r"who approved|approval actor|actor|timestamp|reason code|refund reason|immutable log)\b",
        re.I,
    ),
    "dispute_credit_reversal": re.compile(
        r"\b(?:disputes?|chargebacks?|reversals?|payment reversal|charge reversal|credits?|"
        r"credit note|store credit|account credit|customer credit)\b",
        re.I,
    ),
}
_PLAN_IMPACTS: dict[RefundWorkflowConcern, tuple[str, ...]] = {
    "eligibility": ("Define refund eligibility checks, cutoff windows, and customer-facing denial states.",),
    "refund_scope": ("Model full, partial, prorated, and over-refund boundaries before provider calls.",),
    "approval": ("Plan approval thresholds, approver roles, escalation paths, and manual override handling.",),
    "ledger_accounting": ("Post refund, reversal, and credit effects to ledger/accounting with reconciliation evidence.",),
    "customer_notification": ("Send customer refund status, receipt, and failure notifications from durable events.",),
    "provider_reference": ("Persist provider refund identifiers and original payment references for support and reconciliation.",),
    "idempotency": ("Make refund submission retry-safe with idempotency keys and duplicate-refund suppression.",),
    "audit_evidence": ("Record actor, reason, approval, timestamp, provider response, and immutable audit evidence.",),
    "dispute_credit_reversal": ("Handle disputes, reversals, credits, and chargeback-adjacent states separately from normal refunds.",),
}


@dataclass(frozen=True, slots=True)
class SourceRefundWorkflowRequirement:
    """One source-backed refund workflow requirement."""

    concern: RefundWorkflowConcern
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: RefundWorkflowConfidence = "medium"
    value: str | None = None
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> RefundWorkflowConcern:
        """Compatibility view for extractors that expose category naming."""
        return self.concern

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern": self.concern,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceRefundWorkflowRequirementsReport:
    """Source-level refund workflow requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceRefundWorkflowRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRefundWorkflowRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceRefundWorkflowRequirement, ...]:
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
        """Return refund workflow requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Refund Workflow Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        concern_counts = self.summary.get("concern_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Concern counts: "
            + ", ".join(f"{concern} {concern_counts.get(concern, 0)}" for concern in _CONCERN_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source refund workflow requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Concern | Value | Confidence | Source Field | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.concern} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_refund_workflow_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceRefundWorkflowRequirementsReport:
    """Build a refund workflow requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceRefundWorkflowRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_refund_workflow_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceRefundWorkflowRequirementsReport
        | str
        | object
    ),
) -> SourceRefundWorkflowRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceRefundWorkflowRequirementsReport):
        return dict(source.summary)
    return build_source_refund_workflow_requirements(source)


def derive_source_refund_workflow_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceRefundWorkflowRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_refund_workflow_requirements(source)


def generate_source_refund_workflow_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceRefundWorkflowRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_refund_workflow_requirements(source)


def extract_source_refund_workflow_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceRefundWorkflowRequirement, ...]:
    """Return refund workflow requirement records from brief-shaped input."""
    return build_source_refund_workflow_requirements(source).requirements


def source_refund_workflow_requirements_to_dict(
    report: SourceRefundWorkflowRequirementsReport,
) -> dict[str, Any]:
    """Serialize a refund workflow requirements report to a plain dictionary."""
    return report.to_dict()


source_refund_workflow_requirements_to_dict.__test__ = False


def source_refund_workflow_requirements_to_dicts(
    requirements: (
        tuple[SourceRefundWorkflowRequirement, ...]
        | list[SourceRefundWorkflowRequirement]
        | SourceRefundWorkflowRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize refund workflow requirement records to dictionaries."""
    if isinstance(requirements, SourceRefundWorkflowRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_refund_workflow_requirements_to_dicts.__test__ = False


def source_refund_workflow_requirements_to_markdown(
    report: SourceRefundWorkflowRequirementsReport,
) -> str:
    """Render a refund workflow requirements report as Markdown."""
    return report.to_markdown()


source_refund_workflow_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    concern: RefundWorkflowConcern
    value: str | None
    source_field: str
    evidence: str
    confidence: RefundWorkflowConfidence


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
        concerns = [
            concern for concern in _CONCERN_ORDER if _CONCERN_PATTERNS[concern].search(searchable)
        ]
        for concern in _dedupe(concerns):
            candidates.append(
                _Candidate(
                    concern=concern,
                    value=_value(concern, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceRefundWorkflowRequirement]:
    grouped: dict[RefundWorkflowConcern, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.concern, []).append(candidate)

    requirements: list[SourceRefundWorkflowRequirement] = []
    for concern in _CONCERN_ORDER:
        items = grouped.get(concern, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(
                    _CONFIDENCE_ORDER[item.confidence]
                    for item in items
                    if item.source_field == field
                ),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceRefundWorkflowRequirement(
                concern=concern,
                source_field=source_field,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_plan_impacts=_PLAN_IMPACTS[concern],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONCERN_ORDER.index(requirement.concern),
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
                _STRUCTURED_FIELD_RE.search(key_text) or _REFUND_CONTEXT_RE.search(key_text)
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
                _REFUND_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _REFUND_CONTEXT_RE.search(part)
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
    if not _REFUND_CONTEXT_RE.search(searchable):
        return False
    has_concern = any(pattern.search(searchable) for pattern in _CONCERN_PATTERNS.values())
    if not has_concern:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(
        _REFUND_CONTEXT_RE.search(segment.text)
        and re.search(r"\b(?:eligible|approved|posted|recorded|notified|deduped|reversed)\b", segment.text, re.I)
    )


def _value(concern: RefundWorkflowConcern, text: str) -> str | None:
    if concern == "eligibility":
        if match := re.search(
            r"\b(?P<value>(?:within|after|before|up to|until)?\s*\d+\s*(?:minutes?|hours?|days?|weeks?|months?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>non[- ]?refundable|not refundable)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if concern == "refund_scope":
        if match := re.search(
            r"\b(?P<value>partial refunds?|full refunds?|full or partial refunds?|partial or full refunds?|"
            r"pro[- ]?rated|prorated|refund amount|remaining balance)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        if match := _AMOUNT_RE.search(text):
            return _clean_text(match.group(0)).casefold()
    if concern == "approval":
        if match := re.search(
            r"\b(?P<value>(?:over|above|greater than|under|up to)?\s*\$?\d+(?:[,.]\d+)?|"
            r"manager approval|finance approval|manual approval|auto[- ]?approve)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if concern == "provider_reference":
        if match := re.search(
            r"\b(?P<value>(?:stripe|adyen|paypal|provider|processor|gateway)?\s*"
            r"(?:refund id|charge id|payment intent|provider reference|external reference))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if concern == "idempotency":
        if match := re.search(r"\b(?P<value>idempotency key|request id|external id|exactly once)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if concern == "dispute_credit_reversal":
        if match := re.search(
            r"\b(?P<value>disputes?|chargebacks?|reversals?|credit note|store credit|account credit|customer credit)\b",
            text,
            re.I,
        ):
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


def _confidence(segment: _Segment) -> RefundWorkflowConfidence:
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
                "refund",
                "billing",
                "payment",
                "ledger",
                "accounting",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _REFUND_CONTEXT_RE.search(searchable):
        return "medium"
    if _REFUND_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceRefundWorkflowRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "concerns": [requirement.concern for requirement in requirements],
        "concern_counts": {
            concern: sum(1 for requirement in requirements if requirement.concern == concern)
            for concern in _CONCERN_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_refund_workflow_language",
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
        "refunds",
        "refund",
        "ledger",
        "accounting",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "RefundWorkflowConcern",
    "RefundWorkflowConfidence",
    "SourceRefundWorkflowRequirement",
    "SourceRefundWorkflowRequirementsReport",
    "build_source_refund_workflow_requirements",
    "derive_source_refund_workflow_requirements",
    "extract_source_refund_workflow_requirements",
    "generate_source_refund_workflow_requirements",
    "summarize_source_refund_workflow_requirements",
    "source_refund_workflow_requirements_to_dict",
    "source_refund_workflow_requirements_to_dicts",
    "source_refund_workflow_requirements_to_markdown",
]
