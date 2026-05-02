"""Extract source-level customer support expectations from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


CustomerSupportExpectationCategory = Literal[
    "support_sla",
    "escalation_path",
    "support_tooling",
    "customer_messaging",
    "knowledge_base",
    "ticket_triage",
    "refund_or_credit",
    "support_training",
]
CustomerSupportExpectationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[CustomerSupportExpectationCategory, ...] = (
    "support_sla",
    "escalation_path",
    "support_tooling",
    "customer_messaging",
    "knowledge_base",
    "ticket_triage",
    "refund_or_credit",
    "support_training",
)
_CONFIDENCE_ORDER: dict[CustomerSupportExpectationConfidence, int] = {
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
_SUPPORT_CONTEXT_RE = re.compile(
    r"\b(?:support|customer support|helpdesk|help desk|service desk|support agent|"
    r"agent|agents|ticket|tickets|case|cases|customer care|success team|csm|"
    r"customer success|launch support|handoff|post[- ]launch)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:support|help[-_ ]?desk|service[-_ ]?desk|ticket|triage|escalat|sla|"
    r"response[-_ ]?time|first[-_ ]?response|zendesk|intercom|salesforce|"
    r"messaging|comms?|communication|announcement|email|notification|status[-_ ]?page|"
    r"knowledge[-_ ]?base|help[-_ ]?center|faq|kb|refund|credit|training|"
    r"enablement|playbook|macro|runbook|source[-_ ]?payload|metadata|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|provide|define|document|prepare|publish|train|route|triage|"
    r"escalat(?:e|es|ed|ion)|notify|message|announce|respond|resolve|track|"
    r"configure|set up|done when|acceptance|cannot ship)\b",
    re.I,
)
_THRESHOLD_RE = re.compile(
    r"\b(?:\d+(?:\.\d+)?\s*(?:mins?|minutes?|hours?|hrs?|days?)|same day|"
    r"next business day|business hours?|24[/-]7|p[0-4]|sev(?:erity)?\s*[0-4])\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:support|ticket|triage|escalation|"
    r"customer messaging|knowledge base|refund|credit|training)\b.{0,80}\b"
    r"(?:in scope|required|requirements?|needed|changes?)\b",
    re.I,
)
_NEGATED_SUPPORT_SEGMENT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:support|ticket|triage|escalation|"
    r"customer messaging|knowledge base|refund|credit|training)\b",
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

_CATEGORY_PATTERNS: dict[CustomerSupportExpectationCategory, re.Pattern[str]] = {
    "support_sla": re.compile(
        r"\b(?:support sla|support response|first response|response time|respond within|"
        r"resolution time|business hours|24[/-]7 support|support hours|same day|"
        r"next business day|p[0-4]\s+response|sev(?:erity)?\s*[0-4].{0,40}response)\b",
        re.I,
    ),
    "escalation_path": re.compile(
        r"\b(?:escalation path|escalat(?:e|es|ed|ion)|tier 2|tier two|tier 3|"
        r"tier three|on-call|on call|pager|engineering owner|support owner|"
        r"incident commander|severity routing)\b",
        re.I,
    ),
    "support_tooling": re.compile(
        r"\b(?:zendesk|intercom|salesforce service cloud|service cloud|freshdesk|"
        r"helpscout|help scout|jira service management|support tooling|helpdesk|"
        r"help desk|ticketing system|support inbox|crm case|case management|"
        r"support queue|support macro|canned response|macro)\b",
        re.I,
    ),
    "customer_messaging": re.compile(
        r"\b(?:customer messaging|customer comms|customer communication|announce|"
        r"announcement|notify customers?|customer email|in[- ]app message|"
        r"status page|release notes?|customer-facing copy|support reply|"
        r"customer notice|account manager.{0,40}notify)\b",
        re.I,
    ),
    "knowledge_base": re.compile(
        r"\b(?:knowledge base|kb article|help center|help article|faq|frequently asked|"
        r"support doc|support guide|troubleshooting article|self[- ]serve help)\b",
        re.I,
    ),
    "ticket_triage": re.compile(
        r"\b(?:ticket triage|triage|ticket routing|route tickets?|case routing|"
        r"support queue|priority tags?|ticket tags?|severity classification|"
        r"intake queue|categorize tickets?|classify cases?)\b",
        re.I,
    ),
    "refund_or_credit": re.compile(
        r"\b(?:refund|refunds|refund policy|refund window|credit|credits|service credit|"
        r"goodwill credit|account credit|billing credit|chargeback|billing adjustment)\b",
        re.I,
    ),
    "support_training": re.compile(
        r"\b(?:support training|train support|train agents?|agent training|enablement|"
        r"support enablement|playbook|agent script|talk track|support workshop|"
        r"handoff training)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[CustomerSupportExpectationCategory, str] = {
    "support_sla": "support_lead",
    "escalation_path": "support_lead",
    "support_tooling": "support_ops",
    "customer_messaging": "customer_success",
    "knowledge_base": "support_content",
    "ticket_triage": "support_ops",
    "refund_or_credit": "support_billing",
    "support_training": "support_enablement",
}
_PLANNING_NOTES_BY_CATEGORY: dict[CustomerSupportExpectationCategory, tuple[str, ...]] = {
    "support_sla": (
        "Capture response and resolution commitments, coverage hours, and launch support measurement.",
    ),
    "escalation_path": (
        "Define escalation owners, severity routing, and engineering handoff before launch.",
    ),
    "support_tooling": (
        "Plan support tooling configuration, queues, macros, tags, and reporting updates.",
    ),
    "customer_messaging": (
        "Prepare customer-facing messaging, notification timing, and support reply wording.",
    ),
    "knowledge_base": (
        "Create or update help center, FAQ, knowledge base, and troubleshooting content.",
    ),
    "ticket_triage": (
        "Define ticket intake, triage labels, priority rules, and routing ownership.",
    ),
    "refund_or_credit": (
        "Document refund, credit, service-credit, and billing-adjustment handling for support.",
    ),
    "support_training": (
        "Schedule support enablement with agent scripts, playbooks, and launch handoff material.",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceCustomerSupportExpectation:
    """One source-backed customer support expectation."""

    source_brief_id: str | None
    category: CustomerSupportExpectationCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: CustomerSupportExpectationConfidence = "medium"
    owner_suggestion: str = ""
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "owner_suggestion": self.owner_suggestion,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceCustomerSupportExpectationsReport:
    """Source-level customer support expectations report."""

    source_id: str | None = None
    expectations: tuple[SourceCustomerSupportExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceCustomerSupportExpectation, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return customer support expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Customer Support Expectations Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Expectations found: {self.summary.get('expectation_count', 0)}",
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.expectations:
            lines.extend(["", "No customer support expectations were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Expectations",
                "",
                "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for expectation in self.expectations:
            lines.append(
                "| "
                f"{_markdown_cell(expectation.source_brief_id or '')} | "
                f"{expectation.category} | "
                f"{expectation.confidence} | "
                f"{_markdown_cell(expectation.owner_suggestion)} | "
                f"{_markdown_cell('; '.join(expectation.planning_notes))} | "
                f"{_markdown_cell('; '.join(expectation.evidence))} |"
            )
        return "\n".join(lines)


def build_source_customer_support_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCustomerSupportExpectationsReport:
    """Extract source-level customer support expectation records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    expectations = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceCustomerSupportExpectationsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        expectations=expectations,
        summary=_summary(expectations, len(brief_payloads)),
    )


def extract_source_customer_support_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCustomerSupportExpectationsReport:
    """Compatibility alias for building a customer support expectations report."""
    return build_source_customer_support_expectations(source)


def generate_source_customer_support_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCustomerSupportExpectationsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_customer_support_expectations(source)


def derive_source_customer_support_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCustomerSupportExpectationsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_customer_support_expectations(source)


def summarize_source_customer_support_expectations(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceCustomerSupportExpectationsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted customer support expectations."""
    if isinstance(source_or_result, SourceCustomerSupportExpectationsReport):
        return dict(source_or_result.summary)
    return build_source_customer_support_expectations(source_or_result).summary


def source_customer_support_expectations_to_dict(
    report: SourceCustomerSupportExpectationsReport,
) -> dict[str, Any]:
    """Serialize a customer support expectations report to a plain dictionary."""
    return report.to_dict()


source_customer_support_expectations_to_dict.__test__ = False


def source_customer_support_expectations_to_dicts(
    expectations: (
        tuple[SourceCustomerSupportExpectation, ...]
        | list[SourceCustomerSupportExpectation]
        | SourceCustomerSupportExpectationsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize customer support expectation records to dictionaries."""
    if isinstance(expectations, SourceCustomerSupportExpectationsReport):
        return expectations.to_dicts()
    return [expectation.to_dict() for expectation in expectations]


source_customer_support_expectations_to_dicts.__test__ = False


def source_customer_support_expectations_to_markdown(
    report: SourceCustomerSupportExpectationsReport,
) -> str:
    """Render a customer support expectations report as Markdown."""
    return report.to_markdown()


source_customer_support_expectations_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: CustomerSupportExpectationCategory
    evidence: str
    confidence: CustomerSupportExpectationConfidence


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
        for segment in _candidate_segments(payload):
            if not _is_expectation(segment):
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
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceCustomerSupportExpectation]:
    grouped: dict[tuple[str | None, CustomerSupportExpectationCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    expectations: list[SourceCustomerSupportExpectation] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        expectations.append(
            SourceCustomerSupportExpectation(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                confidence=confidence,
                owner_suggestion=_OWNER_BY_CATEGORY[category],
                planning_notes=_PLANNING_NOTES_BY_CATEGORY[category],
            )
        )
    return sorted(
        expectations,
        key=lambda expectation: (
            _optional_text(expectation.source_brief_id) or "",
            _CATEGORY_ORDER.index(expectation.category),
            _CONFIDENCE_ORDER[expectation.confidence],
            expectation.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "support",
        "customer_support",
        "helpdesk",
        "ticketing",
        "messaging",
        "knowledge_base",
        "refund",
        "training",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
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
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _SUPPORT_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
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
                _SUPPORT_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
                or any(pattern.search(title) for pattern in _CATEGORY_PATTERNS.values())
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        if _is_negated_support_scope(cleaned):
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_expectation(segment: _Segment) -> bool:
    text = segment.text
    searchable = f"{_field_words(segment.source_field)} {text}"
    if _is_negated_support_scope(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if (
        _REQUIREMENT_RE.search(text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    ):
        return True
    return bool(_SUPPORT_CONTEXT_RE.search(searchable) and _THRESHOLD_RE.search(searchable))


def _is_negated_support_scope(text: str) -> bool:
    return bool(_NEGATED_SCOPE_RE.search(text)) or bool(
        _NEGATED_SUPPORT_SEGMENT_RE.search(text)
        and re.search(r"\b(?:changes?|requirements?|needed|required|scope)\b", text, re.I)
    )


def _confidence(segment: _Segment) -> CustomerSupportExpectationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if (
        segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _THRESHOLD_RE.search(searchable)
    ) and _REQUIREMENT_RE.search(segment.text):
        return "high"
    if segment.section_context or _SUPPORT_CONTEXT_RE.search(searchable):
        return "high"
    if _REQUIREMENT_RE.search(segment.text):
        return "medium"
    return "low"


def _summary(
    expectations: tuple[SourceCustomerSupportExpectation, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "expectation_count": len(expectations),
        "category_counts": {
            category: sum(1 for expectation in expectations if expectation.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for expectation in expectations if expectation.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [expectation.category for expectation in expectations],
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
        "support",
        "customer_support",
        "helpdesk",
        "ticketing",
        "messaging",
        "knowledge_base",
        "refund",
        "training",
        "metadata",
        "brief_metadata",
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
    text = str(value).strip()
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
    "CustomerSupportExpectationCategory",
    "CustomerSupportExpectationConfidence",
    "SourceCustomerSupportExpectation",
    "SourceCustomerSupportExpectationsReport",
    "build_source_customer_support_expectations",
    "derive_source_customer_support_expectations",
    "extract_source_customer_support_expectations",
    "generate_source_customer_support_expectations",
    "source_customer_support_expectations_to_dict",
    "source_customer_support_expectations_to_dicts",
    "source_customer_support_expectations_to_markdown",
    "summarize_source_customer_support_expectations",
]
