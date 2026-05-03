"""Extract source-level billing dunning requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


BillingDunningCategory = Literal[
    "retry_cadence",
    "customer_communication",
    "grace_period",
    "service_suspension",
    "plan_downgrade",
    "invoice_recovery",
    "payment_method_update_handoff",
    "billing_engineering_ownership",
    "finance_ownership",
    "support_ownership",
    "lifecycle_messaging_ownership",
]
BillingDunningConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[BillingDunningCategory, ...] = (
    "retry_cadence",
    "customer_communication",
    "grace_period",
    "service_suspension",
    "plan_downgrade",
    "invoice_recovery",
    "payment_method_update_handoff",
    "billing_engineering_ownership",
    "finance_ownership",
    "support_ownership",
    "lifecycle_messaging_ownership",
)
_CONFIDENCE_ORDER: dict[BillingDunningConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DUNNING_CONTEXT_RE = re.compile(
    r"\b(?:dunning|billing recovery|payment recovery|collection workflow|failed payments?|"
    r"payment failures?|payment failed|card declined|declined card|past due|overdue|delinquen(?:t|cy)|"
    r"unpaid invoices?|invoice recovery|invoice collection|retry schedule|retry cadence|"
    r"grace period|service suspension|suspend service|account suspension|plan downgrade|"
    r"payment method update|update payment method|billing notice|payment notice)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:dunning|billing|bill[_ -]?recovery|payment[_ -]?recovery|failed[_ -]?payment|"
    r"payment[_ -]?failure|past[_ -]?due|overdue|delinquen|retry|cadence|grace|"
    r"suspension|suspend|downgrade|invoice|collection|payment[_ -]?method|"
    r"finance|support|lifecycle|messag|notice|notification|email|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|tracks?|records?|audit|notify|send|email|"
    r"retry|attempt|recover|collect|pause|suspend|downgrade|restore|handoff|route|"
    r"own(?:er|ership)?|finance|support|lifecycle|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:dunning|billing recovery|payment recovery|failed payments?|payment failures?|"
    r"past due|overdue|delinquen(?:t|cy)|invoice recovery|invoice collection|retry cadence|"
    r"grace period|service suspension|plan downgrade|payment method update)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:dunning|billing recovery|payment recovery|failed payments?|payment failures?|"
    r"past due|overdue|delinquen(?:t|cy)|invoice recovery|invoice collection|retry cadence|"
    r"grace period|service suspension|plan downgrade|payment method update)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|no changes are in scope)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+(?:st|nd|rd|th)\s+(?:attempt|retry|notice)|"
    r"\d+\s*(?:attempts?|retries|notices|emails)|"
    r"(?:daily|hourly|weekly|monthly|exponential|backoff|smart retries?|automatic retries?)|"
    r"(?:billing engineering|billing eng|finance|support|lifecycle messaging|crm|marketing automation)|"
    r"(?:past due|overdue|unpaid|final notice|account suspended|read[- ]?only|plan downgrade|"
    r"payment method update|hosted billing portal|invoice recovery))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\b", re.I)
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
    "billing",
    "billing_fields",
    "payments",
    "payment_failures",
    "dunning",
    "invoices",
    "invoice_recovery",
    "finance",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[BillingDunningCategory, re.Pattern[str]] = {
    "retry_cadence": re.compile(
        r"\b(?:retry cadence|retry schedule|retry attempts?|payment retries?|failed payment retries?|"
        r"collection attempts?|attempt(?:s|ed)? again|retry after|exponential backoff|smart retries?|"
        r"\d+\s*(?:attempts?|retries|times)|(?:daily|hourly|weekly)\s+retries?)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer notices?|customer communication|billing notices?|payment notices?|"
        r"past[- ]due emails?|dunning emails?|final notice|in[- ]app notice|notify customers?|"
        r"email customers?|sms|receipt|reminder(?:s)?|lifecycle message)\b",
        re.I,
    ),
    "grace_period": re.compile(
        r"\b(?:grace period|grace window|grace days?|before suspension|before suspend(?:ing)?|"
        r"after\s+\d+\s*(?:days?|weeks?)\s+(?:past due|overdue|unpaid)|"
        r"\d+\s*(?:days?|weeks?)\s+grace|after grace|after the grace)\b",
        re.I,
    ),
    "service_suspension": re.compile(
        r"\b(?:service suspension|suspend service|suspend account|account suspension|"
        r"disable access|read[- ]only|block access|deactivate|pause service|restore service|"
        r"reactivat(?:e|ion)|entitlement lock)\b",
        re.I,
    ),
    "plan_downgrade": re.compile(
        r"\b(?:plan downgrade|downgrade plan|downgrade account|free plan|limited plan|"
        r"reduce entitlements|remove premium features|seat reduction)\b",
        re.I,
    ),
    "invoice_recovery": re.compile(
        r"\b(?:invoice recovery|recover invoices?|recover unpaid invoices?|invoice collection|"
        r"collect overdue invoices?|past[- ]due invoices?|unpaid invoices?|reopen invoice|"
        r"mark invoice paid|invoice status|collections queue)\b",
        re.I,
    ),
    "payment_method_update_handoff": re.compile(
        r"\b(?:payment method update|update payment method|update card|card update|"
        r"hosted billing portal|billing portal|payment link|checkout link|secure handoff|"
        r"handoff to payment method|collect new card)\b",
        re.I,
    ),
    "billing_engineering_ownership": re.compile(
        r"\b(?:billing engineering|billing eng|billing platform|billing systems?|"
        r"engineering owner|engineer owns?|owner:\s*billing)\b",
        re.I,
    ),
    "finance_ownership": re.compile(
        r"\b(?:finance owner|finance ownership|finance owns?|finance review|finance approval|finance ops|"
        r"accounts receivable|ar team|collections team|revenue operations|revops)\b",
        re.I,
    ),
    "support_ownership": re.compile(
        r"\b(?:support owner|support owns?|support handoff|support queue|support playbook|"
        r"customer support|support agent|support escalation|ticket queue)\b",
        re.I,
    ),
    "lifecycle_messaging_ownership": re.compile(
        r"\b(?:lifecycle messaging|lifecycle owner|lifecycle owns?|crm owner|marketing automation|"
        r"email operations|messaging owner|comms owner|notification owner)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[BillingDunningCategory, tuple[str, ...]] = {
    "retry_cadence": ("billing_engineering",),
    "customer_communication": ("lifecycle_messaging", "support"),
    "grace_period": ("billing_engineering", "finance"),
    "service_suspension": ("billing_engineering", "support"),
    "plan_downgrade": ("billing_engineering", "finance", "support"),
    "invoice_recovery": ("finance", "billing_engineering"),
    "payment_method_update_handoff": ("billing_engineering", "support"),
    "billing_engineering_ownership": ("billing_engineering",),
    "finance_ownership": ("finance",),
    "support_ownership": ("support",),
    "lifecycle_messaging_ownership": ("lifecycle_messaging",),
}
_PLAN_IMPACTS: dict[BillingDunningCategory, tuple[str, ...]] = {
    "retry_cadence": ("Define failed-payment retry timing, attempt limits, backoff, and provider retry coordination.",),
    "customer_communication": ("Plan durable customer notices for past-due, retry, final-warning, and recovery states.",),
    "grace_period": ("Model grace windows and state transitions before downgrade or suspension actions run.",),
    "service_suspension": ("Gate service suspension, restoration, entitlement locks, and support override handling.",),
    "plan_downgrade": ("Define downgrade triggers, entitlement changes, billing effects, and reversal behavior after recovery.",),
    "invoice_recovery": ("Track unpaid invoice recovery, collection state, reconciliation, and finance reporting.",),
    "payment_method_update_handoff": ("Provide a secure payment method update handoff and resume collection after update.",),
    "billing_engineering_ownership": ("Assign billing engineering ownership for retry, state machine, and provider integration work.",),
    "finance_ownership": ("Assign finance ownership for collection policy, invoice status, write-offs, and reconciliation.",),
    "support_ownership": ("Assign support ownership for customer escalations, playbooks, and manual recovery paths.",),
    "lifecycle_messaging_ownership": ("Assign lifecycle messaging ownership for notice templates, sequencing, and suppression rules.",),
}


@dataclass(frozen=True, slots=True)
class SourceBillingDunningRequirement:
    """One source-backed billing dunning requirement."""

    category: BillingDunningCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: BillingDunningConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> BillingDunningCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> BillingDunningCategory:
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
class SourceBillingDunningRequirementsReport:
    """Source-level billing dunning requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceBillingDunningRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBillingDunningRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceBillingDunningRequirement, ...]:
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
        """Return billing dunning requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Billing Dunning Requirements Report"
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
            lines.extend(["", "No source billing dunning requirements were inferred."])
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


def build_source_billing_dunning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBillingDunningRequirementsReport:
    """Build a billing dunning requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceBillingDunningRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_billing_dunning_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceBillingDunningRequirementsReport
        | str
        | object
    ),
) -> SourceBillingDunningRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceBillingDunningRequirementsReport):
        return dict(source.summary)
    return build_source_billing_dunning_requirements(source)


def derive_source_billing_dunning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBillingDunningRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_billing_dunning_requirements(source)


def generate_source_billing_dunning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceBillingDunningRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_billing_dunning_requirements(source)


def extract_source_billing_dunning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceBillingDunningRequirement, ...]:
    """Return billing dunning requirement records from brief-shaped input."""
    return build_source_billing_dunning_requirements(source).requirements


def source_billing_dunning_requirements_to_dict(
    report: SourceBillingDunningRequirementsReport,
) -> dict[str, Any]:
    """Serialize a billing dunning requirements report to a plain dictionary."""
    return report.to_dict()


source_billing_dunning_requirements_to_dict.__test__ = False


def source_billing_dunning_requirements_to_dicts(
    requirements: (
        tuple[SourceBillingDunningRequirement, ...]
        | list[SourceBillingDunningRequirement]
        | SourceBillingDunningRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize billing dunning requirement records to dictionaries."""
    if isinstance(requirements, SourceBillingDunningRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_billing_dunning_requirements_to_dicts.__test__ = False


def source_billing_dunning_requirements_to_markdown(
    report: SourceBillingDunningRequirementsReport,
) -> str:
    """Render a billing dunning requirements report as Markdown."""
    return report.to_markdown()


source_billing_dunning_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: BillingDunningCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: BillingDunningConfidence


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


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceBillingDunningRequirement]:
    grouped: dict[BillingDunningCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceBillingDunningRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
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
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceBillingDunningRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
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
                _STRUCTURED_FIELD_RE.search(key_text) or _DUNNING_CONTEXT_RE.search(key_text)
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
                _DUNNING_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _DUNNING_CONTEXT_RE.search(part)
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
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_DUNNING_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _DUNNING_CONTEXT_RE.search(segment.text)
        and re.search(r"\b(?:retried|notified|suspended|downgraded|recovered|owned|routed)\b", segment.text, re.I)
    )


def _value(category: BillingDunningCategory, text: str) -> str | None:
    if category in {"retry_cadence", "grace_period"}:
        if category == "retry_cadence":
            if match := re.search(
                r"\b(?P<value>\d+\s*(?:attempts?|retries|times)|exponential backoff|smart retries?)\b",
                text,
                re.I,
            ):
                return _clean_text(match.group("value")).casefold()
        if match := re.search(
            r"\b(?P<value>(?:after|within|for|up to|every)?\s*\d+\s*(?:minutes?|hours?|days?|weeks?|months?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>\d+\s*(?:attempts?|retries)|exponential backoff|smart retries?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "customer_communication":
        if match := re.search(
            r"\b(?P<value>past[- ]due emails?|dunning emails?|final notice|in[- ]app notice|"
            r"billing notices?|customer notices?|sms|lifecycle message)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "service_suspension":
        if match := re.search(
            r"\b(?P<value>account suspension|service suspension|read[- ]only|disable access|"
            r"suspend service|restore service|entitlement lock)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "plan_downgrade":
        if match := re.search(r"\b(?P<value>plan downgrade|free plan|limited plan|reduce entitlements)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "invoice_recovery":
        if match := re.search(
            r"\b(?P<value>invoice recovery|unpaid invoices?|past[- ]due invoices?|invoice collection|collections queue)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "payment_method_update_handoff":
        if match := re.search(r"\b(?P<value>hosted billing portal|billing portal|payment link|checkout link)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(
            r"\b(?P<value>hosted billing portal|billing portal|payment link|checkout link|"
            r"payment method update|update payment method|update card)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category.endswith("_ownership"):
        if match := re.search(
            r"\b(?P<value>billing engineering|billing eng|finance|finance ops|accounts receivable|"
            r"support|customer support|lifecycle messaging|crm|marketing automation)\b",
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


def _confidence(segment: _Segment) -> BillingDunningConfidence:
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
                "dunning",
                "payment",
                "invoice",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _DUNNING_CONTEXT_RE.search(searchable):
        return "medium"
    if _DUNNING_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceBillingDunningRequirement, ...]) -> dict[str, Any]:
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
        "status": "ready_for_planning" if requirements else "no_billing_dunning_language",
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
        "billing",
        "billing_fields",
        "payments",
        "payment_failures",
        "dunning",
        "invoices",
        "invoice_recovery",
        "finance",
        "support",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: BillingDunningCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[BillingDunningCategory, tuple[str, ...]] = {
        "retry_cadence": ("retry", "cadence"),
        "customer_communication": ("notice", "communication", "message", "email"),
        "grace_period": ("grace",),
        "service_suspension": ("suspension", "suspend"),
        "plan_downgrade": ("downgrade",),
        "invoice_recovery": ("invoice", "collection"),
        "payment_method_update_handoff": ("payment method", "payment", "card", "portal"),
        "billing_engineering_ownership": ("engineering", "billing"),
        "finance_ownership": ("finance",),
        "support_ownership": ("support",),
        "lifecycle_messaging_ownership": ("lifecycle", "messaging"),
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
    "BillingDunningCategory",
    "BillingDunningConfidence",
    "SourceBillingDunningRequirement",
    "SourceBillingDunningRequirementsReport",
    "build_source_billing_dunning_requirements",
    "derive_source_billing_dunning_requirements",
    "extract_source_billing_dunning_requirements",
    "generate_source_billing_dunning_requirements",
    "summarize_source_billing_dunning_requirements",
    "source_billing_dunning_requirements_to_dict",
    "source_billing_dunning_requirements_to_dicts",
    "source_billing_dunning_requirements_to_markdown",
]
