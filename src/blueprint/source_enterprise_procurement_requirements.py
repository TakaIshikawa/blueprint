"""Extract source-level enterprise procurement requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


EnterpriseProcurementCategory = Literal[
    "security_review",
    "legal_review",
    "purchase_order",
    "vendor_onboarding",
    "sso_scim_prerequisites",
    "data_processing_agreement",
    "sla_requirements",
    "procurement_timeline",
]
EnterpriseProcurementConfidence = Literal["high", "medium", "low"]
EnterpriseProcurementMissingField = Literal["owner", "due_date", "approval_evidence"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[EnterpriseProcurementCategory, ...] = (
    "security_review",
    "legal_review",
    "purchase_order",
    "vendor_onboarding",
    "sso_scim_prerequisites",
    "data_processing_agreement",
    "sla_requirements",
    "procurement_timeline",
)
_GATED_CATEGORIES: tuple[EnterpriseProcurementCategory, ...] = (
    "security_review",
    "legal_review",
    "purchase_order",
    "vendor_onboarding",
    "sso_scim_prerequisites",
    "data_processing_agreement",
    "sla_requirements",
)
_CONFIDENCE_ORDER: dict[EnterpriseProcurementConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PROCUREMENT_CONTEXT_RE = re.compile(
    r"\b(?:enterprise|procurement|purchasing|vendor|vendor onboarding|supplier|sourcing|"
    r"enterprise sales|sales brief|launch brief|customer launch|commercial approval|"
    r"buyer|security review|infosec|information security|security questionnaire|"
    r"legal review|contract review|redlines?|msa|order form|purchase order|po number|"
    r"dpa|data processing agreement|subprocessor|sla|service level|uptime|support response|"
    r"sso|saml|scim|identity provider|idp|okta|azure ad|vendor risk|approval gate|"
    r"procurement gate|go[- ]?live gate|launch gate)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:enterprise|procurement|purchasing|vendor|supplier|sales|launch|security|infosec|"
    r"legal|contract|purchase[-_ ]?order|po|onboarding|sso|scim|saml|dpa|"
    r"data[-_ ]?processing|sla|service[-_ ]?level|timeline|approval|gate|owner|due|"
    r"requirements?|constraints?|risks?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|"
    r"metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"complete|approve|approved|approval|sign[- ]?off|signed|issue|issued|provide|"
    r"collect|submit|before launch|before go[- ]?live|cannot launch|cannot ship|"
    r"blocked|blocker|blocks?|gate|gated|prerequisite|dependency|due|deadline|timeline)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:enterprise procurement|procurement|security review|legal review|purchase order|"
    r"vendor onboarding|sso|scim|dpa|data processing agreement|sla)\b"
    r".{0,140}\b(?:required|needed|in scope|planned|work|changes?|for this release)\b|"
    r"\b(?:enterprise procurement|procurement|security review|legal review|purchase order|"
    r"vendor onboarding|sso|scim|dpa|data processing agreement|sla)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|excluded)\b",
    re.I,
)
_CONSUMER_SCOPE_RE = re.compile(
    r"\b(?:consumer|self[- ]?serve|b2c|personal account|shopper|checkout copy|mobile app)\b",
    re.I,
)
_STRONG_ENTERPRISE_RE = re.compile(
    r"\b(?:enterprise|procurement|purchasing|vendor|supplier|sourcing|enterprise sales|"
    r"customer launch|commercial approval|buyer|infosec|security questionnaire|vendor risk|"
    r"contract review|msa|dpa|data processing agreement|sla|service level agreement|sso|scim|"
    r"saml|identity provider|approval gate|procurement gate|go[- ]?live gate)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|dri|assignee|assigned to|owned by|team)\s*:?"
    r"|\bby\s+(?:customer it|customer admin|procurement|finance ops|sales ops)\b",
    re.I,
)
_DUE_DATE_RE = re.compile(
    r"\b(?:due|deadline|before|no later than|target date|eta|eod|eow|q[1-4]|"
    r"by\s+(?:\d{4}-\d{2}-\d{2}|q[1-4]|go[- ]?live|launch|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}|\d+\s*(?:business\s+)?(?:days?|weeks?|months?))|"
    r"\d{4}-\d{2}-\d{2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}|"
    r"\d+\s*(?:business\s+)?(?:days?|weeks?|months?)|go[- ]?live|launch)\b",
    re.I,
)
_APPROVAL_EVIDENCE_RE = re.compile(
    r"\b(?:approved|approval|sign[- ]?off|signoff|signed|executed|completed|cleared|"
    r"greenlit|attestation|evidence|ticket|link|record|certificate|po issued|po number|"
    r"vendor id|dpa signed|sla signed|security review passed|legal approved)\b",
    re.I,
)
_TIMELINE_RISK_RE = re.compile(
    r"\b(?:blocked|blocker|risk|slip|delay|cannot launch|cannot go[- ]?live|depends on|"
    r"waiting on|lead time|timeline|deadline|critical path|approval window|procurement cycle|"
    r"\d+\s*(?:business\s+)?(?:days?|weeks?|months?))\b",
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
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
    "created_by",
    "updated_by",
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
    "enterprise",
    "procurement",
    "security",
    "legal",
    "contract",
    "vendor",
    "sso",
    "scim",
    "sla",
    "timeline",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[EnterpriseProcurementCategory, re.Pattern[str]] = {
    "security_review": re.compile(
        r"\b(?:security review|infosec review|information security review|security questionnaire|"
        r"security assessment|vendor risk review|risk assessment|security approval|soc 2|iso 27001|"
        r"penetration test|pen test|security packet)\b",
        re.I,
    ),
    "legal_review": re.compile(
        r"\b(?:legal review|contract review|legal approval|legal sign[- ]?off|redlines?|"
        r"msa|master services agreement|order form|terms review|contracting)\b",
        re.I,
    ),
    "purchase_order": re.compile(
        r"\b(?:purchase order|po number|po issued|po approval|procurement order|"
        r"buyer must issue a po|finance po|invoice requires po)\b",
        re.I,
    ),
    "vendor_onboarding": re.compile(
        r"\b(?:vendor onboarding|supplier onboarding|vendor setup|vendor registration|"
        r"vendor portal|supplier portal|vendor id|vendor master|new vendor form|w[- ]?9|w[- ]?8ben)\b",
        re.I,
    ),
    "sso_scim_prerequisites": re.compile(
        r"\b(?:sso|single sign[- ]?on|saml|oidc|scim|user provisioning|deprovisioning|"
        r"identity provider|idp|okta|azure ad|entra id|directory sync)\b",
        re.I,
    ),
    "data_processing_agreement": re.compile(
        r"\b(?:data processing agreement|dpa|data protection agreement|privacy addendum|"
        r"subprocessor|standard contractual clauses|sccs?|data transfer impact assessment|dtia)\b",
        re.I,
    ),
    "sla_requirements": re.compile(
        r"\b(?:sla|service level agreement|service levels?|uptime|availability commitment|"
        r"support response|response time|remedy|service credit|rto|rpo)\b",
        re.I,
    ),
    "procurement_timeline": re.compile(
        r"\b(?:procurement timeline|procurement cycle|approval timeline|approval window|"
        r"lead time|go[- ]?live date|launch date|deadline|critical path|blocked until|"
        r"cannot launch|cannot go[- ]?live|waiting on procurement|timeline risk)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[EnterpriseProcurementCategory, tuple[str, ...]] = {
    "security_review": ("security", "sales_engineering"),
    "legal_review": ("legal", "sales_ops"),
    "purchase_order": ("finance_ops", "sales_ops"),
    "vendor_onboarding": ("procurement", "finance_ops"),
    "sso_scim_prerequisites": ("identity", "customer_it"),
    "data_processing_agreement": ("legal", "privacy"),
    "sla_requirements": ("support_ops", "legal"),
    "procurement_timeline": ("sales_ops", "implementation_owner"),
}
_PLAN_IMPACTS: dict[EnterpriseProcurementCategory, tuple[str, ...]] = {
    "security_review": ("Track security review artifacts, questionnaire ownership, approval status, and launch dependency.",),
    "legal_review": ("Plan contract review, redline resolution, sign-off evidence, and order-form dependencies.",),
    "purchase_order": ("Confirm PO issuance, invoice requirements, and finance approval before fulfillment or go-live.",),
    "vendor_onboarding": ("Complete supplier setup, vendor IDs, forms, and customer procurement portal work before launch.",),
    "sso_scim_prerequisites": ("Schedule SSO, SAML, SCIM, provisioning, and customer identity-provider prerequisites.",),
    "data_processing_agreement": ("Resolve DPA, privacy addendum, subprocessors, and data-transfer approval before processing data.",),
    "sla_requirements": ("Confirm uptime, support response, remedy, and service-credit commitments in the implementation plan.",),
    "procurement_timeline": ("Treat procurement lead times, approval windows, and go-live blockers as plan constraints.",),
}


@dataclass(frozen=True, slots=True)
class SourceEnterpriseProcurementRequirement:
    """One source-backed enterprise procurement requirement."""

    category: EnterpriseProcurementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: EnterpriseProcurementConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> EnterpriseProcurementCategory:
        """Compatibility view for extractors that expose requirement_category."""
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
class SourceEnterpriseProcurementGap:
    """One missing procurement gate detail to resolve before planning."""

    category: EnterpriseProcurementCategory
    missing_field: EnterpriseProcurementMissingField
    message: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: EnterpriseProcurementConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "missing_field": self.missing_field,
            "message": self.message,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceEnterpriseProcurementRequirementsReport:
    """Source-level enterprise procurement requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceEnterpriseProcurementRequirement, ...] = field(default_factory=tuple)
    evidence_gaps: tuple[SourceEnterpriseProcurementGap, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceEnterpriseProcurementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def gaps(self) -> tuple[SourceEnterpriseProcurementGap, ...]:
        """Compatibility alias for evidence gaps."""
        return self.evidence_gaps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "evidence_gaps": [gap.to_dict() for gap in self.evidence_gaps],
            "records": [record.to_dict() for record in self.records],
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return enterprise procurement requirement records as dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Enterprise Procurement Requirements Report"
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
            f"- Evidence gaps: {self.summary.get('evidence_gap_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source enterprise procurement requirements were inferred."])
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
        if self.evidence_gaps:
            lines.extend(["", "## Evidence Gaps", "", "| Category | Missing Field | Confidence | Message |", "| --- | --- | --- | --- |"])
            for gap in self.evidence_gaps:
                lines.append(
                    f"| {gap.category} | {gap.missing_field} | {gap.confidence} | {_markdown_cell(gap.message)} |"
                )
        return "\n".join(lines)


def build_source_enterprise_procurement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEnterpriseProcurementRequirementsReport:
    """Build an enterprise procurement requirements report from brief-shaped input."""
    brief_id, payload = _source_payload(source)
    requirements = () if _has_global_no_scope(payload) else tuple(_merge_candidates(_requirement_candidates(payload)))
    gaps = tuple(_evidence_gaps(requirements))
    return SourceEnterpriseProcurementRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        evidence_gaps=gaps,
        summary=_summary(requirements, gaps),
    )


def summarize_source_enterprise_procurement_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceEnterpriseProcurementRequirementsReport
        | str
        | object
    ),
) -> SourceEnterpriseProcurementRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceEnterpriseProcurementRequirementsReport):
        return dict(source.summary)
    return build_source_enterprise_procurement_requirements(source)


def derive_source_enterprise_procurement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEnterpriseProcurementRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_enterprise_procurement_requirements(source)


def generate_source_enterprise_procurement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEnterpriseProcurementRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_enterprise_procurement_requirements(source)


def extract_source_enterprise_procurement_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceEnterpriseProcurementRequirement, ...]:
    """Return enterprise procurement requirement records from brief-shaped input."""
    return build_source_enterprise_procurement_requirements(source).requirements


def source_enterprise_procurement_requirements_to_dict(
    report: SourceEnterpriseProcurementRequirementsReport,
) -> dict[str, Any]:
    """Serialize an enterprise procurement requirements report to a plain dictionary."""
    return report.to_dict()


source_enterprise_procurement_requirements_to_dict.__test__ = False


def source_enterprise_procurement_requirements_to_dicts(
    requirements: (
        tuple[SourceEnterpriseProcurementRequirement, ...]
        | list[SourceEnterpriseProcurementRequirement]
        | SourceEnterpriseProcurementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize enterprise procurement requirement records to dictionaries."""
    if isinstance(requirements, SourceEnterpriseProcurementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_enterprise_procurement_requirements_to_dicts.__test__ = False


def source_enterprise_procurement_requirements_to_markdown(
    report: SourceEnterpriseProcurementRequirementsReport,
) -> str:
    """Render an enterprise procurement requirements report as Markdown."""
    return report.to_markdown()


source_enterprise_procurement_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: EnterpriseProcurementCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: EnterpriseProcurementConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (bytes, bytearray)):
        return None, {}
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
    payload = _object_payload(source)
    return _brief_id(payload), payload


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
        root = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceEnterpriseProcurementRequirement]:
    grouped: dict[EnterpriseProcurementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceEnterpriseProcurementRequirement] = []
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
        ordered_items = sorted(
            items,
            key=lambda item: (
                _field_category_rank(category, item.source_field),
                item.source_field.casefold(),
                item.evidence.casefold(),
            ),
        )
        requirements.append(
            SourceEnterpriseProcurementRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in ordered_items))[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                suggested_plan_impacts=_PLAN_IMPACTS[category],
            )
        )
    return requirements


def _evidence_gaps(
    requirements: tuple[SourceEnterpriseProcurementRequirement, ...],
) -> list[SourceEnterpriseProcurementGap]:
    gaps: list[SourceEnterpriseProcurementGap] = []
    for requirement in requirements:
        if requirement.category not in _GATED_CATEGORIES:
            continue
        evidence_text = " ".join(requirement.evidence)
        missing: list[EnterpriseProcurementMissingField] = []
        if not _OWNER_RE.search(evidence_text):
            missing.append("owner")
        if not _DUE_DATE_RE.search(evidence_text):
            missing.append("due_date")
        if not _APPROVAL_EVIDENCE_RE.search(evidence_text):
            missing.append("approval_evidence")
        for field_name in missing:
            gaps.append(
                SourceEnterpriseProcurementGap(
                    category=requirement.category,
                    missing_field=field_name,
                    message=_gap_message(requirement.category, field_name),
                    evidence=requirement.evidence[:2],
                    confidence=requirement.confidence,
                )
            )
    return gaps


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
                _STRUCTURED_FIELD_RE.search(key_text) or _PROCUREMENT_CONTEXT_RE.search(key_text)
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
                _PROCUREMENT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _PROCUREMENT_CONTEXT_RE.search(part)
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
    if _CONSUMER_SCOPE_RE.search(searchable) and not _STRONG_ENTERPRISE_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if not (_PROCUREMENT_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words) or segment.section_context):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(_TIMELINE_RISK_RE.search(segment.text) and _PROCUREMENT_CONTEXT_RE.search(searchable))


def _value(category: EnterpriseProcurementCategory, text: str) -> str | None:
    patterns: dict[EnterpriseProcurementCategory, re.Pattern[str]] = {
        "security_review": re.compile(r"\b(?:security questionnaire|soc 2|iso 27001|security packet|vendor risk review|pen test)\b", re.I),
        "legal_review": re.compile(r"\b(?:msa|order form|redlines?|contract review|legal approval|terms review)\b", re.I),
        "purchase_order": re.compile(r"\b(?:po number|po issued|finance approval|invoice requires po)\b", re.I),
        "vendor_onboarding": re.compile(r"\b(?:vendor id|vendor portal|supplier portal|new vendor form|w[- ]?9|w[- ]?8ben)\b", re.I),
        "sso_scim_prerequisites": re.compile(r"\b(?:sso|saml|oidc|scim|okta|azure ad|entra id|user provisioning|deprovisioning)\b", re.I),
        "data_processing_agreement": re.compile(r"\b(?:dpa|data processing agreement|privacy addendum|subprocessor|sccs?|dtia)\b", re.I),
        "sla_requirements": re.compile(r"\b(?:uptime|availability|support response|response time|service credit|rto|rpo|sla)\b", re.I),
        "procurement_timeline": re.compile(r"\b(?:\d+\s*(?:business\s+)?(?:days?|weeks?|months?)|go[- ]?live|launch date|deadline|lead time|critical path)\b", re.I),
    }
    values = _dedupe(_clean_text(match.group(0)).casefold() for match in patterns[category].finditer(text))
    return ", ".join(values[:3]) or None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            0 if re.search(r"\d", value) else 1,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> EnterpriseProcurementConfidence:
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
                "constraints",
                "risks",
                "procurement",
                "enterprise",
                "security",
                "legal",
                "vendor",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _PROCUREMENT_CONTEXT_RE.search(searchable):
        return "medium"
    if _PROCUREMENT_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceEnterpriseProcurementRequirement, ...],
    gaps: tuple[SourceEnterpriseProcurementGap, ...],
) -> dict[str, Any]:
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
        "evidence_gap_count": len(gaps),
        "evidence_gaps": [f"{gap.category}:{gap.missing_field}" for gap in gaps],
        "status": _status(requirements, gaps),
    }


def _status(
    requirements: tuple[SourceEnterpriseProcurementRequirement, ...],
    gaps: tuple[SourceEnterpriseProcurementGap, ...],
) -> str:
    if not requirements:
        return "no_enterprise_procurement_language"
    if any(gap.missing_field == "approval_evidence" for gap in gaps):
        return "needs_procurement_approval_evidence"
    if gaps:
        return "needs_procurement_gate_detail"
    return "ready_for_planning"


def _gap_message(
    category: EnterpriseProcurementCategory,
    missing_field: EnterpriseProcurementMissingField,
) -> str:
    labels = {
        "owner": "Assign an owner or DRI for the procurement gate.",
        "due_date": "Specify the due date, approval deadline, launch date, or lead time for the procurement gate.",
        "approval_evidence": "Attach approval evidence such as sign-off, issued PO, signed agreement, ticket, or review record.",
    }
    return f"{category}: {labels[missing_field]}"


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: EnterpriseProcurementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[EnterpriseProcurementCategory, tuple[str, ...]] = {
        "security_review": ("security", "infosec", "risk", "questionnaire"),
        "legal_review": ("legal", "contract", "msa", "order form"),
        "purchase_order": ("purchase", "po", "finance"),
        "vendor_onboarding": ("vendor", "supplier", "onboarding"),
        "sso_scim_prerequisites": ("sso", "scim", "saml", "identity"),
        "data_processing_agreement": ("dpa", "data processing", "privacy"),
        "sla_requirements": ("sla", "service level", "support"),
        "procurement_timeline": ("timeline", "deadline", "risk", "launch"),
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
    "EnterpriseProcurementCategory",
    "EnterpriseProcurementConfidence",
    "EnterpriseProcurementMissingField",
    "SourceEnterpriseProcurementRequirement",
    "SourceEnterpriseProcurementGap",
    "SourceEnterpriseProcurementRequirementsReport",
    "build_source_enterprise_procurement_requirements",
    "derive_source_enterprise_procurement_requirements",
    "extract_source_enterprise_procurement_requirements",
    "generate_source_enterprise_procurement_requirements",
    "summarize_source_enterprise_procurement_requirements",
    "source_enterprise_procurement_requirements_to_dict",
    "source_enterprise_procurement_requirements_to_dicts",
    "source_enterprise_procurement_requirements_to_markdown",
]
