"""Extract source-level appeal workflow requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AppealWorkflowRequirementCategory = Literal[
    "appeal_submission",
    "evidence_collection",
    "reviewer_assignment",
    "sla_response_timing",
    "customer_notifications",
    "reversal_remediation",
    "audit_trail",
    "escalation_policy",
]
AppealWorkflowRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[AppealWorkflowRequirementCategory, ...] = (
    "appeal_submission",
    "evidence_collection",
    "reviewer_assignment",
    "sla_response_timing",
    "customer_notifications",
    "reversal_remediation",
    "audit_trail",
    "escalation_policy",
)
_CONFIDENCE_ORDER: dict[AppealWorkflowRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_OWNER_BY_CATEGORY: dict[AppealWorkflowRequirementCategory, str] = {
    "appeal_submission": "product",
    "evidence_collection": "operations",
    "reviewer_assignment": "operations",
    "sla_response_timing": "support",
    "customer_notifications": "support",
    "reversal_remediation": "operations",
    "audit_trail": "compliance",
    "escalation_policy": "trust_and_safety",
}
_PLANNING_NOTES: dict[AppealWorkflowRequirementCategory, str] = {
    "appeal_submission": "Define appeal eligibility, submission channels, required reason fields, and intake states.",
    "evidence_collection": "Specify evidence attachments, enforcement context, reason codes, and reviewer-visible case history.",
    "reviewer_assignment": "Plan queue routing, reviewer independence, permissions, workload balancing, and conflict checks.",
    "sla_response_timing": "Set response SLAs, appeal windows, timeout handling, and overdue escalation behavior.",
    "customer_notifications": "Design submission, status, decision, and remediation notifications across supported channels.",
    "reversal_remediation": "Define reversal outcomes, reinstatement steps, refunds or unlocks, and customer remediation ownership.",
    "audit_trail": "Record appeal actions, actors, timestamps, evidence, decision rationale, and retention expectations.",
    "escalation_policy": "Document escalation triggers, specialist ownership, legal or safety handoffs, and override policy.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_APPEAL_CONTEXT_RE = re.compile(
    r"\b(?:appeals?|appeal workflow|appeal process|dispute|contest|challenge|request review|"
    r"second review|reconsideration|reinstatement request|restore access|enforcement review|"
    r"removal review|account lockout review|fraud hold review|chargeback restriction review|"
    r"access denial review|denial review|hold review|case review)\b",
    re.I,
)
_ENFORCEMENT_CONTEXT_RE = re.compile(
    r"\b(?:moderation removal|content removal|removed content|post removal|account lock(?:out|ed)|"
    r"account suspension|suspended account|ban|disabled account|fraud hold|risk hold|payment hold|"
    r"chargeback restriction|chargeback hold|access denial|denied access|enforcement action|"
    r"restriction|restricted account|blocked account|locked account|denial)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:appeals?|appeal[_ -]?workflow|moderation|fraud|support|enforcement|lockout|"
    r"chargeback|access[_ -]?denial|denial|review|evidence|sla|response|notification|"
    r"remediation|reversal|audit|escalation|acceptance|requirements?|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|define|"
    r"allow|support|provide|enable|submit|collect|attach|assign|route|review|respond|notify|"
    r"send|reverse|restore|reinstate|unlock|refund|remediate|record|log|audit|escalate|"
    r"policy|workflow|process|acceptance|done when|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:appeals?|appeal workflow|disputes?|contests?|second review|reinstatement requests?)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|planned|changes?|for this release|work)\b|"
    r"\b(?:appeals?|appeal workflow|disputes?|contests?|second review|reinstatement requests?)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[AppealWorkflowRequirementCategory, re.Pattern[str]] = {
    "appeal_submission": re.compile(
        r"\b(?:submit appeals?|appeal submission|appeal form|appeal request|appeal intake|"
        r"file an appeal|open an appeal|appeal reason|dispute submission|contest (?:removal|enforcement|decision)|"
        r"challenge (?:removal|lockout|hold|denial|restriction)|reinstatement request)\b",
        re.I,
    ),
    "evidence_collection": re.compile(
        r"\b(?:evidence collection|collect evidence|collect attachments?|attachments?|screenshots?|supporting documents?|case context|reason codes?|"
        r"enforcement context|decision rationale|prior notices?|transaction history|moderation history|"
        r"chargeback evidence|dispute evidence)\b",
        re.I,
    ),
    "reviewer_assignment": re.compile(
        r"\b(?:reviewer assignment|assign reviewers?|route to reviewer|appeal queue|review queue|"
        r"second reviewer|independent reviewer|human review|manual review|specialist review|"
        r"support reviewer|trust reviewer|fraud reviewer|moderator review)\b",
        re.I,
    ),
    "sla_response_timing": re.compile(
        r"\b(?:sla|response time|respond within|appeal window|within \d+\s*(?:minutes?|hours?|days?|business days)|"
        r"\d+\s*(?:minutes?|hours?|days?|business days)|deadline|due within|timing|turnaround|overdue)\b",
        re.I,
    ),
    "customer_notifications": re.compile(
        r"\b(?:notify|notification|email|sms|in[- ]app|webhook|status update|decision notices?|"
        r"submission receipt|appeal received|customer message|tell users?|inform customers?)\b",
        re.I,
    ),
    "reversal_remediation": re.compile(
        r"\b(?:reverse|reversal|overturn|restore|reinstate|unlock|unban|refund|release hold|"
        r"lift restriction|restore access|restore content|remediate|remediation|make[- ]good|correct enforcement)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audit history|case history|decision history|appeal history|"
        r"record decisions?|log decisions?|who reviewed|timestamps?|retention|evidence log|compliance export)\b",
        re.I,
    ),
    "escalation_policy": re.compile(
        r"\b(?:escalation policy|escalate|urgent escalation|specialist escalation|legal escalation|"
        r"trust and safety escalation|safety escalation|fraud escalation|supervisor review|"
        r"manager review|policy override|high[- ]risk appeal)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[AppealWorkflowRequirementCategory, re.Pattern[str]] = {
    category: re.compile(category.replace("_", r"[_ -]?"), re.I) for category in _CATEGORY_ORDER
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
    "moderation",
    "fraud",
    "support",
    "security",
    "compliance",
    "operations",
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
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceAppealWorkflowRequirement:
    """One source-backed appeal workflow requirement."""

    source_brief_id: str | None
    category: AppealWorkflowRequirementCategory
    requirement_text: str
    enforcement_context: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: AppealWorkflowRequirementConfidence = "medium"
    suggested_owner: str = "product"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> AppealWorkflowRequirementCategory:
        """Compatibility alias for callers expecting requirement_category naming."""
        return self.category

    @property
    def suggested_planning_note(self) -> str:
        """Compatibility alias matching other source requirement reports."""
        return self.planning_note

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "enforcement_context": self.enforcement_context,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "planning_note": self.planning_note,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceAppealWorkflowRequirementsReport:
    """Source-level appeal workflow requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAppealWorkflowRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAppealWorkflowRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAppealWorkflowRequirement, ...]:
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
        """Return appeal workflow requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Appeal Workflow Requirements Report"
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
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source appeal workflow requirements were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Enforcement Context | Source Field | Confidence | Owner | Planning Note | Unresolved Questions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.enforcement_context or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.suggested_owner)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_appeal_workflow_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAppealWorkflowRequirementsReport:
    """Extract source-level appeal workflow requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAppealWorkflowRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_appeal_workflow_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAppealWorkflowRequirementsReport:
    """Compatibility alias for building an appeal workflow requirements report."""
    return build_source_appeal_workflow_requirements(source)


def generate_source_appeal_workflow_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAppealWorkflowRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_appeal_workflow_requirements(source)


def derive_source_appeal_workflow_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAppealWorkflowRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_appeal_workflow_requirements(source)


def summarize_source_appeal_workflow_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAppealWorkflowRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted appeal workflow requirements."""
    if isinstance(source_or_result, SourceAppealWorkflowRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_appeal_workflow_requirements(source_or_result).summary


def source_appeal_workflow_requirements_to_dict(
    report: SourceAppealWorkflowRequirementsReport,
) -> dict[str, Any]:
    """Serialize an appeal workflow requirements report to a plain dictionary."""
    return report.to_dict()


source_appeal_workflow_requirements_to_dict.__test__ = False


def source_appeal_workflow_requirements_to_dicts(
    requirements: (
        tuple[SourceAppealWorkflowRequirement, ...]
        | list[SourceAppealWorkflowRequirement]
        | SourceAppealWorkflowRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize appeal workflow requirement records to dictionaries."""
    if isinstance(requirements, SourceAppealWorkflowRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_appeal_workflow_requirements_to_dicts.__test__ = False


def source_appeal_workflow_requirements_to_markdown(
    report: SourceAppealWorkflowRequirementsReport,
) -> str:
    """Render an appeal workflow requirements report as Markdown."""
    return report.to_markdown()


source_appeal_workflow_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: AppealWorkflowRequirementCategory
    requirement_text: str
    enforcement_context: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: AppealWorkflowRequirementConfidence


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        if _brief_out_of_scope(payload):
            continue
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_SCOPE_RE.search(searchable):
                continue
            for category in _categories(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        enforcement_context=_enforcement_context(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(category, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAppealWorkflowRequirement]:
    grouped: dict[tuple[str | None, AppealWorkflowRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.category,
                _dedupe_requirement_key(candidate.requirement_text, candidate.category),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceAppealWorkflowRequirement] = []
    for (_source_brief_id, category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceAppealWorkflowRequirement(
                source_brief_id=best.source_brief_id,
                category=category,
                requirement_text=best.requirement_text,
                enforcement_context=_first_detail(item.enforcement_context for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(
                    sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)
                ),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                suggested_owner=_OWNER_BY_CATEGORY[category],
                planning_note=_PLANNING_NOTES[category],
                unresolved_questions=_unresolved_questions(category, items),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_appeal_context(payload)
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
            for text in _structured_segments(value):
                segments.append(_Segment(source_field, text, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _APPEAL_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
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
            section_context = inherited_context or bool(_APPEAL_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title and not _NEGATED_SCOPE_RE.search(title):
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if any(pattern.search(part) for pattern in _CATEGORY_PATTERNS.values()) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[AppealWorkflowRequirementCategory, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    explicit_category = _explicit_category(segment.text)
    if explicit_category:
        return (explicit_category,)
    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
    ]
    matched_categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    has_appeal_context = bool(
        _APPEAL_CONTEXT_RE.search(searchable)
        or segment.section_context
        or field_categories
    )
    if not has_appeal_context:
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or field_categories
        or matched_categories
    ):
        return ()
    return tuple(_dedupe(field_categories + matched_categories))


def _explicit_category(text: str) -> AppealWorkflowRequirementCategory | None:
    match = re.search(r"\b(?:category|appeal_category|requirement_category):\s*([a-zA-Z0-9_ -]+?)(?:;|$)", text)
    if not match:
        return None
    value = match.group(1).strip().casefold().replace("-", "_").replace(" ", "_")
    return value if value in _CATEGORY_ORDER else None


def _matched_terms(category: AppealWorkflowRequirementCategory, text: str) -> tuple[str, ...]:
    return tuple(
        _dedupe(_clean_text(match.group(0)) for match in _CATEGORY_PATTERNS[category].finditer(text))
    )


def _confidence(category: AppealWorkflowRequirementCategory, segment: _Segment) -> AppealWorkflowRequirementConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_category = bool(_CATEGORY_PATTERNS[category].search(searchable))
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_enforcement = bool(_ENFORCEMENT_CONTEXT_RE.search(searchable) or _enforcement_context(segment.text))
    if has_category and has_explicit_requirement and has_structured_context and has_enforcement:
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceAppealWorkflowRequirement, ...], source_count: int) -> dict[str, Any]:
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
        "owner_counts": {
            owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
            for owner in sorted(_dedupe(_OWNER_BY_CATEGORY.values()), key=str.casefold)
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
        "status": "ready_for_appeal_workflow_planning" if requirements else "no_appeal_workflow_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_SCOPE_RE.search(scoped_text) and not _positive_appeal_requirement(scoped_text))


def _brief_appeal_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_APPEAL_CONTEXT_RE.search(scoped_text) and not _NEGATED_SCOPE_RE.search(scoped_text))


def _positive_appeal_requirement(text: str) -> bool:
    return bool(_APPEAL_CONTEXT_RE.search(text) and _REQUIREMENT_RE.search(text) and not _NEGATED_SCOPE_RE.search(text))


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, Mapping) for value in item.values()):
        return False
    if not (keys & {"category", "appeal_category", "requirement_category"}) and keys & set(_CATEGORY_ORDER):
        return False
    if keys <= {"appeal", "appeals", "moderation", "fraud", "support", "requirements"} and any(
        isinstance(value, (Mapping, list, tuple, set)) for value in item.values()
    ):
        return False
    return bool(
        keys
        & {
            "category",
            "appeal_category",
            "requirement_category",
            "appeal_submission",
            "evidence_collection",
            "reviewer_assignment",
            "sla_response_timing",
            "customer_notifications",
            "reversal_remediation",
            "audit_trail",
            "escalation_policy",
            "enforcement_context",
            "evidence",
            "reviewer",
            "sla",
            "notification",
            "remediation",
            "audit",
            "escalation",
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
        "non_goals",
        "assumptions",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "moderation",
        "fraud",
        "support",
        "security",
        "compliance",
        "operations",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, int, str]:
    return (
        int(bool(candidate.enforcement_context)),
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_source_index(candidate.source_field),
        candidate.evidence,
    )


def _source_index(source_field: str) -> int:
    if match := re.search(r"\[(\d+)\]", source_field):
        return int(match.group(1))
    return 0


def _unresolved_questions(
    category: AppealWorkflowRequirementCategory, items: Iterable[_Candidate]
) -> tuple[str, ...]:
    candidates = tuple(items)
    text = " ".join(candidate.requirement_text for candidate in candidates)
    questions: list[str] = []
    if not any(candidate.enforcement_context for candidate in candidates):
        questions.append("Which enforcement action or denial makes the appeal eligible?")
    if category in {"appeal_submission", "evidence_collection"} and not re.search(r"\b(?:reason|field|form|attachment|evidence|document)\b", text, re.I):
        questions.append("What appeal reason fields or evidence are required?")
    if category in {"reviewer_assignment", "escalation_policy"} and not re.search(r"\b(?:reviewer|queue|owner|team|specialist|manager|legal|trust|fraud|support)\b", text, re.I):
        questions.append("Who owns review or escalation decisions?")
    if category in {"sla_response_timing", "customer_notifications"} and not re.search(r"\b(?:within|sla|days?|hours?|email|sms|in-app|notify|notification)\b", text, re.I):
        questions.append("What timing or customer communication rule applies?")
    return tuple(questions)


def _enforcement_context(text: str) -> str | None:
    if match := re.search(r"(?:^|;\s*)enforcement_context:\s*([^;]+)", text, re.I):
        return _detail(match.group(1))
    if match := _ENFORCEMENT_CONTEXT_RE.search(text):
        return _detail(match.group(0))
    return None


def _first_detail(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


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


def _detail(value: Any) -> str | None:
    text = _clean_text(value).strip("`'\" ;,.")
    return text[:160].rstrip() if text else None


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


def _dedupe_requirement_key(value: str, category: AppealWorkflowRequirementCategory) -> str:
    text = _clean_text(value).casefold()
    return f"{category}:{_SPACE_RE.sub(' ', text).strip()}"


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
    "AppealWorkflowRequirementCategory",
    "AppealWorkflowRequirementConfidence",
    "SourceAppealWorkflowRequirement",
    "SourceAppealWorkflowRequirementsReport",
    "build_source_appeal_workflow_requirements",
    "derive_source_appeal_workflow_requirements",
    "extract_source_appeal_workflow_requirements",
    "generate_source_appeal_workflow_requirements",
    "source_appeal_workflow_requirements_to_dict",
    "source_appeal_workflow_requirements_to_dicts",
    "source_appeal_workflow_requirements_to_markdown",
    "summarize_source_appeal_workflow_requirements",
]
