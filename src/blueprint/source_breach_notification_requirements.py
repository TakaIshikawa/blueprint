"""Extract source-level breach notification requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


BreachNotificationCategory = Literal[
    "detection_threshold",
    "notification_deadline",
    "affected_user_notice",
    "regulator_notice",
    "customer_contract_notice",
    "evidence_preservation",
    "communications_approval",
    "postmortem_requirement",
]
BreachNotificationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[BreachNotificationCategory, ...] = (
    "detection_threshold",
    "notification_deadline",
    "affected_user_notice",
    "regulator_notice",
    "customer_contract_notice",
    "evidence_preservation",
    "communications_approval",
    "postmortem_requirement",
)
_CONFIDENCE_ORDER: dict[BreachNotificationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_SUGGESTIONS: dict[BreachNotificationCategory, str] = {
    "detection_threshold": "security",
    "notification_deadline": "security",
    "affected_user_notice": "legal",
    "regulator_notice": "legal",
    "customer_contract_notice": "customer_success",
    "evidence_preservation": "security",
    "communications_approval": "communications",
    "postmortem_requirement": "engineering",
}
_PLANNING_NOTES: dict[BreachNotificationCategory, str] = {
    "detection_threshold": "Define breach and security incident severity triggers, triage ownership, and escalation gates.",
    "notification_deadline": "Track notification clocks, deadline calculations, timezones, pause rules, and escalation reminders.",
    "affected_user_notice": "Plan affected-user identification, notice content, delivery channels, support routing, and localization.",
    "regulator_notice": "Confirm regulator recipients, jurisdiction rules, filing evidence, approvals, and submission deadlines.",
    "customer_contract_notice": "Map customer contract notice terms, account contacts, tenant impact, and proof of delivery.",
    "evidence_preservation": "Preserve forensic evidence, logs, artifacts, chain of custody, legal hold, and retention controls.",
    "communications_approval": "Define legal, security, support, and communications approval workflow for external statements.",
    "postmortem_requirement": "Schedule incident postmortems, corrective actions, owners, deadlines, and executive reporting.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_BREACH_CONTEXT_RE = re.compile(
    r"\b(?:breach notification|data breach|personal data breach|security breach|security incident|"
    r"incident response|incident notice|breach notice|reportable incident|reportable breach|"
    r"unauthorized access|unauthori[sz]ed disclosure|data leak|exposure of personal data|"
    r"compromise(?:d)? data|privacy incident|regulatory notification|affected users?|"
    r"customer notice|contract notice|forensic evidence|postmortem|post[- ]incident review)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:breach|incident|notification|notice|deadline|regulator|regulatory|authority|customer|"
    r"contract|affected|user|data[_ -]?subject|evidence|forensic|preservation|legal[_ -]?hold|"
    r"communications?|approval|postmortem|post[_ -]?incident|security|privacy|compliance|"
    r"requirements?|acceptance|criteria|source[_ -]?payload|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"define|document|record|track|notify|notice|report|file|submit|preserve|retain|retained|hold|"
    r"approve|review|escalate|complete|publish|send|disclose|cannot ship|done when|acceptance)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,140}\b(?:breach notification|data breach|security incident|"
    r"incident response|reportable breach|regulatory notification|affected user notice|customer notice)\b"
    r".{0,120}\b(?:required|needed|in scope|planned|changes?|impact|work|requirements?)\b|"
    r"\b(?:breach notification|data breach|security incident|incident response|reportable breach|"
    r"regulatory notification|affected user notice|customer notice)\b.{0,140}\b(?:not required|"
    r"not needed|out of scope|no changes?|no work|no impact|non[- ]?goal|unaffected|not impacted)\b|"
    r"\b(?:no customer impact|no user impact|no affected users?|no personal data (?:was )?(?:accessed|exposed|impacted)|"
    r"no regulated data (?:was )?(?:accessed|exposed|impacted)|incident is informational only)\b",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "privacy",
    "compliance",
    "legal",
    "incident_response",
    "breach_notification",
    "communications",
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
_CATEGORY_PATTERNS: dict[BreachNotificationCategory, re.Pattern[str]] = {
    "detection_threshold": re.compile(
        r"\b(?:detection threshold|notification threshold|reportable incident|reportable breach|"
        r"material incident|severity(?: level)?|sev[ -]?[0123]|personal data breach|"
        r"unauthori[sz]ed (?:access|disclosure)|data exfiltration|confirmed breach|"
        r"reasonable belief|determination of breach|risk of harm|trigger(?:s|ed)? notification)\b",
        re.I,
    ),
    "notification_deadline": re.compile(
        r"\b(?:notification deadline|notify within|notice within|report within|within \d+|"
        r"no later than|not later than|without undue delay|as soon as practicable|"
        r"same day|next business day|\d+\s*(?:hour|day|calendar day|business day)s?)\b",
        re.I,
    ),
    "affected_user_notice": re.compile(
        r"\b(?:affected users?|impacted users?|data subjects?|individuals?|consumers?|end users?|"
        r"user notice|notify users?|notice to users?|customer-facing notice|breach email|"
        r"substitute notice|in-app notice|user communications?)\b",
        re.I,
    ),
    "regulator_notice": re.compile(
        r"\b(?:regulators?|regulatory|supervisory authority|data protection authority|dpa|attorney general|"
        r"state authority|government authority|authority filing|regulatory filing|notify authorities|"
        r"report to authorities|gdpr|hipaa|state breach law)\b",
        re.I,
    ),
    "customer_contract_notice": re.compile(
        r"\b(?:customer contract notice|contractual notice|contract notice|enterprise customers?|"
        r"customer admins?|customer security contacts?|tenant admins?|dpa notice|msa notice|"
        r"sla notice|contractually required|subprocessor notice|account contacts?)\b",
        re.I,
    ),
    "evidence_preservation": re.compile(
        r"\b(?:preserve|preservation|retain|retention|forensic evidence|forensics?|chain of custody|"
        r"legal hold|litigation hold|audit logs?|security logs?|snapshots?|artifacts?|"
        r"timeline evidence|evidence package)\b",
        re.I,
    ),
    "communications_approval": re.compile(
        r"\b(?:communications? approval|legal approval|approved by legal|comms approval|"
        r"public statement|external statement|press statement|customer communication approval|"
        r"security approval|support approval|executive approval|approved messaging|message template)\b",
        re.I,
    ),
    "postmortem_requirement": re.compile(
        r"\b(?:postmortem|post[- ]incident review|incident review|root cause analysis|rca|"
        r"lessons learned|corrective actions?|remediation plan|after[- ]action review|"
        r"executive readout|follow[- ]up actions?)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[BreachNotificationCategory, re.Pattern[str]] = {
    category: re.compile(category.replace("_", r"[_ -]?"), re.I) for category in _CATEGORY_ORDER
}
_DEADLINE_RE = re.compile(
    r"\b(?P<value>(?:within|no later than|not later than|after|by|before|inside)\s*"
    r"\d+(?:\.\d+)?\s*(?:minutes?|mins?|hours?|business days?|calendar days?|days?|weeks?))\b|"
    r"\b(?P<named>without undue delay|as soon as practicable|same day|next business day|immediately)\b",
    re.I,
)
_AFFECTED_PARTY_RE = re.compile(
        r"\b(?P<party>affected users?|impacted users?|data subjects?|individuals?|consumers?|end users?|"
    r"enterprise customers?|customer admins?|customer security contacts?|tenant admins?|account contacts?|"
    r"regulators?|supervisory authorit(?:y|ies)|data protection authorit(?:y|ies)|"
    r"attorneys? general|state authorities)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceBreachNotificationRequirement:
    """One source-backed breach notification requirement."""

    source_brief_id: str | None
    category: BreachNotificationCategory
    requirement_text: str
    notification_deadline: str | None = None
    affected_party: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: BreachNotificationConfidence = "medium"
    owner_suggestion: str = ""
    planning_note: str = ""

    @property
    def requirement_category(self) -> BreachNotificationCategory:
        """Compatibility alias matching category-oriented reports."""
        return self.category

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    @property
    def owner_suggestions(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural owner suggestions."""
        return (self.owner_suggestion,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "notification_deadline": self.notification_deadline,
            "affected_party": self.affected_party,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "owner_suggestion": self.owner_suggestion,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceBreachNotificationRequirementsReport:
    """Source-level breach notification requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceBreachNotificationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBreachNotificationRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceBreachNotificationRequirement, ...]:
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
        """Return breach notification requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Breach Notification Requirements Report"
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
            lines.extend(["", "No breach notification requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Deadline | Affected Party | Source Field | Matched Terms | Confidence | Owner | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.notification_deadline or '')} | "
                f"{_markdown_cell(requirement.affected_party or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.owner_suggestion)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_breach_notification_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBreachNotificationRequirementsReport:
    """Extract source-level breach notification requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceBreachNotificationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_breach_notification_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBreachNotificationRequirementsReport:
    """Compatibility alias for building a breach notification requirements report."""
    return build_source_breach_notification_requirements(source)


def generate_source_breach_notification_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBreachNotificationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_breach_notification_requirements(source)


def derive_source_breach_notification_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBreachNotificationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_breach_notification_requirements(source)


def summarize_source_breach_notification_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceBreachNotificationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted breach notification requirements."""
    if isinstance(source_or_result, SourceBreachNotificationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_breach_notification_requirements(source_or_result).summary


def source_breach_notification_requirements_to_dict(
    report: SourceBreachNotificationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a breach notification requirements report to a plain dictionary."""
    return report.to_dict()


source_breach_notification_requirements_to_dict.__test__ = False


def source_breach_notification_requirements_to_dicts(
    requirements: (
        tuple[SourceBreachNotificationRequirement, ...]
        | list[SourceBreachNotificationRequirement]
        | SourceBreachNotificationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize breach notification requirement records to dictionaries."""
    if isinstance(requirements, SourceBreachNotificationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_breach_notification_requirements_to_dicts.__test__ = False


def source_breach_notification_requirements_to_markdown(
    report: SourceBreachNotificationRequirementsReport,
) -> str:
    """Render a breach notification requirements report as Markdown."""
    return report.to_markdown()


source_breach_notification_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: BreachNotificationCategory
    requirement_text: str
    notification_deadline: str | None
    affected_party: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: BreachNotificationConfidence


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
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_RE.search(searchable):
                continue
            for category in _categories(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        notification_deadline=_match_deadline(segment.text)
                        or _field_value_detail("notification_deadline", segment.text)
                        or _field_value_detail("deadline", segment.text),
                        affected_party=_field_value_detail("affected_party", segment.text)
                        or _field_value_detail("party", segment.text)
                        or _match_affected_party(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(category, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceBreachNotificationRequirement]:
    grouped: dict[tuple[str | None, BreachNotificationCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.category, _dedupe_requirement_key(candidate.requirement_text)),
            [],
        ).append(candidate)

    requirements: list[SourceBreachNotificationRequirement] = []
    for (_source_brief_id, category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceBreachNotificationRequirement(
                source_brief_id=best.source_brief_id,
                category=category,
                requirement_text=best.requirement_text,
                notification_deadline=_first_detail(item.notification_deadline for item in items),
                affected_party=_first_detail(item.affected_party for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                owner_suggestion=_OWNER_SUGGESTIONS[category],
                planning_note=_PLANNING_NOTES[category],
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
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _BREACH_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_BREACH_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _BREACH_CONTEXT_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[BreachNotificationCategory, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    field_categories = [category for category in _CATEGORY_ORDER if _FIELD_CATEGORY_PATTERNS[category].search(field_words)]
    text_categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(segment.text)]
    has_breach_context = bool(_BREACH_CONTEXT_RE.search(searchable))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_explicit_category = bool(field_categories or text_categories)
    if not (has_breach_context or has_structured_context or has_explicit_category):
        return ()
    if not (_REQUIREMENT_RE.search(searchable) or has_structured_context):
        return ()
    if (
        _BREACH_CONTEXT_RE.search(segment.text)
        and _REQUIREMENT_RE.search(segment.text)
        and not text_categories
        and not field_categories
    ):
        text_categories.append("detection_threshold")
    return tuple(_dedupe(field_categories + text_categories))


def _confidence(category: BreachNotificationCategory, segment: _Segment) -> BreachNotificationConfidence:
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_category = bool(_CATEGORY_PATTERNS[category].search(segment.text) or _FIELD_CATEGORY_PATTERNS[category].search(field_words))
    has_detail = bool(_match_deadline(segment.text) or _match_affected_party(segment.text))
    if has_category and has_explicit_requirement and has_structured_context and has_detail:
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceBreachNotificationRequirement, ...], source_count: int) -> dict[str, Any]:
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
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
        "affected_parties": sorted(
            _dedupe(requirement.affected_party for requirement in requirements if requirement.affected_party),
            key=str.casefold,
        ),
        "notification_deadlines": sorted(
            _dedupe(requirement.notification_deadline for requirement in requirements if requirement.notification_deadline),
            key=str.casefold,
        ),
        "requires_deadline_tracking": any(requirement.notification_deadline for requirement in requirements),
        "requires_affected_user_notice": any(requirement.category == "affected_user_notice" for requirement in requirements),
        "requires_regulator_notice": any(requirement.category == "regulator_notice" for requirement in requirements),
        "requires_customer_contract_notice": any(requirement.category == "customer_contract_notice" for requirement in requirements),
        "requires_evidence_preservation": any(requirement.category == "evidence_preservation" for requirement in requirements),
        "requires_communications_approval": any(requirement.category == "communications_approval" for requirement in requirements),
        "requires_postmortem": any(requirement.category == "postmortem_requirement" for requirement in requirements),
        "status": "ready_for_breach_notification_planning" if requirements else "no_breach_notification_language",
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "category",
            "requirement_category",
            "notification_deadline",
            "affected_party",
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
        "security",
        "privacy",
        "compliance",
        "legal",
        "incident_response",
        "breach_notification",
        "communications",
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


def _matched_terms(category: BreachNotificationCategory, text: str) -> tuple[str, ...]:
    return tuple(sorted(_dedupe(_clean_text(match.group(0)).casefold() for match in _CATEGORY_PATTERNS[category].finditer(text)), key=str.casefold))


def _match_deadline(text: str) -> str | None:
    match = _DEADLINE_RE.search(text)
    if not match:
        return None
    return _clean_text(match.group("value") or match.group("named")).rstrip(".").casefold()


def _match_affected_party(text: str) -> str | None:
    matches = [_clean_text(match.group("party")).rstrip(".").casefold() for match in _AFFECTED_PARTY_RE.finditer(text)]
    if not matches:
        return None
    return max(matches, key=len)


def _field_value_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;]+)", re.I)
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
    "BreachNotificationCategory",
    "BreachNotificationConfidence",
    "SourceBreachNotificationRequirement",
    "SourceBreachNotificationRequirementsReport",
    "build_source_breach_notification_requirements",
    "derive_source_breach_notification_requirements",
    "extract_source_breach_notification_requirements",
    "generate_source_breach_notification_requirements",
    "source_breach_notification_requirements_to_dict",
    "source_breach_notification_requirements_to_dicts",
    "source_breach_notification_requirements_to_markdown",
    "summarize_source_breach_notification_requirements",
]
