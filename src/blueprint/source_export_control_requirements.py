"""Extract source-level export-control requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ExportControlCategory = Literal[
    "sanctions_screening",
    "restricted_country",
    "export_classification",
    "denied_party_check",
    "technology_transfer",
    "residence_gating",
    "audit_evidence",
]
ExportControlConfidence = Literal["high", "medium", "low"]
ExportControlReadiness = Literal["ready", "needs_detail"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ExportControlCategory, ...] = (
    "sanctions_screening",
    "restricted_country",
    "export_classification",
    "denied_party_check",
    "technology_transfer",
    "residence_gating",
    "audit_evidence",
)
_CONFIDENCE_ORDER: dict[ExportControlConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_DETAIL_ORDER: tuple[str, ...] = (
    "jurisdiction",
    "screening_provider",
    "denied_party_action",
    "review_owner",
    "renewal_cadence",
)
_PLANNING_NOTES: dict[ExportControlCategory, str] = {
    "sanctions_screening": "Plan sanctions screening provider integration, match handling, rescreen cadence, and audit logging.",
    "restricted_country": "Define country blocklists, geo and residence inputs, exception handling, and customer messaging.",
    "export_classification": "Capture export classification such as EAR, ITAR, ECCN, license exceptions, and review ownership.",
    "denied_party_check": "Implement denied-party matching, false-positive review, account or transaction holds, and evidence retention.",
    "technology_transfer": "Review restricted technology transfer, deemed export, source code, data export, and access-control paths.",
    "residence_gating": "Gate onboarding or access by country of residence, citizenship, billing country, IP, and operational overrides.",
    "audit_evidence": "Record screening decisions, review outcomes, provider responses, renewal evidence, and compliance audit trails.",
}
_CATEGORY_PATTERNS: dict[ExportControlCategory, re.Pattern[str]] = {
    "sanctions_screening": re.compile(
        r"\b(?:ofac|sanctions?|sanctioned (?:person|entity|country)|sanctions screening|"
        r"screen(?:ed|ing)? against sanctions|sdn list|special(?:ly)? designated nationals?|"
        r"treasury list|consolidated sanctions list)\b",
        re.I,
    ),
    "restricted_country": re.compile(
        r"\b(?:embargo(?:ed)? countries?|restricted countries?|blocked countries?|prohibited countries?|"
        r"country blocklist|country blacklist|cuba|iran|north korea|syria|crimea|donetsk|luhansk|"
        r"russia|belarus|export restricted territor(?:y|ies))\b",
        re.I,
    ),
    "export_classification": re.compile(
        r"\b(?:export classification|export control classification|eccn|ear99|usml|"
        r"dual[- ]use|commerce control list|ccl|license exception|export license|classification review)\b",
        re.I,
    ),
    "denied_party_check": re.compile(
        r"\b(?:denied[- ]party|restricted[- ]party|screen(?:ed|ing)? parties|party screening|"
        r"entity list|denied persons list|dpl|unverified list|uvl|blocked person|watchlist match)\b",
        re.I,
    ),
    "technology_transfer": re.compile(
        r"\b(?:technology transfer|restricted technology|controlled technology|technical data|deemed export|"
        r"source code export|model weights?|data transfer|access by foreign nationals?|"
        r"cross[- ]border access|export controlled data)\b",
        re.I,
    ),
    "residence_gating": re.compile(
        r"\b(?:country of residence|residence country|citizenship|nationality|billing country|ip geolocation|"
        r"geo[- ]?gate|location gate|residency gate|gate onboarding|block signup|country eligibility)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit trail|screening evidence|screening logs?|compliance logs?|"
        r"export control records?|retain records?|retention|evidence of screening|review evidence|"
        r"provider response|screening result)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[ExportControlCategory, re.Pattern[str]] = {
    category: re.compile(category.replace("_", r"[_ -]?"), re.I) for category in _CATEGORY_ORDER
}
_EXPORT_CONTEXT_RE = re.compile(
    r"\b(?:export control|export controls?|sanctions?|ofac|embargo|restricted countr|denied party|"
    r"restricted party|ear|itar|eccn|technology transfer|deemed export|country of residence|audit evidence)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:export[_ -]?control|sanctions?|ofac|embargo|restricted[_ -]?countr|denied[_ -]?party|"
    r"restricted[_ -]?party|export[_ -]?classification|eccn|ear|itar|technology[_ -]?transfer|"
    r"residence|country[_ -]?gate|audit|evidence|compliance|screening)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|define|"
    r"screen|check|verify|block|deny|hold|freeze|reject|gate|prevent|restrict|classify|review|"
    r"approve|log|record|retain|audit|rescreen|renew|monitor|escalate|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,120}\b(?:export controls?|sanctions?|ofac|embargo|denied party|"
    r"restricted party|ear|itar|eccn|technology transfer|country gating)\b.{0,120}\b(?:required|needed|"
    r"in scope|planned|changes?|impact|work)\b|"
    r"\b(?:export controls?|sanctions?|ofac|embargo|denied party|restricted party|ear|itar|eccn|"
    r"technology transfer|country gating)\b.{0,120}\b(?:not required|not needed|out of scope|"
    r"no changes?|no work|non[- ]?goal)\b",
    re.I,
)
_JURISDICTION_RE = re.compile(
    r"\b(?:jurisdiction|regime|under|per|for)\s*:?\s*((?:US|U\.S\.|United States|EU|UK|OFAC|EAR|ITAR|BIS|DDTC|"
    r"Commerce|State Department|Canada|Australia|Japan|Singapore)[^.;,\n]*)",
    re.I,
)
_PROVIDER_RE = re.compile(
    r"\b(?:provider|vendor|screening provider|screening vendor|use|using|integrate with)\s*:?\s*"
    r"([A-Z][A-Za-z0-9& ._-]{1,60}|(?:ComplyAdvantage|Dow Jones|LexisNexis|Refinitiv|World-Check|Stripe Identity|Persona))",
    re.I,
)
_NAMED_PROVIDER_RE = re.compile(
    r"\b(?:use|using|require|requires?|with|integrate with)\s+"
    r"((?:ComplyAdvantage|Dow Jones|LexisNexis|Refinitiv|World-Check|Stripe Identity|Persona)"
    r"(?:\s+screening provider)?)\b",
    re.I,
)
_ACTION_RE = re.compile(
    r"\b(?:block|deny|hold|freeze|reject|suspend|disable|manual review|escalate|route to review|"
    r"stop onboarding|prevent access|cancel payout|pause transfer)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:review owner|owner|reviewed by|approved by|escalate to|route to)\s*:?\s*"
    r"([^.;,\n]*(?:legal|compliance|export control|trust|risk|operations|ops|finance|support|security)[^.;,\n]*)",
    re.I,
)
_CADENCE_RE = re.compile(
    r"\b(?:renewal cadence|rescreen|re-screen|refresh|recheck|review cadence|renew|recertify)\s*:?\s*"
    r"([^.;\n]*(?:daily|weekly|monthly|quarterly|annually|annual|yearly|every\s+\d+\s+(?:days?|weeks?|months?|years?))[^.;\n]*)",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "security",
    "compliance",
    "legal",
    "export_control",
    "sanctions",
    "metadata",
    "brief_metadata",
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
class SourceExportControlRequirement:
    """One source-backed export-control requirement."""

    source_brief_id: str | None
    requirement_category: ExportControlCategory
    requirement_text: str
    jurisdiction: str | None = None
    screening_provider: str | None = None
    denied_party_action: str | None = None
    review_owner: str | None = None
    renewal_cadence: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: ExportControlConfidence = "medium"
    readiness: ExportControlReadiness = "needs_detail"
    planning_note: str = ""

    @property
    def category(self) -> ExportControlCategory:
        """Compatibility alias for callers expecting category naming."""
        return self.requirement_category

    @property
    def export_control_category(self) -> ExportControlCategory:
        """Compatibility alias for callers expecting a domain-specific category name."""
        return self.requirement_category

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_category": self.requirement_category,
            "requirement_text": self.requirement_text,
            "jurisdiction": self.jurisdiction,
            "screening_provider": self.screening_provider,
            "denied_party_action": self.denied_party_action,
            "review_owner": self.review_owner,
            "renewal_cadence": self.renewal_cadence,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "readiness": self.readiness,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceExportControlRequirementsReport:
    """Source-level export-control requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceExportControlRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceExportControlRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceExportControlRequirement, ...]:
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
        """Return export-control requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Export Control Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        readiness_counts = self.summary.get("readiness_counts", {})
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
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in ("ready", "needs_detail")),
        ]
        if not self.requirements:
            lines.extend(["", "No export-control requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Jurisdiction | Provider | Action | Owner | Renewal | Source Field | Missing Details | Confidence | Readiness | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.requirement_category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.jurisdiction or '')} | "
                f"{_markdown_cell(requirement.screening_provider or '')} | "
                f"{_markdown_cell(requirement.denied_party_action or '')} | "
                f"{_markdown_cell(requirement.review_owner or '')} | "
                f"{_markdown_cell(requirement.renewal_cadence or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.readiness)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_export_control_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceExportControlRequirementsReport:
    """Extract source-level export-control requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceExportControlRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_export_control_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceExportControlRequirementsReport:
    """Compatibility alias for building an export-control requirements report."""
    return build_source_export_control_requirements(source)


def generate_source_export_control_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceExportControlRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_export_control_requirements(source)


def derive_source_export_control_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceExportControlRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_export_control_requirements(source)


def summarize_source_export_control_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceExportControlRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted export-control requirements."""
    if isinstance(source_or_result, SourceExportControlRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_export_control_requirements(source_or_result).summary


def source_export_control_requirements_to_dict(report: SourceExportControlRequirementsReport) -> dict[str, Any]:
    """Serialize an export-control requirements report to a plain dictionary."""
    return report.to_dict()


source_export_control_requirements_to_dict.__test__ = False


def source_export_control_requirements_to_dicts(
    requirements: (
        tuple[SourceExportControlRequirement, ...]
        | list[SourceExportControlRequirement]
        | SourceExportControlRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize export-control requirement records to dictionaries."""
    if isinstance(requirements, SourceExportControlRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_export_control_requirements_to_dicts.__test__ = False


def source_export_control_requirements_to_markdown(report: SourceExportControlRequirementsReport) -> str:
    """Render an export-control requirements report as Markdown."""
    return report.to_markdown()


source_export_control_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_category: ExportControlCategory
    requirement_text: str
    jurisdiction: str | None
    screening_provider: str | None
    denied_party_action: str | None
    review_owner: str | None
    renewal_cadence: str | None
    source_field: str
    evidence: str
    confidence: ExportControlConfidence


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
                        requirement_category=category,
                        requirement_text=_requirement_text(segment.text),
                        jurisdiction=_field_value_detail("jurisdiction", segment.text) or _match_detail(_JURISDICTION_RE, searchable),
                        screening_provider=_field_value_detail("screening_provider", segment.text)
                        or _field_value_detail("provider", segment.text)
                        or _match_detail(_NAMED_PROVIDER_RE, segment.text)
                        or _match_detail(_PROVIDER_RE, segment.text),
                        denied_party_action=_field_value_detail("denied_party_action", segment.text) or _detail(_ACTION_RE, segment.text),
                        review_owner=_field_value_detail("review_owner", segment.text)
                        or _field_value_detail("owner", segment.text)
                        or _match_detail(_OWNER_RE, segment.text),
                        renewal_cadence=_field_value_detail("renewal_cadence", segment.text) or _match_detail(_CADENCE_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(category, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceExportControlRequirement]:
    grouped: dict[tuple[str | None, ExportControlCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.requirement_category,
                _dedupe_requirement_key(candidate.requirement_text, candidate.requirement_category),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceExportControlRequirement] = []
    for (_source_brief_id, category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        missing_details = _missing_details(category, items)
        requirements.append(
            SourceExportControlRequirement(
                source_brief_id=best.source_brief_id,
                requirement_category=category,
                requirement_text=best.requirement_text,
                jurisdiction=_first_detail(item.jurisdiction for item in items),
                screening_provider=_first_detail(item.screening_provider for item in items),
                denied_party_action=_first_detail(item.denied_party_action for item in items),
                review_owner=_first_detail(item.review_owner for item in items),
                renewal_cadence=_first_detail(item.renewal_cadence for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                missing_details=missing_details,
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                readiness="ready" if not missing_details else "needs_detail",
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.requirement_category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.readiness,
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
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _EXPORT_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_EXPORT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if any(pattern.search(part) for pattern in _CATEGORY_PATTERNS.values()) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[ExportControlCategory, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    explicit_category = _explicit_category(segment.text)
    if explicit_category:
        return (explicit_category,)
    if not (
        _EXPORT_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    ):
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
        or any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    ):
        return ()
    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
    ]
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(segment.text)
    ]
    return tuple(_dedupe(field_categories + categories))


def _explicit_category(text: str) -> ExportControlCategory | None:
    match = re.search(r"\b(?:requirement_category|export_control_category|category):\s*([a-zA-Z0-9_ -]+?)(?:;|$)", text)
    if not match:
        return None
    value = match.group(1).strip().casefold().replace("-", "_").replace(" ", "_")
    return value if value in _CATEGORY_ORDER else None


def _confidence(category: ExportControlCategory, segment: _Segment) -> ExportControlConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_category = bool(
        _CATEGORY_PATTERNS[category].search(segment.text)
        or _FIELD_CATEGORY_PATTERNS[category].search(field_words)
    )
    detail_count = sum(
        1
        for value in (
            _match_detail(_JURISDICTION_RE, searchable),
            _match_detail(_PROVIDER_RE, segment.text),
            _detail(_ACTION_RE, segment.text),
            _match_detail(_OWNER_RE, segment.text),
            _match_detail(_CADENCE_RE, segment.text),
        )
        if value
    )
    if has_category and has_explicit_requirement and has_structured_context and detail_count >= 1:
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceExportControlRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.requirement_category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for requirement in requirements if requirement.readiness == readiness)
            for readiness in ("ready", "needs_detail")
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.requirement_category == category for requirement in requirements)
        ],
    }


def _missing_details(category: ExportControlCategory, items: Iterable[_Candidate]) -> tuple[str, ...]:
    candidates = tuple(items)
    needed: tuple[str, ...]
    if category == "sanctions_screening":
        needed = ("jurisdiction", "screening_provider", "denied_party_action", "review_owner", "renewal_cadence")
    elif category == "restricted_country":
        needed = ("jurisdiction", "denied_party_action", "review_owner")
    elif category == "export_classification":
        needed = ("jurisdiction", "review_owner", "renewal_cadence")
    elif category == "denied_party_check":
        needed = ("screening_provider", "denied_party_action", "review_owner", "renewal_cadence")
    elif category == "technology_transfer":
        needed = ("jurisdiction", "denied_party_action", "review_owner")
    elif category == "residence_gating":
        needed = ("jurisdiction", "denied_party_action", "review_owner")
    else:
        needed = ("review_owner", "renewal_cadence")
    values = {
        "jurisdiction": any(candidate.jurisdiction for candidate in candidates),
        "screening_provider": any(candidate.screening_provider for candidate in candidates),
        "denied_party_action": any(candidate.denied_party_action for candidate in candidates),
        "review_owner": any(candidate.review_owner for candidate in candidates),
        "renewal_cadence": any(candidate.renewal_cadence for candidate in candidates),
    }
    return tuple(detail for detail in _DETAIL_ORDER if detail in needed and not values[detail])


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "requirement_category",
            "export_control_category",
            "jurisdiction",
            "screening_provider",
            "provider",
            "denied_party_action",
            "review_owner",
            "owner",
            "renewal_cadence",
            "sanctions_screening",
            "restricted_country",
            "export_classification",
            "denied_party_check",
            "technology_transfer",
            "residence_gating",
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
        "metadata",
        "brief_metadata",
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


def _detail(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(0)).casefold()


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


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


def _dedupe_requirement_key(value: str, category: ExportControlCategory) -> str:
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
    "ExportControlCategory",
    "ExportControlConfidence",
    "ExportControlReadiness",
    "SourceExportControlRequirement",
    "SourceExportControlRequirementsReport",
    "build_source_export_control_requirements",
    "derive_source_export_control_requirements",
    "extract_source_export_control_requirements",
    "generate_source_export_control_requirements",
    "source_export_control_requirements_to_dict",
    "source_export_control_requirements_to_dicts",
    "source_export_control_requirements_to_markdown",
    "summarize_source_export_control_requirements",
]
